#!/usr/bin/env perl

#  Copyright (C) 2011 DeNA Co.,Ltd.
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 2 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#  Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

package MHA::HealthCheck;

use strict;
use warnings FATAL => 'all';
use Carp qw(croak);
use English qw(-no_match_vars);
use Time::HiRes qw( sleep gettimeofday tv_interval );
use POSIX;
use DBI;
use IO::File;
use MHA::DBHelper;
use MHA::ManagerConst;
use MHA::FileStatus;
use MHA::SlaveUtil;
use MHA::NodeUtil;

sub new {
  my $class = shift;
  my $self  = {
    dbh                    => undef,
    interval               => undef,
    user                   => undef,
    password               => undef,
    ip                     => undef,
    hostname               => undef,
    port                   => undef,
    ssh_user               => undef,
    ssh_host               => undef,
    ssh_ip                 => undef,
    ssh_port               => undef,
    ssh_check_command      => undef,
    ssh_connection_timeout => undef,
    workdir                => undef,
    status_handler         => undef,
    secondary_check_script => undef,
    logger                 => undef,
    logfile                => undef,
    ping_type              => undef,

    # internal (read/write) variables
    _tstart            => undef,
    _already_monitored => 0,
    _need_reconnect    => 1,
    _last_ping_fail    => 1,
    _sec_check_invoked => 0,
    _sec_check_pid     => undef,
    _ssh_check_invoked => 0,
    _ssh_check_pid     => undef,
    @_,
  };
  return bless $self, $class;
}

sub connect {
  my $self                  = shift;
  my $connect_timeout       = shift;
  my $wait_timeout          = shift;
  my $advisory_lock_timeout = shift;
  my $log_connect_error     = shift;
  my $raise_error           = shift;
  if ( !defined($connect_timeout) ) {
    $connect_timeout = $self->{interval};
  }
  if ( !defined($wait_timeout) ) {
    $wait_timeout = $connect_timeout * 2;
  }
  if ( !defined($advisory_lock_timeout) ) {
    $advisory_lock_timeout = $wait_timeout * 2;
  }
  if ( !defined($log_connect_error) ) {
    $log_connect_error = 1;
  }
  if ( !defined($raise_error) ) {
    $raise_error = 0;
  }
  my $log = $self->{logger};
  $self->{dbh} = DBI->connect(
    "DBI:mysql:;host=$self->{ip};"
      . "port=$self->{port};mysql_connect_timeout=$connect_timeout",
    $self->{user},
    $self->{password},
    { PrintError => 0, RaiseError => $raise_error }
  );
  if ( $self->{dbh} ) {
    $log->debug("Connected on master.");
    $self->{dbh}->{InactiveDestroy} = 1;
    $self->set_wait_timeout($wait_timeout);
    my $rc =
      MHA::SlaveUtil::get_monitor_advisory_lock( $self->{dbh},
      $advisory_lock_timeout );
    if ( $rc == 0 ) {
      return 0;
    }
    elsif ( $rc == 1 ) {

      # locked by someone or (in rare cases) my previous uncleaned connection
      $self->{_already_monitored} = 1;
      croak;
    }
    else {
      my $msg = "Got unexpected error on getting MySQL advisory lock: ";
      $msg .= $DBI::err if ($DBI::err);
      $msg .= " ($DBI::errstr)" if ($DBI::errstr);
      $log->warning($msg);
      return 1;
    }
  }
  else {
    my $msg = "Got error on MySQL connect: ";
    $msg .= $DBI::err if ($DBI::err);
    $msg .= " ($DBI::errstr)" if ($DBI::errstr);
    if ($log_connect_error) {
      $log->warning($msg);
    }
    else {
      $log->debug($msg);
    }
    return ( 1, $DBI::err );
  }
}

sub disconnect_if {
  my $self = shift;
  my $dbh  = $self->{dbh};
  $dbh->disconnect() if ($dbh);
  $self->{dbh} = undef;
}

sub set_ping_interval($$) {
  my $self     = shift;
  my $interval = shift;
  $self->{interval} = $interval if ($interval);
  return;
}

sub get_ping_interval($) {
  my $self = shift;
  return $self->{interval};
}

sub set_secondary_check_script($$) {
  my $self   = shift;
  my $script = shift;
  $self->{secondary_check_script} = $script if ($script);
  return;
}

sub get_secondary_check_script($) {
  my $self = shift;
  return $self->{secondary_check_script};
}

sub set_ssh_user($$) {
  my $self     = shift;
  my $ssh_user = shift;
  $self->{ssh_user} = $ssh_user if ($ssh_user);
  return;
}

sub get_ssh_user($) {
  my $self = shift;
  return $self->{ssh_user};
}

sub set_workdir($$) {
  my $self    = shift;
  my $workdir = shift;
  $self->{workdir} = $workdir if ($workdir);
  return;
}

sub get_workdir($) {
  my $self = shift;
  return $self->{workdir};
}

sub set_wait_timeout($$) {
  my $self    = shift;
  my $timeout = shift;
  my $log     = $self->{logger};
  my $dbh     = $self->{dbh};
  if ( MHA::DBHelper::set_wait_timeout_util( $self->{dbh}, $timeout ) ) {
    my $msg = "Got error on setting wait_timeout : $@ :";
    $msg .= $DBI::err if ($DBI::err);
    $msg .= " ($DBI::errstr)" if ($DBI::errstr);
    $log->warning($msg);
  }
  else {
    $log->debug("Set short wait_timeout on master: $timeout seconds");
  }
}

sub ping_connect($) {
  my $self = shift;
  my $log  = $self->{logger};
  my $dbh;
  my $rc          = 1;
  my $max_retries = 2;
  eval {
    my $ping_start = [gettimeofday];
    while ( !$self->{dbh} && $max_retries-- ) {
      eval { $rc = $self->connect( 1, $self->{interval}, 0, 0, 1 ); };
      if ( !$self->{dbh} && $@ ) {
        die $@ if ( !$max_retries );
      }
    }
    $rc = $self->ping_select();

    # To hold advisory lock for some periods of time
    $self->sleep_until( $ping_start, $self->{interval} - 1.5 );
    $self->disconnect_if();
  };
  if ($@) {
    my $msg = "Got error on MySQL connect ping: $@";
    undef $@;
    $msg .= $DBI::err if ($DBI::err);
    $msg .= " ($DBI::errstr)" if ($DBI::errstr);
    $log->warning($msg) if ($log);
    $rc = 1;
  }
  return 2 if ( $self->{_already_monitored} );
  return $rc;
}

sub ping_select($) {
  my $self = shift;
  my $log  = $self->{logger};
  my $dbh  = $self->{dbh};
  my ( $query, $sth, $href );
  eval {
    $dbh->{RaiseError} = 1;
    $sth = $dbh->prepare("SELECT 1 As Value");
    $sth->execute();
    $href = $sth->fetchrow_hashref;
    if ( !defined($href)
      || !defined( $href->{Value} )
      || $href->{Value} != 1 )
    {
      die;
    }
  };
  if ($@) {
    my $msg = "Got error on MySQL select ping: ";
    undef $@;
    $msg .= $DBI::err if ($DBI::err);
    $msg .= " ($DBI::errstr)" if ($DBI::errstr);
    $log->warning($msg) if ($log);
    return 1;
  }
  return 0;
}

sub ssh_check_simple {
  my $ssh_user            = shift;
  my $ssh_host            = shift;
  my $ssh_ip              = shift;
  my $ssh_port            = shift;
  my $log                 = shift;
  my $num_secs_to_timeout = shift;
  return ssh_check( $ssh_user, $ssh_host, $ssh_ip, $ssh_port, $log,
    $num_secs_to_timeout, "exit 0" );
}

sub ssh_check {
  my $ssh_user            = shift;
  my $ssh_host            = shift;
  my $ssh_ip              = shift;
  my $ssh_port            = shift;
  my $log                 = shift;
  my $num_secs_to_timeout = shift;
  my $command             = shift;
  my $ssh_user_host       = $ssh_user . '@' . $ssh_ip;
  my $rc                  = 1;
  eval {
    if ( my $pid = fork ) {
      local $SIG{ALRM} = sub {
        kill 9, $pid;
        waitpid( $pid, 0 );
        die "Got timeout on checking SSH connection to $ssh_host!";
      };
      $log->debug(
"SSH connection test to $ssh_host, option $MHA::ManagerConst::SSH_OPT_CHECK, timeout $num_secs_to_timeout"
      );
      alarm $num_secs_to_timeout;
      waitpid( $pid, 0 );
      alarm 0;
      my ( $high, $low ) = MHA::NodeUtil::system_rc($?);
      if ( $high ne '0' || $low ne '0' ) {
        $log->warning("HealthCheck: SSH to $ssh_host is NOT reachable.");
        $rc = 1;
      }
      else {
        $log->info("HealthCheck: SSH to $ssh_host is reachable.");
        $rc = 0;
      }
    }
    elsif ( defined $pid ) {
      exec(
"ssh $MHA::ManagerConst::SSH_OPT_CHECK -p $ssh_port $ssh_user_host \"$command\""
      );
    }
    else {
      croak "Forking SSH connection process failed!\n";
    }
  };
  alarm 0;
  if ($@) {
    $log->warning("HealthCheck: $@");
    $rc = 1;
  }
  return $rc;
}

sub secondary_check($) {
  my $self = shift;
  my $log  = $self->{logger};
  my $command =
      "$self->{secondary_check_script} "
    . " --user=$self->{ssh_user} "
    . " --master_host=$self->{hostname} "
    . " --master_ip=$self->{ip} "
    . " --master_port=$self->{port}";
  if ($MHA::ManagerConst::USE_SSH_OPTIONS) {
    $command .= " --options='$MHA::ManagerConst::SSH_OPT_CHECK' ";
  }
  $log->info("Executing seconary network check script: $command");
  my ( $high, $low ) =
    MHA::ManagerUtil::exec_system( $command, $self->{logfile} );
  if ( $high == 0 && $low == 0 ) {
    $log->info( "Master is not reachable from all other monitoring "
        . "servers. Failover should start." );
    return 0;
  }
  if ( $high == 2 ) {
    $log->warning( "At least one of monitoring servers is not reachable "
        . "from this script. This is likely network problem. Failover should "
        . "not happen." );
    return $high;
  }
  elsif ( $high == 3 ) {
    $log->warning( "Master is reachable from at least one of other "
        . "monitoring servers. Failover should not happen." );
    return $high;
  }
  else {
    $log->error("Got unknown error from $command. exit.");
    return 1;
  }
}

sub terminate_child {
  my $self                = shift;
  my $pid                 = shift;
  my $type                = shift;
  my $num_secs_to_timeout = shift;
  unless ($num_secs_to_timeout) {
    $num_secs_to_timeout = $self->{interval};
  }
  my $log             = $self->{logger};
  my $child_exit_code = 0;
  eval {
    local $SIG{ALRM} = sub {
      kill 9, $pid;
      waitpid( $pid, 0 );
      die "Got timeout on $type child process and killed it!";
    };
    alarm $num_secs_to_timeout;
    waitpid( $pid, 0 );
    alarm 0;
    $child_exit_code = $? >> 8;
  };
  alarm 0;
  if ($@) {
    $log->warning($@) if ($log);
    undef $@;
    $child_exit_code = 1;
  }
  return $child_exit_code;
}

sub invoke_sec_check {
  my $self = shift;
  if ( !$self->{_sec_check_invoked} ) {
    if ( $self->{_sec_check_pid} = fork ) {
      $self->{_sec_check_invoked} = 1;
    }
    elsif ( defined $self->{_sec_check_pid} ) {
      $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";

      #child secondary check process
      exit $self->secondary_check() if ( $self->{secondary_check_script} );
      exit 0;
    }
    else {
      croak
        "Forking secondary check process failed. Can't contine operation.\n";
    }
  }
}

sub invoke_ssh_check {
  my $self = shift;
  my $log  = $self->{logger};
  if ( !$self->{_ssh_check_invoked} ) {
    if ( $self->{_ssh_check_pid} = fork ) {
      $self->{_ssh_check_invoked} = 1;
    }
    elsif ( defined $self->{_ssh_check_pid} ) {
      $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";
      $log->info("Executing SSH check script: $self->{ssh_check_command}");

      #child ssh check process
      exit ssh_check(
        $self->{ssh_user}, $self->{ssh_host},
        $self->{ssh_ip},   $self->{ssh_port},
        $self->{logger},   $self->{ssh_connection_timeout},
        $self->{ssh_check_command}
      );
    }
    else {
      croak "Forking SSH check process failed. Can't continue operation.\n";
    }
  }
}

sub is_secondary_down {
  my $self           = shift;
  my $log            = $self->{logger};
  my $master_is_down = 0;
  eval {
    if ( $self->{_sec_check_invoked} ) {
      waitpid( $self->{_sec_check_pid}, 0 );
      my $sec_check_exit_code = $?;
      $self->{_sec_check_invoked} = 0;
      if ( $sec_check_exit_code == 0 ) {
        $master_is_down = 1;
      }
      else {
        $log->warning(
"Secondary network check script returned errors. Failover should not start so checking server status again. Check network settings for details."
        );
      }
    }
    else {
      $master_is_down = 1;
    }
  };
  if ($@) {
    $log->error("Got unexpected error on secondary network check: $@");
    undef $@;
  }
  return $master_is_down;
}

sub is_ssh_reachable {
  my $self          = shift;
  my $log           = $self->{logger};
  my $ssh_reachable = 2;
  eval {
    if ( $self->{_ssh_check_invoked} ) {
      waitpid( $self->{_ssh_check_pid}, 0 );
      my $ssh_check_exit_code = $?;
      $self->{_ssh_check_invoked} = 0;
      if ( $ssh_check_exit_code == 0 ) {
        $ssh_reachable = 1;
      }
      else {
        $ssh_reachable = 0;
      }
    }
  };
  if ($@) {
    $log->error("Got unexpected error on SSH check: $@");
    undef $@;
  }
  return $ssh_reachable;
}

sub kill_sec_check {
  my $self                = shift;
  my $num_secs_to_timeout = shift;
  my $exit_code           = 1;
  if ( $self->{_sec_check_invoked} ) {
    if ( defined( $self->{_sec_check_pid} ) ) {
      $exit_code = $self->terminate_child(
        $self->{_sec_check_pid},
        "Secondary Check",
        $num_secs_to_timeout
      );
    }
    $self->{_sec_check_invoked} = 0;
  }
  return $exit_code;
}

sub kill_ssh_check {
  my $self                = shift;
  my $num_secs_to_timeout = shift;
  my $exit_code           = 1;
  if ( $self->{_ssh_check_invoked} ) {
    if ( defined( $self->{_ssh_check_pid} ) ) {
      $exit_code = $self->terminate_child( $self->{_ssh_check_pid},
        "SSH Check", $num_secs_to_timeout );
    }
    $self->{_ssh_check_invoked} = 0;
  }
  return $exit_code;
}

sub update_status_ok {
  my $self = shift;

  #updating status time filestamp
  if ( $self->{_last_ping_fail} ) {
    $self->{status_handler}->update_status($MHA::ManagerConst::ST_RUNNING_S);
    $self->{_last_ping_fail} = 0;
  }
  else {
    $self->{status_handler}
      ->update_status_time($MHA::ManagerConst::ST_RUNNING_S);
  }
}

sub sleep_until {
  my $self     = shift;
  my $start    = shift;
  my $interval = shift;
  unless ($start) {
    $start = $self->{_tstart};
  }
  if ( !defined($interval) ) {
    $interval = $self->{interval};
  }
  my $elapsed = tv_interval($start);
  if ( $interval > $elapsed ) {
    sleep( $interval - $elapsed );
  }
}

sub handle_failing {
  my $self = shift;
  $self->{_last_ping_fail} = 1;
  $self->{status_handler}->update_status($MHA::ManagerConst::ST_PING_FAILING_S);
  $self->invoke_sec_check();
  $self->invoke_ssh_check();
}

# main function
sub wait_until_unreachable($) {
  my $self           = shift;
  my $log            = $self->{logger};
  my $ssh_reachable  = 2;
  my $error_count    = 0;
  my $master_is_down = 0;

  eval {
    while (1) {
      $self->{_tstart} = [gettimeofday];
      if ( $self->{_need_reconnect} ) {
        my ( $rc, $mysql_err ) = $self->connect();
        if ($rc) {
          if ($mysql_err) {
            if (
              grep ( $_ == $mysql_err, @MHA::ManagerConst::ALIVE_ERROR_CODES )
              > 0 )
            {
              $log->info(
"Got MySQL error $mysql_err, but this is not mysql crash. Continue health check.."
              );
              $self->sleep_until();
              next;
            }
          }
          $error_count++;
          $log->warning("Connection failed $error_count time(s)..");
          $self->handle_failing();

          if ( $error_count >= 3 ) {
            $ssh_reachable = $self->is_ssh_reachable();
            $master_is_down = 1 if ( $self->is_secondary_down() );
            last if ($master_is_down);
            $error_count = 0;
          }
          $self->sleep_until();
          next;
        }

        # connection ok
        $self->{_need_reconnect} = 0;
        $log->info(
"Ping($self->{ping_type}) succeeded, waiting until MySQL doesn't respond.."
        );
      }
      $self->disconnect_if()
        if ( $self->{ping_type} eq $MHA::ManagerConst::PING_TYPE_CONNECT );

      if ( my $pid = fork ) {

        # Parent process forks one child process. The child process queries
        # from MySQL every <interval> seconds. The child process may hang on
        # executing queries.
        # Since current DBD::mysql does not have an option to set
        # read timeout, executing queries might take forever. To avoid this,
        # the parent process kills the child process if it won't exit within
        # <interval> seconds.

        my $child_exit_code = $self->terminate_child( $pid, "MySQL Ping" );
        if ( $child_exit_code == 0 ) {

          #ping ok
          $self->update_status_ok();
          if ( $error_count > 0 ) {
            $error_count = 0;
          }
          $self->kill_sec_check();
          $self->kill_ssh_check();
        }
        elsif ( $child_exit_code == 2 ) {
          $self->{_already_monitored} = 1;
          croak;
        }
        else {

          # failed on child
          $self->{_need_reconnect} = 1;
          $self->handle_failing();
        }
        $self->sleep_until();
      }
      elsif ( defined $pid ) {
        $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";

        # Child process
        eval {
          if ( $self->{ping_type} eq $MHA::ManagerConst::PING_TYPE_CONNECT ) {
            exit $self->ping_connect();
          }
          elsif ( $self->{ping_type} eq $MHA::ManagerConst::PING_TYPE_SELECT ) {
            exit $self->ping_select();
          }
          else {
            die "Not supported ping_type!\n";
          }
          exit 0;
        };
        if ($@) {
          my $msg = "Unexpected error heppened when pinging from child! $@";
          $log->error($msg);
          undef $@;
          exit 1;
        }
      }
      else {
        croak "fork failed!\n";
      }
    }
    $log->warning("Master is not reachable from health checker!");
  };
  if ($@) {
    my $msg = "Got error when monitoring master: $@";
    $log->warning($msg);
    undef $@;
    return 2 if ( $self->{_already_monitored} );
    return 1;
  }
  return 1 unless ($master_is_down);
  return ( 0, $ssh_reachable );
}

1;
