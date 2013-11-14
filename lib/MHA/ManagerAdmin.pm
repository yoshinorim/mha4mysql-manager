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

package MHA::ManagerAdmin;

use strict;
use warnings FATAL => 'all';
use Carp qw(croak);
use File::Copy;
use Errno ();
use MHA::ManagerConst;
use MHA::FileStatus;
use MHA::ManagerUtil;
use Time::HiRes qw( sleep );

$| = 1;

sub new {
  my $class = shift;
  my $self  = {
    app            => undef,
    pid            => undef,
    global_conf    => $MHA::ManagerConst::DEFAULT_GLOBAL_CONF,
    conf           => undef,
    status_dir     => undef,
    status_file    => undef,
    logfile        => undef,
    master_info    => undef,
    ping_limit     => 10,
    status_handler => undef,
    abort          => 0,
    abort_timeout  => 5,
    @_,
  };
  return bless $self, $class;
}

sub check_timestamp($) {
  my $self         = shift;
  my $status_file  = $self->{status_handler}->{status_file};
  my $current_time = time();
  my $ping_time    = ( stat $status_file )[9];
  unless ($ping_time) {
    print "Failed to get timestamp on $status_file!\n";
  }
  if ( $current_time > $ping_time + $self->{ping_limit} ) {
    printf( "$status_file behinds %d seconds!\n", $current_time - $ping_time );
    return 1;
  }

  return 0;
}

sub pid_trim($) {
  my @pids   = shift;
  my @target = ();
  foreach my $pid (@pids) {
    next unless ($pid);
    chomp($pid);
    push @target, $pid if ( $pid > 1 );
  }
  return @target;
}

sub get_child_pids($) {
  my $self       = shift;
  my $pid        = $self->{pid};
  my @child_pids = ();
  @child_pids = `ps --ppid $pid | grep -v PID | awk '{print \$1}'`;
  return pid_trim(@child_pids);
}

sub is_pid_alive($) {
  my $self       = shift;
  my $pid        = $self->{pid};
  my $pid_exists = kill 0, $pid;
  return $pid_exists;
}

sub check_status_file_pid($) {
  my $self = shift;
  if ( !-f $self->{status_handler}->{status_file} ) {
    return $MHA::ManagerConst::ST_NOT_RUNNING;
  }
  my $status_string;
  ( $self->{pid}, $status_string, $self->{master_info} ) =
    $self->{status_handler}->read_status();
  my $pid_exists = $self->is_pid_alive();

  unless ($pid_exists) {
    my @child_pids = $self->get_child_pids();
    return $MHA::ManagerConst::ST_NOT_RUNNING if ( $#child_pids == -1 );
    return $MHA::ManagerConst::ST_PARTIALLY_RUNNING;
  }

  return $MHA::ManagerConst::ST_INITIALIZING_MONITOR
    if ( $MHA::ManagerConst::ST_INITIALIZING_MONITOR_S eq $status_string );
  return $MHA::ManagerConst::ST_RETRYING_MONITOR
    if ( $MHA::ManagerConst::ST_RETRYING_MONITOR_S eq $status_string );
  return $MHA::ManagerConst::ST_PING_FAILING
    if ( $MHA::ManagerConst::ST_PING_FAILING_S eq $status_string );
  return $MHA::ManagerConst::ST_PING_FAILED
    if ( $MHA::ManagerConst::ST_PING_FAILED_S eq $status_string );
  return $MHA::ManagerConst::ST_CONFIG_ERROR
    if ( $MHA::ManagerConst::ST_CONFIG_ERROR_S eq $status_string );
  return $MHA::ManagerConst::ST_FAILOVER_RUNNING
    if ( $MHA::ManagerConst::ST_FAILOVER_RUNNING_S eq $status_string );
  return $MHA::ManagerConst::ST_FAILOVER_ERROR
    if ( $MHA::ManagerConst::ST_FAILOVER_ERROR_S eq $status_string );

  if ( $MHA::ManagerConst::ST_RUNNING_S eq $status_string ) {
    if ( $self->check_timestamp() ) {
      return $MHA::ManagerConst::ST_TIMESTAMP_OLD;
    }
    else {
      return $MHA::ManagerConst::ST_RUNNING;
    }
  }
  croak "Got unknown status: $status_string\n";
}

sub read_conf($) {
  my $self = shift;
  if ( !-f $self->{conf} ) {
    croak "$self->{conf} not found!\n";
  }

  # Loading Config takes time so we load module only when really needed
  require MHA::Config;
  my $conf = new MHA::Config(
    globalfile => $self->{global_conf},
    file       => $self->{conf},
  );
  my ( $sc_ref, undef ) = $conf->read_config();
  my @sc = @$sc_ref;
  $self->{status_dir} = $sc[0]->{manager_workdir}
    unless ( $self->{status_dir} );
  $self->{logfile} = $sc[0]->{manager_log};
  return $self;
}

sub init {
  my $self = shift;
  if ( $self->{conf} ) {
    $self = $self->read_conf();
  }

  unless ( $self->{status_dir} ) {
    if ( $self->{conf} ) {
      croak
"Failed to get status file directory (set in workdir parameter) from $self->{conf}.\n";
    }
    else {
      croak "Either --conf or --status_dir must be set.\n";
    }
  }
  if ( !-d $self->{status_dir} ) {
    croak "Directory $self->{status_dir} not found!\n";
  }
  if ( $self->{conf} ) {
    $self->{status_handler} = new MHA::FileStatus(
      conffile => $self->{conf},
      dir      => $self->{status_dir}
    );
    $self->{status_handler}->init();
    $self->{app} = $self->{status_handler}->{basename} unless ( $self->{app} );
  }
  unless ( $self->{app} ) {
    croak "app is not defined or could not be fetched from conf file!\n";
  }
  unless ( $self->{conf} ) {
    $self->{status_handler} = new MHA::FileStatus(
      basename => $self->{app},
      dir      => $self->{status_dir}
    );
    $self->{status_handler}->{basename} = $self->{app};
    $self->{status_handler}->init();
  }
}

sub stop_manager {
  my $self = shift;
  my $app;
  my $exit_code = 1;
  my $ret;
  $exit_code = eval {
    $self->init();
    $app = $self->{app};
    $ret = $self->check_status_file_pid();
    if ( $ret == $MHA::ManagerConst::ST_NOT_RUNNING ) {
      print
"MHA Manager is not running on $self->{app}($MHA::ManagerConst::ST_NOT_RUNNING_S).\n";
      return 0;
    }
    elsif ( $ret == $MHA::ManagerConst::ST_FAILOVER_RUNNING
      && !$self->{abort} )
    {
      print "Currently Failover is running on $self->{app}. Should not stop.\n";
      return 1;
    }
    else {
      if ( kill -15, getpgrp $self->{pid} ) {
        my $millis_until_abort = $self->{abort_timeout} * 1000;
        while ( $millis_until_abort > 0 ) {
          my $pid_exists = kill 0, $self->{pid};
          unless ($pid_exists) {
            print "Stopped $self->{app} successfully.\n";
            return 0;
          }
          sleep(0.1);
          $millis_until_abort = $millis_until_abort - 100;
        }
        print
"Manager process $self->{pid} didn't stop in $self->{abort_timeout} seconds.\n";
        if ( $self->{abort} && ( kill -9, getpgrp $self->{pid} ) ) {
          print "Forced stopped $self->{app}.\n";
          return 0;
        }
      }
    }
    print "Failed to stop $self->{app}.\n";
    return 1;
  };
  if ($@) {
    my $msg;
    if ( $self->{conf} ) {
      $msg .= "Got error on conf $self->{conf}: ";
    }
    elsif ( $self->{app} ) {
      $msg .= "Got error on app $app: ";
    }
    $msg .= $@;
    warn $msg;
    undef $@;
  }
  return 1 if ( !defined($exit_code) );
  return $exit_code;
}

sub check_status {
  my $self = shift;
  my $app;
  my $ret = 1;
  eval {
    $self->init();
    $app = $self->{app};
    $ret = $self->check_status_file_pid();
    if ( $ret == $MHA::ManagerConst::ST_RUNNING ) {
      print
"$app (pid:$self->{pid}) is running($MHA::ManagerConst::ST_RUNNING_S), $self->{master_info}";
      print "\n";
    }
    elsif ( $ret == $MHA::ManagerConst::ST_NOT_RUNNING ) {
      print "$app is stopped($MHA::ManagerConst::ST_NOT_RUNNING_S).\n";
    }
    elsif ( $ret == $MHA::ManagerConst::ST_PARTIALLY_RUNNING ) {
      my (@child_pids) = $self->get_child_pids();
      print
        "Main process is not running, but child process is running on $app ";
      if ( $#child_pids > -1 ) {
        printf( "(child pid: %s)", join( ' ', @child_pids ) );
      }
      print
" ($MHA::ManagerConst::ST_PARTIALLY_RUNNING_S). Check ps output for details and kill it.\n";
    }
    elsif ( $ret == $MHA::ManagerConst::ST_INITIALIZING_MONITOR ) {
      print
"$app monitoring program is now on initialization phase($MHA::ManagerConst::ST_INITIALIZING_MONITOR_S). Wait for a while and try checking again.\n";
    }
    elsif ( $ret == $MHA::ManagerConst::ST_PING_FAILING
      || $ret == $MHA::ManagerConst::ST_PING_FAILED )
    {
      print "$app master maybe down";
      print "($MHA::ManagerConst::ST_PING_FAILING_S)"
        if ( $ret == $MHA::ManagerConst::ST_PING_FAILING );
      print "($MHA::ManagerConst::ST_PING_FAILED_S)"
        if ( $ret == $MHA::ManagerConst::ST_PING_FAILED );
      print ". $self->{master_info}\n";
      print "Check $self->{logfile} for details.\n" if ( $self->{logfile} );
    }
    elsif ( $ret == $MHA::ManagerConst::ST_RETRYING_MONITOR ) {
      print
"$app monitoring waits for retrying to monitor master again($MHA::ManagerConst::ST_RETRYING_MONITOR_S). Wait for a while and try checking again.";
    }
    elsif ( $ret == $MHA::ManagerConst::ST_CONFIG_ERROR ) {
      print
"$app servers are not correctly configured($MHA::ManagerConst::ST_CONFIG_ERROR_S).\n";
      print "Check $self->{logfile} for details.\n" if ( $self->{logfile} );
    }
    elsif ( $ret == $MHA::ManagerConst::ST_TIMESTAMP_OLD ) {
      print
"$app is running, but master_ping.health is too old($MHA::ManagerConst::ST_TIMESTAMP_OLD_S). Maybe process hangs?";
      print " $self->{master_info}" if ( $self->{master_info} );
      print "\n";
    }
    elsif ( $ret == $MHA::ManagerConst::ST_FAILOVER_RUNNING ) {
      print
"$app master is down and failover is running($MHA::ManagerConst::ST_FAILOVER_RUNNING_S). $self->{master_info}\n";
      print "Check $self->{logfile} for details.\n" if ( $self->{logfile} );
    }
    elsif ( $ret == $MHA::ManagerConst::ST_FAILOVER_ERROR ) {
      print
"$app master is down and failover was not successful($MHA::ManagerConst::ST_FAILOVER_ERROR_S). $self->{master_info}\n";
      print "Check $self->{logfile} for details.\n" if ( $self->{logfile} );
    }
    else {
      print "Unexpected error on $app.\n";
    }
  };
  if ($@) {
    my $msg;
    if ( $self->{conf} ) {
      $msg .= "Got error on conf $self->{conf}: ";
    }
    elsif ( $self->{app} ) {
      $msg .= "Got error on app $app: ";
    }
    $msg .= $@;
    warn $msg;
    undef $@;
  }
  return $ret;
}
1;
