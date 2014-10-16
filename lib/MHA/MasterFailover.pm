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

package MHA::MasterFailover;

use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Carp qw(croak);
use Getopt::Long qw(:config pass_through);
use Log::Dispatch;
use Log::Dispatch::File;
use MHA::NodeUtil;
use MHA::Config;
use MHA::ServerManager;
use MHA::FileStatus;
use MHA::ManagerUtil;
use MHA::ManagerConst;
use MHA::HealthCheck;
use File::Basename;
use Parallel::ForkManager;
use Sys::Hostname;

my $g_global_config_file = $MHA::ManagerConst::DEFAULT_GLOBAL_CONF;
my $g_config_file;
my $g_new_master_host;
my $g_new_master_port = 3306;
my $g_interactive     = 1;
my $g_ssh_reachable   = 2;
my $g_workdir;
my $g_logfile;
my $g_last_failover_minute   = 480;
my $g_wait_on_failover_error = 0;
my $g_ignore_last_failover;
my $g_skip_save_master_binlog;
my $g_remove_dead_master_conf;
my $g_skip_change_master;
my $g_skip_disable_read_only;
my $g_wait_until_gtid_in_sync = 1;
my $g_ignore_binlog_server_error;
my $_real_ssh_reachable;
my $_saved_file_suffix;
my $_start_datetime;
my $_failover_complete_file;
my $_failover_error_file;
my %_dead_master_arg;
my $_server_manager;
my $_diff_binary_log;
my $_diff_binary_log_basename;
my $_has_saved_binlog = 0;
my $_status_handler;
my $_create_error_file = 0;
my $log;
my $mail_subject;
my $mail_body;
my $GEN_DIFF_OK = 15;

sub exit_by_signal {
  $log->info("Got terminate signal during failover. Exit.");
  eval {
    MHA::NodeUtil::create_file_if($_failover_error_file)
      if ($_create_error_file);
    MHA::NodeUtil::drop_file_if( $_status_handler->{status_file} );
  };
  if ($@) {
    $log->error("Got Error: $@");
    undef $@;
  }
  exit 1;
}

sub exec_ssh_child_cmd {
  my ( $ssh_user_host, $ssh_port, $ssh_cmd, $logger, $file ) = @_;
  my ( $high, $low ) =
    MHA::ManagerUtil::exec_ssh_cmd( $ssh_user_host, $ssh_port, $ssh_cmd,
    $file );
  if ( $logger && $file ) {
    $logger->info( "\n" . `cat $file` );
    unlink $file;
  }
  return ( $high, $low );
}

sub init_config() {
  $log = MHA::ManagerUtil::init_log($g_logfile);

  my ( $sc_ref, $binlog_ref ) = new MHA::Config(
    logger     => $log,
    globalfile => $g_global_config_file,
    file       => $g_config_file
  )->read_config();
  my @servers_config        = @$sc_ref;
  my @binlog_servers_config = @$binlog_ref;

  if ( !$g_logfile
    && !$g_interactive
    && $servers_config[0]->{manager_log} )
  {
    $g_logfile = $servers_config[0]->{manager_log};
  }
  $log =
    MHA::ManagerUtil::init_log( $g_logfile, $servers_config[0]->{log_level} );
  $log->info("MHA::MasterFailover version $MHA::ManagerConst::VERSION.");

  unless ($g_workdir) {
    if ( $servers_config[0]->{manager_workdir} ) {
      $g_workdir = $servers_config[0]->{manager_workdir};
    }
    else {
      $g_workdir = "/var/tmp";
    }
  }
  return ( \@servers_config, \@binlog_servers_config );
}

sub check_settings($) {
  my $servers_config_ref = shift;
  my @servers_config     = @$servers_config_ref;
  my $dead_master;
  MHA::ManagerUtil::check_node_version($log);
  $_status_handler =
    new MHA::FileStatus( conffile => $g_config_file, dir => $g_workdir );
  $_status_handler->init();
  $_status_handler->set_master_host( $_dead_master_arg{hostname} );
  my $appname = $_status_handler->{basename};
  $_failover_complete_file = "$g_workdir/$appname.failover.complete";
  $_failover_error_file    = "$g_workdir/$appname.failover.error";

  $_status_handler->update_status($MHA::ManagerConst::ST_FAILOVER_RUNNING_S);

  $_server_manager = new MHA::ServerManager( servers => \@servers_config );
  $_server_manager->set_logger($log);
  if ($g_interactive) {
    $_server_manager->connect_all_and_read_server_status();
  }
  else {
    $log->debug(
      "Skipping connecting to dead master $_dead_master_arg{hostname}.");
    $_server_manager->connect_all_and_read_server_status(
      $_dead_master_arg{hostname},
      $_dead_master_arg{ip}, $_dead_master_arg{port} );
  }
  my $m = $_server_manager->get_orig_master();
  if (
    !(
         $_dead_master_arg{hostname} eq $m->{hostname}
      && $_dead_master_arg{ip} eq $m->{ip}
      && $_dead_master_arg{port} eq $m->{port}
    )
    )
  {
    $log->error(
      sprintf(
"Detected dead master %s does not match with specified dead master %s(%s:%s)!",
        $m->get_hostinfo(),    $_dead_master_arg{hostname},
        $_dead_master_arg{ip}, $_dead_master_arg{port}
      )
    );
    croak;
  }

  my @dead_servers  = $_server_manager->get_dead_servers();
  my @alive_servers = $_server_manager->get_alive_servers();
  my @alive_slaves  = $_server_manager->get_alive_slaves();

  #Make sure that dead server is current master only
  $log->info("Dead Servers:");
  $_server_manager->print_dead_servers();
  if ( $#dead_servers < 0 ) {
    $log->error("None of server is dead. Stop failover.");
    croak;
  }

  my $dead_master_found = 0;
  foreach my $d (@dead_servers) {
    if ( $d->{hostname} eq $_dead_master_arg{hostname} ) {
      $dead_master_found = 1;
      $dead_master       = $d;
      last;
    }
  }
  unless ($dead_master_found) {
    $log->error(
      "The master $_dead_master_arg{hostname} is not dead. Stop failover.");
    croak;
  }

# quick check that the dead server is really dead
# not double check when ping_type is insert,
# because check_connection_fast_util can rerurn true if insert-check detects I/O failure.
  if ( $servers_config[0]->{ping_type} ne $MHA::ManagerConst::PING_TYPE_INSERT )
  {
    $log->info("Checking master reachability via MySQL(double check)...");
    if (
      my $rc = MHA::DBHelper::check_connection_fast_util(
        $dead_master->{hostname}, $dead_master->{port},
        $dead_master->{user},     $dead_master->{password}
      )
      )
    {
      $log->error(
        sprintf(
          "The master %s is reachable via MySQL (error=%s) ! Stop failover.",
          $dead_master->get_hostinfo(), $rc
        )
      );
      croak;
    }
    $log->info(" ok.");
  }

  $log->info("Alive Servers:");
  $_server_manager->print_alive_servers();
  $log->info("Alive Slaves:");
  $_server_manager->print_alive_slaves();
  $_server_manager->print_failed_slaves_if();
  $_server_manager->print_unmanaged_slaves_if();

  if ( $dead_master->{handle_raw_binlog} ) {
    $_saved_file_suffix = ".binlog";
  }
  else {
    $_saved_file_suffix = ".sql";
  }

  foreach my $slave (@alive_slaves) {

    # Master_Host is either hostname or IP address of the current master
    if ( $dead_master->{hostname} ne $slave->{Master_Host}
      && $dead_master->{ip} ne $slave->{Master_Host}
      && $dead_master->{hostname} ne $slave->{Master_IP}
      && $dead_master->{ip} ne $slave->{Master_IP} )
    {
      $log->error(
        sprintf(
          "Slave %s does not replicate from dead master %s. Stop failover.",
          $slave->get_hostinfo(),
          $dead_master->get_hostinfo()
        )
      );
      croak;
    }
    $slave->{ssh_ok} = 2;
    $slave->{diff_file_readtolatest} =
        "$slave->{remote_workdir}/relay_from_read_to_latest_"
      . $slave->{hostname} . "_"
      . $slave->{port} . "_"
      . $_start_datetime
      . $_saved_file_suffix;
  }
  $_server_manager->validate_num_alive_servers( $dead_master, 1 );

  # Checking last failover error file
  if ($g_ignore_last_failover) {
    MHA::NodeUtil::drop_file_if($_failover_error_file);
    MHA::NodeUtil::drop_file_if($_failover_complete_file);
  }
  if ( -f $_failover_error_file ) {
    my $message =
        "Failover error flag file $_failover_error_file "
      . "exists. This means the last failover failed. Check error logs "
      . "for detail, fix problems, remove $_failover_error_file, "
      . "and restart this script.";
    $log->error($message);
    croak;
  }

  if ($g_interactive) {
    print "Master "
      . $dead_master->get_hostinfo()
      . " is dead. Proceed? (yes/NO): ";
    my $ret = <STDIN>;
    chomp($ret);
    die "Stopping failover." if ( lc($ret) !~ /^y/ );
  }

  # If the last failover was done within 8 hours, we don't do failover
  # to avoid ping-pong
  if ( -f $_failover_complete_file ) {
    my $lastts       = ( stat($_failover_complete_file) )[9];
    my $current_time = time();
    if ( $current_time - $lastts < $g_last_failover_minute * 60 ) {
      my ( $sec, $min, $hh, $dd, $mm, $yy, $week, $yday, $opt ) =
        localtime($lastts);
      my $t = sprintf( "%04d/%02d/%02d %02d:%02d:%02d",
        $yy + 1900, $mm + 1, $dd, $hh, $mm, $sec );
      my $msg =
          "Last failover was done at $t."
        . " Current time is too early to do failover again. If you want to "
        . "do failover, manually remove $_failover_complete_file "
        . "and run this script again.";
      $log->error($msg);
      croak;
    }
    else {
      MHA::NodeUtil::drop_file_if($_failover_complete_file);
    }
  }
  $_server_manager->get_failover_advisory_locks();
  $_server_manager->start_sql_threads_if();
  return $dead_master;
}

sub force_shutdown_internal($) {
  my $dead_master = shift;

  $log->info(
"Forcing shutdown so that applications never connect to the current master.."
  );

  if ( $dead_master->{master_ip_failover_script} ) {
    my $command =
"$dead_master->{master_ip_failover_script} --orig_master_host=$dead_master->{hostname} --orig_master_ip=$dead_master->{ip} --orig_master_port=$dead_master->{port}";
    if ( $_real_ssh_reachable == 1 ) {
      $command .=
        " --command=stopssh" . " --ssh_user=$dead_master->{ssh_user} ";
    }
    else {
      $command .= " --command=stop";
    }
    $command .=
      $dead_master->get_ssh_args_if( 1, "orig", $_real_ssh_reachable );
    $log->info("Executing master IP deactivation script:");
    $log->info("  $command");
    my ( $high, $low ) = MHA::ManagerUtil::exec_system( $command, $g_logfile );
    if ( $high == 0 && $low == 0 ) {
      $log->info(" done.");
      $mail_body .=
        "Invalidated master IP address on "
        . $dead_master->get_hostinfo() . "\n";
    }
    else {
      my $message =
        "Failed to deactivate master IP with return code $high:$low";
      $log->error($message);
      $mail_body .= $message . "\n";
      if ( $high == 10 ) {
        $log->warning("Proceeding.");
      }
      else {
        croak;
      }
    }
  }
  else {
    $log->warning(
"master_ip_failover_script is not set. Skipping invalidating dead master IP address."
    );
  }

  # force master shutdown
  if ( $dead_master->{shutdown_script} ) {
    my $command = "$dead_master->{shutdown_script}";
    if ( $_real_ssh_reachable == 1 ) {
      $command .=
        " --command=stopssh" . " --ssh_user=$dead_master->{ssh_user} ";
    }
    else {
      $command .= " --command=stop";
    }
    $command .=
" --host=$dead_master->{hostname}  --ip=$dead_master->{ip}  --port=$dead_master->{port} ";
    $command .= " --pid_file=$dead_master->{master_pid_file}"
      if ( $dead_master->{master_pid_file} );
    $command .=
      $dead_master->get_ssh_args_if( 1, "shutdown", $_real_ssh_reachable );
    $log->info("Executing SHUTDOWN script:");
    $log->info("  $command");
    my ( $high, $low ) = MHA::ManagerUtil::exec_system( $command, $g_logfile );
    if ( $high == 0 && $low == 0 ) {
      $log->info(" Power off done.");
      $mail_body .= "Power off $dead_master->{hostname}.\n";
      $_real_ssh_reachable = 0;
    }
    else {
      if ( $high == 10 ) {
        $log->info(" SSH reachable. Shutting down mysqld done.");
        $mail_body .=
"SSH reachable on $dead_master->{hostname}. Shutting down mysqld done.\n";
        $_real_ssh_reachable = 1;
      }
      else {
        my $message =
          "Failed to execute shutdown_script with return code $high:$low";
        $log->error($message);
        $mail_body .= $message . "\n";
        croak;
      }
    }
  }
  else {
    $log->warning(
"shutdown_script is not set. Skipping explicit shutting down of the dead master."
    );
  }
  return 0;
}

sub force_shutdown($) {
  my $dead_master = shift;

  my $appname      = $_status_handler->{basename};
  my @alive_slaves = $_server_manager->get_alive_slaves();
  $mail_subject =
    $appname . ": MySQL Master failover " . $dead_master->get_hostinfo();
  $mail_body = "Master " . $dead_master->get_hostinfo() . " is down!\n\n";

  $mail_body .= "Check MHA Manager logs at " . hostname();
  $mail_body .= ":$g_logfile" if ($g_logfile);
  $mail_body .= " for details.\n\n";
  if ($g_interactive) {
    $mail_body .= "Started manual(interactive) failover.\n";
  }
  else {
    $mail_body .= "Started automated(non-interactive) failover.\n";
  }

  # If any error happens after here, a special error file is created so that
  # it won't automatically repeat the same error.
  $_create_error_file = 1;

  my $slave_io_stopper = new Parallel::ForkManager( $#alive_slaves + 1 );
  my $stop_io_failed   = 0;
  $slave_io_stopper->run_on_start(
    sub {
      my ( $pid, $target ) = @_;
    }
  );
  $slave_io_stopper->run_on_finish(
    sub {
      my ( $pid, $exit_code, $target ) = @_;
      return if ( $target->{ignore_fail} );
      $stop_io_failed = 1 if ($exit_code);
    }
  );

  foreach my $target (@alive_slaves) {
    $slave_io_stopper->start($target) and next;
    eval {
      $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";
      my $rc = $target->stop_io_thread();
      $slave_io_stopper->finish($rc);
    };
    if ($@) {
      $log->error($@);
      undef $@;
      $slave_io_stopper->finish(1);
    }
    $slave_io_stopper->finish(0);
  }

  $_real_ssh_reachable = $g_ssh_reachable;

  # SSH reachability is unknown. Verify here.
  if ( $_real_ssh_reachable >= 2 ) {
    if (
      MHA::HealthCheck::ssh_check_simple(
        $dead_master->{ssh_user}, $dead_master->{ssh_host},
        $dead_master->{ssh_ip},   $dead_master->{ssh_port},
        $dead_master->{logger},   $dead_master->{ssh_connection_timeout}
      )
      )
    {
      $_real_ssh_reachable = 0;
    }
    else {

      # additional check
      if (
        MHA::ManagerUtil::get_node_version(
          $dead_master->{logger},   $dead_master->{ssh_user},
          $dead_master->{ssh_host}, $dead_master->{ssh_ip},
          $dead_master->{ssh_port}
        )
        )
      {
        $_real_ssh_reachable = 1;
      }
      else {
        $log->warning(
"Failed to get MHA Node version from dead master. Guessing that SSH is NOT reachable."
        );
        $_real_ssh_reachable = 0;
      }
    }
  }
  force_shutdown_internal($dead_master);

  $slave_io_stopper->wait_all_children;
  if ($stop_io_failed) {
    $log->error("Stopping IO thread failed! Check slave status!");
    $mail_body .= "Stopping IO thread failed.\n";
    croak;
  }
}

sub check_set_latest_slaves {
  $_server_manager->read_slave_status();
  $_server_manager->identify_latest_slaves();
  $log->info(
    "Latest slaves (Slaves that received relay log files to the latest):");
  $_server_manager->print_latest_slaves();
  $_server_manager->identify_oldest_slaves();
  $log->info("Oldest slaves:");
  $_server_manager->print_oldest_slaves();
}

sub save_from_binlog_server {
  my $relay_master_log_file = shift;
  my $exec_master_log_pos   = shift;
  my $binlog_server_ref     = shift;
  my @binlog_servers        = @$binlog_server_ref;
  my $max_saved_binlog_size = 0;
  my $failed_servers        = 0;

  my $pm = new Parallel::ForkManager( $#binlog_servers + 1 );
  $pm->run_on_start(
    sub {
      my ( $pid, $target ) = @_;
      $log->info(
        sprintf(
          "-- Saving binlog from host %s started, pid: %d",
          $target->{hostname}, $pid
        )
      );
    }
  );

  $pm->run_on_finish(
    sub {
      my ( $pid, $exit_code, $target ) = @_;
      $log->info();
      $log->info("Log messages from $target->{hostname} ...");
      my $saved_binlog =
"$g_workdir/saved_binlog_$target->{hostname}_$target->{id}_$_start_datetime.binlog";
      my $local_file =
"$g_workdir/saved_binlog_$target->{hostname}_$target->{id}_$_start_datetime.log";
      if ( -f $local_file ) {
        $log->info( "\n" . `cat $local_file` );
        unlink $local_file;
      }
      $log->info("End of log messages from $target->{hostname}.");
      if ( $exit_code == 0 ) {
        if ( -f $saved_binlog ) {
          my $saved_binlog_size = -s $saved_binlog;
          $log->info(
"Saved mysqlbinlog size from $target->{hostname} is $saved_binlog_size bytes."
          );
          if ( $saved_binlog_size > $max_saved_binlog_size ) {
            $_diff_binary_log      = $saved_binlog;
            $max_saved_binlog_size = $saved_binlog_size;
          }
        }
      }
      elsif ( $exit_code == 2 ) {
        $failed_servers++;
        $log->warning("SSH is not reachable on $target->{hostname}. Skipping");
      }
      elsif ( $exit_code == 10 ) {
        $log->info("No binlog events found from $target->{hostname}. Skipping");
      }
      else {
        $failed_servers++;
        $log->warning("Got error from $target->{hostname}.");
      }
    }
  );

  foreach my $target (@binlog_servers) {
    my $pid = $pm->start($target) and next;
    my $pplog;
    eval {
      $pm->finish(2) unless ( $target->{ssh_reachable} );
      $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";
      my $saved_binlog =
"$g_workdir/saved_binlog_$target->{hostname}_$target->{id}_$_start_datetime.binlog";
      my $saved_binlog_remote =
"$target->{remote_workdir}/saved_binlog_$target->{id}_$_start_datetime.binlog";
      my $local_file =
"$g_workdir/saved_binlog_$target->{hostname}_$target->{id}_$_start_datetime.log";
      if ( -f $local_file ) {
        unlink $local_file;
      }
      $pplog = Log::Dispatch->new( callbacks => $MHA::ManagerConst::log_fmt );
      $pplog->add(
        Log::Dispatch::File->new(
          name      => 'file',
          filename  => $local_file,
          min_level => $target->{log_level},
          callbacks => $MHA::ManagerConst::add_timestamp,
          mode      => 'append'
        )
      );
      $pplog->info(
        "Fetching binary logs from binlog server $target->{hostname}..");
      my $command =
"save_binary_logs --command=save --start_file=$relay_master_log_file  --start_pos=$exec_master_log_pos --output_file=$saved_binlog_remote --handle_raw_binlog=0 --skip_filter=1 --disable_log_bin=0 --manager_version=$MHA::ManagerConst::VERSION";
      if ( $target->{client_bindir} ) {
        $command .= " --client_bindir=$target->{client_bindir}";
      }
      if ( $target->{client_libdir} ) {
        $command .= " --client_libdir=$target->{client_libdir}";
      }
      my $oldest_version = $_server_manager->get_oldest_version();
      $command .= " --oldest_version=$oldest_version ";
      if ( $target->{log_level} eq "debug" ) {
        $command .= " --debug ";
      }
      $command .= " --binlog_dir=$target->{master_binlog_dir} ";
      $pplog->info("Executing binlog save command: $command");
      my $ssh_user_host = $target->{ssh_user} . '@' . $target->{ssh_ip};
      my ( $high, $low ) =
        MHA::ManagerUtil::exec_ssh_cmd( $ssh_user_host, $target->{ssh_port},
        $command, $local_file );
      if ( $high == 0 && $low == 0 ) {
        if (
          MHA::NodeUtil::file_copy(
            0,                   $saved_binlog,     $saved_binlog_remote,
            $target->{ssh_user}, $target->{ssh_ip}, $local_file,
            $target->{ssh_port}
          )
          )
        {
          $pplog->error(
"scp from $ssh_user_host:$saved_binlog_remote to local:$saved_binlog failed!"
          );
          croak;
        }
        else {
          $pplog->info(
"scp from $ssh_user_host:$saved_binlog_remote to local:$saved_binlog succeeded."
          );
          $pm->finish(0);
        }
      }
      elsif ( $high == 10 && $low == 0 ) {
        $pplog->info(
"Additional events were not found from the binlog server. No need to save."
        );
        $pm->finish(10);
      }
      else {
        $pplog->error(
"Failed to save binary log events from the binlog server. Maybe disks on binary logs are not accessible or binary log itself is corrupt?"
        );
      }
    };
    if ($@) {
      $pplog->error($@) if ($pplog);
      undef $@;
    }
    $pm->finish(1);
  }
  $pm->wait_all_children;

  if (!$g_ignore_binlog_server_error
    && $#binlog_servers >= 0
    && $#binlog_servers + 1 <= $failed_servers )
  {
    $log->error("All binlog servers failed!");
    croak;
  }
  if ($_diff_binary_log) {
    return 1;
  }
  else {
    return 0;
  }
}

sub save_master_binlog_internal {
  my $master_log_file     = shift;
  my $read_master_log_pos = shift;
  my $dead_master         = shift;

  $log->info("Fetching dead master's binary logs..");
  $_diff_binary_log_basename =
      "saved_master_binlog_from_"
    . $dead_master->{hostname} . "_"
    . $dead_master->{port} . "_"
    . $_start_datetime
    . $_saved_file_suffix;
  $_diff_binary_log = "$g_workdir/$_diff_binary_log_basename";
  my $_diff_binary_log_remote =
    "$dead_master->{remote_workdir}/$_diff_binary_log_basename";

  if ( -f $_diff_binary_log ) {
    unlink($_diff_binary_log);
  }
  my $command =
"save_binary_logs --command=save --start_file=$master_log_file  --start_pos=$read_master_log_pos --binlog_dir=$dead_master->{master_binlog_dir} --output_file=$_diff_binary_log_remote --handle_raw_binlog=$dead_master->{handle_raw_binlog} --disable_log_bin=$dead_master->{disable_log_bin} --manager_version=$MHA::ManagerConst::VERSION";
  if ( $dead_master->{client_bindir} ) {
    $command .= " --client_bindir=$dead_master->{client_bindir}";
  }
  if ( $dead_master->{client_libdir} ) {
    $command .= " --client_libdir=$dead_master->{client_libdir}";
  }
  unless ( $dead_master->{handle_raw_binlog} ) {
    my $oldest_version = $_server_manager->get_oldest_version();
    $command .= " --oldest_version=$oldest_version ";
  }
  if ( $dead_master->{log_level} eq "debug" ) {
    $command .= " --debug ";
  }
  my $ssh_user_host = $dead_master->{ssh_user} . '@' . $dead_master->{ssh_ip};
  $log->info(
    sprintf(
      "Executing command on the dead master %s: %s",
      $dead_master->get_hostinfo(), $command
    )
  );
  my ( $high, $low ) =
    MHA::ManagerUtil::exec_ssh_cmd( $ssh_user_host, $dead_master->{ssh_port},
    $command, $g_logfile );
  if ( $high == 0 && $low == 0 ) {
    if (
      MHA::NodeUtil::file_copy(
        0,                        $_diff_binary_log,
        $_diff_binary_log_remote, $dead_master->{ssh_user},
        $dead_master->{ssh_ip},   $g_logfile,
        $dead_master->{ssh_port}
      )
      )
    {
      $log->error(
"scp from $ssh_user_host:$_diff_binary_log_remote to local:$_diff_binary_log failed!"
      );
      croak;
    }
    else {
      $log->info(
"scp from $ssh_user_host:$_diff_binary_log_remote to local:$_diff_binary_log succeeded."
      );
      $_has_saved_binlog = 1;
    }
  }
  elsif ( $high == 10 && $low == 0 ) {
    $log->info(
      "Additional events were not found from the orig master. No need to save."
    );
  }
  else {
    $log->error(
"Failed to save binary log events from the orig master. Maybe disks on binary logs are not accessible or binary log itself is corrupt?"
    );
  }

  if ($_has_saved_binlog) {
    my @alive_slaves = $_server_manager->get_alive_slaves();
    foreach my $slave (@alive_slaves) {
      $slave->check_set_ssh_status( $log, 1 );
    }
  }
}

sub save_master_binlog {
  my $dead_master = shift;
  if ( $_real_ssh_reachable && !$g_skip_save_master_binlog ) {
    MHA::ManagerUtil::check_node_version(
      $log,
      $dead_master->{ssh_user},
      $dead_master->{ssh_host},
      $dead_master->{ssh_ip},
      $dead_master->{ssh_port}
    );
    my $latest_file =
      ( $_server_manager->get_latest_slaves() )[0]->{Master_Log_File};
    my $latest_pos =
      ( $_server_manager->get_latest_slaves() )[0]->{Read_Master_Log_Pos};
    save_master_binlog_internal( $latest_file, $latest_pos, $dead_master, );
  }
  else {
    if ($g_skip_save_master_binlog) {
      $log->info("Skipping trying to save dead master's binary log.");
    }
    elsif ( !$_real_ssh_reachable ) {
      $log->warning(
"Dead Master is not SSH reachable. Could not save it's binlogs. Transactions that were not sent to the latest slave (Read_Master_Log_Pos to the tail of the dead master's binlog) were lost."
      );
    }
  }
}

sub find_slave_with_all_relay_logs {
  my $oldest_master_log_file = shift;
  my $oldest_master_log_pos  = shift;
  my $skip_ssh_check         = shift;
  my @latest                 = $_server_manager->get_latest_slaves();
  foreach my $latest_slave (@latest) {
    if ( !$latest_slave->{dead} && !$skip_ssh_check ) {

      # Need to check ssh connectivity if it is not confirmed
      $latest_slave->check_set_ssh_status( $log, 0 )
        if ( $latest_slave->{ssh_ok} >= 2 );
      next if ( $latest_slave->{ssh_ok} == 0 );
    }

    my $ssh_user_host =
      $latest_slave->{ssh_user} . '@' . $latest_slave->{ssh_ip};
    my $command =
"apply_diff_relay_logs --command=find --latest_mlf=$latest_slave->{Master_Log_File} --latest_rmlp=$latest_slave->{Read_Master_Log_Pos} --target_mlf=$oldest_master_log_file --target_rmlp=$oldest_master_log_pos --server_id=$latest_slave->{server_id} --workdir=$latest_slave->{remote_workdir} --timestamp=$_start_datetime --manager_version=$MHA::ManagerConst::VERSION";
    if ( $latest_slave->{client_bindir} ) {
      $command .= " --client_bindir=$latest_slave->{client_bindir}";
    }
    if ( $latest_slave->{client_libdir} ) {
      $command .= " --client_libdir=$latest_slave->{client_libdir}";
    }
    if ( $latest_slave->{relay_log_info_type} eq "TABLE" ) {
      $command .=
" --relay_dir=$latest_slave->{relay_dir} --current_relay_log=$latest_slave->{current_relay_log} ";
    }
    else {
      $command .= " --relay_log_info=$latest_slave->{relay_log_info} ";
      $command .= " --relay_dir=$latest_slave->{datadir} ";
    }
    if ( $latest_slave->{log_level} eq "debug" ) {
      $command .= " --debug ";
    }
    $log->info(
"Checking whether $latest_slave->{hostname} has relay logs from the oldest position.."
    );
    if ($MHA::ManagerConst::USE_SSH_OPTIONS) {
      $command .= " --ssh_options='$MHA::NodeConst::SSH_OPT_ALIVE' ";
    }
    $log->info("Executing command: $command :");
    my ( $high, $low ) =
      MHA::ManagerUtil::exec_ssh_cmd( $ssh_user_host, $latest_slave->{ssh_port},
      $command, $g_logfile );
    if ( $high eq '0' && $low eq '0' ) {
      $log->info("OK. $latest_slave->{hostname} has all relay logs.");
      return $latest_slave;
    }
    else {
      $log->warning(
"$latest_slave->{hostname} doesn't have all relay logs. Maybe some logs were purged."
      );
    }
  }
}

sub find_latest_base_slave_internal {
  my $oldest_slave  = ( $_server_manager->get_oldest_slaves() )[0];
  my @latest_slaves = $_server_manager->get_latest_slaves();
  my $oldest_mlf    = $oldest_slave->{Master_Log_File};
  my $oldest_mlp    = $oldest_slave->{Read_Master_Log_Pos};
  my $latest_mlf    = $latest_slaves[0]->{Master_Log_File};
  my $latest_mlp    = $latest_slaves[0]->{Read_Master_Log_Pos};

  if (
    $_server_manager->pos_cmp( $oldest_mlf, $oldest_mlp, $latest_mlf,
      $latest_mlp ) >= 0
    )
  {
    $log->info(
"All slaves received relay logs to the same position. No need to resync each other."
    );
    return $latest_slaves[0];
  }
  else {

# We pick relay logs here. This should not reconfigure slave settings until other slaves connect to new master
    my $target =
      find_slave_with_all_relay_logs( $oldest_slave->{Master_Log_File},
      $oldest_slave->{Read_Master_Log_Pos} );
    unless ($target) {
      $log->warning(
"None of latest servers have enough relay logs from oldest position. We can't recover oldest slaves."
      );
      my @oldest_slaves = $_server_manager->get_oldest_slaves();
      foreach (@oldest_slaves) {
        $_->{lack_relay_log} = 1;
        $_->{no_master}      = 1;
      }
      my ( $oldest_limit_mlf, $oldest_limit_mlp ) =
        $_server_manager->get_oldest_limit_pos();
      unless ($oldest_limit_mlf) {
        $log->warning(
          "All slave servers set ignore_fail parameters. Continuing failover.");
        $target = $latest_slaves[0];
      }
      elsif (
        $_server_manager->pos_cmp(
          $oldest_slave->{Master_Log_File},
          $oldest_slave->{Read_Master_Log_Pos},
          $oldest_limit_mlf,
          $oldest_limit_mlp
        ) >= 0
        )
      {

        # None of the slave sets ignore_fail parameters. Can't continue failover
        return;
      }
      else {
        $log->warning(
          sprintf(
"The oldest master position from non-ignore_fail slaves is %s:%d. Checking whether latest slave's relay logs from this position.",
            $oldest_limit_mlf, $oldest_limit_mlp
          )
        );
        if (
          $_server_manager->pos_cmp( $oldest_limit_mlf, $oldest_limit_mlp,
            $latest_mlf, $latest_mlp ) >= 0
          )
        {
          $log->info(
"The oldest master position from non-ignore_fail slaves is equal to the latest slave. Can continue failover."
          );
          return $latest_slaves[0];
        }
        $target =
          find_slave_with_all_relay_logs( $oldest_limit_mlf, $oldest_limit_mlp,
          1 );
        $_server_manager->set_no_master_if_older( $oldest_limit_mlf,
          $oldest_limit_mlp );
      }
    }
    return $target;
  }
}

sub find_latest_base_slave($) {
  my $dead_master = shift;
  $log->info(
"Finding the latest slave that has all relay logs for recovering other slaves.."
  );
  my $latest_base_slave = find_latest_base_slave_internal();
  unless ($latest_base_slave) {
    my $msg = "None of the latest slaves has enough relay logs for recovery.";
    $log->error($msg);
    $mail_body .= $msg . "\n";
    croak;
  }
  $mail_body .=
      "The latest slave "
    . $latest_base_slave->get_hostinfo()
    . " has all relay logs for recovery.\n";

  reconf_alive_servers($dead_master);
  return $latest_base_slave;
}

sub select_new_master($$) {
  my $dead_master       = shift;
  my $latest_base_slave = shift;

  my $new_master =
    $_server_manager->select_new_master( $g_new_master_host, $g_new_master_port,
    $latest_base_slave->{check_repl_delay} );
  unless ($new_master) {
    my $msg =
"None of existing slaves matches as a new master. Maybe preferred node is misconfigured or all slaves are too  far behind.";
    $log->error($msg);
    $mail_body .= $msg . "\n";
    croak;
  }
  $log->info( "New master is " . $new_master->get_hostinfo() );
  $mail_body .=
    "Selected " . $new_master->get_hostinfo() . " as a new master.\n";
  $log->info("Starting master failover..");
  $_server_manager->print_servers_migration_ascii( $dead_master, $new_master );
  if ($g_interactive) {
    $new_master =
      $_server_manager->manually_decide_new_master( $dead_master, $new_master );
    $log->info(
      "New master decided manually is " . $new_master->get_hostinfo() );
  }
  return $new_master;
}

sub send_binlog {
  my ( $target, $logger ) = @_;
  $logger = $log unless ($logger);
  if ($_has_saved_binlog) {
    $logger->info("Sending binlog..");
    my $_diff_binary_log_remote =
      "$target->{remote_workdir}/$_diff_binary_log_basename";
    if (
      MHA::NodeUtil::file_copy(
        1,                        $_diff_binary_log,
        $_diff_binary_log_remote, $target->{ssh_user},
        $target->{ssh_ip},        $g_logfile,
        $target->{ssh_port}
      )
      )
    {
      $logger->error(
            "scp from local:$_diff_binary_log to $target->{ssh_user}" . '@'
          . $target->{hostname}
          . "$_diff_binary_log_remote failed." );
      return 1;
    }
    else {
      $logger->info(
            "scp from local:$_diff_binary_log to $target->{ssh_user}" . '@'
          . $target->{hostname}
          . ":$_diff_binary_log_remote succeeded." );
    }
  }
  return 0;
}

sub generate_diff_from_readpos {
  my ( $target, $latest_slave, $logger ) = @_;
  $logger = $log unless ($logger);

  $logger->info(
"Server $target->{hostname} received relay logs up to: $target->{Master_Log_File}:$target->{Read_Master_Log_Pos}"
  );
  $logger->info(
"Need to get diffs from the latest slave($latest_slave->{hostname}) up to: $latest_slave->{Master_Log_File}:$latest_slave->{Read_Master_Log_Pos} (using the latest slave's relay logs)"
  );

  my $ssh_user_host = $latest_slave->{ssh_user} . '@' . $latest_slave->{ssh_ip};
  my ( $high, $low ) =
    MHA::ManagerUtil::exec_ssh_cmd( $ssh_user_host, $latest_slave->{ssh_port},
    "exit 0", $g_logfile );
  if ( $high ne '0' || $low ne '0' ) {
    $logger->error("SSH authentication test failed. user=$ssh_user_host");
    return ( $high, $low );
  }

  $logger->info(
"Connecting to the latest slave host $latest_slave->{hostname}, generating diff relay log files.."
  );
  my $command =
"apply_diff_relay_logs --command=generate_and_send --scp_user=$target->{ssh_user} --scp_host=$target->{ssh_ip} --latest_mlf=$latest_slave->{Master_Log_File} --latest_rmlp=$latest_slave->{Read_Master_Log_Pos} --target_mlf=$target->{Master_Log_File} --target_rmlp=$target->{Read_Master_Log_Pos} --server_id=$latest_slave->{server_id} --diff_file_readtolatest=$target->{diff_file_readtolatest} --workdir=$latest_slave->{remote_workdir} --timestamp=$_start_datetime --handle_raw_binlog=$target->{handle_raw_binlog} --disable_log_bin=$target->{disable_log_bin} --manager_version=$MHA::ManagerConst::VERSION";
  if ( $latest_slave->{client_bindir} ) {
    $command .= " --client_bindir=$latest_slave->{client_bindir}";
  }
  if ( $latest_slave->{client_libdir} ) {
    $command .= " --client_libdir=$latest_slave->{client_libdir}";
  }

  if ( $target->{ssh_port} ne 22 ) {
    $command .= " --scp_port=$target->{ssh_port}";
  }

  if ( $latest_slave->{relay_log_info_type} eq "TABLE" ) {
    $command .=
" --relay_dir=$latest_slave->{relay_dir} --current_relay_log=$latest_slave->{current_relay_log} ";
  }
  else {
    $command .= " --relay_log_info=$latest_slave->{relay_log_info} ";
    $command .= " --relay_dir=$latest_slave->{datadir} ";
  }
  unless ( $target->{handle_raw_binlog} ) {
    $command .= " --target_version=$target->{mysql_version} ";
  }
  if ( $target->{log_level} eq "debug" ) {
    $command .= " --debug ";
  }
  if ($MHA::ManagerConst::USE_SSH_OPTIONS) {
    $command .= " --ssh_options='$MHA::NodeConst::SSH_OPT_ALIVE' ";
  }
  $logger->info("Executing command: $command");
  return exec_ssh_child_cmd( $ssh_user_host, $latest_slave->{ssh_port},
    $command, $logger,
    "$g_workdir/$latest_slave->{hostname}_$latest_slave->{port}.work" );
}

# 0: no need to generate diff
# 15: generating diff succeeded
# 1: fail
sub recover_relay_logs {
  my ( $target, $latest_slave, $logger ) = @_;
  $logger = $log unless ($logger);
  if ( $target->{latest} eq '0' ) {
    my ( $high, $low ) =
      generate_diff_from_readpos( $target, $latest_slave, $logger );
    if ( $high ne '0' || $low ne '0' ) {
      $logger->error(
        " Generating diff files failed with return code $high:$low.");
      return 1;
    }
    $logger->info(" Generating diff files succeeded.");
    return $GEN_DIFF_OK;
  }
  else {
    $logger->info(
" This server has all relay logs. No need to generate diff files from the latest slave."
    );
    return 0;
  }
}

sub recover_all_slaves_relay_logs {
  my $new_master        = shift;
  my $latest_base_slave = shift;
  my @alive_slaves      = $_server_manager->get_alive_slaves();

  $log->info(
    "* Phase 4.1: Starting Parallel Slave Diff Log Generation Phase..\n");
  $log->info();
  my $pm            = new Parallel::ForkManager( $#alive_slaves + 1 );
  my $diff_log_fail = 0;

  $pm->run_on_start(
    sub {
      my ( $pid, $target ) = @_;
      $log->info(
        sprintf(
"-- Slave diff file generation on host %s started, pid: %d. Check tmp log $g_workdir/$target->{hostname}_$target->{port}_$_start_datetime.log if it takes time..",
          $target->get_hostinfo(), $pid
        )
      );
    }
  );

  $pm->run_on_finish(
    sub {
      my ( $pid, $exit_code, $target ) = @_;
      $log->info();
      $log->info("Log messages from $target->{hostname} ...");
      my $local_file =
        "$g_workdir/$target->{hostname}_$target->{port}_$_start_datetime.log";
      $log->info( "\n" . `cat $local_file` ) if ( -f $local_file );
      $log->info("End of log messages from $target->{hostname}.");
      unlink $local_file if ( -f $local_file );

      if ( $exit_code == 0 ) {
        $target->{gen_diff_ok} = 1;
        $log->info(
          sprintf( "-- %s has the latest relay log events.",
            $target->get_hostinfo() )
        );
        $mail_body .=
          $target->get_hostinfo()
          . ": This host has the latest relay log events.\n";
      }
      elsif ( $exit_code == $GEN_DIFF_OK ) {
        $target->{gen_diff_ok} = 1;
        $log->info(
          sprintf( "-- Slave diff log generation on host %s succeeded.",
            $target->get_hostinfo() )
        );
        $mail_body .=
            $target->get_hostinfo()
          . ": Generating differential relay logs up to "
          . $latest_base_slave->get_hostinfo()
          . "succeeded.\n";
      }
      else {
        $diff_log_fail = 1;
        $log->info(
          sprintf(
            "-- Slave diff log generation on host %s failed, exit code %d",
            $target->get_hostinfo(), $exit_code
          )
        );
        $mail_body .=
            $target->get_hostinfo()
          . ": Generating differential relay logs up to "
          . $latest_base_slave->get_hostinfo()
          . " failed.\n";
      }
    }
  );

  foreach my $target (@alive_slaves) {

    # master was already recovered
    next if ( $target->{id} eq $new_master->{id} );

    my $pid = $pm->start($target) and next;
    my $pplog;
    eval {
      $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";
      $pm->finish(2) if ( $target->{lack_relay_log} );
      my $local_file =
        "$g_workdir/$target->{hostname}_$target->{port}_$_start_datetime.log";
      unlink $local_file;
      $pplog = Log::Dispatch->new( callbacks => $MHA::ManagerConst::log_fmt );
      $pplog->add(
        Log::Dispatch::File->new(
          name      => 'file',
          filename  => $local_file,
          min_level => $target->{log_level},
          callbacks => $MHA::ManagerConst::add_timestamp,
          mode      => 'append'
        )
      );
      $target->current_slave_position();
      my $rc = recover_relay_logs( $target, $latest_base_slave, $pplog );
      $pm->finish(0) if ( $rc == 0 );
      $pm->finish($GEN_DIFF_OK) if ( $rc == $GEN_DIFF_OK );
      $pm->finish(1);
    };
    if ($@) {
      $pplog->error($@) if ($pplog);
      undef $@;
      $pm->finish(1);
    }
  }

  $pm->wait_all_children;

  return 1 if ($diff_log_fail);
  return 0;
}

sub gen_diff_from_exec_to_read {
  my ( $target, $logger ) = @_;
  $logger = $log unless ($logger);
  my $ret;

  if ( ( $target->{Master_Log_File} eq $target->{Relay_Master_Log_File} )
    && ( $target->{Read_Master_Log_Pos} == $target->{Exec_Master_Log_Pos} ) )
  {
    $logger->info(
      sprintf(
"This slave(%s)'s Exec_Master_Log_Pos equals to Read_Master_Log_Pos(%s:%d). No need to recover from Exec_Master_Log_Pos.",
        $target->{hostname}, $target->{Master_Log_File},
        $target->{Read_Master_Log_Pos}
      )
    );
    return 0;
  }
  else {
    $logger->info(
      sprintf(
"This slave(%s)'s Exec_Master_Log_Pos(%s:%d) does not equal to Read_Master_Log_Pos(%s:%d). It is likely that relay log was cut during transaction. Need to recover from Exec_Master_Log_Pos.",
        $target->{hostname},            $target->{Relay_Master_Log_File},
        $target->{Exec_Master_Log_Pos}, $target->{Master_Log_File},
        $target->{Read_Master_Log_Pos}
      )
    );
    $logger->info(
"Saving local relay logs from exec pos to read pos on $target->{hostname}: from $target->{Relay_Log_File}:$target->{Relay_Log_Pos} to the end of the relay log.."
    );
    $target->{relay_from_exectoread} =
        "$target->{remote_workdir}/relay_from_exec_to_read_"
      . $target->{hostname} . "_"
      . $target->{port} . "_"
      . $_start_datetime
      . $_saved_file_suffix;

    my $command =
"save_binary_logs --command=save --start_file=$target->{Relay_Log_File}  --start_pos=$target->{Relay_Log_Pos} --output_file=$target->{relay_from_exectoread} --handle_raw_binlog=$target->{handle_raw_binlog} --disable_log_bin=$target->{disable_log_bin} --manager_version=$MHA::ManagerConst::VERSION";
    if ( $target->{client_bindir} ) {
      $command .= " --client_bindir=$target->{client_bindir}";
    }
    if ( $target->{client_libdir} ) {
      $command .= " --client_libdir=$target->{client_libdir}";
    }
    if ( $target->{relay_log_info_type} eq "TABLE" ) {
      $command .= " --binlog_dir=$target->{relay_dir} ";
    }
    else {
      $command .= " --relay_log_info=$target->{relay_log_info} ";
      $command .= " --binlog_dir=$target->{datadir} ";
    }
    unless ( $target->{handle_raw_binlog} ) {
      $command .= " --oldest_version=$target->{mysql_version} ";
    }
    if ( $target->{log_level} eq "debug" ) {
      $command .= " --debug ";
    }
    $logger->info("Executing command : $command");
    my $ssh_user_host = $target->{ssh_user} . '@' . $target->{ssh_ip};
    $target->check_set_ssh_status( $logger, 1 ) if ( $target->{ssh_ok} >= 2 );
    if ( $target->{ssh_ok} == 0 ) {
      $logger->error("Failed to connect via SSH!");
      return 1;
    }
    my ( $high, $low ) =
      exec_ssh_child_cmd( $ssh_user_host, $target->{ssh_port}, $command,
      $logger, "$g_workdir/$target->{hostname}_$target->{port}.work" );
    if ( $high eq '0' && $low eq '0' ) {
      return 0;
    }
    else {
      $logger->error("Saving relay logs failed!");
      return 1;
    }
  }
  return 0;
}

sub apply_diff {
  my ( $target, $logger ) = @_;
  $logger = $log unless ($logger);
  my $ret = 0;

  $logger->info("Waiting until all relay logs are applied.");
  $ret = $target->wait_until_relay_log_applied();
  if ($ret) {
    $logger->error("Applying existing relay logs failed!");
    return $ret;
  }
  $logger->info(" done.");
  $target->stop_sql_thread($logger);
  $logger->info("Getting slave status..");
  my %status = $target->check_slave_status();
  if ( $status{Status} eq '0' ) {
    $target->{Relay_Master_Log_File} = $status{Relay_Master_Log_File};
    $target->{Exec_Master_Log_Pos}   = $status{Exec_Master_Log_Pos};
    $target->{Relay_Log_File}        = $status{Relay_Log_File};
    $target->{Relay_Log_Pos}         = $status{Relay_Log_Pos};
  }
  else {
    $logger->error("Getting slave status failed");
    return $status{Status};
  }

  # generate from exec pos to my latest pos
  $ret = gen_diff_from_exec_to_read( $target, $logger );
  return ( $ret, 0 ) if ($ret);

  # if exec pos != read pos (relay logs cut in transactions)
  my $exec_diff = $target->{relay_from_exectoread};

  # if read pos != latest (io thread)
  my $read_diff = $target->{diff_file_readtolatest};

  # if binlogs are rescued from master
  my $binlog_diff;
  if ($_diff_binary_log_basename) {
    $binlog_diff = "$target->{remote_workdir}/$_diff_binary_log_basename";
  }
  my @diffs;
  push @diffs, $exec_diff   if ($exec_diff);
  push @diffs, $read_diff   if ( $read_diff && !$target->{latest} );
  push @diffs, $binlog_diff if ( $binlog_diff && $_has_saved_binlog );
  my $diff_files = join( ",", @diffs );

  $target->get_and_set_high_max_allowed_packet($logger);

  my $ssh_user_host = $target->{ssh_user} . '@' . $target->{ssh_ip};
  $logger->info(
"Connecting to the target slave host $target->{hostname}, running recover script.."
  );
  my $command =
"apply_diff_relay_logs --command=apply --slave_user=$target->{escaped_user} --slave_host=$target->{hostname} --slave_ip=$target->{ip}  --slave_port=$target->{port} --apply_files=$diff_files --workdir=$target->{remote_workdir} --target_version=$target->{mysql_version} --timestamp=$_start_datetime --handle_raw_binlog=$target->{handle_raw_binlog} --disable_log_bin=$target->{disable_log_bin} --manager_version=$MHA::ManagerConst::VERSION";
  if ( $target->{client_bindir} ) {
    $command .= " --client_bindir=$target->{client_bindir}";
  }
  if ( $target->{client_libdir} ) {
    $command .= " --client_libdir=$target->{client_libdir}";
  }
  if ( $target->{log_level} eq "debug" ) {
    $command .= " --debug ";
  }
  if ($MHA::ManagerConst::USE_SSH_OPTIONS) {
    $command .= " --ssh_options='$MHA::NodeConst::SSH_OPT_ALIVE' ";
  }
  $logger->info("Executing command: $command --slave_pass=xxx");
  if ( $target->{escaped_password} ne "" ) {
    $command .= " --slave_pass=$target->{escaped_password} ";
  }
  $target->check_set_ssh_status( $logger, 1 ) if ( $target->{ssh_ok} >= 2 );
  if ( $target->{ssh_ok} == 0 ) {
    $logger->error("Failed to connect via SSH!");
    return ( 1, 0 );
  }
  my ( $high, $low ) =
    exec_ssh_child_cmd( $ssh_user_host, $target->{ssh_port}, $command, $logger,
    "$g_workdir/$target->{hostname}_$target->{port}.work" );

  $target->set_default_max_allowed_packet($logger);
  return ( $high, $low );
}

# apply diffs to master and get master status
# We do not reset slave here
sub recover_slave {
  my ( $target, $logger ) = @_;
  $logger = $log unless ($logger);

  $logger->info(
    sprintf(
      "Starting recovery on %s(%s:%d)..",
      $target->{hostname}, $target->{ip}, $target->{port}
    )
  );

  if ( $target->{latest} eq '0' || $_has_saved_binlog ) {
    $logger->info(" Generating diffs succeeded.");
    my ( $high, $low ) = apply_diff( $target, $logger );
    if ( $high ne '0' || $low ne '0' ) {
      $logger->error(" Applying diffs failed with return code $high:$low.");
      return -1;
    }
  }
  else {
    $logger->info(
      " This server has all relay logs. Waiting all logs to be applied.. ");
    my $ret = $target->wait_until_relay_log_applied($logger);
    if ($ret) {
      $logger->error(" Failed with return code $ret");
      return -1;
    }
    $logger->info("  done.");
    $target->stop_sql_thread($logger);
  }
  $logger->info(" All relay logs were successfully applied.");
  return 0;
}

sub apply_binlog_to_master($) {
  my $target   = shift;
  my $err_file = "$g_workdir/mysql_from_binlog.err";
  my $command =
"cat $_diff_binary_log | mysql --binary-mode --user=$target->{mysql_escaped_user} --password=$target->{mysql_escaped_password} --host=$target->{ip} --port=$target->{port} -vvv --unbuffered > $err_file 2>&1";
  $log->info("Applying differential binlog $_diff_binary_log ..");
  if ( my $rc = system($command) ) {
    my ( $high, $low ) = MHA::NodeUtil::system_rc($rc);
    $log->error("FATAL: applying log files failed with rc $high:$low!");
    $log->error(
      sprintf(
        "Error logs from %s:%s (the last 200 lines)..",
        $target->{hostname}, $err_file
      )
    );
    $log->error(`tail -200 $err_file`);
    croak;
  }
  else {
    $log->info("Differential log apply from binlog server succeeded.");
  }
  return 0;
}

sub recover_master_gtid_internal($$$) {
  my $target            = shift;
  my $latest_slave      = shift;
  my $binlog_server_ref = shift;
  my $relay_master_log_file;
  my $exec_master_log_pos;
  $log->info();
  $log->info("* Phase 3.3: New Master Recovery Phase..\n");
  $log->info();
  $log->info(" Waiting all logs to be applied.. ");
  my $ret = $target->wait_until_relay_log_applied($log);

  if ($ret) {
    $log->error(" Failed with return code $ret");
    return -1;
  }
  $log->info("  done.");
  $target->stop_slave($log);
  if ( $target->{id} ne $latest_slave->{id} ) {
    $log->info(
      sprintf( " Replicating from the latest slave %s and waiting to apply..",
        $latest_slave->get_hostinfo() )
    );
    $log->info(" Waiting all logs to be applied on the latest slave.. ");
    $ret = $latest_slave->wait_until_relay_log_applied($log);
    if ($ret) {
      $log->error(" Failed with return code $ret");
      return -1;
    }
    $latest_slave->current_slave_position();
    $relay_master_log_file = $latest_slave->{Relay_Master_Log_File};
    $exec_master_log_pos   = $latest_slave->{Exec_Master_Log_Pos};
    $ret =
      $_server_manager->change_master_and_start_slave( $target, $latest_slave,
      undef, undef, $log );
    if ($ret) {
      $log->error(" Failed with return code $ret");
      return -1;
    }
    $ret = $_server_manager->wait_until_in_sync( $target, $latest_slave );
    if ($ret) {
      $log->error(" Failed with return code $ret");
      return -1;
    }
    $log->info("  done.");
  }
  else {
    $target->current_slave_position();
    $relay_master_log_file = $target->{Relay_Master_Log_File};
    $exec_master_log_pos   = $target->{Exec_Master_Log_Pos};
  }
  if (
    save_from_binlog_server(
      $relay_master_log_file, $exec_master_log_pos, $binlog_server_ref
    )
    )
  {
    apply_binlog_to_master($target);
  }
  return $_server_manager->get_new_master_binlog_position($target);
}

sub recover_master_internal($$) {
  my $target       = shift;
  my $latest_slave = shift;
  $log->info();
  $log->info("* Phase 3.3: New Master Diff Log Generation Phase..\n");
  $log->info();
  my $rc = recover_relay_logs( $target, $latest_slave );
  if ( $rc && $rc != $GEN_DIFF_OK ) {
    return;
  }
  if ( send_binlog($target) ) {
    return;
  }
  $log->info();
  $log->info("* Phase 3.4: Master Log Apply Phase..\n");
  $log->info();
  $log->info(
    "*NOTICE: If any error happens from this phase, manual recovery is needed."
  );
  if ( recover_slave($target) ) {
    return;
  }
  return $_server_manager->get_new_master_binlog_position($target);
}

sub recover_master($$$$) {
  my $dead_master       = shift;
  my $new_master        = shift;
  my $latest_base_slave = shift;
  my $binlog_server_ref = shift;

  my ( $master_log_file, $master_log_pos, $exec_gtid_set );
  if ( $_server_manager->is_gtid_auto_pos_enabled() ) {
    ( $master_log_file, $master_log_pos, $exec_gtid_set ) =
      recover_master_gtid_internal( $new_master, $latest_base_slave,
      $binlog_server_ref );
    if ( !$exec_gtid_set ) {
      my $msg = "Recovering master server failed.";
      $log->error($msg);
      $mail_body .= $msg . "\n";
      croak;
    }
    $log->info(
      sprintf(
        "Master Recovery succeeded. File:Pos:Exec_Gtid_Set: %s, %d, %s",
        $master_log_file, $master_log_pos, $exec_gtid_set
      )
    );
  }
  else {
    ( $master_log_file, $master_log_pos ) =
      recover_master_internal( $new_master, $latest_base_slave );
    if ( !$master_log_file or !defined($master_log_pos) ) {
      my $msg = "Recovering master server failed.";

      # generating diff file failed: try to use other latest server
      # recoverable error on master: try to recover other master
      # unrecoverable error on master: destroying the master
      $log->error($msg);
      $mail_body .= $msg . "\n";
      croak;
    }
  }
  $mail_body .=
    $new_master->get_hostinfo() . ": OK: Applying all logs succeeded.\n";

  if ( $new_master->{master_ip_failover_script} ) {
    my $command =
"$new_master->{master_ip_failover_script} --command=start --ssh_user=$new_master->{ssh_user} --orig_master_host=$dead_master->{hostname} --orig_master_ip=$dead_master->{ip} --orig_master_port=$dead_master->{port} --new_master_host=$new_master->{hostname} --new_master_ip=$new_master->{ip} --new_master_port=$new_master->{port} --new_master_user=$new_master->{escaped_user}";
    $command .=
      $dead_master->get_ssh_args_if( 1, "orig", $_real_ssh_reachable );
    $command .= $new_master->get_ssh_args_if( 2, "new", 1 );
    $log->info("Executing master IP activate script:");
    $log->info("  $command --new_master_password=xxx");
    $command .= " --new_master_password=$new_master->{escaped_password}";
    my ( $high, $low ) = MHA::ManagerUtil::exec_system( $command, $g_logfile );
    if ( $high == 0 && $low == 0 ) {
      $log->info(" OK.");
      $mail_body .=
        $new_master->get_hostinfo() . ": OK: Activated master IP address.\n";
    }
    else {
      my $message =
          "Failed to activate master IP address for "
        . $new_master->get_hostinfo()
        . " with return code $high:$low";
      $log->error( " " . $message );
      $mail_body .= $message . "\n";
      if ( $high == 10 ) {
        $log->warning("Proceeding.");
      }
      else {
        croak;
      }
    }
  }
  else {
    $log->warning(
"master_ip_failover_script is not set. Skipping taking over new master IP address."
    );
  }

  # Allow write access on master (if read_only==1)
  unless ($g_skip_disable_read_only) {
    $new_master->disable_read_only();
  }

  $log->info("** Finished master recovery successfully.");
  $mail_subject .= " to " . $new_master->get_hostinfo();
  return ( $master_log_file, $master_log_pos, $exec_gtid_set );
}

sub recover_slaves_gtid_internal {
  my $new_master    = shift;
  my $exec_gtid_set = shift;
  my @alive_slaves  = $_server_manager->get_alive_slaves();
  $log->info();
  $log->info("* Phase 4.1: Starting Slaves in parallel..\n");
  $log->info();
  my $pm                  = new Parallel::ForkManager( $#alive_slaves + 1 );
  my $slave_starting_fail = 0;
  $pm->run_on_start(
    sub {
      my ( $pid, $target ) = @_;
      $log->info(
        sprintf(
"-- Slave recovery on host %s started, pid: %d. Check tmp log $g_workdir/$target->{hostname}_$target->{port}_$_start_datetime.log if it takes time..",
          $target->get_hostinfo(), $pid
        )
      );
    }
  );
  $pm->run_on_finish(
    sub {
      my ( $pid, $exit_code, $target ) = @_;
      $log->info();
      $log->info("Log messages from $target->{hostname} ...");
      my $local_file =
        "$g_workdir/$target->{hostname}_$target->{port}_$_start_datetime.log";
      $log->info( "\n" . `cat $local_file` );
      $log->info("End of log messages from $target->{hostname}.");
      unlink $local_file;

      if ( $exit_code == 0 ) {
        $target->{recover_ok} = 1;
        $log->info(
          sprintf( "-- Slave on host %s started.", $target->get_hostinfo() ) );
        $mail_body .=
            $target->get_hostinfo()
          . ": OK: Slave started, replicating from "
          . $new_master->get_hostinfo() . "\n";
      }
      elsif ( $exit_code == 100 ) {
        $slave_starting_fail = 1;
        $mail_body .=
          $target->get_hostinfo() . ": ERROR: Starting slave failed.\n";
      }
      elsif ( $exit_code == 120 ) {
        $slave_starting_fail = 1;
        $mail_body .=
          $target->get_hostinfo()
          . ": ERROR: Failed on waiting gtid exec set on master.\n";
      }
      else {
        $slave_starting_fail = 1;
        $log->info(
          sprintf(
            "-- Recovery on host %s failed, exit code %d",
            $target->get_hostinfo(), $exit_code
          )
        );
        $mail_body .=
          $target->get_hostinfo() . ": ERROR: Starting slave failed.\n";
      }
    }
  );

  foreach my $target (@alive_slaves) {

    # master was already recovered
    next if ( $target->{id} eq $new_master->{id} );

    my $pid = $pm->start($target) and next;

    my $pplog;
    eval {
      $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";
      my $local_file =
        "$g_workdir/$target->{hostname}_$target->{port}_$_start_datetime.log";
      unlink $local_file;
      $pplog = Log::Dispatch->new( callbacks => $MHA::ManagerConst::log_fmt );
      $pplog->add(
        Log::Dispatch::File->new(
          name      => 'file',
          filename  => $local_file,
          min_level => $target->{log_level},
          callbacks => $MHA::ManagerConst::add_timestamp,
          mode      => 'append'
        )
      );
      if (
        $_server_manager->change_master_and_start_slave(
          $target, $new_master, undef, undef, $pplog
        )
        )
      {
        $pm->finish(100);
      }
      if ( $g_wait_until_gtid_in_sync
        && $target->gtid_wait( $exec_gtid_set, $pplog ) )
      {
        $pm->finish(120);
      }
      else {
        $pm->finish(0);
      }
    };
    if ($@) {
      $pplog->error($@) if ($pplog);
      undef $@;
      $pm->finish(1);
    }
  }

  $pm->wait_all_children;

  return ($slave_starting_fail);
}

sub recover_slaves_internal {
  my $new_master        = shift;
  my $master_log_file   = shift;
  my $master_log_pos    = shift;
  my $latest_base_slave = shift;
  my @alive_slaves      = $_server_manager->get_alive_slaves();

  $log->info();
  $log->info("* Phase 4.2: Starting Parallel Slave Log Apply Phase..\n");
  $log->info();

# Recover other slaves
# start slave if needed
# Concurrency should be the number of alive slave servers -1. Sometimes the new master and the latest slave becomes the same machine.
  my $pm                  = new Parallel::ForkManager( $#alive_slaves + 1 );
  my $skipping            = 0;
  my $copy_fail           = 0;
  my $recover_fail        = 0;
  my $slave_starting_fail = 0;

  $pm->run_on_start(
    sub {
      my ( $pid, $target ) = @_;
      $log->info(
        sprintf(
"-- Slave recovery on host %s started, pid: %d. Check tmp log $g_workdir/$target->{hostname}_$target->{port}_$_start_datetime.log if it takes time..",
          $target->get_hostinfo(), $pid
        )
      );
    }
  );

  $pm->run_on_finish(
    sub {
      my ( $pid, $exit_code, $target ) = @_;
      $log->info();
      $log->info("Log messages from $target->{hostname} ...");
      my $local_file =
        "$g_workdir/$target->{hostname}_$target->{port}_$_start_datetime.log";
      $log->info( "\n" . `cat $local_file` );
      $log->info("End of log messages from $target->{hostname}.");
      unlink $local_file;

      if ( $exit_code == 0 ) {
        $target->{recover_ok} = 1;
        $log->info(
          sprintf( "-- Slave recovery on host %s succeeded.",
            $target->get_hostinfo() )
        );
        $mail_body .=
            $target->get_hostinfo()
          . ": OK: Applying all logs succeeded. Slave started, replicating from "
          . $new_master->get_hostinfo() . "\n";
      }
      elsif ( $exit_code == 10 ) {
        $target->{recover_ok} = 1;
        $log->info(
          sprintf( "-- Slave recovery on host %s succeeded.",
            $target->get_hostinfo() )
        );
        $mail_body .=
          $target->get_hostinfo() . ": OK: Applying all logs succeeded.\n";
      }
      elsif ( $exit_code == 20 ) {
        $skipping = 1;
        $log->info(
          sprintf(
"-- Skipping recovering slave %s because diff log generation failed.",
            $target->get_hostinfo() )
        );
        $mail_body .=
          $target->get_hostinfo()
          . ": ERROR: Skipping applying logs because diff log generation failed.\n";
      }
      elsif ( $exit_code == 30 ) {
        $copy_fail = 1;
        $log->info(
          sprintf( "-- Copying master binlog to host %s failed.",
            $target->get_hostinfo() )
        );
        $mail_body .=
          $target->get_hostinfo()
          . ": ERROR: Sending dead master's binlog failed.\n";
      }
      elsif ( $exit_code == 100 ) {
        $slave_starting_fail = 1;
        $mail_body .=
          $target->get_hostinfo()
          . ": WARN: Applying all logs succeeded. But starting slave failed.\n";
      }
      else {
        $recover_fail = 1;
        $log->info(
          sprintf(
            "-- Recovery on host %s failed, exit code %d",
            $target->get_hostinfo(), $exit_code
          )
        );
        $mail_body .=
          $target->get_hostinfo() . ": ERROR: Applying logs failed.\n";
      }
    }
  );

  foreach my $target (@alive_slaves) {

    # master was already recovered
    next if ( $target->{id} eq $new_master->{id} );

    my $pid = $pm->start($target) and next;

    my $pplog;
    eval {
      $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";
      my $local_file =
        "$g_workdir/$target->{hostname}_$target->{port}_$_start_datetime.log";
      unlink $local_file;
      $pplog = Log::Dispatch->new( callbacks => $MHA::ManagerConst::log_fmt );
      $pplog->add(
        Log::Dispatch::File->new(
          name      => 'file',
          filename  => $local_file,
          min_level => $target->{log_level},
          callbacks => $MHA::ManagerConst::add_timestamp,
          mode      => 'append'
        )
      );

      unless ( $target->{gen_diff_ok} ) {
        $pm->finish(20);
      }

      if ( send_binlog( $target, $pplog ) ) {
        $pm->finish(30);
      }
      $target->current_slave_position();
      if ( recover_slave( $target, $pplog ) ) {
        $pm->finish(1);
      }
      if ($g_skip_change_master) {
        $pplog->info("Skipping change master and start slave");
        $pm->finish(10);
      }
      if (
        $_server_manager->change_master_and_start_slave(
          $target, $new_master, $master_log_file, $master_log_pos, $pplog
        )
        )
      {
        $pm->finish(100);
      }
      else {
        $pm->finish(0);
      }
    };
    if ($@) {
      $pplog->error($@) if ($pplog);
      undef $@;
      $pm->finish(1);
    }
  }

  $pm->wait_all_children;

  return ( $skipping || $copy_fail || $recover_fail || $slave_starting_fail );
}

sub reconf_alive_servers {
  my $dead_master  = shift;
  my @alive_slaves = $_server_manager->get_alive_slaves();
  foreach my $slave (@alive_slaves) {
    next if ( $slave->{latest} );
    next if ( $slave->{ssh_ok} < 2 );
    next if ( $slave->{lack_relay_log} );
    $slave->check_set_ssh_status( $log, 1 );
  }
  my $init_again = 0;
  foreach my $slave (@alive_slaves) {
    if ( $slave->{ssh_ok} == 0 || $slave->{lack_relay_log} ) {
      $init_again = 1;
      last;
    }
  }
  if ($init_again) {
    $_server_manager->init_servers();
    $log->info("Dead Servers:");
    $_server_manager->print_dead_servers();
    $log->info("Alive Slaves:");
    $_server_manager->print_alive_slaves();
    $_server_manager->print_failed_slaves_if();
    $_server_manager->print_unmanaged_slaves_if();
  }
  $_server_manager->validate_num_alive_servers( $dead_master, 1 );
}

sub report_failed_slaves($) {
  my $dead_master       = shift;
  my $has_failed_slaves = 0;
  my @dead_servers      = $_server_manager->get_dead_servers();
  foreach (@dead_servers) {
    next if ( $_->{id} eq $dead_master->{id} );
    $mail_body .=
      $_->get_hostinfo()
      . ": ERROR: Could not be reachable so couldn't recover.\n";
    $has_failed_slaves = 1;
  }

  my @failed_slaves = $_server_manager->get_failed_slaves();
  foreach (@failed_slaves) {
    $mail_body .=
      $_->get_hostinfo() . ": ERROR: Slave failed so couldn't recover.\n";
    $has_failed_slaves = 1;
  }
  return $has_failed_slaves;
}

sub recover_slaves($$$$$$) {
  my $dead_master       = shift;
  my $new_master        = shift;
  my $latest_base_slave = shift;
  my $master_log_file   = shift;
  my $master_log_pos    = shift;
  my $exec_gtid_set     = shift;
  my $recover_slave_rc;

  if ( $_server_manager->is_gtid_auto_pos_enabled() ) {
    $recover_slave_rc =
      recover_slaves_gtid_internal( $new_master, $exec_gtid_set );
  }
  else {
    if ( recover_all_slaves_relay_logs( $new_master, $latest_base_slave ) ) {
      my $msg = "Generating relay diff files from the latest slave failed.";
      $log->error($msg);
      $mail_body .= "$msg\n";
    }
    else {
      my $msg = "Generating relay diff files from the latest slave succeeded.";
      $log->info($msg);
      $mail_body .= "$msg\n";
    }
    $recover_slave_rc =
      recover_slaves_internal( $new_master, $master_log_file, $master_log_pos,
      $latest_base_slave );
  }
  my $reset_slave_rc;
  if ( $recover_slave_rc == 0 ) {
    if ($g_skip_change_master) {
      $log->info("All slave servers are applied logs successfully.");
      $log->info();
    }
    else {
      $log->info("All new slave servers recovered successfully.");
      $log->info();
      $log->info("* Phase 5: New master cleanup phase..");
      $log->info();
      if ( $new_master->{skip_reset_slave} ) {
        $log->info("Skipping RESET SLAVE on the new master.");
        $reset_slave_rc = 0;
      }
      else {
        $log->info("Resetting slave info on the new master..");
        $reset_slave_rc = $new_master->reset_slave_on_new_master();
        if ( $reset_slave_rc eq '0' ) {
          $mail_body .=
            $new_master->get_hostinfo() . ": Resetting slave info succeeded.\n";
        }
        else {
          $mail_body .=
            $new_master->get_hostinfo() . ": Resetting slave info failed.\n";
        }
      }
    }
  }
  my $has_failed_servers = report_failed_slaves($dead_master);
  my $all_ok             = 1;
  if ( $recover_slave_rc || $reset_slave_rc || $has_failed_servers ) {
    $all_ok = 0;
  }
  if ($all_ok) {
    $mail_subject .= " succeeded";
    my $message = sprintf( "Master failover to %s completed successfully.",
      $new_master->get_hostinfo() );
    $log->info($message);
    $mail_body .= $message . "\n";
    return 0;
  }
  else {
    my $message = sprintf(
      "Master failover to %s done, but recovery on slave partially failed.",
      $new_master->get_hostinfo() );
    $log->error($message);
    $mail_body .= $message . "\n";
    return 10;
  }
}

sub cleanup {
  $_server_manager->release_failover_advisory_lock();
  $_server_manager->disconnect_all();
  MHA::NodeUtil::create_file_if($_failover_complete_file);
  $_create_error_file = 0;
  return 0;
}

sub send_report {
  my $dead_master = shift;
  my $new_master  = shift;

  if ( $mail_subject && $mail_body ) {
    $log->info( "\n\n"
        . "----- Failover Report -----\n\n"
        . $mail_subject . "\n\n"
        . $mail_body );
    if ( $dead_master->{report_script} ) {
      my $new_slaves   = "";
      my @alive_slaves = $_server_manager->get_alive_slaves();
      foreach my $slave (@alive_slaves) {
        if ( $slave->{recover_ok} ) {
          $new_slaves .= "," if ($new_slaves);
          $new_slaves .= $slave->{hostname};
        }
      }
      my $command =
"$dead_master->{report_script} --orig_master_host=$dead_master->{hostname} ";
      if ( $new_master && $new_master->{hostname} && $new_master->{activated} )
      {
        $command .= " --new_master_host=$new_master->{hostname} ";
        $command .= " --new_slave_hosts=$new_slaves ";
      }
      $command .= " --conf=$g_config_file ";
      $command .= " --subject=\"$mail_subject\" --body=\"$mail_body\"";
      $log->info("Sending mail..");
      my ( $high, $low ) =
        MHA::ManagerUtil::exec_system( $command, $g_logfile );
      if ( $high != 0 || $low != 0 ) {
        $log->error("Failed to send mail with return code $high:$low");
      }
    }
  }
}

sub do_master_failover {
  my $error_code = 1;
  my ( $dead_master, $new_master );

  eval {
    my ( $servers_config_ref, $binlog_server_ref ) = init_config();
    $log->info("Starting master failover.");
    $log->info();
    $log->info("* Phase 1: Configuration Check Phase..\n");
    $log->info();
    MHA::ServerManager::init_binlog_server( $binlog_server_ref, $log );
    $dead_master = check_settings($servers_config_ref);
    if ( $_server_manager->is_gtid_auto_pos_enabled() ) {
      $log->info("Starting GTID based failover.");
    }
    else {
      $_server_manager->force_disable_log_bin_if_auto_pos_disabled();
      $log->info("Starting Non-GTID based failover.");
    }
    $log->info();
    $log->info("** Phase 1: Configuration Check Phase completed.\n");
    $log->info();
    $log->info("* Phase 2: Dead Master Shutdown Phase..\n");
    $log->info();
    force_shutdown($dead_master);

    $log->info("* Phase 2: Dead Master Shutdown Phase completed.\n");
    $log->info();
    $log->info("* Phase 3: Master Recovery Phase..\n");
    $log->info();

    $log->info("* Phase 3.1: Getting Latest Slaves Phase..\n");
    $log->info();
    check_set_latest_slaves();

    if ( !$_server_manager->is_gtid_auto_pos_enabled() ) {
      $log->info();
      $log->info("* Phase 3.2: Saving Dead Master's Binlog Phase..\n");
      $log->info();
      save_master_binlog($dead_master);
    }

    $log->info();
    $log->info("* Phase 3.3: Determining New Master Phase..\n");
    $log->info();

    my $latest_base_slave;
    if ( $_server_manager->is_gtid_auto_pos_enabled() ) {
      $latest_base_slave = $_server_manager->get_most_advanced_latest_slave();
    }
    else {
      $latest_base_slave = find_latest_base_slave($dead_master);
    }
    $new_master = select_new_master( $dead_master, $latest_base_slave );
    my ( $master_log_file, $master_log_pos, $exec_gtid_set ) =
      recover_master( $dead_master, $new_master, $latest_base_slave,
      $binlog_server_ref );
    $new_master->{activated} = 1;

    $log->info("* Phase 3: Master Recovery Phase completed.\n");
    $log->info();
    $log->info("* Phase 4: Slaves Recovery Phase..\n");
    $log->info();
    $error_code = recover_slaves(
      $dead_master,     $new_master,     $latest_base_slave,
      $master_log_file, $master_log_pos, $exec_gtid_set
    );

    if ( $g_remove_dead_master_conf && $error_code == 0 ) {
      MHA::Config::delete_block_and_save( $g_config_file, $dead_master->{id},
        $log );
    }
    cleanup();
  };
  if ($@) {
    if ( $dead_master && $dead_master->{not_error} ) {
      $log->info($@);
    }
    else {
      MHA::ManagerUtil::print_error( "Got ERROR: $@", $log );
      $mail_body .= "Got Error so couldn't continue failover from here.\n"
        if ($mail_body);
    }
    $_server_manager->disconnect_all() if ($_server_manager);
    undef $@;
  }
  eval {
    send_report( $dead_master, $new_master );
    MHA::NodeUtil::drop_file_if( $_status_handler->{status_file} )
      unless ($error_code);

    if ($_create_error_file) {
      MHA::NodeUtil::create_file_if($_failover_error_file);
    }
  };
  if ($@) {
    MHA::ManagerUtil::print_error( "Got ERROR on final reporting: $@", $log );
    undef $@;
  }
  return $error_code;
}

sub finalize_on_error {
  eval {

    # Failover failure happened
    $_status_handler->update_status($MHA::ManagerConst::ST_FAILOVER_ERROR_S)
      if ($_status_handler);
    if ( $g_wait_on_failover_error > 0 && !$g_interactive ) {
      if ($log) {
        $log->info(
          "Waiting for $g_wait_on_failover_error seconds for error exit..");
      }
      else {
        print
          "Waiting for $g_wait_on_failover_error seconds for error exit..\n";
      }
      sleep $g_wait_on_failover_error;
    }
    MHA::NodeUtil::drop_file_if( $_status_handler->{status_file} )
      if ($_status_handler);
  };
  if ($@) {
    MHA::ManagerUtil::print_error(
      "Got Error on finalize_on_error at failover: $@", $log );
    undef $@;
  }

}

sub main {
  local $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = \&exit_by_signal;
  local @ARGV = @_;
  my ( $master_host, $master_ip, $master_port, $error_code );
  my ( $year, $mon, @time ) = reverse( (localtime)[ 0 .. 5 ] );
  $_start_datetime = sprintf '%04d%02d%02d%02d%02d%02d', $year + 1900, $mon + 1,
    @time;

  GetOptions(
    'global_conf=s'              => \$g_global_config_file,
    'conf=s'                     => \$g_config_file,
    'dead_master_host=s'         => \$master_host,
    'dead_master_ip=s'           => \$master_ip,
    'dead_master_port=i'         => \$master_port,
    'new_master_host=s'          => \$g_new_master_host,
    'new_master_port=i'          => \$g_new_master_port,
    'interactive=i'              => \$g_interactive,
    'ssh_reachable=i'            => \$g_ssh_reachable,
    'last_failover_minute=i'     => \$g_last_failover_minute,
    'wait_on_failover_error=i'   => \$g_wait_on_failover_error,
    'ignore_last_failover'       => \$g_ignore_last_failover,
    'workdir=s'                  => \$g_workdir,
    'manager_workdir=s'          => \$g_workdir,
    'log_output=s'               => \$g_logfile,
    'manager_log=s'              => \$g_logfile,
    'skip_save_master_binlog'    => \$g_skip_save_master_binlog,
    'remove_dead_master_conf'    => \$g_remove_dead_master_conf,
    'remove_orig_master_conf'    => \$g_remove_dead_master_conf,
    'skip_change_master'         => \$g_skip_change_master,
    'skip_disable_read_only'     => \$g_skip_disable_read_only,
    'wait_until_gtid_in_sync=i'  => \$g_wait_until_gtid_in_sync,
    'ignore_binlog_server_error' => \$g_ignore_binlog_server_error,
  );
  setpgrp( 0, $$ ) unless ($g_interactive);

  unless ($g_config_file) {
    print "--conf=<server_config_file> must be set.\n";
    return 1;
  }
  unless ($master_host) {
    print "--dead_master_host=<dead_master_hostname> must be set.\n";
    return 1;
  }
  unless ($master_ip) {
    $master_ip = MHA::NodeUtil::get_ip($master_host);
    print "--dead_master_ip=<dead_master_ip> is not set. Using $master_ip.\n";
  }
  unless ($master_port) {
    $master_port = 3306;
    print
      "--dead_master_port=<dead_master_port> is not set. Using $master_port.\n";
  }

  $_dead_master_arg{hostname} = $master_host;
  $_dead_master_arg{ip}       = $master_ip;
  $_dead_master_arg{port}     = $master_port;

  # in interactive mode, always prints to stdout/stderr
  $g_logfile = undef if ($g_interactive);

  eval { $error_code = do_master_failover(); };
  if ($@) {
    $error_code = 1;
  }
  if ($error_code) {
    finalize_on_error();
  }
  return $error_code;
}

1;

