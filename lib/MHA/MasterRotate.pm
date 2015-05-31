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

package MHA::MasterRotate;

use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Carp qw(croak);
use POSIX qw(:signal_h);
use Getopt::Long;
use Pod::Usage;
use Log::Dispatch;
use Log::Dispatch::Screen;
use Log::Dispatch::File;
use MHA::Config;
use MHA::ServerManager;
use MHA::Server;
use MHA::ManagerUtil;
use File::Basename;
use Parallel::ForkManager;

my $g_global_config_file = $MHA::ManagerConst::DEFAULT_GLOBAL_CONF;
my $g_config_file;
my $g_check_only;
my $g_new_master_host;
my $g_new_master_port = 3306;
my $g_workdir;
my $g_flush_tables = 2;
my $g_orig_master_is_new_slave;
my $g_running_updates_limit = 1;
my $g_running_seconds_limit = 10;
my $g_skip_lock_all_tables;
my $g_remove_orig_master_conf;
my $g_interactive = 1;
my $_server_manager;
my $start_datetime;

my $log = MHA::ManagerUtil::init_log();

sub identify_orig_master() {
  my $orig_master;
  my ( @servers, @dead_servers, @alive_servers, @alive_slaves );
  $log->info("MHA::MasterRotate version $MHA::ManagerConst::VERSION.");
  $log->info("Starting online master switch..");
  $log->info();
  $log->info("* Phase 1: Configuration Check Phase..\n");
  $log->info();
  my ( $sc_ref, undef ) = new MHA::Config(
    logger     => $log,
    globalfile => $g_global_config_file,
    file       => $g_config_file
  )->read_config();
  my @servers_config = @$sc_ref;
  $log = MHA::ManagerUtil::init_log( undef, $servers_config[0]->{log_level} );

  unless ($g_workdir) {
    if ( $servers_config[0]->{manager_workdir} ) {
      $g_workdir = $servers_config[0]->{manager_workdir};
    }
    else {
      $g_workdir = "/var/tmp";
    }
  }

  $_server_manager = new MHA::ServerManager( servers => \@servers_config );
  $_server_manager->set_logger($log);
  $_server_manager->connect_all_and_read_server_status();
  @servers       = $_server_manager->get_servers();
  @dead_servers  = $_server_manager->get_dead_servers();
  @alive_servers = $_server_manager->get_alive_servers();
  @alive_slaves  = $_server_manager->get_alive_slaves();

  #Make sure that currently there is not any dead server.
  if ( $#dead_servers >= 0 ) {
    $log->error(
      "Switching master should not be started if one or more servers is down."
    );
    $log->info("Dead Servers:");
    $_server_manager->print_dead_servers();
    croak;
  }

  $orig_master = $_server_manager->get_current_alive_master();
  if ( !$orig_master ) {
    $log->error(
"Failed to get current master. Maybe replication is misconfigured or configuration file is broken?"
    );
    croak;
  }

  $log->info("Alive Slaves:");
  $_server_manager->print_alive_slaves(1);
  $_server_manager->print_unmanaged_slaves_if();

  $_server_manager->check_repl_priv();

  my $run_flush_tables = 1;

  if ( $g_interactive && $g_flush_tables == 2 ) {
    printf(
"\nIt is better to execute FLUSH NO_WRITE_TO_BINLOG TABLES on the master before switching. Is it ok to execute on %s? (YES/no): ",
      $orig_master->get_hostinfo() );
    my $ret = <STDIN>;
    chomp($ret);
    if ( lc($ret) !~ /^n/ ) {
      $run_flush_tables = 1;
    }
    else {
      $run_flush_tables = 0;
    }
  }
  else {
    $run_flush_tables = $g_flush_tables;
  }
  if ($run_flush_tables) {
    $orig_master->flush_tables();
  }
  else {
    $log->info("Skipping executing FLUSH NO_WRITE_TO_BINLOG TABLES.");
  }

  $log->info("Checking MHA is not monitoring or doing failover..");
  if ( $orig_master->get_monitor_advisory_lock(0) ) {
    $log->error(
"Getting advisory lock failed on the current master. MHA Monitor runs on the current master. Stop MHA Manager/Monitor and try again."
    );
    croak;
  }
  foreach my $target (@alive_slaves) {
    if ( $target->get_failover_advisory_lock(0) ) {
      $log->error(
"Getting advisory lock failed on $target->{hostname}. Maybe failover script or purge_relay_logs script is running on the same slave?"
      );
      croak;
    }
  }

  $_server_manager->check_replication_health($g_running_updates_limit);

  my @threads =
    $orig_master->get_running_update_threads( $g_running_updates_limit + 1 );
  if ( $#threads >= 0 ) {
    $log->error(
      sprintf(
"We should not start online master switch when one of connections are running long updates on the current master(%s). Currently %d update thread(s) are running.",
        $orig_master->get_hostinfo(),
        $#threads + 1
      )
    );
    MHA::DBHelper::print_threads_util( \@threads, 10 );
    croak;
  }
  return $orig_master;
}

sub check_filter {
  my $orig_master = shift;
  my $new_master  = shift;
  $orig_master->{Binlog_Do_DB}     ||= "";
  $new_master->{Binlog_Do_DB}      ||= "";
  $orig_master->{Binlog_Ignore_DB} ||= "";
  $new_master->{Binlog_Ignore_DB}  ||= "";
  if ( $orig_master->{Binlog_Do_DB} ne $new_master->{Binlog_Do_DB}
    || $orig_master->{Binlog_Ignore_DB} ne $new_master->{Binlog_Ignore_DB} )
  {
    $log->error(
"Binlog filtering check failed on the new master! Orig master and New master must have same binlog filtering rules (same binlog-do-db and binlog-ignore-db). Check SHOW MASTER STATUS output and set my.cnf correctly."
    );
    my $msg = "Bad Binlog/Replication filtering rules:\n";
    $msg .= $orig_master->print_filter( 1, 0 );
    $msg .= $new_master->print_filter( 0, 0 );
    $log->warning($msg);
    croak;
  }
  if ($g_orig_master_is_new_slave) {
    $orig_master->read_repl_filter();

    $orig_master->{Replicate_Do_Table}          ||= "";
    $orig_master->{Replicate_Ignore_Table}      ||= "";
    $orig_master->{Replicate_Wild_Do_Table}     ||= "";
    $orig_master->{Replicate_Wild_Ignore_Table} ||= "";
    $new_master->{Replicate_Do_Table}           ||= "";
    $new_master->{Replicate_Ignore_Table}       ||= "";
    $new_master->{Replicate_Wild_Do_Table}      ||= "";
    $new_master->{Replicate_Wild_Ignore_Table}  ||= "";

    if ( $orig_master->{Replicate_Do_Table} ne $new_master->{Replicate_Do_Table}
      || $orig_master->{Replicate_Ignore_Table} ne
      $new_master->{Replicate_Ignore_Table}
      || $orig_master->{Replicate_Wild_Do_Table} ne
      $new_master->{Replicate_Wild_Do_Table}
      || $orig_master->{Replicate_Wild_Ignore_Table} ne
      $new_master->{Replicate_Wild_Ignore_Table} )
    {
      $log->error(
"Replication filtering check failed on the orig/new master! Orig master and New master must have same replication filtering rules --orig_master_is_new_slave is set. Check SHOW SLAVE STATUS output and/or set my.cnf correctly."
      );
      my $msg = "Bad Binlog/Replication filtering rules:\n";
      $msg .= $orig_master->print_filter( 1, 1 );
      $msg .= $new_master->print_filter( 0, 1 );
      $log->warning($msg);
      croak;
    }
  }
}

sub identify_new_master {
  my $orig_master = shift;
  $_server_manager->set_latest_slaves( $_server_manager->{alive_slaves} );
  my $new_master =
    $_server_manager->select_new_master( $g_new_master_host,
    $g_new_master_port, 0 );
  unless ($new_master) {
    $log->error("Failed to get new master!");
    croak;
  }
  $_server_manager->print_servers_migration_ascii( $orig_master, $new_master,
    $g_orig_master_is_new_slave );

  if ( $g_interactive && !$g_check_only ) {
    $new_master =
      $_server_manager->manually_decide_new_master( $orig_master, $new_master );
  }

  $log->info(
    sprintf( "Checking whether %s is ok for the new master..",
      $new_master->get_hostinfo() )
  );
  if ( $_server_manager->is_target_bad_for_new_master($new_master) ) {
    $log->error(
      sprintf( "Server %s is not correctly configured to be new master!",
        $new_master->get_hostinfo() )
    );
    die;
  }
  $log->info(" ok.");

  if ( $orig_master->{check_repl_filter} ) {
    check_filter( $orig_master, $new_master );
  }

  my @threads = $new_master->get_running_threads($g_running_seconds_limit);
  if ( $#threads >= 0 ) {
    $log->error(
      sprintf(
"We should not start online master switch when one of connections are running long queries on the new master(%s). Currently %d thread(s) are running.",
        $new_master->get_hostinfo(),
        $#threads + 1
      )
    );
    MHA::DBHelper::print_threads_util( \@threads, 10 );
    croak;
  }

  return $new_master;
}

sub reject_update($$) {
  my $orig_master = shift;
  my $new_master  = shift;
  my $ret;
  $orig_master->release_monitor_advisory_lock();
  $orig_master->disconnect();

  $log->info("* Phase 2: Rejecting updates Phase..\n");
  $log->info();
  if ( $new_master->{master_ip_online_change_script} ) {
    my $command =
"$orig_master->{master_ip_online_change_script} --command=stop --orig_master_host=$orig_master->{hostname} --orig_master_ip=$orig_master->{ip} --orig_master_port=$orig_master->{port} --orig_master_user=$orig_master->{escaped_user} --new_master_host=$new_master->{hostname} --new_master_ip=$new_master->{ip} --new_master_port=$new_master->{port} --new_master_user=$new_master->{escaped_user}";
    $command .= " --orig_master_ssh_user=$orig_master->{ssh_user}";
    $command .= " --new_master_ssh_user=$new_master->{ssh_user}";
    $command .= $orig_master->get_ssh_args_if( 1, "orig", 1 );
    $command .= $new_master->get_ssh_args_if( 2, "new", 1 );
    if ($g_orig_master_is_new_slave) {
      $command .= " --orig_master_is_new_slave";
    }
    $log->info(
"Executing master ip online change script to disable write on the current master:"
    );
    $log->info(
      "  $command --orig_master_password=xxx --new_master_password=xxx");
    $command .=
" --orig_master_password=$orig_master->{escaped_password} --new_master_password=$new_master->{escaped_password}";
    my ( $high, $low ) = MHA::ManagerUtil::exec_system($command);

    if ( $high == 0 && $low == 0 ) {
      $log->info(" ok.");
    }
    else {
      if ( $high == 10 ) {
        $log->warning("Proceeding.");
      }
      else {
        croak;
      }
    }
  }
  elsif ($g_interactive) {
    print
"master_ip_online_change_script is not defined. If you do not disable writes on the current master manually, applications keep writing on the current master. Is it ok to proceed? (yes/NO): ";
    $ret = <STDIN>;
    chomp($ret);
    if ( lc($ret) !~ /^y/ ) {
      $orig_master->{not_error} = 1;
      die "Not typed yes. Stopping.";
    }
  }
  else {
    $log->warning(
"master_ip_online_change_script is not defined. Skipping disabling writes on the current master."
    );
  }

  # It is necessary to keep connecting on the orig master to check
  # binary log is not proceeding. master write control script may kill
  # previous connections, so it is needed to establish connection again.
  unless ( $orig_master->reconnect() ) {
    $log->error(
      sprintf( "Failed to connect to the orig master %s!",
        $orig_master->get_hostinfo() )
    );
    croak;
  }

  my ( $orig_master_log_file, $orig_master_log_pos );
  if ($g_skip_lock_all_tables) {
    $log->info("Skipping locking all tables.");
  }
  else {
    $log->info(
"Locking all tables on the orig master to reject updates from everybody (including root):"
    );
  }
  if ( $g_skip_lock_all_tables || $orig_master->lock_all_tables() ) {

    # FLUSH TABLES WITH READ LOCK is skipped or failed.
    # So we need to verify binlog writes are stopped or not.
    # All slaves should complete replication until this position
    ( $orig_master_log_file, $orig_master_log_pos ) =
      $orig_master->check_binlog_stop();
  }
  else {
    ( $orig_master_log_file, $orig_master_log_pos ) =
      $orig_master->get_binlog_position();
  }
  if ( !$orig_master_log_file || !defined($orig_master_log_pos) ) {
    $log->error("Failed to get orig master's binlog and/or position!");
    croak;
  }
  $log->info(
    sprintf(
      "Orig master binlog:pos is %s:%d.",
      $orig_master_log_file, $orig_master_log_pos
    )
  );
  return ( $orig_master_log_file, $orig_master_log_pos );
}

sub switch_master_internal($$$) {
  my $target               = shift;
  my $orig_master_log_file = shift;
  my $orig_master_log_pos  = shift;
  if ( $target->master_pos_wait( $orig_master_log_file, $orig_master_log_pos ) )
  {
    return;
  }
  return $_server_manager->get_new_master_binlog_position($target);
}

sub switch_master($$$$) {
  my $orig_master          = shift;
  my $new_master           = shift;
  my $orig_master_log_file = shift;
  my $orig_master_log_pos  = shift;

  my ( $master_log_file, $master_log_pos ) =
    switch_master_internal( $new_master, $orig_master_log_file,
    $orig_master_log_pos );
  if ( !$master_log_file or !defined($master_log_pos) ) {
    $log->error("Failed to get new master's binlog and/or position!");
    croak;
  }

  # Allow write access on the new master
  if ( $new_master->{master_ip_online_change_script} ) {
    my $command =
"$new_master->{master_ip_online_change_script} --command=start --orig_master_host=$orig_master->{hostname} --orig_master_ip=$orig_master->{ip} --orig_master_port=$orig_master->{port} --orig_master_user=$orig_master->{escaped_user} --new_master_host=$new_master->{hostname} --new_master_ip=$new_master->{ip} --new_master_port=$new_master->{port} --new_master_user=$new_master->{escaped_user}";
    $command .= " --orig_master_ssh_user=$orig_master->{ssh_user}";
    $command .= " --new_master_ssh_user=$new_master->{ssh_user}";
    $command .= $orig_master->get_ssh_args_if( 1, "orig", 1 );
    $command .= $new_master->get_ssh_args_if( 2, "new", 1 );
    if ($g_orig_master_is_new_slave) {
      $command .= " --orig_master_is_new_slave";
    }
    $log->info(
"Executing master ip online change script to allow write on the new master:"
    );
    $log->info(
      "  $command --orig_master_password=xxx --new_master_password=xxx");
    $command .=
" --orig_master_password=$orig_master->{escaped_password} --new_master_password=$new_master->{escaped_password}";
    my ( $high, $low ) = MHA::ManagerUtil::exec_system($command);

    if ( $high == 0 && $low == 0 ) {
      $log->info(" ok.");
    }
    else {
      if ( $high == 10 ) {
        $log->warning("Proceeding.");
      }
      else {
        croak;
      }
    }
  }

  # Allow write access on master (if read_only==1)
  $new_master->disable_read_only();

  return ( $master_log_file, $master_log_pos );
}

sub switch_slaves_internal {
  my $new_master           = shift;
  my $orig_master_log_file = shift;
  my $orig_master_log_pos  = shift;
  my $master_log_file      = shift;
  my $master_log_pos       = shift;
  my @alive_slaves         = $_server_manager->get_alive_slaves();

  $log->info();
  $log->info("* Switching slaves in parallel..\n");
  $log->info();

  my $pm                  = new Parallel::ForkManager( $#alive_slaves + 1 );
  my $wait_fail           = 0;
  my $slave_starting_fail = 0;

  $pm->run_on_start(
    sub {
      my ( $pid, $target ) = @_;
      $log->info(
        sprintf(
          "-- Slave switch on host %s started, pid: %d",
          $target->get_hostinfo(), $pid
        )
      );
      $log->info();
    }
  );

  $pm->run_on_finish(
    sub {
      my ( $pid, $exit_code, $target ) = @_;
      $log->info("Log messages from $target->{hostname} ...");
      my $local_file =
"$g_workdir/masteronlineswitch_$target->{hostname}_$target->{port}_$start_datetime.log";
      $log->info( "\n" . `cat $local_file` );
      $log->info("End of log messages from $target->{hostname} ...");
      $log->info();
      unlink $local_file;

      if ( $exit_code == 0 ) {
        $log->info(
          sprintf( "-- Slave switch on host %s succeeded.",
            $target->get_hostinfo() )
        );
      }
      elsif ( $exit_code == 100 ) {
        $slave_starting_fail = 1;
      }
      else {
        $wait_fail = 1;
        $log->info(
          sprintf(
            "-- Switching slave on host %s failed, exit code %d",
            $target->get_hostinfo(), $exit_code
          )
        );
      }
    }
  );

  foreach my $target (@alive_slaves) {

    # master was already switched
    next if ( $target->{id} eq $new_master->{id} );

    my $pid = $pm->start($target) and next;

    my $pplog;
    eval {
      $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";
      my $local_file =
"$g_workdir/masteronlineswitch_$target->{hostname}_$target->{port}_$start_datetime.log";
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

      $target->current_slave_position($pplog);
      if (
        $target->master_pos_wait(
          $orig_master_log_file, $orig_master_log_pos, $pplog
        )
        )
      {
        $pm->finish(1);
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

  return ( $wait_fail || $slave_starting_fail );
}

sub switch_slaves($$$$$$) {
  my $orig_master          = shift;
  my $new_master           = shift;
  my $orig_master_log_file = shift;
  my $orig_master_log_pos  = shift;
  my $master_log_file      = shift;
  my $master_log_pos       = shift;
  my $ret = switch_slaves_internal( $new_master, $orig_master_log_file,
    $orig_master_log_pos, $master_log_file, $master_log_pos );
  $log->info("Unlocking all tables on the orig master:");
  $orig_master->unlock_tables();

  if ($g_orig_master_is_new_slave) {
    $log->info("Starting orig master as a new slave..");
    if ( exists( $new_master->{use_ip_for_change_master} ) ) {
      $orig_master->{use_ip_for_change_master} =
        $new_master->{use_ip_for_change_master};
    }
    if (
      $_server_manager->change_master_and_start_slave(
        $orig_master, $new_master, $master_log_file, $master_log_pos
      )
      )
    {
      $log->error(" Failed!");
      $ret = 1;
    }
    else {
      $log->debug(" ok.");
    }
  }
  if ( $ret eq '0' ) {
    $log->info("All new slave servers switched successfully.");
    $log->info();
    $log->info("* Phase 5: New master cleanup phase..");
    $log->info();
    if ( $new_master->{skip_reset_slave} ) {
      $log->info("Skipping RESET SLAVE on the new master.");
    }
    else {
      $ret = $new_master->reset_slave_on_new_master();
    }
  }
  if ( $ret eq '0' ) {
    my $message = sprintf( "Switching master to %s completed successfully.",
      $new_master->get_hostinfo() );
    $log->info($message);
    return 0;
  }
  else {
    my $message = sprintf(
      "Switching master to %s done, but switching slaves partially failed.",
      $new_master->get_hostinfo() );
    $log->error($message);
    return 10;
  }
}

sub do_master_online_switch {
  my $error_code = 1;
  my $orig_master;

  eval {
    $orig_master = identify_orig_master();
    my $new_master = identify_new_master($orig_master);
    $log->info("** Phase 1: Configuration Check Phase completed.\n");
    $log->info();

    if ($g_check_only) {
      $log->info("--check_only is set. Exit.");
      $error_code = 0;
      return;
    }

    my ( $orig_master_log_file, $orig_master_log_pos ) =
      reject_update( $orig_master, $new_master );

    $_server_manager->read_slave_status();

    my ( $master_log_file, $master_log_pos ) =
      switch_master( $orig_master, $new_master, $orig_master_log_file,
      $orig_master_log_pos );

    $error_code =
      switch_slaves( $orig_master, $new_master, $orig_master_log_file,
      $orig_master_log_pos, $master_log_file, $master_log_pos );

    if ( $g_remove_orig_master_conf
      && !$g_orig_master_is_new_slave
      && $error_code == 0 )
    {
      MHA::Config::delete_block_and_save( $g_config_file, $orig_master->{id},
        $log );
    }

    $_server_manager->release_failover_advisory_lock();
    $_server_manager->disconnect_all();
  };
  if ($@) {
    if ( $orig_master && $orig_master->{not_error} ) {
      $log->info($@);
    }
    else {
      MHA::ManagerUtil::print_error( "Got ERROR: $@", $log );
    }
    $_server_manager->disconnect_all() if ($_server_manager);
    undef $@;
  }

  return $error_code;
}

sub handle_sigint {
  if ( my $pid = fork ) {
    waitpid( $pid, 0 );
  }
  elsif ( defined $pid ) {
    if ($_server_manager) {
      my @alive_servers = $_server_manager->get_alive_servers();
      foreach my $target (@alive_servers) {
        my $dbh = $target->connect_util();
        if ( $dbh
          && $target->{dbhelper}
          && $target->{dbhelper}->{connection_id} )
        {
          $log->info(
            sprintf(
              "Killing thread %d on %s..",
              $target->{dbhelper}->{connection_id},
              $target->get_hostinfo()
            )
          );
          MHA::DBHelper::kill_thread_util( $dbh,
            $target->{dbhelper}->{connection_id} );
          $log->info("ok.");
        }
        $dbh->disconnect() if ($dbh);
      }
    }
    exit 0;
  }
  exit 1;
}

sub main {
  my $sigset = POSIX::SigSet->new(SIGINT);
  my $sigaction =
    POSIX::SigAction->new( \&handle_sigint, $sigset, &POSIX::SA_NOCLDSTOP );
  POSIX::sigaction( SIGINT, $sigaction );
  local @ARGV = @_;
  my $a = GetOptions(
    'global_conf=s'            => \$g_global_config_file,
    'conf=s'                   => \$g_config_file,
    'check_only'               => \$g_check_only,
    'new_master_host=s'        => \$g_new_master_host,
    'new_master_port=i'        => \$g_new_master_port,
    'workdir=s'                => \$g_workdir,
    'manager_workdir=s'        => \$g_workdir,
    'interactive=i'            => \$g_interactive,
    'orig_master_is_new_slave' => \$g_orig_master_is_new_slave,
    'running_updates_limit=i'  => \$g_running_updates_limit,
    'running_seconds_limit=i'  => \$g_running_seconds_limit,
    'skip_lock_all_tables'     => \$g_skip_lock_all_tables,
    'remove_dead_master_conf'  => \$g_remove_orig_master_conf,
    'remove_orig_master_conf'  => \$g_remove_orig_master_conf,
    'flush_tables=i'           => \$g_flush_tables,
  );
  if ( $#ARGV >= 0 ) {
    print "Unknown options: ";
    print $_ . " " foreach (@ARGV);
    print "\n";
    return 1;
  }
  unless ($g_config_file) {
    print "--conf=<server_config_file> must be set.\n";
    return 1;
  }
  my ( $year, $mon, @time ) = reverse( (localtime)[ 0 .. 5 ] );
  $start_datetime = sprintf '%04d%02d%02d%02d%02d%02d', $year + 1900, $mon + 1,
    @time;
  return do_master_online_switch();
}

1;

