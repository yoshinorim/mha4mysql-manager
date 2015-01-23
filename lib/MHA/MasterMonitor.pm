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

package MHA::MasterMonitor;

use strict;
use warnings FATAL => 'all';
use Carp qw(croak);
use English qw(-no_match_vars);
use Getopt::Long qw(:config pass_through);
use Pod::Usage;
use Log::Dispatch;
use Log::Dispatch::Screen;
use MHA::Config;
use MHA::ServerManager;
use MHA::HealthCheck;
use MHA::FileStatus;
use MHA::SSHCheck;
use MHA::ManagerConst;
use MHA::ManagerUtil;
use MHA::BinlogManager;
use File::Basename;

my $g_global_config_file = $MHA::ManagerConst::DEFAULT_GLOBAL_CONF;
my $g_config_file;
my $g_check_only;
my $g_check_repl_health;
my $g_seconds_behind_master = 30;
my $g_monitor_only;
my $g_workdir;
my $g_interactive = 1;
my $g_logfile;
my $g_wait_on_monitor_error = 0;
my $g_skip_ssh_check;
my $g_ignore_fail_on_start = 0;
my $_master_node_version;
my $_server_manager;
my $_master_ping;
my $RETRY = 100;
my $_status_handler;
my $log;

sub exit_by_signal {
  $log->info("Got terminate signal. Exit.");
  eval {
    MHA::NodeUtil::drop_file_if( $_status_handler->{status_file} )
      unless ($g_check_only);
    $_server_manager->disconnect_all() if ($_server_manager);
    $_master_ping->disconnect_if()     if ($_master_ping);
  };
  if ($@) {
    $log->error("Got Error: $@");
    undef $@;
  }
  exit 1;
}

sub get_binlog_check_command {
  my $target     = shift;
  my $use_prefix = shift;

  # this file is not created. just checking directory path
  my $workfile = "$target->{remote_workdir}/save_binary_logs_test";
  my $command =
"save_binary_logs --command=test --start_pos=4 --binlog_dir=$target->{master_binlog_dir} --output_file=$workfile --manager_version=$MHA::ManagerConst::VERSION";
  my $file;
  if ( $target->{File} ) {
    $file = $target->{File};
  }
  else {
    my @alive_slaves = $_server_manager->get_alive_slaves();
    my $slave        = $alive_slaves[0];
    $slave->current_slave_position();
    $file = $slave->{Master_Log_File};
  }
  if ($use_prefix) {
    my ( $binlog_prefix, $number ) =
      MHA::BinlogManager::get_head_and_number($file);
    $command .= " --binlog_prefix=$binlog_prefix";
  }
  else {
    $command .= " --start_file=$file";
  }

  unless ( $target->{handle_raw_binlog} ) {
    my $oldest_version = $_server_manager->get_oldest_version();
    $command .= " --oldest_version=$oldest_version ";
  }
  if ( $target->{log_level} eq "debug" ) {
    $command .= " --debug ";
  }
  return $command;
}

sub check_master_ssh_env($) {
  my $target = shift;
  $log->info(
    "Checking SSH publickey authentication settings on the current master..");

  my $ssh_reachable;
  if (
    MHA::HealthCheck::ssh_check_simple(
      $target->{ssh_user}, $target->{ssh_host},
      $target->{ssh_ip},   $target->{ssh_port},
      $target->{logger},   $target->{ssh_connection_timeout}
    )
    )
  {
    $ssh_reachable = 0;
  }
  else {
    $ssh_reachable = 1;
  }
  if ( $ssh_reachable && !$target->{has_gtid} ) {
    $_master_node_version =
      MHA::ManagerUtil::get_node_version( $log, $target->{ssh_user},
      $target->{ssh_host}, $target->{ssh_ip}, $target->{ssh_port} );
    if ( !$_master_node_version ) {
      $log->error(
"Failed to get MHA node version on the current master even though current master is reachable via SSH!"
      );
      croak;
    }
    $log->info("Master MHA Node version is $_master_node_version.");
  }
  return $ssh_reachable;
}

sub check_binlog($) {
  my $target = shift;
  $log->info(
    sprintf( "Checking recovery script configurations on %s..",
      $target->get_hostinfo() )
  );
  my $ssh_user_host = $target->{ssh_user} . '@' . $target->{ssh_ip};
  my $command       = get_binlog_check_command($target);
  $log->info("  Executing command: $command ");
  $log->info(
    "  Connecting to $ssh_user_host($target->{ssh_host}:$target->{ssh_port}).. "
  );
  my ( $high, $low ) =
    MHA::ManagerUtil::exec_ssh_cmd( $ssh_user_host, $target->{ssh_port},
    $command, $g_logfile );
  if ( $high ne '0' || $low ne '0' ) {
    $log->error("Binlog setting check failed!");
    return 1;
  }
  $log->info("Binlog setting check done.");
  return 0;
}

sub check_slave_env() {
  my @alive_servers = $_server_manager->get_alive_slaves();
  $log->info(
"Checking SSH publickey authentication and checking recovery script configurations on all alive slave servers.."
  );
  foreach my $s (@alive_servers) {
    my $ssh_user_host = $s->{ssh_user} . '@' . $s->{ssh_ip};
    my $command =
"apply_diff_relay_logs --command=test --slave_user=$s->{escaped_user} --slave_host=$s->{hostname} --slave_ip=$s->{ip} --slave_port=$s->{port} --workdir=$s->{remote_workdir} --target_version=$s->{mysql_version} --manager_version=$MHA::ManagerConst::VERSION";
    if ( $s->{client_bindir} ) {
      $command .= " --client_bindir=$s->{client_bindir}";
    }
    if ( $s->{client_libdir} ) {
      $command .= " --client_libdir=$s->{client_libdir}";
    }
    if ( $s->{relay_log_info_type} eq "TABLE" ) {
      $command .=
" --relay_dir=$s->{relay_dir} --current_relay_log=$s->{current_relay_log} ";
    }
    else {
      $command .= " --relay_log_info=$s->{relay_log_info} ";
      $command .= " --relay_dir=$s->{datadir} ";
    }
    if ( $s->{log_level} eq "debug" ) {
      $command .= " --debug ";
    }
    if ($MHA::ManagerConst::USE_SSH_OPTIONS) {
      $command .= " --ssh_options='$MHA::NodeConst::SSH_OPT_ALIVE' ";
    }
    $log->info("  Executing command : $command --slave_pass=xxx");

    if ( $s->{escaped_password} ne "" ) {
      $command .= " --slave_pass=$s->{escaped_password}";
    }
    $log->info(
      "  Connecting to $ssh_user_host($s->{ssh_host}:$s->{ssh_port}).. ");
    my ( $high, $low ) =
      MHA::ManagerUtil::exec_ssh_cmd( $ssh_user_host, $s->{ssh_port}, $command,
      $g_logfile );
    if ( $high ne '0' || $low ne '0' ) {
      $log->error("Slaves settings check failed!");
      return 1;
    }
  }
  $log->info("Slaves settings check done.");
  return 0;
}

sub check_scripts($) {
  my $current_master = shift;
  if ( $current_master->{master_ip_failover_script} ) {
    my $command =
"$current_master->{master_ip_failover_script} --command=status --ssh_user=$current_master->{ssh_user} --orig_master_host=$current_master->{hostname} --orig_master_ip=$current_master->{ip} --orig_master_port=$current_master->{port}";
    $command .= $current_master->get_ssh_args_if( 1, "orig", 1 );
    $log->info("Checking master_ip_failover_script status:");
    $log->info("  $command");
    my ( $high, $low ) = MHA::ManagerUtil::exec_system( $command, $g_logfile );
    if ( $high == 0 && $low == 0 ) {
      $log->info(" OK.");
    }
    else {
      $log->error(
" Failed to get master_ip_failover_script status with return code $high:$low."
      );
      croak;
    }
  }
  else {
    $log->warning("master_ip_failover_script is not defined.");
  }

  if ( $current_master->{shutdown_script} ) {
    my $command =
"$current_master->{shutdown_script} --command=status --ssh_user=$current_master->{ssh_user} --host=$current_master->{hostname} --ip=$current_master->{ip}";
    $command .= $current_master->get_ssh_args_if( 1, "shutdown", 1 );
    $log->info("Checking shutdown script status:");
    $log->info("  $command");
    my ( $high, $low ) = MHA::ManagerUtil::exec_system( $command, $g_logfile );
    if ( $high == 0 && $low == 0 ) {
      $log->info(" OK.");
    }
    else {
      $log->error(" Failed to get power status with return code $high:$low.");
      croak;
    }
  }
  else {
    $log->warning("shutdown_script is not defined.");
  }
}

sub check_binlog_servers {
  my $binlog_server_ref = shift;
  my $log               = shift;
  my @binlog_servers    = @$binlog_server_ref;
  if ( $#binlog_servers >= 0 ) {
    MHA::ServerManager::init_binlog_server( $binlog_server_ref, $log );
    foreach my $server (@binlog_servers) {
      if ( check_binlog($server) ) {
        $log->error("Binlog server configuration failed.");
        croak;
      }
    }
  }
}

sub wait_until_master_is_unreachable() {
  my ( @servers_config, @servers, @dead_servers, @alive_servers, @alive_slaves,
    $current_master, $ret, $ssh_reachable );
  my $func_rc = 1;
  eval {
    $g_logfile = undef if ($g_check_only);
    $log = MHA::ManagerUtil::init_log($g_logfile);

    unless ( -f $g_config_file ) {
      $log->error("Configuration file $g_config_file not found!");
      croak;
    }
    my ( $sc_ref, $binlog_server_ref ) = new MHA::Config(
      logger     => $log,
      globalfile => $g_global_config_file,
      file       => $g_config_file
    )->read_config();
    @servers_config = @$sc_ref;

    if ( !$g_logfile && !$g_check_only && $servers_config[0]->{manager_log} ) {
      $g_logfile = $servers_config[0]->{manager_log};
    }
    $log =
      MHA::ManagerUtil::init_log( $g_logfile, $servers_config[0]->{log_level} );
    $log->info("MHA::MasterMonitor version $MHA::ManagerConst::VERSION.");
    unless ($g_workdir) {
      if ( $servers_config[0]->{manager_workdir} ) {
        $g_workdir = $servers_config[0]->{manager_workdir};
      }
      else {
        $g_workdir = "/var/tmp";
      }
    }

    MHA::ManagerUtil::check_node_version($log);
    MHA::NodeUtil::check_manager_version($MHA::ManagerConst::VERSION);
    MHA::NodeUtil::create_dir_if($g_workdir);
    unless ($g_check_only) {
      $_status_handler =
        new MHA::FileStatus( conffile => $g_config_file, dir => $g_workdir );
      $_status_handler->init();

      if ( -f $_status_handler->{status_file} ) {
        $log->warning(
"$_status_handler->{status_file} already exists. You might have killed manager with SIGKILL(-9), may run two or more monitoring process for the same application, or use the same working directory. Check for details, and consider setting --workdir separately."
        );
        MHA::NodeUtil::drop_file_if( $_status_handler->{status_file} );
      }
      $_status_handler->update_status(
        $MHA::ManagerConst::ST_INITIALIZING_MONITOR_S);
    }

    $_server_manager = new MHA::ServerManager( servers => \@servers_config );
    $_server_manager->set_logger($log);

    $_server_manager->connect_all_and_read_server_status();
    @dead_servers  = $_server_manager->get_dead_servers();
    @alive_servers = $_server_manager->get_alive_servers();
    @alive_slaves  = $_server_manager->get_alive_slaves();
    $log->info("Dead Servers:");
    $_server_manager->print_dead_servers();
    $log->info("Alive Servers:");
    $_server_manager->print_alive_servers();
    $log->info("Alive Slaves:");
    $_server_manager->print_alive_slaves();
    $_server_manager->print_failed_slaves_if();
    $_server_manager->print_unmanaged_slaves_if();

    $current_master = $_server_manager->get_current_alive_master();

    unless ($current_master) {
      if ($g_interactive) {
        print "Master is not currently alive. Proceed? (yes/NO): ";
        my $ret = <STDIN>;
        chomp($ret);
        die "abort" if ( lc($ret) !~ /^y/ );
      }
    }
    if (
      $_server_manager->validate_slaves(
        $servers_config[0]->{check_repl_filter},
        $current_master
      )
      )
    {
      $log->error("Slave configurations is not valid.");
      croak;
    }
    my @bad = $_server_manager->get_bad_candidate_masters();
    if ( $#alive_slaves <= $#bad ) {
      $log->error( "None of slaves can be master. Check failover "
          . "configuration file or log-bin settings in my.cnf" );
      croak;
    }
    $_server_manager->check_repl_priv();

    if ( !$_server_manager->is_gtid_auto_pos_enabled() ) {
      $log->info("GTID (with auto-pos) is not supported");
      MHA::SSHCheck::do_ssh_connection_check( \@alive_servers, $log,
        $servers_config[0]->{log_level}, $g_workdir )
        unless ($g_skip_ssh_check);
      $log->info("Checking MHA Node version..");
      foreach my $slave (@alive_slaves) {
        MHA::ManagerUtil::check_node_version(
          $log,             $slave->{ssh_user}, $slave->{ssh_host},
          $slave->{ssh_ip}, $slave->{ssh_port}
        );
      }
      $log->info(" Version check ok.");
    }
    else {
      $log->info(
"GTID (with auto-pos) is supported. Skipping all SSH and Node package checking."
      );
      check_binlog_servers( $binlog_server_ref, $log );
    }

    unless ($current_master) {
      $log->info("Getting current master (maybe dead) info ..");
      $current_master = $_server_manager->get_orig_master();
      if ( !$current_master ) {
        $log->error("Failed to get current master info!");
        croak;
      }
      $log->info(
        sprintf( "Identified master is %s.", $current_master->get_hostinfo() )
      );
    }
    $_server_manager->validate_num_alive_servers( $current_master,
      $g_ignore_fail_on_start );
    if ( check_master_ssh_env($current_master) ) {
      if ( !$_server_manager->is_gtid_auto_pos_enabled()
        && check_binlog($current_master) )
      {
        $log->error("Master configuration failed.");
        croak;
      }
    }
    $_status_handler->set_master_host( $current_master->{hostname} )
      unless ($g_check_only);

    if ( !$_server_manager->is_gtid_auto_pos_enabled() && check_slave_env() ) {
      $log->error("Slave configuration failed.");
      croak;
    }
    $_server_manager->print_servers_ascii($current_master);
    $_server_manager->check_replication_health($g_seconds_behind_master)
      if ($g_check_repl_health);
    check_scripts($current_master);
    $_server_manager->disconnect_all();
    $func_rc = 0;
  };
  if ($@) {
    $log->error("Error happened on checking configurations. $@") if ($log);
    undef $@;
    return $func_rc;
  }
  return $func_rc if ($g_check_only);

  # master ping. This might take hours/days/months..
  $func_rc = 1;
  eval {
    my $ssh_check_command;
    if (!$_server_manager->is_gtid_auto_pos_enabled()
      && $_master_node_version
      && $_master_node_version >= 0.53 )
    {
      $ssh_check_command = get_binlog_check_command( $current_master, 1 );
    }
    else {
      $ssh_check_command = "exit 0";
    }
    $log->debug("SSH check command: $ssh_check_command");

    $_master_ping = new MHA::HealthCheck(
      user                   => $current_master->{user},
      password               => $current_master->{password},
      ip                     => $current_master->{ip},
      hostname               => $current_master->{hostname},
      port                   => $current_master->{port},
      interval               => $current_master->{ping_interval},
      ssh_user               => $current_master->{ssh_user},
      ssh_host               => $current_master->{ssh_host},
      ssh_ip                 => $current_master->{ssh_ip},
      ssh_port               => $current_master->{ssh_port},
      ssh_connection_timeout => $current_master->{ssh_connection_timeout},
      ssh_check_command      => $ssh_check_command,
      status_handler         => $_status_handler,
      logger                 => $log,
      logfile                => $g_logfile,
      workdir                => $g_workdir,
      ping_type              => $current_master->{ping_type},
    );
    $log->info(
      sprintf( "Set master ping interval %d seconds.",
        $_master_ping->get_ping_interval() )
    );
    if ( $current_master->{secondary_check_script} ) {
      $_master_ping->set_secondary_check_script(
        $current_master->{secondary_check_script} );
      $log->info(
        sprintf( "Set secondary check script: %s",
          $_master_ping->get_secondary_check_script() )
      );
    }
    else {
      $log->warning(
"secondary_check_script is not defined. It is highly recommended setting it to check master reachability from two or more routes."
      );
    }

    $log->info(
      sprintf( "Starting ping health check on %s..",
        $current_master->get_hostinfo() )
    );
    ( $ret, $ssh_reachable ) = $_master_ping->wait_until_unreachable();
    if ( $ret eq '2' ) {
      $log->error(
"Target master's advisory lock is already held by someone. Please check whether you monitor the same master from multiple monitoring processes."
      );
      croak;
    }
    elsif ( $ret ne '0' ) {
      croak;
    }
    $log->warning(
      sprintf( "Master %s is not reachable!", $current_master->get_hostinfo() )
    );
    if ($ssh_reachable) {
      $log->warning("SSH is reachable.");
    }
    else {
      $log->warning("SSH is NOT reachable.");
    }
    $func_rc = 0;
  };
  if ($@) {
    $log->error("Error happened on health checking. $@");
    undef $@;
    return $func_rc;
  }
  $_status_handler->update_status($MHA::ManagerConst::ST_PING_FAILED_S);

  return ( $func_rc, $current_master, $ssh_reachable );
}

sub wait_until_master_is_dead {
  my $exit_code = 1;
  my ( $ret, $dead_master, $ssh_reachable ) =
    wait_until_master_is_unreachable();
  if ( !defined($ret) || $ret ne '0' ) {
    $log->error("Error happened on monitoring servers.");
    return $exit_code;
  }

  if ($g_check_only) {
    return 0;
  }

  # this should not happen
  unless ($dead_master) {
    $log->error("Dead master not found!\n");
    return $exit_code;
  }

  # Master fails!
  # Reading config file and connecting to all hosts except master again
  # to check current availability
  $exit_code = eval {
    $log->info( "Connecting to a master server failed. Reading configuration "
        . "file $g_global_config_file and $g_config_file again, and trying to connect to all servers to "
        . "check server status.." );
    my $conf = new MHA::Config(
      logger     => $log,
      globalfile => $g_global_config_file,
      file       => $g_config_file
    );

    my ( $sc_ref, $binlog_server_ref ) = $conf->read_config();
    my @servers_config = @$sc_ref;
    $_server_manager = new MHA::ServerManager( servers => \@servers_config );
    $_server_manager->set_logger($log);
    $log->debug(
      sprintf( "Skipping connecting to dead master %s.",
        $dead_master->get_hostinfo() )
    );
    $_server_manager->connect_all_and_read_server_status(
      $dead_master->{hostname},
      $dead_master->{ip}, $dead_master->{port} );
    my @dead_servers  = $_server_manager->get_dead_servers();
    my @alive_servers = $_server_manager->get_alive_servers();
    $log->info("Dead Servers:");
    $_server_manager->print_dead_servers();
    $log->info("Alive Servers:");
    $_server_manager->print_alive_servers();
    $log->info("Alive Slaves:");
    $_server_manager->print_alive_slaves();
    $_server_manager->print_failed_slaves_if();
    $_server_manager->print_unmanaged_slaves_if();

    my $real_master = $_server_manager->get_orig_master();
    if ( $dead_master->{id} ne $real_master->{id} ) {
      $log->error(
        sprintf(
"Monitor detected %s failed, but actual master server is %s. Check replication configurations again.",
          $dead_master->get_hostinfo(),
          $real_master->get_hostinfo()
        )
      );
      return 1;
    }

    # When this condition is met, master is actually alive.
    if ( $_server_manager->get_alive_server_by_id( $dead_master->{id} ) ) {
      $log->warning("master is actually alive. starting monitoring again.");
      return $RETRY;
    }
    if (
      $_server_manager->validate_slaves(
        $servers_config[0]->{check_repl_filter}
      )
      )
    {
      $log->error( "At least one alive slave is not correctly configured. "
          . "Can't execute failover" );
      return 1;
    }

    $log->info("Master is down!");
    $log->info("Terminating monitoring script.");
    $_server_manager->disconnect_all() if ($_server_manager);
    return $MHA::ManagerConst::MASTER_DEAD_RC;
  };
  if ($@) {
    $log->warning("Got Error: $@");
    undef $@;
    $exit_code = 1;
  }
  return 1 if ( !defined($exit_code) );
  return $MHA::ManagerConst::MASTER_DEAD_RC, $dead_master, $ssh_reachable
    if ( $exit_code == $MHA::ManagerConst::MASTER_DEAD_RC );
  return $exit_code;
}

sub prepare_for_retry {
  eval {
    $_status_handler->update_status($MHA::ManagerConst::ST_RETRYING_MONITOR_S);
    $log->info("Waiting for $g_wait_on_monitor_error seconds for retrying..");
    sleep $g_wait_on_monitor_error;
    MHA::NodeUtil::drop_file_if( $_status_handler->{status_file} );
  };
  if ($@) {
    MHA::ManagerUtil::print_error(
      "Got Error on prepare_for_retry at monitor: $@", $log );
    undef $@;
  }
}

sub finalize_on_error {
  eval {

    # Monitor failure happened
    $_status_handler->update_status($MHA::ManagerConst::ST_CONFIG_ERROR_S)
      if ($_status_handler);
    if ( $g_wait_on_monitor_error > 0 ) {
      $log->info(
        "Waiting for $g_wait_on_monitor_error seconds for error exit..");
      sleep $g_wait_on_monitor_error;
    }
    MHA::NodeUtil::drop_file_if( $_status_handler->{status_file} )
      if ($_status_handler);
  };
  if ($@) {
    MHA::ManagerUtil::print_error(
      "Got Error on finalize_on_error at monitor: $@", $log );
    undef $@;
  }
}

sub finalize {
  eval {
    MHA::NodeUtil::drop_file_if( $_status_handler->{status_file} )
      if ($_status_handler);
  };
  if ($@) {
    MHA::ManagerUtil::print_error( "Got Error on finalize at monitor: $@",
      $log );
    undef $@;
  }

}

sub main {
  local $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = \&exit_by_signal;
  local @ARGV = @_;
  GetOptions(
    'global_conf=s'           => \$g_global_config_file,
    'conf=s'                  => \$g_config_file,
    'check_only'              => \$g_check_only,
    'check_repl_health'       => \$g_check_repl_health,
    'seconds_behind_master=i' => \$g_seconds_behind_master,
    'monitor_only'            => \$g_monitor_only,
    'interactive=i'           => \$g_interactive,
    'wait_on_monitor_error=i' => \$g_wait_on_monitor_error,
    'workdir=s'               => \$g_workdir,
    'manager_workdir=s'       => \$g_workdir,
    'log_output=s'            => \$g_logfile,
    'manager_log=s'           => \$g_logfile,
    'skip_ssh_check'          => \$g_skip_ssh_check,          # for testing
    'skip_check_ssh'          => \$g_skip_ssh_check,
    'ignore_fail_on_start'    => \$g_ignore_fail_on_start,
  );
  setpgrp( 0, $$ ) unless ($g_interactive);

  unless ($g_config_file) {
    print "--conf=<server config file> must be set.\n";
    return 1;
  }

  while (1) {
    my ( $exit_code, $dead_master, $ssh_reachable ) =
      wait_until_master_is_dead();
    my $msg = sprintf( "Got exit code %d (%s).",
      $exit_code,
      $exit_code == $MHA::ManagerConst::MASTER_DEAD_RC
      ? "Master dead"
      : "Not master dead" );
    $log->info($msg) if ($log);
    if ($g_check_only) {
      finalize();
      return $exit_code;
    }
    if ( $exit_code && $exit_code == $RETRY ) {
      prepare_for_retry();
    }
    else {
      if ( $exit_code && $exit_code != $MHA::ManagerConst::MASTER_DEAD_RC ) {
        finalize_on_error();
      }
      elsif ($g_monitor_only) {
        finalize();
      }
      return ( $exit_code, $dead_master, $ssh_reachable );
    }
  }
}

1;
