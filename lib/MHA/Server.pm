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

package MHA::Server;

use strict;
use warnings FATAL => 'all';
use Carp qw(croak);
use Time::HiRes qw( sleep );
use English qw(-no_match_vars);
use MHA::ManagerConst;
use MHA::HealthCheck;
use MHA::ManagerUtil;
use MHA::DBHelper;

sub new {
  my $pkg = shift;
  bless { logger => undef, }, $pkg;
}

sub set_logger {
  my $self   = shift;
  my $logger = shift;
  $self->{logger} = $logger;
}

sub version_ge($$) {
  my $self       = shift;
  my $compare    = shift;
  my $my_version = $self->{mysql_version};
  return MHA::NodeUtil::mysql_version_ge( $my_version, $compare );
}

sub server_equals {
  my ( $self, $host, $ip, $port ) = @_;
  if ( $self->{ip} eq $ip
    && $self->{port} == $port
    && $self->{hostname} eq $host )
  {
    return 1;
  }
  return 0;
}

sub get_hostinfo($) {
  my $self = shift;
  return "$self->{hostname}($self->{ip}:$self->{port})";
}

sub check_slave_status($) {
  my $self     = shift;
  my $dbhelper = $self->{dbhelper};
  return $dbhelper->check_slave_status();
}

sub disable_read_only($) {
  my $self     = shift;
  my $dbhelper = $self->{dbhelper};
  $dbhelper->disable_read_only();
  $self->{read_only} = $dbhelper->is_read_only();
  if ( $self->{read_only} ) {
    return 1;
  }
  else {
    return 0;
  }
}

sub get_failover_advisory_lock($) {
  my $self = shift;
  my $dbh  = $self->{dbh};
  return MHA::SlaveUtil::get_failover_advisory_lock( $dbh, 10 );
}

sub release_failover_advisory_lock($) {
  my $self = shift;
  my $dbh  = $self->{dbh};
  return MHA::SlaveUtil::release_failover_advisory_lock($dbh);
}

sub get_monitor_advisory_lock($) {
  my $self = shift;
  my $dbh  = $self->{dbh};
  return MHA::SlaveUtil::get_monitor_advisory_lock( $dbh, 1 );
}

sub release_monitor_advisory_lock($) {
  my $self = shift;
  my $dbh  = $self->{dbh};
  return MHA::SlaveUtil::release_monitor_advisory_lock($dbh);
}

sub enable_read_only($) {
  my $self     = shift;
  my $dbhelper = $self->{dbhelper};
  $dbhelper->enable_read_only();
  $self->{read_only} = $dbhelper->is_read_only();
  if ( $self->{read_only} ) {
    return 0;
  }
  else {
    return 1;
  }
}

sub get_running_update_threads {
  my ( $self, $arg ) = @_;
  my $dbhelper = $self->{dbhelper};
  return $dbhelper->get_running_update_threads($arg);
}

sub connect_check {
  my ( $self, $num_retries, $log ) = @_;

  my $dbhelper = new MHA::DBHelper();
  my $dbh =
    $dbhelper->connect( $self->{ip}, $self->{port}, $self->{user},
    $self->{password}, 0, $num_retries );
  if ( !defined($dbh) ) {
    my $mysql_err = DBI->err;
    my $msg       = sprintf( "Got MySQL error when connecting %s :$mysql_err:",
      $self->get_hostinfo() );
    $msg .= "$DBI::errstr" if ($DBI::errstr);
    $log->debug($msg) if ($log);
    if ( $mysql_err
      && grep ( $_ == $mysql_err, @MHA::ManagerConst::ALIVE_ERROR_CODES ) > 0 )
    {
      $msg .= ", but this is not mysql crash. Check MySQL server settings.";
      if ($log) {
        $log->error($msg);
        croak;
      }
      else {
        croak("$msg\n");
      }
    }
    $self->{dead} = 1;
    return $MHA::ManagerConst::MYSQL_DEAD_RC;
  }
  $self->{dbhelper} = $dbhelper;
  $self->{dbh}      = $dbh;
  return 0;
}

# Failed to connect does not result in script die, because it is sometimes expected.
# Configuration error results in script die, because it should not happen if correctly configured.
sub connect_and_get_status {
  my ( $self, $log ) = @_;
  $log = $self->{logger} unless ($log);
  if ( $self->connect_check( 5, $log ) == $MHA::ManagerConst::MYSQL_DEAD_RC ) {
    return;
  }
  my $dbhelper = $self->{dbhelper};
  my $dbh      = $self->{dbh};
  $self->{dead} = 0;
  $log->debug(
    sprintf(
      " Connected to: %s, user=%s\n",
      $self->get_hostinfo(), $self->{user}
    )
  );
  $dbhelper->set_long_wait_timeout();
  my ( $sstatus, $mip, $mport, $read_only, $relay_purge ) = ();
  $self->{server_id}     = $dbhelper->get_server_id();
  $self->{mysql_version} = $dbhelper->get_version();
  $self->{log_bin}       = $dbhelper->is_binlog_enabled();

  #if log-bin is enabled, check binlog filtering rules on all servers
  if ( $self->{log_bin} ) {
    my ( $file, $pos, $binlog_do_db, $binlog_ignore_db ) =
      $dbhelper->show_master_status();
    $self->{File}             = $file;
    $self->{Binlog_Do_DB}     = $binlog_do_db;
    $self->{Binlog_Ignore_DB} = $binlog_ignore_db;
  }

  $self->{relay_log_info_type} =
    $dbhelper->get_relay_log_info_type( $self->{mysql_version} );
  if ( $self->{relay_log_info_type} eq "TABLE" ) {
    my ( $relay_dir, $current_relay_log ) =
      MHA::SlaveUtil::get_relay_dir_file_from_table($dbh);
    $self->{relay_dir}         = $relay_dir;
    $self->{current_relay_log} = $current_relay_log;
    if ( !$relay_dir || !$current_relay_log ) {
      $log->error(
        sprintf(
" Getting relay log directory or current relay logfile from replication table failed on %s!",
          $self->get_hostinfo() )
      );
      croak;
    }
  }
  else {
    my $relay_log_info =
      $dbhelper->get_relay_log_info_path( $self->{mysql_version} );
    $self->{relay_log_info} = $relay_log_info;

    unless ($relay_log_info) {
      $log->error(
        sprintf( " Getting relay_log_info failed on %s!",
          $self->get_hostinfo() )
      );
      croak;
    }
  }

  my %status = $dbhelper->check_slave_status();
  $read_only   = $dbhelper->is_read_only();
  $relay_purge = $dbhelper->is_relay_log_purge();
  $sstatus     = $status{Status};
  if ( $sstatus == 1 ) {

    # I am not a slave
    $self->{not_slave} = 1;
  }
  elsif ($sstatus) {
    $log->error(
      sprintf(
        "Checking slave status failed on %s. err= %s",
        $self->get_hostinfo(), $status{Errstr}
      )
    );
    croak;
  }
  else {
    $self->{read_only}   = $read_only;
    $self->{relay_purge} = $relay_purge;
    my $master_bin_addr = gethostbyname( $status{Master_Host} );
    my $master_ip = sprintf( "%vd", $master_bin_addr ) if ($master_bin_addr);
    unless ($master_ip) {
      $log->error(
        sprintf(
" Failed to get an IP address of %s! %s replicates from %s, but maybe invalid.",
          $status{Master_Host}, $self->get_hostinfo(), $status{Master_Host}
        )
      );
      croak;
    }
    $self->{Master_IP}   = $master_ip;
    $self->{Master_Port} = $status{Master_Port};
    $self->{not_slave}   = 0;
    $self->{Master_Host} = $status{Master_Host};
    $self->{repl_user}   = $status{Master_User} unless ( $self->{repl_user} );

# Master_Host is ip address when you use ip address to connect. In this case, you should use ip address to change master.
    if ( $self->{Master_Host} eq $self->{Master_IP} ) {
      $self->{use_ip_for_change_master} = 1;
    }
    $self->{Replicate_Do_DB}             = $status{Replicate_Do_DB};
    $self->{Replicate_Ignore_DB}         = $status{Replicate_Ignore_DB};
    $self->{Replicate_Do_Table}          = $status{Replicate_Do_Table};
    $self->{Replicate_Ignore_Table}      = $status{Replicate_Ignore_Table};
    $self->{Replicate_Wild_Do_Table}     = $status{Replicate_Wild_Do_Table};
    $self->{Replicate_Wild_Ignore_Table} = $status{Replicate_Wild_Ignore_Table};
  }
  return $self;
}

sub check_set_ssh_status {
  my $self     = shift;
  my $log      = shift;
  my $set_dead = shift;
  if ( !$self->{dead} ) {
    if (
      MHA::HealthCheck::ssh_check($self)
      || MHA::ManagerUtil::check_node_version_nodie(
        $log, $self->{ssh_user}, $self->{hostname}, $self->{ip}
      )
      )
    {
      $self->{ssh_ok} = 0;
      $self->{dead} = 1 if ($set_dead);
    }
    else {
      $self->{ssh_ok} = 1;
    }
  }
}

sub check_repl_priv {
  my ( $self, $log ) = @_;
  $log = $self->{logger} unless ($log);
  if ( !$self->{no_master} && $self->{log_bin} && !$self->{not_slave} ) {
    my $dbhelper = $self->{dbhelper};
    unless ( $dbhelper->has_repl_priv( $self->{repl_user} ) ) {
      $log->error(
        sprintf(
"%s: User %s does not exist or does not have REPLICATION SLAVE privilege! Other slaves can not start replication from this host.",
          $self->get_hostinfo(), $self->{repl_user}
        )
      );
      croak;
    }
  }
}

sub get_and_set_high_max_allowed_packet {
  my ( $self, $log ) = @_;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  $self->{orig_max_allowed_packet} = $dbhelper->get_max_allowed_packet();
  $log->debug(
    "Current max_allowed_packet is $self->{orig_max_allowed_packet}.");
  if ( $dbhelper->set_max_allowed_packet_1g() ) {
    $log->warning("Tentatively setting max_allowed_packet to 1GB failed.");
    return 1;
  }
  else {
    $log->debug("Tentatively setting max_allowed_packet to 1GB succeeded.");
    return 0;
  }
}

sub set_default_max_allowed_packet {
  my ( $self, $log ) = @_;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  if ( $dbhelper->set_max_allowed_packet( $self->{orig_max_allowed_packet} ) ) {
    $log->warning(
"Setting max_allowed_packet back to $self->{orig_max_allowed_packet} failed."
    );
    return 1;
  }
  else {
    $log->debug(
"Setting max_allowed_packet back to $self->{orig_max_allowed_packet} succeeded."
    );
    return 0;
  }
}

sub disable_relay_log_purge {
  my ( $self, $log ) = @_;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  $dbhelper->disable_relay_log_purge();
  $log->debug("Explicitly disabled relay_log_purge.");
  return 0;
}

sub current_slave_position {
  my ( $self, $log ) = @_;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  my %status   = $dbhelper->check_slave_status();
  if ( $status{Status} ) {
    $log->error( "checking slave status failed. err= " . $status{Errstr} );
    return;
  }
  $self->{Master_Log_File}       = $status{Master_Log_File};
  $self->{Read_Master_Log_Pos}   = $status{Read_Master_Log_Pos};
  $self->{Relay_Master_Log_File} = $status{Relay_Master_Log_File};
  $self->{Exec_Master_Log_Pos}   = $status{Exec_Master_Log_Pos};
  $self->{Master_Log_File}       = $status{Master_Log_File};
  $self->{Relay_Log_File}        = $status{Relay_Log_File};
  $self->{Relay_Log_Pos}         = $status{Relay_Log_Pos};
  return $self;
}

#Check whether slave is running and not delayed
sub has_replication_problem {
  my $self                = shift;
  my $allow_delay_seconds = shift;
  $allow_delay_seconds = 1 unless ($allow_delay_seconds);
  my $log      = $self->{logger};
  my $dbhelper = $self->{dbhelper};
  my %status   = $dbhelper->check_slave_status();
  if ( $status{Status} ne '0' ) {
    $log->error(
      sprintf( "Getting slave status failed on %s", $self->get_hostinfo() ) );
    return 1;
  }
  elsif ( $status{Slave_IO_Running} ne "Yes" ) {
    $log->error(
      sprintf( "Slave IO thread is not running on %s", $self->get_hostinfo() )
    );
    return 2;
  }
  elsif ( $status{Slave_SQL_Running} ne "Yes" ) {
    $log->error(
      sprintf( "Slave SQL thread is not running on %s", $self->get_hostinfo() )
    );
    return 3;
  }
  elsif ( $status{Seconds_Behind_Master}
    && $status{Seconds_Behind_Master} > $allow_delay_seconds )
  {
    $log->error(
      sprintf(
        "Slave is currently behind %d seconds on %s",
        $status{Seconds_Behind_Master},
        $self->get_hostinfo()
      )
    );
    return 4;
  }
  elsif ( !defined( $status{Seconds_Behind_Master} ) ) {
    $log->error(
      sprintf( "Failed to get Seconds_Behind_Master on %s",
        $self->get_hostinfo() )
    );
    return 5;
  }
  return 0;
}

sub get_num_running_update_threads($$) {
  my $self     = shift;
  my $mode     = shift;
  my $dbhelper = $self->{dbhelper};
  $dbhelper->get_num_running_update_threads($mode);
}

sub wait_until_relay_log_applied {
  my $self = shift;
  my $log  = shift;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  my %status   = $dbhelper->wait_until_relay_log_applied();
  if ( $status{Status} ) {
    $log->error("Got error: $status{Errstr}");
  }
  return $status{Status};
}

sub get_binlog_position($) {
  my $self     = shift;
  my $dbhelper = $self->{dbhelper};
  return $dbhelper->show_master_status();
}

sub check_binlog_stop {
  my $self = shift;
  my $log  = $self->{logger};
  $log->info("Checking binlog writes are stopped or not..");
  my ( $file, $pos ) = $self->get_binlog_position();
  sleep 1;
  my ( $file2, $pos2 ) = $self->get_binlog_position();
  if ( ( $file2 ne $file ) || ( $pos != $pos2 ) ) {
    $log->error(
"Binlog is not stopped! Prev binlog file:pos=$file:$pos, post binlog file:pos=$file2:$pos2."
    );
    return;
  }
  else {
    $log->info(" ok.");
  }
  return ( $file, $pos );
}

sub disconnect($) {
  my $self = shift;
  my $log  = $self->{logger};
  if ( defined( $self->{dbh} ) ) {
    $self->{dbh}->disconnect();
    $self->{dbh} = undef;
    $log->debug( sprintf( " Disconnected from %s\n", $self->get_hostinfo() ) );
  }
  else {
    $log->debug(
      sprintf( " Already disconnected from %s\n", $self->get_hostinfo() ) );
  }
}

sub reconnect($) {
  my $self      = shift;
  my $conn_lost = 0;
  eval {
    if ( !$self->{dbh}->ping() )
    {
      die;
    }
  };
  if ($@) {
    undef $@;
    $conn_lost = 1;
  }
  return $self if ( !$conn_lost );

  my $dbh =
    $self->{dbhelper}
    ->connect( $self->{ip}, $self->{port}, $self->{user}, $self->{password} );
  if ( !defined($dbh) ) {
    $self->{dead} = 1;
    return;
  }
  $self->{dbhelper}->set_long_wait_timeout();
  $self->{dbh} = $dbh;
  return $self;
}

sub flush_tables($) {
  my $self     = shift;
  my $dbhelper = $self->{dbhelper};
  my $log      = $self->{logger};
  my $errstr;
  $log->info(
    "Executing FLUSH NO_WRITE_TO_BINLOG TABLES. This may take long time..");
  if ( $errstr = $dbhelper->flush_tables_nolog() ) {
    $log->error( " Failed! " . $errstr );
  }
  else {
    $log->info(" ok.");
  }
}

sub lock_all_tables($) {
  my $self     = shift;
  my $dbhelper = $self->{dbhelper};
  my $log      = $self->{logger};
  my $errstr;
  $log->info("Executing FLUSH TABLES WITH READ LOCK..");
  if ( $errstr = $dbhelper->flush_tables_with_read_lock() ) {
    $log->error( " Failed! " . $errstr );
    return 1;
  }
  else {
    $log->info(" ok.");
  }
  return 0;
}

sub unlock_tables($) {
  my $self     = shift;
  my $dbhelper = $self->{dbhelper};
  my $log      = $self->{logger};
  my $errstr;
  $log->info("Executing UNLOCK TABLES..");
  if ( $errstr = $dbhelper->unlock_tables() ) {
    $log->error( "Failed! " . $errstr );
    return 1;
  }
  else {
    $log->info(" ok.");
  }
  return 0;
}

# Let the server to return nothing at SHOW SLAVE STATUS (Without this, the new master still points to the previous master)
sub reset_slave_info($) {
  my $self     = shift;
  my $log      = $self->{logger};
  my $dbhelper = $self->{dbhelper};
  $log->debug(" Clearing slave info..");
  if ( $self->stop_slave() ) {
    $log->error(" Stopping slave failed!");
    return 1;
  }
  $dbhelper->reset_slave();

  # Obsolete. Throws error on 5.5+
  #  $dbhelper->reset_slave_master_host();
  my %status = $dbhelper->check_slave_status();
  if ( $status{Status} == 1 ) {
    $log->debug(
" SHOW SLAVE STATUS shows new master does not replicate from anywhere. OK."
    );
    return 0;
  }
  else {
    $log->error(
" SHOW SLAVE STATUS shows new master replicates from somewhere. Check for details!"
    );
    return 1;
  }
}

sub reset_slave_on_new_master($) {
  my $self     = shift;
  my $dbhelper = $self->{dbhelper};
  my $log      = $self->{logger};
  my $ret      = $self->reset_slave_info();
  if ($ret) {
    my $message = " $self->{hostname}: Resetting slave info failed.";
    $log->error($message);
    return 1;
  }
  else {
    my $message = " $self->{hostname}: Resetting slave info succeeded.";
    $log->debug($message);
    return 0;
  }
}

# It is possible that slave io thread has not started or established yet
# when you execute "START SLAVE". It should start within 0-4 seconds.
# So we wait some time until slave starts.
# Return: 0: OK  1: NG
sub wait_until_slave_starts($$) {
  my $self        = shift;
  my $type        = shift;
  my $log         = $self->{logger};
  my $dbhelper    = $self->{dbhelper};
  my $retry_count = 100;
  for ( my $i = 0 ; $i < $retry_count ; $i++ ) {
    my %status = $dbhelper->check_slave_status();
    if ( $status{Status} ) {
      $log->error(
        sprintf(
          "Checking slave status failed on %s. err=%s",
          $self->get_hostinfo(), $status{Errstr}
        )
      );
      return 1;
    }
    if ( $type eq "IO" ) {
      return 0 if ( $status{Slave_IO_Running} eq "Yes" );
    }
    elsif ( $type eq "SQL" ) {
      return 0 if ( $status{Slave_SQL_Running} eq "Yes" );
    }
    else {
      return 0
        if ( $status{Slave_IO_Running} eq "Yes"
        && $status{Slave_SQL_Running} eq "Yes" );
    }

    if ( $status{Slave_SQL_Running} eq "No" && $status{Last_Errno} ne '0' ) {
      $log->error(
        sprintf( "SQL Thread could not be started on %s! Check slave status.",
          $self->get_hostinfo() )
      );
      $log->error(
        sprintf(
          " Last Error= %d, Last Error=%s",
          $status{Last_Errno}, $status{Last_Error}
        )
      );
      return 1;
    }
    sleep(0.1);
  }
  $log->error(
    sprintf( "Slave could not be started on %s! Check slave status.",
      $self->get_hostinfo() )
  );
  return 1;
}

sub wait_until_slave_stops {
  my $self = shift;
  my $type = shift;
  my $log  = shift;
  $log = $self->{logger} unless ($log);
  my $dbhelper    = $self->{dbhelper};
  my $retry_count = 100;
  for ( my $i = 0 ; $i < $retry_count ; $i++ ) {
    my %status = $dbhelper->check_slave_status();
    if ( $status{Status} ) {
      $log->error(
        sprintf(
          "Checking slave status failed on %s. err=%s",
          $self->get_hostinfo(), $status{Errstr}
        )
      );
      return 1;
    }
    if ( $type eq "IO" ) {
      return 0 if ( $status{Slave_IO_Running} eq "No" );
    }
    elsif ( $type eq "SQL" ) {
      return 0 if ( $status{Slave_SQL_Running} eq "No" );
    }
    else {
      return 0
        if ( $status{Slave_IO_Running} eq "No"
        && $status{Slave_SQL_Running} eq "No" );
    }
    sleep(0.1);
  }
  $log->error(
    sprintf( "Slave could not be stopped on %s! Check slave status.",
      $self->get_hostinfo() )
  );
  return 1;
}

sub stop_slave {
  my $self = shift;
  my $log  = shift;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  my ($sstatus) = ();
  $log->debug(
    sprintf( " Stopping slave IO/SQL thread on %s..", $self->get_hostinfo() ) );
  $dbhelper->stop_slave();
  if ( $self->wait_until_slave_stops( 'ALL', $log ) ) {
    $log->error(
      sprintf( "Stopping slave IO/SQL thread on %s Failed!",
        $self->get_hostinfo() )
    );
    return 1;
  }
  $log->debug("  done.");
  return 0;
}

sub start_slave {
  my $self = shift;
  my $log  = shift;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  my ($sstatus) = ();
  $log->debug(
    sprintf( " Starting slave IO/SQL thread on %s..", $self->get_hostinfo() ) );
  $dbhelper->start_slave();
  if ( $self->wait_until_slave_starts( 'ALL', $log ) ) {
    $log->error(
      sprintf( "Starting slave IO/SQL thread on %s failed!",
        $self->get_hostinfo() )
    );
    return 1;
  }
  $log->debug("  done.");
  return 0;
}

sub stop_io_thread {
  my $self = shift;
  my $log  = shift;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  my ($sstatus) = ();
  $log->debug(
    sprintf( " Stopping IO thread on %s..", $self->get_hostinfo() ) );
  $dbhelper->stop_io_thread();
  if ( $self->wait_until_slave_stops( 'IO', $log ) ) {
    $log->error(
      sprintf( "Failed to stop IO thread on %s!", $self->get_hostinfo() ) );
    return 1;
  }
  $log->debug(
    sprintf( " Stop IO thread on %s done.", $self->get_hostinfo() ) );
  return 0;
}

sub stop_sql_thread {
  my $self = shift;
  my $log  = shift;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  my ($sstatus) = ();
  $log->debug(
    sprintf( " Stopping SQL thread on %s..", $self->get_hostinfo() ) );
  $dbhelper->stop_sql_thread();
  if ( $self->wait_until_slave_stops( 'SQL', $log ) ) {
    $log->error(
      sprintf( "Stopping SQL thread on %s failed!", $self->get_hostinfo() ) );
    return 1;
  }
  $log->debug("  done.");
  return 0;
}

sub is_sql_thread_error {
  my $self = shift;
  my $log  = shift;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  my %status   = $dbhelper->check_slave_status();
  if ( $status{Status} ) {
    $log->error(
      sprintf(
        "Checking slave status failed on %s. err=%s",
        $self->get_hostinfo(), $status{Errstr}
      )
    );
    return 1;
  }
  return 0 if ( $status{Slave_SQL_Running} eq "Yes" );
  if ( $status{Slave_SQL_Running} eq "No" && $status{Last_Errno} eq '0' ) {
    $log->warning(
      sprintf( "SQL Thread is stopped(no error) on %s", $self->get_hostinfo() )
    );
    return 0;
  }
  $log->error(
    sprintf(
      "SQL Thread is stopped(error) on %s! Errno:%s, Error:%s",
      $self->get_hostinfo(), $status{Last_Errno}, $status{Last_Error}
    )
  );
  return 1;
}

sub start_sql_thread_if {
  my $self = shift;
  my $log  = shift;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  my %status   = $dbhelper->check_slave_status();
  if ( $status{Status} ) {
    $log->error(
      sprintf(
        "Checking slave status failed on %s. err=%s",
        $self->get_hostinfo(), $status{Errstr}
      )
    );
    return 1;
  }
  return 0 if ( $status{Slave_SQL_Running} eq "Yes" );
  $log->info(
    sprintf( " Starting SQL thread on %s ..", $self->get_hostinfo() ) );
  $dbhelper->start_sql_thread();
  if ( $self->wait_until_slave_starts( 'SQL', $log ) ) {
    $log->info("  Failed!");
    return 1;
  }
  $log->info("  done.");
  return 0;
}

sub master_pos_wait_internal {
  my ( $self, $binlog_file, $binlog_pos, $log ) = @_;
  $log = $self->{logger} unless ($log);
  my $dbhelper = $self->{dbhelper};
  my $res = $dbhelper->master_pos_wait( $binlog_file, $binlog_pos );
  if ( !defined($res) ) {
    $log->error(
      sprintf(
"master_pos_wait(%s:%d) returned NULL on %s. Maybe SQL thread was aborted?",
        $binlog_file, $binlog_pos, $self->get_hostinfo()
      )
    );
    return 1;
  }
  if ( $res >= 0 ) {
    $log->info(
      sprintf(
        " master_pos_wait(%s:%d) completed on %s. Executed %d events.",
        $binlog_file, $binlog_pos, $self->get_hostinfo(), $res
      )
    );
    return 0;
  }
  else {
    $log->error(
      sprintf(
        "master_pos_wait(%s:%d) got error on %s: $res",
        $binlog_file, $binlog_pos, $self->get_hostinfo()
      )
    );
    return 1;
  }
}

# We do not reset slave here
sub master_pos_wait {
  my ( $self, $binlog_file, $binlog_pos, $log ) = @_;
  $log = $self->{logger} unless ($log);

  $log->info(
    sprintf( " Waiting to execute all relay logs on %s..",
      $self->get_hostinfo() )
  );
  my $ret = $self->master_pos_wait_internal( $binlog_file, $binlog_pos, $log );
  if ($ret) {
    return $ret;
  }
  $log->info("  done.");
  $self->stop_sql_thread($log);
  return 0;
}

sub print_server {
  my $self    = shift;
  my $log     = $self->{logger};
  my $ssh_str = "";
  $ssh_str = " Not reachable via SSH"
    if ( defined( $self->{ssh_ok} ) && $self->{ssh_ok} == 0 );
  my $version_str = "";
  $version_str = "  Version=$self->{mysql_version}"
    if ( $self->{mysql_version} );
  $version_str = $version_str . " (oldest major version between slaves)"
    if ( defined( $self->{oldest_major_version} )
    && $self->{oldest_major_version} >= 1 );
  my $binlog_str = "";

  if ( defined( $self->{log_bin} ) ) {
    if ( $self->{log_bin} > 0 ) {
      $binlog_str = " log-bin:enabled";
    }
    else {
      $binlog_str = " log-bin:disabled";
    }
  }
  $log->info(
    "  " . $self->get_hostinfo() . $ssh_str . $version_str . $binlog_str );
  $log->debug("   Relay log info repository: $self->{relay_log_info_type}")
    if ( $self->{relay_log_info_type} );
  if ( $self->{Master_IP} && $self->{Master_Port} ) {
    $log->info(
      sprintf(
        "    Replicating from %s(%s:%d)",
        $self->{Master_Host}, $self->{Master_IP}, $self->{Master_Port}
      )
    );
    if ( $self->{no_master} ) {
      $log->info("    Not candidate for the new Master (no_master is set)");
    }
    elsif ( $self->{candidate_master} ) {
      $log->info(
        "    Primary candidate for the new Master (candidate_master is set)");
    }
  }
}

sub print_filter {
  my $self       = shift;
  my $is_master  = shift;
  my $print_repl = shift;
  $is_master = 0 unless ($is_master);
  $print_repl = 1 if ( !defined($print_repl) );
  my $str = "";
  $str .= "$self->{hostname}";
  $str .= " (current_master)" if ($is_master);
  $str .= " ($self->{node_label})" if ( $self->{node_label} );
  $str .= "\n";
  $str .= sprintf( "  Binlog_Do_DB: %s\n",
    $self->{Binlog_Do_DB} ? $self->{Binlog_Do_DB} : "" );
  $str .= sprintf( "  Binlog_Ignore_DB: %s\n",
    $self->{Binlog_Ignore_DB} ? $self->{Binlog_Ignore_DB} : "" );

  if ($print_repl) {
    $str .= sprintf( "  Replicate_Do_DB: %s\n",
      $self->{Replicate_Do_DB} ? $self->{Replicate_Do_DB} : "" );
    $str .= sprintf( "  Replicate_Ignore_DB: %s\n",
      $self->{Replicate_Ignore_DB} ? $self->{Replicate_Ignore_DB} : "" );
    $str .= sprintf( "  Replicate_Do_Table: %s\n",
      $self->{Replicate_Do_Table} ? $self->{Replicate_Do_Table} : "" );
    $str .= sprintf( "  Replicate_Ignore_Table: %s\n",
      $self->{Replicate_Ignore_Table} ? $self->{Replicate_Ignore_Table} : "" );
    $str .= sprintf( "  Replicate_Wild_Do_Table: %s\n",
        $self->{Replicate_Wild_Do_Table}
      ? $self->{Replicate_Wild_Do_Table}
      : "" );
    $str .= sprintf( "  Replicate_Wild_Ignore_Table: %s\n",
        $self->{Replicate_Wild_Ignore_Table}
      ? $self->{Replicate_Wild_Ignore_Table}
      : "" );
  }
  $str .= "\n";
  return $str;
}

1;
