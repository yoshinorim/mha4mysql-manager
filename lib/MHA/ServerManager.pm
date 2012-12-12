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

package MHA::ServerManager;

use strict;
use warnings FATAL => 'all';
use Carp qw(croak);
use English qw(-no_match_vars);
use MHA::SlaveUtil;
use MHA::DBHelper;
use MHA::Server;
use MHA::ManagerConst;
use Parallel::ForkManager;

sub new {
  my $class = shift;
  my $self  = {
    servers          => [],
    dead_servers     => [],
    alive_servers    => [],
    alive_slaves     => [],
    failed_slaves    => [],
    latest_slaves    => [],
    oldest_slaves    => [],
    unmanaged_slaves => [],
    orig_master      => undef,
    new_master       => undef,
    logger           => undef,
    @_,
  };
  return bless $self, $class;
}

sub set_servers($$) {
  my $self        = shift;
  my $servers_ref = shift;
  $self->{servers} = $servers_ref;
}

sub set_latest_slaves($$) {
  my $self        = shift;
  my $servers_ref = shift;
  $self->{latest_slaves} = $servers_ref;
}

sub set_oldest_slaves($$) {
  my $self        = shift;
  my $servers_ref = shift;
  $self->{oldest_slaves} = $servers_ref;
}

sub set_unmanaged_slaves($$) {
  my $self        = shift;
  my $servers_ref = shift;
  $self->{unmanaged_slaves} = $servers_ref;
}

sub get_servers($) {
  my $self = shift;
  return @{ $self->{servers} };
}

sub get_dead_servers($) {
  my $self = shift;
  return @{ $self->{dead_servers} };
}

sub get_alive_servers($) {
  my $self = shift;
  return @{ $self->{alive_servers} };
}

sub get_alive_slaves($) {
  my $self = shift;
  return @{ $self->{alive_slaves} };
}

sub get_failed_slaves($) {
  my $self = shift;
  return @{ $self->{failed_slaves} };
}

sub get_latest_slaves($) {
  my $self = shift;
  return @{ $self->{latest_slaves} };
}

sub get_oldest_slaves($) {
  my $self = shift;
  return @{ $self->{oldest_slaves} };
}

sub get_unmanaged_slaves($) {
  my $self = shift;
  return @{ $self->{unmanaged_slaves} };
}

sub add_dead_server($$) {
  my $self   = shift;
  my $server = shift;
  push @{ $self->{dead_servers} }, $server;
}

sub add_alive_server($$) {
  my $self   = shift;
  my $server = shift;
  push @{ $self->{alive_servers} }, $server;
}

sub add_alive_slave($$) {
  my $self   = shift;
  my $server = shift;
  push @{ $self->{alive_slaves} }, $server;
}

sub add_failed_slave($$) {
  my $self   = shift;
  my $server = shift;
  push @{ $self->{failed_slaves} }, $server;
}

sub add_unmanaged_slave($$) {
  my $self   = shift;
  my $server = shift;
  push @{ $self->{unmanaged_slaves} }, $server;
}

sub set_orig_master($$) {
  my $self   = shift;
  my $server = shift;
  $self->{orig_master}   = $server;
  $server->{orig_master} = 1;
}

sub get_orig_master($) {
  my $self = shift;
  return $self->{orig_master};
}

sub init_servers($) {
  my $self    = shift;
  my $log     = $self->{logger};
  my @servers = $self->get_servers();
  $self->{dead_servers}     = [];
  $self->{alive_servers}    = [];
  $self->{alive_slaves}     = [];
  $self->{failed_slaves}    = [];
  $self->{unmanaged_slaves} = [];
  foreach my $server (@servers) {

    if ( $server->{dead} ) {
      $self->add_dead_server($server);
    }
    elsif ( $server->{unmanaged} ) {
      $self->add_unmanaged_slave($server);
    }
    else {
      $self->add_alive_server($server);
      if ( $server->{not_slave} eq '0' && !$server->{orig_master} ) {
        if ( !$server->is_sql_thread_error() && !$server->{lack_relay_log} ) {
          $self->add_alive_slave($server);
        }
        else {
          $self->add_failed_slave($server);
        }
      }
    }
  }
  my @alive_servers = $self->get_alive_servers();
  if ( $#alive_servers <= -1 ) {
    $log->error("There is no alive server. We can't do failover");
    croak;
  }
  my @alive_slaves = $self->get_alive_slaves();
  if ( $#alive_slaves <= -1 ) {
    $log->error("There is no alive slave. We can't do failover");
    croak;
  }
}

sub set_logger($$) {
  my $self   = shift;
  my $logger = shift;
  $self->{logger} = $logger;
}

sub connect_all_and_read_server_status($$$$) {
  my $self             = shift;
  my $dead_master_host = shift;
  my $dead_master_ip   = shift;
  my $dead_master_port = shift;
  my $log              = $self->{logger};
  my @servers          = $self->get_servers();
  $log->debug("Connecting to servers..");

  my $should_die         = 0;
  my $connection_checker = new Parallel::ForkManager( $#servers + 1 );
  $connection_checker->run_on_start(
    sub {
      my ( $pid, $target ) = @_;
    }
  );
  $connection_checker->run_on_finish(
    sub {
      my ( $pid, $exit_code, $target ) = @_;
      if ( $exit_code == $MHA::ManagerConst::MYSQL_DEAD_RC ) {
        $target->{dead} = 1;
      }
      elsif ($exit_code) {
        $should_die = 1;
      }
    }
  );
  foreach my $target (@servers) {
    unless ( $target->{logger} ) {
      $target->{logger} = $log;
    }
    $connection_checker->start($target) and next;
    eval {
      $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";
      if ( $dead_master_host
        && $dead_master_ip
        && $dead_master_port )
      {
        if (
          $target->server_equals(
            $dead_master_host, $dead_master_ip, $dead_master_port
          )
          )
        {
          $connection_checker->finish($MHA::ManagerConst::MYSQL_DEAD_RC);
        }
      }
      my $rc = $target->connect_check(2);
      $connection_checker->finish($rc);
    };
    if ($@) {
      $log->error($@);
      undef $@;
      $connection_checker->finish(1);
    }
    $connection_checker->finish(0);
  }
  $connection_checker->wait_all_children;
  if ($should_die) {
    $log->error("Got fatal error, stopping operations");
    croak;
  }

  foreach my $target (@servers) {
    next if ( $target->{dead} );
    $target->connect_and_get_status();
  }
  $self->init_servers();
  $self->compare_slave_version();
  $log->debug("Connecting to servers done.");
  $self->validate_current_master();
}

sub get_oldest_version($) {
  my $self    = shift;
  my @servers = $self->get_alive_servers();
  my $oldest_version;
  foreach my $server (@servers) {
    if ( $server->{oldest_major_version} ) {
      $oldest_version = $server->{mysql_version};
      last;
    }
  }
  return $oldest_version;
}

sub compare_slave_version($) {
  my $self    = shift;
  my @servers = $self->get_alive_servers();
  my $log     = $self->{logger};
  $log->debug(" Comparing MySQL versions..");
  my $min_major_version;
  foreach (@servers) {
    my $dbhelper = $_->{dbhelper};
    next if ( $_->{dead} || $_->{not_slave} );
    my $parsed_major_version =
      MHA::NodeUtil::parse_mysql_major_version( $_->{mysql_version} );
    if (!$min_major_version
      || $parsed_major_version < $min_major_version )
    {
      $min_major_version = $parsed_major_version;
    }
  }
  foreach (@servers) {
    my $dbhelper = $_->{dbhelper};
    next if ( $_->{dead} || $_->{not_slave} );
    my $parsed_major_version =
      MHA::NodeUtil::parse_mysql_major_version( $_->{mysql_version} );
    if ( $min_major_version == $parsed_major_version ) {
      $_->{oldest_major_version} = 1;
    }
    else {
      $_->{oldest_major_version} = 0;
    }
  }
  $log->debug("  Comparing MySQL versions done.");
}

sub print_filter_rules($$) {
  my $self   = shift;
  my $master = shift;
  my $log    = $self->{logger};
  my $msg    = "Bad Binlog/Replication filtering rules:\n";
  $msg .= $master->print_filter(1) if ( $master && !$master->{dead} );
  my @slaves = $self->get_alive_slaves();
  foreach my $slave (@slaves) {
    $msg .= $slave->print_filter();
  }
  $log->warning($msg);
}

sub validate_repl_filter($$) {
  my $self   = shift;
  my $master = shift;
  my $log    = $self->{logger};

  $log->info("Checking replication filtering settings..");

  my $binlog_do_db;
  my $binlog_ignore_db;

  # If master is alive
  if ( $master && !$master->{dead} ) {
    $binlog_do_db     = $master->{Binlog_Do_DB};
    $binlog_ignore_db = $master->{Binlog_Ignore_DB};
    $log->info(
      " binlog_do_db= $binlog_do_db, binlog_ignore_db= $binlog_ignore_db");
  }

  my @slaves = $self->get_alive_slaves();
  my $replicate_do_db;
  my $replicate_ignore_db;
  my $replicate_do_table;
  my $replicate_ignore_table;
  my $replicate_wild_do_table;
  my $replicate_wild_ignore_table;
  foreach (@slaves) {
    $replicate_do_db = $_->{Replicate_Do_DB} unless ($replicate_do_db);
    $replicate_ignore_db = $_->{Replicate_Ignore_DB}
      unless ($replicate_ignore_db);
    $replicate_do_table = $_->{Replicate_Do_Table} unless ($replicate_do_table);
    $replicate_ignore_table = $_->{Replicate_Ignore_Table}
      unless ($replicate_ignore_table);
    $replicate_wild_do_table = $_->{Replicate_Wild_Do_Table}
      unless ($replicate_wild_do_table);
    $replicate_wild_ignore_table = $_->{Replicate_Wild_Ignore_Table}
      unless ($replicate_wild_ignore_table);
    if ( $_->{log_bin} ) {
      $binlog_do_db     = $_->{Binlog_Do_DB}     unless ($binlog_do_db);
      $binlog_ignore_db = $_->{Binlog_Ignore_DB} unless ($binlog_ignore_db);
    }
    if ( $replicate_do_db ne $_->{Replicate_Do_DB}
      || $replicate_ignore_db ne $_->{Replicate_Ignore_DB}
      || $replicate_do_table ne $_->{Replicate_Do_Table}
      || $replicate_ignore_table ne $_->{Replicate_Ignore_Table}
      || $replicate_wild_do_table ne $_->{Replicate_Wild_Do_Table}
      || $replicate_wild_ignore_table ne $_->{Replicate_Wild_Ignore_Table} )
    {
      $log->error(
        sprintf(
"Replication filtering check failed on %s! All slaves must have same replication filtering rules. Check SHOW SLAVE STATUS output and set my.cnf correctly.",
          $_->get_hostinfo() )
      );
      $self->print_filter_rules($master);
      return 1;
    }
    if ( $_->{log_bin} ) {
      if ( $binlog_do_db ne $_->{Binlog_Do_DB}
        || $binlog_ignore_db ne $_->{Binlog_Ignore_DB} )
      {
        $log->error(
          sprintf(
"Binlog filtering check failed on %s! All log-bin enabled servers must have same binlog filtering rules (same binlog-do-db and binlog-ignore-db). Check SHOW MASTER STATUS output and set my.cnf correctly.",
            $_->get_hostinfo() )
        );
        $self->print_filter_rules($master);
        return 1;
      }
    }
  }
  if ( $binlog_do_db && $replicate_do_db ) {
    if ( $binlog_do_db ne $replicate_do_db ) {
      $log->error(
        sprintf(
"binlog_do_db on master(%s) must be the same as replicate_do_db on slaves(%s).",
          $binlog_do_db, $replicate_do_db
        )
      );
      $self->print_filter_rules($master);
      return 1;
    }
  }
  if ( $binlog_ignore_db && $replicate_ignore_db ) {
    if ( $binlog_ignore_db ne $replicate_ignore_db ) {
      $log->error(
        sprintf(
"binlog_ignore_db on master(%s) must be the same as replicate_ignore_db on slaves(%s).",
          $binlog_ignore_db, $replicate_ignore_db
        )
      );
      $self->print_filter_rules($master);
      return 1;
    }
  }
  $log->info(" Replication filtering check ok.");
  return 0;
}

sub validate_num_alive_servers($$$) {
  my $self              = shift;
  my $current_master    = shift;
  my $ignore_fail_check = shift;
  my $log               = $self->{logger};
  my @dead_servers      = $self->get_dead_servers();
  my @failed_slaves     = $self->get_failed_slaves();

  foreach (@dead_servers) {
    next if ( $_->{id} eq $current_master->{id} );
    next if ( $ignore_fail_check && $_->{ignore_fail} );
    $log->error(
      sprintf( " Server %s is dead, but must be alive! Check server settings.",
        $_->get_hostinfo() )
    );
    croak;
  }
  foreach (@failed_slaves) {
    next if ( $ignore_fail_check && $_->{ignore_fail} );
    $log->error(
      sprintf( " Replication on %s fails! Check server settings.",
        $_->get_hostinfo() )
    );
    croak;
  }

  return 0;
}

# Check the following
# 1. All slaves are read_only (INFO)
# 2. All slaves see the same master ip/port (ERROR)
# 3. All slaves set relay_log_purge=0 (WARN)
# 4. All slaves have same replication filter rules with a master (ERROR)
# return 0: ok, others: NG
sub validate_slaves($$$) {
  my $self              = shift;
  my $check_repl_filter = shift;
  my $master            = shift;
  my $log               = $self->{logger};
  my @slaves            = $self->get_alive_slaves();
  my ( $mip, $mport ) = ();
  my $error = 0;
  $log->info("Checking slave configurations..");

  foreach (@slaves) {
    if ( $_->{read_only} ne '1' ) {
      $log->info(
        sprintf( " read_only=1 is not set on slave %s.\n", $_->get_hostinfo() )
      );
    }
    if ( $_->{relay_purge} ne '0' ) {
      $log->warning(
        sprintf( " relay_log_purge=0 is not set on slave %s.\n",
          $_->get_hostinfo() )
      );
    }
    if ( $_->{log_bin} eq '0' ) {
      $log->warning(
        sprintf(
          " log-bin is not set on slave %s. This host can not be a master.\n",
          $_->get_hostinfo() )
      );
    }
  }
  $error = $self->validate_repl_filter($master)
    if ($check_repl_filter);
  return $error;
}

sub get_alive_server_by_ipport {
  my $self = shift;
  my $ip   = shift;
  my $port = shift;
  $self->get_server_by_ipport( $ip, $port, 1 );
}

sub get_server_by_ipport {
  my $self       = shift;
  my $ip         = shift;
  my $port       = shift;
  my $alive_only = shift;
  my @servers;
  if ($alive_only) {
    @servers = $self->get_alive_servers();
  }
  else {
    @servers = $self->get_servers();
  }
  foreach (@servers) {
    if ( $_->{ip} eq $ip && $_->{port} == $port ) {
      return $_;
    }
  }
  return;
}

sub get_alive_server_by_hostport {
  my $self    = shift;
  my $host    = shift;
  my $port    = shift;
  my @servers = $self->get_alive_servers();
  foreach (@servers) {
    if ( $_->{hostname} eq $host && $_->{port} == $port ) {
      return $_;
    }
  }
  return;
}

sub get_server_from_by_id {
  my $self        = shift;
  my $servers_ref = shift;
  my $id          = shift;
  my @servers     = @$servers_ref;
  foreach (@servers) {
    if ( $_->{id} eq $id ) {
      return $_;
    }
  }
  return;
}

sub get_alive_server_by_id {
  my $self          = shift;
  my $id            = shift;
  my @alive_servers = $self->get_alive_servers();
  foreach (@alive_servers) {
    if ( $_->{id} eq $id ) {
      return $_;
    }
  }
  return;
}

sub get_alive_slave_by_id {
  my $self         = shift;
  my $id           = shift;
  my @alive_slaves = $self->get_alive_slaves();
  foreach (@alive_slaves) {
    if ( $_->{id} eq $id ) {
      return $_;
    }
  }
  return;
}

sub get_master_by_slave {
  my $self  = shift;
  my $slave = shift;
  return $self->get_server_by_ipport( $slave->{Master_IP},
    $slave->{Master_Port} );
}

sub validate_current_master($) {
  my $self          = shift;
  my $log           = $self->{logger};
  my @alive_servers = $self->get_alive_servers();
  my %master_hash;
  my $num_slaves        = 0;
  my $not_slave_servers = 0;
  foreach (@alive_servers) {
    if ( $_->{not_slave} eq '0' ) {
      $master_hash{"$_->{Master_IP}:$_->{Master_Port}"} = $_;
      $num_slaves++;
    }
    else {
      $not_slave_servers++;
    }
  }

  if ( $not_slave_servers >= 2 ) {
    $log->error(
"There are $not_slave_servers non-slave servers! MHA manages at most one non-slave server. Check configurations."
    );
    croak;
  }

  if ( $num_slaves < 1 ) {
    $log->error(
      "There is not any alive slave! Check slave settings for details.");
    croak;
  }

  # verify masters exist in a config file
  my $master;
  foreach my $key ( keys(%master_hash) ) {
    my $slave = $master_hash{$key};
    $master = $self->get_master_by_slave($slave);
    unless ($master) {
      $log->error(
        sprintf(
"Master %s:%d from which slave %s replicates is not defined in the configuration file!",
          $slave->{Master_IP}, $slave->{Master_Port},
          $slave->get_hostinfo()
        )
      );
      croak;
    }
  }

  my $real_master;
  if ( keys(%master_hash) >= 2 ) {
    $real_master = $self->get_primary_master( \%master_hash );
  }
  else {
    $real_master = $master;
    $self->set_orig_master($real_master);
  }
  $self->validate_master_ip_port($real_master);
  return $real_master;
}

sub validate_master_ip_port {
  my $self                 = shift;
  my $real_master          = shift;
  my $log                  = $self->{logger};
  my $has_unmanaged_slaves = 0;
  my @alive_servers        = $self->get_alive_servers();
  foreach my $slave (@alive_servers) {
    next if ( $slave->{id} eq $real_master->{id} );
    unless ( $self->get_alive_slave_by_id( $slave->{id} ) ) {
      $log->error(
        sprintf( "Server %s is alive, but does not work as a slave!",
          $slave->get_hostinfo() )
      );
      croak;
    }
    if (
      !(
           ( $slave->{Master_IP} eq $real_master->{ip} )
        && ( $slave->{Master_Port} == $real_master->{port} )
      )
      )
    {
      if ( $slave->{multi_tier_slave} ) {
        $slave->{unmanaged} = 1;
        $has_unmanaged_slaves = 1;
      }
      else {
        my $msg = sprintf(
          "Slave %s replicates from %s:%d, but real master is %s!",
          $slave->get_hostinfo(), $slave->{Master_Host},
          $slave->{Master_Port},  $real_master->get_hostinfo()
        );
        $log->error($msg);
        croak;
      }
    }
  }
  if ($has_unmanaged_slaves) {
    $self->init_servers();
  }
}

sub get_multi_master_print_info {
  my $self            = shift;
  my $master_hash_ref = shift;
  my %master_hash     = %$master_hash_ref;
  my $str             = "";
  foreach my $key ( keys(%master_hash) ) {
    my $slave  = $master_hash{$key};
    my $master = $self->get_master_by_slave($slave);
    $str .= "Master " . $master->get_hostinfo();
    $str .=
", replicating from $master->{Master_Host}($master->{Master_IP}:$master->{Master_Port})"
      if ( $master->{Master_Host} );
    $str .= ", read-only" if ( $master->{read_only} );
    $str .= ", dead"      if ( $master->{dead} );
    $str .= "\n";
  }
  $str .= "\n";
  return $str;
}

sub get_primary_master {
  my $self            = shift;
  my $master_hash_ref = shift;
  my $log             = $self->{logger};
  my @alive_servers   = $self->get_alive_servers();
  my %master_hash     = %$master_hash_ref;

  my $num_real_masters = 0;
  my $real_master;
  foreach my $key ( keys(%master_hash) ) {
    my $slave  = $master_hash{$key};
    my $master = $self->get_master_by_slave($slave);
    next if ( !$master->{dead} && $master->{read_only} );
    $real_master = $master;
    $num_real_masters++;
  }
  if ( $num_real_masters < 1 ) {
    $log->error(
      sprintf(
"Multi-master configuration is detected, but all of them are read-only! Check configurations for details. Master configurations are as below: \n%s",
        $self->get_multi_master_print_info($master_hash_ref) )
    );
    croak;
  }
  elsif ( $num_real_masters >= 2 ) {
    $log->error(
      sprintf(
"Multi-master configuration is detected, but two or more masters are either writable (read-only is not set) or dead! Check configurations for details. Master configurations are as below: \n%s",
        $self->get_multi_master_print_info($master_hash_ref) )
    );
    croak;
  }
  else {
    $self->set_orig_master($real_master);
    $log->info(
      sprintf(
"Multi-master configuration is detected. Current primary(writable) master is %s",
        $real_master->get_hostinfo() )
    );
    $log->info(
      sprintf( "Master configurations are as below: \n%s",
        $self->get_multi_master_print_info($master_hash_ref) )
    );
    $self->init_servers();
  }
  return $real_master;
}

sub get_candidate_masters($) {
  my $self        = shift;
  my $log         = $self->{logger};
  my @servers     = $self->get_servers();
  my @ret_servers = ();
  foreach (@servers) {
    next if ( $_->{dead} eq '1' );
    if ( $_->{candidate_master} >= 1 ) {
      push( @ret_servers, $_ );
    }
  }
  return @ret_servers;
}

sub print_dead_servers {
  my $self = shift;
  $self->print_servers( $self->{dead_servers} );
}

sub print_alive_servers {
  my $self          = shift;
  my $log           = $self->{logger};
  my @alive_servers = $self->get_alive_servers();
  foreach (@alive_servers) {
    $log->info( "  " . $_->get_hostinfo() );
  }
}

sub print_alive_slaves {
  my $self = shift;
  $self->print_servers( $self->{alive_slaves} );
}

sub print_latest_slaves {
  my $self = shift;
  $self->print_servers( $self->{latest_slaves} );
}

sub print_oldest_slaves {
  my $self = shift;
  $self->print_servers( $self->{oldest_slaves} );
}

sub print_failed_slaves_if {
  my $self          = shift;
  my $log           = $self->{logger};
  my @failed_slaves = $self->get_failed_slaves();
  if ( $#failed_slaves >= 0 ) {
    $log->info("Failed Slaves:");
    $self->print_servers( $self->{failed_slaves} );
  }
}

sub print_unmanaged_slaves_if {
  my $self             = shift;
  my $log              = $self->{logger};
  my @unmanaged_slaves = $self->get_unmanaged_slaves();
  if ( $#unmanaged_slaves >= 0 ) {
    $log->info("Unmanaged Servers:");
    $self->print_servers( $self->{unmanaged_slaves} );
  }
}

sub print_servers {
  my ( $self, $servers_ref ) = @_;
  my @servers = @$servers_ref;
  foreach (@servers) {
    $_->print_server();
  }
}

sub disconnect_all($) {
  my $self    = shift;
  my $log     = $self->{logger};
  my @servers = $self->get_alive_servers();
  foreach (@servers) {
    $_->disconnect();
  }
}

# Check master is not reachable from all alive slaves
# prerequisite: all slaves see the same master
# return 0;ok 1: running
sub is_master_reachable_from_slaves($$) {
  my $self       = shift;
  my $slaves_ref = shift;
  my $log        = $self->{logger};
  my @slaves     = $self->get_alive_slaves();
  $log->info("Checking the current master is not reachable from all slaves..");
  foreach (@slaves) {
    my $dbhelper = $_->{dbhelper};
    $dbhelper->stop_io_thread();
    $dbhelper->start_io_thread();
    sleep(3);
    my %status = $dbhelper->check_slave_status();
    if ( $status{Status} ne '0' || !defined( $status{Slave_IO_Running} ) ) {
      $log->error(
        sprintf( "Got error when stopping/starting io thread on %s",
          $_->get_hostinfo() )
      );
      return 1;
    }
    if ( $status{Slave_IO_Running} eq "Yes" ) {
      $log->warning(
        sprintf( "Master is reachable from slave %s", $_->get_hostinfo() ) );
      return 1;
    }
    $dbhelper->stop_io_thread();
    $log->info(
      sprintf( " Master is not reachable from slave %s", $_->get_hostinfo() ) );
  }
  $log->info("  done.");
  return 0;
}

# checking slave status again before starting main operations.
# alive slaves info was already fetched by connect_all_and_read_server_status,
# so check_slave_status should not fail here. If it fails, we die here.
sub read_slave_status($) {
  my $self   = shift;
  my $log    = $self->{logger};
  my @slaves = $self->get_alive_slaves();

  $log->debug("Fetching current slave status..");
  foreach (@slaves) {
    my $dbhelper  = $_->{dbhelper};
    my ($sstatus) = ();
    my %status    = $dbhelper->check_slave_status();

    # This should not happen so die if it happens
    if ( $status{Status} ) {
      my $msg = "Checking slave status failed.";
      $msg .= " err=$status{Errstr}" if ( $status{Errstr} );
      $log->error($msg);
      croak;
    }

    $_->{latest}                = 0;
    $_->{Master_Log_File}       = $status{Master_Log_File};
    $_->{Read_Master_Log_Pos}   = $status{Read_Master_Log_Pos};
    $_->{Relay_Master_Log_File} = $status{Relay_Master_Log_File};
    $_->{Exec_Master_Log_Pos}   = $status{Exec_Master_Log_Pos};
    $_->{Relay_Log_File}        = $status{Relay_Log_File};
    $_->{Relay_Log_Pos}         = $status{Relay_Log_Pos};
  }
  $log->debug(" Fetching current slave status done.");
}

sub start_sql_threads_if($) {
  my $self   = shift;
  my @slaves = $self->get_alive_slaves();
  foreach my $slave (@slaves) {
    $slave->start_sql_thread_if();
  }
}

sub get_failover_advisory_locks($) {
  my $self   = shift;
  my $log    = $self->{logger};
  my @slaves = $self->get_alive_slaves();
  foreach my $slave (@slaves) {
    if ( $slave->get_failover_advisory_lock() ) {
      $log->error(
        sprintf(
"Getting advisory lock failed on %s. Maybe failover script or purge_relay_logs script is running on the same slave?",
          $slave->get_hostinfo() )
      );
      croak;
    }
  }
}

sub identify_latest_slaves($$) {
  my $self        = shift;
  my $find_oldest = shift;
  $find_oldest = 0 unless ($find_oldest);
  my $log    = $self->{logger};
  my @slaves = $self->get_alive_slaves();
  my @latest = ();
  foreach (@slaves) {
    my $a = $latest[0]{Master_Log_File};
    my $b = $latest[0]{Read_Master_Log_Pos};
    if (
      !$find_oldest
      && (
           ( !$a && !defined($b) )
        || ( $_->{Master_Log_File} gt $latest[0]{Master_Log_File} )
        || ( ( $_->{Master_Log_File} ge $latest[0]{Master_Log_File} )
          && $_->{Read_Master_Log_Pos} > $latest[0]{Read_Master_Log_Pos} )
      )
      )
    {
      @latest = ();
      push( @latest, $_ );
    }
    elsif (
      $find_oldest
      && (
           ( !$a && !defined($b) )
        || ( $_->{Master_Log_File} lt $latest[0]{Master_Log_File} )
        || ( ( $_->{Master_Log_File} le $latest[0]{Master_Log_File} )
          && $_->{Read_Master_Log_Pos} < $latest[0]{Read_Master_Log_Pos} )
      )
      )
    {
      @latest = ();
      push( @latest, $_ );
    }
    elsif ( ( $_->{Master_Log_File} eq $latest[0]{Master_Log_File} )
      && ( $_->{Read_Master_Log_Pos} == $latest[0]{Read_Master_Log_Pos} ) )
    {
      push( @latest, $_ );
    }
  }
  foreach (@latest) {
    $_->{latest} = 1 if ( !$find_oldest );
    $_->{oldest} = 1 if ($find_oldest);
  }
  $log->info(
    sprintf(
      "The %s binary log file/position on all slaves is" . " %s:%d\n",
      $find_oldest ? "oldest" : "latest", $latest[0]{Master_Log_File},
      $latest[0]{Read_Master_Log_Pos}
    )
  );
  if ($find_oldest) {
    $self->set_oldest_slaves( \@latest );
  }
  else {
    $self->set_latest_slaves( \@latest );
  }
}

sub identify_oldest_slaves($) {
  my $self = shift;
  return $self->identify_latest_slaves(1);
}

# 1: higher
# -1: older
# 0: equal
sub pos_cmp {
  my ( $self, $a_mlf, $a_mlp, $b_mlf, $b_mlp ) = @_;
  return 0 if ( $a_mlf eq $b_mlf && $a_mlp == $b_mlp );
  return -1 if ( $a_mlf lt $b_mlf || ( $a_mlf le $b_mlf && $a_mlp < $b_mlp ) );
  return 1;
}

sub set_no_master_if_older($$$) {
  my $self   = shift;
  my $mlf    = shift;
  my $mlp    = shift;
  my @slaves = $self->get_alive_slaves();
  foreach (@slaves) {
    $_->{no_master} = 1
      if (
      $self->pos_cmp( $_->{Master_Log_File}, $_->{Read_Master_Log_Pos},
        $mlf, $mlp ) < 0
      );
  }
}

sub get_oldest_limit_pos($) {
  my $self   = shift;
  my @slaves = $self->get_alive_slaves();
  my $target;
  foreach (@slaves) {
    next if ( $_->{ignore_fail} );
    my $a = $target->{Master_Log_File};
    my $b = $target->{Read_Master_Log_Pos};
    if (
         ( !$a && !defined($b) )
      || ( $_->{Master_Log_File} lt $target->{Master_Log_File} )
      || ( ( $_->{Master_Log_File} le $target->{Master_Log_File} )
        && $_->{Read_Master_Log_Pos} < $target->{Read_Master_Log_Pos} )
      )
    {
      $target = $_;
    }
  }
  return ( $target->{Master_Log_File}, $target->{Read_Master_Log_Pos} )
    if ($target);
}

# check slave is too behind master or not
# 0: no or acceptable delay
# 1: unacceptable delay (can not be a master)
sub check_slave_delay($$$) {
  my $self   = shift;
  my $target = shift;
  my $latest = shift;
  my $log    = $self->{logger};
  $log->debug(
    sprintf( "Checking replication delay on %s.. ", $target->get_hostinfo() ) );
  if (
    ( $latest->{Master_Log_File} gt $target->{Relay_Master_Log_File} )
    || ( $latest->{Read_Master_Log_Pos} >
      $target->{Exec_Master_Log_Pos} + 100000000 )
    )
  {
    $log->warning(
      sprintf(
" Slave %s SQL Thread delays too much. Latest log file:%s:%d, Current log file:%s:%d. This server is not selected as a new master because recovery will take long time.\n",
        $target->get_hostinfo(),        $latest->{Master_Log_File},
        $latest->{Read_Master_Log_Pos}, $target->{Relay_Master_Log_File},
        $target->{Exec_Master_Log_Pos}
      )
    );
    return 1;
  }
  $log->debug(" ok.");
  return 0;
}

# The following servers can not be master:
# - dead servers
# - Set no_master in conf files (i.e. DR servers)
# - log_bin is disabled
# - Major version is not the oldest
# - too much replication delay
sub get_bad_candidate_masters($$$) {
  my $self                    = shift;
  my $latest_slave            = shift;
  my $check_replication_delay = shift;
  my $log                     = $self->{logger};

  my @servers     = $self->get_alive_slaves();
  my @ret_servers = ();
  foreach (@servers) {
    if (
         $_->{no_master} >= 1
      || $_->{log_bin} eq '0'
      || $_->{oldest_major_version} eq '0'
      || (
        $latest_slave
        && ( $check_replication_delay
          && $self->check_slave_delay( $_, $latest_slave ) >= 1 )
      )
      )
    {
      push( @ret_servers, $_ );
    }
  }
  return @ret_servers;
}

sub is_target_bad_for_new_master {
  my $self   = shift;
  my $target = shift;
  my @bad    = $self->get_bad_candidate_masters();
  foreach (@bad) {
    return 1 if ( $target->{id} eq $_->{id} );
  }
  return 0;
}

# Picking up new master
# If preferred node is specified, one of active preferred nodes will be new master.
# If the latest server behinds too much (i.e. stopping sql thread for online backups), we should not use it as a new master, but we should fetch relay log there. Even though preferred master is configured, it does not become a master if it's far behind.
sub select_new_master {
  my $self                    = shift;
  my $prio_new_master_host    = shift;
  my $prio_new_master_port    = shift;
  my $check_replication_delay = shift;
  $check_replication_delay = 1 if ( !defined($check_replication_delay) );

  my $log    = $self->{logger};
  my @latest = $self->get_latest_slaves();
  my @slaves = $self->get_alive_slaves();

  my @pref = $self->get_candidate_masters();
  my @bad =
    $self->get_bad_candidate_masters( $latest[0], $check_replication_delay );

  if ( $prio_new_master_host && $prio_new_master_port ) {
    my $new_master =
      $self->get_alive_server_by_hostport( $prio_new_master_host,
      $prio_new_master_port );
    if ($new_master) {
      my $a = $self->get_server_from_by_id( \@bad, $new_master->{id} );
      unless ($a) {
        $log->info("$prio_new_master_host can be new master.");
        return $new_master;
      }
      else {
        $log->error("$prio_new_master_host is bad as a new master!");
        return;
      }
    }
    else {
      $log->error("$prio_new_master_host is not alive!");
      return;
    }
  }

  $log->info("Searching new master from slaves..");
  $log->info(" Candidate masters from the configuration file:");
  $self->print_servers( \@pref );
  $log->info(" Non-candidate masters:");
  $self->print_servers( \@bad );

  return $latest[0]
    if ( $#pref < 0 && $#bad < 0 && $latest[0]->{latest_priority} );

  if ( $latest[0]->{latest_priority} ) {
    $log->info(
" Searching from candidate_master slaves which have received the latest relay log events.."
    ) if ( $#pref >= 0 );
    foreach my $h (@latest) {
      foreach my $p (@pref) {
        if ( $h->{id} eq $p->{id} ) {
          return $h
            if ( !$self->get_server_from_by_id( \@bad, $p->{id} ) );
        }
      }
    }
    $log->info("  Not found.") if ( $#pref >= 0 );
  }

  #new master is not latest
  $log->info(" Searching from all candidate_master slaves..")
    if ( $#pref >= 0 );
  foreach my $s (@slaves) {
    foreach my $p (@pref) {
      if ( $s->{id} eq $p->{id} ) {
        my $a = $self->get_server_from_by_id( \@bad, $p->{id} );
        return $s unless ($a);
      }
    }
  }
  $log->info("  Not found.") if ( $#pref >= 0 );

  if ( $latest[0]->{latest_priority} ) {
    $log->info(
" Searching from all slaves which have received the latest relay log events.."
    );
    foreach my $h (@latest) {
      my $a = $self->get_server_from_by_id( \@bad, $h->{id} );
      return $h unless ($a);
    }
    $log->info("  Not found.");
  }

  # none of latest servers can not be a master
  $log->info(" Searching from all slaves..");
  foreach my $s (@slaves) {
    my $a = $self->get_server_from_by_id( \@bad, $s->{id} );
    return $s unless ($a);
  }
  $log->info("  Not found.");

  return;
}

sub get_new_master_binlog_position($$) {
  my $self     = shift;
  my $target   = shift;                 # master
  my $dbhelper = $target->{dbhelper};
  my $log      = $self->{logger};
  $log->info("Getting new master's binlog name and position..");
  my ( $file, $pos ) = $dbhelper->show_master_status();
  if ( $file && defined($pos) ) {
    $log->info(" $file:$pos");
    $log->info(
      sprintf(
" All other slaves should start replication from here. Statement should be: CHANGE MASTER TO MASTER_HOST='%s', MASTER_PORT=%d, MASTER_LOG_FILE='%s', MASTER_LOG_POS=%d, MASTER_USER='%s', MASTER_PASSWORD='xxx';",
        ( $target->{hostname} eq $target->{ip} )
        ? $target->{hostname}
        : ("$target->{hostname} or $target->{ip}"),
        $target->{port},
        $file,
        $pos,
        $target->{repl_user}
      )
    );
  }
  else {
    $log->error("Getting new master's binlog position failed!");
    return;
  }
  return ( $file, $pos );
}

sub change_master_and_start_slave {
  my ( $self, $target, $master, $master_log_file, $master_log_pos, $log ) = @_;
  $log = $self->{logger} unless ($log);
  return if ( $target->{id} eq $master->{id} );
  my $dbhelper = $target->{dbhelper};
  $log->info(
    sprintf(
      " Resetting slave %s and starting replication from the new master %s..",
      $target->get_hostinfo(),
      $master->get_hostinfo()
    )
  );
  $target->stop_slave($log) unless ( $target->{not_slave} );
  $dbhelper->reset_slave()  unless ( $target->{not_slave} );
  $dbhelper->change_master( $target->{use_ip_for_change_master}
    ? $master->{ip}
    : $master->{hostname},
    $master->{port}, $master_log_file, $master_log_pos, $master->{repl_user},
    $master->{repl_password} );
  $log->info(" Executed CHANGE MASTER.");

  # After executing CHANGE MASTER, relay_log_purge is automatically disabled.
  # If the original value is 0, we should turn to 0 explicitly.
  unless ( $target->{relay_purge} ) {
    $target->disable_relay_log_purge();
  }
  my $ret = $target->start_slave($log);
  unless ($ret) {
    $log->info(" Slave started.");
  }
  return $ret;
}

sub get_current_alive_master($) {
  my $self   = shift;
  my $log    = $self->{logger};
  my $master = $self->get_orig_master();
  unless ($master) {
    $log->error(
      "MySQL master is not correctly configured. Check master/slave settings");
    croak;
  }
  my $m = $self->get_alive_server_by_id( $master->{id} );
  unless ($m) {
    $log->warning("MySQL master is not currently alive!");
    return;
  }
  $log->info( sprintf( "Current Alive Master: %s", $m->get_hostinfo() ) );
  return $master;
}

sub stop_io_threads {
  my $self         = shift;
  my $log          = $self->{logger};
  my @alive_slaves = $self->get_alive_slaves();
  my $pm           = new Parallel::ForkManager( $#alive_slaves + 1 );
  foreach my $target (@alive_slaves) {
    $target->stop_io_thread($target);
    exit 0;
  }
  $pm->wait_all_children;
  return 0;
}

sub check_repl_priv {
  my $self    = shift;
  my @servers = $self->get_alive_servers();
  foreach my $target (@servers) {
    $target->check_repl_priv();
  }
}

sub release_failover_advisory_lock {
  my $self    = shift;
  my @servers = $self->get_alive_servers();
  foreach my $target (@servers) {
    $target->release_failover_advisory_lock();
  }
}

sub get_current_servers_ascii {
  my $self         = shift;
  my $orig_master  = shift;
  my @alive_slaves = $self->get_alive_slaves();

  my $str = "$orig_master->{hostname} (current master)";
  $str .= " ($orig_master->{node_label})"
    if ( $orig_master->{node_label} );
  $str .= "\n";
  foreach my $slave (@alive_slaves) {
    $str .= " +--" . "$slave->{hostname}";
    $str .= " ($slave->{node_label})" if ( $slave->{node_label} );
    $str .= "\n";
  }
  $str .= "\n";
  return $str;
}

sub print_servers_ascii {
  my $self         = shift;
  my $orig_master  = shift;
  my $log          = $self->{logger};
  my @alive_slaves = $self->get_alive_slaves();

  my $str = "\n";
  $str .= $self->get_current_servers_ascii($orig_master);
  $log->info($str);
}

sub print_servers_migration_ascii {
  my $self                     = shift;
  my $orig_master              = shift;
  my $new_master               = shift;
  my $orig_master_is_new_slave = shift;
  my $log                      = $self->{logger};
  my @alive_slaves             = $self->get_alive_slaves();

  my $str = "\n";
  $str .= "From:\n";
  $str .= $self->get_current_servers_ascii($orig_master);

  $str .= "To:\n";
  $str .= "$new_master->{hostname} (new master)";
  $str .= " ($new_master->{node_label})"
    if ( $new_master->{node_label} );
  $str .= "\n";
  foreach my $slave (@alive_slaves) {
    next if ( $slave->{id} eq $new_master->{id} );
    $str .= " +--" . "$slave->{hostname}";
    $str .= " ($slave->{node_label})" if ( $slave->{node_label} );
    $str .= "\n";
  }
  if ($orig_master_is_new_slave) {
    $str .= " +--" . "$orig_master->{hostname}";
    $str .= " ($orig_master->{node_label})" if ( $orig_master->{node_label} );
    $str .= "\n";
  }
  $log->info($str);
}

# for manual failover/switch only
sub manually_decide_new_master {
  my $self        = shift;
  my $orig_master = shift;
  my $new_master  = shift;
  my $log         = $self->{logger};

  printf(
    "\nStarting master switch from %s to %s? (yes/NO): ",
    $orig_master->get_hostinfo(),
    $new_master->get_hostinfo()
  );
  my $ret = <STDIN>;
  chomp($ret);
  if ( lc($ret) !~ /^y/ ) {
    print "Continue? (yes/NO): ";
    $ret = <STDIN>;
    chomp($ret);
    if ( lc($ret) !~ /^y/ ) {
      $orig_master->{not_error} = 1;
      die "Not typed yes. Stopping.";
    }
    print "Enter new master host name: ";
    $ret = <STDIN>;
    chomp($ret);
    $new_master = $self->get_alive_server_by_hostport( $ret, 3306 );

    if ( !$new_master ) {
      die "New server not found!\n";
    }
    printf "Master switch to %s. OK? (yes/NO): ", $new_master->get_hostinfo();
    $ret = <STDIN>;
    chomp($ret);
    die "Not typed yes. Stopping. \n" if ( lc($ret) !~ /^y/ );
  }
  return $new_master;
}

sub check_replication_health {
  my $self                = shift;
  my $allow_delay_seconds = shift;
  $allow_delay_seconds = 1 unless ($allow_delay_seconds);
  my $log          = $self->{logger};
  my @alive_slaves = $self->get_alive_slaves();
  foreach my $target (@alive_slaves) {
    $log->info("Checking replication health on $target->{hostname}..");
    if ( !$target->current_slave_position() ) {
      $log->error("Getting slave status failed!");
      croak;
    }
    if ( $target->has_replication_problem($allow_delay_seconds) ) {
      $log->error(" failed!");
      croak;
    }
    else {
      $log->info(" ok.");
    }
  }
}

1;
