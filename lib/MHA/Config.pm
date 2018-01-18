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

package MHA::Config;

use strict;
use warnings FATAL => 'all';

use Carp qw(croak);
use English qw(-no_match_vars);
use Config::Tiny;
use Log::Dispatch;
use MHA::Server;
use MHA::NodeUtil;
use MHA::ManagerConst;

my @PARAM_ARRAY =
  qw/ hostname ip port ssh_host ssh_ip ssh_port ssh_connection_timeout ssh_options node_label candidate_master no_master ignore_fail skip_init_ssh_check skip_reset_slave user password repl_user repl_password disable_log_bin master_pid_file handle_raw_binlog ssh_user remote_workdir master_binlog_dir log_level manager_workdir manager_log check_repl_delay check_repl_filter latest_priority multi_tier_slave ping_interval ping_type secondary_check_script master_ip_failover_script master_ip_online_change_script shutdown_script report_script init_conf_load_script client_bindir client_libdir use_gtid_auto_pos/;
my %PARAM;
for (@PARAM_ARRAY) { $PARAM{$_} = 1; }

sub new {
  my $class = shift;
  my $self  = {
    file       => undef,
    globalfile => undef,
    logger     => undef,
    @_,
  };
  return bless $self, $class;
}

sub parse_server {
  my $self      = shift;
  my $param_arg = shift;
  my $default   = shift;

  my %value;
  foreach my $key ( sort keys(%$param_arg) ) {
    unless ( exists( $PARAM{$key} ) ) {
      croak "Parameter name $key is invalid!\n";
    }
  }

  $value{hostname} = $param_arg->{hostname};
  $value{ip}       = $param_arg->{ip};
  $value{ip} = MHA::NodeUtil::get_ip( $value{hostname} ) unless ( $value{ip} );
  $value{node_label} = $param_arg->{node_label};

  $value{port} = $param_arg->{port};
  if ( !defined( $value{port} ) ) {
    $value{port} = $default->{port};
    $value{port} = 3306 unless ( $value{port} );
  }

  $value{ssh_host} = $param_arg->{ssh_host};
  if ( !defined( $value{ssh_host} ) ) {
    $value{ssh_host} = $value{hostname};
  }
  if ( $value{hostname}
    && $value{ssh_host}
    && $value{ssh_host} eq $value{hostname} )
  {
    $value{ssh_ip} = $value{ip};
  }
  else {
    $value{ssh_ip} = MHA::NodeUtil::get_ip( $value{ssh_host} );
  }

  $value{ssh_port} = $param_arg->{ssh_port};
  if ( !defined( $value{ssh_port} ) ) {
    $value{ssh_port} = $default->{ssh_port};
    $value{ssh_port} = 22 unless ( $value{ssh_port} );
  }

  $value{ssh_options} = $param_arg->{ssh_options};
  if ( $value{ssh_options} ) {
    $value{ssh_options} =~ s/['"]//g;
    if ( $value{ssh_options} ) {
      $MHA::ManagerConst::USE_SSH_OPTIONS = 1;
      $MHA::ManagerConst::SSH_OPT_ALIVE =
        $MHA::ManagerConst::SSH_OPT_ALIVE_DEFAULT . " $value{ssh_options}";
      $MHA::NodeConst::SSH_OPT_ALIVE =
        $MHA::ManagerConst::SSH_OPT_ALIVE_DEFAULT . " $value{ssh_options}";
      $MHA::ManagerConst::SSH_OPT_CHECK =
        $MHA::ManagerConst::SSH_OPT_CHECK_DEFAULT . " $value{ssh_options}";
    }
  }

  $value{ssh_connection_timeout} = $param_arg->{ssh_connection_timeout};
  if ( !defined( $value{ssh_connection_timeout} ) ) {
    $value{ssh_connection_timeout} = $default->{ssh_connection_timeout};
    $value{ssh_connection_timeout} = 5
      unless ( $value{ssh_connection_timeout} );
  }
  check_positive_int( "ssh_connection_timeout",
    $value{ssh_connection_timeout} );
  $MHA::ManagerConst::SSH_OPT_CHECK =~
    s/VAR_CONNECT_TIMEOUT/$value{ssh_connection_timeout}/;

  $value{user} = $param_arg->{user};
  if ( !defined( $value{user} ) ) {
    $value{user} = $default->{user};
    $value{user} = 'root' unless ( $value{user} );
  }
  $value{password} = $param_arg->{password};
  if ( !defined( $value{password} ) ) {
    $value{password} = $default->{password};
    $value{password} = '' if ( !defined( $value{password} ) );
  }
  $value{repl_user} = $param_arg->{repl_user};
  if ( !defined( $value{repl_user} ) ) {
    $value{repl_user} = $default->{repl_user};
  }
  $value{repl_password} = $param_arg->{repl_password};
  if ( !defined( $value{repl_password} ) ) {
    $value{repl_password} = $default->{repl_password};
  }
  $value{master_pid_file} = $param_arg->{master_pid_file};
  if ( !defined( $value{master_pid_file} ) ) {
    $value{master_pid_file} = $default->{master_pid_file};
  }

  $value{log_level} = $param_arg->{log_level};
  if ( !defined( $value{log_level} ) ) {
    $value{log_level} = $default->{log_level};
    $value{log_level} = 'info' unless ( $value{log_level} );
  }
  $value{init_conf_load_script} = $param_arg->{init_conf_load_script};
  if ( !defined( $value{init_conf_load_script} ) ) {
    $value{init_conf_load_script} = $default->{init_conf_load_script};
  }
  $value{secondary_check_script} = $param_arg->{secondary_check_script};
  if ( !defined( $value{secondary_check_script} ) ) {
    $value{secondary_check_script} = $default->{secondary_check_script};
  }
  $value{shutdown_script} = $param_arg->{shutdown_script};
  if ( !defined( $value{shutdown_script} ) ) {
    $value{shutdown_script} = $default->{shutdown_script};
  }
  $value{report_script} = $param_arg->{report_script};
  if ( !defined( $value{report_script} ) ) {
    $value{report_script} = $default->{report_script};
  }
  $value{master_ip_failover_script} = $param_arg->{master_ip_failover_script};
  if ( !defined( $value{master_ip_failover_script} ) ) {
    $value{master_ip_failover_script} = $default->{master_ip_failover_script};
  }

  # For online master switch only
  $value{master_ip_online_change_script} =
    $param_arg->{master_ip_online_change_script};
  if ( !defined( $value{master_ip_online_change_script} ) ) {
    $value{master_ip_online_change_script} =
      $default->{master_ip_online_change_script};
  }

  $value{disable_log_bin} = $param_arg->{disable_log_bin};
  if ( !defined( $value{disable_log_bin} ) ) {
    $value{disable_log_bin} = $default->{disable_log_bin};
    $value{disable_log_bin} = 0 if ( !defined( $value{disable_log_bin} ) );
  }

  $value{handle_raw_binlog} = $param_arg->{handle_raw_binlog};
  if ( !defined( $value{handle_raw_binlog} ) ) {
    $value{handle_raw_binlog} = $default->{handle_raw_binlog};
    $value{handle_raw_binlog} = 1 if ( !defined( $value{handle_raw_binlog} ) );
  }

  $value{check_repl_delay} = $param_arg->{check_repl_delay};
  if ( !defined( $value{check_repl_delay} ) ) {
    $value{check_repl_delay} = $default->{check_repl_delay};
    $value{check_repl_delay} = 1 if ( !defined( $value{check_repl_delay} ) );
  }

  $value{check_repl_filter} = $param_arg->{check_repl_filter};
  if ( !defined( $value{check_repl_filter} ) ) {
    $value{check_repl_filter} = $default->{check_repl_filter};
    $value{check_repl_filter} = 1 if ( !defined( $value{check_repl_filter} ) );
  }

  $value{latest_priority} = $param_arg->{latest_priority};
  if ( !defined( $value{latest_priority} ) ) {
    $value{latest_priority} = $default->{latest_priority};
    $value{latest_priority} = 1 if ( !defined( $value{latest_priority} ) );
  }

  $value{multi_tier_slave} = $param_arg->{multi_tier_slave};
  if ( !defined( $value{multi_tier_slave} ) ) {
    $value{multi_tier_slave} = $default->{multi_tier_slave};
    $value{multi_tier_slave} = 0 if ( !defined( $value{multi_tier_slave} ) );
  }

  $value{skip_reset_slave} = $param_arg->{skip_reset_slave};
  if ( !defined( $value{skip_reset_slave} ) ) {
    $value{skip_reset_slave} = $default->{skip_reset_slave};
    $value{skip_reset_slave} = 0 if ( !defined( $value{skip_reset_slave} ) );
  }

  $value{use_gtid_auto_pos} = $param_arg->{use_gtid_auto_pos};
  if ( !defined( $value{use_gtid_auto_pos} ) ) {
    $value{use_gtid_auto_pos} = $default->{use_gtid_auto_pos};
    $value{use_gtid_auto_pos} = 1 if ( !defined( $value{use_gtid_auto_pos} ) );
  }

  $value{master_binlog_dir} = $param_arg->{master_binlog_dir};
  if ( !defined( $value{master_binlog_dir} ) ) {
    $value{master_binlog_dir} = $default->{master_binlog_dir};
    $value{master_binlog_dir} = "/var/lib/mysql,/var/log/mysql"
      unless ( $value{master_binlog_dir} );
  }
  $value{master_binlog_dir} =~ s/\s//g if ( $value{master_binlog_dir} );

  $value{manager_workdir} = $param_arg->{manager_workdir};
  if ( !defined( $value{manager_workdir} ) ) {
    $value{manager_workdir} = $default->{manager_workdir};
    $value{manager_workdir} = "/var/tmp" unless ( $value{manager_workdir} );
  }

  $value{manager_log} = $param_arg->{manager_log};
  if ( !defined( $value{manager_log} ) ) {
    $value{manager_log} = $default->{manager_log};
  }

  $value{remote_workdir} = $param_arg->{remote_workdir};
  if ( !defined( $value{remote_workdir} ) ) {
    $value{remote_workdir} = $default->{remote_workdir};
    $value{remote_workdir} = "/var/tmp" unless ( $value{remote_workdir} );
  }

  $value{ssh_user} = $param_arg->{ssh_user};
  if ( !defined( $value{ssh_user} ) ) {
    $value{ssh_user} = $default->{ssh_user};
    $value{ssh_user} = getpwuid($>) unless ( $value{ssh_user} );
  }

  $value{candidate_master} = $param_arg->{candidate_master};
  $value{candidate_master} = 0 if ( !defined( $value{candidate_master} ) );

  $value{no_master} = $param_arg->{no_master};
  $value{no_master} = 0 if ( !defined( $value{no_master} ) );

  $value{ignore_fail} = $param_arg->{ignore_fail};
  $value{ignore_fail} = 0 if ( !defined( $value{ignore_fail} ) );

  $value{skip_init_ssh_check} = $param_arg->{skip_init_ssh_check};
  $value{skip_init_ssh_check} = 0
    if ( !defined( $value{skip_init_ssh_check} ) );

  $value{ping_type} = $param_arg->{ping_type};
  if ( !defined( $value{ping_type} ) ) {
    $value{ping_type} = $default->{ping_type};
    $value{ping_type} = $MHA::ManagerConst::PING_TYPE_SELECT
      if ( !defined( $value{ping_type} ) );
  }
  $value{ping_type} = uc( $value{ping_type} );
  croak
"Parameter ping_type must be either '$MHA::ManagerConst::PING_TYPE_CONNECT' or '$MHA::ManagerConst::PING_TYPE_SELECT' or '$MHA::ManagerConst::PING_TYPE_INSERT'. Current value: $value{ping_type}\n"
    if ( $value{ping_type} ne $MHA::ManagerConst::PING_TYPE_CONNECT
    && $value{ping_type} ne $MHA::ManagerConst::PING_TYPE_SELECT
    && $value{ping_type} ne $MHA::ManagerConst::PING_TYPE_INSERT );

  $value{ping_interval} = $param_arg->{ping_interval};
  if ( !defined( $value{ping_interval} ) ) {
    $value{ping_interval} = $default->{ping_interval};
    $value{ping_interval} = 3 if ( !defined( $value{ping_interval} ) );
  }
  check_positive_int( "ping_interval", $value{ping_interval} );

  $value{client_bindir} = $param_arg->{client_bindir};
  if ( !defined( $value{client_bindir} ) ) {
    $value{client_bindir} = $default->{client_bindir};
  }
  $value{client_libdir} = $param_arg->{client_libdir};
  if ( !defined( $value{client_libdir} ) ) {
    $value{client_libdir} = $default->{client_libdir};
  }

  my $server = new MHA::Server();
  foreach my $key ( keys(%PARAM) ) {
    if ( $value{$key} ) {
      $value{$key} =~ s/^['"]?(.*)['"]$/$1/;
    }
    $server->{$key} = $value{$key};
  }

  # set escaped_user and escaped_password
  foreach my $key ( 'user', 'password' ) {
    my $new_key       = "escaped_" . $key;
    my $new_mysql_key = "mysql_escaped_" . $key;
    if ( $server->{$key} ) {
      $server->{$new_key} = MHA::NodeUtil::escape_for_shell( $server->{$key} );
      $server->{$new_mysql_key} =
        MHA::NodeUtil::escape_for_mysql_command( $server->{$key} );
    }
    else {
      $server->{$new_key} = $server->{$new_mysql_key} = "";
    }
  }
  return $server;
}

sub check_positive_int($$) {
  my $param_name  = shift;
  my $param_value = shift;
  croak
"Parameter $param_name must be positive integer! current value: $param_value\n"
    if ( $param_value !~ /\d/
    || $param_value =~ /\D/
    || $param_value < 1 );
}

sub read_config($) {
  my $self              = shift;
  my $log               = $self->{logger};
  my @servers           = ();
  my @binlog_servers    = ();
  my $global_configfile = $self->{globalfile};
  my $configfile        = $self->{file};
  my $sd;

  if ( -f $global_configfile ) {
    my $global_cfg = Config::Tiny->read($global_configfile)
      or croak "Unable to parse/read configuration file: $global_configfile: $!\n";

    $log->info("Reading default configuration from $self->{globalfile}..")
      if ($log);
    $sd = $self->parse_server_default( $global_cfg->{"server default"} );
  }
  else {
    $log->warning(
      "Global configuration file $self->{globalfile} not found. Skipping.")
      if ($log);
    $sd = new MHA::Server();
  }

  my $cfg = Config::Tiny->read($configfile) or croak "$configfile:$!\n";
  $log->info("Reading application default configuration from $self->{file}..")
    if ($log);

  # Read application default settings
  $sd = $self->parse_server( $cfg->{"server default"}, $sd );

  if ( defined( $sd->{init_conf_load_script} ) ) {
    $log->info( "Updating application default configuration from "
        . $sd->{init_conf_load_script}
        . ".." )
      if ($log);
    my @rows = `$sd->{init_conf_load_script}`;
    my $param;
    foreach my $row (@rows) {
      chomp($row);
      my ( $name, $value ) = split( /=/, $row );
      $param->{$name} = $value;
    }
    $sd = $self->parse_server( $param, $sd );
  }

  $log->info("Reading server configuration from $self->{file}..") if ($log);

  my @blocks = sort keys(%$cfg);
  foreach my $block (@blocks) {
    next if ( $block eq "server default" );
    if ( $block !~ /^server\S+/ && $block !~ /^binlog\S+/ ) {
      my $msg =
"Block name \"$block\" is invalid. Block name must be \"server default\" or start from \"server\"(+ non-whitespace characters).";
      $log->error($msg) if ($log);
      croak($msg);
    }
    my $server = $self->parse_server( $cfg->{$block}, $sd );
    $server->{id} = $block;
    if ( $block =~ /^server\S+/ ) {
      push( @servers, $server );
    }
    elsif ( $block =~ /^binlog\S+/ ) {
      push( @binlog_servers, $server );
    }
  }
  my @tmp;
  foreach (@servers) {
    push @tmp, [ $1, $_ ] if ( $_->{id} =~ m/^server\D*([\d]+).*/ );
  }

  # If all IDs are integers, sort by intergers
  if ( $#servers == $#tmp ) {
    @servers = map { $_->[1] } ( sort { $a->[0] <=> $b->[0] } @tmp );
  }
  unless (@servers) {
    my $msg =
"No server is defined in configurations file. Check configurations for details";
    $log->error($msg) if ($log);
    croak($msg);
  }

  # check hostname exists
  for ( my $i = 0 ; $i <= $#servers ; $i++ ) {
    unless ( $servers[$i]->{hostname} ) {
      my $msg = sprintf(
"Server %s does not have hostname! Check configurations and make sure to set hostname parameter.",
        $servers[$i]->{id},
      );
      $log->error($msg) if ($log);
      croak($msg);
    }
  }

  # check duplicate hosts
  for ( my $i = 0 ; $i <= $#servers ; $i++ ) {
    for ( my $j = $i + 1 ; $j <= $#servers ; $j++ ) {
      if ( $servers[$i]->{ip} eq $servers[$j]->{ip}
        && $servers[$i]->{port} eq $servers[$j]->{port} )
      {
        my $msg = sprintf(
"Server %s(hostname %s) and %s(hostname %s) have duplicate ip:port(%s:%d)! Check configurations.",
          $servers[$i]->{id}, $servers[$i]->{hostname},
          $servers[$j]->{id}, $servers[$j]->{hostname},
          $servers[$i]->{ip}, $servers[$i]->{port}
        );
        $log->error($msg) if ($log);
        croak($msg);
      }
    }
  }

  return ( \@servers, \@binlog_servers );
}

sub parse_server_default {
  my $self = shift;
  my $arg  = shift;
  return $self->parse_server($arg);
}

sub print_msg {
  my $msg = shift;
  my $log = shift;
  if ($log) {
    $log->info($msg);
  }
  else {
    print "$msg\n";
  }
}

sub add_block_and_save {
  my $file       = shift;
  my $block_name = shift;
  my $hostname   = shift;
  my $params_ref = shift;
  my $log        = shift;
  my @params     = @$params_ref;

  my $config = Config::Tiny->read($file);
  my $msg;
  unless ($config) {
    croak "Failed to open $file!";
  }
  if ( $config->{$block_name} ) {
    croak "Entry $block_name already exists on $file .";
  }
  $config->{$block_name}->{hostname} = $hostname;
  foreach my $param_value (@params) {
    my ( $key, $value ) = split( /=/, $param_value );
    unless ( exists( $PARAM{$key} ) ) {
      croak "Parameter name $key is invalid!\n";
    }
    else {
      $config->{$block_name}->{$key} = $value;
    }
  }

  $config->write($file);
  $msg = "Wrote $block_name entry to $file .";
  print_msg( $msg, $log );
}

sub delete_block_and_save {
  my $file       = shift;
  my $block_name = shift;
  my $log        = shift;
  my $config     = Config::Tiny->read($file);
  my $msg;
  unless ($config) {
    $msg = "Failed to open $file!";
    print_msg( $msg, $log );
    return;
  }
  unless ( $config->{$block_name} ) {
    $msg = "Entry $block_name not found from $file .";
    print_msg( $msg, $log );
    return;
  }
  delete $config->{$block_name};
  $config->write($file);
  $msg = "Deleted $block_name entry from $file .";
  print_msg( $msg, $log );
}

1;
