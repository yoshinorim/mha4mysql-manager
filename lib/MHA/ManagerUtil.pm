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

package MHA::ManagerUtil;

use strict;
use warnings FATAL => 'all';
use Carp qw(croak);
use MHA::ManagerConst;
use MHA::NodeUtil;
use Log::Dispatch;
use Log::Dispatch::File;
use Log::Dispatch::Screen;

sub init_log {
  my $log_output = shift;
  my $level      = shift;
  $level = "info" unless ($level);
  my $log = Log::Dispatch->new( callbacks => $MHA::ManagerConst::log_fmt );
  unless ($log_output) {
    $log->add(
      Log::Dispatch::Screen->new(
        name      => 'screen',
        min_level => $level,
        callbacks => $MHA::ManagerConst::add_timestamp,
        mode      => 'append',
      )
    );
  }
  else {
    $log->add(
      Log::Dispatch::File->new(
        name              => 'file',
        filename          => $log_output,
        min_level         => $level,
        callbacks         => $MHA::ManagerConst::add_timestamp,
        mode              => 'append',
        close_after_write => 1,
      )
    );
  }
  return $log;
}

sub exec_system {
  my $cmd        = shift;
  my $log_output = shift;
  if ($log_output) {
    return MHA::NodeUtil::system_rc( system("$cmd >> $log_output 2>&1") );
  }
  else {
    return MHA::NodeUtil::system_rc( system($cmd) );
  }
}

sub exec_ssh_check_cmd($$$$) {
  my $ssh_host   = shift;
  my $ssh_port   = shift;
  my $ssh_cmd    = shift;
  my $log_output = shift;
  my $ret;
  return exec_system(
    "ssh $MHA::ManagerConst::SSH_OPT_CHECK -p $ssh_port $ssh_host \"$ssh_cmd\"",
    $log_output
  );
}

sub exec_ssh_cmd($$$$) {
  my $ssh_host   = shift;
  my $ssh_port   = shift;
  my $ssh_cmd    = shift;
  my $log_output = shift;
  my $ret;
  return exec_system(
    "ssh $MHA::ManagerConst::SSH_OPT_ALIVE -p $ssh_port $ssh_host \"$ssh_cmd\"",
    $log_output
  );
}

sub get_node_version {
  my $log      = shift;
  my $ssh_user = shift;
  my $ssh_host = shift;
  my $ssh_ip   = shift;
  my $ssh_port = shift;
  my $ssh_user_host;
  my $node_version;
  my $command = "apply_diff_relay_logs --version";

  if ( $ssh_host || $ssh_ip ) {
    if ($ssh_ip) {
      $ssh_user_host = $ssh_user . '@' . $ssh_ip;
    }
    elsif ($ssh_host) {
      $ssh_user_host = $ssh_user . '@' . $ssh_host;
    }
    $command =
"ssh $MHA::ManagerConst::SSH_OPT_ALIVE $ssh_user_host -p $ssh_port \"$command\" 2>&1";
  }
  my $v = `$command`;
  chomp($v);
  if ( $v =~ /version (\d+\.\d+)/ ) {
    $node_version = $1;
  }
  else {
    $log->error("Got error when getting node version. Error:");
    $log->error("\n$v") if ($v);
  }
  return $node_version;
}

sub check_node_version {
  my $log      = shift;
  my $ssh_user = shift;
  my $ssh_host = shift;
  my $ssh_ip   = shift;
  my $ssh_port = shift;
  my $node_version;
  eval {
    $node_version =
      get_node_version( $log, $ssh_user, $ssh_host, $ssh_ip, $ssh_port );
    my $host = $ssh_host ? $ssh_host : $ssh_ip;
    croak "node version on $host not found! Is MHA Node package installed ?\n"
      unless ($node_version);
    if ( $node_version < $MHA::ManagerConst::NODE_MIN_VERSION ) {
      $host = "local" unless ($host);
      my $msg =
        sprintf( "Node version(%s) on %s must be equal or higher than %s.\n",
        $node_version, $host, $MHA::ManagerConst::NODE_MIN_VERSION );
      croak $msg;
    }
  };
  if ($@) {
    $log->error($@);
    die;
  }
  return $node_version;
}

sub check_node_version_nodie {
  my $log      = shift;
  my $ssh_user = shift;
  my $ssh_host = shift;
  my $ssh_ip   = shift;
  my $ssh_port = shift;
  my $rc       = 1;
  eval {
    check_node_version( $log, $ssh_user, $ssh_host, $ssh_ip, $ssh_port );
    $rc = 0;
  };
  if ($@) {
    undef $@;
  }
  return $rc;
}

# should be used when it is unclear whether $log is initialized or not
sub print_error {
  my $str = shift;
  my $log = shift;
  if ($log) {
    $log->error($str);
  }
  else {
    warn "$str\n";
  }
}

1;
