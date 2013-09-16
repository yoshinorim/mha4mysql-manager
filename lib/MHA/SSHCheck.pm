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

package MHA::SSHCheck;

use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Getopt::Long qw(:config pass_through);
use Carp qw(croak);
use Time::HiRes qw( sleep );
use Log::Dispatch;
use Log::Dispatch::Screen;
use Parallel::ForkManager;
use MHA::Config;
use MHA::ManagerConst;
use MHA::ManagerUtil;
$| = 1;
my $g_global_config_file = $MHA::ManagerConst::DEFAULT_GLOBAL_CONF;
my $g_config_file;

sub do_ssh_connection_check {
  my $servers_ref = shift;
  my $log         = shift;
  my $log_level   = shift;
  my $workdir     = shift;
  my @servers     = @$servers_ref;
  $log->info("Starting SSH connection tests..");
  my $failed = 0;
  my $pm     = new Parallel::ForkManager( $#servers + 1 );

  $pm->run_on_start(
    sub {
      my ( $pid, $target ) = @_;
    }
  );

  $pm->run_on_finish(
    sub {
      my ( $pid, $exit_code, $target ) = @_;
      return if ( $target->{skip_init_ssh_check} );
      my $local_file =
        "$workdir/$target->{ssh_host}_$target->{ssh_port}_ssh_check.log";
      if ($exit_code) {
        $failed = 1;
        if ( -f $local_file ) {
          $log->error( "\n" . `cat $local_file` );
        }
      }
      else {
        if ( -f $local_file ) {
          $log->debug( "\n" . `cat $local_file` );
        }
      }
      unlink $local_file;
    }
  );

  foreach my $src (@servers) {
    if ( $pm->start($src) ) {

 # By default, sshd normally accepts only 10 concurrent authentication requests.
 # If we have lots of alive servers, we might reach this limitation so
 # shifting child process invocation time a bit to avoid this problem.
      sleep 0.5;
      next;
    }
    my ( $file, $pplog );
    eval {
      $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = "DEFAULT";
      $pm->finish(0) if ( $src->{skip_init_ssh_check} );
      $file = "$workdir/$src->{ssh_host}_$src->{ssh_port}_ssh_check.log";
      unlink $file;
      $pplog = Log::Dispatch->new( callbacks => $MHA::ManagerConst::log_fmt );
      $pplog->add(
        Log::Dispatch::File->new(
          name      => 'file',
          filename  => $file,
          min_level => $log_level,
          callbacks => $MHA::ManagerConst::add_timestamp,
          mode      => 'append'
        )
      );
      foreach my $dst (@servers) {
        next if ( $dst->{skip_init_ssh_check} );
        next if ( $src->{id} eq $dst->{id} );
        $pplog->debug(
" Connecting via SSH from $src->{ssh_user}\@$src->{ssh_host}($src->{ssh_ip}:$src->{ssh_port}) to $dst->{ssh_user}\@$dst->{ssh_host}($dst->{ssh_ip}:$dst->{ssh_port}).."
        );
        my $command =
"ssh $MHA::ManagerConst::SSH_OPT_CHECK -p $src->{ssh_port} $src->{ssh_user}\@$src->{ssh_ip} \"ssh $MHA::ManagerConst::SSH_OPT_CHECK -p $dst->{ssh_port} $dst->{ssh_user}\@$dst->{ssh_ip} exit 0\"";
        my ( $high, $low ) = MHA::ManagerUtil::exec_system( $command, $file );
        if ( $high != 0 || $low != 0 ) {
          $pplog->error(
"SSH connection from $src->{ssh_user}\@$src->{ssh_host}($src->{ssh_ip}:$src->{ssh_port}) to $dst->{ssh_user}\@$dst->{ssh_host}($dst->{ssh_ip}:$dst->{ssh_port}) failed!"
          );
          $pm->finish(1);
        }
        $pplog->debug("  ok.");
      }
      $pm->finish(0);
    };
    if ($@) {
      $pplog->error($@) if ($pplog);
      undef $@;
      $pm->finish(1);
    }
  }
  $pm->wait_all_children;
  croak "SSH Configuration Check Failed!\n" if ($failed);

  $log->info("All SSH connection tests passed successfully.");
}

sub main {
  @ARGV = @_;
  GetOptions(
    'global_conf=s' => \$g_global_config_file,
    'conf=s'        => \$g_config_file,
  );
  unless ($g_config_file) {
    print "--conf=<server_config_file> must be set.\n";
    return 1;
  }
  my $log  = MHA::ManagerUtil::init_log();
  my $conf = new MHA::Config(
    logger     => $log,
    globalfile => $g_global_config_file,
    file       => $g_config_file
  );
  my ( $sc_ref, undef ) = $conf->read_config();
  my @servers_config = @$sc_ref;
  $log = MHA::ManagerUtil::init_log( undef, "debug" );
  return do_ssh_connection_check( \@servers_config, $log, "debug", "/tmp" );
}

1;

