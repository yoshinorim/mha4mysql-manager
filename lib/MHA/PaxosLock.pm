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

package MHA::PaxosLock;

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
use MHA::ManagerUtil;
use MHA::ManagerConst;
use File::Basename;
use Parallel::ForkManager;
use Sys::Hostname;

my $log;
my $g_global_config_file = $MHA::ManagerConst::DEFAULT_GLOBAL_CONF;
my $g_config_file;
my $g_workdir;
my $g_logfile;
my $_server_manager;
my $g_lock;
my $_start_datetime;
my $g_holder;

sub exit_by_signal {
  $log->info("Got terminate signal during paxos_lock. Exit.");
  exit 1;
}

sub init_config() {
    $log = MHA::ManagerUtil::init_log($g_logfile);

    my @servers_config = new MHA::Config(
        logger     => $log,
        globalfile => $g_global_config_file,
        file       => $g_config_file
    )->read_config();

    if ( !$g_logfile
        && $servers_config[0]->{manager_log} )
    {
        $g_logfile = $servers_config[0]->{manager_log};
    }
    $log =
        MHA::ManagerUtil::init_log( $g_logfile, $servers_config[0]->{log_level} );
    $log->info("MHA::PaxosLock version $MHA::ManagerConst::VERSION.");

    unless ($g_workdir) {
        if ( $servers_config[0]->{remote_workdir} ) {
            $g_workdir = $servers_config[0]->{remote_workdir};
        } else {
            $log->error("Could not find remote_workdir in configuration file.");
            croak;
        }
    }
    return ( \@servers_config );
}

sub check_settings($) {
    my $servers_config_ref = shift;
    my @servers_config     = @$servers_config_ref;
    MHA::ManagerUtil::check_node_version($log);
    $_server_manager = new MHA::ServerManager( servers => \@servers_config );
    $_server_manager->set_logger($log);
}

sub do_lock() {
    my ( $servers_config_ref ) = init_config();
    check_settings($servers_config_ref);
    
    if ($g_lock) {
        $log->info("Starting paxos lock.");
    } else {
        $log->info("Starting paxos unlock.");
    }

    my @servers = $_server_manager->get_servers();
    my $locker = new Parallel::ForkManager( $#servers + 1 );
    my $locker_failed_cnt = 0;
    $locker->run_on_start(
        sub {
            my ( $pid, $target ) = @_;
        }
    );
    $locker->run_on_finish(
        sub {
            my ( $pid, $exit_code, $target ) = @_;
            if ($exit_code) {
                $log->info("  $target->{hostname} $target->{id} failed.");
                $locker_failed_cnt += 1;
            }
        }
    );
    foreach my $target (@servers) {
        $locker->start($target) and next;
        my $command;
        my $local_file =
        "$g_workdir/paxos_lock_$target->{hostname}_$target->{id}_$_start_datetime.log";
        if ( -f $local_file ) {
            unlink $local_file;
        }
        my $ssh_user_host = $target->{ssh_user} . '@' . $target->{ssh_ip};
        if ($g_lock) {
            $command = "try_paxos_lock --workdir=$g_workdir --holder=$g_holder";
        } else {
            $command = "try_paxos_lock --workdir=$g_workdir --holder=$g_holder --unlock";
        }
        my ( $high, $low ) =
            MHA::ManagerUtil::exec_ssh_cmd( $ssh_user_host, $target->{ssh_port},
                $command, $local_file );
        if ( $high == 0 && $low == 0 ) {
            $locker->finish(0);
        } else {
            $locker->finish(1);
        }
    }
    $locker->wait_all_children;
    my $locker_total_cnt = $#servers + 1;
    if ($locker_failed_cnt >= $locker_total_cnt / 2) {
        if ($g_lock) {
            $log->error("Lock failed! failed:$locker_failed_cnt total:$locker_total_cnt.");
        } else {
            $log->error("Unlock failed! failed:$locker_failed_cnt total:$locker_total_cnt.");
        }
        croak;
    }
    if ($g_lock) {
        $log->info("Locked successfully(holder: $g_holder)! failed:$locker_failed_cnt total:$locker_total_cnt.");
    } else {
        $log->info("Unlocked successfully(holder: $g_holder)! failed:$locker_failed_cnt total:$locker_total_cnt.");
    }
    return 0;
}

sub main {
    local $SIG{INT} = $SIG{HUP} = $SIG{QUIT} = $SIG{TERM} = \&exit_by_signal;
    local @ARGV = @_;
    my ($unlock, $error_code);
    my ( $year, $mon, @time ) = reverse( (localtime)[ 0 .. 5 ] );
    $_start_datetime = sprintf '%04d%02d%02d%02d%02d%02d', $year + 1900, $mon + 1,
        @time;
    
    $g_lock = 1;
    
    GetOptions(
        'global_conf=s'              => \$g_global_config_file,
        'conf=s'                     => \$g_config_file,
        'lock'                       => \$g_lock,
        'unlock'                     => \$unlock,
        'holder=s'                   => \$g_holder,
    );
    
    unless ($g_holder) {
        croak "holder is not specified.\n";
    }
    
    if ($unlock) {
        $g_lock = 0;
    }
    
    $error_code = do_lock();
    return $error_code;
}

1;