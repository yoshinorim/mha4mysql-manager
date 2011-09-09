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

package MHA::ManagerAdminWrapper;

use strict;
use warnings FATAL => 'all';

use Getopt::Long qw(:config pass_through);
use MHA::ManagerConst;
use MHA::ManagerAdmin;

my $default_confdir = "/usr/local/masterha/conf";
my $all;
my $app;
my $global_conf = $MHA::ManagerConst::DEFAULT_GLOBAL_CONF;
my $conf;
my $basedir;
my $baselog;
my $internal_basedir;
my $status_dir;
my $logfile;
my $ping_limit = 10;
my $abort;
my $abort_timeout = 5;

sub get_masterha_daemontool_appnames {
  opendir my $dir, "/service";
  my @dirs =
    map { my $s = $_; $s =~ s/^masterha_//; $s }
    grep { m/^masterha_/ } readdir $dir;
  @dirs = sort @dirs;
  closedir $dir;
  return @dirs;
}

sub process_all {
  my $command = shift;
  my @apps    = get_masterha_daemontool_appnames();
  my $ret     = 0;
  foreach my $appname (@apps) {
    $conf = "$default_confdir/$appname.cnf";
    if ($basedir) {
      $status_dir = "$basedir/$appname";
      $app        = $appname;
      undef $conf;
      if ($baselog) {
        $logfile = "$baselog/$appname/$appname.log";
      }
    }
    my $app_ret;
    $app_ret = check_single_app_status() if ( $command == 1 );
    $app_ret = stop_single_app_manager() if ( $command == 2 );
    $ret     = 1                         if ($app_ret);
  }
  return $ret;
}

sub init {
  GetOptions(
    'all'               => \$all,
    'app=s'             => \$app,
    'global_conf=s'     => \$global_conf,
    'conf=s'            => \$conf,
    'basedir=s'         => \$basedir,
    'baselog=s'         => \$baselog,
    'status_dir=s'      => \$status_dir,
    'manager_workdir=s' => \$status_dir,
    'workdir=s'         => \$status_dir,
    'log_output=s'      => \$logfile,
    'manager_log=s'     => \$logfile,
    'ping_limit=i'      => \$ping_limit,
    'abort'             => \$abort,
    'abort_timeout=i'   => \$abort_timeout,
  );
}

sub check_status {
  init();
  if ($all) {
    return process_all(1);
  }
  else {
    if ( $app && !$conf && !$status_dir ) {
      $conf = "$default_confdir/$app.cnf";
    }
    return check_single_app_status();
  }
}

sub stop_manager {
  init();
  if ($all) {
    return process_all(2);
  }
  else {
    if ( $app && !$conf && !$status_dir ) {
      $conf = "$default_confdir/$app.cnf";
    }
    return stop_single_app_manager();
  }
}

sub check_single_app_status {
  return new MHA::ManagerAdmin(
    app         => $app,
    global_conf => $global_conf,
    conf        => $conf,
    status_dir  => $status_dir,
    logfile     => $logfile,
    ping_limit  => $ping_limit
  )->check_status();
}

sub stop_single_app_manager {
  return new MHA::ManagerAdmin(
    app           => $app,
    global_conf   => $global_conf,
    conf          => $conf,
    status_dir    => $status_dir,
    logfile       => $logfile,
    ping_limit    => $ping_limit,
    abort         => $abort,
    abort_timeout => $abort_timeout,
  )->stop_manager();
}

1;
