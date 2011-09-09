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

package MHA::FileStatus;

use strict;
use warnings FATAL => 'all';

use English qw(-no_match_vars);
use Carp qw(croak);
use File::Basename;
use MHA::ManagerConst;
use MHA::NodeUtil;

sub new {
  my $class = shift;
  my $self  = {
    conffile    => undef,
    dir         => undef,
    status_file => undef,
    basename    => undef,
    master_host => undef,
    @_,
  };
  return bless $self, $class;
}

sub init($) {
  my $self = shift;
  unless ( $self->{basename} ) {
    $self->{basename} = $self->get_basename();
  }
  unless ( $self->{status_file} ) {
    $self->{status_file} =
      "$self->{dir}/" . $self->{basename} . ".master_status.health";
  }
}

sub set_master_host($$) {
  my $self        = shift;
  my $master_host = shift;
  $self->{master_host} = $master_host;
}

# utility function. getting basename from conf file
sub get_basename($) {
  my $self     = shift;
  my $basename = basename( $self->{conffile} );
  $basename =~ s/(.*)\..*?$/$1/;
  if ( $basename eq '' ) {
    $basename = $self->{conffile};
  }
  return $basename;
}

sub update_status {
  my $self          = shift;
  my $status_string = shift;
  my $file          = $self->{status_file};
  my $master_host   = $self->{master_host};
  my $out;
  if ( -f $file ) {
    open( $out, "+<", $file ) or croak "$!:$file";
    flock( $out, 2 );
    truncate( $out, 0 );
    seek( $out, 0, 0 );
  }
  else {
    open( $out, ">", $file ) or croak "$!:$file";
  }
  print $out "$$\t$status_string";
  print $out "\tmaster:$master_host" if ($master_host);
  close($out);
}

sub read_status {
  my $self = shift;
  my $file = $self->{status_file};
  open( my $in, "+<", $file ) or croak "$!:$file";
  flock( $in, 2 );
  my $line          = readline($in);
  my @values        = split( /\t/, $line );
  my $pid           = $values[0];
  my $status_string = $values[1];
  my $master_info;

  if ( $values[2] ) {
    $master_info = $values[2];
  }
  close($in);
  return ( $pid, $status_string, $master_info );
}

sub update_status_time {
  my $self          = shift;
  my $status_string = shift;
  my $file          = $self->{status_file};
  my $master_host   = $self->{master_host};
  if ( !-f $file ) {
    $self->update_status($status_string);
  }
  else {
    my $ftime = time;
    utime $ftime, $ftime, $file;
  }
}

1;
