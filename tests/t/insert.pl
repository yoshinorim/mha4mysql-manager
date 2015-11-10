#!/usr/bin/perl

use strict;
use warnings;
use DBI;

my ( $port, $user, $pass, $start, $stop, $single_tran, $rollback ) = @ARGV;
$rollback = 0 if ( !defined($rollback) );

my $dsn = "DBI:mysql:test;host=[127.0.0.1];port=$port";
my $dbh = DBI->connect( $dsn, $user, $pass );

my $sth;
if ($single_tran) {
  $sth = $dbh->prepare("BEGIN");
  $sth->execute();
}
for ( my $i = $start ; $i <= $stop ; $i++ ) {
  $sth = $dbh->prepare("INSERT INTO t1 VALUES(?, ?, 'aaaaaaa')");
  my @params;
  push @params, $i;
  push @params, $i;
  $sth->execute(@params);
}
if ($single_tran) {
  if ($rollback) {
    $sth = $dbh->prepare("ROLLBACK");
  }
  else {
    $sth = $dbh->prepare("COMMIT");
  }
  $sth->execute();
}
