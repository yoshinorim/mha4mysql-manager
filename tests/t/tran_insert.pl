#!/usr/bin/perl

use strict;
use warnings;
use DBI;

my ( $port, $user, $pass, $start, $stop, $commit_interval ) = @ARGV;
my $pk             = $start;
my $committed_rows = 0;
my $dsn            = "DBI:mysql:test;host=[127.0.0.1];port=$port";
my $dbh            = DBI->connect( $dsn, $user, $pass );
if ( $commit_interval == 1 ) {
  $dbh->do("SET AUTOCOMMIT=1");
}
else {
  $dbh->do("SET AUTOCOMMIT=0");
}

while ( $pk <= $stop ) {
  my ( $sth, $ret );
  $sth = $dbh->prepare(<<'SQL');
                    insert into t1 values(?, ?, ?)
SQL
  eval {
    $ret = $sth->execute( $pk, $pk, 'aaaaaa' . $pk );
    die if ( $ret != 1 );
  };
  if ($@) {
    return 1;
  }

  if ( $commit_interval == 1 ) {
    $committed_rows++;
  }
  elsif ( $commit_interval > 1 && $pk % $commit_interval == 0 ) {
    eval { $dbh->do("COMMIT"); };
    if ($@) {
      return 1;
    }
    $committed_rows += $commit_interval;
  }
  $pk++;
}
