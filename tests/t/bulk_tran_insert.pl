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
                    insert into t1 values(?, ?, ?),
(?, ?, ?),
(?, ?, ?),
(?, ?, ?),
(?, ?, ?),
(?, ?, ?),
(?, ?, ?),
(?, ?, ?),
(?, ?, ?),
(?, ?, ?)
SQL
  eval {
    my @params = ();
    for ( my $j = 0 ; $j < 10 ; $j++ ) {
      my $val  = $pk + $j;
      my $val2 = 'aaaaaa' . $pk;
      push @params, $val;
      push @params, $val;
      push @params, $val2;
    }
    $ret = $sth->execute(@params);
    die if ( $ret != 10 );
  };
  if ($@) {
    return 1;
  }

  $pk = $pk + 10;
  if ( $commit_interval == 1 ) {
    $committed_rows = $committed_rows + 10;
  }
  elsif ( $commit_interval > 1 && ( $pk - 1 ) % $commit_interval == 0 ) {
    eval { $dbh->do("COMMIT"); };
    if ($@) {
      return 1;
    }
    $committed_rows += $commit_interval;
  }
}
