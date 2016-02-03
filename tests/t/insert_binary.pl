#!/usr/bin/perl

use strict;
use warnings FATAL=>'all';
use DBI;

my ( $port, $user, $pass ) = @ARGV;

my $file= "/tmp/test.bin";
open my $out, ">", $file or die "file open error $!\n";
binmode $out;

print $out pack('C*', 65, 66, 67, 68, 69, 13, 10, 68, 69);
#print $out pack('C*', 65, 66, 67, 0, 68, 69, 13, 10, 68, 69);
close($out);

open my $in , "<", $file or die "file open error $!\n";
binmode $in;
my $buf;
read($in, $buf, -s $file);
close($in);
unlink $file;

my $dsn = "DBI:mysql:test;host=[127.0.0.1];port=$port";
my $dbh = DBI->connect( $dsn, $user, $pass );

my $value= $buf;
my $query= "INSERT INTO binfile (bin_data) VALUES ('$value')";
my $sth= $dbh->prepare($query);
$sth->execute();
$dbh->disconnect();

