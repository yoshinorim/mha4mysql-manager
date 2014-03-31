use inc::Module::Install;

name 'mha4mysql-manager';

version_from 'lib/MHA/ManagerConst.pm';

requires 'DBI';
requires 'DBD::mysql';
requires 'Time::HiRes';
requires 'Config::Tiny';
requires 'Log::Dispatch';
requires 'Parallel::ForkManager';
requires 'MHA::NodeConst';

license 'GPL v2';
author 'Yoshinori Matsunobu <Yoshinori.Matsunobu@gmail.com>';

if ($ENV{REWRITE_SHEBANG}) {
    mkdir 'xbin';
    for my $bin (glob 'bin/*') {
        (my $xbin = $bin) =~ s{^bin/}{xbin/};
        open my $in,  '<', $bin  or die $!;
        open my $out, '>', $xbin or die $!;
        while (<$in>) {
            s|^#!/usr/bin/env perl|#!perl|; # so MakeMaker can fix it
            print $out $_;
        }
        close $in;
        close $out;
    }
    install_script(glob 'xbin/*');
} else {
    install_script(glob 'bin/*');
}
auto_install;
WriteAll;
