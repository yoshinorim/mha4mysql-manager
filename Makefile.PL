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

install_script(glob 'bin/*');
auto_install;
WriteAll;
