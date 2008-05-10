use inc::Module::Install;

perl_version '5.8.1';

name 'MapReduce-Lite';
all_from 'lib/MapReduce/Lite.pm';
requires 'perl' => '5.8.1';

requires 'Exporter::Lite';

build_requires 'Test::More';

auto_install;
WriteAll;
