use inc::Module::Install;

perl_version '5.8.1';

name 'MapReduce-Lite';
all_from 'lib/MapReduce/Lite.pm';
requires 'perl' => '5.8.1';

requires 'threads';
requires 'Exporter::Lite';
requires 'Moose';
requires 'MooseX::Types';
requires 'Thread::Queue::Any';
requires 'Params::Validate';
requires 'List::RubyLike';
requires 'Tie::Hash::Sorted';
requires 'Text::CSV_XS';

build_requires 'Test::More';

auto_install;
WriteAll;
