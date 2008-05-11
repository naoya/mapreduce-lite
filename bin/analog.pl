#!/usr/local/bin/perl
use strict;
use warnings;

use FindBin::libs;
use MapReduce::Lite;

my $spec = MapReduce::Lite::Spec->new(intermidate_dir => "./tmp");

for (@ARGV) {
    my $in = $spec->create_input;
    $in->file($_);
    $in->mapper('Analog::Mapper');
#    $in->partitioning_function(sub {
#        my ($key, $R) = @_;
#        $key % $R;
#    });
}

$spec->out->reducer('Analog::Reducer');
$spec->out->num_tasks(3);

mapreduce $spec;
