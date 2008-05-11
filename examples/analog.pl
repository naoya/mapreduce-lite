#!/usr/bin/env/perl

package Analog::Mapper;
use Moose;
with 'MapReduce::Lite::Mapper';

sub map {
    my ($self, $key, $value) = @_;
    my @elements = split /\s+/, $value;
    if ($elements[8]) {
        $self->emit($elements[8], 1);
    }
}

package Analog::Reducer;
use Moose;

with 'MapReduce::Lite::Reducer';

sub reduce {
    my ($self, $key, $values) = @_;
    $self->emit($key, $values->size);
}

1;

package main;
use FindBin::libs;
use MapReduce::Lite;

my $spec = MapReduce::Lite::Spec->new(intermidate_dir => "./tmp");

for (@ARGV) {
    my $in = $spec->create_input;
    $in->file($_);
    $in->mapper('Analog::Mapper');
}

$spec->out->reducer('Analog::Reducer');
$spec->out->num_tasks(3);

mapreduce($spec);
