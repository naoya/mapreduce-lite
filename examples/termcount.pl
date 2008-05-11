#!/usr/bin/env/perl

package TermCount::Mapper;
use Moose;
with 'MapReduce::Lite::Mapper';

sub map {
    my ($self, $key, $value) = @_;
    for (split /:/, $value) {
        next unless $_;
        if (! m!^\d+$!) {
            $self->emit($_ => 1);
        }
    }
}

package TermCount::Reducer;
use Moose;
with 'MapReduce::Lite::Reducer';

sub reduce {
    my ($self, $key, $values) = @_;
    $self->emit( $key => $values->size );
}

package main;

use FindBin::libs;
use MapReduce::Lite;

my $spec = MapReduce::Lite::Spec->new(intermidate_dir => "./tmp");

for (@ARGV) {
    my $in = $spec->create_input;
    $in->file($_);
    $in->mapper('TermCount::Mapper');
}

$spec->out->reducer('TermCount::Reducer');
$spec->out->num_tasks(1);

mapreduce($spec);
