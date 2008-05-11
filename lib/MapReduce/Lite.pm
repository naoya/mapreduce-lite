package MapReduce::Lite;
use strict;
use warnings;
use threads;
use Exporter::Lite;

use Thread::Queue::Any;
use MapReduce::Lite::Spec;
use MapReduce::Lite::Conduit;
use Params::Validate qw/validate_pos/;

our $VERSION = 0.01;
our @EXPORT = qw/mapreduce/;

sub mapreduce ($) {
    my ($spec) = validate_pos(@_, { isa => 'MapReduce::Lite::Spec' });

    do_map( $spec );

    my $conduit = MapReduce::Lite::Conduit->new;
    while (my $file = $spec->intermidate_dir->next) {
        next if $file->is_dir;
        $conduit->consume( $file );
    }

    do_reduce( $spec, $conduit );
}

sub do_map {
    my ($spec) = validate_pos(@_, 1);
    my $queue = Thread::Queue::Any->new;

    for my $in (@{$spec->inputs}) {
        my $iter = $in->iterator;
        while ($iter->has_next) {
            $queue->enqueue([ $in->file => $iter->next ]);
        }

        $in->mapper->num_reducers( $spec->out->num_tasks );

        for (my $i = 0; $i < $spec->num_threads; $i++) {
            threads->create(map_thread => $queue, $in->mapper);
        }
        $_->join for threads->list;
    }
}

sub map_thread {
    my ($queue, $mapper) = validate_pos(@_, 1, 1);

    while (my $left = $queue->pending) {
        my ($task) = $queue->dequeue;
        $mapper->map(@$task);
    }

    $mapper->done;
}

sub do_reduce {
    my ($spec, $conduit) = validate_pos(@_, 1, 1);
    my $queue = Thread::Queue::Any->new;

    my $iter = $conduit->iterator;
    while ($iter->has_next) {
        $queue->enqueue([ $iter->next ]);
    }

    for (my $i = 0; $i < $spec->num_threads; $i++) {
        threads->create(reduce_thread => $queue, $spec->out->reducer);
    }
    $_->join for threads->list;
}

sub reduce_thread {
    my ($queue, $reducer) = validate_pos(@_, 1, 1);

    while (my $left = $queue->pending) {
        my ($task) = $queue->dequeue;
        $reducer->reduce(@$task);
    }
}

1;

=head1 NAME

MapReduce::Lite - An experimental implementation of MapReduce for perl

=head1 AUTHOR

Naoya Ito, E<lt>naoya at hatena ne jp<gt>

=head1 COPYRIGHT AND LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
