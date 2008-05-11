package MapReduce::Lite;
use strict;
use warnings;
use Exporter::Lite;

use MapReduce::Lite::Spec;
use MapReduce::Lite::Conduit;
use Params::Validate qw/validate_pos/;

our $VERSION = 0.01;
our @EXPORT = qw/mapreduce/;

sub mapreduce ($) {
    my ($spec) = validate_pos(@_, { isa => 'MapReduce::Lite::Spec' });

    $spec->inputs->collect(sub {
        $_->mapper->num_reducers( $spec->out->num_tasks );
        my $iter = $_->iterator;
        while ($iter->has_next) {
            $_->mapper->map( $_->file => $iter->next );
        }
        $_->mapper->done;
    });

    my $conduit = MapReduce::Lite::Conduit->new;
    while (my $file = $spec->intermidate_dir->next) {
        next if $file->is_dir;
        $conduit->consume( $file );
    }

    my $iter = $conduit->iterator;
    while ($iter->has_next) {
        $spec->out->reducer->reduce($iter->next);
    }
}

1;
