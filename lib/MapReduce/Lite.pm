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
        my $fh = $_->file->openr or die $!;
        while (my $line = $fh->getline) {
            chomp $line;
            $_->mapper->map( $_->file => $line );
        }
        $fh->close;
        $_->mapper->done;
    });

    my $conduit = MapReduce::Lite::Conduit->new;

    while (my $file = $spec->intermidate_dir->next) {
        next if $file->is_dir;

        my $fh = $file->openr or die "Can't open %s: %s", $file, $!;
        while (my $line = $fh->getline) {
            chomp $line;
            my ($key, $value) = split ':', $line;
            $conduit->put($key => $value);
        }

        $fh->close;
        $file->remove or die "Can't remove %s: %s", $file, $!;
    }

    my $iter = $conduit->iterator;
    while (my ($key, $values) = $iter->next) {
        $spec->out->reducer->reduce($key, $values);
    }
}

1;
