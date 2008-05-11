package MapReduce::Lite::Conduit;
use Moose;

use Text::CSV_XS;
use Data::Dumper;
use Tie::Hash::Sorted;
use List::RubyLike;
use Params::Validate qw/validate_pos/;

use MapReduce::Lite::FileIterator;
use MapReduce::Lite::Conduit::Iterator;

has 'csv' => (
    is      => 'ro',
    default => sub {
        Text::CSV_XS->new;
    }
);

has 'data' => (
    is      => 'ro',
    default => sub {
        tie my %sorted, 'Tie::Hash::Sorted';
        \%sorted;
    }
);

sub put {
    my ($self, $key, $value) = validate_pos(@_, 1, 1, 1);
    if (my $list = $self->data->{ $key }) {
        $list->push($value);
    } else {
        $self->data->{ $key } = list( $value );
    }
}

sub dump {
    my $self = shift;
    Data::Dumper::Dumper($self->data);
}

sub each {
    my $self = shift;
    each %{ $self->data };
}

sub size {
    scalar keys %{shift->data};
}

sub iterator {
    my $self = shift;
    my $iter = MapReduce::Lite::Conduit::Iterator->new;
    $iter->conduit( $self );
    $iter;
}

sub consume {
    my ($self, $file) = validate_pos(@_, 1, { isa => 'Path::Class::File' });

    my $iter = MapReduce::Lite::FileIterator->new(handle => $file->openr);
    while ($iter->has_next) {
        $self->csv->parse( $iter->next );
        $self->put( $self->csv->fields );
    }

    $file->remove or die "Can't remove %s: %s", $file, $!;
}

1;
