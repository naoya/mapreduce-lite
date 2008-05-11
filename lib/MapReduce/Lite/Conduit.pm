package MapReduce::Lite::Conduit;
use Moose;

use Data::Dumper;
use Tie::Hash::Sorted;
use List::RubyLike;
use Params::Validate qw/validate_pos/;

use MapReduce::Lite::Conduit::Iterator;

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

1;
