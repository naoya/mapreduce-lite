package MapReduce::Lite::Conduit::Iterator;
use Moose;
with 'MapReduce::Lite::Role::Iterator';

has size    => ( is => 'rw', isa => 'Int');
has conduit => (
    is  => 'rw',
    isa => 'MapReduce::Lite::Conduit',
    trigger => sub {
        my ($self, $conduit) = @_;
        $self->size( $conduit->size )
    }
);

sub has_next {
    shift->size > 0;
}

sub next {
    my $self = shift;
    $self->size( $self->size - 1 );
    $self->conduit->each;
}

1;
