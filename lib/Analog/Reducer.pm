package Analog::Reducer;
use Moose;

with 'MapReduce::Lite::Reducer';

sub reduce {
    my ($self, $key, $values) = @_;
    $self->emit($key, $values->size);
}

1;

