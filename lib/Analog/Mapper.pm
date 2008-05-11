package Analog::Mapper;
use Moose;

with 'MapReduce::Lite::Mapper';

sub map {
    my ($self, $key, $value) = @_;
    my @elements = split /\s+/, $value;
    $self->emit( $elements[8], 1);
}

1;
