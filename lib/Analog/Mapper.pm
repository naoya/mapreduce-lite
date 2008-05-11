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

1;
