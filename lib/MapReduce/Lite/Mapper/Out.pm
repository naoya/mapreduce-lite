package MapReduce::Lite::Mapper::Out;
use Moose;

extends 'MapReduce::Lite::BufferedFile';

use Text::CSV_XS;
use Params::Validate qw/validate_pos/;

has csv => (
    is      => 'ro',
    default => sub {
        Text::CSV_XS->new({ binary => 1 });
    }
);

sub put {
    my ($self, $key, $value) = validate_pos(@_, 1, 1, 1);
    $self->csv->combine($key, $value);
    $self->append(sprintf "%s\n", $self->csv->string);
}

1;
