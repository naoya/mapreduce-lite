package MapReduce::Lite::Mapper::Out;
use Moose;

extends 'MapReduce::Lite::BufferedFile';

use Params::Validate qw/validate_pos/;

sub put {
    my ($self, $key, $value) = validate_pos(@_, 1, 1, 1);
    $self->append( sprintf "%s\n", join(':', $key, $value) ); # FIXME
}

1;
