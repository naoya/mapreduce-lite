package MapReduce::Lite::Reducer;
use Moose::Role;

requires qw/reduce/;

use Params::Validate qw/validate_pos/;

sub emit {
    my ($self, $key, $value) = validate_pos(@_, 1, 1, 1);
    printf "%s => %s\n", $key, $value;
}

1;
