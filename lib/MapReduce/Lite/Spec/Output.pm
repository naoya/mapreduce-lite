package MapReduce::Lite::Spec::Output;
use Moose;

use MapReduce::Lite::Types qw/Reducer/;

has reducer => (
    is       => 'rw',
    does     => 'Reducer',
    coerce   => 1,
);

1;
