package MapReduce::Lite::Spec::Output;
use Moose;

use MapReduce::Lite::Types qw/Reducer/;

has reducer => (
    is       => 'rw',
    does     => 'Reducer',
    coerce   => 1,
);

has num_tasks => (
    is      => 'rw',
    default => 1,
);

1;
