package MapReduce::Lite::Spec::Input;
use Moose;
use MapReduce::Lite::Types qw/Directory Mapper/;

has intermidate_dir => (
    is       => 'rw',
    does     => 'Directory',
    coerce   => 1,
    required => 1,
);

has mapper => (
    is       => 'rw',
    does     => 'Mapper',
    coerce   => 1,
    trigger  => sub {
        my ($self, $mapper) = @_;
        $mapper->intermidate_dir( $self->intermidate_dir );
    }
);

has file => (
    is     => 'rw',
    isa    => 'File',
    coerce => 1,
);

1;
