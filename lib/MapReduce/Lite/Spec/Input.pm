package MapReduce::Lite::Spec::Input;
use Moose;
use MapReduce::Lite::Types qw/Directory Mapper/;
use MapReduce::Lite::FileIterator;

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
        if (my $func = $self->partitioning_function) {
            $mapper->partitioning_function( $func );
        }
        $mapper->intermidate_dir( $self->intermidate_dir );
    }
);

has partitioning_function => (
    is  => 'rw',
    isa => 'CodeRef'
);

has file => (
    is     => 'rw',
    isa    => 'File',
    coerce => 1,
);

sub iterator {
    MapReduce::Lite::FileIterator->new(handle => shift->file->openr);
}

1;
