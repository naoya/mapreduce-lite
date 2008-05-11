package MapReduce::Lite::BufferedFile;
use Moose;

use MapReduce::Lite::Buffer;

has buffer => (
    is      => 'ro',
    isa     => 'MapReduce::Lite::Buffer',
    lazy    => 1,
    handles => [qw/append/],
    default => sub {
        MapReduce::Lite::Buffer->new
    }
);

has handle => (
    is       => 'rw',
    isa      => 'IO::Handle',
    required => 1,
);

has flush_size => (
    is  => 'rw',
    isa => 'Int',
);

after 'append' => sub {
    my $self = shift;
    if ($self->flush_size and $self->buffer->size >= $self->flush_size) {
        $self->flush;
    }
};

sub flush {
    my $self = shift;
    $self->buffer->flush( $self->handle );
}

sub close {
    my $self = shift;
    if ($self->buffer->size) {
        $self->flush;
    }
    $self->handle->close;
}

sub DESTROY {
    my $self = shift;
    if ($self->handle and $self->handle->opened) {
        $self->close;
    }
}

1;
