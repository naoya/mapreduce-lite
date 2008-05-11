package MapReduce::Lite::Buffer;
use Moose;
use overload '""' => 'as_string';
use threads::shared;
use Params::Validate qw/validate_pos/;

my $mutex : shared;

has buffer => (is => 'rw', default => sub { '' });

sub size {
    use bytes;
    return length shift->buffer;
}

sub append {
    my ($self, $val) = @_;
    $self->buffer( join '', $self->buffer, $val );
}

sub clear {
    shift->buffer('');
}

sub flush {
    my ($self, $fh) = validate_pos(@_, 1, { isa => 'IO::Handle' });
    my $len = $self->size;
    {
        lock ($mutex);
        $fh->syswrite($self->as_string);
        $self->clear;
    }
    return $len;
}

sub as_string {
    shift->buffer;
}

1;
