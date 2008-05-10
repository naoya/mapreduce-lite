package MapReduce::Lite::Mapper;
use strict;
use warnings;

use Moose;
use Moose::Util::TypeConstraints;

use Path::Class qw/dir/;
use Params::Validate qw/validate_pos SCALAR/;

use MapReduce::Lite::Buffer;

subtype 'Path::Class'
    => as 'Object',
    => where { $_->isa('Path::Class::Dir')  };

coerce 'Path::Class'
    => from 'Str',
    => via { dir $_ };

has intermidate_dir => ( is => 'rw', isa => 'Path::Class', coerce => 1 );
has _buffer_pool    => ( is => 'ro', default => sub { [] } );

sub emit {
    my ($self, $key, $value) = validate_pos(@_, 1, 1, 1);

    ## FIXME
    my $R = 0;

    # $self->intermidate_buffers( $r )->put( $key => $value );
    warn $self->intermidate_buffers( 0 );
}

sub intermidate_buffers {
    my ($self, $R) = validate_pos(@_, 1, { type => SCALAR });

    unless ($self->_buffer_pool->[$R]) {
        $self->_buffer_pool->[$R] = MapReduce::Lite::Buffer->new;
    }
}

1;
