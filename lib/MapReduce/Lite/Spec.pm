package MapReduce::Lite::Spec;
use Moose;

use MapReduce::Lite::Types qw/Directory/;
use MapReduce::Lite::Spec::Input;
use MapReduce::Lite::Spec::Output;

use List::RubyLike;

has intermidate_dir => (
    is       => 'rw',
    does     => 'Directory',
    coerce   => 1,
    required => 1,
);

has inputs => (
    is      => 'ro',
    default => sub { list }
);

has out => (
    is      => 'ro',
    default => sub {
        MapReduce::Lite::Spec::Output->new;
    }
);

has num_threads => (
    is      => 'rw',
    isa     => 'Int',
    default => 5,
);

sub create_input {
    my $self = shift;
    my $in = MapReduce::Lite::Spec::Input->new(
        intermidate_dir => $self->intermidate_dir
    );
    $self->inputs->push( $in );
    return $in;
}

1;
