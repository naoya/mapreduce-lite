package MapReduce::Lite::Mapper;
use Moose::Role;

use Params::Validate qw/validate_pos SCALAR/;
use List::RubyLike;

use MapReduce::Lite::Types qw/Directory/;
use MapReduce::Lite::Mapper::Out;

with 'MapReduce::Lite::Role::Emitter';
requires qw/map/;

has intermidate_dir => (
    is     => 'rw',
    does   => 'Directory',
    coerce => 1
);

has _files => (
    is      => 'ro',
    default => sub { list }
);

has num_reducers => (
    is      => 'rw',
    isa     => 'Int',
    lazy    => 1,
    default => 1,
);

has partitioning_function => (
    is      => 'rw',
    isa     => 'CodeRef',
    lazy    => 1,
    default => sub {
        return sub {
            my ($key, $R) = @_;
            length($key) % $R;
        }
    }
);

before 'intermidate_buffers' => sub {
    my ($self, $R) = validate_pos(@_, 1, { type => SCALAR });
    unless ($self->_files->[$R]) {
        my $file = $self->intermidate_dir->file( sprintf "R%d.dat", $R );
        my $handle = $file->open('>>')
            or confess sprintf "Can't create an intermediate file: %s", $!;

        $self->_files->[$R] = MapReduce::Lite::Mapper::Out->new(
            handle     => $handle,
            flush_size => 1024, # FIXME
        );
    }
};

sub intermidate_buffers {
    my ($self, $id) = validate_pos(@_, 1, { type => SCALAR });
    return $self->_files->[$id];
}

sub emit {
    my ($self, $key, $value) = validate_pos(@_, 1, 1, 1);
    my $id = $self->partitioning_function->($key, $self->num_reducers);
    $self->intermidate_buffers($id)->put($key, $value);
}

sub done {
    shift->_files->each(sub { defined $_ and $_->close });
}

1;
