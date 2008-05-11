package MapReduce::Lite::FileIterator;
use Moose;
use MapReduce::Lite::Types qw/File/;

with 'MapReduce::Lite::Role::Iterator';

has handle => (
    is       => 'ro',
    isa      => 'IO::Handle',
    required => 1,
);

sub has_next {
    shift->handle->eof ? 0 : 1;
}

sub next {
    my $line = shift->handle->getline;
    chomp $line;
    $line;
}

1;
