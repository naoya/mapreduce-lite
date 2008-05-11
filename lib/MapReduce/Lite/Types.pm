package MapReduce::Lite::Types;
use strict;
use warnings;
use Path::Class;
use UNIVERSAL::require;

use MooseX::Types -declare => [qw/Directory File Mapper Reducer/];
use MooseX::Types::Moose qw/Str Object/;

subtype 'Directory'
    => as 'Object',
    => where { $_->isa('Path::Class::Dir')  };

coerce 'Directory'
    => from 'Str',
    => via { Path::Class::Dir->new($_) };

subtype 'File'
    => as 'Object',
    => where { $_->isa('Path::Class::File') };

coerce 'File'
    => from 'Str'
    => via { Path::Class::File->new($_) };

subtype 'Mapper'
    => as 'Object',
    => where { $_->does('MapReduce::Lite::Mapper')  };

coerce 'Mapper'
    => from 'Str'
    => via {
        $_->require or die $@;
        $_->new;
    };

subtype 'Reducer'
    => as 'Object',
    => where { $_->does('MapReduce::Lite::Reducer')  };

coerce 'Reducer'
    => from 'Str'
    => via {
        $_->require or die $@;
        $_->new;
    };

1;
