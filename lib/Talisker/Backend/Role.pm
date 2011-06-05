package Talisker::Backend::Role;

use Moose::Role;

requires qw(read write compact);

has redis => (
    is       => 'ro',
    isa      => 'AnyEvent::Hiredis',
    required => 1,
);

1;
