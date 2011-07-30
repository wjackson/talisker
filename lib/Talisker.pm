package Talisker;

use Moose;
use namespace::autoclean;

with 'Talisker::RedisRole';

has backend_type => (
    is      => 'ro',
    isa     => 'Str',
    default => 'Revision',
);

has backend => (
    accessor   => 'backend',
    does       => 'Talisker::Backend::Role',
    lazy_build => 1,
    handles    => [ qw(read write delete compact tags count) ],
);

sub _build_backend {
    my ($self) = @_;

    my $backend_class = 'Talisker::Backend::' . $self->backend_type;
    Class::MOP::load_class($backend_class);

    return $backend_class->new(redis => $self->redis);
}

__PACKAGE__->meta->make_immutable;
1;
