package Talisker;

use namespace::autoclean;
use Moose;

has host => (
    is      => 'ro',
    isa     => 'Str',
    default => '127.0.0.1',
);

has port => (
    is      => 'ro',
    isa     => 'Int',
    default => 6379,
);

has redis => (
    is => 'ro',
    isa => 'AnyEvent::Hiredis',
    lazy_build => 1,
);

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

sub _build_redis {
    my ($self) = @_;

    return AnyEvent::Hiredis->new(
        host => $self->host,
        port => $self->port,
    );
}

sub _build_backend {
    my ($self) = @_;

    my $backend_class = 'Talisker::Backend::' . $self->backend_type;
    Class::MOP::load_class($backend_class);

    return $backend_class->new(redis => $self->redis);
}

1;
