package Talisker::Handle;

use Moose;
use namespace::autoclean;
use Talisker::Types qw(RedisDatabaseNumber BackendType);
use JSON;

has redis => (
    is       => 'ro',
    isa      => 'AnyEvent::Hiredis',
    required => 1,
);

has backend_type => (
    is      => 'ro',
    isa     => BackendType,
    default => 'Simple',
);

has backend => (
    accessor   => 'backend',
    does       => 'Talisker::Backend::Role',
    lazy_build => 1,
    handles    => [ qw(read write delete compact tags count ts_meta link
                       resolve_link count read_fields write_fields exists) ],
);

has db => (
    accessor => 'db',
    isa      => RedisDatabaseNumber,
    required => 1,
);

sub _build_backend {
    my ($self) = @_;

    my $backend_class = 'Talisker::Backend::' . $self->backend_type;
    Class::MOP::load_class($backend_class);

    return $backend_class->new(redis => $self->redis);
}

sub BUILD {
    my ($self) = @_;

    # this can't fail
    $self->redis->command(['SELECT', $self->db], sub {});

    return;
}

__PACKAGE__->meta->make_immutable;
1;
