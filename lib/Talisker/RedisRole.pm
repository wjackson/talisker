package Talisker::RedisRole;

use Moose::Role;
use namespace::autoclean;
use Talisker::Types qw(RedisDatabaseNumber);
use AnyEvent::Hiredis;

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

has default_db => (
    is      => 'ro',
    isa     => RedisDatabaseNumber,
    default => 1,
);

has redis => (
    is         => 'ro',
    isa        => 'AnyEvent::Hiredis',
    lazy_build => 1,
);

sub _build_redis {
    my ($self) = @_;

    my $redis = AnyEvent::Hiredis->new(
        host => $self->host,
        port => $self->port,
    );

    # this can't fail (maybe)
    $redis->command(['SELECT', $self->default_db], sub {});

    return $redis;
}

1;
