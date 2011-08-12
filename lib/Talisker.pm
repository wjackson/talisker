package Talisker;

use Moose;
use namespace::autoclean;

with 'Talisker::RedisRole';

has backend_type => (
    is      => 'ro',
    isa     => 'Str',
    default => 'Simple',
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

sub link {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $target = $args{target};
    my $cb     = $args{cb};

    my ($set_tags_entry, $set_type, $set_target);

    $set_tags_entry = sub {
        $self->redis->command(
            ['ZADD', 'tags', 0, $tag], sub {
                my (undef, $err) = @_;

                return $cb->(undef, $err) if $err;
                return $set_type->();
            }
        );
    };

    $set_type = sub {
        $self->redis->command(
            [ 'HSET', "$tag:meta", 'type', 'link' ], sub {
                my (undef, $err) = @_;

                return $cb->(undef, $err) if $err;
                return $set_target->();
            },
        );
    };

    $set_target = sub {
        $self->redis->command(
            [ 'HSET', "$tag:meta", 'target', $target ], sub {
                my (undef, $err) = @_;

                return $cb->(undef, $err) if $err;
                return $cb->();
            },
        )
    };

    $set_tags_entry->();

    return;
}

__PACKAGE__->meta->make_immutable;
1;
