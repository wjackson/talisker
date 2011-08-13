package Talisker;
# ABSTRACT: time series store

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
            },
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
        );
    };

    $set_tags_entry->();

    return;
}

sub resolve_link {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $cb     = $args{cb};

    my ($read_tag_entry, $read_meta);

    $read_tag_entry = sub {
        $self->redis->command(
            ['ZRANK', 'tags', $tag], sub {
                my ($rank, $err) = @_;

                return $cb->(undef, $err) if $err;
                return $cb->()            if !defined $rank;

                return $read_meta->();
            },
        );
    };

    $read_meta = sub {
        $self->redis->command(
            [ 'HGETALL', "$tag:meta" ], sub {
                my ($meta, $err) = @_;

                return $cb->(undef, $err) if $err;

                my %meta = @{ $meta // [] };

                return $cb->(undef, qq/$tag isn't a link/)
                    if defined $meta{type} && $meta{type} ne 'link';

                return $cb->($meta{target});
            },
        );
    };

    $read_tag_entry->();

    return;
}

__PACKAGE__->meta->make_immutable;
1;
