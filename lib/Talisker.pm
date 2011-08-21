package Talisker;
# ABSTRACT: time series store

use Moose;
use namespace::autoclean;
use Talisker::Util qw(chain);

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

    chain(
        steps => [

            # set tags entry
            sub {
                $self->redis->command(
                    ['ZADD', 'tags', 0, $tag], $_[1]
                )
            },

            # set type to link
            sub {
                $self->redis->command(
                    ['HSET', "$tag:meta", 'type', 'link'], $_[1]
                );
            },

            # set link target
            sub {
                $self->redis->command(
                    ['HSET', "$tag:meta", 'target', $target], $_[1]
                );
            },
        ],
        finished => sub { $cb->(@_) },
    );

    return;
}

sub resolve_link {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $cb     = $args{cb};

    chain(
        steps => [

            # read tag entry
            sub {
                my (undef, $inner_cb) = @_;
                $self->redis->command(
                    ['ZRANK', 'tags', $tag], sub {
                        my ($rank, $err) = @_;

                        return $inner_cb->(undef, $err) if $err;
                        return $cb->()            if !defined $rank;
                        return $inner_cb->();
                    },
                );
            },

            # read target from meta
            sub {
                my (undef, $inner_cb) = @_;

                $self->ts_meta(tag => $tag, cb => sub {
                    my ($meta, $err) = @_;

                    return $inner_cb->(undef, $err) if $err;

                    # error if tag isn't a link
                    return $inner_cb->(undef, qq/$tag isn't a link/)
                        if !defined $meta->{type} || $meta->{type} ne 'link';

                    return $inner_cb->($meta->{target});
                });
            },

        ],
        finished => $cb
    );

    return;
}

sub ts_meta {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $cb     = $args{cb};

    $self->redis->command(
        ['HGETALL', "$tag:meta"], sub {
            my ($meta, $err) = @_;

            return $cb->(undef, $err) if $err;
            return $cb->({ @{ $meta // [] } });
        },
    );

    return;
}

__PACKAGE__->meta->make_immutable;
1;
