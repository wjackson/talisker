package Talisker::Backend::Role;

use Moose::Role;
use JSON;
use Talisker::Util qw(chain);

requires qw(read write delete);

has redis => (
    is       => 'ro',
    isa      => 'AnyEvent::Hiredis',
    required => 1,
);


sub tags {
    my ($self, %args) = @_;

    my $start_idx = $args{start_idx} // 0;
    my $end_idx   = $args{end_idx}   // -1;
    my $cb        = $args{cb};

    $self->redis->command(
        ['ZRANGE', ':tags', $start_idx, $end_idx], sub {
            my ($tags, $err) = @_;

            return $cb->(undef, $err) if $err;
            return $cb->($tags);
        },
    );

    return;
}

sub count {
    my ($self, %args) = @_;

    my $cb = $args{cb};

    $self->redis->command(
        ['ZCARD', ':tags'], sub {
            my ($count, $err) = @_;

            return $cb->(undef, $err) if $err;
            return $cb->($count);
        },
    );

    return;
}

sub write_fields {
    my ($self, %args) = @_;

    my $fields = $args{fields};
    my $cb     = $args{cb};

    $self->redis->command(['SET', ':fields', encode_json($fields)], $cb);
}

sub read_fields {
    my ($self, %args) = @_;

    my $cb = $args{cb};

    $self->redis->command(['GET', ':fields'], sub {
        my ($fields_json, $err) = @_;

        $cb->(undef, $err) if $err;

        return $cb->(decode_json($fields_json));
    });
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
                    ['ZADD', ':tags', 0, $tag], $_[1]
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
                    ['ZRANK', ':tags', $tag], sub {
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

1;
