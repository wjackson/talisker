package Talisker::Backend::Simple;

use Moose;
use namespace::autoclean;
use AnyEvent::Hiredis;
use List::MoreUtils qw(pairwise);
use Talisker::Util qw(merge_point chain);
use Time::HiRes;
use JSON;

with 'Talisker::Backend::Role';

sub write {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $points = $args{points};
    my $cb     = $args{cb};

    my $target;

    chain(
        finished => $cb,
        steps    => [

            sub {
                $self->_write_tag_entry(
                    tag => $tag,
                    cb  => $_[1],
                );
            },

            sub {
                my (undef, $cb) = @_;
                $self->_resolve_target(
                    tag => $tag,
                    cb  => sub { $target = shift; $cb->() },
                ),
            },

            sub {
                $self->_write_points(
                    tag    => $target,
                    points => $points,
                    cb     => $_[1],
                );
            },

            sub {
                $self->_write_stamps_index(
                    tag    => $target,
                    points => $points,
                    cb     => $_[1],
                );
            },

            sub {
                $self->_update_mtime (
                    tag    => $target,
                    points => $points,
                    cb     => $_[1],
                );
            },

            sub {
                $self->_update_collections(
                    tag    => $tag,
                    points => $points,
                    cb     => $_[1],
                );
            },

        ],
    );

    return;
}

sub _write_tag_entry {
    my ($self, %args) = @_;

    my $tag = $args{tag};
    my $cb  = $args{cb};

    $self->redis->command(
        ['ZADD', ':tags', 0, $tag], $cb,
    );

    return;
}

sub _resolve_target {
    my ($self, %args) = @_;

    my $tag = $args{tag};
    my $cb  = $args{cb};

    $self->redis->command(
        ['HGETALL', "$tag:meta"], sub {
            my ($meta, $err) = @_;

            return $cb->(undef, $err) if $err;

            my %meta = @{ $meta };

            my $target
                = defined $meta{type} && $meta{type} eq 'link'
                ? $meta{target}
                : $tag
                ;

            $cb->($target);
        },
    );

    return;
}

sub _write_points {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $points = $args{points};
    my $cb     = $args{cb};

    my @point_hset_args
        = map { $_->{stamp} => encode_json($_) }
             @{ $points };

    $self->redis->command(
        ['HMSET', $tag, @point_hset_args], $cb
    );

    return;
}

sub _write_stamps_index {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $points = $args{points};
    my $cb     = $args{cb};

    my @stamp_zset_args
        = map { $_->{stamp}, $_->{stamp} }
             @{ $points };

    $self->redis->command(
        ['ZADD', "$tag:stamps", @stamp_zset_args], $cb
    );

    return;
}

sub _update_mtime {
    my ($self, %args) = @_;

    my $tag = $args{tag};
    my $cb  = $args{cb};

    $self->redis->command(
        ['HSET', "$tag:meta", 'mtime', Time::HiRes::time ], $cb
    );

    return;
}

# TODO: implement this
sub _update_collections {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $points = $args{points};
    my $cb     = $args{cb};

    my $redis  = $self->redis;

    my $work_cb = sub {
        my ($point, $cb) = @_;

        # lookup the points collections

        # update the point's collections
        $cb->();
    };

    my $finished_cb = sub {
        $cb->();
    };

    merge_point(
        inputs   => $points,
        work     => $work_cb,
        finished => $finished_cb,
    );

    return;
}

sub read {
    my ($self, %args) = @_;

    my $tag         = $args{tag};
    my $start_stamp = $args{start_stamp};
    my $end_stamp   = $args{end_stamp};
    my $cb          = $args{cb};

    my $stamps;
    my $points;
    my $target;

    chain(
        steps => [

            # make sure the tag exists
            sub {
                my (undef, $inner_cb) = @_;
                $self->_exists(
                    tag => $tag,
                    cb  => sub { shift && return $inner_cb->(); $cb->() },
                ),
            },

            # resolve the tag in case it's a link
            sub {
                my (undef, $cb) = @_;
                $self->_resolve_target(
                    tag => $tag,
                    cb  => sub { $target = shift; $cb->() },
                ),
            },

            # read from the stamps index
            sub {
                my (undef, $cb) = @_;
                $self->_read_stamps(
                    tag         => $target,
                    start_stamp => $start_stamp,
                    end_stamp   => $end_stamp,
                    cb          => sub { $stamps = shift; $cb->() },
                ),
            },

            # read the points by stamp
            sub {
                my (undef, $cb) = @_;
                $self->_read_points(
                    tag    => $target,
                    stamps => $stamps,
                    cb     => sub { $points = shift; $cb->() },
                ),
            },
        ],

        # make the time series and return it
        finished => sub {
            $cb->({
                tag    => $target,
                points => $points,
            });
        },

    );

    return;
}

sub _read_stamps {
    my ($self, %args) = @_;

    my $tag         = $args{tag};
    my $start_stamp = $args{start_stamp} // '-inf';
    my $end_stamp   = $args{end_stamp}   // '+inf';
    my $cb          = $args{cb};

    $self->redis->command(
        ['ZRANGEBYSCORE', "$tag:stamps", $start_stamp, $end_stamp], $cb
    );

    return;
}

sub _read_points {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $stamps = $args{stamps};
    my $cb     = $args{cb};

    $self->redis->command(
        ['HMGET', $tag, @{ $stamps } ], sub {
            my ($values, $err) = @_;

            return $cb->(undef, $err) if $err;

            my @pts = map { decode_json($_) } @{ $values };

            return $cb->(\@pts);
        },
    );

    return;
}

sub _exists {
    my ($self, %args) = @_;

    my $tag = $args{tag};
    my $cb  = $args{cb};

    $self->redis->command(
        ['ZRANK', ':tags', $tag],
        sub {
            my ($rank, $err) = @_;
            return $cb->(defined $rank);
        },
    );

    return;
}

sub delete {
    my ($self, %args) = @_;

    return defined $args{stamps}
         ? $self->_delete_points(%args)
         : $self->_delete_ts(%args)
         ;
}

sub _delete_points {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $stamps = $args{stamps};
    my $cb     = $args{cb};

    my $redis            = $self->redis;
    my $delete_cnt       = 0;
    my $deletes_expected = 2;

    my $pt_deleted_cb;

    my @point_hdel_args;
    my @stamp_zdel_args;

    for my $stamp (@{ $stamps }) {

        # delete the point
        push @point_hdel_args, $stamp;

        # delete the stamp from the index of the time series' stamps
        push @stamp_zdel_args, $stamp;
    }

    # TODO: merge_point these
    $redis->command(
        ['HDEL', $tag, @point_hdel_args ],
        sub { $pt_deleted_cb->(@_) },
    );

    $redis->command(
        ['ZREM', "$tag:stamps", @stamp_zdel_args ],
        sub { $pt_deleted_cb->(@_) },
    );

    $pt_deleted_cb = sub {
        my ($ok, $err) = @_;

        return $cb->(undef, $err) if $err;

        return if ++$delete_cnt < $deletes_expected;

        $cb->();
    };

    return;
}

sub _delete_ts {
    my ($self, %args) = @_;

    my $tag = $args{tag};
    my $cb  = $args{cb};

    my $redis     = $self->redis;
    my $cmds_run  = 0;
    my $cmds_ret  = 0;

    my $ts_deleted_cb;

    $cmds_run++;
    $redis->command(
        ['ZREM', ':tags', $tag ],
        sub { $ts_deleted_cb->(@_) },
    );

    $cmds_run++;
    $redis->command(
        ['DEL', $tag ],
        sub { $ts_deleted_cb->(@_) },
    );

    $cmds_run++;
    $redis->command(
        ['DEL', "$tag:stamps" ],
        sub { $ts_deleted_cb->(@_) },
    );

    $cmds_run++;
    $redis->command(
        ['DEL', "$tag:meta" ],
        sub { $ts_deleted_cb->(@_) },
    );

    $ts_deleted_cb = sub {
        $cmds_ret++;
        my (undef, $err) = @_;

        return $cb->(undef, $err) if $err;
        return $cb->()            if $cmds_run == $cmds_ret;
    };

    return;
}

sub compact {
}

1;
