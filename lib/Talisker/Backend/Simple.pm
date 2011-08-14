package Talisker::Backend::Simple;

use Moose;
use namespace::autoclean;
use AnyEvent::Hiredis;
use List::MoreUtils qw(pairwise);
use Talisker::Util qw(merge_point);

with 'Talisker::Backend::Role';

sub write {
    my ($self, %args) = @_;

    my $tag    = $args{tag};
    my $points = $args{points};
    my $cb     = $args{cb};

    my $redis           = $self->redis;

    my @point_hset_args;
    my @stamp_zset_args;

    for my $pt (@{ $points }) {

        my $stamp = $pt->{stamp};
        my $value = $pt->{value};

        # store the point
        push @point_hset_args, $stamp, $value;

        # index of the time series' stamps
        push @stamp_zset_args, $stamp, $stamp;
    }

    my ($write_tag, $read_meta, $write_pts, $write_stamps);

    # make sure there's a tag entry for $tag
    $write_tag = sub {
        $redis->command(
            ['ZADD', 'tags', 0, $tag], sub {
                my ($meta, $err) = @_;
                return $cb->(undef, $err) if $err;
                $read_meta->();
            },
        );
    };

    # read meta info and switch tag if this is a link
    $read_meta = sub {
        $redis->command(
            ['HGETALL', "$tag:meta"], sub {
                my ($meta, $err) = @_;

                return $cb->(undef, $err) if $err;

                my %meta = @{ $meta };

                if (exists $meta{type} && $meta{type} eq 'link') {
                    $tag = $meta{target};
                }

                $write_pts->();
            },
        );
    };

    # write out all the pts
    $write_pts = sub {
        $redis->command(
            ['HMSET', $tag, @point_hset_args], sub {
                my (undef, $err) = @_;

                return $cb->(undef, $err) if $err;

                $write_stamps->();
            },
        );
    };

    # write to the stamps index
    $write_stamps = sub {
        $redis->command(
            ['ZADD', "$tag:stamps", @stamp_zset_args], sub {
                my (undef, $err) = @_;

                return $cb->(undef, $err) if $err;

                $cb->();
            },
        );
    };

    # go
    $write_tag->();

    return;
}

sub read {
    my ($self, %args) = @_;

    my $tag         = $args{tag};
    my $start_stamp = $args{start_stamp} // '-inf';
    my $end_stamp   = $args{end_stamp}   // '+inf';
    my $cb  = $args{cb};

    my $redis        = $self->redis;
    my $pts          = [];
    my $pts_read_cnt = 0;
    my $pts_read_exp;

    my ($exists_cb, $meta_cb, $stamps_cb, $mtimes_cb, $pts_cb);

    my @stamps;
    my @values;

    # does the ts exist?
    $redis->command(
        ['ZRANK', 'tags', $tag],
        sub {
            my ($rank, $err) = @_;

            return $cb->(undef, $err) if $err;
            return $cb->()            if !defined $rank;

            $exists_cb->();
        },
    );

    # get the ts' meta data
    $exists_cb = sub {
        $redis->command(
            ['HGETALL', "$tag:meta"],
            sub {
                my ($meta, $err) = @_;

                $cb->(undef, $err) if $err;

                return $meta_cb->() if !@{ $meta };

                my %meta = @{ $meta };

                if (exists $meta{type} && $meta{type} eq 'link') {
                    $tag = $meta{target};
                }

                $meta_cb->();
            },
        );
    };

    # read the ts' stamps index
    $meta_cb = sub {
        $redis->command(
            ['ZRANGEBYSCORE', "$tag:stamps", $start_stamp, $end_stamp],
            sub {
                my ($stamps, $err) = @_;

                return $cb->(undef, $err) if $err;

                @stamps = @{ $stamps };

                return $cb->({ tag => $tag, points => [] })
                    if !@stamps;

                $stamps_cb->();
            },
        );
    };

    # read the ts' points
    $stamps_cb = sub {
        $redis->command(
            ['HMGET', $tag, @stamps ],
            sub {
                my ($values, $err) = @_;

                return $cb->(undef, $err) if $err;

                @values = @{ $values };
                $pts_cb->();
            },
        );
    };

    # construct a ts data structure
    $pts_cb = sub {
        my @pts = pairwise { { stamp => $a, value => $b } } @stamps, @values;

        $cb->({
            tag    => $tag,
            points => \@pts,
        });
    };

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
        ['ZREM', 'tags', $tag ],
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

sub tags {
    my ($self, %args) = @_;

    my $start_idx = $args{start_idx} // 0;
    my $end_idx   = $args{end_idx}   // -1;
    my $cb        = $args{cb};

    $self->redis->command(
        ['ZRANGE', 'tags', $start_idx, $end_idx], sub {
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
        ['ZCARD', 'tags'], sub {
            my ($count, $err) = @_;

            return $cb->(undef, $err) if $err;
            return $cb->($count);
        },
    );

    return;
}

sub compact {
}

1;
