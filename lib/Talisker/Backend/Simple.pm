package Talisker::Backend::Simple;

use Moose;
use namespace::autoclean;
use AnyEvent::Hiredis;
use List::MoreUtils qw(pairwise);

with 'Talisker::Backend::Role';

sub write {
    my ($self, %args) = @_;

    my $tag         = $args{tag};
    my $points      = $args{points};
    my $ts_write_cb = $args{callback};

    my $redis           = $self->redis;
    my $writes_cnt      = 0;
    my $writes_expected = 3;

    my $pt_written_cb;

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

    $redis->command(
        ['ZADD', 'tags', 0, $tag],
        sub { $pt_written_cb->(@_) },
    );

    $redis->command(
        ['HMSET', $tag, @point_hset_args],
        sub { $pt_written_cb->(@_) },
    );

    $redis->command(
        ['ZADD', "$tag:stamps", @stamp_zset_args],
        sub { $pt_written_cb->(@_) },
    );

    $pt_written_cb = sub {
        my ($ok, $err) = @_;

        confess "Write failed: $err" if defined $err;

        return if ++$writes_cnt < $writes_expected;

        $ts_write_cb->();
    };

    return;
}

sub read {
    my ($self, %args) = @_;

    my $tag         = $args{tag};
    my $start_stamp = $args{start_stamp} // '-inf';
    my $end_stamp   = $args{end_stamp}   // '+inf';
    my $ts_read_cb  = $args{callback};

    my $redis        = $self->redis;
    my $pts          = [];
    my $pts_read_cnt = 0;
    my $pts_read_exp;

    # intermediate callbacks
    my ($exists_cb, $stamps_cb, $mtimes_cb, $pts_cb);

    my @stamps;
    my @values;

    $redis->command(
        ['ZRANK', 'tags', $tag],
        sub {
            my ($rank, $err) = @_;
            confess $err if defined $err;
            return $ts_read_cb->() if !defined $rank;

            $exists_cb->();
        },
    );

    $exists_cb = sub {
        $redis->command(
            ['ZRANGEBYSCORE', "$tag:stamps", $start_stamp, $end_stamp],
            sub {
                my ($stamps, $err) = @_;
                confess $err if defined $err;
                @stamps = @{ $stamps };

                return $ts_read_cb->({ tag => $tag, points => [] })
                    if !@stamps;

                $stamps_cb->();
            },
        );
    };

    $stamps_cb = sub {
        $redis->command(
            ['HMGET', $tag, @stamps ],
            sub {
                my ($values, $err) = @_;
                confess $err if defined $err;
                @values = @{ $values };
                $pts_cb->();
            },
        );
    };

    $pts_cb = sub {
        my @pts = pairwise { { stamp => $a, value => $b } } @stamps, @values;

        $ts_read_cb->({
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

    my $tag       = $args{tag};
    my $stamps    = $args{stamps};
    my $delete_cb = $args{callback};

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

        confess "Delete failed: $err" if defined $err;

        return if ++$delete_cnt < $deletes_expected;

        $delete_cb->();
    };

    return;
}

sub _delete_ts {
    my ($self, %args) = @_;

    my $tag       = $args{tag};
    my $delete_cb = $args{callback};

    my $redis            = $self->redis;
    my $delete_cnt       = 0;
    my $deletes_expected = 3;

    my $ts_deleted_cb;

    my @ts_hset_del_args;
    my @ts_zset_del_args;

    $redis->command(
        ['ZREM', 'tags', $tag ],
        sub { $ts_deleted_cb->(@_) },
    );

    $redis->command(
        ['DEL', $tag ],
        sub { $ts_deleted_cb->(@_) },
    );

    $redis->command(
        ['DEL', "$tag:stamps" ],
        sub { $ts_deleted_cb->(@_) },
    );

    $ts_deleted_cb = sub {
        my ($ok, $err) = @_;

        confess "Delete failed: $err" if defined $err;

        return if ++$delete_cnt < $deletes_expected;

        $delete_cb->();
    };

    return;
}

sub tags {
    my ($self, %args) = @_;

    my $start_idx = $args{start_idx} // 0;
    my $end_idx   = $args{end_idx}   // -1;
    my $tags_cb   = $args{callback};

    $self->redis->command(
        ['ZRANGE', 'tags', $start_idx, $end_idx],
        sub {
            my ($tags, $err) = @_;
            confess $err if defined $err;
            $tags_cb->($tags);
        },
    );

    return;
}

sub count {
    my ($self, %args) = @_;

    my $count_cb = $args{callback};
    
    $self->redis->command(
        ['ZCARD', 'tags'],
        sub {
            my ($count, $err) = @_;
            confess $err if defined $err;
            $count_cb->($count);
        },
    );

    return;
}

sub compact {
}

1;
