package Talisker::Backend::Revision;

use Moose;
use namespace::autoclean;
use Readonly;
use Time::HiRes qw(gettimeofday);
use AnyEvent::Hiredis;

Readonly my $INF => (2**64) -1;

with 'Talisker::Backend::Role';

sub write {
    my ($self, %args) = @_;

    my $tag         = $args{tag};
    my $points      = $args{points};
    my $ts_write_cb = $args{cb};

    my $redis           = $self->redis;
    my $now             = gettimeofday;
    my $writes_cnt      = 0;
    my $writes_expected = @{$points} + 2;
    # my $writes_expected = @{$points} * 3;

    my $pt_written_cb;

    my @point_hset_args;
    my @stamp_zset_args;

    for my $pt (@{ $points }) {

        my $stamp = $pt->{stamp};
        my $value = $pt->{value};
        my $mtime = $pt->{mtime} // $now;

        # store the point
        push @point_hset_args, "$stamp:$mtime", $value;
        # $redis->command(
        #     ['HSET', $tag, "$stamp:$mtime", $value ],
        #     sub { $pt_written_cb->(@_) },
        # );

        # index of the time series' stamps
        push @stamp_zset_args, $stamp, $stamp;
        # $redis->command(
        #     ['ZADD', "$tag:stamps", $stamp, $stamp ],
        #     sub { $pt_written_cb->(@_) },
        # );

        # index of the point's mtimes
        $redis->command(
            ['ZADD', "$tag:$stamp", $mtime, $mtime ],
            sub { $pt_written_cb->(@_) },
        );
    }

    $redis->command(
        ['HMSET', $tag, @point_hset_args ],
        sub { $pt_written_cb->(@_) },
    );

    $redis->command(
        ['ZADD', "$tag:stamps", @stamp_zset_args ],
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
    my $start_stamp = $args{start_stamp} // 0;
    my $end_stamp   = $args{end_stamp}   // $INF;
    my $as_of       = $args{as_of}       // $INF;
    my $ts_read_cb  = $args{cb};
    # my $cv          = AnyEvent->condvar;

    my $redis        = $self->redis;
    my $pts          = [];
    my $pts_read_cnt = 0;
    my $pts_read_exp;

    # intermediate callbacks
    my ($stamps_cb, $mtimes_cb, $pt_cb);

    # get the list of stamps that we have points for
    $redis->command(
        ['ZRANGEBYSCORE', "$tag:stamps", $start_stamp, $end_stamp],
        sub { $stamps_cb->(@_) },
    );

    $stamps_cb = sub {
        my ($stamps, $err) = @_;

        # confess "Read failed: $err" if $err;

        $pts_read_exp = @$stamps;

        for my $pt_idx (0..$#{ $stamps }) {
            my $stamp = $stamps->[$pt_idx];

            $redis->command(
                ['ZRANGEBYSCORE', "$tag:$stamp", 0, $as_of],
                sub { $mtimes_cb->($pt_idx, $stamp, $_[0]) },
            );
        }
    };

    $mtimes_cb = sub {
        my ($pt_idx, $stamp, $mtimes) = @_;
        my $mtime = $mtimes->[-1] // '';

        $redis->command(
            ['HGET', $tag, "$stamp:$mtime"],
            sub { $pt_cb->($pt_idx, $stamp, $mtime, $_[0]) },
        );
    };

    $pt_cb = sub {
        my ($pt_idx, $stamp, $mtime, $value) = @_;

        $pts->[$pt_idx] = { stamp => $stamp, mtime => $mtime, value => $value };

        return if ++$pts_read_cnt < $pts_read_exp;

        $ts_read_cb->({
            tag    => $tag,
            points => [ grep { defined $_->{value} } @$pts ],
        });
    };

    return;
}

sub compact {
}

1;
