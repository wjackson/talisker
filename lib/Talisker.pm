package Talisker;

use Moose;
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

has redis => (
    is => 'ro',
    isa => 'AnyEvent::Hiredis',
    lazy_build => 1,
);

sub _build_redis {
    my ($self) = @_;

    return AnyEvent::Hiredis->new(
        host => $self->host,
        port => $self->port,
    );
}

sub write {
    my ($self, %args) = @_;

    my $tag         = $args{tag};
    my $points      = $args{points};
    my $ts_write_cb = $args{callback};

    my $redis                = $self->redis;
    my $now                  = time;
    my $pts_written_cnt      = 0;
    my $pts_written_expected = @{$points} * 3;

    my $pt_written_cb;

    for my $pt (@{ $points }) {

        my $date  = $pt->{date};
        my $value = $pt->{value};
        my $mtime = $pt->{mtime} // $now;

        # store the point
        $redis->command(
            ['HSET', $tag, "$date:$mtime", $value ],
            sub { $pt_written_cb->(@_) },
        );

        # index of the time series' dates
        $redis->command(
            ['ZADD', "$tag:dates", $date, $date ],
            sub { $pt_written_cb->(@_) },
        );

        # index of the point's mtimes
        $redis->command(
            ['ZADD', "$tag:$date", $mtime, $mtime ],
            sub { $pt_written_cb->(@_) },
        );
    }

    $pt_written_cb = sub {
        return if ++$pts_written_cnt < $pts_written_expected;
        return $ts_write_cb->();
    };

    return;
}

sub read {
    my ($self, %args) = @_;

    my $tag        = $args{tag};
    my $start_date = $args{start_date} // 0;
    my $end_date   = $args{end_date}   // 9999_99_99;
    my $as_of      = $args{as_of}      // 9999999999;
    my $ts_cb      = $args{callback};

    my $redis        = $self->redis;
    my $pts          = [];
    my $pts_read_cnt = 0;
    my $pts_read_exp;

    # intermediate callbacks
    my ($dates_cb, $mtimes_cb, $pt_cb);

    # get the list of dates that we have points for
    $redis->command(
        ['ZRANGEBYSCORE', "$tag:dates", $start_date, $end_date],
        sub { $dates_cb->(@_) },
    );

    $dates_cb = sub {
        my ($dates) = @_;

        $pts_read_exp = @$dates;

        for my $pt_idx (0..$#{ $dates }) {
            my $date = $dates->[$pt_idx];

            $redis->command(
                ['ZRANGEBYSCORE', "$tag:$date", 0, $as_of],
                sub { $mtimes_cb->($pt_idx, $date, $_[0]) },
            );
        }
    };

    $mtimes_cb = sub {
        my ($pt_idx, $date, $mtimes) = @_;
        my $mtime = $mtimes->[-1];

        $redis->command(
            ['HGET', $tag, "$date:$mtime"],
            sub { $pt_cb->($pt_idx, $date, $mtime, $_[0]) },
        );
    };

    $pt_cb = sub {
        my ($pt_idx, $date, $mtime, $value) = @_;

        $pts->[$pt_idx] = { date => $date, mtime => $mtime, value => $value };

        return if ++$pts_read_cnt < $pts_read_exp;

        $ts_cb->({
            tag    => $tag,
            points => $pts,
        });
    };

    return;
}

1;
