package Talisker;

use namespace::autoclean;
use Moose;
use AnyEvent::Hiredis;
use Time::HiRes qw(gettimeofday);
use Readonly;

Readonly my $INF => (2**64) -1;

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
    my $cv          = AnyEvent->condvar;

    my $redis                = $self->redis;
    my $now                  = gettimeofday;
    my $pts_written_cnt      = 0;
    my $pts_written_expected = @{$points} * 3;

    my $pt_written_cb;

    for my $pt (@{ $points }) {

        my $stamp = $pt->{stamp};
        my $value = $pt->{value};
        my $mtime = $pt->{mtime} // $now;

        # store the point
        $redis->command(
            ['HSET', $tag, "$stamp:$mtime", $value ],
            sub { $pt_written_cb->(@_) },
        );

        # index of the time series' stamps
        $redis->command(
            ['ZADD', "$tag:stamps", $stamp, $stamp ],
            sub { $pt_written_cb->(@_) },
        );

        # index of the point's mtimes
        $redis->command(
            ['ZADD', "$tag:$stamp", $mtime, $mtime ],
            sub { $pt_written_cb->(@_) },
        );
    }

    $pt_written_cb = sub {
        my ($ok, $err) = @_;
        confess "Write failed: $err" if !$err;

        return if ++$pts_written_cnt < $pts_written_expected;
        $cv->send;
    };

    return $cv;
}

sub read {
    my ($self, %args) = @_;

    my $tag         = $args{tag};
    my $start_stamp = $args{start_stamp} // 0;
    my $end_stamp   = $args{end_stamp}   // $INF;
    my $as_of       = $args{as_of}       // $INF;
    # my $ts_cb       = $args{callback};
    my $cv          = AnyEvent->condvar;

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

        $cv->send({
            tag    => $tag,
            points => [ grep { defined $_->{value} } @$pts ],
        });
    };

    return $cv;
}

1;
