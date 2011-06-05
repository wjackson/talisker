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
    my $writes_expected = 2;

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
    my $start_stamp = $args{start_stamp} // '-inf';
    my $end_stamp   = $args{end_stamp}   // '+inf';
    my $ts_read_cb  = $args{callback};

    my $redis        = $self->redis;
    my $pts          = [];
    my $pts_read_cnt = 0;
    my $pts_read_exp;

    # intermediate callbacks
    my ($stamps_cb, $mtimes_cb, $pts_cb);

    my @stamps;
    my @values;

    $redis->command(
        ['ZRANGEBYSCORE', "$tag:stamps", $start_stamp, $end_stamp],
        sub { @stamps = @{ $_[0] }; $stamps_cb->() },
    );

    $stamps_cb = sub {
        $redis->command(
            ['HMGET', $tag, @stamps ],
            sub { @values = @{ $_[0] }; $pts_cb->() },
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

sub compact {
}

1;
