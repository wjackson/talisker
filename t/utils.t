use strict;
use warnings;
use Test::More;
use AnyEvent;

use Talisker::Util qw(merge_point);

my $inflight     = 0;
my $max_inflight = 0;

my @timers;
my $work_cb = sub {
    my ($n, $cb) = @_;
    $inflight++;

    # record the maximum number of workers that are in flight
    $max_inflight = $inflight > $max_inflight ? $inflight : $max_inflight;

    # use a timer to make the work asynchronous
    push @timers,
        AnyEvent->timer(
            after => 0,
            cb => sub { $inflight--; $cb->( $n * 2 ) },
        );
};

my $finished_cb = sub {
    my ($outputs, $err) = @_;

    is_deeply
        $outputs,
        [ 0, 2, 4, 6, 8 ],
        'inputs doubled',
        ;

    is $max_inflight, 3, 'no more than 3 workers were run at once',
};

my $cv = AE::cv;

merge_point(
    inputs    => [ 0..4 ],
    work      => $work_cb,
    finished  => sub { $finished_cb->(@_); $cv->send},
    at_a_time => 3,
);

$cv->recv;

done_testing;
