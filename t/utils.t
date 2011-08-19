use strict;
use warnings;
use Test::More;
use AnyEvent;

use Talisker::Util qw(
    merge_point
    chain
);

{ # merge_point (map syntax)

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

    # apply_merge
    merge_point(
        inputs    => [ 0..4 ],
        work      => $work_cb,
        finished  => sub { $finished_cb->(@_); $cv->send},
        at_a_time => 3,
    );

    $cv->recv;

}

{ # merge_point (map syntax with errors)

    my $inflight     = 0;
    my $max_inflight = 0;

    my $count = 0;

    my @timers;
    my $work_cb = sub {
        my ($n, $cb) = @_;
        $inflight++;
        $count++;

        # record the maximum number of workers that are in flight
        $max_inflight = $inflight > $max_inflight ? $inflight : $max_inflight;

        my $local_count = $count;

        # use a timer to make the work asynchronous
        push @timers,
            AnyEvent->timer(
                after => 0,
                cb => sub {
                    $inflight--;

                    # simulate an error
                    if ($local_count == 2) {
                        $cb->(17, 'something bad happened');
                    }
                    else {
                        $cb->( $n * 2 );
                    }
                },
            );
    };

    my $finished_cb = sub {
        my ($outputs, $err) = @_;

        is $outputs, undef, 'output is undef when an error occurs';
        is $err, 'something bad happened', 'error message propagated';
        is $max_inflight, 3, 'no more than 3 workers were run at once',
    };

    my $cv = AE::cv;

    # apply_merge
    merge_point(
        inputs    => [ 0..4 ],
        work      => $work_cb,
        finished  => sub { $finished_cb->(@_); $cv->send},
        at_a_time => 3,
    );

    $cv->recv;

}

{ # merge_point (mesh syntax)

    my @timers;
    my $cv           = AE::cv;
    my $inflight     = 0;
    my $max_inflight = 0;

    my $mk_sub =  sub {
        my ($n) = @_ ;

        return sub {
            my ($input, $cb) = @_;

            $inflight++;

            $max_inflight
                = $inflight > $max_inflight ? $inflight : $max_inflight;

            push @timers,
                AnyEvent->timer(
                    after => 0,
                    cb => sub {
                        $inflight--;
                        $cb->($input+$n);
                    },
                );
        };
    };

    my $finished_cb = sub {
        my ($results) = @_;

        is_deeply
            $results,
            [ 2, 3, 4, 5, 6 ],
            'successfully generated results';

        is $max_inflight, 3, 'max inflight = 3';
    };

    merge_point(
        inputs => [ 1, 1, 1, 1, 1, 1 ],
        work   => [
            $mk_sub->(1),
            $mk_sub->(2),
            $mk_sub->(3),
            $mk_sub->(4),
            $mk_sub->(5),
        ],
        finished  => sub { $finished_cb->(@_); $cv->send },
        at_a_time => 3,
    );

    $cv->recv;
}

{ # merge_point (mesh syntax with error)

    my @timers;
    my $cv           = AE::cv;
    my $inflight     = 0;
    my $max_inflight = 0;
    my $count = 0;

    my $mk_sub =  sub {
        my ($n) = @_ ;

        return sub {
            my ($input, $cb) = @_;

            $inflight++;
            $count++;

            $max_inflight
                = $inflight > $max_inflight ? $inflight : $max_inflight;

            my $local_count = $count;

            push @timers,
                AnyEvent->timer(
                    after => 0,
                    cb => sub {
                        $inflight--;

                        if ($local_count == 2) {
                            $cb->(undef, 'something bad happened');
                        }
                        else {
                            $cb->($input+$n);
                        }
                    },
                );
        };
    };

    my $finished_cb = sub {
        my ($results, $err) = @_;

        is $err, 'something bad happened', 'error propagated';
        is $results, undef, 'undef results on error';
        is $max_inflight, 3, 'max inflight = 3';
    };

    merge_point(
        inputs => [ 1, 1, 1, 1, 1, 1 ],
        work   => [
            $mk_sub->(1),
            $mk_sub->(2),
            $mk_sub->(3),
            $mk_sub->(4),
            $mk_sub->(5),
        ],
        finished  => sub { $finished_cb->(@_); $cv->send },
        at_a_time => 3,
    );

    $cv->recv;
}
{ # chain

    my @timers;
    my $cv = AE::cv;

    chain(
        input    => 2,
        finished => sub { $cv->send(@_) },
        steps => [

            sub {
                my ($input, $cb) = @_;

                push @timers, AnyEvent->timer(
                    after => 0,
                    cb    => sub { $cb->($input+1) },
                );
            },

            sub {
                my ($input, $cb) = @_;
                push @timers, AnyEvent->timer(
                    after => 0,
                    cb    => sub { $cb->($input * 2) },
                );
            },

        ],
    );

    my ($res) = $cv->recv;
    is $res, 6, 'chain result is 6';

}


done_testing;
