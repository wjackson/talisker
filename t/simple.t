use strict;
use warnings;
use Test::More;
use AnyEvent;

use t::Redis;
use ok 'Talisker';

test_redis {
    my $port = shift;
    my $talisker = Talisker->new(port => $port);
    isa_ok $talisker, 'Talisker';

    { # write a time series to the db
        my $t1 = Time::HiRes::time;

        my $cv = AE::cv;
        $talisker->write(
            tag    => 'GRR',
            points => [
                { stamp  => 20100405, value => 1.1  },
                { stamp  => 20100406, value => 1.21 },
                { stamp  => 20100407, value => 1.3  },
            ],
            cb => sub { $cv->send },
        );
        $cv->recv;

        $cv = AE::cv;
        $talisker->ts_meta( tag => 'GRR', cb  => sub { $cv->send(@_) });
        my ($ts_meta, $err) = $cv->recv;

        confess $err if $err;

        my $t2 = Time::HiRes::time;

        ok $ts_meta->{mtime} >= $t1 && $ts_meta->{mtime} <= $t2, 'ts_meta mtime looks right';
    }

    { # read tags
        my $cv = AE::cv;

        $talisker->tags(cb => sub { $cv->send(@_) });
        my ($tags, $err) = $cv->recv;

        confess $err if $err;

        is_deeply $tags, ['GRR'], 'tags';
    }

    { # read written time series
        my $cv = AE::cv;
        $talisker->read(
            tag => 'GRR',
            cb  => sub { $cv->send(@_) },
        );
        my ($read_ts, $err) = $cv->recv;

        confess $err if $err;

        is_deeply
            $read_ts,
            {
                tag => 'GRR',
                points => [
                    { stamp => 20100405, value => 1.1  },
                    { stamp => 20100406, value => 1.21 },
                    { stamp => 20100407, value => 1.3  },
                ],
            },
            'read ts'
            ;
    }

    { # read with stamp range
        my $cv = AE::cv;
        $talisker->read(
            tag         => 'GRR',
            start_stamp => 20100405,
            end_stamp   => 20100405,
            cb          => sub { $cv->send(@_) },
        );
        my ($read_ts, $err) = $cv->recv;

        confess $err if $err;

        is_deeply
            $read_ts,
            {
                tag => 'GRR',
                points => [
                    { stamp => 20100405, value => 1.1  },
                ],
            },
            'read ts'
            ;
    }

    { # read with as_of
        my $cv = AE::cv;
        $talisker->read(
            tag   => 'GRR',
            as_of => 1234567891,
            cb    => sub { $cv->send(@_) },
        );
        my ($read_ts, $err) = $cv->recv;

        confess $err if $err;

        is_deeply
            $read_ts,
            {
                tag => 'GRR',
                points => [
                    { stamp => 20100405, value => 1.1  },
                    { stamp => 20100406, value => 1.21 },
                    { stamp => 20100407, value => 1.3  },
                ],
            },
            'read ts with late as_of'
            ;
    }

    { # read with as_of again
        my $cv = AE::cv;
        $talisker->read(
            tag   => 'GRR',
            as_of => 1234567889,
            cb    => sub { $cv->send(@_) },
        );
        my ($read_ts, $err) = $cv->recv;

        confess $err if $err;

        is_deeply
            $read_ts,
            {
                tag    => 'GRR',
                points => [
                    { stamp => 20100405, value => 1.1  },
                    { stamp => 20100406, value => 1.21 },
                    { stamp => 20100407, value => 1.3  },
                ],
            },
            'read ts with early as_of'
            ;
    }

    { # delete point
        my $cv = AE::cv;
        $talisker->delete(
            tag    => 'GRR',
            stamps => [ 20100405 ],
            cb     => sub { $cv->send(@_) },
        );
        my (undef, $err) = $cv->recv;

        confess $err if $err;
    }

    { # verify point delete
        my $cv = AE::cv;
        $talisker->read(
            tag => 'GRR',
            cb  => sub { $cv->send(@_) },
        );
        my ($read_ts, $err) = $cv->recv;

        confess $err if $err;

        is_deeply
            $read_ts,
            {
                tag => 'GRR',
                points => [
                    { stamp => 20100406, value => 1.21 },
                    { stamp => 20100407, value => 1.3  },
                ],
            },
            'pt successfully deleted'
            ;
    }

    { # count the time series
        my $cv = AE::cv;
        $talisker->count(
            cb => sub { $cv->send(@_) },
        );
        my ($count, $err) = $cv->recv;

        confess $err if $err;

        is $count, 1, 'count is 1';
    }

    { # delete time series
        my $cv = AE::cv;
        $talisker->delete(
            tag => 'GRR',
            cb  => sub { $cv->send(@_) },
        );

        my (undef, $err) = $cv->recv;

        confess $err if $err;
    }

    { # verify time series delete
        my $cv = AE::cv;
        $talisker->read(
            tag => 'GRR',
            cb  => sub { $cv->send(@_) },
        );
        my ($read_ts, $err) = $cv->recv;

        confess $err if $err;

        is $read_ts, undef, 'time series successfully deleted';
    }

};

done_testing;
