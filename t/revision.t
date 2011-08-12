use strict;
use warnings;
use Test::More;
use AE;

use t::Redis;
use ok 'Talisker';

test_redis {
    my $port = shift;
    my $talisker = Talisker->new(backend_type => 'Revision', port => $port);

    isa_ok $talisker, 'Talisker';

    # write some some pts to a time series
    {
        my $cv = AE::cv;
        $talisker->write(
            tag    => 'BAC',
            points => [
                { stamp  => 20100405, value => 1.1,  mtime => 1234567890},
                { stamp  => 20100406, value => 1.2,  mtime => 1234567890},
                { stamp  => 20100406, value => 1.21, mtime => 1234567892},
                { stamp  => 20100407, value => 1.3,  mtime => 1234567890},
            ],
            cb => sub { $cv->send },
        );
        $cv->recv;

    }

    {
        my $cv = AE::cv;
        my $read_ts1; $talisker->read(
            tag => 'BAC',
            cb  => sub { $read_ts1 = shift; $cv->send },
        );
        $cv->recv;

        is_deeply
            $read_ts1,
            {
                tag => 'BAC',
                points => [
                    { stamp => 20100405, value => 1.1,  mtime => 1234567890 },
                    { stamp => 20100406, value => 1.21, mtime => 1234567892 },
                    { stamp => 20100407, value => 1.3,  mtime => 1234567890 },
                ],
            },
            'read ts'
            ;
    }

    {
        my $cv = AE::cv;
        my $read_ts; $talisker->read(
            tag   => 'BAC',
            as_of => 1234567891,
            cb    => sub { $read_ts = shift; $cv->send },
        );
        $cv->recv;

        is_deeply
            $read_ts,
            {
                tag => 'BAC',
                points => [
                    { stamp => 20100405, value => 1.1, mtime => 1234567890 },
                    { stamp => 20100406, value => 1.2, mtime => 1234567890 },
                    { stamp => 20100407, value => 1.3, mtime => 1234567890 },
                ],
            },
            'read ts with late as_of'
            ;
    }

    {
        my $cv = AE::cv;
        my $read_ts; $talisker->read(
            tag   => 'BAC',
            as_of => 1234567889,
            cb    => sub { $read_ts = shift; $cv->send },
        );
        $cv->recv;

        is_deeply
            $read_ts,
            {
                tag    => 'BAC',
                points => [],
            },
            'read ts with early as_of'
            ;
    }
};

done_testing;
