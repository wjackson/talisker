use strict;
use warnings;
use Test::More;
use AE;

use t::Redis;
use ok 'Talisker';

test_redis {
    my $port = shift;
    my $talisker = Talisker->new(backend_type => 'Simple', port => $port);
    
    isa_ok $talisker, 'Talisker';
    
    # write some some pts to a time series
    {
        my $cv = AE::cv;
        $talisker->write(
            tag    => 'BAC',
            points => [
                { stamp  => 20100405, value => 1.1  },
                { stamp  => 20100406, value => 1.21 },
                { stamp  => 20100407, value => 1.3  },
            ],
            callback => sub { $cv->send },
        );
        $cv->recv;

    }

    {
        my $cv = AE::cv;
        my $read_ts; $talisker->read(
            tag => 'BAC',
            callback => sub { $read_ts = shift; $cv->send },
        );
        $cv->recv;

        is_deeply
            $read_ts,
            {
                tag => 'BAC',
                points => [
                    { stamp => 20100405, value => 1.1  },
                    { stamp => 20100406, value => 1.21 },
                    { stamp => 20100407, value => 1.3  },
                ],
            },
            'read ts'
            ;
    }

    {
        my $cv = AE::cv;
        my $read_ts; $talisker->read(
            tag      => 'BAC',
            as_of    => 1234567891,
            callback => sub { $read_ts = shift; $cv->send },
        );
        $cv->recv;

        is_deeply
            $read_ts,
            {
                tag => 'BAC',
                points => [
                    { stamp => 20100405, value => 1.1  },
                    { stamp => 20100406, value => 1.21 },
                    { stamp => 20100407, value => 1.3  },
                ],
            },
            'read ts with late as_of'
            ;
    }

    {
        my $cv = AE::cv;
        my $read_ts; $talisker->read(
            tag      => 'BAC',
            as_of    => 1234567889,
            callback => sub { $read_ts = shift; $cv->send },
        );
        $cv->recv;

        is_deeply
            $read_ts,
            {
                tag    => 'BAC',
                points => [
                    { stamp => 20100405, value => 1.1  },
                    { stamp => 20100406, value => 1.21 },
                    { stamp => 20100407, value => 1.3  },
                ],
            },
            'read ts with early as_of'
            ;
    }
};

done_testing;
