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
        my $tags;
        $talisker->tags(callback => sub {
            $tags = shift;
            $cv->send;
        });
        $cv->recv;

        is_deeply $tags, ['BAC'], 'tags';
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

    {
        my $cv = AE::cv;
        $talisker->delete(
            tag      => 'BAC',
            stamps   => [ 20100405 ],
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
                    { stamp => 20100406, value => 1.21 },
                    { stamp => 20100407, value => 1.3  },
                ],
            },
            'pt successfully deleted'
            ;
    }

    {
        my $cv = AE::cv;
        $talisker->delete(
            tag      => 'BAC',
            callback => sub { $cv->send },
        );
        $cv->recv;
    }

    {
        my $cv = AE::cv;
        my $read_ts; $talisker->read(
            tag      => 'BAC',
            callback => sub { $read_ts = shift; $cv->send },
        );
        $cv->recv;

        is $read_ts, undef, 'time series successfully deleted';
    }
};

done_testing;
