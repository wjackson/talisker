use strict;
use warnings;
use Test::More;
use AE;

use t::Redis;
use ok 'Talisker';

test_redis {
    my $port = shift;
    my $talisker = Talisker->new(port => $port);
    
    isa_ok $talisker, 'Talisker';
    
    my $cv = AE::cv;
    my $read_value;
    
    my ($write_cb, $read_cb_a, $read_cb_b);
    my ($ts_a, $ts_b);

    $talisker->write(
        tag    => 'BAC',
        points => [
            { date  => '20100405', value => 1.1,  mtime => 1234567890},
            { date  => '20100406', value => 1.2,  mtime => 1234567890},
            { date  => '20100406', value => 1.21, mtime => 1234567892},
            { date  => '20100407', value => 1.3,  mtime => 1234567890},
        ],
        callback => sub { $write_cb->(@_) },
    );

    $write_cb = sub {
        $talisker->read(
            tag      => 'BAC',
            callback => sub { $read_cb_a->(@_) },
        );
    };

    $read_cb_a = sub {
        ($ts_a) = @_;
        $talisker->read(
            tag      => 'BAC',
            as_of    => 1234567891,
            callback => sub { $read_cb_b->(@_) },
        );
    };

    $read_cb_b = sub {
        ($ts_b) = @_;
        $cv->send;
    };
    
    $cv->recv;
    
    is_deeply
        $ts_a,
        {
            tag => 'BAC',
            points => [
                { date => 20100405, value => 1.1,  mtime => 1234567890 },
                { date => 20100406, value => 1.21, mtime => 1234567892 },
                { date => 20100407, value => 1.3,  mtime => 1234567890 },
            ],
        },
        'read ts'
        ;

    is_deeply
        $ts_b,
        {
            tag => 'BAC',
            points => [
                { date => 20100405, value => 1.1, mtime => 1234567890 },
                { date => 20100406, value => 1.2, mtime => 1234567890 },
                { date => 20100407, value => 1.3, mtime => 1234567890 },
            ],
        },
        'read ts with as_of'
        ;
};

done_testing;
