use strict;
use warnings;
use Test::More;

use t::Redis;
use ok 'Talisker';

test_redis {
    my $port = shift;
    my $talisker = Talisker->new(port => $port);
    
    isa_ok $talisker, 'Talisker';
    
    $talisker->write(
        tag    => 'BAC',
        points => [
            { stamp  => 20100405, value => 1.1,  mtime => 1234567890},
            { stamp  => 20100406, value => 1.2,  mtime => 1234567890},
            { stamp  => 20100406, value => 1.21, mtime => 1234567892},
            { stamp  => 20100407, value => 1.3,  mtime => 1234567890},
        ],
    )->recv;

    is_deeply
        $talisker->read(tag => 'BAC')->recv,
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

    is_deeply
        $talisker->read(tag => 'BAC', as_of => 1234567891)->recv,
        {
            tag => 'BAC',
            points => [
                { stamp => 20100405, value => 1.1, mtime => 1234567890 },
                { stamp => 20100406, value => 1.2, mtime => 1234567890 },
                { stamp => 20100407, value => 1.3, mtime => 1234567890 },
            ],
        },
        'read ts with an as_of'
        ;

    is_deeply
        $talisker->read(tag => 'BAC', as_of => 1234567889)->recv,
        {
            tag => 'BAC',
            points => [],
        },
        'read ts with an early as_of'
        ;
};

done_testing;
