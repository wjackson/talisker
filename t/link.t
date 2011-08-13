use strict;
use warnings;
use Test::More;
use AnyEvent;
use Carp qw(confess);

use t::Redis;
use ok 'Talisker';

test_redis {
    my $port = shift // 6379;
    my $talisker = Talisker->new(backend_type => 'Simple', port => $port);

    isa_ok $talisker, 'Talisker';

    my $TS = {
        tag    => 'BAC',
        points => [
            { stamp  => 20100405, value => 1.1  },
            { stamp  => 20100406, value => 1.21 },
            { stamp  => 20100407, value => 1.3  },
        ],
    };

    my (
        $write_ts, $mk_link, $resolve_link, $read_link, $write_link,
        $read_ts, $cv
    );

    $write_ts = sub {
        $talisker->write(
            %{ $TS },
            cb => sub {
                my (undef, $err) = @_;

                return $cv->send($err) if $err;
                return $mk_link->();
            },
        );
    };

    $mk_link = sub {
        $talisker->link(
            tag    => 'LinkToBAC',
            target => 'BAC',
            cb     => $resolve_link,
        );
    };

    $resolve_link = sub {
        $talisker->resolve_link(
            tag => 'LinkToBAC',
            cb  => sub {
                my ($target, $err) = @_;

                return $cv->send($err) if $err;

                is $target, 'BAC', 'link resolved correctly';

                $read_link->();
            },
        );
    };

    $read_link = sub {
        $talisker->read(
            tag => 'LinkToBAC',
            cb  => sub {
                my ($ts, $err) = @_;

                is_deeply $ts, $TS, 'ts read by link worked';

                $write_link->();
            },
        );
    };

    $write_link = sub {
        $talisker->write(
            tag => 'LinkToBAC',
            points => [
                { stamp => 20100408, value => 1.4 },
            ],
            cb  => $read_ts,
        );
    };

    $read_ts = sub {
        $talisker->read(
            tag => 'BAC',
            cb  => sub {
                my ($ts, $err) = @_;

                is_deeply
                    $ts,
                    {
                        tag    => 'BAC',
                        points => [
                            { stamp  => 20100405, value => 1.1  },
                            { stamp  => 20100406, value => 1.21 },
                            { stamp  => 20100407, value => 1.3  },
                            { stamp  => 20100408, value => 1.4  },
                        ],
                    },
                    'ts write to link worked';

                $cv->send;
            },
        );
    };

    # go
    $cv = AE::cv;
    $write_ts->();
    my $err = $cv->recv;

    confess $err if $err;
};

done_testing;
