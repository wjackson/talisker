use strict;
use warnings;
use Test::More;
use AnyEvent;
use Carp qw(confess);
use Talisker::Util qw(chain);
use Time::HiRes;

use t::Redis;
use ok 'Talisker';

test_redis {

    my $port = shift // 6379;
    my $talisker = Talisker->new(backend_type => 'Simple', port => $port);

    isa_ok $talisker, 'Talisker';

    my $t1 = Time::HiRes::time;

    my $TS = {
        tag    => 'foo',
        points => [
            { stamp  => 20100405, value => 1.21 },
            { stamp  => 20100406, value => 1.21 },
            { stamp  => 20100407, value => 1.3  },
        ],
    };

    my $cv = AE::cv;

    chain(
        steps => [

            sub {
                $talisker->write(
                    %{ $TS },
                    cb => $_[1],
                );
            },

            sub {
                $talisker->ts_meta(
                    tag => 'foo',
                    cb => $_[1],
                );
            },

        ],

        finished => sub { $cv->send(@_) },
    );

    my ($ts_meta, $err) = $cv->recv;
    my $t2 = Time::HiRes::time;

    confess $err if $err;

    ok $ts_meta->{mtime} >= $t1 && $ts_meta->{mtime} <= $t2, 'ts_meta mtime looks right';
};

done_testing;
