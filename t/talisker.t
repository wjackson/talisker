use strict;
use warnings;
use Test::More;
use AnyEvent;
use Carp qw(confess);
use Talisker::Util qw(chain);
use Talisker::Admin;
use Time::HiRes;

use t::Redis;
use ok 'Talisker';

test_redis {

    my $port = shift // 6379;
    my $talisker = Talisker::Admin->new(port => $port);

    isa_ok $talisker, 'Talisker::Admin';

    my $cv = AE::cv;

    $talisker->initialize(
        db     => 7,
        fields => [
            { name => 'value' },
        ],
        cb => sub { $cv->send(@_) },
    );

    my (undef, $err) = $cv->recv;

    confess $err if $err;

    # TODO: make some assertions about an intialized talisker db
};

done_testing;
