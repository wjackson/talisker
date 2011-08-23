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
    my $talisker = Talisker->new(port => $port);

    isa_ok $talisker, 'Talisker';

    my $cv = AE::cv;

    $talisker->create(
        db     => 7,
        fields => [
            { name => 'value' },
        ],
        cb => sub { $cv->send(@_) },
    );

    my ($th, $err) = $cv->recv;

    confess $err if $err;

    isa_ok $th, 'Talisker::Handle';

    isa_ok $talisker->handle(db => 7), 'Talisker::Handle';

};

done_testing;
