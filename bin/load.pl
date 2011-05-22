use strict;
use warnings;
use feature ':5.10';

use AE;
use Talisker;
use Talisker::RandomWalk;

MAIN: {
    my $talisker = Talisker->new;

    my $cv = AE::cv;
    my $decr = 1000;
    my $cb; $cb = sub {
        if ($decr-- <= 0) {
            $cv->send;
        }

        $talisker->write(%{ mk_ts() }, callback => $cb);
    };

    $cb->();

    $cv->recv;
}

sub mk_ts {
    state $ts_cnt++;

    my $tag = "tag_${ts_cnt}";
    my $random_walk = Talisker::RandomWalk->new;
    my @dates  = map { 20020101 + $_ } (1..1000);
    my @points = map { { date => $_, value => $random_walk->next } } @dates;

    return {
        tag => $tag,
        points => \@points,
    };
}
