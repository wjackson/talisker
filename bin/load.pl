use strict;
use warnings;
use feature ':5.10';

use JSON::XS;
use AE;
use Talisker;
use Talisker::RandomWalk;

MAIN: {
    my $talisker = Talisker->new(backend_type => 'Simple');

    my $cv = AE::cv;
    my $decr = 1000;
    my $running = 1;
    my $pts = mk_pts();
    my $cb; $cb = sub {

        $running--;

        if ($decr <= 0 && $running <= 0) {
            $cv->send;
            return;
        }

        while ($running < 10 && $decr > 0) {
            $talisker->write(
                tag => "tag_$decr",
                points => $pts,
                callback => $cb,
            );

            $running++;
            $decr--;
        }
    };

    my $timer = AnyEvent->timer( after => 3, interval => 3, cb => sub {
       say "$decr items remaining";
    });

    $cb->();

    $cv->recv;
}

sub mk_pts {
    my $random_walk = Talisker::RandomWalk->new;
    my @stamps  = map { 20020101 + $_ } (1..1000);
    my $now = time;

    my @points  = map {
        {
            stamp => $_,
            value => encode_json {
                value       => $random_walk->next,
                mtime       => $now,
                fill_status => 'filled',
            }
        },
    } @stamps;

    return \@points;
}
