use strict;
use warnings;
use feature ':5.10';

use JSON::XS;
use AE;
use Talisker;

MAIN: {
    my $talisker = Talisker->new(backend_type => 'Simple');

    my $cv = AE::cv;
    my $decr = 1000;
    my $running = 1;
    my $cb; $cb = sub {
        my ($ts) = shift;

        $running--;

        if (defined $ts) {
            my $tag = $ts->{tag};
            for my $pt (@{ $ts->{points} }) {
                my $stamp = $pt->{stamp};
                my $value = decode_json $pt->{value};

                say join ',',
                    $tag,
                    $stamp,
                    $value->{value},
                    $value->{mtime},
                    $value->{fill_status}
                    ;
            }
        }

        if ($decr <= 0) {
            $cv->send;
            return;
        }

        while ($running < 10 && $decr > 0) {
            $talisker->read(tag => "tag_$decr", callback => $cb);
            $running++;
        }

        $decr--;
    };

    my $timer = AnyEvent->timer( after => 3, interval => 3, cb => sub {
        warn "$decr items remaining";
    });

    $cb->();

    $cv->recv;
}
