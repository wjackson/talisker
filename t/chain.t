use strict;
use warnings;
use Test::More;
use AnyEvent;

use ok 'Talisker::Chain';

my $cv = AE::cv;

my $res;
my @timers;

Talisker::Chain->new(
    workers => 2,
    inputs  => [ 1, 2, 3, 4 ],
    work_cb => sub {
        my ($input, $cb) = @_;

        push @timers,  AnyEvent->timer(after => 1, cb => sub {
            $cb->($input * 2);
        });
    },
    finished_cb => sub { $res = shift; $cv->send },
)->go;

$cv->recv;

is_deeply $res, [ 2, 4, 6, 8 ], 'numbers are doubled';

done_testing();
