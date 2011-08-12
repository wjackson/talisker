use strict;
use warnings;
use Test::More;
use AnyEvent;
use Carp;
use JSON;

use t::Redis;
use Talisker;
use ok 'Talisker::Collection';

my $all_selected = [
    {
        city        => 'Chicago',
        date        => '04/05/2010',
        temperature => '40.1',
        humidity    => '20',
        description => 'Cloudy',
    },
    {
        city        => 'Chicago',
        date        => '04/06/2010',
        temperature => '42.3',
        humidity    => '30',
        description => 'Sunny',
    },
    {
        city        => 'Chicago',
        date        => '04/07/2010',
        temperature => '48.2',
        humidity    => '40',
        description => 'Overcast',
    },

    {
        city        => 'Portland',
        date        => '04/06/2010',
        temperature => '72.3',
        humidity    => '30',
        description => 'Rainy',
    },
    {
        city        => 'Portland',
        date        => '04/07/2010',
        temperature => '68.2',
        humidity    => '40',
        description => 'Sunny',
    },
    {
        city        => 'Portland',
        date        => '04/10/2010',
        temperature => '66.2',
        humidity    => '20',
        description => 'Rainy',
    },
];

test_redis {
    my $port = shift // 6379;

    my $talisker = Talisker->new(
        port         => $port,
        backend_type => 'Simple',
    );

    { # setup the talisker db

        my $cv = AE::cv;
        my $cmds_run = 0;
        my $cmds_ret = 0;

        $cmds_run++;
        $talisker->write(
            tag    => 'chicago',
            points => [
                {
                    stamp => 20100405,
                    value => encode_json {
                        city        => 'Chicago',
                        date        => '04/05/2010',
                        temperature => '40.1',
                        humidity    => '20',
                        description => 'Cloudy',
                    },
                },
                {
                    stamp => 20100406,
                    value => encode_json {
                        city        => 'Chicago',
                        date        => '04/06/2010',
                        temperature => '42.3',
                        humidity    => '30',
                        description => 'Sunny',
                    },
                },
                {
                    stamp => 20100407,
                    value => encode_json {
                        city        => 'Chicago',
                        date        => '04/07/2010',
                        temperature => '48.2',
                        humidity    => '40',
                        description => 'Overcast',
                    },
                },
                {
                    stamp => 20100408,
                    value => encode_json {
                        city        => 'Chicago',
                        date        => '04/08/2010',
                        temperature => '46.2',
                        humidity    => '20',
                        description => 'Balmy',
                    },
                },
            ],
            cb => sub {
                my (undef, $err) = @_;

                confess $err if $err;

                $cv->send if ++$cmds_ret == $cmds_run;
            },
        );

        $talisker->write(
            tag    => 'portland',
            points => [
                {
                    stamp => 20100405,
                    value => encode_json {
                        city        => 'Portland',
                        date        => '04/05/2010',
                        temperature => '70.1',
                        humidity    => '20',
                        description => 'Rainy',
                    },
                },
                  {
                    stamp => 20100406,
                    value => encode_json {
                        city        => 'Portland',
                        date        => '04/06/2010',
                        temperature => '72.3',
                        humidity    => '30',
                        description => 'Rainy',
                    },
                  },
                  {
                    stamp => 20100407,
                    value => encode_json {
                        city        => 'Portland',
                        date        => '04/07/2010',
                        temperature => '68.2',
                        humidity    => '40',
                        description => 'Sunny',
                    },
                  },
                  {
                    stamp => 20100408,
                    value => encode_json {
                        city        => 'Portland',
                        date        => '04/10/2010',
                        temperature => '66.2',
                        humidity    => '20',
                        description => 'Rainy',
                    },
                  },
            ],
            cb => sub {
                my (undef, $err) = @_;

                confess $err if $err;

                $cv->send if ++$cmds_ret == $cmds_run;
            },
        );

        $cv->recv;
    }

    {

        my $tcol = Talisker::Collection->new(
            talisker => $talisker,
            id       => 'cities',
            indexes  => [
                {
                    field => 'city',
                    sort  => 'alpha',
                },
                {
                    field => 'temperature',
                    sort  => 'numeric',
                },
                {
                    field => 'humidity',
                    sort  => 'numeric',
                },
                {
                    field => 'description',
                    sort  => 'alpha',
                },
            ],
        );

        my $cv = AE::cv;
        my $err;

        $tcol->write(
            points => [
                { tag => 'chicago',  stamp => 20100405 },
                { tag => 'chicago',  stamp => 20100406 },
                { tag => 'chicago',  stamp => 20100407 },
                { tag => 'portland', stamp => 20100406 },
                { tag => 'portland', stamp => 20100407 },
                { tag => 'portland', stamp => 20100408 },
            ],
            cb => sub {
                (undef, $err) = @_;

                $cv->send;
            },
        );

        $cv->recv;

        confess $err if $err;

    }

    {
        my $cv = AE::cv;
        my $err;
        my $points = [];

        my $tcol = Talisker::Collection->new(
            talisker => $talisker,
            id       => 'cities',
        );

        $tcol->read(
            order_by => 'description',
            cb => sub {
                ($points, $err) = @_;

                confess $err if $err;

                $cv->send;
            },
        );

        $cv->recv;

        confess $err if $err;

        is_deeply
            $points,
            [
                sort { $a->{description} cmp $b->{description} }
                    @{ $all_selected }
            ],
            'sorted by description'
            ;
    }

    {
        my $cv = AE::cv;
        my $err;
        my $points = [];

        my $tcol = Talisker::Collection->new(
            talisker => $talisker,
            id      => 'cities',
        );

        $tcol->read(
            order_by => 'humidity',
            cb => sub {
                ($points, $err) = @_;

                confess $err if $err;

                $cv->send;
            },
        );

        $cv->recv;

        confess $err if $err;

        is_deeply
            $points,
            [
                sort { $a->{humidity} <=> $b->{humidity} }
                    @{ $all_selected }
            ],
            'sorted by humidity'
            ;
    }

    {
        my $cv = AE::cv;
        my $err;
        my $points = [];

        my $tcol = Talisker::Collection->new(
            talisker => $talisker,
            id       => 'cities',
        );

        $tcol->read(
            order_by => 'stamp',
            cb => sub {
                ($points, $err) = @_;

                confess $err if $err;

                $cv->send;
            },
        );

        $cv->recv;

        confess $err if $err;

        is_deeply
            $points,
            [
                sort {
                    join('', (split m{/}, $a->{date} )[2,0,1] )
                    <=>
                    join('', (split m{/}, $b->{date} )[2,0,1] )
                }
                    @{ $all_selected }
            ],
            'sorted by stamp'
            ;
    }
};

done_testing();
