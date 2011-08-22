use strict;
use warnings;
use Test::More;
use AnyEvent;
use Carp qw(confess);
use Talisker::Util qw(chain);

use t::Redis;
use ok 'Talisker';

test_redis {
    my $port = shift // 6379;
    my $talisker = Talisker->new(port => $port);

    isa_ok $talisker, 'Talisker';

    my $TS = {
        tag    => 'GRR',
        points => [
            { stamp  => 20100405, value => 1.1  },
            { stamp  => 20100406, value => 1.21 },
            { stamp  => 20100407, value => 1.3  },
        ],
    };

    my $th;
    my $cv = AE::cv;

    chain(
        steps => [

            # make talisker db
            sub {
                my (undef, $cb) = @_;

                $th = $talisker->mkdb(
                    fields => [
                        { name => 'value' },
                    ],
                    cb => sub {
                        $th = shift;
                        my $err = shift;

                        return $cb->(undef, $err) if $err;
                        return $cb->();
                    },
                );
            },

            # write time series
            sub {
                $th->write(
                    %{ $TS },
                    cb => $_[1],
                );
            },

            # link
            sub {
                $th->link(
                    tag    => 'LinkToGRR',
                    target => 'GRR',
                    cb     => $_[1],
                );
            },

            # resolve
            sub {
                my (undef, $cb) = @_;
                $th->resolve_link(
                    tag => 'LinkToGRR',
                    cb  => sub {
                        my ($target, $err) = @_;

                        return $cb->(undef, $err) if $err;

                        is $target, 'GRR', 'link resolved correctly';

                        return $cb->();
                    },
                );
            },

            # read from link
            sub {
                my (undef, $cb) = @_;
                $th->read(
                    tag => 'LinkToGRR',
                    cb  => sub {
                        my ($ts, $err) = @_;

                        return $cb->(undef, $err) if $err;

                        is_deeply $ts, $TS, 'ts read by link worked';

                        return $cb->();
                    },
                );
            },

            # write to link
            sub {
                $th->write(
                    tag => 'LinkToGRR',
                    points => [
                        { stamp => 20100408, value => 1.4 },
                    ],
                    cb  => $_[1],
                );
            },

            # read
            sub {
                my (undef, $cb) = @_;
                $th->read(
                    tag => 'GRR',
                    cb  => sub {
                        my ($ts, $err) = @_;

                        return $cb->(undef, $err) if $err;

                        is_deeply
                            $ts,
                            {
                                tag    => 'GRR',
                                points => [
                                    { stamp  => 20100405, value => 1.1  },
                                    { stamp  => 20100406, value => 1.21 },
                                    { stamp  => 20100407, value => 1.3  },
                                    { stamp  => 20100408, value => 1.4  },
                                ],
                            },
                            'ts write to link worked';

                        return $cb->();
                    },
                );
            },

        ],
        finished => sub { $cv->send(@_) },
    );

    my (undef, $err) = $cv->recv;

    confess $err if $err;
};

done_testing;
