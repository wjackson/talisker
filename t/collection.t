use strict;
use warnings;
use Test::More;
use AnyEvent;
use Carp;

use t::Redis;
use ok 'Talisker::Collection';

test_redis {
    my $port = shift;

    my $tcol = Talisker::Collection->new(
        port    => $port,
        id      => 'books',
        expire  => 60,        # TODO
        indexes => [
            {
                field => 'title',
                sort  => 'lexical',
            },
            {
                field => 'pages',
                sort  => 'numerical',
            },
            {
                field => 'author',
                sort  => 'lexical',
            },
        ],
    );

    my $books = [
        {
            title  => 'Nightfall',
            author => 'Isaac Asimov',
            pages  => 8,
        },
        {
            title  => 'Ubik',
            author => 'Phillip K. Dick',
            pages  => 9,
        },
        {
            title  => 'Daemon',
            author => 'Daniel Suarez',
            pages  => 10,
        },
        {
            title  => 'Lost Horizon',
            author => 'James Hilton',
            pages  => 12,
        },
        {
            title  => 'Three Laws of Robotics',
            author => 'Isaac Asimov',
            pages  => 88,
        },
    ];

    #
    # write
    #
    my $cv = AE::cv;
    $tcol->write(
        elements => $books,
        cb       => sub {
            my ( undef, $err ) = @_;

            warn $err if $err;

            $cv->send;
        },
    );
    $cv->recv;

    #
    # read by title
    #
    my $read_by_title;
    $cv = AE::cv;
    $tcol->read(
        order_by  => 'title',
        start_idx => 1,
        stop_idx  => 2,
        cb        => sub {
            $read_by_title = shift;
            my $err = shift;
            warn $err if $err;

            $cv->send;
        },
    );
    $cv->recv;

    is_deeply $read_by_title,
      [
        {
            title  => 'Lost Horizon',
            author => 'James Hilton',
            pages  => 12,
        },
        {
            title  => 'Nightfall',
            author => 'Isaac Asimov',
            pages  => 8,
        },
      ],
      'read elems ordered by title';


    #
    # read by pages
    #
    my $read_by_pages;
    $cv = AE::cv;
    $tcol->read(
        order_by  => 'pages',
        start_idx => 1,
        stop_idx  => 3,
        cb        => sub {
            $read_by_pages = shift;
            my $err = shift;

            warn $err if $err;

            $cv->send;
        },
    );
    $cv->recv;

    is_deeply $read_by_pages,
      [
        {
            title  => 'Ubik',
            author => 'Phillip K. Dick',
            pages  => 9,
        },
        {
            title  => 'Daemon',
            author => 'Daniel Suarez',
            pages  => 10,
        },
        {
            title  => 'Lost Horizon',
            author => 'James Hilton',
            pages  => 12,
        },
      ],
      'read elems ordered by pages';


    #
    # read by author
    #
    my $read_by_author;
    $cv = AE::cv;
    $tcol->read(
        order_by => 'author',
        cb       => sub {
            $read_by_author = shift;
            my $err = shift;

            warn $err if $err;

            $cv->send;
        },
    );
    $cv->recv;

    is_deeply $read_by_author,
        [
            {
                title  => 'Daemon',
                author => 'Daniel Suarez',
                pages  => 10,
            },
            {
                title  => 'Three Laws of Robotics',
                author => 'Isaac Asimov',
                pages  => 88,
            },
            {
                title  => 'Nightfall',
                author => 'Isaac Asimov',
                pages  => 8,
            },
            {
                title  => 'Lost Horizon',
                author => 'James Hilton',
                pages  => 12,
            },
            {
                title  => 'Ubik',
                author => 'Phillip K. Dick',
                pages  => 9,
            },
        ],
        'read elems ordered by author';
};

done_testing();
