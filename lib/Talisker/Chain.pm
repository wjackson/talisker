package Talisker::Chain;

use Moose;
use namespace::autoclean;

has workers => (
    is      => 'ro',
    isa     => 'Int',
    default => 100,
);

has work_cb => (
    is       => 'ro',
    isa      => 'CodeRef',
    required => 1,
);

has finished_cb => (
    is       => 'ro',
    isa      => 'CodeRef',
    required => 1,
);

has inputs => (
    is       => 'ro',
    isa      => 'ArrayRef',
    required => 1,
);

has outputs => (
    is      => 'ro',
    isa     => 'ArrayRef',
    default => sub { [] },
    lazy    => 1,
);

has index => (
    is      => 'rw',
    isa     => 'Int',
    default => 0,
);

sub go {
    my ($self) = @_;

    my $workers = $self->workers;
    my $work_cb = $self->work_cb;
    my $inputs  = $self->inputs;

    my $inflight = 0;
    my $cb_count = 0;

    my $cb; $cb = sub {

        while ($inflight <= $workers && $self->index <= $#{ $inputs }) {

            $inflight++;

            my $index = $self->index;
            my $input = $inputs->[ $index ];

            $self->index( $index + 1 );

            $work_cb->($input, sub {
                my ($output, $err) = @_;

                $cb_count++;
                $inflight--;

                return $self->finished_cb->(undef, $err) if $err;

                $self->outputs->[$index] = $output;

                return $self->finished_cb->($self->outputs)
                    if $cb_count == @{ $inputs };

                $cb->();
            });
        }

        return;
    };

    $cb->();
}

__PACKAGE__->meta->make_immutable;
1;
