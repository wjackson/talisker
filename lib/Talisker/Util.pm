package Talisker::Util;
use strict;
use warnings;
use Carp;

our @ISA = qw(Exporter);
our @EXPORT_OK = qw(merge_point chain);

my %DEFAULTS = (
    at_a_time => 100,
);

sub merge_point {
    my (%args) = @_;

    my $inputs      = $args{inputs};
    my $work_cb     = $args{work};
    my $finished_cb = $args{finished};
    my $at_a_time   = $args{at_a_time} // $DEFAULTS{at_a_time};

    croak q/Argument 'inputs' is required/   if !defined $inputs;
    croak q/Argument 'work' is required/     if !defined $work_cb;
    croak q/Argument 'finished' is required/ if !defined $finished_cb;

    croak q/Argument 'inputs' must be an ArrayRef/ if ref $inputs ne 'ARRAY';
    croak q/Argument 'work_cb' must be a CodeRef/  if ref $work_cb ne 'CODE';
    croak q/Argument 'finished' must be a CodeRef/ if ref $finished_cb ne 'CODE';

    my $inflight    = 0;
    my $cb_count    = 0;
    my $input_index = 0;
    my $outputs     = [];

    my $cb; $cb = sub {

        while ($inflight < $at_a_time && $input_index <= $#{ $inputs }) {

            $inflight++;

            # setup this work cb
            my $index = $input_index;
            my $input = $inputs->[ $index ];
            $input_index++;

            $work_cb->($input, sub {
                my ($output, $err) = @_;

                $cb_count++;
                $inflight--;

                return $finished_cb->(undef, $err) if $err;

                $outputs->[$index] = $output;

                return $finished_cb->($outputs)
                    if $cb_count == @{ $inputs };

                $cb->();
            });
        }
    };

    $cb->();

    return;
}

sub chain {
    my (%args) = @_;

    my $input       = $args{input};
    my $finished_cb = $args{finished};
    my $steps       = $args{steps};

    croak q/Argument 'finished' is required/ if !defined $finished_cb;
    croak q/Argument 'steps' is required/    if !defined $steps;

    croak q/Argument 'finished' must be a CodeRef/ if ref $finished_cb ne 'CODE';
    croak q/Argument 'steps' must be an ArrayRef/  if ref $steps ne 'ARRAY';

    my $cb; $cb = sub {
        my ($result, $err) = @_;

        return $finished_cb->(undef, $err) if $err;

        my $next_cb = shift @{ $steps };

        return $finished_cb->($result) if !defined $next_cb;

        $next_cb->($result, $cb);
    };

    $cb->($input);

    return;
}

1;
