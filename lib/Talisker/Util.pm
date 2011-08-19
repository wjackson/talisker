package Talisker::Util;
use strict;
use warnings;
use feature 'switch';
use Carp;

our @ISA = qw(Exporter);
our @EXPORT_OK = qw(merge_point chain);

my %DEFAULTS = (
    at_a_time => 100,
);

sub merge_point {
    my (%args) = @_;

    given ( ref $args{work} ) {
        when ('ARRAY') { goto \&_mesh_merge_point }
        when ('CODE')  { goto \&_map_merge_point  }
        default { croak q/Argument work must be an ArrayRef or a CodeRef/ }
    };
}

#
# _map_merge_point
#
# Applies a callback given by argument 'work' to each element in 'inputs'.
#
sub _map_merge_point {
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
    my $any_err     = 0;

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

                return if $any_err;

                if ($err) {
                    $any_err = 1;
                    return $finished_cb->(undef, $err);
                }

                $outputs->[$index] = $output;

                return $finished_cb->($outputs) if $cb_count == @{ $inputs };

                $cb->();
            });
        }
    };

    $cb->();

    return;
}

#
# _mesh_merge_point
#
# Applies each cb in 'work' to its corresponding argument in 'inputs'.
#
sub _mesh_merge_point {
    my (%args) = @_;

    my $inputs      = $args{inputs};
    my $work_cbs    = $args{work};
    my $finished_cb = $args{finished};
    my $at_a_time   = $args{at_a_time} // $DEFAULTS{at_a_time};

    croak q/Argument 'inputs' is required/   if !defined $inputs;
    croak q/Argument 'work' is required/     if !defined $work_cbs;
    croak q/Argument 'finished' is required/ if !defined $finished_cb;

    croak q/Argument 'work' must be an ArrayRef/   if ref $work_cbs ne 'ARRAY';
    croak q/Argument 'finished' must be a CodeRef/ if ref $finished_cb ne 'CODE';

    $inputs //= map { undef } 1..@{ $work_cbs };

    my $inflight = 0;
    my $cb_count = 0;
    my $work_idx = 0;
    my $outputs  = [];
    my $any_err  = 0;

    my $cb; $cb = sub {

        while ($inflight < $at_a_time && $work_idx <= $#{ $work_cbs }) {

            $inflight++;

            # setup this work cb
            my $index   = $work_idx;
            my $work_cb = $work_cbs->[ $index ];
            my $input   = $inputs->[ $index ];
            $work_idx++;

            $work_cb->($input, sub {
                my ($output, $err) = @_;

                $cb_count++;
                $inflight--;

                return if $any_err;

                if ($err) {
                    $any_err = 1;
                    $finished_cb->(undef, $err);
                }

                $outputs->[$index] = $output;

                return $finished_cb->($outputs) if $cb_count == @{ $work_cbs };

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
