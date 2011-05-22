package Talisker::RandomWalk;

use namespace::autoclean;
use Moose;

has prev_value => (
    is         => 'rw',
    isa        => 'Num',
    lazy_build => 1,
);

sub _build_prev_value {
    return rand(100);
}

has trend => (
    is       => 'ro',
    isa      => 'Int',
    lazy_build => 1,
);

sub _build_trend {
    return int(rand(10)) + 1;
}

# has avg_move => (
#     is         => 'ro',
#     isa        => 'Num',
#     lazy_build => 1,
# );
# 
# sub _build_avg_move {
#     my ($self) = @_;
# 
#     return $self->first_value * (rand(10) / 100);
# };

has volatility => (
    is => 'ro',
    isa => 'Num',
    lazy_build => 1,
);

sub _build_volatility {
    return rand(20);
}

sub _direction {
    my ($self) = @_;
    
    return int(rand(10))+1 <= $self->trend;
}

sub next {
    my ($self) = @_;

    my $prev       = $self->prev_value;
    my $volatility = $self->volatility;

    my $move_pct = rand($volatility);
    my $move     = $prev * ($move_pct / 100);
    my $next     = $self->_direction ? $prev + $move : $prev - $move;

    $self->prev_value($next);

    return $next;
}

1;
