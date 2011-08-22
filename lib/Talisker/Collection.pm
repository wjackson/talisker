package Talisker::Collection;

use Moose;
use namespace::autoclean;
use Talisker::Util qw(merge_point chain);
use List::Util qw(sum);

has id => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has th => (
    accessor => 'th',
    isa      => 'Talisker::Handle',
    required => 1,
    handles  => [ 'redis' ],
);

sub write {
    my ($self, %args) = @_;

    my $pts = $args{points};
    my $cb  = $args{cb};

    chain(
        steps => [

            # write collection meta
            sub {
                $self->_write_collection_meta(
                    cb => $_[1],
                );
            },

            # write points
            sub {
                $self->_write_points(
                    points => $pts,
                    cb => $_[1],
                );
            },

        ],
        finished => $cb,
    );

    return;
}

sub _write_collection_meta {
    my ($self, %args) = @_;

    my $cb  = $args{cb};

    # collection entry
    $self->redis->command(['ZADD', ':collections', 0, $self->id], $cb);

    # collection meta

    return;
}

sub _write_points {
    my ($self, %args) = @_;

    my $pts = $args{points};
    my $cb  = $args{cb};

    chain(
        steps => [

            # read fields
            sub {
                $self->th->read_fields(
                    cb => $_[1],
                );
            },

            # write points
            sub {
                my ($fields, $cb) = @_;

                my $work_cb = sub {
                    my ($pt, $cb) = @_;

                    $self->_write_pt(
                        point  => $pt,
                        fields => $fields,
                        cb     => $cb,
                    );
                };

                merge_point(
                    inputs   => $pts,
                    work     => $work_cb,
                    finished => $cb,
                );
            },
        ],
        finished => $cb,
    );
}

sub _write_pt {
    my ($self, %args) = @_;

    my $pt     = $args{point};
    my $fields = $args{fields};
    my $cb     = $args{cb};

    my $redis = $self->redis;
    my $id    = $self->id;
    my $pt_id = "$pt->{tag}:$pt->{stamp}";

    chain(
        steps => [

            # read the point
            sub {
                my (undef, $cb) = @_;

                $self->th->read(
                    tag         => $pt->{tag},
                    start_stamp => $pt->{stamp},
                    end_stamp   => $pt->{stamp},
                    cb          => sub {
                        my ($ts, $err) = @_;

                        return $cb->(undef, $err) if $err;

                        my $read_pt = $ts->{points}->[0];

                        return $cb->($read_pt);
                    },
                );
            },

            # index the point in the collection
            sub {
                my ($read_pt, $cb) = @_;

                my $work_cb = sub {
                    my ($field, $cb) = @_;

                    my $fname = $field->{name};
                    my $fsort = $field->{sort};

                    my $value = $read_pt->{$fname};
                    my $score = $self->_score($value, $fsort);

                    $redis->command(
                        [ 'ZADD', "$id:idx:$fname", $score, $pt_id ], $cb
                    );
                };

                merge_point(
                    inputs  => [ { name => 'stamp' }, @{ $fields } ],
                    work    => $work_cb,
                    finished => $cb,
                );
            },

        ],
        finished => $cb,
    );

    return;
}

sub _score {
    my ($self, $value, $sort) = @_;

    $sort //= 'numeric';

    return $value if $sort eq 'numeric';

    #  score
    #       = first-byte-value*(256^3)
    #       + second-byte-value*(256^2)
    #       + third-byte-value*(256^1)
    #       + fourth-byte-value

    while (length($value) < 4) {
        $value .= '\0';
    }

    my @chars = reverse( (split '', $value)[0..3] );
    return sum map { ord( $chars[$_] // '' ) * 256**$_ } 0..$#chars;
}

sub read {
    my ($self, %args) = @_;

    my $order_by  = $args{order_by};
    my $start_idx = $args{start_idx} // 0;
    my $stop_idx  = $args{stop_idx}  // -1;
    my $cb        = $args{cb};

    my $th        = $self->th;
    my $redis     = $self->redis;
    my $id        = $self->id;

    my $pts       = [];

    $redis->command(
        ['ZRANGE', "$id:idx:$order_by", $start_idx, $stop_idx ], sub {
            my ($pt_ids, $err) = @_;

            return $cb->(undef, $err) if $err;

            my $work_cb = sub {
                my ($pt_id, $cb)  = @_;

                # XXX: maybe use a differnt delimter to seperate tag from stamp?
                my @pt_id = split /:/, $pt_id;
                my $stamp = pop @pt_id;
                my $tag   = join ':', @pt_id;

                $th->read(
                    tag         => $tag,
                    start_stamp => $stamp,
                    end_stamp   => $stamp,
                    cb          => sub {
                        my ( $ts, $err ) = @_;

                        return $cb->(undef, $err) if $err;

                        my $pt = $ts->{points}->[0];

                        push @{ $pts }, $pt;

                        $cb->();
                    },
                );
            };

            merge_point(
                inputs   => $pt_ids,
                work     => $work_cb,
                finished => sub { $cb->( $pts, $_[1] ) },
            );
        }
    );

    return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__
