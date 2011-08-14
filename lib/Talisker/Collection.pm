package Talisker::Collection;

use Moose;
use namespace::autoclean;
use JSON;
use Talisker::Util qw(merge_point);
use List::Util qw(sum);

has id => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has indexes => (
    is  => 'ro',
    isa => 'ArrayRef',
);

has talisker => (
    is       => 'ro',
    isa      => 'Talisker',
    required => 1,
    handles  => [ 'redis' ],
);

sub write {
    my ($self, %args) = @_;

    my $pts = $args{points};
    my $cb  = $args{cb};

    $self->_write_index_meta(sub { # write index data if we have it

        $self->_read_index_meta(sub { # read index data if we don't have it

            # write each pt
            my $work_cb = sub {
                my ( $pt, $cb ) = @_;

                $self->_write_pt( $pt, $cb );
            };

            merge_point(
                inputs   => $pts,
                work     => $work_cb,
                finished => $cb,
            );

        });
    });

    return;
}

sub _write_index_meta {
    my ($self, $cb) = @_;

    my $redis = $self->redis;
    my $id    = $self->id;

    return $cb->() if $self->indexes;

    $redis->command(
        ['SET', "$id:index_meta", encode_json $self->indexes], sub {
            my (undef, $err) = @_;

            return $cb->(undef, $err) if $err;
            return $cb->();
        }
    );
}

sub _read_index_meta {
    my ($self, $cb) = @_;

    my $redis = $self->redis;
    my $id    = $self->id;

    return $cb->() if $self->indexes;

    $redis->command(
        ['GET', "$id:index_meta"], sub {
            my ($indexes_json, $err) = @_;

            return $cb->(undef, $err) if $err;

            $self->indexes(decode_json($indexes_json));

            return $cb->();
        }
    );
}

sub _write_pt {
    my ($self, $pt, $cb) = @_;

    my $redis = $self->redis;
    my $id    = $self->id;
    my $pt_id = "$pt->{tag}:$pt->{stamp}";

    my $cmds_run = 0;
    my $cmds_ret = 0;

    my $cb_wrapper = sub {
        $cmds_ret++;
        my (undef, $err) = @_;

        return $cb->(undef, $err) if $err;
        return $cb->()            if $cmds_run == $cmds_ret;
    };

    # always index by stamp
    $cmds_run++;
    $redis->command(
        [ 'ZADD', "$id:idx:stamp", $pt->{stamp}, $pt_id ], $cb_wrapper
    );

    for my $index (@{ $self->indexes }) {

        my $field = $index->{field};
        my $sort  = $index->{sort};

        $cmds_run++;
        $self->talisker->read(
            tag         => $pt->{tag},
            start_stamp => $pt->{stamp},
            end_stamp   => $pt->{stamp},
            cb          => sub {
                $cmds_ret++;
                my ($ts, $err) = @_;

                return $cb->(undef, $err) if $err;

                my $value = decode_json( $ts->{points}->[0]->{value} )->{$field};

                my $score = $self->_score($value, $sort);

                $cmds_run++;
                $redis->command(
                    [ 'ZADD', "$id:idx:$field", $score, $pt_id ], $cb_wrapper
                );
            },
        );
    }

    return;
}

sub _score {
    my ($self, $value, $sort) = @_;

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

    my $talisker  = $self->talisker;
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

                $talisker->read(
                    tag         => $tag,
                    start_stamp => $stamp,
                    end_stamp   => $stamp,
                    cb          => sub {
                        my ( $ts, $err ) = @_;

                        return $cb->(undef, $err) if $err;

                        my $pt = decode_json $ts->{points}->[0]->{value};

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
