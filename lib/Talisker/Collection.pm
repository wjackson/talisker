package Talisker::Collection;

use feature 'say';
use Moose;
use namespace::autoclean;
use JSON;
use Digest::SHA1 qw(sha1_hex);
use Talisker::Chain;

with 'Talisker::RedisRole';

has id => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has indexes => (
    is       => 'ro',
    isa      => 'ArrayRef',
    required => 1,
);

has expire => (
    is      => 'ro',
    isa     => 'Int',
    default => 0,
);

has ops_per_write => (
    accessor   => 'ops_per_write',
    isa        => 'Int',
    lazy_build => 1,
);

sub _build_ops_per_write {
    my ($self) = @_;

    my $ops = 1 + @{ $self->indexes };;

    return $ops;
}

sub write {
    my ($self, %args) = @_;

    my $elements = $args{elements};
    my $cb       = $args{cb};

    # each element needs to be written
    my $work_cb = sub {
        my ( $elem, $cb ) = @_;

        $self->_write_elem( $elem, $cb );
    };

    # update indexes after all writes have completed
    my $finished_cb = sub {

        $self->_clear_indexes(sub {
            $self->_mk_indexes($cb);
        });
    };

    Talisker::Chain->new(
        inputs      => $elements,
        workers     => 10,
        work_cb     => $work_cb,
        finished_cb => $finished_cb,
    )->go;

    return;
}

sub _write_elem {
    my ($self, $elem, $cb) = @_;

    my $redis     = $self->redis;
    my $id        = $self->id;
    my $elem_json = encode_json $elem;
    my $elem_sha1 = sha1_hex $elem_json;

    my $cb_count     = 0;
    my $exp_cb_count = 0;

    # store the elment in a hash by it's SHA1
    $exp_cb_count++;
    $redis->command( ['HSET', $id, $elem_sha1, $elem_json ], sub {
        my (undef, $err) = @_;

        $cb_count++;

        return $cb->(undef, $err) if $err;
        return $cb->() if $cb_count == $exp_cb_count;
    });

    LEXICAL_INDEX_PREP_LOOP:
    for my $index (@{ $self->indexes }) {

        my $field = $index->{field};
        my $sort  = $index->{sort};

        next LEXICAL_INDEX_PREP_LOOP if $sort ne 'lexical';

        my $value = $elem->{$field};

        # maintain a sorted set of all the possible values for $field
        $exp_cb_count++;
        $redis->command( ['ZADD', "$id:$field", 0, $value ], sub {
            my (undef, $err) = @_;

            $cb_count++;

            return $cb->(undef, $err) if $err;
            return $cb->() if $cb_count == $exp_cb_count;
        });
    }

    return;
}

sub _mk_indexes {
    my ($self, $cb) = @_;

    my $id    = $self->id;
    my $redis = $self->redis;

    # get all the keys
    $redis->command( ['HKEYS', $id], sub {
        my ($sha1s, $err) = @_;

        return $cb->(undef, $err) if $err;

        # process each key...
        my $work_cb = sub {
            my ($sha1, $cb) = @_;

            $redis->command( ['HGET', $id, $sha1 ], sub {
                my ($elem_json, $err) = @_;

                return $cb->(undef, $err) if $err;

                my $elem = decode_json $elem_json;

                $self->_index_elem($sha1, $elem, $cb);
            });
        };

        Talisker::Chain->new(
            workers     => 10,
            inputs      => $sha1s,
            finished_cb => sub { $cb->() },
            work_cb     => $work_cb,
        )->go;

    });

    return;
}

sub _clear_indexes {
    my ($self, $cb) = @_;

    my $id    = $self->id;
    my $redis = $self->redis;

    my $work_cb = sub {
        my ($index, $cb) = @_;

        my $field = $index->{field};

        $redis->command( ['DEL', "$id:$field:idx"], sub {
            my (undef, $err);
            return $cb->(undef, $err) if $err;
            return $cb->();
        });
    };

    Talisker::Chain->new(
        workers     => 10,
        inputs      => $self->indexes,
        finished_cb => sub { $cb->() },
        work_cb     => $work_cb,
    )->go;

    return;
}

sub _index_elem {
    my ($self, $sha1, $elem, $cb) = @_;

    my $redis = $self->redis;
    my $id    = $self->id;

    my $cb_count     = 0;
    my $exp_cb_count = 0;

    for my $index (@{ $self->indexes }) {

        my $field = $index->{field};
        my $sort  = $index->{sort};

        my $value = $elem->{$field};

        if ($index->{sort} eq 'lexical') {

            # lookup score and index
            $exp_cb_count++;
            $redis->command( ['ZRANK', "$id:$field", $value], sub {
                my ($rank, $err) = @_;

                $cb_count++;

                return $cb->(undef, $err) if $err;

                $exp_cb_count++;
                $redis->command( ['ZADD', "$id:$field:idx", $rank, $sha1], sub {
                    my (undef, $err) = @_;

                    $cb_count++;

                    return $cb->(undef, $err) if $err;
                    return $cb->() if $cb_count == $exp_cb_count;
                });
            });
        }
        else {
            # index with value
            $exp_cb_count++;
            $redis->command( ['ZADD', "$id:$field:idx", $value, $sha1], sub {
                my (undef, $err) = @_;

                $cb_count++;

                return $cb->(undef, $err) if $err;
                return $cb->() if $cb_count == $exp_cb_count;
            });
        }
    }

    return;
}

sub read {
    my ($self, %args) = @_;

    my $order_by  = $args{order_by};
    my $start_idx = $args{start_idx} // 0;
    my $stop_idx  = $args{stop_idx}  // -1;
    my $cb        = $args{cb};

    my $redis     = $self->redis;
    my $id        = $self->id;

    my $elems     = {};
    my $elem_ids  = [];

    $redis->command( ['ZRANGE', "$id:$order_by:idx", $start_idx, $stop_idx ], sub {
        my ($elem_ids, $err) = @_;

        return $cb->(undef, $err) if $err;

        for my $elem_id (@{ $elem_ids }) {

            $redis->command( ['HGET', $id, $elem_id], sub {
                my ($elem_json, $err) = @_;

                return $cb->(undef, $err) if $err;

                $elems->{$elem_id} = decode_json $elem_json;

                if (keys(%$elems) == @{ $elem_ids }) {
                    $cb->([ map { $elems->{$_} } @{ $elem_ids } ]);
                }
            });
        }
    });

    return;
}

__PACKAGE__->meta->make_immutable;

1;

__END__
