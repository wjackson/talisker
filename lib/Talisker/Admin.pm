package Talisker::Admin;

use Moose;
use namespace::autoclean;
use JSON;

with 'Talisker::RedisRole';

sub initialize {
    my ($self, %args) = @_;

    my $db     = $args{db};
    my $fields = $args{fields};
    my $cb     = $args{cb};

    # write out fields
    $self->write_fields(
        fields => $fields,
        cb     => sub {
            my (undef, $err) = @_;

            return $cb->(undef, $err) if $err;
            return $cb->();
        },
    );

    return;
}

sub write_fields {
    my ($self, %args) = @_;

    my $fields = $args{fields};
    my $cb     = $args{cb};

    $self->redis->command(['SET', ':fields', encode_json($fields)], $cb);
}

__PACKAGE__->meta->make_immutable;
1;
