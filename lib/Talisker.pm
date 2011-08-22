package Talisker;
# ABSTRACT: time series store

use Moose;
use namespace::autoclean;
use Talisker::Handle;

with 'Talisker::RedisRole';

# create a talisker db and return a handle
sub create {
    my ($self, %args) = @_;

    my $db     = $args{db};
    my $fields = $args{fields};
    my $cb     = $args{cb};

    my $t_handle = $self->handle(redis => $self->redis, db => $db);

    # write out fields
    $t_handle->write_fields(
        fields => $fields,
        cb     => sub {
            my (undef, $err) = @_;

            return $cb->(undef, $err) if $err;

            $cb->($t_handle);
        },
    );

    return;
}

# do we really want to block here?
sub handle {
    my ($self, %args) = @_;

    my $db = $args{db} // $self->default_db;

    return Talisker::Handle->new(redis => $self->redis, db => $db);
}

sub delete {
    # TODO: delete a db
}

__PACKAGE__->meta->make_immutable;
1;
