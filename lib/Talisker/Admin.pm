package Talisker::Admin;

use Moose;
use namespace::autoclean;
use JSON;

with 'Talisker::RedisRole';

__PACKAGE__->meta->make_immutable;
1;
