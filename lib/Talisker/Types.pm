package Talisker::Types;
use strict;
use warnings;

use MooseX::Types::Moose qw(Int);
use MooseX::Types -declare => [qw/RedisDatabaseNumber/];

subtype RedisDatabaseNumber, as Int, where { $_ >= 0 && $_ < 16 },
    message { 'RedisDatabaseNumber should be between 0 and 15' };
