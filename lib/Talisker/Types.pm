package Talisker::Types;
use strict;
use warnings;

use MooseX::Types::Moose qw(Int Str);
use MooseX::Types -declare => [qw/BackendType RedisDatabaseNumber/];

subtype RedisDatabaseNumber, as Int, where { $_ >= 0 && $_ < 16 },
    message { 'RedisDatabaseNumber should be between 0 and 15' };

subtype BackendType, as Str, where { $_ eq 'Simple' || $_ eq 'Revision' },
    message { 'BackendType should be Simple or Revision' };
