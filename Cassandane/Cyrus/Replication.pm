#!/usr/bin/perl
#
#  Copyright (c) 2011 Opera Software Australia Pty. Ltd.  All rights
#  reserved.
#
#  Redistribution and use in source and binary forms, with or without
#  modification, are permitted provided that the following conditions
#  are met:
#
#  1. Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following disclaimer.
#
#  2. Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in
#     the documentation and/or other materials provided with the
#     distribution.
#
#  3. The name "Opera Software Australia" must not be used to
#     endorse or promote products derived from this software without
#     prior written permission. For permission or any legal
#     details, please contact
# 	Opera Software Australia Pty. Ltd.
# 	Level 50, 120 Collins St
# 	Melbourne 3000
# 	Victoria
# 	Australia
#
#  4. Redistributions of any form whatsoever must retain the following
#     acknowledgment:
#     "This product includes software developed by Opera Software
#     Australia Pty. Ltd."
#
#  OPERA SOFTWARE AUSTRALIA DISCLAIMS ALL WARRANTIES WITH REGARD TO
#  THIS SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
#  AND FITNESS, IN NO EVENT SHALL OPERA SOFTWARE AUSTRALIA BE LIABLE
#  FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
#  WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
#  AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
#  OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#

use strict;
use warnings;
package Cassandane::Cyrus::Replication;
use base qw(Cassandane::Cyrus::TestCase);
use DateTime;
use Cassandane::Util::Log;
use Cassandane::Service;
use Cassandane::Config;

sub new
{
    my $class = shift;
    return $class->SUPER::new({ replica => 1 }, @_);
}

sub set_up
{
    my ($self) = @_;
    $self->SUPER::set_up();
}

sub tear_down
{
    my ($self) = @_;
    $self->SUPER::tear_down();
}

#
# Test replication of messages APPENDed to the master
#
sub test_append
{
    my ($self) = @_;

    my $master_store = $self->{master_store};
    my $replica_store = $self->{replica_store};

    xlog "generating messages A..D";
    my %exp;
    $exp{A} = $self->make_message("Message A", store => $master_store);
    $exp{B} = $self->make_message("Message B", store => $master_store);
    $exp{C} = $self->make_message("Message C", store => $master_store);
    $exp{D} = $self->make_message("Message D", store => $master_store);

    xlog "Before replication, the master should have all four messages";
    $self->check_messages(\%exp, store => $master_store);
    xlog "Before replication, the replica should have no messages";
    $self->check_messages({}, store => $replica_store);

    $self->run_replication();

    xlog "After replication, the master should still have all four messages";
    $self->check_messages(\%exp, store => $master_store);
    xlog "After replication, the replica should now have all four messages";
    $self->check_messages(\%exp, store => $replica_store);
}

1;
