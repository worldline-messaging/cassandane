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

package Cassandane::MaildirMessageStore;
use strict;
use warnings;
use base qw(Cassandane::MessageStore);
use File::Path qw(mkpath rmtree);

sub new
{
    my ($class, %params) = @_;
    my %bits = (
	directory => delete $params{directory},
	next_uid => 0 + (delete $params{next_uid} || 1),
	uids_to_read => [],
    );
    my $self = $class->SUPER::new(%params);
    map { $self->{$_} = $bits{$_}; } keys %bits;
    return $self;
}

sub write_begin
{
    my ($self) = @_;

    if (defined $self->{directory} && ! -d $self->{directory})
    {
	mkpath($self->{directory})
	    or die "Couldn't make path $self->{directory}";
    }
}

sub write_message
{
    my ($self, $msg) = @_;

    # find a filename which doesn't exist -- we're appending
    my $directory = ($self->{directory} || ".");
    my $filename;
    for (;;)
    {
	my $uid = $self->{next_uid};
	$self->{next_uid} = $self->{next_uid} + 1;
	$filename = "$directory/$uid.";
	last unless ( -f $filename );
    }

    my $fh;
    open $fh,'>',$filename
	or die "Cannot open $filename for writing: $!";
    print $fh $msg;
    close $fh;
}

sub write_end
{
    my ($self) = @_;
    # Nothing to do
}

sub read_begin
{
    my ($self) = @_;

    die "No such directory: $self->{directory}"
	if (defined $self->{directory} && ! -d $self->{directory});

    # Scan the directory for filenames.  We need to read the
    # whole directory and sort the results because the messages
    # need to be returned in uid order not directory order.
    $self->{uids_to_read} = [];
    my @uids;
    my $directory = ($self->{directory} || ".");
    my $fh;
    opendir $fh,$directory
	or die "Cannot open directory $directory for reading: $!";

    while (my $e = readdir $fh)
    {
	my ($uid) = ($e =~ m/^(\d+)\.$/);
	next unless defined $uid;
	push(@uids, 0+$uid);
    }

    @uids = sort { $a <=> $b } @uids;
    $self->{uids_to_read} = \@uids;
    closedir $fh;
}

sub read_message
{
    my ($self) = @_;

    my $directory = ($self->{directory} || ".");
    my $filename;

    for (;;)
    {
	my $uid = shift(@{$self->{uids_to_read}});
	return undef
	    unless defined $uid;
	$filename = "$directory/$uid.";
	# keep trying if a message disappeared
	last if ( -f $filename );
    }

    my $fh;
    open $fh,'<',$filename
	or die "Cannot open $filename for reading: $!";
    my $msg = Cassandane::Message->new(fh => $fh);
    close $fh;

    return $msg;
}

sub read_end
{
    my ($self) = @_;

    $self->{uids_to_read} = [];
}

sub remove
{
    my ($self) = @_;

    if (defined $self->{directory})
    {
	my $r = rmtree($self->{directory});
	die "rmtree failed: $!"
	    if (!$r && ! $!{ENOENT} );
    }
}

1;
