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
package Cassandane::Cyrus::Quota;
use base qw(Cassandane::Cyrus::TestCase);
use Cwd qw(abs_path);
use File::Path qw(mkpath);
use DateTime;
use Cassandane::Util::Log;
use Data::Dumper;

sub new
{
    my $class = shift;
    return $class->SUPER::new({ adminstore => 1 }, @_);
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

sub _set_quotaroot
{
    my ($self, $quotaroot) = @_;
    $self->{quotaroot} = $quotaroot;
}

# Utility function to set quota limits and check that it stuck
sub _set_limits
{
    my ($self, %resources) = @_;
    my $admintalk = $self->{adminstore}->get_client();

    my @quotalist;
    foreach my $resource (keys %resources)
    {
	my $limit = $resources{$resource}
	    or die "No limit specified for $resource";
	push(@quotalist, uc($resource), $limit);
    }
    $self->{limits}->{$self->{quotaroot}} = { @quotalist };
    $admintalk->setquota($self->{quotaroot}, \@quotalist);
    $self->assert_str_equals('ok', $admintalk->get_last_completion_response());
}

# Utility function to check that quota's usages
# and limits are where we expect it to be
sub _check_usages
{
    my ($self, %expecteds) = @_;
    my $admintalk = $self->{adminstore}->get_client();

    my $limits = $self->{limits}->{$self->{quotaroot}};

    my @result = $admintalk->getquota($self->{quotaroot});
    $self->assert_str_equals('ok', $admintalk->get_last_completion_response());

    # check actual and expected number of resources do match
    $self->assert_num_equals(scalar(keys %$limits) * 3, scalar(@result));

    # Convert the IMAP result to a conveniently checkable hash.
    # By checkable, we mean that a failure in assert_deep_equals()
    # will give a human some idea of what went wrong.
    my %act;
    while (scalar(@result)) {
	my ($res, $used, $limit) = splice(@result, 0, 3);
	$res = uc($res);
	die "Resource $res appears twice in result"
	    if defined $act{$res};
	$act{$res} = {
	    used => $used,
	    limit => $limit,
	};
    }

    # Build a conveniently checkable hash from %expecteds
    # and limits previously by _set_limits().
    my %exp;
    foreach my $res (keys %expecteds)
    {
	$exp{uc($res)} = {
	    used => $expecteds{$res},
	    limit => $limits->{uc($res)},
	};
    }

    # Now actually compare
    $self->assert_deep_equals(\%exp, \%act);
}

# Reset the recorded usage in the database.  Used for testing
# quota -f.  Rather hacky.  Both _set_quotaroot() and _set_limits()
# can be used to set default values.
sub _zap_quota
{
    my ($self, %params) = @_;

    my $quotaroot = $params{quotaroot} || $self->{quotaroot};
    my $limits = $params{limits} || $self->{limits}->{$quotaroot};
    my $useds = $params{useds} || {};

    # double check that some other part of Cassandane didn't
    # accidentally futz with the expected quota db backend
    my $backend = $self->{instance}->{config}->get('quota_db');
    $self->assert_str_equals('quotalegacy', $backend)
	if defined $backend;	    # the default value is also ok

    my ($c) = ($quotaroot =~ m/^user\.(.)/);
    my $dirname = $self->{instance}->{basedir} . "/conf/quota/$c";
    my $filename = "$dirname/$quotaroot";
    mkpath $dirname;

    open QUOTA,'>',$filename
	or die "Failed to open $filename for writing: $!";

    # STORAGE is special and always present, but -1 if unlimited
    my $limit = $limits->{STORAGE} || -1;
    my $used = $useds->{STORAGE} || 0;
    print QUOTA "$used\n$limit";

    # other resources have a leading keyword if present
    my %keywords = ( MESSAGE => 'M', 'X-ANNOTATION-STORAGE' => 'AS' );
    foreach my $resource (keys %$limits)
    {
	my $kw = $keywords{$resource} or next;
	$limit = $limits->{$resource};
	$used = $useds->{$resource} || 0;
	print QUOTA " $kw $used $limit";
    }

    print QUOTA "\n";
    close QUOTA;

    $self->{instance}->_fix_ownership($self->{instance}{basedir} . "/conf/quota");
}

# Utility function to check that there is no quota
sub _check_no_quota
{
    my ($self) = @_;
    my $admintalk = $self->{adminstore}->get_client();

    my @res = $admintalk->getquota($self->{quotaroot});
    $self->assert_str_equals('no', $admintalk->get_last_completion_response());
}

sub _delayed_expunge
{
    my ($self) = @_;

    $self->{store}->disconnect();
    $self->{adminstore}->disconnect();

    my @cmd = ( 'cyr_expire', '-E', '1', '-X', '0', '-D', '0' );
    push(@cmd, '-v')
	if get_verbose;
    $self->{instance}->run_command({ cyrus => 1 }, @cmd);

    $self->{store}->get_client();
    $self->{adminstore}->get_client();
}

sub test_using_storage
{
    my ($self) = @_;

    xlog "test increasing usage of the STORAGE quota resource as messages are added";
    $self->_set_quotaroot('user.cassandane');
    xlog "set ourselves a basic limit";
    $self->_set_limits(storage => 100000);
    $self->_check_usages(storage => 0);
    my $talk = $self->{store}->get_client();

    $talk->create("INBOX.sub") || die "Failed to create subfolder";

    # append some messages
    my %expecteds = ();
    my $expected = 0;
    foreach my $folder ("INBOX", "INBOX.sub")
    {
	$expecteds{$folder} = 0;
	$self->{store}->set_folder($folder);

	for (1..10)
	{
	    my $msg = $self->make_message("Message $_",
					  extra_lines => 10 + rand(5000));
	    my $len = length($msg->as_string());
	    $expecteds{$folder} += $len;
	    $expected += $len;
	    xlog "added $len bytes of message";
	    $self->_check_usages(storage => int($expected/1024));
	}
    }

    # delete subfolder
    $talk->delete("INBOX.sub") || die "Failed to delete subfolder";
    $expected -= delete($expecteds{"INBOX.sub"});
    $self->_check_usages(storage => int($expected/1024));

    # delete messages
    $talk->select("INBOX");
    $talk->store('1:*', '+flags', '(\\deleted)');
    $talk->close();
    $expected -= delete($expecteds{"INBOX"});
    $self->assert_num_equals(0, $expected);
    $self->_check_usages(storage => int($expected/1024));
}

sub test_using_storage_late
{
    my ($self) = @_;

    xlog "test setting STORAGE quota resource after messages are added";

    $self->_set_quotaroot('user.cassandane');
    $self->_check_no_quota();
    my $talk = $self->{store}->get_client();

    $talk->create("INBOX.sub") || die "Failed to create subfolder";

    # append some messages
    my %expecteds = ();
    my $expected = 0;
    foreach my $folder ("INBOX", "INBOX.sub")
    {
	$expecteds{$folder} = 0;
	$self->{store}->set_folder($folder);

	for (1..10)
	{
	    my $msg = $self->make_message("Message $_",
					  extra_lines => 10 + rand(5000));
	    my $len = length($msg->as_string());
	    $expecteds{$folder} += $len;
	    $expected += $len;
	    xlog "added $len bytes of message";
	}
    }

    $self->_set_limits(storage => 100000);
    $self->_check_usages(storage => int($expected/1024));

    # delete subfolder
    $talk->delete("INBOX.sub") || die "Failed to delete subfolder";
    $expected -= delete($expecteds{"INBOX.sub"});
    $self->_check_usages(storage => int($expected/1024));

    # delete messages
    $talk->select("INBOX");
    $talk->store('1:*', '+flags', '(\\deleted)');
    $talk->close();
    $expected -= delete($expecteds{"INBOX"});
    $self->assert_num_equals(0, $expected);
    $self->_check_usages(storage => int($expected/1024));
}

sub test_exceeding_storage
{
    my ($self) = @_;

    xlog "test exceeding the STORAGE quota limit";

    my $imaptalk = $self->{store}->get_client();

    xlog "set a low limit";
    $self->_set_quotaroot('user.cassandane');
    $self->_set_limits(storage => 210);
    $self->_check_usages(storage => 0);

    xlog "adding messages to get just below the limit";
    my %msgs;
    my $slack = 200 * 1024;
    my $n = 1;
    my $expected = 0;
    while ($slack > 1000)
    {
	my $nlines = int(($slack - 640) / 23);
	$nlines = 1000 if ($nlines > 1000);

	my $msg = $self->make_message("Message $n",
				      extra_lines => $nlines);
	my $len = length($msg->as_string());
	$slack -= $len;
	$expected += $len;
	xlog "added $len bytes of message";
	$msgs{$n} = $msg;
	$n++;
    }
    xlog "check that the messages are all in the mailbox";
    $self->check_messages(\%msgs);
    xlog "check that the usage is just below the limit";
    $self->_check_usages(storage => int($expected/1024));

    xlog "add a message that exceeds the limit";
    my $nlines = int(($slack - 640) / 23) * 2;
    $nlines = 500 if ($nlines < 500);
    $self->assert_str_equals('ok', $imaptalk->get_last_completion_response());
    eval
    {
	my $msg = $self->make_message("Message $n",
				      extra_lines => $nlines);
    };
    $self->assert_str_equals('no', $imaptalk->get_last_completion_response());
    $self->assert($imaptalk->get_last_error() =~ m/over quota/i);

    xlog "check that the exceeding message is not in the mailbox";
    $self->check_messages(\%msgs);
    xlog "check that the quota usage is still just below the limit";
    $self->_check_usages(storage => int($expected/1024));
}

sub test_using_message
{
    my ($self) = @_;

    xlog "test increasing usage of the MESSAGE quota resource as messages are added";

    $self->_set_quotaroot('user.cassandane');
    my $talk = $self->{store}->get_client();

    xlog "set ourselves a basic limit";
    $self->_set_limits(message => 50000);
    $self->_check_usages(message => 0);

    $talk->create("INBOX.sub") || die "Failed to create subfolder";

    # append some messages
    my %expecteds = ();
    my $expected = 0;
    foreach my $folder ("INBOX", "INBOX.sub")
    {
	$expecteds{$folder} = 0;
	$self->{store}->set_folder($folder);

	for (1..10)
	{
	    my $msg = $self->make_message("Message $_");
	    $expecteds{$folder}++;
	    $expected++;
	    $self->_check_usages(message => $expected);
	}
    }

    # delete subfolder
    $talk->delete("INBOX.sub") || die "Failed to delete subfolder";
    $expected -= $expecteds{"INBOX.sub"};
    $self->_check_usages(message => $expected);

    # delete messages
    $talk->select("INBOX");
    $talk->store('1:*', '+flags', '(\\deleted)');
    $talk->close();
    $expected -= delete($expecteds{"INBOX"});
    $self->assert_num_equals(0, $expected);
    $self->_check_usages(message => $expected);
}

sub test_using_message_late
{
    my ($self) = @_;

    xlog "test setting MESSAGE quota resource after messages are added";

    $self->_set_quotaroot('user.cassandane');
    my $talk = $self->{store}->get_client();
    $self->_check_no_quota();

    $talk->create("INBOX.sub") || die "Failed to create subfolder";

    # append some messages
    my %expecteds = ();
    my $expected = 0;
    foreach my $folder ("INBOX", "INBOX.sub")
    {
	$expecteds{$folder} = 0;
	$self->{store}->set_folder($folder);

	for (1..10)
	{
	    my $msg = $self->make_message("Message $_");
	    $expecteds{$folder}++;
	    $expected++;
	}
    }

    $self->_set_limits(message => 50000);
    $self->_check_usages(message => $expected);

    # delete subfolder
    $talk->delete("INBOX.sub") || die "Failed to delete subfolder";
    $expected -= $expecteds{"INBOX.sub"};
    $self->_check_usages(message => $expected);

    # delete messages
    $talk->select("INBOX");
    $talk->store('1:*', '+flags', '(\\deleted)');
    $talk->close();
    $expected -= delete($expecteds{"INBOX"});
    $self->assert_num_equals(0, $expected);
    $self->_check_usages(message => $expected);
}

sub test_exceeding_message
{
    my ($self) = @_;

    xlog "test exceeding the MESSAGE quota limit";

    my $talk = $self->{store}->get_client();

    xlog "set a low limit";
    $self->_set_quotaroot('user.cassandane');
    $self->_set_limits(message => 10);
    $self->_check_usages(message => 0);

    xlog "adding messages to get just below the limit";
    my %msgs;
    for (1..10)
    {
	$msgs{$_} = $self->make_message("Message $_");
    }
    xlog "check that the messages are all in the mailbox";
    $self->check_messages(\%msgs);
    xlog "check that the usage is just below the limit";
    $self->_check_usages(message => 10);

    xlog "add a message that exceeds the limit";
    eval
    {
	my $msg = $self->make_message("Message 11");
    };
    # As opposed to storage checking, which is currently done after receiving the
    # (LITERAL) mail, message count checking is performed right away. This early
    # NO response while writing the LITERAL triggers a die in IMAPTalk, leaving
    # the completion response undefined.
    my $ex = $@;
    $self->assert_not_null($ex);
    $self->assert($ex =~ m/Got - \d+ NO Over quota/);

    xlog "check that the exceeding message is not in the mailbox";
    $self->_check_usages(message => 10);
    $self->check_messages(\%msgs);
}

sub test_using_annotstorage_msg
{
    my ($self) = @_;

    xlog "test setting X-ANNOTATION-STORAGE quota resource after";
    xlog "per-message annotations are added";

    $self->_set_quotaroot('user.cassandane');
    my $talk = $self->{store}->get_client();

    xlog "set ourselves a basic limit";
    $self->_set_limits('x-annotation-storage' => 100000);
    $self->_check_usages('x-annotation-storage' => 0);

    $talk->create("INBOX.sub1") || die "Failed to create subfolder";
    $talk->create("INBOX.sub2") || die "Failed to create subfolder";

    xlog "make some messages to hang annotations on";
    my %expecteds = ();
    my $expected = 0;
    foreach my $folder ("INBOX", "INBOX.sub1", "INBOX.sub2")
    {
	$self->{store}->set_folder($folder);
	$expecteds{$folder} = 0;
	my $uid = 1;
	for (1..5)
	{
	    $self->make_message("Message $uid");

            my $data = $self->make_random_data(10);
	    $talk->store('' . $uid, 'annotation', ['/comment', ['value.priv', { Literal => $data }]]);
	    $self->assert_str_equals('ok', $talk->get_last_completion_response());
	    $uid++;
	    $expecteds{$folder} += length($data);
	    $expected += length($data);
	    $self->_check_usages('x-annotation-storage' => int($expected/1024));
	}
    }

    xlog "delete subfolder sub1";
    $talk->delete("INBOX.sub1") || die "Failed to delete subfolder";
    $expected -= delete($expecteds{"INBOX.sub1"});

    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "delete messages in sub2";
    $talk->select("INBOX.sub2");
    $talk->store('1:*', '+flags', '(\\deleted)');
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $talk->expunge();

    xlog "Unlike STORAGE, X-ANNOTATION-STORAGE quota is not updated until actual expunge";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "Force a delayed expunge";
    $self->_delayed_expunge();
    $talk = $self->{store}->get_client();

    xlog "X-ANNOTATION-STORAGE quota should have gone down";
    $expected -= delete($expecteds{"INBOX.sub2"});
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "delete annotations on INBOX";
    $talk->select("INBOX");
    $talk->store('1:*', 'annotation', ['/comment', ['value.priv', undef]]);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $talk->close();
    $expected -= delete($expecteds{"INBOX"});
    $self->assert_num_equals(0, $expected);
    $self->_check_usages('x-annotation-storage' => int($expected/1024));
}

sub test_using_annotstorage_msg_late
{
    my ($self) = @_;

    xlog "test increasing usage of the X-ANNOTATION-STORAGE quota";
    xlog "resource as per-message annotations are added";

    $self->_set_quotaroot('user.cassandane');
    my $talk = $self->{store}->get_client();

    $self->_check_no_quota();

    $talk->create("INBOX.sub1") || die "Failed to create subfolder";
    $talk->create("INBOX.sub2") || die "Failed to create subfolder";

    xlog "make some messages to hang annotations on";
    my %expecteds = ();
    my $expected = 0;
    foreach my $folder ("INBOX", "INBOX.sub1", "INBOX.sub2")
    {
	$self->{store}->set_folder($folder);
	$expecteds{$folder} = 0;
	my $uid = 1;
	for (1..5)
	{
	    $self->make_message("Message $uid");

            my $data = $self->make_random_data(10);
	    $talk->store('' . $uid, 'annotation', ['/comment', ['value.priv', { Literal => $data }]]);
	    $self->assert_str_equals('ok', $talk->get_last_completion_response());
	    $uid++;
	    $expecteds{$folder} += length($data);
	    $expected += length($data);
	}
    }

    $self->_set_limits('x-annotation-storage' => 100000);
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "delete subfolder sub1";
    $talk->delete("INBOX.sub1") || die "Failed to delete subfolder";
    $expected -= delete($expecteds{"INBOX.sub1"});
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "delete messages in sub2";
    $talk->select("INBOX.sub2");
    $talk->store('1:*', '+flags', '(\\deleted)');
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $talk->expunge();

    xlog "Unlike STORAGE, X-ANNOTATION-STORAGE quota is not updated until actual expunge";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "Force a delayed expunge";
    $self->_delayed_expunge();
    $talk = $self->{store}->get_client();

    xlog "X-ANNOTATION-STORAGE quota should have gone down";
    $expected -= delete($expecteds{"INBOX.sub2"});
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "delete annotations on INBOX";
    $talk->select("INBOX");
    $talk->store('1:*', 'annotation', ['/comment', ['value.priv', undef]]);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $expected -= delete($expecteds{"INBOX"});
    $self->assert_num_equals(0, $expected);
    $self->_check_usages('x-annotation-storage' => int($expected/1024));
}

sub test_using_annotstorage_mbox
{
    my ($self) = @_;

    xlog "test setting X-ANNOTATION-STORAGE quota resource after";
    xlog "per-mailbox annotations are added";

    $self->_set_quotaroot('user.cassandane');
    my $talk = $self->{store}->get_client();

    xlog "set ourselves a basic limit";
    $self->_set_limits('x-annotation-storage' => 100000);
    $self->_check_usages('x-annotation-storage' => 0);

    $talk->create("INBOX.sub") || die "Failed to create subfolder";

    xlog "store annotations";
    my %expecteds = ();
    my $expected = 0;
    foreach my $folder ("INBOX", "INBOX.sub")
    {
	$expecteds{$folder} = 0;
	$self->{store}->set_folder($folder);
	my $data = '';
	while ($expecteds{$folder} <= 60*1024)
	{
	    my $moredata = $self->make_random_data(5);
	    $data .= $moredata;
	    $talk->setmetadata($self->{store}->{folder}, '/private/comment', { Literal => $data });
	    $self->assert_str_equals('ok', $talk->get_last_completion_response());
	    $expecteds{$folder} += length($moredata);
	    $expected += length($moredata);
	    $self->_check_usages('x-annotation-storage' => int($expected/1024));
	}
    }

    # delete subfolder
    $talk->delete("INBOX.sub") || die "Failed to delete subfolder";
    $expected -= delete($expecteds{"INBOX.sub"});
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    # delete remaining annotations
    $self->{store}->set_folder("INBOX");
    $talk->setmetadata($self->{store}->{folder}, '/private/comment', undef);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $expected -= delete($expecteds{"INBOX"});
    $self->assert_num_equals(0, $expected);
    $self->_check_usages('x-annotation-storage' => int($expected/1024));
}

sub test_using_annotstorage_mbox_late
{
    my ($self) = @_;

    xlog "test increasing usage of the X-ANNOTATION-STORAGE quota";
    xlog "resource as per-mailbox annotations are added";

    $self->_set_quotaroot('user.cassandane');
    my $talk = $self->{store}->get_client();

    $self->_check_no_quota();

    $talk->create("INBOX.sub") || die "Failed to create subfolder";

    xlog "store annotations";
    my %expecteds = ();
    my $expected = 0;
    foreach my $folder ("INBOX", "INBOX.sub")
    {
	$expecteds{$folder} = 0;
	$self->{store}->set_folder($folder);
	my $data = '';
	while ($expecteds{$folder} <= 60*1024)
	{
	    my $moredata = $self->make_random_data(5);
	    $data .= $moredata;
	    $talk->setmetadata($self->{store}->{folder}, '/private/comment', { Literal => $data });
	    $self->assert_str_equals('ok', $talk->get_last_completion_response());
	    $expecteds{$folder} += length($moredata);
	    $expected += length($moredata);
	}
    }

    $self->_set_limits('x-annotation-storage' => 100000);
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    # delete subfolder
    $talk->delete("INBOX.sub") || die "Failed to delete subfolder";
    $expected -= delete($expecteds{"INBOX.sub"});
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    # delete remaining annotations
    $self->{store}->set_folder("INBOX");
    $talk->setmetadata($self->{store}->{folder}, '/private/comment', undef);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $expected -= delete($expecteds{"INBOX"});
    $self->assert_num_equals(0, $expected);
    $self->_check_usages('x-annotation-storage' => int($expected/1024));
}

#
# Test renames
#
sub test_quotarename
{
    my ($self) = @_;

    my $admintalk = $self->{adminstore}->get_client();
    my $imaptalk = $self->{store}->get_client();

    # Right - let's set ourselves a basic usage quota
    $self->_set_quotaroot('user.cassandane');
    $self->_set_limits(
	storage => 100000,
	message => 50000,
	'x-annotation-storage' => 10000,
    );
    $self->_check_usages(
	storage => 0,
	message => 0,
	'x-annotation-storage' => 0,
    );

    my $expected_storage = 0;
    my $expected_message = 0;
    my $expected_annotation_storage = 0;
    my $uid = 1;
    for (1..10) {
	my $msg = $self->make_message("Message $_", extra_lines => 5000);
	$expected_storage += length($msg->as_string());
	$expected_message++;

	my $annotation = $self->make_random_data(1);
	$expected_annotation_storage += length($annotation);
	$imaptalk->store('' . $uid, 'annotation', ['/comment', ['value.priv', { Literal => $annotation }]]);
	$self->assert_str_equals('ok', $imaptalk->get_last_completion_response());
	$uid++;
    }

    $self->_check_usages(
	storage => int($expected_storage/1024),
	message => $expected_message,
	'x-annotation-storage' => int($expected_annotation_storage/1024),
    );

    $imaptalk->create("INBOX.sub") || die "Failed to create subfolder";
    $self->{store}->set_folder("INBOX.sub");
    $imaptalk->select($self->{store}->{folder}) || die;
    my $expected_storage_more = $expected_storage;
    my $expected_message_more = $expected_message;
    my $expected_annotation_storage_more = $expected_annotation_storage;
    $uid = 1;
    for (1..10) {

	my $msg = $self->make_message("Message $_",
				      extra_lines => 10 + rand(5000));
	$expected_storage_more += length($msg->as_string());
	$expected_message_more++;

	my $annotation = $self->make_random_data(1);
	$expected_annotation_storage_more += length($annotation);
	$imaptalk->store('' . $uid, 'annotation', ['/comment', ['value.priv', { Literal => $annotation }]]);
	$self->assert_str_equals('ok', $imaptalk->get_last_completion_response());
	$uid++;
    }
    $self->{store}->set_folder("INBOX");
    $imaptalk->select($self->{store}->{folder}) || die;

    $self->_check_usages(
	storage => int($expected_storage_more/1024),
	message => $expected_message_more,
	'x-annotation-storage' => int($expected_annotation_storage_more/1024),
    );

    $imaptalk->rename("INBOX.sub", "INBOX.othersub") || die;
    $imaptalk->select("INBOX.othersub") || die;

    # usage should be the same after a rename
    $self->_check_usages(
	storage => int($expected_storage_more/1024),
	message => $expected_message_more,
	'x-annotation-storage' => int($expected_annotation_storage_more/1024),
    );

    $imaptalk->delete("INBOX.othersub") || die;

    $self->_check_usages(
	storage => int($expected_storage/1024),
	message => $expected_message,
	'x-annotation-storage' => int($expected_annotation_storage/1024),
    );
}

sub test_quota_f
{
    my ($self) = @_;

    my $admintalk = $self->{adminstore}->get_client();

    # Right - let's set ourselves a basic usage quota
    $self->_set_quotaroot('user.cassandane');
    $self->_set_limits(
	storage => 100000,
	message => 50000,
	'x-annotation-storage' => 10000,
    );
    $self->_check_usages(
	storage => 0,
	message => 0,
	'x-annotation-storage' => 0,
    );

    $self->{instance}->create_user("quotafuser");
    $self->{adminstore}->set_folder("user.quotafuser");
    my $quotafuser_expected_storage = 0;
    my $quotafuser_expected_message = 0;
    my $quotafuser_expected_annotation_storage = 0;
    for (1..3) {
	my $msg = $self->make_message("QuotaFUser $_", store => $self->{adminstore}, extra_lines => 17000);
	$quotafuser_expected_storage += length($msg->as_string());
	$quotafuser_expected_message++;
    }
    for (1..10) {
	$self->make_message("Cassandane $_", extra_lines => 5000);
    }

    my $annotation = $self->make_random_data(10);
    $admintalk->setmetadata($self->{adminstore}->{folder}, '/private/comment', { Literal => $annotation });
    $quotafuser_expected_annotation_storage = length($annotation);
    $admintalk->setmetadata($self->{store}->{folder}, '/private/comment', { Literal => $annotation });

    my @origcasres = $admintalk->getquota("user.cassandane");
    $self->assert_num_equals(9, scalar @origcasres);

    # create a bogus quota file
    $self->_zap_quota(quotaroot => 'user.quotafuser',
		      limits => {
			storage => 100000,
			message => 50000,
			'x-annotation-storage' => 10000,
		      });

    # find and add the quota
    $self->{instance}->run_command({ cyrus => 1 }, 'quota', '-f');

    my @res = $admintalk->getquota("user.quotafuser");
    $self->assert_num_equals(9, scalar @res);
    my @casres = $admintalk->getquota("user.cassandane");
    $self->assert_num_equals(9, scalar @casres);

    # re-run the quota utility
    $self->{instance}->run_command({ cyrus => 1 }, 'quota', '-f');

    my @res2 = $admintalk->getquota("user.quotafuser");
    $self->assert_num_equals(9, scalar @res2);
    my @casres2 = $admintalk->getquota("user.cassandane");
    $self->assert_num_equals(9, scalar @casres2);

    $self->assert_num_equals(int($quotafuser_expected_storage/1024), $res[1]);
    $self->assert_num_equals($quotafuser_expected_message, $res[4]);
    $self->assert_num_equals(int($quotafuser_expected_annotation_storage/1024), $res[7]);

    # usage should be unchanged
    $self->assert($res[1] == $res2[1] && $res[4] == $res2[4] && $res[7] == $res2[7] && 
		  $casres[1] == $casres2[1] && $casres[4] == $casres2[4] && $casres[7] == $casres2[7] &&
		  $casres[1] == $origcasres[1] && $casres[4] == $origcasres[4] && $casres[7] == $origcasres[7], 
		  "Quota mismatch: quotafuser (1: " . Dumper(\@res) . ", 2: " . Dumper(\@res2) . ") " .
		  "cassandane (0: " . Dumper(\@origcasres) . ", 1: " . Dumper(\@casres) . ", 2: " . Dumper(\@casres2) . ")");
}

# Test races between quota -f and updates to mailboxes
sub test_quota_f_vs_update
{
    my ($self) = @_;

    my $basefolder = "user.cassandane";
    my @folders = qw(a b c d e);
    my $msg;
    my $expected;

    xlog "Set up a large but limited quota";
    $self->_set_quotaroot($basefolder);
    $self->_set_limits(storage => 1000000);
    $self->_check_usages(storage => 0);
    my $imaptalk = $self->{store}->get_client();

    xlog "Create some sub folders";
    for my $f (@folders)
    {
	$imaptalk->create("$basefolder.$f") || die "Failed $@";
	$self->{store}->set_folder("$basefolder.$f");
	$msg = $self->make_message("Cassandane $f",
				      extra_lines => 2000+rand(5000));
	$expected += length($msg->as_string());
    }
    # unselect so quota -f can lock the mailboxes
    $imaptalk->unselect();

    xlog "Check that we have some quota usage";
    $self->_check_usages(storage => int($expected/1024));

    xlog "Start a quota -f scan";
    $self->{instance}->quota_Z_go($basefolder);
    $self->{instance}->quota_Z_go("$basefolder.a");
    $self->{instance}->quota_Z_go("$basefolder.b");
    my (@bits) = $self->{instance}->run_command({ cyrus => 1, background => 1 },
	'quota', '-Z', '-f', $basefolder);

    # waiting for quota -f to ensure that
    # a) the -Z mechanism is working and
    # b) quota -f has at least initialised and started scanning.
    $self->{instance}->quota_Z_wait("$basefolder.b");

    # quota -f is now waiting to be allowed to proceed to "c"

    xlog "Mailbox update behind the scan";
    $self->{store}->set_folder("$basefolder.b");
    $msg = $self->make_message("Cassandane b UPDATE",
				  extra_lines => 2000+rand(3000));
    $expected += length($msg->as_string());

    xlog "Mailbox update in front of the scan";
    $self->{store}->set_folder("$basefolder.d");
    $msg = $self->make_message("Cassandane d UPDATE",
				  extra_lines => 2000+rand(3000));
    $expected += length($msg->as_string());

    xlog "Let quota -f continue and finish";
    $self->{instance}->quota_Z_go("$basefolder.c");
    $self->{instance}->quota_Z_go("$basefolder.d");
    $self->{instance}->quota_Z_go("$basefolder.e");
    $self->{instance}->quota_Z_wait("$basefolder.e");
    $self->{instance}->reap_command(@bits);

    xlog "Check that we have the correct quota usage";
    $self->_check_usages(storage => int($expected/1024));
}

sub test_quota_f_nested_qr
{
    my ($self) = @_;

    xlog "Test that quota -f correctly calculates the STORAGE quota";
    xlog "when a nested quotaroot exists [Bug 3621]";

    my $inbox = "user.cassandane";
    my $nested = "$inbox.nested";

    xlog "set ourselves a basic usage quota";
    $self->_set_quotaroot($inbox);
    $self->_set_limits(storage => 100000);
    $self->_check_usages(storage => 0);

    xlog "make an empty nested mailbox";
    my $talk = $self->{store}->get_client();
    $talk->create($nested)
	or die "Cannot create mailbox $nested: $@";

    xlog "setup a quotaroot on the nested mailbox";
    $self->_set_quotaroot($nested);
    $self->_set_limits(storage => 5);
    $self->_check_usages(storage => 0);

    $self->_set_quotaroot($inbox);

    xlog "add messages to use some STORAGE quota";
    my $expected = 0;
    for (1..20) {
	my $msg = $self->make_message("$_",
				      min_extra_lines => 5000,
				      max_extra_lines => 20000);
	$expected += length($msg->as_string());
    }

    xlog "check that STORAGE quota is used";
    $self->_check_usages(storage => int($expected/1024));

    xlog "create a bogus quota file";
    $self->_zap_quota();
    $self->_check_usages(storage => 0);

    xlog "run quota -f to find and add the quota";
    $self->{instance}->run_command({ cyrus => 1 }, 'quota', '-f');

    xlog "check that STORAGE quota is restored";
    $self->_check_usages(storage => int($expected/1024));

    xlog "run quota -f again";
    $self->{instance}->run_command({ cyrus => 1 }, 'quota', '-f');

    xlog "check that STORAGE quota is still correct";
    $self->_check_usages(storage => int($expected/1024));
}

sub test_prefix_mboxexists
{
    my ($self) = @_;

    my $admintalk = $self->{adminstore}->get_client();

    $self->{instance}->create_user("base",
				   subdirs => [ qw(subdir subdir2) ]);
    $self->{adminstore}->set_folder("user.base");
    for (1..3) {
	$self->make_message("base $_", store => $self->{adminstore}, extra_lines => 12345);
    }
    $self->{adminstore}->set_folder("user.base.subdir2");
    for (1..3) {
	$self->make_message("base $_", store => $self->{adminstore}, extra_lines => 12345);
    }

    $self->{instance}->create_user("baseplus",
				   subdirs => [ qw(subdir) ]);
    $admintalk->setquota("user.baseplus", "(storage 1000000)");
    $self->{adminstore}->set_folder("user.baseplus");
    for (1..3) {
	$self->make_message("baseplus $_", store => $self->{adminstore}, extra_lines => 31419);
    }
    $self->{adminstore}->set_folder("user.baseplus.subdir");
    for (1..3) {
	$self->make_message("baseplus $_", store => $self->{adminstore}, extra_lines => 31419);
    }

    my @origplus = $admintalk->getquota("user.baseplus");
    $self->assert_num_equals(3, scalar @origplus);

    $self->{instance}->run_command({ cyrus => 1 }, 'quota', '-f', "user.base");

    my @nextplus = $admintalk->getquota("user.baseplus");
    $self->assert_num_equals(3, scalar @nextplus);

    # usage should be unchanged
    $self->assert($origplus[1] == $nextplus[1],
		  "usage of subdir (1: $origplus[1], 2: $nextplus[1])");
}

sub test_upgrade_v2_4
{
    my ($self) = @_;

    xlog "test resources usage computing upon upgrading a cyrus v2.4 mailbox";

    $self->_set_quotaroot('user.cassandane');
    my $talk = $self->{store}->get_client();
    my $admintalk = $self->{adminstore}->get_client();

    xlog "set ourselves a basic limit";
    $self->_set_limits('x-annotation-storage' => 100000);
    $self->_check_usages('x-annotation-storage' => 0);

    xlog "store annotations";
    my $data = $self->make_random_data(10);
    my $expected_annotation_storage = length($data);
    $talk->setmetadata($self->{store}->{folder}, '/private/comment', { Literal => $data });
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $self->_check_usages('x-annotation-storage' => int($expected_annotation_storage/1024));

    xlog "restore cyrus v2.4 mailbox content and quota file";
    $self->{instance}->unpack(abs_path('data/cyrus/quota_upgrade_v2_4.user.tar.gz'), 'data/user');
    $self->{instance}->unpack(abs_path('data/cyrus/quota_upgrade_v2_4.quota.tar.gz'), 'conf/quota/c');

    # count messages and size from restored mailbox
    my $expected_storage = 0;
    my $expected_message = 0;
    $talk->select($self->{store}->{folder});
    my $responses = $talk->fetch('1:*', 'RFC822.SIZE');
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $self->assert_not_null($responses);
    foreach my $response (values(%$responses)) {
	$expected_message++;
	$expected_storage += $response->{'rfc822.size'};
    }
    $talk->close();

    # check we did restore something
    $self->assert_num_not_equals($expected_storage, 0);
    $self->assert_num_not_equals($expected_message, 0);

    # set quota limits on resources which did not exist in previous cyrus versions;
    # when the mailbox was upgraded, new resources quota usage shall have been
    # computed automatically
    $self->_set_limits(
	storage => 100000,
	message => 50000,
	'x-annotation-storage' => 10000,
    );
    $self->_check_usages(
	storage => int($expected_storage/1024),
	message => $expected_message,
	'x-annotation-storage' => int($expected_annotation_storage/1024),
    );
}

sub test_bz3529
{
    my ($self) = @_;

    xlog "testing annot storage quota when setting annots on multiple";
    xlog "messages in a single STORE command, using quotalegacy backend.";

    # double check that some other part of Cassandane didn't
    # accidentally futz with the expected quota db backend
    my $backend = $self->{instance}->{config}->get('quota_db');
    $self->assert_str_equals('quotalegacy', $backend)
	if defined $backend;	    # the default value is also ok

    $self->_set_quotaroot('user.cassandane');
    my $talk = $self->{store}->get_client();

    xlog "set ourselves a basic limit";
    $self->_set_limits('x-annotation-storage' => 100000);
    $self->_check_usages('x-annotation-storage' => 0);

    xlog "make some messages to hang annotations on";
# 	$self->{store}->set_folder($folder);
    my $uid = 1;
    my %msgs;
    for (1..20)
    {
	$msgs{$uid} = $self->make_message("Message $uid");
	$msgs{$uid}->set_attribute('uid', $uid);
	$uid++;
    }

    my $data = $self->make_random_data(30);
    $talk->store('1:*', 'annotation', ['/comment', ['value.priv', { Literal => $data }]]);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());

    my $expected = ($uid-1) * length($data);
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    # delete annotations
    $talk->store('1:*', 'annotation', ['/comment', ['value.priv', undef]]);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $self->_check_usages('x-annotation-storage' => 0);
}

# Magic: the word 'replication' in the name enables a replica
sub test_replication_storage
{
    my ($self) = @_;

    xlog "testing replication of STORAGE quota";

    my $mastertalk = $self->{master_adminstore}->get_client();
    my $replicatalk = $self->{replica_adminstore}->get_client();

    my $folder = "user.cassandane";
    my @res;

    xlog "checking there are no initial quotas";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('no', $mastertalk->get_last_completion_response());
    $self->assert($mastertalk->get_last_error() =~ m/Quota root does not exist/i);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('no', $replicatalk->get_last_completion_response());
    $self->assert($replicatalk->get_last_error() =~ m/Quota root does not exist/i);

    xlog "set a STORAGE quota on the master";
    $mastertalk->setquota($folder, "(storage 12345)");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals(['STORAGE', 0, 12345], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals(['STORAGE', 0, 12345], \@res);

    xlog "change the STORAGE quota on the master";
    $mastertalk->setquota($folder, "(storage 67890)");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals(['STORAGE', 0, 67890], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals(['STORAGE', 0, 67890], \@res);

    xlog "clear the STORAGE quota on the master";
    $mastertalk->setquota($folder, "()");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals([], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals([], \@res);
}

# Magic: the word 'replication' in the name enables a replica
sub test_replication_message
{
    my ($self) = @_;

    xlog "testing replication of MESSAGE quota";

    my $mastertalk = $self->{master_adminstore}->get_client();
    my $replicatalk = $self->{replica_adminstore}->get_client();

    my $folder = "user.cassandane";
    my @res;

    xlog "checking there are no initial quotas";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('no', $mastertalk->get_last_completion_response());
    $self->assert($mastertalk->get_last_error() =~ m/Quota root does not exist/i);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('no', $replicatalk->get_last_completion_response());
    $self->assert($replicatalk->get_last_error() =~ m/Quota root does not exist/i);

    xlog "set a STORAGE quota on the master";
    $mastertalk->setquota($folder, "(message 12345)");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals(['MESSAGE', 0, 12345], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals(['MESSAGE', 0, 12345], \@res);

    xlog "change the MESSAGE quota on the master";
    $mastertalk->setquota($folder, "(message 67890)");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals(['MESSAGE', 0, 67890], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals(['MESSAGE', 0, 67890], \@res);

    xlog "clear the MESSAGE quota on the master";
    $mastertalk->setquota($folder, "()");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals([], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals([], \@res);
}

# Magic: the word 'replication' in the name enables a replica
sub test_replication_annotstorage
{
    my ($self) = @_;

    xlog "testing replication of X-ANNOTATION-STORAGE quota";

    my $folder = "user.cassandane";
    my $mastertalk = $self->{master_adminstore}->get_client();
    my $replicatalk = $self->{replica_adminstore}->get_client();

    my @res;

    xlog "checking there are no initial quotas";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('no', $mastertalk->get_last_completion_response());
    $self->assert($mastertalk->get_last_error() =~ m/Quota root does not exist/i);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('no', $replicatalk->get_last_completion_response());
    $self->assert($replicatalk->get_last_error() =~ m/Quota root does not exist/i);

    xlog "set an X-ANNOTATION-STORAGE quota on the master";
    $mastertalk->setquota($folder, "(x-annotation-storage 12345)");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals(['X-ANNOTATION-STORAGE', 0, 12345], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals(['X-ANNOTATION-STORAGE', 0, 12345], \@res);

    xlog "change the X-ANNOTATION-STORAGE quota on the master";
    $mastertalk->setquota($folder, "(x-annotation-storage 67890)");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals(['X-ANNOTATION-STORAGE', 0, 67890], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals(['X-ANNOTATION-STORAGE', 0, 67890], \@res);

    xlog "add an annotation to use some quota";
    my $data = $self->make_random_data(13);
    my $msg = $self->make_message("Message A", store => $self->{master_store});
    $mastertalk->store('1', 'annotation', ['/comment', ['value.priv', { Literal => $data }]]);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
## This doesn't work because per-mailbox annots are not
## replicated when sync_client is run in -u mode...sigh
#     $mastertalk->setmetadata($folder, '/private/comment', { Literal => $data });
#     $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    my $used = int(length($data)/1024);

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check the annotation used some quota on the master";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals([
	'X-ANNOTATION-STORAGE', $used, 67890
    ], \@res);

    xlog "check the annotation used some quota on the replica";
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals([
	'X-ANNOTATION-STORAGE', $used, 67890
    ], \@res);

    xlog "clear the X-ANNOTATION-STORAGE quota on the master";
    $mastertalk->setquota($folder, "()");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals([], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals([], \@res);
}


sub XXtest_getset_multiple
{
    my ($self) = @_;

    xlog "testing getting and setting multiple quota resources";

    my $admintalk = $self->{adminstore}->get_client();
    my $folder = "user.cassandane";
    my @res;

    xlog "checking there are no initial quotas";
    @res = $admintalk->getquota($folder);
    $self->assert_str_equals('no', $admintalk->get_last_completion_response());
    $self->assert($admintalk->get_last_error() =~ m/Quota root does not exist/i);

    xlog "set both X-ANNOT-COUNT and X-ANNOT-SIZE quotas";
    $admintalk->setquota($folder, "(x-annot-count 20 x-annot-size 16384)");
    $self->assert_str_equals('ok', $admintalk->get_last_completion_response());

    xlog "get both resources back, and not STORAGE";
    @res = $admintalk->getquota($folder);
    $self->assert_str_equals('ok', $admintalk->get_last_completion_response());
    $self->assert_deep_equals(['X-ANNOT-COUNT', 0, 20, 'X-ANNOT-SIZE', 0, 16384], \@res);

    xlog "set the X-ANNOT-SIZE resource only";
    $admintalk->setquota($folder, "(x-annot-size 32768)");
    $self->assert_str_equals('ok', $admintalk->get_last_completion_response());

    xlog "get new -SIZE only and neither STORAGE nor -COUNT";
    @res = $admintalk->getquota($folder);
    $self->assert_str_equals('ok', $admintalk->get_last_completion_response());
    $self->assert_deep_equals(['X-ANNOT-SIZE', 0, 32768], \@res);

    xlog "set all of -COUNT -SIZE and STORAGE";
    $admintalk->setquota($folder, "(x-annot-count 123 storage 123456 x-annot-size 65536)");
    $self->assert_str_equals('ok', $admintalk->get_last_completion_response());

    xlog "get back all three new values";
    @res = $admintalk->getquota($folder);
    $self->assert_str_equals('ok', $admintalk->get_last_completion_response());
    $self->assert_deep_equals(['STORAGE', 0, 123456, 'X-ANNOT-COUNT', 0, 123, 'X-ANNOT-SIZE', 0, 65536], \@res);

    xlog "clear all quotas";
    $admintalk->setquota($folder, "()");
    $self->assert_str_equals('ok', $admintalk->get_last_completion_response());

    # Note: the RFC does not define what happens if you remove all the
    # quotas from a quotaroot.  Cyrus leaves the quotaroot around until
    # quota -f is run to clean it up.
    xlog "get back an empty set of quotas, but the quota root still exists";
    @res = $admintalk->getquota($folder);
    $self->assert_str_equals('ok', $admintalk->get_last_completion_response());
    $self->assert_deep_equals([], \@res);
}

# Magic: the word 'replication' in the name enables a replica
sub XXtest_replication_multiple
{
    my ($self) = @_;

    xlog "testing replication of multiple quotas";

    my $mastertalk = $self->{master_adminstore}->get_client();
    my $replicatalk = $self->{replica_adminstore}->get_client();

    my $folder = "user.cassandane";
    my @res;

    xlog "checking there are no initial quotas";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('no', $mastertalk->get_last_completion_response());
    $self->assert($mastertalk->get_last_error() =~ m/Quota root does not exist/i);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('no', $replicatalk->get_last_completion_response());
    $self->assert($replicatalk->get_last_error() =~ m/Quota root does not exist/i);

    xlog "set a X-ANNOT-COUNT and X-ANNOT-SIZE quotas on the master";
    $mastertalk->setquota($folder, "(x-annot-count 20 x-annot-size 16384)");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals(['X-ANNOT-COUNT', 0, 20, 'X-ANNOT-SIZE', 0, 16384], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals(['X-ANNOT-COUNT', 0, 20, 'X-ANNOT-SIZE', 0, 16384], \@res);

    xlog "set the X-ANNOT-SIZE quota on the master";
    $mastertalk->setquota($folder, "(x-annot-size 32768)");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals(['X-ANNOT-SIZE', 0, 32768], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals(['X-ANNOT-SIZE', 0, 32768], \@res);

    xlog "clear all the quotas";
    $mastertalk->setquota($folder, "()");
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());

    xlog "run replication";
    $self->run_replication();
    $mastertalk = $self->{master_adminstore}->get_client();
    $replicatalk = $self->{replica_adminstore}->get_client();

    xlog "check that the new quota is at both ends";
    @res = $mastertalk->getquota($folder);
    $self->assert_str_equals('ok', $mastertalk->get_last_completion_response());
    $self->assert_deep_equals([], \@res);
    @res = $replicatalk->getquota($folder);
    $self->assert_str_equals('ok', $replicatalk->get_last_completion_response());
    $self->assert_deep_equals([], \@res);
}

sub test_using_annotstorage_msg_copy_exdel
{
    my ($self) = @_;

    xlog "testing X-ANNOTATION-STORAGE quota usage as messages are COPYd";
    xlog "and original messages are deleted, expunge_mode=delayed version";
    xlog "(BZ3527)";

    my $entry = '/comment';
    my $attrib = 'value.priv';
    my $from_folder = 'INBOX.from';
    my $to_folder = 'INBOX.to';

    xlog "Check the expunge mode is \"delayed\"";
    my $expunge_mode = $self->{instance}->{config}->get('expunge_mode');
    $self->assert_str_equals('delayed', $expunge_mode);

    $self->_set_quotaroot('user.cassandane');
    xlog "set ourselves a basic limit";
    $self->_set_limits('x-annotation-storage' => 100000);
    $self->_check_usages('x-annotation-storage' => 0);
    my $talk = $self->{store}->get_client();

    my $store = $self->{store};
    $store->set_fetch_attributes('uid', "annotation ($entry $attrib)");

    xlog "Create subfolders to copy from and to";
    $talk = $store->get_client();
    $talk->create($from_folder)
	or die "Cannot create mailbox $from_folder: $@";
    $talk->create($to_folder)
	or die "Cannot create mailbox $to_folder: $@";

    $store->set_folder($from_folder);

    xlog "Append some messages and store annotations";
    my %exp;
    my $expected = 0;
    my $uid = 1;
    for (1..20)
    {
	my $data = $self->make_random_data(10);
	my $msg = $self->make_message("Message $uid");
	$msg->set_attribute('uid', $uid);
	$msg->set_annotation($entry, $attrib, $data);
	$exp{$uid} = $msg;
	$talk->store('' . $uid, 'annotation', [$entry, [$attrib, { Literal => $data }]]);
	$expected += length($data);
	$uid++;
    }

    xlog "Check the annotations are there";
    $self->check_messages(\%exp);
    xlog "Check the quota usage is correct";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "COPY the messages";
    $talk = $store->get_client();
    $talk->copy('1:*', $to_folder);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());

    xlog "Messages are now in the destination folder";
    $store->set_folder($to_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Check the quota usage is now doubled";
    $self->_check_usages('x-annotation-storage' => int(2*$expected/1024));

    xlog "Messages are still in the origin folder";
    $store->set_folder($from_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Delete the messages from the origin folder";
    $store->set_folder($from_folder);
    $store->_select();
    $talk = $store->get_client();
    $talk->store('1:*', '+flags', '(\\Deleted)');
    $talk->expunge();

    xlog "Messages are gone from the origin folder";
    $store->set_folder($from_folder);
    $store->_select();
    $self->check_messages({});

    xlog "Messages are still in the destination folder";
    $store->set_folder($to_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Check the quota usage is still doubled";
    $self->_check_usages('x-annotation-storage' => int(2*$expected/1024));

    xlog "Performing delayed expunge";
    $self->_delayed_expunge();

    xlog "Check the quota usage is back to single";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));
}

sub config_using_annotstorage_msg_copy_eximm
{
    my ($self, $conf) = @_;
    xlog "Setting expunge_mode=immediate";
    $conf->set(expunge_mode => 'immediate');
}

sub test_using_annotstorage_msg_copy_eximm
{
    my ($self) = @_;

    xlog "testing X-ANNOTATION-STORAGE quota usage as messages are COPYd";
    xlog "and original messages are deleted, expunge_mode=immediate version";
    xlog "(BZ3527)";

    my $entry = '/comment';
    my $attrib = 'value.priv';
    my $from_folder = 'INBOX.from';
    my $to_folder = 'INBOX.to';

    xlog "Check the expunge mode is \"immediate\"";
    my $expunge_mode = $self->{instance}->{config}->get('expunge_mode');
    $self->assert_str_equals('immediate', $expunge_mode);

    $self->_set_quotaroot('user.cassandane');
    xlog "set ourselves a basic limit";
    $self->_set_limits('x-annotation-storage' => 100000);
    $self->_check_usages('x-annotation-storage' => 0);
    my $talk = $self->{store}->get_client();

    my $store = $self->{store};
    $store->set_fetch_attributes('uid', "annotation ($entry $attrib)");

    xlog "Create subfolders to copy from and to";
    $talk = $store->get_client();
    $talk->create($from_folder)
	or die "Cannot create mailbox $from_folder: $@";
    $talk->create($to_folder)
	or die "Cannot create mailbox $to_folder: $@";

    $store->set_folder($from_folder);

    xlog "Append some messages and store annotations";
    my %exp;
    my $expected = 0;
    my $uid = 1;
    for (1..20)
    {
	my $data = $self->make_random_data(10);
	my $msg = $self->make_message("Message $uid");
	$msg->set_attribute('uid', $uid);
	$msg->set_annotation($entry, $attrib, $data);
	$exp{$uid} = $msg;
	$talk->store('' . $uid, 'annotation', [$entry, [$attrib, { Literal => $data }]]);
	$expected += length($data);
	$uid++;
    }

    xlog "Check the annotations are there";
    $self->check_messages(\%exp);
    xlog "Check the quota usage is correct";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "COPY the messages";
    $talk = $store->get_client();
    $talk->copy('1:*', $to_folder);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());

    xlog "Messages are now in the destination folder";
    $store->set_folder($to_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Check the quota usage is now doubled";
    $self->_check_usages('x-annotation-storage' => int(2*$expected/1024));

    xlog "Messages are still in the origin folder";
    $store->set_folder($from_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Delete the messages from the origin folder";
    $store->set_folder($from_folder);
    $store->_select();
    $talk = $store->get_client();
    $talk->store('1:*', '+flags', '(\\Deleted)');
    $talk->expunge();

    xlog "Messages are gone from the origin folder";
    $store->set_folder($from_folder);
    $store->_select();
    $self->check_messages({});

    xlog "Messages are still in the destination folder";
    $store->set_folder($to_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Check the quota usage is back to single";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));
}

sub test_using_annotstorage_msg_copy_dedel
{
    my ($self) = @_;

    xlog "testing X-ANNOTATION-STORAGE quota usage as messages are COPYd";
    xlog "and original folder is deleted, delete_mode=delayed version";
    xlog "(BZ3527)";

    my $entry = '/comment';
    my $attrib = 'value.priv';
    my $from_folder = 'INBOX.from';
    my $to_folder = 'INBOX.to';

    xlog "Check the delete mode is \"delayed\"";
    my $delete_mode = $self->{instance}->{config}->get('delete_mode');
    $self->assert_str_equals('delayed', $delete_mode);

    $self->_set_quotaroot('user.cassandane');
    xlog "set ourselves a basic limit";
    $self->_set_limits('x-annotation-storage' => 100000);
    $self->_check_usages('x-annotation-storage' => 0);
    my $talk = $self->{store}->get_client();

    my $store = $self->{store};
    $store->set_fetch_attributes('uid', "annotation ($entry $attrib)");

    xlog "Create subfolders to copy from and to";
    $talk = $store->get_client();
    $talk->create($from_folder)
	or die "Cannot create mailbox $from_folder: $@";
    $talk->create($to_folder)
	or die "Cannot create mailbox $to_folder: $@";

    $store->set_folder($from_folder);

    xlog "Append some messages and store annotations";
    my %exp;
    my $expected = 0;
    my $uid = 1;
    for (1..20)
    {
	my $data = $self->make_random_data(10);
	my $msg = $self->make_message("Message $uid");
	$msg->set_attribute('uid', $uid);
	$msg->set_annotation($entry, $attrib, $data);
	$exp{$uid} = $msg;
	$talk->store('' . $uid, 'annotation', [$entry, [$attrib, { Literal => $data }]]);
	$expected += length($data);
	$uid++;
    }

    xlog "Check the annotations are there";
    $self->check_messages(\%exp);
    xlog "Check the quota usage is correct";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "COPY the messages";
    $talk = $store->get_client();
    $talk->copy('1:*', $to_folder);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());

    xlog "Messages are now in the destination folder";
    $store->set_folder($to_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Check the quota usage is now doubled";
    $self->_check_usages('x-annotation-storage' => int(2*$expected/1024));

    xlog "Messages are still in the origin folder";
    $store->set_folder($from_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Delete the origin folder";
    $talk = $store->get_client();
    $talk->unselect();
    $talk->delete($from_folder)
	or die "Cannot delete folder $from_folder: $@";

    xlog "Messages are still in the destination folder";
    $store->set_folder($to_folder);
    $store->_select();
    $self->check_messages(\%exp);

    # Note that, unlike with delayed expunge, with delayed delete the
    # annotations are deleted immediately and so the negative delta to
    # quota is applied immediately.  Whether this is sensible is a
    # different question.

    xlog "Check the quota usage is back to single";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "Performing delayed expunge";
    $self->_delayed_expunge();

    xlog "Check the quota usage is still back to single";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));
}

sub config_using_annotstorage_msg_copy_deimm
{
    my ($self, $conf) = @_;
    xlog "Setting delete_mode=immediate";
    $conf->set(delete_mode => 'immediate');
}

sub test_using_annotstorage_msg_copy_deimm
{
    my ($self) = @_;

    xlog "testing X-ANNOTATION-STORAGE quota usage as messages are COPYd";
    xlog "and original folder is deleted, delete_mode=immediate version";
    xlog "(BZ3527)";

    my $entry = '/comment';
    my $attrib = 'value.priv';
    my $from_folder = 'INBOX.from';
    my $to_folder = 'INBOX.to';

    xlog "Check the delete mode is \"immediate\"";
    my $delete_mode = $self->{instance}->{config}->get('delete_mode');
    $self->assert_str_equals('immediate', $delete_mode);

    $self->_set_quotaroot('user.cassandane');
    xlog "set ourselves a basic limit";
    $self->_set_limits('x-annotation-storage' => 100000);
    $self->_check_usages('x-annotation-storage' => 0);
    my $talk = $self->{store}->get_client();

    my $store = $self->{store};
    $store->set_fetch_attributes('uid', "annotation ($entry $attrib)");

    xlog "Create subfolders to copy from and to";
    $talk = $store->get_client();
    $talk->create($from_folder)
	or die "Cannot create mailbox $from_folder: $@";
    $talk->create($to_folder)
	or die "Cannot create mailbox $to_folder: $@";

    $store->set_folder($from_folder);

    xlog "Append some messages and store annotations";
    my %exp;
    my $expected = 0;
    my $uid = 1;
    for (1..20)
    {
	my $data = $self->make_random_data(10);
	my $msg = $self->make_message("Message $uid");
	$msg->set_attribute('uid', $uid);
	$msg->set_annotation($entry, $attrib, $data);
	$exp{$uid} = $msg;
	$talk->store('' . $uid, 'annotation', [$entry, [$attrib, { Literal => $data }]]);
	$expected += length($data);
	$uid++;
    }

    xlog "Check the annotations are there";
    $self->check_messages(\%exp);
    xlog "Check the quota usage is correct";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));

    xlog "COPY the messages";
    $talk = $store->get_client();
    $talk->copy('1:*', $to_folder);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());

    xlog "Messages are now in the destination folder";
    $store->set_folder($to_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Check the quota usage is now doubled";
    $self->_check_usages('x-annotation-storage' => int(2*$expected/1024));

    xlog "Messages are still in the origin folder";
    $store->set_folder($from_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Delete the origin folder";
    $talk = $store->get_client();
    $talk->unselect();
    $talk->delete($from_folder)
	or die "Cannot delete folder $from_folder: $@";

    xlog "Messages are still in the destination folder";
    $store->set_folder($to_folder);
    $store->_select();
    $self->check_messages(\%exp);

    xlog "Check the quota usage is back to single";
    $self->_check_usages('x-annotation-storage' => int($expected/1024));
}

sub test_reconstruct
{
    my ($self) = @_;

    xlog "test resources usage calculated when reconstructing an index";

    $self->_set_quotaroot('user.cassandane');
    my $folder = 'INBOX';
    my $fentry = '/private/comment';
    my $mentry1 = '/comment';
    my $mentry2 = '/altsubject';
    my $mattrib = 'value.priv';

    my $store = $self->{store};
    $store->set_fetch_attributes('uid',
				 "annotation ($mentry1 $mattrib)",
				 "annotation ($mentry2 $mattrib)");
    my $talk = $store->get_client();
    my $admintalk = $self->{adminstore}->get_client();

    xlog "set ourselves a basic limit";
    $self->_set_limits(
	storage => 100000,
	message => 50000,
	'x-annotation-storage' => 100000,
    );
    $self->_check_usages(
	storage => 0,
	message => 0,
	'x-annotation-storage' => 0,
    );
    my $expected_annotation_storage = 0;
    my $expected_storage = 0;
    my $expected_message = 0;

    xlog "store annotations";
    my $data = $self->make_random_data(10);
    $expected_annotation_storage += length($data);
    $talk->setmetadata($folder, $fentry, { Literal => $data });
    $self->assert_str_equals('ok', $talk->get_last_completion_response());

    xlog "add some messages";
    my $uid = 1;
    my %exp;
    for (1..10)
    {
	my $msg = $self->make_message("Message $_",
				      extra_lines => 10 + rand(5000));
	$exp{$uid} = $msg;
	my $data1 = $self->make_random_data(7);
	my $data2 = $self->make_random_data(3);
	$msg->set_attribute('uid', $uid);
	$msg->set_annotation($mentry1, $mattrib, $data1);
	$msg->set_annotation($mentry2, $mattrib, $data2);
	$talk->store('' . $uid, 'annotation',
		    [$mentry1, [$mattrib, { Literal => $data1 }],
		     $mentry2, [$mattrib, { Literal => $data2 }]]);
	$self->assert_str_equals('ok', $talk->get_last_completion_response());
	$expected_annotation_storage += (length($data1) + length($data2));
	$expected_storage += length($msg->as_string());
	$expected_message++;
	$uid++;
    }

    xlog "Check the messages are all there";
    $self->check_messages(\%exp);

    xlog "Check the mailbox annotation is still there";
    my $res = $talk->getmetadata($folder, $fentry);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $self->assert_deep_equals({
	$folder => { $fentry => $data }
    }, $res);

    xlog "Check the quota usage is as expected";
    $self->_check_usages(
	storage => int($expected_storage/1024),
	message => $expected_message,
	'x-annotation-storage' => int($expected_annotation_storage/1024),
    );

    $self->{store}->disconnect();
    $self->{adminstore}->disconnect();
    $talk = undef;
    $admintalk = undef;

    xlog "Moving the cyrus.index file out of the way";
    my $mbdir = $self->{instance}->{basedir} . '/data/user/cassandane';
    my $cyrus_index = "$mbdir/cyrus.index";
    $self->assert(( -f $cyrus_index ));
    rename($cyrus_index, $cyrus_index . '.NOT')
	or die "Cannot rename $cyrus_index: $!";

    xlog "Running reconstruct";
    $self->{instance}->run_command({ cyrus => 1 },
				   'reconstruct', 'user.cassandane');
    xlog "Running quota -f";
    $self->{instance}->run_command({ cyrus => 1 },
				   'quota', '-f', "user.cassandane");

    $talk = $store->get_client();

    xlog "Check the messages are still all there";
    $self->check_messages(\%exp);

    xlog "Check the mailbox annotation is still there";
    $res = $talk->getmetadata($folder, $fentry);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $self->assert_deep_equals({
	$folder => { $fentry => $data }
    }, $res);

    xlog "Check the quota usage is still as expected";
    $self->_check_usages(
	storage => int($expected_storage/1024),
	message => $expected_message,
	'x-annotation-storage' => int($expected_annotation_storage/1024),
    );
}

sub test_reconstruct_orphans
{
    my ($self) = @_;

    xlog "test resources usage calculated when reconstructing an index";
    xlog "with messages disappearing, resulting in orphan annotations";

    $self->_set_quotaroot('user.cassandane');
    my $folder = 'INBOX';
    my $fentry = '/private/comment';
    my $mentry1 = '/comment';
    my $mentry2 = '/altsubject';
    my $mattrib = 'value.priv';

    my $store = $self->{store};
    $store->set_fetch_attributes('uid',
				 "annotation ($mentry1 $mattrib)",
				 "annotation ($mentry2 $mattrib)");
    my $talk = $store->get_client();
    my $admintalk = $self->{adminstore}->get_client();

    xlog "set ourselves a basic limit";
    $self->_set_limits(
	storage => 100000,
	message => 50000,
	'x-annotation-storage' => 100000,
    );
    $self->_check_usages(
	storage => 0,
	message => 0,
	'x-annotation-storage' => 0,
    );
    my $expected_annotation_storage = 0;
    my $expected_storage = 0;
    my $expected_message = 0;

    xlog "store annotations";
    my $data = $self->make_random_data(10);
    $expected_annotation_storage += length($data);
    $talk->setmetadata($folder, $fentry, { Literal => $data });
    $self->assert_str_equals('ok', $talk->get_last_completion_response());

    xlog "add some messages";
    my $uid = 1;
    my %exp;
    for (1..10)
    {
	my $msg = $self->make_message("Message $_",
				      extra_lines => 10 + rand(5000));
	$exp{$uid} = $msg;
	my $data1 = $self->make_random_data(7);
	my $data2 = $self->make_random_data(3);
	$msg->set_attribute('uid', $uid);
	$msg->set_annotation($mentry1, $mattrib, $data1);
	$msg->set_annotation($mentry2, $mattrib, $data2);
	$talk->store('' . $uid, 'annotation',
		    [$mentry1, [$mattrib, { Literal => $data1 }],
		     $mentry2, [$mattrib, { Literal => $data2 }]]);
	$self->assert_str_equals('ok', $talk->get_last_completion_response());
	$expected_annotation_storage += (length($data1) + length($data2));
	$expected_storage += length($msg->as_string());
	$expected_message++;
	$uid++;
    }

    xlog "Check the messages are all there";
    $self->check_messages(\%exp);

    xlog "Check the mailbox annotation is still there";
    my $res = $talk->getmetadata($folder, $fentry);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $self->assert_deep_equals({
	$folder => { $fentry => $data }
    }, $res);

    xlog "Check the quota usage is as expected";
    $self->_check_usages(
	storage => int($expected_storage/1024),
	message => $expected_message,
	'x-annotation-storage' => int($expected_annotation_storage/1024),
    );

    $self->{store}->disconnect();
    $self->{adminstore}->disconnect();
    $talk = undef;
    $admintalk = undef;

    xlog "Moving the cyrus.index file out of the way";
    my $mbdir = $self->{instance}->{basedir} . '/data/user/cassandane';
    my $cyrus_index = "$mbdir/cyrus.index";
    $self->assert(( -f $cyrus_index ));
    rename($cyrus_index, $cyrus_index . '.NOT')
	or die "Cannot rename $cyrus_index: $!";

    xlog "Delete a couple of messages";
    foreach $uid (2, 7)
    {
	xlog "Deleting uid $uid";
	unlink("$mbdir/$uid.");

	my $msg = delete $exp{$uid};
	my $data1 = $msg->get_annotation($mentry1, $mattrib);
	my $data2 = $msg->get_annotation($mentry2, $mattrib);

	$expected_annotation_storage -= (length($data1) + length($data2));
	$expected_storage -= length($msg->as_string());
	$expected_message--;
    }

    xlog "Running reconstruct";
    $self->{instance}->run_command({ cyrus => 1 },
				   'reconstruct', 'user.cassandane');
    xlog "Running quota -f";
    $self->{instance}->run_command({ cyrus => 1 },
				   'quota', '-f', "user.cassandane");

    $talk = $store->get_client();

    xlog "Check the messages are still all there";
    $self->check_messages(\%exp);

    xlog "Check the mailbox annotation is still there";
    $res = $talk->getmetadata($folder, $fentry);
    $self->assert_str_equals('ok', $talk->get_last_completion_response());
    $self->assert_deep_equals({
	$folder => { $fentry => $data }
    }, $res);

    xlog "Check the quota usage is still as expected";
    $self->_check_usages(
	storage => int($expected_storage/1024),
	message => $expected_message,
	'x-annotation-storage' => int($expected_annotation_storage/1024),
    );
}

1;
