#!/usr/bin/perl
#

use strict;
use warnings;
package Cassandane::Cyrus::Events;
use base qw(Cassandane::Cyrus::TestCase);
use File::Path qw(mkpath);
use DateTime;
use Cassandane::Util::Log;
use IO::Socket;
use Net::ManageSieve;
use JSON;
use IO::Select;

my $notisock = undef;
my $notiselc = undef;

sub new
{
    my $class = shift;
    return $class->SUPER::new({
            services => ['imap'],
            adminstore => 1
        },
        @_
    );

}

sub set_up
{
    my ($self) = @_;
    $self->SUPER::set_up();
    my $sock_path = $self->{instance}->{basedir} . '/conf/socket/notify';
    $notisock= IO::Socket::UNIX->new(
        Type   => SOCK_DGRAM,
        Local  => $sock_path,
        Blocking => 0,
    )
        or die("Can't create notify socket: $!\n");
    $notiselc = IO::Select->new( $notisock )
        or die("Can't create notify select: $!\n");
}

sub tear_down
{
    my ($self) = @_;
    $self->SUPER::tear_down();
    $notisock->close();
}

sub recv_event_notification
{
    my ($self, $timeout) = @_;

    if(! defined $timeout) { $timeout = 1; }

    my $array; 
    my $hash = {};
    my $data = "[";
 
    my $cpt = 0;
    my $buf;

    while ($notiselc->can_read($timeout) && defined ($notisock->recv($buf,8192,0))){
        $buf =~ s/\x00//g;
        $buf =~ s/logEVENT\d+(.*)/$1/;
        $data .= $buf; 
    }

    $data =~ s/\}\{/},{/g;
    $data = $data . "]";
    xlog "DATA:$data";
    my $json = JSON->new;
    $array = $json->decode($data);

    return $array;
}

sub _split_folder
{
    my ($self, $folder) = @_;

    my @folderEl = split(/\./, $folder);
    shift(@folderEl);
    my $user = shift(@folderEl);
    my $subFolder = join('.', @folderEl);
    my $userFolder = 'INBOX' . ($subFolder ? '.' . $subFolder : '');

    return ($user, $subFolder, $userFolder);
}

sub get_message_path
{
    my ($self, $message) = @_;

    my $path = $self->{instance}{basedir} . '/data/user/' . $message->{user};
    my $folder = $message->{subFolder};
    if (defined($folder) && length($folder)) {
	$path .= '/' . join('/', split(/\./, $folder));
    }
    $path .= '/' . $message->{uid} . '.';

    return $path;
}

sub create_folder
{
    my ($self, $talk, $folder, $label) = @_;

    return if ($talk->{CurrentFolder} eq $folder);

    my $exists = 0;
    my @res = $talk->list('', $folder);
    if (scalar(@res) == 1) {
	$exists = 1;
	foreach my $flag (@{$res[0]->[0]}) {
	    if (lc($flag) eq '\\noselect') {
		$exists = 0;
		last;
	    }
	}
    }
    return if ($exists);

    $talk->create($folder)
	|| die($label . ": cannot create folder[$folder]; $@");
}

sub _save_message
{
    my ($self, $msg, $store) = @_;

    $store ||= $self->{store};

    $store->write_begin();
    $store->write_message($msg);
    $store->write_end();
}

sub make_message
{
    my ($self, $subject, %attrs) = @_;

    my $store = $attrs{store};	# may be undef
    delete $attrs{store};

    my $msg = $self->{gen}->generate(subject => $subject, %attrs);
    $self->_save_message($msg, $store);

    return $msg;
}

sub set_message_folder
{
    my ($self, $message, $folder) = @_;

    my $unix = $self->{instance}->{config}->get_bool('unixhierarchysep');
    my $db = $folder;
    if ($unix) {
	$db =~ s/\./\^/g;
	$db =~ s/\//\./g;
    }

    my ($user, $subFolder, $userFolder) = $self->_split_folder($folder);
    $message->{user} = $user;
    $message->{subFolder} = $subFolder;
    $message->{folder} = {
	user => $userFolder,
	admin => $folder,
	db => $db
    };
}

sub prepare_message
{
    my ($self, $folder, $label, $check, %attrs) = @_;

    my $message;

    my ($user, $subFolder, $userFolder) = $self->_split_folder($folder);

    xlog "set folder:$userFolder";

    my $store = $attrs{store};
    $store = $self->{store}
	if (!defined($store));

    $store->set_folder($userFolder);
    my $talk = $store->get_client();

    # create folder if necessary
    $self->create_folder($talk, $userFolder, $label);

    if (!defined($attrs{id}) || !defined($attrs{uid})) {
	my $res = $talk->status($userFolder, '(MESSAGES UIDNEXT)');
	$self->assert(defined($res),
	    $label . ": STATUS failed on folder[$folder]");

	$attrs{id} = $res->{messages} + 1
	    unless defined($attrs{id});
	$attrs{uid} = $res->{uidnext}
	    unless defined($attrs{uid});
    }

    $attrs{user} = $user
	unless defined($attrs{user});
    my $msg = $self->make_message($label, 'Message ' . $attrs{uid},
	%attrs);
    
    $message = {
	id => $attrs{id},
	uid => $attrs{uid},
	label => $label,
	msg => $msg
    };
    $self->set_message_folder($message, $folder);

    $message->{label} = $label;

    if ($check) {
        $self->check_message($message);
    }
    return $message;
}

sub check_message
{
    my ($self, $message) = @_;
    $self->assert(-e $self->get_message_path($message),
            $message->{label} . ': local file does not exist');
}

sub copy_message
{
    my ($self, $id, $srcFolder, $dstFolder) = @_;
    my $store = $self->{store};

    my $talk = $store->get_client();
    $talk->select('INBOX');
    $talk->copy($id,'INBOX');
    $talk->close();
}

sub set_flag
{
    my ($self, $id, $flag, $talk) = @_;
    my $must_close = 0;
    if(! defined $talk) {
        $talk = $self->{store}->get_client();
        $must_close = 1;
    }
    $talk->select('INBOX');
    $talk->store([$id], '+flags', $flag);
    if($must_close) {
        $talk->close();
    }
}

sub clear_flag
{
    my ($self, $id, $flag, $talk) = @_;
    my $must_close = 0;
    if(! defined $talk) {
        $talk = $self->{store}->get_client();
        $must_close = 1;
    }
    $talk->select('INBOX');
    $talk->store([$id], '-flags', $flag);
    if($must_close) {
        $talk->close();
    }
}

sub fetch_message
{
    my ($self, $id, $fetcharg) = @_;
    my $talk = $self->{store}->get_client();
    $talk->select('INBOX');
    $talk->fetch([$id], $fetcharg);
    $talk->close(); 
}

sub expunge_inbox
{
    my ($self, $talk) = @_;
    my $must_close = 0;
    if(! defined $talk) {
        $talk = $self->{store}->get_client();
        $must_close = 1;
    }
    $talk->select('INBOX');
    $talk->expunge();
    $self->assert_str_equals('ok', $talk->get_last_completion_response()); 
    if($must_close) {
        $talk->close();
    }
}

sub test_events_message
{
    my ($self) = @_;

    # MessageNew
    xlog "generating message A";
    my $message = $self->prepare_message("user.cassandane","Message A", 1);
    my $events = $self->recv_event_notification();
    $self->assert_matches(qr/imap:\/\/cassandane\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+\/;UID=1/, $$events[0]->{uri});
    $self->assert_str_equals("MessageAppend", $$events[0]->{event});

    xlog "copy message A into B";
    my $msgcpy = $self->copy_message(1);
    $events = $self->recv_event_notification();
    $self->assert_matches(qr/imap:\/\/cassandane\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+/, $$events[0]->{oldMailboxID});
    $self->assert_str_equals("vnd.cmu.MessageCopy", $$events[0]->{event});
    $self->assert_str_equals("1", $$events[0]->{"vnd.cmu.oldUidset"});
    $self->assert_str_equals("2", $$events[0]->{uidset});
    $self->assert_matches(qr/imap:\/\/cassandane\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+/, $$events[0]->{uri});

    xlog "setting flag answered on message A";
    $self->set_flag(1,'(\\answered)');
    $events = $self->recv_event_notification();
    $self->assert_str_equals("FlagsSet", $$events[0]->{event});
    $self->assert_str_equals("1", $$events[0]->{uidset});
    $self->assert_str_equals("\\Answered", $$events[0]->{flagNames});
    $self->assert_matches(qr/imap:\/\/cassandane\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+/, $$events[0]->{uri});

    xlog "clearing flag answered on message A";
    $self->clear_flag(1,'(\\answered)');
    $events = $self->recv_event_notification();
    $self->assert_str_equals("FlagsClear", $$events[0]->{event});
    $self->assert_str_equals("1", $$events[0]->{uidset});
    $self->assert_str_equals("\\Answered", $$events[0]->{flagNames});
    $self->assert_matches(qr/imap:\/\/cassandane\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+/, $$events[0]->{uri});

    xlog "fetch message A";
    $self->fetch_message(1,"body[]");
    $events = $self->recv_event_notification();
    $self->assert_str_equals("MessageRead", $$events[0]->{event});
    $self->assert_str_equals("1", $$events[0]->{uidset});
    $self->assert_matches(qr/imap:\/\/cassandane\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+/, $$events[0]->{uri});

    xlog "deleting message A & B";
    my $talk = $self->{store}->get_client();
    $self->set_flag(1,'(\\deleted)', $talk);
    $self->set_flag(2,'(\\deleted)', $talk);
    $self->expunge_inbox($talk);
    $talk->close();
    $events = $self->recv_event_notification();
    $self->assert_str_equals("MessageTrash", $$events[0]->{event});
    $self->assert_str_equals("1", $$events[0]->{uidset});
    $self->assert_matches(qr/imap:\/\/cassandane\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+/, $$events[0]->{uri});
    $self->assert_str_equals("MessageTrash", $$events[1]->{event});
    $self->assert_str_equals("2", $$events[1]->{uidset});
    $self->assert_matches(qr/imap:\/\/cassandane\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+/, $$events[1]->{uri});
    $self->assert_str_equals("MessageExpunge", $$events[2]->{event});
    $self->assert_str_equals("1:2", $$events[2]->{uidset});
    $self->assert_matches(qr/imap:\/\/cassandane\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+/, $$events[2]->{uri});

}

sub test_events_mailbox
{
    my ($self) = @_;

    my $adminstore = $self->{instance}->get_service('imap')->create_store(username => 'admin');
    my $adminclient = $adminstore->get_client();

    xlog "create a mailbox";
    $adminclient->create("user.cassandane2") or die("Can't create user.cassandane2: $!\n");
    my $events = $self->recv_event_notification();
    $self->assert_str_equals("MailboxCreate", $$events[0]->{event});
    $self->assert_matches(qr/imap:\/\/cassandane2\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+/, $$events[0]->{uri});
    $self->assert_matches(qr/imap:\/\/cassandane2\@localhost\.localdomain\/INBOX;UIDVALIDITY=\d+/, $$events[0]->{mailboxID});

    xlog "set the acl on the new mailbox";
    $adminclient->setacl("user.cassandane2", admin => 'lrswipkxtecda');
    xlog "create a subfolder";
    $adminclient->create("user.cassandane2.folder1") or die("Can't create user.cassandane2.folder1: $!\n");
    $events = $self->recv_event_notification();
    $self->assert_str_equals("MailboxCreate", $$events[0]->{event});
    $self->assert_matches(qr/imap:\/\/cassandane2\@localhost\.localdomain\/INBOX\.folder1;UIDVALIDITY=\d+/, $$events[0]->{uri});
    $self->assert_matches(qr/imap:\/\/cassandane2\@localhost\.localdomain\/INBOX\.folder1;UIDVALIDITY=\d+/, $$events[0]->{mailboxID});

    xlog "rename the subfolder";
    $adminclient->rename("user.cassandane2.folder1","user.cassandane2.folder2") or die("Can't rename user.cassandane2.folder1: $!\n");
    $events = $self->recv_event_notification();
    $self->assert_str_equals("MailboxRename", $$events[0]->{event});
    $self->assert_matches(qr/imap:\/\/cassandane2\@localhost\.localdomain\/INBOX\.folder2;UIDVALIDITY=\d+/, $$events[0]->{uri});
    $self->assert_matches(qr/imap:\/\/cassandane2\@localhost\.localdomain\/INBOX\.folder2;UIDVALIDITY=\d+/, $$events[0]->{mailboxID});
    $self->assert_matches(qr/imap:\/\/cassandane2\@localhost\.localdomain\/INBOX\.folder1;UIDVALIDITY=\d+/, $$events[0]->{oldMailboxID});

    xlog "delete the subfolder";
    $adminclient->delete("user.cassandane2.folder2") or die("Can't delete user.cassandane.folder2: $!\n");
    $events = $self->recv_event_notification();
    $self->assert_str_equals("MailboxDelete", $$events[0]->{event});
    $self->assert_matches(qr/imap:\/\/cassandane2\@localhost\.localdomain\/INBOX\.folder2;UIDVALIDITY=\d+/, $$events[0]->{uri});
    $self->assert_matches(qr/imap:\/\/cassandane2\@localhost\.localdomain\/INBOX\.folder2;UIDVALIDITY=\d+/, $$events[0]->{mailboxID});

}

1;



