from unittest import skip

from .dispersytestclass import DispersyTestFunc
from ..util import blocking_call_on_reactor_thread


class TestCancel(DispersyTestFunc):

    def test_self_cancel_own(self):
        """
        NODE generates a few messages and then cancels them.

        This is always allowed.  In fact, no check is made since only externally received packets
        will be checked.
        """
        node, = self.create_nodes(1)

        # create messages
        messages = [node.create_full_sync_text("Should cancel #%d" % i, i + 10) for i in xrange(10)]
        node.give_messages(messages, node)

        # check that they are in the database and are NOT undone
        node.assert_is_stored(messages=messages)

        # undo all messages
        cancels = [node.create_undo_own(message, i + 100) for i, message in enumerate(messages)]

        node.give_messages(cancels, node)

        # check that they are in the database and ARE undone
        node.assert_is_canceled(messages=messages)
        node.assert_is_stored(messages=cancels)

    def test_node_cancel_other(self):
        """
        MM gives NODE permission to cancel, OTHER generates a few messages and then NODE cancels
        them.
        """
        node, other = self.create_nodes(2)
        other.send_identity(node)

        # MM grants undo permission to NODE
        authorize = self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"full-sync-text"),
                                                u"cancel")], self._mm.claim_global_time())
        node.give_message(authorize, self._mm)
        other.give_message(authorize, self._mm)

        # OTHER creates messages
        messages = [other.create_full_sync_text("Should cancel #%d" % i, i + 10) for i in xrange(10)]
        node.give_messages(messages, other)

        # check that they are in the database and are NOT undone
        node.assert_is_stored(messages=messages)

        # NODE cancels all messages
        cancels = [node.create_cancel_other(message, message.distribution.global_time + 100)
                   for i, message in enumerate(messages)]
        node.give_messages(cancels, node)

        # check that they are in the database and ARE undone
        node.assert_is_canceled(messages=messages)
        node.assert_is_stored(messages=cancels)

    def test_self_attempt_cancel_twice(self):
        """
        NODE generated a message and then cancels it twice. The dispersy core should ensure that
        that the second cancel is refused and the first cancel message should be returned instead.
        """
        node, = self.create_nodes(1)

        # create message
        message = node.create_full_sync_text("Should cancel @%d" % 1, 1)
        node.give_message(message, node)

        # cancel twice
        @blocking_call_on_reactor_thread
        def create_cancel():
            return node._community.create_cancel(message)

        cancel1 = node.call(create_cancel)
        self.assertIsNotNone(cancel1.packet)

        self.assertRaises(RuntimeError, create_cancel)

    @skip("Will change this test to a meaningful one")
    def test_node_resolve_cancel_twice(self):
        """
        Make sure that in the event of receiving two cancel messages from the same member, both will be stored,
        and in case of receiving a lower one, that we will send the higher one back to the sender.

        MM gives NODE permission to cancel, NODE generates a message and then cancels it twice.
        Both messages should be kept and the lowest one should be canceled.

        """
        node, other = self.create_nodes(2)
        node.send_identity(other)

        # MM grants cancel permission to NODE
        authorize = self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"full-sync-text"),
                                                u"cancel")], self._mm.claim_global_time())
        node.give_message(authorize, self._mm)
        other.give_message(authorize, self._mm)

        # create message
        message = node.create_full_sync_text("Should cancel @%d" % 10, 10)

        # create cancels
        cancel1 = node.create_cancel_own(message, 11)
        cancel2 = node.create_cancel_own(message, 12)
        low_message, high_message = sorted([cancel1, cancel2], key=lambda msg: msg.packet)
        self._logger.debug("!!! give message")
        other.give_message(message, node)
        self._logger.debug("!!! give low_message")
        other.give_message(low_message, node)
        self._logger.debug("!!! give high_message")
        other.give_message(high_message, node)
        # OTHER should send the first message back when receiving
        # the second one (its "higher" than the one just received)
        cancel_packets = list()

        for candidate, b in node.receive_packets():
            self._logger.debug(candidate)
            self._logger.debug(type(b))
            self._logger.debug("%d", len(b))
            self._logger.debug("before %d", len(cancel_packets))
            cancel_packets.append(b)
            self._logger.debug("packets amount: %d", len(cancel_packets))
            self._logger.debug("first cancel %d", len(cancel_packets[0]))
            self._logger.debug("%d", len(b))

            for x in cancel_packets:
                self._logger.debug("loop%d", len(x))

        def fetch_all_messages():
            for row in list(other._dispersy.database.execute(u"SELECT * FROM sync")):
                self._logger.debug("_______ %s", row)
        other.call(fetch_all_messages)

        self._logger.debug("%d", len(low_message.packet))

        self.assertEqual(len(cancel_packets), len([low_message.packet]))

        # NODE should have both messages on the database and the lowest one should be undone by the highest.
        messages = other.fetch_messages((u"dispersy-cancel-own",))
        self.assertEquals(len(messages), 2)
        other.assert_is_done(low_message)
        other.assert_is_canceled(high_message)
        other.assert_is_canceled(high_message, canceled_by=low_message)
        other.assert_is_canceled(message, canceled_by=low_message)

    def test_missing_message(self):
        """
        NODE generates a few messages without sending them to OTHER. Following, NODE cancels the
        messages and sends the cancel messages to OTHER. OTHER must now use a dispersy-missing-message
        to request the messages that are about to be canceled. The messages need to be processed and
        subsequently canceled.
        """
        node, other = self.create_nodes(2)
        node.send_identity(other)

        # create messages
        messages = [node.create_full_sync_text("Should cancel @%d" % i, i + 10) for i in xrange(10)]

        # cancel all messages
        cancels = [node.create_cancel_own(message, message.distribution.global_time + 100)
                   for i, message in enumerate(messages)]

        # send cancels to OTHER
        other.give_messages(cancels, node)

        # receive the dispersy-missing-message messages
        global_times = [message.distribution.global_time for message in messages]
        global_time_requests = []

        for _, message in node.receive_messages(names=[u"dispersy-missing-message"]):
            self.assertEqual(message.payload.member.public_key, node.my_member.public_key)
            global_time_requests.extend(message.payload.global_times)

        self.assertEqual(sorted(global_times), sorted(global_time_requests))

        # give all 'delayed' messages
        other.give_messages(messages, node)

        # check that they are in the database and ARE undone
        other.assert_is_canceled(messages=messages)
        other.assert_is_stored(messages=cancels)

    def test_revoke_causing_cancel(self):
        """
        SELF gives NODE permission to cancel, OTHER created a message, NODE cancels the message, SELF
        revokes the cancel permission AFTER the message was canceled -> the message is not re-done.
        """
        node, other = self.create_nodes(2)
        node.send_identity(other)

        # MM grants cancel permission to NODE
        authorize = self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"full-sync-text"),
                                                u"cancel")], self._mm.claim_global_time())
        node.give_message(authorize, self._mm)
        other.give_message(authorize, self._mm)

        # OTHER creates a message
        message = other.create_full_sync_text("will be canceled", 42)
        other.give_message(message, other)
        other.assert_is_stored(message)

        # NODE cancels the message
        cancel = node.create_cancel_other(message, message.distribution.global_time + 1)
        other.give_message(cancel, node)
        other.assert_is_canceled(message)
        other.assert_is_stored(cancel)

        # SELF revoke cancel permission from NODE
        revoke = self._mm.create_revoke([(node.my_member, self._community.get_meta_message(u"full-sync-text"),
                                          u"cancel")])
        other.give_message(revoke, self._mm)
        other.assert_is_canceled(message)
