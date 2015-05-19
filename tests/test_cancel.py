from time import sleep

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

        # check that they are in the database and are NOT cancelled
        node.assert_is_stored(messages=messages)

        # cancel all messages
        cancels = [node.create_cancel_own(message, i + 100) for i, message in enumerate(messages)]

        node.give_messages(cancels, node)

        # check that they are in the database and ARE cancelled
        node.assert_is_cancelled(messages=messages)
        node.assert_is_stored(messages=cancels)

    def test_node_cancel_other(self):
        """
        MM gives NODE permission to cancel, OTHER generates a few messages and then NODE cancels
        them.
        """
        node, other = self.create_nodes(2)
        other.send_identity(node)

        # MM grants cancel permission to NODE
        authorize = self._mm.create_authorize([(node.my_member, self._community.get_meta_message(u"full-sync-text"),
                                                u"cancel")], self._mm.claim_global_time())
        node.give_message(authorize, self._mm)
        other.give_message(authorize, self._mm)

        # OTHER creates messages
        messages = [other.create_full_sync_text("Should cancel #%d" % i, i + 10) for i in xrange(10)]
        node.give_messages(messages, other)

        # check that they are in the database and are NOT cancelled
        node.assert_is_stored(messages=messages)

        # NODE cancels all messages
        cancels = [node.create_cancel_other(message, message.distribution.global_time + 100)
                   for i, message in enumerate(messages)]
        node.give_messages(cancels, node)

        # check that they are in the database and ARE cancelled
        node.assert_is_cancelled(messages=messages)
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

    def test_node_resolve_cancel_twice(self):
        """
        If we receive two cancel messages canceling the same message, the higher one should be stored as the
        latest cancel messages.

        The latest cancel message is the one that has the highest packet binary.

        In this test, we first same the original message C0, then we send the higher cancel message C2,
        and finally the lower cancel message C1.

        The receiver first updates its database with C2 as the latest cancel message of C0.
        When it receives C1, it will recognise that it has C2 > C1, and it sends C2 to the sender
        to inform him with the latest cancel message of C0.
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
        other.give_message(message, node)
        other.give_message(high_message, node)
        other.give_message(low_message, node)
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

        self._logger.debug("%d", len(high_message.packet))

        self.assertEqual(len(cancel_packets), len([high_message.packet]))

        # NODE should have both messages on the database and the lowest one should be cancelled by the highest.
        messages = other.fetch_messages((u"dispersy-cancel-own",))
        self.assertEquals(len(messages), 2)
        other.assert_is_stored(low_message)
        other.assert_is_stored(high_message)
        other.assert_is_cancelled(message, cancelled_by=high_message)

    def test_missing_message(self):
        """
        NODE generates a few messages without sending them to OTHER. Following, NODE cancels the
        messages and sends the cancel messages to OTHER. OTHER must now use a dispersy-missing-message
        to request the messages that are about to be cancelled. The messages need to be processed and
        subsequently cancelled.
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
        global_time_requests = list()

        for _, message in node.receive_messages(names=[u"dispersy-missing-message"]):
            self.assertEqual(message.payload.member.public_key, node.my_member.public_key)
            global_time_requests.extend(message.payload.global_times)

        self.assertEqual(sorted(global_times), sorted(global_time_requests))

        # give all 'delayed' messages
        other.give_messages(messages, node)

        # check that they are in the database and ARE cancelled
        other.assert_is_cancelled(messages=messages)
        other.assert_is_stored(messages=cancels)

    def test_revoke_causing_cancel(self):
        """
        3 peers: SELF, p1, p2
        steps:
        (1) SELF authorizes p1 to have cancel permission.
        (2) p2 creates message m1.
        (3) p1 creates message c1 to cancel m1.
        (4) SELF revokes the cancel permission from p1.
        (5) After (4), m1 should still be cancelled by c1.
        """
        p1, p2 = self.create_nodes(2)
        p1.send_identity(p2)

        # MM grants cancel permission to NODE
        authorize = self._mm.create_authorize([(p1.my_member, self._community.get_meta_message(u"full-sync-text"),
                                                u"cancel")], self._mm.claim_global_time())
        self._mm.give_message(authorize, self._mm)
        p1.give_message(authorize, self._mm)
        p2.give_message(authorize, self._mm)
        sleep(1)

        # OTHER creates a message
        m1 = p2.create_full_sync_text("will be cancelled", 10)
        self._mm.give_message(m1, p2)
        p2.give_message(m1, p2)
        p1.give_message(m1, p2)
        sleep(1)
        p2.assert_is_stored(m1)
        p1.assert_is_stored(m1)

        # NODE cancels the message
        cancel = p1.create_cancel_other(m1, m1.distribution.global_time + 1)
        p1.give_message(cancel, p1)
        p2.give_message(cancel, p1)
        self._mm.give_message(cancel, p1)
        sleep(1)
        p2.assert_is_cancelled(m1, cancelled_by=cancel)
        p2.assert_is_stored(cancel)

        # SELF revoke cancel permission from NODE
        revoke = self._mm.create_revoke([(p1.my_member, self._community.get_meta_message(u"full-sync-text"),
                                          u"cancel")], self._mm.claim_global_time())
        self._mm.give_message(revoke, self._mm)
        p1.give_message(revoke, self._mm)
        p2.give_message(revoke, self._mm)
        sleep(1)

        p1.assert_is_cancelled(m1, cancelled_by=cancel)
        p2.assert_is_cancelled(m1, cancelled_by=cancel)
