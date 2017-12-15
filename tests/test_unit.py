from __future__ import unicode_literals

import threading
import time
from collections import defaultdict
from itertools import count

import pytest
from asgi_rabbitmq import RabbitmqChannelLayer, RabbitmqLocalChannelLayer
from asgi_rabbitmq.core import (
    EXPIRE_GROUP_MEMBER,
    ConnectionThread,
    Protocol,
    RabbitmqConnection,
)
from asgi_rabbitmq.test import RabbitmqLayerTestCaseMixin
from asgiref.conformance import ConformanceTestCase
from channels.test import ChannelTestCase
from django.test import SimpleTestCase, TestCase
from msgpack.exceptions import ExtraData
from pika.exceptions import ConnectionClosed
from twisted.internet import defer, reactor
from twisted.trial.unittest import TestCase as TwistedTestCase


class SetupMixin(object):

    channel_layer_cls = RabbitmqChannelLayer
    expiry_delay = 1.1
    capacity_limit = 5
    heartbeat_interval = 15

    def setUp(self):

        self.amqp_url = '%s?heartbeat_interval=%d' % (self.amqp_url,
                                                      self.heartbeat_interval)
        self.channel_layer = self.channel_layer_cls(
            self.amqp_url,
            expiry=1,
            group_expiry=5,
            capacity=self.capacity_limit,
        )
        super(SetupMixin, self).setUp()

    @property
    def defined_exchanges(self):
        """Get exchange names defined in current vhost."""

        definitions = self.management.get_definitions()
        exchange_definitions = defaultdict(set)
        for exchange in definitions['exchanges']:
            exchange_definitions[exchange['vhost']].add(exchange['name'])
        exchanges = exchange_definitions[self.virtual_host]
        return exchanges

    @property
    def defined_queues(self):
        """Get queue names defined in current vhost."""

        definitions = self.management.get_definitions()
        queue_definitions = defaultdict(set)
        for queue in definitions['queues']:
            queue_definitions[queue['vhost']].add(queue['name'])
        queues = queue_definitions[self.virtual_host]
        return queues


class RabbitmqChannelLayerTest(SetupMixin, RabbitmqLayerTestCaseMixin,
                               SimpleTestCase, ConformanceTestCase):

    def test_send_to_empty_group(self):
        """Send to empty group works as usual."""

        self.skip_if_no_extension('groups')
        self.channel_layer.send_group('tgroup_1', {'value': 'orange'})
        # After send to an empty group we should be able to use layer
        # without error triggered by `send_group`.
        self.channel_layer.receive(['foo'])

    def test_discard_from_empty_group(self):
        """Discard from empty group works as usual."""

        self.skip_if_no_extension('groups')
        self.channel_layer.group_discard('tgroup_2', 'tg_test3')

    def test_receive_from_non_existed_channel(self):
        """
        If we tries to receive messages from channel which queue was not
        declared already, things should works just fine.
        """

        channel, message = self.channel_layer.receive(['foo'])
        assert (channel, message) == (None, None)

    def test_group_persistence_message_expiry(self):
        """
        Discard channel from all its groups when first message expires in
        channel.
        """

        # Setup group membership.
        self.skip_if_no_extension('groups')
        self.channel_layer.group_add('tgme_group1', 'tgme_test')
        self.channel_layer.group_add('tgme_group2', 'tgme_test')
        self.channel_layer.send('tgme_test', {'hello': 'world'})
        # Wait until message in the channel expires.
        time.sleep(self.channel_layer.expiry + 1)
        # Channel lost its membership in the group #1.
        self.channel_layer.send_group('tgme_group1', {'hello': 'world1'})
        time.sleep(0.2)  # Give dead letters time to work.
        channel, message = self.channel_layer.receive(['tgme_test'])
        self.assertIs(channel, None)
        self.assertIs(message, None)
        # Channel lost its membership in the group #2.
        self.channel_layer.send_group('tgme_group2', {'hello': 'world2'})
        time.sleep(0.2)  # Give dead letters time to work.
        channel, message = self.channel_layer.receive(['tgme_test'])
        self.assertIs(channel, None)
        self.assertIs(message, None)

    def test_reply_channel_group_persistence_message_expiry(self):
        """
        Discard reply channel from all its groups when first message
        expires in it.  Same as previous test, but check reply
        channels.
        """

        # Setup group membership.
        self.skip_if_no_extension('groups')
        name = self.channel_layer.new_channel('tgme_test?')
        self.channel_layer.group_add('tgme_group1', name)
        self.channel_layer.group_add('tgme_group2', name)
        self.channel_layer.send(name, {'hello': 'world'})
        # Wait until message in the channel expires.
        time.sleep(self.channel_layer.expiry + 1)
        # Channel lost its membership in the group #1.
        self.channel_layer.send_group('tgme_group1', {'hello': 'world1'})
        time.sleep(0.2)  # Give dead letters time to work.
        channel, message = self.channel_layer.receive([name])
        self.assertIs(channel, None)
        self.assertIs(message, None)
        # Channel lost its membership in the group #2.
        self.channel_layer.send_group('tgme_group2', {'hello': 'world2'})
        time.sleep(0.2)  # Give dead letters time to work.
        channel, message = self.channel_layer.receive([name])
        self.assertIs(channel, None)
        self.assertIs(message, None)

    @pytest.mark.slow
    def test_connection_heartbeats(self):
        """
        We must answer for RabbitMQ heartbeat frames responsively.
        Otherwise connection will be closed by server.
        """

        self.channel_layer.send('x', {'foo': 'bar'})
        channel, message = self.channel_layer.receive(['x'])
        time.sleep(self.heartbeat_interval * 3)
        # Code below will throw an exception if we don't send
        # heartbeat frames during sleep.
        self.channel_layer.send('x', {'baz': 'quux'})
        channel, message = self.channel_layer.receive(['x'])

    @pytest.mark.xfail
    def test_group_channels(self):

        # TODO: figure out how to check group membership.
        super(RabbitmqChannelLayerTest, self).test_group_channels()

    def test_new_channel_capacity(self):
        """Channel created with `new_channel` must support capacity check."""

        name = self.channel_layer.new_channel('test.foo?')
        for _ in range(self.capacity_limit):
            self.channel_layer.send(name, {'hey': 'there'})
        with self.assertRaises(self.channel_layer.ChannelFull):
            self.channel_layer.send(name, {'hey': 'there'})

    def test_per_channel_capacity(self):
        """
        Channel capacity can be unique per channel.  Check we support
        different capacity values in different channels.
        """

        layer = self.channel_layer_cls(
            self.amqp_url,
            expiry=1,
            group_expiry=5,
            capacity=self.capacity_limit,
            channel_capacity={
                'http.response?*': 10,
                'http.request': 30,
            },
        )
        # Test direct match.
        for _ in range(30):
            layer.send('http.request', {'hey': 'there'})
        with pytest.raises(self.channel_layer.ChannelFull):
            layer.send('http.request', {'hey': 'there'})
        # Test regexp match.
        name = layer.new_channel('http.response?')
        for _ in range(10):
            layer.send(name, {'hey': 'there'})
        with pytest.raises(self.channel_layer.ChannelFull):
            layer.send(name, {'hey': 'there'})

    def test_add_reply_channel_to_group(self):
        """
        Reply channel is the most popular candidate for group membership.
        Check we can do it.
        """

        name = self.channel_layer.new_channel('test.foo?')
        self.channel_layer.group_add('test.group', name)
        self.channel_layer.send_group('test.group', {'foo': 'bar'})
        time.sleep(0.2)  # Give dead letters time to work.
        channel, message = self.channel_layer.receive([name])
        assert channel == name
        assert message == {'foo': 'bar'}
        self.channel_layer.group_discard('test.group', name)
        self.channel_layer.send_group('test.group', {'foo': 'bar'})
        time.sleep(0.2)  # Give dead letters time to work anyway.
        channel, message = self.channel_layer.receive([name])
        assert channel is None
        assert message is None

    def test_access_to_the_layer_instance_from_different_threads(self):
        """
        Operations made by one thread shouldn't change connection state
        made from another thread.
        """

        def receive_non_existed():
            time.sleep(0.015)
            self.channel_layer.receive(['bar'])

        thread = threading.Thread(target=receive_non_existed)
        thread.deamon = True
        thread.start()

        # If all things goes fine, this line will be executed
        # successfully.
        self.channel_layer.group_add('my_group', 'foo')

    def test_receive_blocking_mode(self):
        """Check we can wait until message arrives and return it."""

        name = self.channel_layer.new_channel('foo?')

        def wait_and_send():
            time.sleep(1)
            self.channel_layer.send(name, {'bar': 'baz'})

        thread = threading.Thread(target=wait_and_send)
        thread.deamon = True
        thread.start()

        channel, message = self.channel_layer.receive([name], block=True)
        assert channel == name
        assert message == {'bar': 'baz'}

    def test_process_local_receive_blocking_mode(self):
        """
        Check we can wait until message arrives in the process local
        channel and return it.
        """

        def wait_and_send():
            time.sleep(1)
            self.channel_layer.send('foo!aaa', {'bar': 'baz'})

        thread = threading.Thread(target=wait_and_send)
        thread.deamon = True
        thread.start()

        channel, message = self.channel_layer.receive(['foo!'], block=True)
        assert channel == 'foo!aaa'
        assert message == {'bar': 'baz'}

    def test_send_group_message_expiry(self):
        """
        Tests that messages expire correctly when it was sent to group.
        """
        self.channel_layer.group_add('gr_test', 'me_test')
        self.channel_layer.send_group('gr_test', {'value': 'blue'})
        time.sleep(self.expiry_delay)
        channel, message = self.channel_layer.receive(['me_test'])
        self.assertIs(channel, None)
        self.assertIs(message, None)

    def test_group_add_is_idempotent(self):
        """
        Calling group_add continuously should set system into the right
        state.
        """

        name = self.channel_layer.new_channel('ch_test?')
        self.channel_layer.group_add('gr_test', name)
        self.channel_layer.thread.schedule(
            EXPIRE_GROUP_MEMBER,
            'gr_test',
            name,
        )
        time.sleep(0.1)
        # NOTE: Implementation detail.  Dead letters consumer should
        # ignore messages died with maxlen reason.  This messages
        # caused by sequential group_add calls.
        self.channel_layer.send_group('gr_test', {'value': 'blue'})
        time.sleep(0.2)  # Give dead letters time to work.
        channel, message = self.channel_layer.receive([name])
        assert channel == name
        assert message == {'value': 'blue'}

    def test_connection_on_close_notify_futures(self):
        """
        If connection in the connection thread was closed for some reason
        we should notify waiting thread about this error.
        """

        # Wait for connection established.
        while not self.channel_layer.thread.connection.protocols:
            time.sleep(0.5)
        # Get dead letters future.
        future = self.channel_layer.thread.connection.protocols[None].resolve
        # Look into on_close_callback.
        self.channel_layer.thread.connection.connection.close()
        with pytest.raises(ConnectionClosed):
            future.result()

    def test_deny_schedule_calls_to_the_closed_connection(self):
        """
        If connection is already closed, it shouldn't be possible to call
        layer methods on it.
        """

        # Wait for connection established.
        while not self.channel_layer.thread.connection.connection.is_open:
            time.sleep(0.5)
        name = self.channel_layer.new_channel('foo?')
        # Close connection and wait for it.
        self.channel_layer.thread.connection.connection.close()
        while not self.channel_layer.thread.connection.connection.is_closed:
            time.sleep(0.5)
        # Look into is_closed check.
        with pytest.raises(ConnectionClosed):
            self.channel_layer.send(name, {'bar': 'baz'})

    def test_resolve_callbacks_during_connection_close(self):
        """
        Connection can be in closing state.  If during this little time
        frame another thread tries to schedule callback into this
        connection, we should interrupt immediately.
        """

        # Wait for connection established.
        while not self.channel_layer.thread.connection.connection.is_open:
            time.sleep(0.5)
        name = self.channel_layer.new_channel('foo?')
        # Try to call layer send right after connection close frame
        # was sent.
        self.channel_layer.thread.connection.connection.close()
        with pytest.raises(ConnectionClosed):
            self.channel_layer.send(name, {'bar': 'baz'})

    def test_message_cryptography(self):
        """
        We can encrypt messages.  Layer without crypto keys can't read
        messages sent with the layer which has one.
        """

        name = self.channel_layer.new_channel('foo?')
        crypto_layer = self.channel_layer_cls(
            self.amqp_url,
            expiry=1,
            group_expiry=5,
            capacity=self.capacity_limit,
            symmetric_encryption_keys=['test', 'old'],
        )

        crypto_layer.send(name, {'bar': 'baz'})
        with pytest.raises(ExtraData):
            self.channel_layer.receive([name])
        crypto_layer.send(name, {'bar': 'baz'})
        channel, message = crypto_layer.receive([name])
        assert channel == name
        assert message == {'bar': 'baz'}

    def test_protocol_concurrent_open(self):
        """We can open only one amqp channel per thread at the same time."""

        class TestProtocol(Protocol):

            counter = count(start=1)

            def __init__(self, *args, **kwargs):

                next(self.counter)
                super(TestProtocol, self).__init__(*args, **kwargs)

        class TestRabbitmqConnection(RabbitmqConnection):

            Protocol = TestProtocol

        class TestConnectionThread(ConnectionThread):

            Connection = TestRabbitmqConnection

        class TestRabbitmqChannelLayer(self.channel_layer_cls):

            Thread = TestConnectionThread

        layer = TestRabbitmqChannelLayer(
            self.amqp_url,
            expiry=1,
            group_expiry=5,
            capacity=self.capacity_limit,
        )
        layer.send_group('foo', {'bar': 'baz'})
        layer.send_group('foo', {'x': 'y'})
        # One for worker thread, one for dead letters.
        assert next(TestProtocol.counter) == 3

    def test_expired_message_does_not_destroy_process_local_channel(self):
        """
        When expired message comes into dead letter handler we need
        exclude channel from all its group.  For process local
        channels this means remove fully qualified intermediate queue.
        This queue name is similar to the non-local part of process
        channel.  Lets check we destroy right queue.
        """

        # Declare exclusive queue implicitly.
        self.channel_layer.receive(['test!'])
        # Actual test.
        self.channel_layer.group_add('test_group', 'test!foo')
        self.channel_layer.send_group('test_group', {'x': 'y'})
        time.sleep(self.expiry_delay)
        assert 'test!' in self.defined_queues
        assert 'test!foo' not in self.defined_queues

    def test_error_in_the_dead_letter(self):
        """Error in the dead letter processing shouldn't broke connection."""

        time.sleep(0.2)  # Give dead letters time to create exchange.
        # Publish to the dead letter broken message.  It's header does
        # not has `x-death` key so callback will fail.
        self.channel_layer.thread.connection.is_open.wait()
        with self.channel_layer.thread.connection.lock:
            self.channel_layer.thread.connection.connection.channel(
                lambda amqp_channel: amqp_channel.basic_publish(
                    exchange='',
                    routing_key='dead-letters',
                    body='foobazbar',
                ))
        time.sleep(0.2)  # Give dead letters time to work.
        # This should work as expected.
        self.channel_layer.send('foo', {'bar': 'baz'})
        assert 'foo', {'bar': 'baz'} == self.channel_layer.receive(['foo'])

    def test_groups_exchange_cleanup(self):
        """
        When all queues of all channels in the group were removed, we
        should remove groups and intermediate exchanges.
        """

        foo = self.channel_layer.new_channel('foo?')
        bar = self.channel_layer.new_channel('bar?')
        baz = self.channel_layer.new_channel('baz?')
        self.channel_layer.group_add('gr_foo', foo)
        self.channel_layer.group_add('gr_foo', bar)
        self.channel_layer.group_add('gr_foo', baz)

        # Wait for group cleanup.
        time.sleep(self.channel_layer.expiry * 2)

        # Assert intermediate exchanges.
        assert 'gr_foo' in self.defined_exchanges
        assert foo.rsplit('?', 1)[-1] not in self.defined_exchanges
        assert bar.rsplit('?', 1)[-1] not in self.defined_exchanges
        assert baz.rsplit('?', 1)[-1] not in self.defined_exchanges

    def test_regular_channel_queues_cleanup(self):
        """
        We should remove regular channel queue after message TTL x2 time.
        """

        # Create process local channel.
        self.channel_layer.send('foo', {'bar': 'baz'})

        # Wait for group cleanup.
        time.sleep(self.channel_layer.expiry * 2)

        # Check chat corresponding queue was removed.
        assert 'foo' not in self.defined_queues

    def test_single_reader_queues_cleanup(self):
        """
        We should remove single reader queue after message TTL x2 time.
        """

        # Create single reader channel.
        name = self.channel_layer.new_channel('foo?')

        # Wait for group cleanup.
        time.sleep(self.channel_layer.expiry * 2)

        # Check chat corresponding queue was removed.
        assert name not in self.defined_queues

    def test_process_local_queues_cleanup(self):
        """
        We should remove process local queue after message TTL x2 time.
        """

        # Create process local channel.
        self.channel_layer.send('foo.xxx!yyy', {'bar': 'baz'})

        # Wait for group cleanup.
        time.sleep(self.channel_layer.expiry * 2)

        # Check chat corresponding queue was removed.
        assert 'foo.xxx!' not in self.defined_queues

    def test_delay_connection_for_first_method_call(self):
        """
        We shouldn't connect to the broker until use call layer method
        first time.
        """

        time.sleep(0.2)  # Give connection time to be open.
        assert not self.defined_queues
        self.channel_layer.send('foo.xxx!yyy', {'bar': 'baz'})
        assert {'foo.xxx!', 'dead-letters'} == self.defined_queues

    def test_continuous_group_add_handle_expire_marker(self):
        """
        When we sequentially call `group_add` it create expiration marker
        queue.  This queue has maximum length equal to one.  Marker
        message from previous call will be dead lettered with `maxlen`
        reason.  This message should be dropped without further
        processing.


        Initial issue: https://github.com/proofit404/asgi_rabbitmq/pull/10
        """

        # Hack to declare `expire.bind.foo.xxx!` queue.
        self.channel_layer.receive(['expire.bind.foo.xxx!'])
        # Sequentially add channel to the group.
        self.channel_layer.group_add('foo', 'xxx!a')
        self.channel_layer.group_add('foo', 'xxx!a')
        # Check if message was dropped in the dead letters handler.
        channel, message = self.channel_layer.receive(['expire.bind.foo.xxx!'])
        assert channel is None
        assert message is None


class RabbitmqLocalChannelLayerTest(RabbitmqChannelLayerTest):

    channel_layer_cls = RabbitmqLocalChannelLayer

    def test_send_normal_channel_to_local_layer(self):
        """If this is usual channel we must use local channel layer."""

        self.channel_layer.send('foo', {'bar': 'baz'})
        assert 'foo' not in self.defined_queues

    def test_send_reply_channel_to_rabbitmq_layer(self):
        """If this is reply channel we must use rabbitmq channel layer."""

        name = self.channel_layer.new_channel('foo?')
        self.channel_layer.send(name, {'bar': 'baz'})
        assert name in self.defined_queues

        name = self.channel_layer.new_channel('foo?')
        self.channel_layer.send(name, {'bar': 'baz'})
        assert name in self.defined_queues

    def test_groups(self):
        """
        Tests that basic group addition and send works.  We need to use
        single reader channels to be sure messages will go through
        RabbitMQ.
        """

        self.skip_if_no_extension('groups')
        name1 = self.channel_layer.new_channel('tg_test?')
        name2 = self.channel_layer.new_channel('tg_test?')
        name3 = self.channel_layer.new_channel('tg_test?')
        # Make a group and send to it
        self.channel_layer.group_add('tgroup', name1)
        self.channel_layer.group_add('tgroup', name2)
        self.channel_layer.group_add('tgroup', name3)
        self.channel_layer.group_discard('tgroup', name3)
        self.channel_layer.send_group('tgroup', {'value': 'orange'})
        # Receive from the two channels in the group and ensure messages
        channel, message = self.channel_layer.receive([name1])
        assert channel == name1
        assert message == {'value': 'orange'}
        channel, message = self.channel_layer.receive([name2])
        assert channel == name2
        assert message == {'value': 'orange'}
        # Make sure another channel does not get a message
        channel, message = self.channel_layer.receive([name3])
        assert channel is None
        assert message is None


class InheritanceTest(TestCase):

    def test_mixin_inheritance_verification(self):
        """
        RabbitmqLayerTestCaseMixin should deny multiple inheritance
        together with ChannelTestCaseMixin.  This mixin substitute our
        layer with inmemory one, but our goal here to test real project
        against real broker.
        """

        class Test(RabbitmqLayerTestCaseMixin, ChannelTestCase):

            def runTest(self):

                pass

        # Emulate test run.
        test = Test()
        result = test.defaultTestResult()
        test(result)
        [(_, error)] = result.errors
        assert 'ImproperlyConfigured' in error


@pytest.mark.twisted
class RabbitmqChannelLayerTwistedTest(SetupMixin, RabbitmqLayerTestCaseMixin,
                                      SimpleTestCase, TwistedTestCase):

    def tearDown(self):

        reactor.removeAll()
        super(RabbitmqChannelLayerTwistedTest, self).tearDown()

    @defer.inlineCallbacks
    def test_receive_twisted(self):
        """Receive with twisted extension."""

        self.channel_layer.send("sr_test", {"value": "blue"})
        self.channel_layer.send("sr_test", {"value": "green"})
        self.channel_layer.send("sr_test2", {"value": "red"})

        # Get just one first
        channel, message = yield self.channel_layer.receive_twisted(
            ["sr_test"])
        self.assertEqual(channel, "sr_test")
        self.assertEqual(message, {"value": "blue"})

        # And the second
        channel, message = yield self.channel_layer.receive_twisted(
            ["sr_test"])
        self.assertEqual(channel, "sr_test")
        self.assertEqual(message, {"value": "green"})

        # And the other channel with multi select
        channel, message = yield self.channel_layer.receive_twisted(
            ["sr_test", "sr_test2"])
        self.assertEqual(channel, "sr_test2")
        self.assertEqual(message, {"value": "red"})


@pytest.mark.twisted
@pytest.mark.local
class RabbitmqLocalChannelLayerTwistedTest(RabbitmqChannelLayerTwistedTest):

    channel_layer_cls = RabbitmqLocalChannelLayer
