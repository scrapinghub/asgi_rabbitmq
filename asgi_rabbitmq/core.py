import base64
import collections
import hashlib
import logging
import random
import string
import time
from concurrent.futures import Future
from functools import partial
from threading import Thread, Lock, Event, get_ident

import msgpack
from asyncio import wrap_future
from channels.exceptions import ChannelFull
from channels.layers import BaseChannelLayer
from pika import SelectConnection, URLParameters
from pika.channel import Channel
from pika.exceptions import ChannelClosed, ConnectionClosed
from pika.spec import BasicProperties, Confirm

logger = logging.getLogger(__name__)

SEND = 0
RECEIVE = 1
GROUP_ADD = 3
GROUP_DISCARD = 4
SEND_GROUP = 5
DECLARE_DEAD_LETTERS = 6
EXPIRE_GROUP_MEMBER = 7


def _apply_channel_prefix(prefix, channel):
    return f'{prefix}:{channel}'


def _apply_group_prefix(prefix, group):
    return f'{prefix}:group:{group}'


def _strip_channel_prefix(prefix, name):
    return name[len(_apply_channel_prefix(prefix, '')):]


def _strip_group_prefix(prefix, name):
    return name[len(_apply_group_prefix(prefix, '')):]


class Protocol(object):
    """ASGI implementation in the terms of AMQP channel methods."""

    dead_letters = 'dead-letters'
    expire_marker = 'expire.bind.'
    """Name of the protocol dead letters exchange and queue."""

    def __init__(self, channel_factory, prefix, expiry, group_expiry,
                 get_capacity, non_local_name, crypter):
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.get_capacity = get_capacity
        self.non_local_name = non_local_name
        self.crypter = crypter
        # Mapping for the connection schedule method.
        self.methods = {
            SEND: self.send,
            RECEIVE: self.receive,
            GROUP_ADD: self.group_add,
            GROUP_DISCARD: self.group_discard,
            SEND_GROUP: self.send_group,
            DECLARE_DEAD_LETTERS: self.declare_dead_letters,
        }
        self.waiting_calls = []
        self.messages = {}
        self.message_number = 0
        self.unresolved_futures = set()
        self.confirmed_message_number = -1
        self.are_confirmations_enabled = False
        self.received_messages = collections.defaultdict(collections.deque)
        self.waiting_receivers = collections.defaultdict(collections.deque)
        self.consumer_tags = {}
        self.prefix = prefix
        self.dead_letters = _apply_channel_prefix(prefix, self.dead_letters)
        self.expire_marker = _apply_channel_prefix(prefix, self.expire_marker)
        self.amqp_channel = channel_factory(self.on_connect)
        # Handle AMQP channel level errors with protocol.
        self.amqp_channel.on_callback_error_callback = self.protocol_error

    # Utilities.

    def on_connect(self, unused_channel):
        # set prefetch count for the channel to prevent redelivery
        # amplification when the consumer stops consuming after each message
        self.amqp_channel.basic_qos(
            self.apply_waiting,
            prefetch_count=1,
            # in RabbitMQ this is actually all consumers on the channel
            # https://www.rabbitmq.com/consumer-prefetch.html
            all_channels=True,
        )

    def apply(self, method_id, args, kwargs, future):
        """Take method from the mapping and call it."""

        if self.is_open:
            self.register_future(future)
            self.methods[method_id](future, *args, **kwargs)
        else:
            self.waiting_calls.append((method_id, args, kwargs, future))

    def apply_waiting(self, unused_frame):
        if self.is_open:
            for call in self.waiting_calls:
                self.apply(*call)
            self.waiting_calls.clear()

    def register_future(self, future):
        if isinstance(future, Future):
            future.add_done_callback(self.unregister_future)
            self.unresolved_futures.add(future)

    def unregister_future(self, future):
        self.unresolved_futures.discard(future)

    def protocol_error(self, error):
        """
        AMQP channel level error callback.  Pass error to the waiting
        thread.
        """

        for future in list(self.unresolved_futures):
            future.set_exception(error)

    @property
    def is_open(self):
        return self.amqp_channel.is_open

    @property
    def is_closed(self):
        return self.amqp_channel.is_closing or self.amqp_channel.is_closed

    # Send.

    def send(self, future, channel, message):
        """Start message sending.  Declare necessary queue first."""

        queue = self.non_local_name(channel)
        self.amqp_channel.queue_declare(
            partial(self.handle_publish, future, channel, message),
            queue=queue,
            arguments=self.queue_arguments,
        )

    def handle_publish(self, future, channel, message, method_frame):
        """Queue declared.  Check channel capacity."""

        if method_frame.method.message_count >= self.get_capacity(channel):
            future.set_exception(ChannelFull())
            return
        body = self.serialize(message)
        self.publish_message(future, channel, body)

    def publish_message(self, future, channel, body):
        """Channel capacity check is done.  Publish message."""
        queue = self.non_local_name(channel)
        self.basic_publish(
            future,
            exchange='',
            routing_key=queue,
            body=body,
            properties=self.publish_properties(channel),
        )

    def basic_publish(self, future, **kwargs):
        self.message_number += 1
        if future is not None:
            self.messages[self.message_number] = future
        # enable delivery confirmations once on first message
        if self.are_confirmations_enabled:
            self.amqp_channel.basic_publish(mandatory=True, **kwargs)
        else:
            self.amqp_channel.add_callback(
                lambda unused_frame: self.amqp_channel.basic_publish(
                    mandatory=True,
                    **kwargs,
                ),
                [Confirm.SelectOk],
            )
            self.amqp_channel.confirm_delivery(self.on_delivery_confirmation)
            self.are_confirmations_enabled = True

    def on_delivery_confirmation(self, frame):
        for message_number in range(
            (self.confirmed_message_number + 1
             if frame.method.multiple
             else frame.method.delivery_tag),
            frame.method.delivery_tag + 1,
        ):
            future = self.messages.pop(message_number, None)
            if future is not None:
                future.set_result(None)
        self.confirmed_message_number = frame.method.delivery_tag

    def publish_properties(self, channel=None):
        """AMQP message properties."""

        # Store local part of the process local channel in the AMQP
        # message header.
        if channel and '!' in channel:
            headers = {'asgi_channel': channel.rsplit('!')[-1]}
        else:
            headers = None
        # Specify message expiration in milliseconds.
        expiration = str(self.expiry * 1000)
        properties = BasicProperties(headers=headers, expiration=expiration)
        return properties

    # Receive.

    def receive(self, future, channel):
        """Initiate message receive."""

        if channel in self.received_messages:
            received = self.received_messages[channel]
            future.set_result(received.popleft())
            if not len(received):
                del self.received_messages[channel]
            return

        self.waiting_receivers[channel].append(future)
        future.add_done_callback(partial(self.cancel_receiving, channel))

        queue = self.non_local_name(channel)
        if queue not in self.consumer_tags:
            # reserve the consumer tag
            self.consumer_tags[queue] = None
            self.amqp_channel.queue_declare(
                partial(self.receive_queue_declared, queue),
                queue,
                arguments=self.queue_arguments,
            )

    def receive_queue_declared(self, queue, frame):
        consumer_tag = self.amqp_channel.basic_consume(
            partial(self.consume_message, queue),
            queue=queue,
        )
        self.consumer_tags[queue] = consumer_tag

    def consume_message(
        self, queue, amqp_channel, method_frame, properties, body
    ):
        if properties.headers and 'asgi_channel' in properties.headers:
            channel = queue + properties.headers['asgi_channel']
        else:
            channel = queue

        if channel in self.waiting_receivers:
            channel_receivers = self.waiting_receivers[channel]

            try:
                while channel_receivers:
                    future = channel_receivers.popleft()

                    # Don't allow cancel after we got a message
                    if future.set_running_or_notify_cancel():
                        if not self._has_receivers(queue):
                            amqp_channel.basic_cancel(
                                consumer_tag=method_frame.consumer_tag,
                            )
                            del self.consumer_tags[queue]
                        amqp_channel.basic_ack(method_frame.delivery_tag)

                        # Send the message to the waiting Future.
                        future.set_result(self.deserialize(body))
                        return
            finally:
                if not len(channel_receivers):
                    # it might have been removed by a future callback
                    if channel in self.waiting_receivers:
                        del self.waiting_receivers[channel]

        # otherwise save this message for later
        self.received_messages[channel].append(self.deserialize(body))

    def cancel_receiving(self, channel, future):
        channel_receivers = self.waiting_receivers[channel]
        try:
            channel_receivers.remove(future)
        except ValueError:
            pass
        if not channel_receivers:
            del self.waiting_receivers[channel]

        queue = self.non_local_name(channel)
        if queue in self.consumer_tags:
            consumer_tag = self.consumer_tags[queue]
            if (consumer_tag is not None and
                    not self._has_receivers(queue)):
                try:
                    self.amqp_channel.basic_cancel(
                        consumer_tag=consumer_tag,
                    )
                except ChannelClosed:
                    pass

                del self.consumer_tags[queue]

    def _has_receivers(self, queue):
        return any((self.non_local_name(channel) == queue
                    and receivers)
                   for channel, receivers
                   in self.waiting_receivers.items())

    @property
    def queue_arguments(self):
        """Channel queue declaration arguments."""

        return {
            'x-dead-letter-exchange': self.dead_letters,
            'x-expires': self.expiry * 2000,
        }

    # Groups.

    def group_add(self, future, group, channel):
        """Initiate member addition to the group."""

        self.expire_group_member(group, channel)

        # The last callback.
        def after_bind(method_frame):
            future.set_result(None)

        if '!' in channel:
            # Process local channels needs its own queue for
            # membership.  This queue will be bound to the group
            # exchange.

            def bind_channel(method_frame):

                self.amqp_channel.queue_bind(
                    callback=after_bind,
                    queue=channel,
                    exchange=group,
                )

            def declare_member(method_frame):

                self.amqp_channel.queue_declare(
                    callback=bind_channel,
                    queue=channel,
                    arguments={
                        'x-dead-letter-exchange': self.dead_letters,
                        'x-expires': self.group_expiry * 1000,
                        'x-max-length': 0,
                    },
                )
        else:
            # Regular channel and single reader channels needs
            # exchange to exchange binding.  So message will be routed
            # to the queue without dead letters mechanism.

            def bind_channel(method_frame):

                self.amqp_channel.queue_bind(
                    after_bind,
                    queue=channel,
                    exchange=channel,
                )

            def bind_group(method_frame):

                self.amqp_channel.exchange_bind(
                    bind_channel,
                    destination=channel,
                    source=group,
                )

            def declare_channel(method_frame):

                self.amqp_channel.queue_declare(
                    bind_group,
                    queue=channel,
                    arguments=self.queue_arguments,
                )

            def declare_member(method_frame):

                self.amqp_channel.exchange_declare(
                    declare_channel,
                    exchange=channel,
                    exchange_type='fanout',
                    auto_delete=True,
                )

        # Declare group exchange and start one of the callback chains
        # described above.
        self.amqp_channel.exchange_declare(
            declare_member,
            exchange=group,
            exchange_type='fanout',
            auto_delete=True,
        )

    def group_discard(self, future, group, channel):
        """Initiate member removing from the group."""

        def after_unbind(method_frame):
            if future is not None:
                future.set_result(None)

        if '!' in channel:
            self.amqp_channel.queue_unbind(
                after_unbind,
                queue=channel,
                exchange=group,
            )
        else:
            self.amqp_channel.exchange_unbind(
                after_unbind,
                destination=channel,
                source=group,
            )

    def send_group(self, future, group, message):
        """Publish the message to the group exchange."""

        body = self.serialize(message)
        self.basic_publish(
            future,
            exchange=group,
            routing_key='',
            body=body,
            properties=self.publish_properties(),
        )

    # Dead letters processing.

    def expire_group_member(self, group, channel):
        """
        Create the queue with group membership expiration marker message.
        """

        ttl = self.group_expiry * 1000
        self.amqp_channel.queue_declare(
            partial(self.push_marker, group, channel),
            queue=self.get_expire_marker(group, channel),
            arguments={
                'x-dead-letter-exchange': self.dead_letters,
                'x-max-length': 1,
                'x-message-ttl': ttl,
                # Give broker some time to expire message before
                # expire whole queue.
                'x-expires': ttl + 25,
            },
        )

    def push_marker(self, group, channel, method_frame):
        """The queue was created.  Push the marker."""

        body = self.serialize({
            'group': group,
            'channel': channel,
        })
        self.basic_publish(
            None,
            exchange='',
            routing_key=self.get_expire_marker(group, channel),
            body=body,
        )

    def get_expire_marker(self, group, channel):
        """Get expire marker queue name."""

        prefix = self.prefix
        return '%s%s.%s' % (
            self.expire_marker,
            _strip_group_prefix(prefix, group),
            _strip_channel_prefix(prefix, channel),
        )

    def declare_dead_letters(self, future):
        """
        Initiate dead letters processing.  Declare dead letters exchange
        and queue.  Bind them together.  Start dead letter consumer.
        """

        def consume(method_frame):

            self.amqp_channel.basic_consume(
                self.on_dead_letter,
                queue=self.dead_letters,
            )

        def do_bind(method_frame):

            self.amqp_channel.queue_bind(
                consume,
                queue=self.dead_letters,
                exchange=self.dead_letters,
            )

        def declare_queue(method_frame):

            self.amqp_channel.queue_declare(
                do_bind,
                queue=self.dead_letters,
                arguments={
                    'x-expires': max(
                        self.expiry * 2000,
                        self.group_expiry * 1000,
                    ) * 2,
                },
            )

        self.amqp_channel.exchange_declare(
            declare_queue,
            exchange=self.dead_letters,
            exchange_type='fanout',
            auto_delete=True,
        )

    def on_dead_letter(self, amqp_channel, method_frame, properties, body):
        """Consumer callback for messages from the dead letter exchange."""

        amqp_channel.basic_ack(method_frame.delivery_tag)
        # Take the most recent death reason.
        queue = properties.headers['x-death'][0]['queue']
        reason = properties.headers['x-death'][0]['reason']
        if reason == 'expired' and self.is_expire_marker(queue):
            # Group membership expired.  Discard it.
            message = self.deserialize(body)
            group = message['group']
            channel = message['channel']
            self.group_discard(None, group, channel)
        elif reason == 'expired' and not self.is_expire_marker(queue):
            # The message was expired in the channel.  Discard all
            # group membership for this channel.
            if '!' in queue:
                queue = queue + properties.headers['asgi_channel']
                amqp_channel.queue_delete(queue=queue)
            else:
                amqp_channel.exchange_delete(exchange=queue)
        elif reason == 'maxlen' and self.is_expire_marker(queue):
            # Existing group membership was updated second time.
            return
        elif reason == 'maxlen' and '!' in queue:
            # Send group method was applied to the process local
            # channel.  Redeliver message to the right queue.
            self.publish_message(None, queue, body)

    def is_expire_marker(self, queue):
        """Check if the queue is an expiration marker."""

        return queue.startswith(self.expire_marker)

    # Serialization.

    def serialize(self, message):
        """Serialize message."""

        value = msgpack.packb(message, use_bin_type=True)
        if self.crypter:
            value = self.crypter.encrypt(value)
        return value

    def deserialize(self, message):
        """Deserialize message."""

        if self.crypter:
            message = self.crypter.decrypt(message, self.expiry + 10)
        return msgpack.unpackb(message, raw=False)


class LayerChannel(Channel):
    """`pika.Channel` wrapper with additional error handling logic."""

    def __init__(self, *args, **kwargs):

        # Will be set in the connection wrapper process method.
        # Usually point to the corresponding protocol method.
        self.on_callback_error_callback = None
        super(LayerChannel, self).__init__(*args, **kwargs)

    # Errors of the AMQP channel level are likely to happens in
    # `_on_deliver`, `_on_getok` and `_on_close` methods.

    def _on_deliver(self, method_frame, header_frame, body):

        try:
            super(LayerChannel, self)._on_deliver(method_frame, header_frame,
                                                  body)
        except Exception as error:
            if self.on_callback_error_callback:
                self.on_callback_error_callback(error)

    def _on_getok(self, method_frame, header_frame, body):

        try:
            super(LayerChannel, self)._on_getok(method_frame, header_frame,
                                                body)
        except Exception as error:
            if self.on_callback_error_callback:
                self.on_callback_error_callback(error)

    def _on_close(self, method_frame):

        super(LayerChannel, self)._on_close(method_frame)
        if self.on_callback_error_callback:
            self.on_callback_error_callback(
                ChannelClosed(
                    method_frame.method.reply_code,
                    method_frame.method.reply_text,
                ),
            )


class LayerConnection(SelectConnection):
    """
    `pika.Connection` wrapper with multi-thread access support and
    additional error handler.
    """

    Channel = LayerChannel

    def __init__(self, *args, **kwargs):

        self.on_callback_error_callback = kwargs.pop(
            'on_callback_error_callback',
        )
        self.lock = kwargs.pop('lock')
        super(LayerConnection, self).__init__(*args, **kwargs)

    def _process_frame(self, frame_value):

        # Connection event loop should be accessed only from one
        # thread simultaneously.  So we protect it with the lock.
        # Access from other threads grained in the connection wrapper.
        # But event loop will be affected by processing frames from
        # RabbitMQ.  So we protect event loop from connection thread
        # itself.
        with self.lock:
            return super(LayerConnection, self)._process_frame(frame_value)

    def _process_callbacks(self, frame_value):

        try:
            return super(LayerConnection, self)._process_callbacks(frame_value)
        except Exception as error:
            # Error happens in the event loop.  Pass control to the
            # connection wrapper handler.
            self.on_callback_error_callback(error)
            raise

    def _create_channel(self, channel_number, on_open_callback):

        return self.Channel(self, channel_number, on_open_callback)


class RabbitmqConnection(object):
    """RabbitMQ connection wrapper."""

    Parameters = URLParameters
    Connection = LayerConnection
    Protocol = Protocol

    def __init__(self, url, prefix, expiry, group_expiry, get_capacity,
                 non_local_name, crypter):

        self.url = url
        self.prefix = prefix
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.get_capacity = get_capacity
        self.non_local_name = non_local_name
        self.crypter = crypter

        # Connection access lock.
        self.lock = Lock()
        # Connection startup event.
        self.is_open = Event()
        self.parameters = self.Parameters(self.url)
        self.connection = self.connect()

    def run(self):
        """Start connection event loop."""

        self.connection.ioloop.start()

    def connect(self):
        """Prepare connection state."""

        # Connection startup event.
        self.is_open.clear()
        # Thread to protocol instance mapping.
        self.protocols = {}
        # Connection object.
        return self.Connection(
            parameters=self.parameters,
            on_open_callback=self.on_connection_open,
            on_close_callback=self.on_connection_closed,
            on_callback_error_callback=self.protocol_error,
            stop_ioloop_on_close=False,
            lock=self.lock,
        )

    def on_connection_open(self, connection):
        """Connection open callback."""

        self.is_open.set()
        self.process(None, (DECLARE_DEAD_LETTERS, (), {}, Future()))

    def on_connection_closed(self, connection, code, msg):
        """
        Connection close callback.  By default notify all waiting threads
        about connection state, then teardown event loop or try to reconnect.
        """

        try:
            self.protocol_error(ConnectionClosed())
        except:
            logger.warning(
                'Exception when notifying threads about closed '
                'connection', exc_info=True)
        if self.connection.is_closing:
            self.connection.ioloop.stop()
        else:
            logger.info('Connection closed, reopening in 5 seconds')
            self.connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        """Create new connection and restart IO loop."""

        self.connection.ioloop.stop()
        if not self.connection.is_closing:
            # Create a new connection
            self.connection = self.connect()
            # There is now a new connection, needs a new ioloop to run
            self.connection.ioloop.start()

    def protocol_error(self, error):
        """
        Notify threads waited for results that fatal error happens inside
        connection event loop.
        """

        for protocol in self.protocols.values():
            protocol.protocol_error(error)

    def process(self, ident, method):
        """
        Apply protocol method to the existed protocol instance.  Create
        one if necessary.
        """

        if ident not in self.protocols or self.protocols[ident].is_closed:
            # Given thread doesn't have corresponding protocol instance.
            # Start new AMQP channel for it and wrap it into protocol
            # instance.  Protocol method will be called as channel open
            # callback.
            self.protocols[ident] = self.Protocol(
                channel_factory=self.connection.channel,
                prefix=self.prefix,
                expiry=self.expiry,
                group_expiry=self.group_expiry,
                get_capacity=self.get_capacity,
                non_local_name=self.non_local_name,
                crypter=self.crypter,
            )
        # We have running AMQP channel for current given thread.
        # Apply protocol method directly.
        self.protocols[ident].apply(*method)

    def wait_open(self):
        """Wait for connection to start."""

        if self.connection.is_closing or self.connection.is_closed:
            raise ConnectionClosed
        # This method is accessed from other thread.  We must ensure
        # that connection event loop is ready.
        self.is_open.wait()

    def schedule(self, f, *args, **kwargs):
        """
        Try to acquire connection access lock.  Then call protocol method.
        Return concurrent Future instance you can wait in the other
        thread.
        """

        self.wait_open()
        # RabbitMQ operations are multiplexed between different AMQP
        # method callbacks.  Final result of the protocol method call
        # will be set inside one of this callbacks.  So other thread
        # will be able to wait unless this event happens in the
        # connection event loop.
        future = Future()
        with self.lock:
            self.process(get_ident(), (f, args, kwargs, future))
        return future

    @property
    def thread_protocol(self):
        """
        Protocol instance corresponding to the current thread.  Not
        intended to be called from connection thread.
        """

        return self.protocols[get_ident()]


class ConnectionThread(Thread):
    """
    Thread holding connection.

    Separate heartbeat frames processing from actual work.
    """

    Connection = RabbitmqConnection

    def __init__(self, url, prefix, expiry, group_expiry, get_capacity,
                 non_local_name, crypter):

        super(ConnectionThread, self).__init__()
        self.daemon = True
        self.connection = self.Connection(
            url, prefix, expiry, group_expiry, get_capacity, non_local_name,
            crypter,
        )

    def run(self):
        """Start connection thread."""

        self.connection.run()

    def schedule(self, f, *args, **kwargs):
        """
        Schedule protocol method execution in the context of the
        connection thread.
        """

        while True:
            try:
                return self.connection.schedule(f, *args, **kwargs)
            except ConnectionClosed:
                delay = self.connection.parameters.retry_delay
                logger.warning(
                    'Disconnected, retrying after %s seconds', delay)
                time.sleep(delay)


class RabbitmqChannelLayer(BaseChannelLayer):
    """
    RabbitMQ channel layer.

    It routes all messages into remote RabbitMQ server. Support for
    RabbitMQ cluster and message encryption are provided.
    """

    extensions = ['groups']

    Thread = ConnectionThread

    def __init__(
        self,
        url,
        prefix='asgi:',
        expiry=60,
        group_expiry=86400,
        capacity=100,
        channel_capacity=None,
        symmetric_encryption_keys=None,
    ):

        super(RabbitmqChannelLayer, self).__init__(
            expiry=expiry, capacity=capacity, channel_capacity=channel_capacity
        )
        self.prefix = prefix
        self.client_prefix = ''.join(
            random.choice(string.ascii_letters) for i in range(8)
        )
        self.group_expiry = group_expiry
        if symmetric_encryption_keys:
            try:
                from cryptography.fernet import MultiFernet
            except ImportError:
                raise ValueError("Cannot run without 'cryptography' installed")
            sub_fernets = [
                self.make_fernet(key) for key in symmetric_encryption_keys
            ]
            crypter = MultiFernet(sub_fernets)
        else:
            crypter = None
        self.thread = self.Thread(
            url, prefix, expiry, group_expiry, self.get_capacity,
            self.non_local_name, crypter,
        )
        self.thread.start()

    def make_fernet(self, key):
        """
        Given a single encryption key, returns a Fernet instance using it.
        """

        from cryptography.fernet import Fernet

        key = key.encode('utf8')
        formatted_key = base64.urlsafe_b64encode(hashlib.sha256(key).digest())
        return Fernet(formatted_key)

    # API

    async def send(self, channel, message):
        """Send the message to the channel."""
        # Typecheck
        assert isinstance(message, dict), 'message is not a dict'
        assert self.valid_channel_name(channel), 'Channel name is not valid'
        # Make sure the message does not contain reserved keys
        assert '__asgi_channel__' not in message
        return await self._schedule_until_success(
            SEND, self._apply_channel_prefix(channel), message
        )

    async def receive(self, channel):
        """Receive one message from the channel."""
        fail_msg = 'Channel name %s is not valid' % channel
        assert self.valid_channel_name(channel), fail_msg
        return await self._schedule_until_success(
            RECEIVE, self._apply_channel_prefix(channel)
        )

    async def new_channel(self, prefix='specific'):
        """Create new single reader channel."""
        return '%s.%s!%s' % (
            prefix,
            self.client_prefix,
            ''.join(random.choice(string.ascii_letters) for i in range(20)),
        )

    async def group_add(self, group, channel):
        """Add channel to the group."""

        assert self.valid_group_name(group), 'Group name is not valid'
        assert self.valid_channel_name(channel), 'Channel name is not valid'
        return await self._schedule_until_success(
            GROUP_ADD,
            self._apply_group_prefix(group),
            self._apply_channel_prefix(channel),
        )

    async def group_discard(self, group, channel):
        """Remove the channel from the group."""

        assert self.valid_group_name(group), 'Group name is not valid'
        assert self.valid_channel_name(channel), 'Channel name is not valid'
        return await self._schedule_until_success(
            GROUP_DISCARD,
            self._apply_group_prefix(group),
            self._apply_channel_prefix(channel),
        )

    async def group_send(self, group, message):
        """Send the message to the group."""

        assert self.valid_group_name(group), 'Group name is not valid'
        prefixed_group = self._apply_group_prefix(group)
        while True:
            future = self.thread.schedule(
                SEND_GROUP, prefixed_group, message
            )
            try:
                return await wrap_future(future)
            except ChannelClosed as e:
                # Channel was closed because corresponding group exchange
                # does not exist yet.  This mean no one call `group_add`
                # yet, so group is empty and we should not worry about.
                if e.args[0] == 404 and f"'{prefixed_group}'" in e.args[1]:
                    return

    async def _schedule_until_success(self, message, *args):
        """
        Since schedule method asynchronously waits it may fail due to a
        ChannelClosed exception caused by another concurrent call.
        We should retry until we get a response or the coroutine is
        cancelled.
        """

        while True:
            future = self.thread.schedule(message, *args)
            try:
                return await wrap_future(future)
            except ChannelClosed:
                pass

    def _apply_channel_prefix(self, channel):
        new_name = _apply_channel_prefix(self.prefix, channel)
        # https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf
        # Short strings are limited to 255 octets and can be
        # parsed with no risk of buffer overflows.
        assert len(new_name) <= 255
        return new_name

    def _apply_group_prefix(self, group):
        new_name = _apply_group_prefix(self.prefix, group)
        # https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf
        # Short strings are limited to 255 octets and can be
        # parsed with no risk of buffer overflows.
        assert len(new_name) <= 255
        return new_name
