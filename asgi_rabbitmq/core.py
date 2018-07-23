import base64
import hashlib
import logging
import random
import time
from functools import partial
from string import ascii_letters as ASCII

import msgpack
from asgiref.base_layer import BaseChannelLayer
from cached_property import threaded_cached_property
from pika import SelectConnection, URLParameters
from pika.channel import Channel
from pika.exceptions import ChannelClosed, ConnectionClosed
from pika.spec import Basic, BasicProperties, Confirm

try:
    from concurrent.futures import Future
except ImportError:
    from futures import Future

try:
    from threading import Thread, Lock, Event, get_ident
except ImportError:
    from threading import Thread, Lock, Event, _get_ident as get_ident

try:
    from twisted.internet import defer, reactor
    TWISTED_AVAILABLE = True
except ImportError:
    TWISTED_AVAILABLE = False

logger = logging.getLogger(__name__)

SEND = 0
RECEIVE = 1
NEW_CHANNEL = 2
GROUP_ADD = 3
GROUP_DISCARD = 4
SEND_GROUP = 5
DECLARE_DEAD_LETTERS = 6
EXPIRE_GROUP_MEMBER = 7
RECEIVE_TWISTED = 8


class Protocol(object):
    """ASGI implementation in the terms of AMQP channel methods."""

    dead_letters = 'dead-letters'
    """Name of the protocol dead letters exchange and queue."""

    def __init__(self, channel_factory, expiry, group_expiry, get_capacity,
                 crypter, auto_close=False):
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.get_capacity = get_capacity
        self.crypter = crypter
        self.auto_close = auto_close
        # Mapping for the connection schedule method.
        self.methods = {
            SEND: self.send,
            RECEIVE: self.receive,
            NEW_CHANNEL: self.new_channel,
            GROUP_ADD: self.group_add,
            GROUP_DISCARD: self.group_discard,
            SEND_GROUP: self.send_group,
            DECLARE_DEAD_LETTERS: self.declare_dead_letters,
            RECEIVE_TWISTED: self.receive_twisted,
        }
        self.waiting_calls = []
        self.messages = {}
        self.message_number = 0
        self.confirmed_message_number = -1
        self.are_confirmations_enabled = False

        self.amqp_channel = channel_factory(self.apply_waiting)
        # Handle AMQP channel level errors with protocol.
        self.amqp_channel.on_callback_error_callback = self.protocol_error

    # Utilities.

    def apply(self, method_id, args, kwargs, future):
        """Take method from the mapping and call it."""

        if self.is_open:
            if self.auto_close:
                future.add_done_callback(
                    lambda method_frame: self.amqp_channel.close(),
                )
            self.methods[method_id](future, *args, **kwargs)
        else:
            self.waiting_calls.append((method_id, args, kwargs, future))

    def apply_waiting(self, unused_channel):
        if self.is_open:
            for call in self.waiting_calls:
                self.apply(*call)
            self.waiting_calls.clear()

    def protocol_error(self, error):
        """
        AMQP channel level error callback.  Pass error to the waiting
        thread.
        """

        for future in self.messages.values():
            future.set_exception(error)

    def get_queue_name(self, channel):
        """Translate ASGI channel name to the RabbitMQ queue name."""

        if '!' in channel:
            return channel[:channel.rfind('!') + 1]
        else:
            return channel

    @property
    def is_open(self):
        return self.amqp_channel.is_open

    @property
    def is_closed(self):
        return self.amqp_channel.is_closing or self.amqp_channel.is_closed

    # Send.

    def send(self, future, channel, message):
        """Start message sending.  Declare necessary queue first."""

        queue = self.get_queue_name(channel)
        self.amqp_channel.queue_declare(
            partial(self.handle_publish, future, channel, message),
            queue=queue,
            arguments=self.queue_arguments,
        )

    def handle_publish(self, future, channel, message, method_frame):
        """Queue declared.  Check channel capacity."""

        if method_frame.method.message_count >= self.get_capacity(channel):
            future.set_exception(RabbitmqChannelLayer.ChannelFull())
            return
        body = self.serialize(message)
        self.publish_message(future, channel, body)

    def publish_message(self, future, channel, body):
        """Channel capacity check is done.  Publish message."""
        queue = self.get_queue_name(channel)
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

    def receive(self, future, channels, block):
        """Initiate message receive."""

        # Declare necessary queues in parallel.
        queues = set(map(self.get_queue_name, channels))
        known_queues = set()
        queues_declared = partial(self.queues_declared, future, queues,
                                  known_queues, channels, block)
        for queue in queues:
            self.amqp_channel.queue_declare(
                queues_declared,
                queue,
                arguments=self.queue_arguments,
            )

    def queues_declared(self, future, queues, known_queues, channels, block,
                        method_frame):
        """One queue was declared.  Continue if it was the last one."""

        known_queues.add(method_frame.method.queue)
        # If all queues are known at this moment, that basically means
        # we are in the last callback and can safely go further.  If
        # any queue isn't known, we simply skip processing at this
        # point.
        unknown_queues = queues - known_queues
        if unknown_queues:
            return
        if block:
            self.start_blocking_receive(future, channels)
        else:
            channels = list(channels)  # Daphne sometimes pass dict.keys()
            self.start_non_blocking_receive(future, channels)

    # Blocking receive.

    def start_blocking_receive(self, future, channels):
        """Create parallel consumers in one AMQP channel."""

        # In blocking mode create consumers in parallel.
        consumer_tags = {}
        for channel in channels:
            tag = self.amqp_channel.basic_consume(
                partial(self.consume_message, future, consumer_tags),
                queue=self.get_queue_name(channel),
            )
            consumer_tags[tag] = channel

    def consume_message(self, future, consumer_tags, amqp_channel,
                        method_frame, properties, body):

        amqp_channel.basic_ack(method_frame.delivery_tag)
        # Cancel parallel consumers.
        for tag in consumer_tags:
            amqp_channel.basic_cancel(consumer_tag=tag)
        # Send the message to the waiting thread.
        channel = consumer_tags[method_frame.consumer_tag]
        if properties.headers and 'asgi_channel' in properties.headers:
            channel = channel + properties.headers['asgi_channel']
        message = self.deserialize(body)
        future.set_result((channel, message))

    # Non blocking receive.

    def start_non_blocking_receive(self, future, channels):
        """Sequentially check each channel queue."""

        # In the non-blocking mode get the message from each queue
        # in series.
        channel = channels[0]
        no_message = partial(self.no_message, future, channels[1:])
        self.amqp_channel.add_callback(no_message, [Basic.GetEmpty])
        self.amqp_channel.basic_get(
            partial(self.get_message, future, channel, no_message),
            queue=self.get_queue_name(channel),
        )

    def get_message(self, future, channel, no_message, amqp_channel,
                    method_frame, properties, body):
        """Message was received in the non-blocking mode."""

        # Cancel no message callback for this queue.
        amqp_channel.callbacks.remove(
            amqp_channel.channel_number,
            Basic.GetEmpty,
            no_message,
        )
        # Send the message to the waiting thread.
        amqp_channel.basic_ack(method_frame.delivery_tag)
        if properties.headers and 'asgi_channel' in properties.headers:
            channel = channel + properties.headers['asgi_channel']
        message = self.deserialize(body)
        future.set_result((channel, message))

    def no_message(self, future, channels, method_frame):
        """
        The queue is empty.  Tries to read from next channel in
        non-blocking mode.
        """

        if channels:
            self.start_non_blocking_receive(future, channels)
        else:
            future.set_result((None, None))

    @property
    def queue_arguments(self):
        """Channel queue declaration arguments."""

        return {
            'x-dead-letter-exchange': self.dead_letters,
            'x-expires': self.expiry * 2000,
        }

    # Twisted receive.

    def receive_twisted(self, future, channels):

        self.receive(future, channels, block=True)

    # New channel.

    def new_channel(self, future, new_name):
        """Initiate new single reader channel creation."""

        self.amqp_channel.queue_declare(
            partial(self.new_channel_declared, future),
            queue=new_name,
            arguments=self.queue_arguments,
        )

    def new_channel_declared(self, future, method_frame):
        """Notify waiting thread new single reader channel was created."""

        future.set_result(None)

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

                if '?' in channel:
                    bind_group(None)
                else:
                    self.amqp_channel.queue_declare(
                        bind_group,
                        queue=self.get_queue_name(channel),
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
        """
        Declare group exchange.  Pass execution to the group declared
        callback.
        """

        self.amqp_channel.exchange_declare(
            partial(self.group_declared, future, group, message),
            exchange=group,
            exchange_type='fanout',
        )

    def group_declared(self, future, group, message, method_frame):
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

        return 'expire.bind.%s.%s' % (group, channel)

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
            )

        self.amqp_channel.exchange_declare(
            declare_queue,
            exchange=self.dead_letters,
            exchange_type='fanout',
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

        return queue.startswith('expire.bind.')

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
        return msgpack.unpackb(message, encoding='utf8')


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

    def __init__(self, url, expiry, group_expiry, get_capacity, crypter):

        self.url = url
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.get_capacity = get_capacity
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
            logger.warning('Exception when notifying threads about closed '
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
            self.protocols[ident] = self.open_amqp_channel()
        # We have running AMQP channel for current given thread.
        # Apply protocol method directly.
        self.protocols[ident].apply(*method)

    def open_amqp_channel(self, single_use=False):
        """
        Open new AMQP Channel and associate new `Protocol` instance with
        it.
        """

        return self.Protocol(
            channel_factory=self.connection.channel,
            expiry=self.expiry,
            group_expiry=self.group_expiry,
            get_capacity=self.get_capacity,
            crypter=self.crypter,
            auto_close=single_use,
        )

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

    def twisted_schedule(self, f, *args, **kwargs):
        """
        Try to acquire connection access lock.  Then create new protocol
        instance for one Twisted deferred object resolution.
        """

        self.wait_open()
        future = Future()
        with self.lock:
            protocol = self.open_amqp_channel(single_use=True)
            protocol.apply(f, args, kwargs, future)
        return future


class ConnectionThread(Thread):
    """
    Thread holding connection.

    Separate heartbeat frames processing from actual work.
    """

    Connection = RabbitmqConnection

    def __init__(self, url, expiry, group_expiry, get_capacity, crypter):

        super(ConnectionThread, self).__init__()
        self.daemon = True
        self.connection = self.Connection(url, expiry, group_expiry,
                                          get_capacity, crypter)

    def run(self):
        """Start connection thread."""

        self.connection.run()

    def schedule(self, f, *args, **kwargs):
        """
        Schedule protocol method execution in the context of the
        connection thread.
        """

        return self.connection.schedule(f, *args, **kwargs)

    def twisted_schedule(self, f, *args, **kwargs):
        """
        Schedule protocol method execution in the context of the
        connection thread which will resolve twisted deferred object
        in the end.
        """

        return self.connection.twisted_schedule(f, *args, **kwargs)


class RabbitmqChannelLayer(BaseChannelLayer):
    """
    RabbitMQ channel layer.

    It routes all messages into remote RabbitMQ server.  Support for
    RabbitMQ cluster and message encryption are provided.  Only
    synchronous approach is implemented.
    """

    extensions = ['groups']

    Thread = ConnectionThread

    def __init__(self,
                 url,
                 expiry=60,
                 group_expiry=86400,
                 capacity=100,
                 channel_capacity=None,
                 symmetric_encryption_keys=None):

        super(RabbitmqChannelLayer, self).__init__(
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )
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
        # Connection thread will be started on first method access.
        self._thread = self.Thread(url, expiry, group_expiry,
                                   self.get_capacity, crypter)

    @threaded_cached_property
    def thread(self):
        """
        Connection thread.  Holds connection heartbeats.  Ensure that
        thread is started.
        """

        self._thread.start()
        return self._thread

    def make_fernet(self, key):
        """
        Given a single encryption key, returns a Fernet instance using it.
        """

        from cryptography.fernet import Fernet
        key = key.encode('utf8')
        formatted_key = base64.urlsafe_b64encode(hashlib.sha256(key).digest())
        return Fernet(formatted_key)

    def send(self, channel, message):
        """Send the message to the channel."""

        assert self.valid_channel_name(channel), 'Channel name is not valid'
        return self._schedule(SEND, channel, message)

    def receive(self, channels, block=False):
        """Receive one message from one of the channels."""

        for channel in channels:
            fail_msg = 'Channel name %s is not valid' % channel
            assert self.valid_channel_name(channel, receive=True), fail_msg
        return self._schedule(RECEIVE, channels, block)

    def new_channel(self, pattern):
        """Create new single reader channel."""

        assert pattern.endswith('?')
        random_string = "".join(random.choice(ASCII) for i in range(20))
        new_name = pattern + random_string
        self._schedule(NEW_CHANNEL, new_name)
        return new_name

    def group_add(self, group, channel):
        """Add channel to the group."""

        assert self.valid_group_name(group), 'Group name is not valid'
        assert self.valid_channel_name(channel), 'Channel name is not valid'
        return self._schedule(GROUP_ADD, group, channel)

    def group_discard(self, group, channel):
        """Remove the channel from the group."""

        assert self.valid_group_name(group), 'Group name is not valid'
        assert self.valid_channel_name(channel), 'Channel name is not valid'
        return self._schedule(GROUP_DISCARD, group, channel)

    def send_group(self, group, message):
        """Send the message to the group."""

        assert self.valid_group_name(group), 'Group name is not valid'
        try:
            return self._schedule(SEND_GROUP, group, message)
        except ChannelClosed:
            # Channel was closed because corresponding group exchange
            # does not exist yet.  This mean no one call `group_add`
            # yet, so group is empty and we should not worry about.
            pass

    def _schedule(self, f, *args, **kwargs):
        """Schedule action and wait for result with retrying logic."""

        while True:
            try:
                future = self.thread.schedule(f, *args, *kwargs)
                return future.result()
            except ConnectionClosed:
                delay = self.thread.connection.parameters.retry_delay
                logger.warning(
                    'Disconnected, retrying after %s seconds', delay)
                time.sleep(delay)

    if TWISTED_AVAILABLE:

        extensions.append('twisted')

        @defer.inlineCallbacks
        def receive_twisted(self, channels):
            """Twisted-native implementation of receive."""

            deferred = defer.Deferred()

            def resolve_deferred(future):

                reactor.callFromThread(deferred.callback, future.result())

            future = self.thread.twisted_schedule(RECEIVE_TWISTED, channels)
            future.add_done_callback(resolve_deferred)
            defer.returnValue((yield deferred))


# TODO: Is it optimal to read bytes from content frame, call python
# decode method to convert it to string and than parse it with
# msgpack?  We should minimize useless work on message receive.
