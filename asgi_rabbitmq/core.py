import base64
import hashlib
import logging
import random
import string
from functools import partial

import aio_pika
import msgpack
from aio_pika.exchange import ExchangeType
from asgiref.sync import async_to_sync
from channels.exceptions import ChannelFull
from channels.layers import BaseChannelLayer

logger = logging.getLogger(__name__)


def _apply_channel_prefix(prefix, channel):
    return f'{prefix}:{channel}'


def _apply_group_prefix(prefix, group):
    return f'{prefix}:group:{group}'


def _strip_channel_prefix(prefix, name):
    return name[len(_apply_channel_prefix(prefix, '')):]


def _strip_group_prefix(prefix, name):
    return name[len(_apply_group_prefix(prefix, '')):]


class RabbitmqChannel(object):
    """ASGI implementation in the terms of AMQP channel methods."""

    dead_letters = 'dead-letters'
    expire_marker = 'expire.bind.'
    """Name of the protocol dead letters exchange and queue."""

    def __init__(self, connection, prefix, group_prefix, expiry,
                 group_expiry, get_capacity, non_local_name, crypter):
        self.connection = connection
        self.prefix = prefix
        self.group_prefix = group_prefix
        self.dead_letters = _apply_channel_prefix(prefix, self.dead_letters)
        self.expire_marker = _apply_channel_prefix(prefix, self.expire_marker)
        self.are_confirmations_enabled = False

        # FIXME Do we need the variables?
        self.messages = {}
        self.message_number = 0
        self.unresolved_futures = set()
        self.consumer_tags = {}

    async def initialize(self):
        self.amqp_channel = await self.connection.channel()
        # set prefetch count for the channel to prevent redelivery
        # amplification when the consumer stops consuming after each message
        await self.amqp_channel.set_qos(
            prefetch_count=1,
            # in RabbitMQ this is actually all consumers on the channel
            # https://www.rabbitmq.com def/consumer-prefetch.html
            all_channels=True,
        )

    # Send.

    async def send(self, future, channel, message):
        """Start message sending.  Declare necessary queue first."""

        queue = self.non_local_name(channel)
        declare_result = await self.amqp_channel.queue_declare(
            queue, arguments=self.queue_arguments,
        )
        if declare_result.message_count >= self.get_capacity(channel):
            raise ChannelFull
        body = self.serialize(message)
        await self.publish_message(channel, body)

    async def publish_message(self, channel, body):
        """Channel capacity check is done.  Publish message."""
        queue = self.non_local_name(channel)
        headers = {}
        if '!' in channel:
            headers['asgi_channel'] = channel.rsplit('!')[-1]
        message = aio_pika.Message(
            body=body,
            expiration=self.expiry,
            headers=headers,
        )
        await self.amqp_channel.default_exchange.publish(
            message,
            routing_key=queue,
        )

    # Receive.

    async def receive(self, channel):
        """Initiate message receive."""

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

        try:
            if channel in self.waiting_receivers:
                channel_receivers = self.waiting_receivers[channel]

                try:
                    while channel_receivers:
                        future = channel_receivers.popleft()

                        # Don't allow cancel after we got a message
                        if future.set_running_or_notify_cancel():
                            # Send the message to the waiting Future.
                            future.set_result(self.deserialize(body))
                            return
                finally:
                    if not channel_receivers:
                        # it might have been removed by a future callback
                        if channel in self.waiting_receivers:
                            del self.waiting_receivers[channel]
        finally:
            amqp_channel.basic_ack(method_frame.delivery_tag)

        # if we didn't resolve a future and return, save this message for later
        self.received_messages[channel].append(self.deserialize(body))

    def cancel_receiving(self, channel, future):
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

    @property
    def queue_arguments(self):
        """Channel queue declaration arguments."""

        return {
            'x-dead-letter-exchange': self.dead_letters,
            'x-expires': self.expiry * 2000,
        }

    # Groups.

    async def group_add(self, group, channel):
        """Initiate member addition to the group."""

        await self.expire_group_member(group, channel)

        if '!' in channel:
            # Process local channels needs its own queue for
            # membership.  This queue will be bound to the group
            # exchange.
            exchange = await self.amqp_channel.declare_exchange(
                group,
                exchange_type=ExchangeType.FANOUT,
                auto_delete=True,
            )
            queue = await self.amqp_channel.declare_queue(
                channel,
                arguments={
                    'x-dead-letter-exchange': self.dead_letters,
                    'x-expires': self.group_expiry * 1000,
                    'x-max-length': 0,
                },
            )
            await queue.bind(exchange)
        else:
            # Regular channel and single reader channels needs
            # exchange to exchange binding.  So message will be routed
            # to the queue without dead letters mechanism.
            group_exchange = await self.amqp_channel.declare_exchange(
                group,
                exchange_type=ExchangeType.FANOUT,
                auto_delete=True,
            )
            # Declare member
            channel_exchange = await self.amqp_channel.declare_exchange(
                channel,
                exchange_type=ExchangeType.FANOUT,
                auto_delete=True,
            )
            # Declare channel
            channel_queue = await self.amqp_channel.declare_queue(
                channel, arguments=self.queue_arguments,
            )
            # Bind group
            await group_exchange.bind(channel_exchange)
            # Bind channel
            await channel_queue.bind(channel_exchange)

    async def group_discard(self, group, channel):
        """Initiate member removing from the group."""

        if '!' in channel:
            queue = self.amqp_channel.QUEUE_CLASS(
                self.connection, self.amqp_channel, channel,
                # other options are redundant but required by constructor
                durable=False, exclusive=False, auto_delete=True,
                arguments=None)
            await queue.unbind(group)
        else:
            exchange = self.amqp_channel.EXCHANGE_CLASS(
                connection=self.connection,
                channel=self.amqp_channel,
                name=channel, type=ExchangeType.FANOUT,
            )
            await exchange.unbind(group)

    async def group_send(self, group, message):
        """Publish the message to the group exchange."""

        body = self.serialize(message)
        message = aio_pika.Message(body=body, expiration=self.expiry)
        exchange = await self.amqp_channel.declare_exchange(group)
        await exchange.publish(message, routing_key='')

    # Dead letters processing.

    async def expire_group_member(self, group, channel):
        """
        Create the queue with group membership expiration marker message.
        """

        ttl = self.group_expiry * 1000
        await self.amqp_channel.declare_queue(
            self.get_expire_marker(group, channel),
            arguments={
                'x-dead-letter-exchange': self.dead_letters,
                'x-max-length': 1,
                'x-message-ttl': ttl,
                # Give broker some time to expire message before
                # expire whole queue.
                'x-expires': ttl + 25,
            },
        )
        # The queue was created.  Push the marker
        body = self.serialize({'group': group, 'channel': channel})
        await self.amqp_channel.default_exchange.publish(
            aio_pika.Message(body=body),
            routing_key=self.get_expire_marker(group, channel)
        )

    async def on_dead_letter(self, message):
        """Consumer callback for messages from the dead letter exchange."""

        await message.ack()
        # Take the most recent death reason.
        queue = message.headers['x-death'][0]['queue']
        reason = message.headers['x-death'][0]['reason']
        if reason == 'expired' and self.is_expire_marker(queue):
            # Group membership expired.  Discard it.
            message_body = self.deserialize(message.body)
            group = message_body['group']
            channel = message_body['channel']
            await self.group_discard(group, channel)
        elif reason == 'expired' and not self.is_expire_marker(queue):
            # The message was expired in the channel.  Discard all
            # group membership for this channel.
            if '!' in queue:
                queue = queue + message.headers['asgi_channel']
                await message.channel.queue_delete(queue)
            else:
                await message.channel.exchange_delete(queue)
        elif reason == 'maxlen' and self.is_expire_marker(queue):
            # Existing group membership was updated second time.
            return
        elif reason == 'maxlen' and '!' in queue:
            # Send group method was applied to the process local
            # channel.  Redeliver message to the right queue.
            await self.publish_message(queue, message.body)

    def is_expire_marker(self, queue):
        """Check if the queue is an expiration marker."""

        return queue.startswith(self.expire_marker)

    def get_expire_marker(self, group, channel):
        """Get expire marker queue name."""

        return '%s%s.%s' % (
            self.expire_marker,
            _strip_group_prefix(self.group_prefix, group),
            _strip_channel_prefix(self.prefix, channel),
        )

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


class RabbitmqConnection(object):
    """RabbitMQ connection wrapper."""

    def __init__(self, url, prefix, group_prefix, expiry, group_expiry,
                 get_capacity, non_local_name, crypter):

        self.url = url
        self.prefix = prefix
        self.expiry = expiry
        self.group_prefix = group_prefix
        self.group_expiry = group_expiry
        self.get_capacity = get_capacity
        self.non_local_name = non_local_name
        self.crypter = crypter

    @async_to_sync
    async def initialize(self):
        """Prepare connection state."""

        self.connection = await aio_pika.robust_connect(self.url)
        await self.connection.declare_dead_letters()

    @async_to_sync
    async def declare_dead_letters(self):
        """
        Initiate dead letters processing.  Declare dead letters exchange
        and queue.  Bind them together.  Start dead letter consumer.
        """

        # FIXME is it correct to create a channel here this way?
        channel = await self.connection.channel()
        dlx_exchange = await channel.declare_exchange(
            self.dead_letters,
            type=ExchangeType.FANOUT,
            auto_delete=True,
        )
        dlx_queue = await channel.queue_declare(
            self.dead_letters,
            arguments={
                'x-expires': max(
                    self.expiry * 2000,
                    self.group_expiry * 1000,
                ) * 2,
            },
        )
        await dlx_queue.bind(dlx_exchange, "")
        await dlx_queue.consume(self.connection.on_dead_letter)

    async def get_channel(self):
        channel = RabbitmqChannel(
            connection=self.connection,
            prefix=self.prefix,
            group_prefix=self.group_prefix,
            expiry=self.expiry,
            group_expiry=self.group_expiry,
            get_capacity=self.get_capacity,
            non_local_name=self.non_local_name,
            crypter=self.crypter,
        )
        await channel.initialize()
        return channel


class RabbitmqChannelLayer(BaseChannelLayer):
    """
    RabbitMQ channel layer.

    It routes all messages into remote RabbitMQ server. Support for
    RabbitMQ cluster and message encryption are provided.
    """

    Connection = RabbitmqConnection

    extensions = ['groups']

    def __init__(
        self,
        url,
        prefix='asgi:',
        group_prefix=None,
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
        self.group_prefix = prefix if group_prefix is None else group_prefix
        self.client_prefix = ''.join(
            random.choice(string.ascii_letters) for i in range(8)
        )
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.crypter = self.make_crypter(symmetric_encryption_keys)

        self.connection = self.get_connection(url)

    @async_to_sync
    async def get_connection(self, url):
        connection = self.Connection(
            url, self.prefix, self.group_prefix, self.expiry,
            self.group_expiry, self.get_capacity, self.non_local_name,
            self.crypter
        )
        await connection.initialize()
        return connection

    # API

    async def send(self, channel, message):
        """Send the message to the channel."""
        # Typecheck
        assert isinstance(message, dict), 'message is not a dict'
        assert self.valid_channel_name(channel), 'Channel name is not valid'
        # Make sure the message does not contain reserved keys
        assert '__asgi_channel__' not in message

        rmq_channel = await self.connection.get_channel()
        return await rmq_channel.send(channel, message)

    async def receive(self, channel):
        """Receive one message from the channel."""
        fail_msg = 'Channel name %s is not valid' % channel
        assert self.valid_channel_name(channel), fail_msg

        rmq_channel = await self.connection.get_channel()
        return await rmq_channel.receive(channel)

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

        rmq_channel = await self.connection.get_channel()
        return await rmq_channel.group_add(
            self._apply_group_prefix(group),
            self._apply_channel_prefix(channel),
        )

    async def group_discard(self, group, channel):
        """Remove the channel from the group."""

        assert self.valid_group_name(group), 'Group name is not valid'
        assert self.valid_channel_name(channel), 'Channel name is not valid'

        rmq_channel = await self.connection.get_channel()
        return await rmq_channel.group_discard(
            self._apply_group_prefix(group),
            self._apply_channel_prefix(channel),
        )

    async def group_send(self, group, message):
        """Send the message to the group."""

        assert self.valid_group_name(group), 'Group name is not valid'

        rmq_channel = await self.connection.get_channel()
        return await rmq_channel.group_send(
            self._apply_group_prefix(group), message
        )

    # Crypto helpers.

    @staticmethod
    def make_crypter(symmetric_encryption_keys):
        if symmetric_encryption_keys:
            try:
                from cryptography.fernet import MultiFernet
            except ImportError:
                raise ValueError("Cannot run without 'cryptography' installed")
            sub_fernets = [
                super().make_fernet(key) for key in symmetric_encryption_keys
            ]
            return MultiFernet(sub_fernets)

    @staticmethod
    def make_fernet(key):
        """
        Given a single encryption key, returns a Fernet instance using it.
        """

        from cryptography.fernet import Fernet

        key = key.encode('utf8')
        formatted_key = base64.urlsafe_b64encode(hashlib.sha256(key).digest())
        return Fernet(formatted_key)

    # Auxiliary helpers.

    def _apply_channel_prefix(self, channel):
        new_name = _apply_channel_prefix(self.prefix, channel)
        # https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf
        # Short strings are limited to 255 octets and can be
        # parsed with no risk of buffer overflows.
        assert len(new_name) <= 255
        return new_name

    def _apply_group_prefix(self, group):
        new_name = _apply_group_prefix(self.group_prefix, group)
        # https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf
        # Short strings are limited to 255 octets and can be
        # parsed with no risk of buffer overflows.
        assert len(new_name) <= 255
        return new_name
