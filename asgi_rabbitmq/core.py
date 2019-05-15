import base64
import hashlib
import logging
import random
import string
from contextlib import asynccontextmanager
from functools import partial

import asyncio
import aio_pika
import msgpack
from aio_pika.exchange import ExchangeType
from aiormq.exceptions import ChannelNotFoundEntity, ChannelClosed
from channels.exceptions import ChannelFull
from channels.layers import BaseChannelLayer

from .connection import ConnectionPool

logger = logging.getLogger(__name__)


def _apply_channel_prefix(prefix, channel):
    return f'{prefix}:{channel}'


def _apply_group_prefix(prefix, group):
    return f'{prefix}:group:{group}'


def _strip_channel_prefix(prefix, name):
    return name[len(_apply_channel_prefix(prefix, '')):]


def _strip_group_prefix(prefix, name):
    return name[len(_apply_group_prefix(prefix, '')):]


class Channel(object):

    dead_letters = 'dead-letters'
    expire_marker = 'expire.bind.'
    """Name of the protocol dead letters exchange and queue."""

    def __init__(self, connection, prefix, group_prefix, expiry,
                 group_expiry, get_capacity, non_local_name, crypter,
                 close_after=True):
        self.channel = connection.channel()
        self.prefix = prefix
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.group_prefix = group_prefix
        self.get_capacity = get_capacity
        self.non_local_name = non_local_name
        self.crypter = crypter
        self.close_after = close_after
        self.dead_letters = _apply_channel_prefix(prefix, self.dead_letters)
        self.expire_marker = _apply_channel_prefix(prefix, self.expire_marker)

    async def __aenter__(self):
        await self.channel.initialize()
        await self.channel.set_qos(prefetch_count=1)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.close_after:
            await self.channel.close()

    # Send.

    async def send(self, future, channel, message):
        """Start message sending.  Declare necessary queue first."""

        queue = self.non_local_name(channel)
        declare_result = await self.channel.declare_queue(
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
        await self.channel.default_exchange.publish(
            message,
            routing_key=queue,
        )

    # Receive.

    async def receive(self, channel):
        """Initiate message receive."""
        queue_name = self.non_local_name(channel)
        future = self.channel._connection.loop.create_future()
        queue = await self.channel.declare_queue(
            queue_name, arguments=self.queue_arguments,
        )
        future.add_done_callback(
            partial(self.cancel_receiving, queue)
        )
        await queue.consume(
            partial(self.consume_message, queue, future)
        )
        return await future

    async def consume_message(self, queue, future, message):
        channel = queue.name
        if 'asgi_channel' in message.headers:
            channel += message.headers['asgi_channel']
        await queue.cancel(message.consumer_tag)
        await message.ack()
        if not future.cancelled():
            future.set_result(self.deserialize(message.body))

    def cancel_receiving(self, queue, future):
        if not self.channel.is_closed:
            asyncio.create_task(queue.cancel(queue.name))

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

        group_exchange = await self.channel.declare_exchange(
            group, ExchangeType.FANOUT, auto_delete=True,
        )
        if '!' in channel:
            # Process local channels needs its own queue for
            # membership.  This queue will be bound to the group
            # exchange.
            queue = await self.channel.declare_queue(
                channel,
                arguments={
                    'x-dead-letter-exchange': self.dead_letters,
                    'x-expires': self.group_expiry * 1000,
                    'x-max-length': 0,
                },
            )
            await queue.bind(group_exchange)
        else:
            # Regular channel and single reader channels needs
            # exchange to exchange binding.  So message will be routed
            # to the queue without dead letters mechanism.
            # Declare member
            exchange = await self.channel.declare_exchange(
                channel, ExchangeType.FANOUT, auto_delete=True,
            )
            # Declare channel
            queue = await self.channel.declare_queue(
                channel, arguments=self.queue_arguments,
            )
            # Bind group
            await exchange.bind(group_exchange)
            # Bind channel
            await queue.bind(exchange)

    async def group_discard(self, group, channel):
        """Initiate member removing from the group."""

        if '!' in channel:
            queue = self.channel.QUEUE_CLASS(
                self.channel._connection, self.channel.channel, channel,
                # other options are redundant but required by constructor
                durable=False, exclusive=False, auto_delete=True,
                arguments=None)
            await queue.unbind(group)
        else:
            exchange = self.channel.EXCHANGE_CLASS(
                self.channel._connection, self.channel.channel, channel,
                ExchangeType.FANOUT,  # other options required by constructor
                auto_delete=True, durable=False, internal=False, passive=False
            )
            await exchange.unbind(group)

    async def group_send(self, group, message):
        """Publish the message to the group exchange."""

        body = self.serialize(message)
        message = aio_pika.Message(body=body, expiration=self.expiry)
        exchange = self.channel.EXCHANGE_CLASS(
            self.channel._connection, self.channel.channel, group,
            ExchangeType.FANOUT,  # other options required by constructor
            auto_delete=True, durable=False, internal=False, passive=False
        )
        await exchange.publish(message, routing_key='')

    # Dead letters processing.

    async def expire_group_member(self, group, channel):
        """
        Create the queue with group membership expiration marker message.
        """

        ttl = self.group_expiry * 1000
        await self.channel.declare_queue(
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
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=body),
            routing_key=self.get_expire_marker(group, channel)
        )

    def get_expire_marker(self, group, channel):
        """Get expire marker queue name."""

        return '%s%s.%s' % (
            self.expire_marker,
            _strip_group_prefix(self.group_prefix, group),
            _strip_channel_prefix(self.prefix, channel),
        )

    async def declare_dead_letters(self):
        """
        Initiate dead letters processing.  Declare dead letters exchange
        and queue.  Bind them together.  Start dead letter consumer.
        """

        # FIXME is it correct to create a channel here this way?
        dlx_exchange = await self.channel.declare_exchange(
            self.dead_letters,
            type=ExchangeType.FANOUT,
            auto_delete=True,
        )
        dlx_queue = await self.channel.declare_queue(
            self.dead_letters,
            arguments={
                'x-expires': max(
                    self.expiry * 2000,
                    self.group_expiry * 1000,
                ) * 2,
            },
        )
        await dlx_queue.bind(dlx_exchange)
        await dlx_queue.consume(self.on_dead_letter)

    async def on_dead_letter(self, message):
        """Consumer callback for messages from the dead letter exchange."""
        async with message.process():
            # Take the most recent death reason.
            queue = message.headers['x-death'][0]['queue'].decode('utf8')
            reason = message.headers['x-death'][0]['reason'].decode('utf8')
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
                    channel = message.headers['asgi_channel'].decode('utf8')
                    queue = queue + channel
                    await self.channel.queue_delete(queue)
                else:
                    await self.channel.exchange_delete(queue)
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


class ConnectionManager:
    """RabbitMQ connection wrapper."""

    Channel = Channel

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
        self.lock = asyncio.Lock()
        self.connection_pool = ConnectionPool(self)

    async def new_connection(self, loop):
        connection = await aio_pika.connect_robust(self.url, loop=loop)
        async with self.new_channel(connection, close_after=False) as channel:
            await channel.declare_dead_letters()
        return connection

    def new_channel(self, connection, close_after=True):
        return self.Channel(
            connection, self.prefix, self.group_prefix, self.expiry,
            self.group_expiry, self.get_capacity, self.non_local_name,
            self.crypter, close_after=close_after,
        )

    @asynccontextmanager
    async def get_connection(self):
        connection = await self.connection_pool.get()
        try:
            yield connection
        except Exception:
            await self.connection_pool.conn_error(connection)
            raise

    @asynccontextmanager
    async def get_channel(self, close_after=True):
        async with self.get_connection() as connection:
            async with self.new_channel(connection, close_after) as channel:
                yield channel


class RabbitmqChannelLayer(BaseChannelLayer):
    """
    RabbitMQ channel layer.

    It routes all messages into reote RabbitMQ server. Support for
    RabbitMQ cluster and message encryption are provided.
    """

    ConnectionManager = ConnectionManager

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
        self.url = url
        self.prefix = prefix
        self.group_prefix = prefix if group_prefix is None else group_prefix
        self.client_prefix = ''.join(
            random.choice(string.ascii_letters) for i in range(8)
        )
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.crypter = self.make_crypter(symmetric_encryption_keys)
        self.connection = self.ConnectionManager(
            self.url, self.prefix, self.group_prefix, self.expiry,
            self.group_expiry, self.get_capacity, self.non_local_name,
            self.crypter
        )

    # API

    async def send(self, channel, message):
        """Send the message to the channel."""

        # Typecheck
        assert isinstance(message, dict), 'message is not a dict'
        assert self.valid_channel_name(channel), 'Channel name is not valid'
        # Make sure the message does not contain reserved keys
        assert '__asgi_channel__' not in message

        return await self._schedule_until_success(
            'send', self._apply_channel_prefix(channel), message,
        )

    async def receive(self, channel):
        """Receive one message from the channel."""

        fail_msg = 'Channel name %s is not valid' % channel
        assert self.valid_channel_name(channel), fail_msg

        return await self._schedule_until_success(
            'receive', self._apply_channel_prefix(channel),
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
            'group_add',
            self._apply_group_prefix(group),
            self._apply_channel_prefix(channel),
        )

    async def group_discard(self, group, channel):
        """Remove the channel from the group."""

        assert self.valid_group_name(group), 'Group name is not valid'
        assert self.valid_channel_name(channel), 'Channel name is not valid'

        return await self._schedule_until_success(
            'group_discard',
            self._apply_group_prefix(group),
            self._apply_channel_prefix(channel),
        )

    async def group_send(self, group, message):
        """Send the message to the group."""

        assert self.valid_group_name(group), 'Group name is not valid'

        return await self._schedule_until_success(
            'group_send', self._apply_group_prefix(group), message,
            # ChannelNotFoundEntity:
            # corresponding group exchange does not exist yet:
            # no one called `group_add` yet, so group is empty
            stop_on=(ChannelNotFoundEntity,), retry_on=None,
        )

    async def _schedule_until_success(
        self, method, *args, retry_on=(ChannelClosed,), stop_on=None
    ):
        while True:
            try:
                async with self.connection.get_channel() as channel:
                    return await getattr(channel, method)(*args)
            except stop_on or ():
                return
            except retry_on or ():
                pass

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
