import base64
import hashlib
import random
from string import ascii_letters as ASCII

import aio_pika
import msgpack
from channels.layers import BaseChannelLayer
from channels.exchange import ExchangeType

from .connection import ConnectionPool, ConnectionContextManager


class RabbitmqChannelLayer(BaseChannelLayer):

    def __init__(self,
                 url,
                 expiry=60,
                 group_expiry=86400,
                 capacity=100,
                 channel_capacity=None,
                 receive_timeout=10,
                 symmetric_encryption_keys=None):
        super().__init__(
            expiry=expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )
        self.pool = ConnectionPool(url)

        self.channels = {}
        self.groups = {}
        self.crypter = self.make_crypter(symmetric_encryption_keys)

    async def send(self, channel, message):
        """
        Send a message onto a (general or specific) channel.
        """
        queue = self.get_queue_name(channel)
        async with self.connection() as connection:
            channel_obj = await connection.channel()
            await channel_obj.declare_queue(
                queue,
                arguments=self.queue_arguments,
            )
            # TODO we should check for channel's capacity here
            properties = self.get_publish_properties(channel)
            serialized_msg = self.serialize(message, **properties)
            # FIXME it's a way more advanced logic in the prev version
            # we should probably need a customized version to achive
            # better reliability in a similar way
            await channel_obj.default_exchange.publish(
                serialized_msg, routing_key=queue,
            )

    async def receive(self, channel):
        """
        Receives a single message off of the channel and returns it.
        """
        queue = self.get_queue_name(channel)
        async with self.connection() as connection:
            channel_obj = await connection.channel()
            queue = await channel_obj.declare_queue(
                queue, arguments=self.queue_arguments
            )
            return await queue.get()

    async def new_channel(self, prefix="specific"):
        assert prefix.endswith('?')
        random_string = "".join(random.choice(ASCII) for i in range(20))
        new_name = prefix + random_string
        async with self.connection() as connection:
            channel_obj = await connection.channel()
            await channel_obj.declare_queue(
                new_name, arguments=self.queue_arguments
            )

    # Groups extension ###

    async def group_add(self, group, channel):
        pass  # TODO

    async def group_discard(self, group, channel):
        await super().group_discard(group, channel)
        async with self.connection() as connection:
            channel_obj = await connection.channel()
            if '!' in channel:
                queue_name = self.get_queue_name(channel)
                queue = await channel_obj.declare_queue(
                    queue_name, arguments=self.queue_arguments
                )
                await queue.unbind(exchange=group)
            else:
                exchange = await channel_obj.declare_exchange(
                    name=channel, type=ExchangeType.FANOUT,
                    auto_delete=True,
                )
                await exchange.unbind(exchange=group)

    #  Auxiliary utilities

    def get_queue_name(self, channel):
        """Translate ASGI channel name to the RabbitMQ queue name."""

        if '!' in channel:
            return channel[:channel.rfind('!') + 1]
        else:
            return channel

    @property
    def queue_arguments(self):
        """Channel queue declaration arguments."""

        return {
            'x-dead-letter-exchange': 'dead-letters',
            'x-expires': self.expiry * 2000,
        }

    def get_publish_properties(self, channel=None):
        """Get publish properties based on channel name."""

        # Store local part of the process local channel in the AMQP
        # message header.
        if channel and '!' in channel:
            headers = {'asgi_channel': channel.rsplit('!')[-1]}
        else:
            headers = None
        # Specify message expiration in milliseconds.
        expiration = str(self.expiry * 1000)
        return {"headers": headers, "expiration": expiration}

    # Connection handling

    def connection(self):
        """
        Returns the correct connection for the index given.
        Lazily instantiates pools.
        """
        return ConnectionContextManager(self.pool)

    # Crypto section

    def make_crypter(self, symmetric_encryption_keys=None):
        if not symmetric_encryption_keys:
            return
        try:
            from cryptography.fernet import MultiFernet
        except ImportError:
            raise ValueError("Cannot run without 'cryptography' installed")
        sub_fernets = [
            self.make_fernet(key) for key in symmetric_encryption_keys
        ]
        return MultiFernet(sub_fernets)

    def make_fernet(self, key):
        """
        Given a single encryption key, returns a Fernet instance using it.
        """

        from cryptography.fernet import Fernet
        key = key.encode('utf8')
        formatted_key = base64.urlsafe_b64encode(hashlib.sha256(key).digest())
        return Fernet(formatted_key)

    # Serialization.

    def serialize(self, message, **properties):
        """Serialize message."""

        value = msgpack.packb(message, use_bin_type=True)
        if self.crypter:
            value = self.crypter.encrypt(value)
        return aio_pika.Message(body=value, **properties)

    def deserialize(self, message):
        """Deserialize message."""

        if self.crypter:
            message = self.crypter.decrypt(message, self.expiry + 10)
        return msgpack.unpackb(message, encoding='utf8')
