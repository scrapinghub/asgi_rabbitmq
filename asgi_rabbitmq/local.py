from .core import RabbitmqChannelLayer


class RabbitmqLocalChannelLayer(RabbitmqChannelLayer):
    """
    Variant of the RabbitMQ channel layer that also uses a
    local-machine channel layer instance to route all
    non-machine-specific messages to a local machine, while using the
    RabbitMQ backend for all machine-specific messages and group
    management/sends.

    This allows the majority of traffic to go over the local layer for
    things like http.request and websocket.receive, while still
    allowing Groups to broadcast to all connected clients and keeping
    reply_channel names valid across all workers.
    """

    def __init__(self,
                 url,
                 expiry=60,
                 group_expiry=86400,
                 capacity=100,
                 channel_capacity=None,
                 symmetric_encryption_keys=None,
                 prefix='asgi'):

        # Initialise the base class.
        super(RabbitmqLocalChannelLayer, self).__init__(
            url,
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
            symmetric_encryption_keys=symmetric_encryption_keys,
        )
        # Set up our local transport layer as well.
        try:
            from asgi_ipc import IPCChannelLayer
        except ImportError:
            raise ValueError(
                'You must install asgi_ipc to use the local variant',
            )
        self.local_layer = IPCChannelLayer(
            prefix,
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )

    def send(self, channel, message):
        """Send the message to the channel."""

        # If the channel is "normal", use IPC layer, otherwise use
        # RabbitMQ layer.
        if "!" in channel or "?" in channel:
            return super(RabbitmqLocalChannelLayer, self).send(
                channel,
                message,
            )
        else:
            return self.local_layer.send(channel, message)

    def receive(self, channels, block=False):
        """Receive one message from one of the channels."""

        # Work out what kinds of channels are in there.
        num_remote = len([ch for ch in channels if "!" in ch or "?" in ch])
        num_local = len(channels) - num_remote
        # If they mixed types, force nonblock mode and query both
        # backends, local first.
        if num_local and num_remote:
            result = self.local_layer.receive(channels, block=False)
            if result[0] is not None:
                return result
            return super(RabbitmqLocalChannelLayer, self).receive(
                channels,
                block,
            )
        # If they just did one type, pass off to that backend.
        elif num_local:
            return self.local_layer.receive(channels, block)
        else:
            return super(RabbitmqLocalChannelLayer, self).receive(
                channels,
                block,
            )

    # `new_channel` always goes to RabbitMQ as it's always remote
    # channels.  Group APIs always go to RabbitMQ too.
