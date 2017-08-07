ASGI and AMQP
=============

ASGI is asynchronous server gateway interface.  Channels is build on
top of this abstraction.  AMQP is advanced message queuing protocol.
RabbitMQ is a message broker which provide AMQP 0.9.1 version.

ASGI and AMQP protocols are similar in the terms they operate on.
ASGI has channels, AMQP has queues. ASGI has groups, AMQP has
exchanges.  But there is a huge difference between those protocols
design.  AMQP broker decides on its own when to deliver message to
client.  ASGI application server wants to receive messages
when it is defined by its internal logic.

This difference dictate few design decisions of this layer.  Also with
some approximation we can say that AMQP protocol is stateful and ASGI
is stateless.

Please read ASGI_ and AMQP_ specifications before further reading.
Understanding of `RabbitMQ extensions`_ is also required.

Channels
--------

Regular channels like ``websocket.receive`` are implemented as regular
RabbitMQ queues.

Send
~~~~

Message publishing requires a queue.  We declare queue before
each message sending.  When we receive Declare.Ok response, it contains
number of messages in the queue.  This number allows us to check if
queue length exceeds channel capacity.  In this case ChannelFull
exception is raised.  Otherwise we publish message.  Expiration time
is set as message property.


Receive
~~~~~~~

We declare queue before each message sending.  This allows us prevent
404 errors when receiving from non existing channel.

In the non blocking mode we use Basic.Get operation.  Receiving
Get.Ok means that we successfully received a message.  We can stop
processing and return its context to user.  Receiving Get.Empty
means current queue contains no messages.  We should try next
queue from receive argument.  If it was last queue in the list, we
get empty response (``None`` tuple).

In the blocking mode we use Basic.Consume operation.  Since we don't
know which queue from argument list contains messages we apply
this consume operation in parallel.  First Deliver.Ok response will
got message, set acknowledgment on it and cancel all consumers.  After
that message will be delivered to the application code.

Single reader channels
----------------------

Single readers like ``http.request.body?ABCDEF`` have unique part in
their names.  These channels should be created with ``new_channel``
method.  This method generate random string and call Queue.Declare
with that name.

Process specific channels
-------------------------

Process specific channels like ``http.response.A1B2C3!D4E5F6`` are
represented by single queue which name equals to the non local part of
channel.  For example ``http.response.A1B2C3!`` will be a queue name.

Send
~~~~

Queue length check is the same as in regular channels.  When we
publish message, non local part of the channel name is stored as
``asgi_channel`` AMQP header.

Receive
~~~~~~~

Most part of the receive process is the same as with regular
channels.  When we receives message we check if it has
``asgi_channel`` header.  If so we append it to the channel name.

Groups
------

Groups are meant to be broadcast mechanism.  Basically they are
implemented as fanout exchanges.  With some additions.

Send
~~~~

To send the message to the group we need to publish it to the exchange
named after group.  We declare exchange which name equals to the group
name before each send call.

Add
~~~

Groups for regular and single reader channels are implemented as two
steps bindings.  First of all we have fanout exchange which name
equals to the group name.  Then we have intermediate exchange named
after channel we want to add to group.  Intermediate exchange name is
equal to channel.  After that we create exchange to exchange binding
between group exchange and the intermediate one.  After this we
declare queue according to the rules above and bind it to the
intermediate exchange.

Also we create marker queue which can hold only one marker message at
the time.  We push marker message to this queue on each call to the
``group_add``.

Discard
~~~~~~~

Channel can be removed from group manually calling ``group_discard``
method.  When discarding we just unbind intermediate exchange from group
exchange.

Channel should be removed from all its groups if message expires in
it.  For this reason we have message TTL and intermediate exchange.
All channel queues were declared with dead letters exchange.  Each
channel layer instance listens to dead letter queue.  When message was
dead lettered because of x-expires reason we delete intermediate
exchange.  This destroys all bindings to group exchanges.  As a result we
remove channel from all its groups.

If no one calls ``group_add`` long enough (the value of group expiry)
we need to remove channel from exact group.  For this reason we
have marker queues.  Message expiration in this queue equals to the
group expiry value.  When this message will be dead lettered because
of x-expires we will unbind intermediate exchange from group
exchange.  If this message was dead lettered because of x-maxlen, we
simply ignore this message.  This mean someone calls ``group_add``
second time.

Groups for process local channels
---------------------------------

Process local channels requires ``asgi_channel`` header.  When we send
message to group we don't know which channels are members of this
group.  Also we can't add process local queue to the exchange because
it will lead to situation where each process channel will receive
message from the group.

Add
~~~

Insisted of intermediate exchanges we create queue named exactly as
process local channel including local part.  This queue is bound to
the group exchange.  When we send message to this group exchange
routes message to this queue.  This queue has max length set to zero.
Message routed to this queue will be immediately dead lettered.  In
the dead letter consumer we will see death queue name.  This allow us
to send message into right process local queue with ``asgi_channel``
header calculated from the queue name.  Also this allows to "copy"
message into same process local queue twice.

Discard
~~~~~~~

If we decide to remove process local channel from one or all of its
groups, we need to do the same as we do for regular channels.  But
instead of intermediate exchange we operate on intermediate queue.

Resource Cleanup
----------------

* Queues for regular channels, single reader channels and process
  local channels will be removed after TTL equals message expiry x2
  since last message was sent to this channel.
* Intermediate queues for process local channels will expire after
  group expiry seconds.
* Queues for group membership marker will expire after group expiry
  seconds.
* Group exchanges are never deleted.
* Intermediate group exchanges are deleted after corresponding queue
  was cleaned up.

.. _asgi: http://channels.readthedocs.io/en/stable/asgi.html
.. _amqp: https://www.rabbitmq.com/amqp-0-9-1-reference.html
.. _rabbitmq extensions: https://www.rabbitmq.com/extensions.html
