
.. :changelog:

Changelog
---------

0.5.4 (2017-10-22)
++++++++++++++++++

- Upgrade `pika` library to the `0.11`.  Fix some framing errors.

0.5.3 (2017-08-08)
++++++++++++++++++

- Drop group membership marker on continuous `group_add` calls.
- Fix send to the empty group.

0.5.2 (2017-06-07)
++++++++++++++++++

- Don't try to start connection thread twice.

0.5.1 (2017-06-06)
++++++++++++++++++

- Fix ``AttributeError`` on Thread access.  Issue `#6`_.

0.5 (2017-05-28)
++++++++++++++++

- Resource cleanup.
- Start connection on first network operation call.

0.4.1 (2017-04-15)
++++++++++++++++++

- Add ``__version__`` variable to pass channels package compatibility
  test.

0.4 (2017-04-15)
++++++++++++++++

- New style process local channels support.  This version is
  compatible with ASGI reference >= 1.1
- Declared queues cache was introduced.  Channels worker bootstrap
  hook was removed.
- Python 3.6 and Django 1.11 compatibility.
- Connection and AMQP channels level errors are propagated to the
  caller thread.
- ``TEST_CONFIG`` support for test case mixin.

0.3 (2017-03-28)
++++++++++++++++

- Add ``RabbitmqLocalChannelLayer`` to use RabbitMQ layer together
  with IPC.
- Add ``RabbitmqLayerTestCaseMixin`` to use with Channels live server
  test case.
- Improved thread locking mechanism.
- Cryptography support.
- Layer ``channel_capacity`` option support.

0.2 (2017-01-29)
++++++++++++++++

- Significant speed improvement for layer receive method.

0.1 (2017-01-13)
++++++++++++++++

- Initial public release.

.. _#6: https://github.com/proofit404/asgi_rabbitmq/issues/6
