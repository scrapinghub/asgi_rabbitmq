from os import environ
from random import choice
from string import ascii_letters

from channels.test.base import ChannelTestCaseMixin
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.test.utils import override_settings
from rabbitmq_admin import AdminAPI


class RabbitmqLayerTestCaseMixin(object):
    """
    TestCase mixin for Django tests.

    Allow to test your channels project against real broker.  Provide
    isolated virtual host for each test.
    """

    local = False

    def _pre_setup(self):
        """Create RabbitMQ virtual host."""

        # Check if we are mixed with the class that makes sense.
        if ChannelTestCaseMixin in self.__class__.__mro__:
            raise ImproperlyConfigured(
                'ChannelTestCase is not allowed as base class for '
                'RabbitmqLayerTestCaseMixin'
            )
        # Create new virtual host for this test and grant permissions
        # to it.
        hostname = environ.get('RABBITMQ_HOST', 'localhost')
        port = environ.get('RABBITMQ_PORT', '5672')
        user = environ.get('RABBITMQ_USER', 'guest')
        password = environ.get('RABBITMQ_PASSWORD', 'guest')
        management_port = environ.get('RABBITMQ_MANAGEMENT_PORT', '15672')
        management_url = 'http://%s:%s' % (hostname, management_port)
        self.virtual_host = ''.join(choice(ascii_letters) for i in range(8))
        self.amqp_url = 'amqp://%s:%s/%s' % (hostname, port, self.virtual_host)
        self.management = AdminAPI(management_url, (user, password))
        self.management.create_vhost(self.virtual_host)
        self.management.create_user_permission(user, self.virtual_host)
        # Substitute Django settings with this virtual host.
        if self.local:
            layer_class_name = 'asgi_rabbitmq.RabbitmqLocalChannelLayer'
        else:
            layer_class_name = 'asgi_rabbitmq.RabbitmqChannelLayer'
        self._overridden_settings = {
            'CHANNEL_LAYERS': {
                'default': {
                    'BACKEND': layer_class_name,
                    'ROUTING': settings.CHANNEL_LAYERS['default']['ROUTING'],
                    'CONFIG': settings.CHANNEL_LAYERS['default']['CONFIG'],
                    'TEST_CONFIG': {'url': self.amqp_url},
                }
            }
        }
        self._self_overridden_context = override_settings(**self._overridden_settings)
        self._self_overridden_context.enable()
        super(RabbitmqLayerTestCaseMixin, self)._pre_setup()

    def _post_teardown(self):
        """Remove RabbitMQ virtual host."""

        super(RabbitmqLayerTestCaseMixin, self)._post_teardown()
        # Disable overridden settting.
        self._self_overridden_context.disable()
        delattr(self, '_self_overridden_context')
        self._overridden_settings = None
        # Remove temporal virtual host.
        self.management.delete_vhost(self.virtual_host)
        # Cleanup test instance.
        del self.virtual_host
        del self.amqp_url
        del self.management
