Running Tests
=============

ASGI RabbitMQ has solid test suite.  If you consider to contribute to
this project you should provide tests for new functionality.  We
choose docker as our test infrastructure automation tool.

To run all tests in parallel::

    docker-compose up py27 py34 py35 py36

This will run whole test set against all supported python and django
versions in all possible combinations.  To run whole test set in one
specific environment run this command::

    docker-compose run --rm py27dj18

Inside docker we run common tox & pytest stack.  So you have ability
to run exactly one test this way::

    docker-compose run --rm py34dj19 tox -- tests/test_integration.py::IntegrationTest::test_http_request

To run quick test subset run::

    docker-compose run --rm py35dj110 tox -- -m "not slow"

To rebuild specific test environment run::

    docker-compose run --rm py36dj111 tox -r --notest

If you want to run tests on your own infrastructure without docker you
can do the same with::

    tox -e py27-django111

If RabbitMQ is running on different host you have few environment
variables (like ``RABBITMQ_HOST`` and ``RABBITMQ_PORT``) to specify
this endpoint.

Development
===========

Sometimes it's handy just jump into a live process and hack some
things.  You can run Channels infrastructure with following commands::

    docker-compose run --rm \
        -e DJANGO_SETTINGS_MODULE=testproject.settings.channels_rabbitmq \
        -e RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/%2F \
        py36dj111 \
        /code/.tox3.6.3/py36-django111/bin/daphne -v 2 testproject.asgi.rabbitmq:channel_layer

    docker-compose run --rm \
        -e DJANGO_SETTINGS_MODULE=testproject.settings.channels_rabbitmq \
        -e RABBITMQ_URL=amqp://guest:guest@rabbitmq:5672/%2F \
        py36dj111 \
        /code/.tox3.6.3/py36-django111/bin/django-admin runworker -v 3

Debugging
=========

If you have a hard time with understanding what's going on under the
hood, try ``hunter`` tracing tool::

    PYTHONHUNTER="module='asgi_rabbitmq.core', threading_support=True, actions=[CallPrinter]" daphne testproject.asgi.rabbitmq:channel_layer

Or you can run tests with hunter trace enabled::

    docker-compose run --rm -e PYTHONHUNTER="module__in=['asgi_rabbitmq.core', 'test_unit'], threading_support=True, actions=[CallPrinter]" py36dj111 tox
