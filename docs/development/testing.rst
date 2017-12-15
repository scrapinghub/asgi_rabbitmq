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

To test local channel layer only run::

    docker-compose run --rm py35dj110 tox -- -m "local"

To run quick test subset for regular channel layer run::

    docker-compose run --rm py35dj110 tox -- -m "not slow and not local"

To run test subset related to the Twisted extension run::

    docker-compose run --rm py35dj110 tox -- -m "twisted"

To rebuild specific test environment run::

    docker-compose run --rm py36dj111 tox -r --notest

If you want to run tests on your own infrastructure without docker you
can do the same with::

    tox -e py27-django111

If RabbitMQ is running on different host you have few environment
variables (like ``RABBITMQ_HOST`` and ``RABBITMQ_PORT``) to specify
this endpoint.
