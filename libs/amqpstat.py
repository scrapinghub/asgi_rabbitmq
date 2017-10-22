"""AMQP benchmarking tool."""

from __future__ import print_function

import glob
import json
import logging
import os
import random
import signal
import statistics
import sys
import time
from functools import partial, wraps
from operator import itemgetter

import asgi_rabbitmq
from channels.test.liveserver import (
    ChannelLiveServerTestCase,
    DaphneProcess,
    WorkerProcess,
)
from daphne.access import AccessLogGenerator
from daphne.server import Server
from pika.spec import Basic
from tabulate import tabulate
from twisted.python.log import PythonLoggingObserver

amqp_stats = {}
layer_stats = {}
consumers = {}

BENCHMARK = os.environ.get('BENCHMARK', 'False') == 'True'
DEBUGLOG = os.environ.get('DEBUGLOG', 'False') == 'True'
PIKALOG = os.environ.get('PIKALOG', 'False') == 'True'


def maybe_monkeypatch(todir):
    """Setup benchmark if enable.[]"""

    if BENCHMARK:
        monkeypatch_all(todir)


def maybe_print_stats(fromdir):
    """Print benchmark stats if enable."""

    if BENCHMARK:
        print_stats(fromdir)


def monkeypatch_all(todir):
    """Setup benchmark."""

    monkeypatch_connection()
    monkeypatch_layer()
    monkeypatch_test_case(todir)


def monkeypatch_connection():
    """Substitute AMQP channel with benchmark measurement class."""

    asgi_rabbitmq.core.LayerConnection.Channel = DebugChannel


def monkeypatch_layer():
    """Decorate layer methods with benchmark."""

    layer = asgi_rabbitmq.core.RabbitmqChannelLayer
    layer.send = bench(layer.send)
    layer.receive = bench(layer.receive)
    layer.new_channel = bench(layer.new_channel)
    layer.group_add = bench(layer.group_add)
    layer.group_discard = bench(layer.group_discard)
    layer.send_group = bench(layer.send_group, count=True)


def monkeypatch_test_case(todir):
    """
    Setup live server test case with benchmark measurement processes.
    """

    case = ChannelLiveServerTestCase
    case.ProtocolServerProcess = partial(DebugDaphneProcess, todir)
    case.WorkerProcess = partial(DebugWorkerProcess, todir)
    # Decorate test teardown method.
    case._post_teardown = signal_first(case._post_teardown)


def percentile(values, fraction):
    """
    Returns a percentile value (e.g. fraction = 0.95 -> 95th percentile)
    """

    values = sorted(values)
    stopat = int(len(values) * fraction)
    if stopat == len(values):
        stopat -= 1
    return values[stopat]


def print_stats(fromdir):
    """Print collected statistics."""

    # Include statistics from subprocesses.
    for statfile in glob.glob('%s/*.dump' % fromdir):
        with open(statfile) as f:
            statblob = f.read()
        statdata = json.loads(statblob)
        for num, stat in enumerate([amqp_stats, layer_stats]):
            for k, v in statdata[num].items():
                if isinstance(v, list):
                    stat.setdefault(k, [])
                    stat[k].extend(v)
                else:
                    stat.setdefault(k, 0)
                    stat[k] += v
    headers = ['method', 'calls', 'mean', 'median', 'stdev', '95%', '99%']
    for num, stats in enumerate([amqp_stats, layer_stats], start=1):
        if stats:
            # Print statistic table.
            data = []
            for method, latencies in stats.items():
                if isinstance(latencies, list):
                    data.append([
                        method,
                        len(latencies),
                        statistics.mean(latencies),
                        statistics.median(latencies),
                        statistics.stdev(latencies)
                        if len(latencies) > 1 else None,
                        percentile(latencies, 0.95),
                        percentile(latencies, 0.99),
                    ])
                elif isinstance(latencies, int):
                    data.append(
                        [method, latencies, None, None, None, None, None],
                    )
                else:
                    raise Exception(
                        'Stat(%d) was currupted at method %s' % (num, method),
                    )
            data = sorted(data, key=itemgetter(1), reverse=True)
            print()
            print(tabulate(data, headers))
        else:
            print("%d) No statistic available" % num)


def save_stats(todir):
    """
    Dump collected statistic to the json file.  Used to from live
    server test case subprocesses.
    """

    statdata = [amqp_stats, layer_stats]
    statblob = json.dumps(statdata)
    path = os.path.join(todir, '%d.dump' % random.randint(0, 100))
    with open(path, 'w') as f:
        f.write(statblob)


def bench(f, count=False):
    """Collect function call duration statistics."""

    if count:
        # Just count the numbers of function calls.
        @wraps(f)
        def wrapper(*args, **kwargs):
            layer_stats.setdefault(f.__name__, 0)
            layer_stats[f.__name__] += 1
            return f(*args, **kwargs)
    else:
        # Calculate exact duration of each function call.
        @wraps(f)
        def wrapper(*args, **kwargs):

            start = time.time()
            result = f(*args, **kwargs)
            latency = time.time() - start
            layer_stats.setdefault(f.__name__, [])
            layer_stats[f.__name__] += [latency]
            return result

    return wrapper


def wrap(method, callback):
    """
    Measure the latency between request start and response callback.
    Used to measure low-level AMQP frame operations.
    """

    if callback is None:
        return

    start = time.time()

    def wrapper(*args):
        latency = time.time() - start
        amqp_stats.setdefault(method, [])
        amqp_stats[method] += [latency]
        if callback:
            callback(*args)

    return wrapper


class DebugChannel(asgi_rabbitmq.core.LayerConnection.Channel):
    """Collect statistics about RabbitMQ methods usage on channel."""

    def basic_ack(self, *args, **kwargs):

        amqp_stats.setdefault('basic_ack', 0)
        amqp_stats['basic_ack'] += 1
        return super(DebugChannel, self).basic_ack(*args, **kwargs)

    def basic_cancel(self, callback=None, *args, **kwargs):

        return super(DebugChannel, self).basic_cancel(
            wrap('basic_cancel', callback), *args, **kwargs)

    def basic_consume(self, *args, **kwargs):

        start = time.time()
        consumer_tag = super(DebugChannel, self).basic_consume(*args, **kwargs)
        consumers[consumer_tag] = start
        return consumer_tag

    def _on_eventok(self, method_frame):

        end = time.time()
        if isinstance(method_frame.method, Basic.ConsumeOk):
            start = consumers.pop(method_frame.method.consumer_tag)
            latency = end - start
            amqp_stats.setdefault('basic_consume', [])
            amqp_stats['basic_consume'] += [latency]
            return
        return super(DebugChannel, self)._on_eventok(method_frame)

    def basic_get(self, callback=None, *args, **kwargs):

        # TODO: Measure latency for Get-Empty responses.
        return super(DebugChannel, self).basic_get(
            wrap('basic_get', callback), *args, **kwargs)

    def basic_publish(self, *args, **kwargs):

        amqp_stats.setdefault('basic_publish', 0)
        amqp_stats['basic_publish'] += 1
        return super(DebugChannel, self).basic_publish(*args, **kwargs)

    def exchange_bind(self, callback=None, *args, **kwargs):

        return super(DebugChannel, self).exchange_bind(
            wrap('exchange_bind', callback), *args, **kwargs)

    def exchange_declare(self, callback=None, *args, **kwargs):

        return super(DebugChannel, self).exchange_declare(
            wrap('exchange_declare', callback), *args, **kwargs)

    def exchange_delete(self, callback=None, *args, **kwargs):

        return super(DebugChannel, self).exchange_delete(
            wrap('exchange_delete', callback), *args, **kwargs)

    def exchange_unbind(self, callback=None, *args, **kwargs):

        return super(DebugChannel, self).exchange_unbind(
            wrap('exchange_unbind', callback), *args, **kwargs)

    def queue_bind(self, callback, *args, **kwargs):

        return super(DebugChannel, self).queue_bind(
            wrap('queue_bind', callback), *args, **kwargs)

    def queue_declare(self, callback, *args, **kwargs):

        return super(DebugChannel, self).queue_declare(
            wrap('queue_declare', callback), *args, **kwargs)


class DebugDaphneProcess(DaphneProcess):
    """
    Live server test case subprocess which dumps benchmark statistics
    to the json file before exit.
    """

    def __init__(self, todir, *args):

        self.todir = todir
        super(DebugDaphneProcess, self).__init__(*args)

    def run(self):

        setup_logger('Daphne')
        monkeypatch_all(self.todir)
        signal.signal(signal.SIGCHLD, partial(at_exit, self.todir))
        super(DebugDaphneProcess, self).run()


class DebugWorkerProcess(WorkerProcess):
    """
    Live server test case subprocess which dumps benchmark statistics
    to the json file before exit.
    """

    def __init__(self, todir, *args):

        self.todir = todir
        super(DebugWorkerProcess, self).__init__(*args)

    def run(self):

        setup_logger('Worker')
        monkeypatch_all(self.todir)
        signal.signal(signal.SIGCHLD, partial(at_exit, self.todir))
        super(DebugWorkerProcess, self).run()


def signal_first(method):
    """Decorate function call with test subprocess teardown."""

    def decorated_method(self):

        os.kill(self._server_process.pid, signal.SIGCHLD)
        os.kill(self._worker_process.pid, signal.SIGCHLD)
        time.sleep(0.1)
        method(self)

    return decorated_method


def at_exit(todir, signum, frame):
    """Save statistics to the file."""

    if BENCHMARK:
        save_stats(todir)


def setup_logger(name):
    """Enable debug logging."""

    if DEBUGLOG:
        logging.basicConfig(
            level=logging.DEBUG,
            format=name + ' %(asctime)-15s %(levelname)-8s %(message)s',
        )
        disabled_loggers = []
        if not PIKALOG:
            disabled_loggers.append('pika')
        for logger in disabled_loggers:
            logging.getLogger(logger).setLevel(logging.WARNING)
        new_defaults = list(Server.__init__.__defaults__)
        # NOTE: Patch `action_logger` argument default value.
        new_defaults[6] = AccessLogGenerator(sys.stdout)
        Server.__init__.__defaults__ = tuple(new_defaults)
        observer = PythonLoggingObserver(loggerName='twisted')
        observer.start()
