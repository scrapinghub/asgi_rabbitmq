import subprocess
import sys
import time

import benchmark
import pytest
import requests
import websocket
from asgi_rabbitmq.test import RabbitmqLayerTestCaseMixin
from channels.test import ChannelLiveServerTestCase

pytestmark = pytest.mark.slow


class IntegrationTest(RabbitmqLayerTestCaseMixin, ChannelLiveServerTestCase):

    def test_http_request(self):
        """Test the ability to send http requests and receive responses."""

        response = requests.get(self.live_server_url)
        assert response.status_code == 200

    def test_websocket_message(self):
        """Test the ability to send and receive messages over WebSocket."""

        ws = websocket.create_connection(self.live_server_ws_url)
        ws.send('test')
        response = ws.recv()
        ws.close()
        assert 'test' == response

    def test_benchmark(self):
        """Run channels benchmark test suite."""

        proc = subprocess.Popen([
            sys.executable,
            benchmark.__file__,
            self.live_server_ws_url,
        ])
        for _ in range(0, 90, 5):
            time.sleep(5)
            if proc.returncode:
                break
        else:
            proc.terminate()
            proc.wait()
        assert proc.returncode == 0


# FIXME: This will mark all `IntegrationTest` subclasses as `local`.
@pytest.mark.local
class LocalIntegrationTest(IntegrationTest):

    local = True


class ConcurrentIntegrationTest(IntegrationTest):

    worker_threads = 4


@pytest.mark.local
class LocalConcurrentIntegrationTest(IntegrationTest):

    local = True
    worker_threads = 4
