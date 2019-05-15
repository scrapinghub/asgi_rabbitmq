import types
import asyncio

import aio_pika


def _wrap_close(loop, pool):
    """
    Decorate an event loop's close method with our own.
    """
    original_impl = loop.close

    def _wrapper(self, *args, **kwargs):
        # If the event loop was closed, there's nothing we can do anymore.
        if not self.is_closed():
            self.run_until_complete(pool.close_loop(self))
        # Restore the original close() implementation after we're done.
        self.close = original_impl
        return self.close(*args, **kwargs)

    loop.close = types.MethodType(_wrapper, loop)


class ConnectionPool:
    """
    Connection pool manager for the channel layer. It manages a connection
    perfor the given url and taking into account asyncio event
    loops. Copied almost as-is from django/channels_redis implementation.
    """

    def __init__(self, manager):
        self.manager = manager
        self.conn_map = {}
        self.lock = asyncio.Lock()

    async def get(self, loop=None):
        """
        Get a connection for the given identifier and loop.
        """
        async with self.lock:
            if loop is None:
                loop = asyncio.get_event_loop()

            if loop not in self.conn_map:
                # Swap the loop's close method with our own so we get
                # a chance to do some cleanup.
                _wrap_close(loop, self)
                conn = await self.manager.new_connection(loop)
                self.conn_map[loop] = conn

            return self.conn_map[loop]

    async def close_loop(self, loop):
        """
        Close all connections owned by the pool on the given loop.
        """
        async with self.lock:
            if loop in self.conn_map:
                await self.conn_map[loop].close()
                del self.conn_map[loop]

    async def close(self):
        """
        Close all connections owned by the pool.
        """
        conn_map = self.conn_map
        self.conn_map = {}
        for conns in conn_map.values():
            await conn.close()