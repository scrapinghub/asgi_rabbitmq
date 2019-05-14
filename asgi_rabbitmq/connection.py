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
    Connection pool manager for the channel layer. It manages a set of
    connections for the given url and taking into account asyncio event
    loops. Copied almost as-is from django/channels_redis implementation.
    """

    def __init__(self, url):
        self.url = url
        self.conn_map = {}
        self.in_use = {}
        self.lock = asyncio.Lock()

    def _ensure_loop(self, loop):
        """
        Get connection list for the specified loop.
        """
        if loop is None:
            loop = asyncio.get_event_loop()

        if loop not in self.conn_map:
            # Swap the loop's close method with our own so we get
            # a chance to do some cleanup.
            _wrap_close(loop, self)
            self.conn_map[loop] = []

        return self.conn_map[loop], loop

    async def pop(self, loop=None):
        """
        Get a connection for the given identifier and loop.
        """
        async with self.lock:  # FIXME check if this is correct?
            conns, loop = self._ensure_loop(loop)
            if not conns:
                conns.append(await aio_pika.connect_robust(self.url, loop=loop))
            conn = conns.pop()
            self.in_use[conn] = loop
            return conn

    def push(self, conn):
        """
        Return a connection to the pool.
        """
        loop = self.in_use[conn]
        del self.in_use[conn]
        if loop is not None:
            conns, _ = self._ensure_loop(loop)
            conns.append(conn)

    def conn_error(self, conn):
        """
        Handle a connection that produced an error.
        """
        await conn.close()
        del self.in_use[conn]

    def reset(self):
        """
        Clear all connections from the pool.
        """
        self.conn_map = {}
        self.in_use = {}

    async def close_loop(self, loop):
        """
        Close all connections owned by the pool on the given loop.
        """
        if loop in self.conn_map:
            for conn in self.conn_map[loop]:
                await conn.close()
            del self.conn_map[loop]

        for k, v in self.in_use.items():
            if v is loop:
                self.in_use[k] = None

    async def close(self):
        """
        Close all connections owned by the pool.
        """
        conn_map = self.conn_map
        in_use = self.in_use
        self.reset()
        for conns in conn_map.values():
            for conn in conns:
                await conn.close()
        for conn in in_use:
            await conn.close()