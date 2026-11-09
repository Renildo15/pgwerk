"""A small bounded connection pool for long-running server processes.

``Werk`` opens a fresh connection per operation by default, which is fine for
workers and producers. A server process (``werk api``, especially with remote workers)
serves many concurrent requests and must not open one Postgres backend per request:
that exhausts ``max_connections`` for the whole database. :class:`ConnectionPool`
caps how many connections the process holds and reuses idle ones.

It deliberately keeps the call shape of a plain connect (``async with await connect()
as conn``), so the repositories don't change. Long-lived ``LISTEN`` connections never
come from the pool.
"""

from __future__ import annotations

import time
import asyncio
import logging

from typing import Any
from typing import Callable
from typing import Awaitable

import psycopg

from psycopg.pq import TransactionStatus


logger = logging.getLogger(__name__)


class PoolTimeout(psycopg.OperationalError):
    """No connection became free within the acquire timeout."""


class ConnectionPool:
    """At most *max_size* connections, reused while idle for less than *max_idle* seconds.

    Args:
        connect: Opens a new connection (``autocommit=True``).
        max_size: Connections this process may hold at once, busy or idle.
        acquire_timeout: Seconds to wait for a free connection before
            :class:`PoolTimeout`.
        max_idle: An idle connection older than this is closed instead of reused, so
            connections broken by a server restart don't linger.
    """

    def __init__(
        self,
        connect: Callable[[], Awaitable[Any]],
        max_size: int,
        *,
        acquire_timeout: float = 30.0,
        max_idle: float = 60.0,
    ) -> None:
        if max_size < 1:
            raise ValueError("pool max_size must be at least 1")
        self._connect = connect
        self.max_size = max_size
        self._acquire_timeout = acquire_timeout
        self._max_idle = max_idle
        self._sem: asyncio.Semaphore | None = None
        self._idle: list[tuple[Any, float]] = []
        self._closed = False

    async def __call__(self) -> "_Lease":
        """Borrow a connection: ``async with await pool() as conn``."""
        if self._closed:
            raise psycopg.OperationalError("connection pool is closed")
        if self._sem is None:
            self._sem = asyncio.Semaphore(self.max_size)
        try:
            await asyncio.wait_for(self._sem.acquire(), self._acquire_timeout)
        except asyncio.TimeoutError:
            raise PoolTimeout(f"no database connection free within {self._acquire_timeout:.0f}s") from None
        try:
            now = time.monotonic()
            while self._idle:
                conn, since = self._idle.pop()
                if _reusable(conn) and now - since < self._max_idle:
                    return _Lease(self, conn)
                await _close_quietly(conn)
            return _Lease(self, await self._connect())
        except BaseException:
            self._sem.release()
            raise

    async def _release(self, conn: Any, healthy: bool) -> None:
        try:
            if healthy and not self._closed and _reusable(conn):
                self._idle.append((conn, time.monotonic()))
            else:
                await _close_quietly(conn)
        finally:
            assert self._sem is not None
            self._sem.release()

    async def close(self) -> None:
        self._closed = True
        idle, self._idle = self._idle, []
        for conn, _ in idle:
            await _close_quietly(conn)

    @property
    def idle(self) -> int:
        return len(self._idle)


class _Lease:
    """What ``await pool()`` returns: an async context manager around one connection."""

    def __init__(self, pool: ConnectionPool, conn: Any) -> None:
        self._pool = pool
        self._conn = conn

    async def __aenter__(self) -> Any:
        return self._conn

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        # A connection-level failure leaves the connection unusable: don't reuse it.
        await self._pool._release(self._conn, healthy=not isinstance(exc, psycopg.OperationalError))


def _reusable(conn: Any) -> bool:
    return not conn.closed and conn.info.transaction_status == TransactionStatus.IDLE


async def _close_quietly(conn: Any) -> None:
    try:
        await conn.close()
    except Exception as exc:  # pragma: no cover - best effort
        logger.debug("pool: closing a connection failed: %s", exc)
