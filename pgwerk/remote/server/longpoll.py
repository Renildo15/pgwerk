"""Long-poll support for remote ``claim``: wake waiting requests when work may be there.

One ``LISTEN`` connection per server process serves every waiting request, so a
request never holds a database connection while it waits. A ``NOTIFY`` on a queue
wakes **all** requests waiting on it; each retries its claim, and ``SKIP LOCKED``
makes the losers return empty cheaply. A tick every couple of seconds also wakes
queues that have runnable work. That covers ``scheduled_at`` jobs becoming due,
missed notifications, and ``listen=False`` (PgBouncer in transaction mode), where the
tick is the only signal.
"""

from __future__ import annotations

import asyncio
import logging

from typing import Any
from typing import AsyncIterator
from contextlib import asynccontextmanager

from psycopg.sql import SQL
from psycopg.sql import Identifier


logger = logging.getLogger(__name__)


class Waiters:
    def __init__(
        self,
        connect: Any,
        prefix: str,
        jobs_table: Any,
        *,
        listen: bool = True,
        tick: float = 2.0,
    ) -> None:
        self._connect = connect
        self._prefix = prefix
        self._jobs = jobs_table
        self._listen = listen
        self._tick = tick
        self._waiting: dict[str, set[asyncio.Event]] = {}
        self._tasks: list[asyncio.Task] = []
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._tasks.append(asyncio.create_task(self._tick_loop(), name="pgwerk-remote-tick"))
        if self._listen:
            self._tasks.append(asyncio.create_task(self._listen_loop(), name="pgwerk-remote-listen"))

    async def stop(self) -> None:
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    @asynccontextmanager
    async def subscribe(self, queues: list[str]) -> AsyncIterator[asyncio.Event]:
        """An event set whenever one of *queues* may have runnable work.

        Subscribe *before* trying to claim, so a notification that arrives between
        an empty claim and the wait isn't missed.
        """
        event = asyncio.Event()
        for q in queues:
            self._waiting.setdefault(q, set()).add(event)
        try:
            yield event
        finally:
            for q in queues:
                waiters = self._waiting.get(q)
                if waiters is not None:
                    waiters.discard(event)
                    if not waiters:
                        del self._waiting[q]

    def wake(self, queue: str) -> None:
        for event in self._waiting.get(queue, ()):
            event.set()

    def waiting_queues(self) -> set[str]:
        return set(self._waiting)

    async def _tick_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._tick)
            queues = list(self._waiting)
            if not queues:
                continue
            try:
                async with await self._connect() as conn:
                    cur = await conn.execute(
                        SQL("""
                            SELECT DISTINCT queue FROM {jobs}
                            WHERE queue = ANY(%(queues)s)
                              AND status IN ('queued', 'scheduled')
                              AND scheduled_at <= NOW()
                              AND (expires_at IS NULL OR expires_at > NOW())
                        """).format(jobs=self._jobs),
                        {"queues": queues},
                    )
                    for (queue,) in await cur.fetchall():
                        self.wake(queue)
            except Exception as exc:
                logger.warning("remote long-poll tick failed: %s", exc)

    async def _listen_loop(self) -> None:
        backoff = 1.0
        prefix = f"{self._prefix}:"
        while self._running:
            try:
                async with await self._connect() as conn:
                    listening: set[str] = set()
                    backoff = 1.0
                    while self._running:
                        wanted = set(self._waiting)
                        for q in wanted - listening:
                            await conn.execute(SQL("LISTEN {ch}").format(ch=Identifier(prefix + q)))
                        for q in listening - wanted:
                            await conn.execute(SQL("UNLISTEN {ch}").format(ch=Identifier(prefix + q)))
                        listening = wanted
                        # Short iterations so newly waited-on queues get LISTENed promptly.
                        async for note in conn.notifies(timeout=0.5):
                            if note.channel.startswith(prefix):
                                self.wake(note.channel[len(prefix) :])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._running:
                    return
                logger.warning("remote long-poll listener error, reconnecting in %.1fs: %s", backoff, exc)
                # Wake everyone: notifications may have been missed while disconnected.
                for queue in list(self._waiting):
                    self.wake(queue)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
