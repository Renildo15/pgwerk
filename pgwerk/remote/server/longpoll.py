"""Long-poll support for remote ``claim``: wake waiting requests when work may be there.

One ``LISTEN`` connection per server process serves every waiting request, so a
request never holds a database connection while it waits.

Waiting requests are grouped by the functions their worker declared (workers of one
deployment share a group). A ``NOTIFY`` on a queue wakes **one** request per group,
the one waiting longest. A request that then claims a job wakes the next one of its
group (there may be more work), so a burst fans out in a chain while one enqueue costs
one claim per group instead of one per idle worker. A tick every couple of seconds
wakes one request per group that declared a function with runnable work. That covers
``scheduled_at`` jobs becoming due, missed notifications, a woken request that lost
the race, and ``listen=False`` (PgBouncer in transaction mode), where the tick is the
only signal.
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
        listen_connect: Any = None,
    ) -> None:
        self._connect = connect
        # The LISTEN connection is held for the process's lifetime: never a pooled one.
        self._listen_connect = listen_connect or connect
        self._prefix = prefix
        self._jobs = jobs_table
        self._listen = listen
        self._tick = tick
        # {queue: {group: {event: None}}}; a group is the frozenset of declared functions
        # (None: any function). Insertion order is waiting order.
        self._waiting: dict[str, dict[frozenset[str] | None, dict[asyncio.Event, None]]] = {}
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
    async def subscribe(self, queues: list[str], functions: list[str] | None = None) -> AsyncIterator[asyncio.Event]:
        """An event set when one of *queues* may have work for these *functions*.

        Subscribe *before* trying to claim, so a notification that arrives between
        an empty claim and the wait isn't missed. Clear the event before each claim
        attempt: only a waiter whose event is clear can be picked to wake.
        """
        event = asyncio.Event()
        group = frozenset(functions) if functions is not None else None
        for q in queues:
            self._waiting.setdefault(q, {}).setdefault(group, {})[event] = None
        try:
            yield event
        finally:
            for q in queues:
                groups = self._waiting.get(q)
                if groups is None:
                    continue
                members = groups.get(group)
                if members is not None:
                    members.pop(event, None)
                    if not members:
                        del groups[group]
                if not groups:
                    del self._waiting[q]

    @staticmethod
    def _wake_one(members: dict[asyncio.Event, None]) -> bool:
        for event in members:  # the longest-waiting request that isn't already awake
            if not event.is_set():
                event.set()
                return True
        return False

    def wake(self, queue: str, function: str | None = None) -> None:
        """Wake one waiting request per group that may take work on *queue*.

        With *function* (known from the tick), only groups that declared it.
        """
        for group, members in list(self._waiting.get(queue, {}).items()):
            if function is not None and group is not None and function not in group:
                continue
            self._wake_one(members)

    def chain(self, queue: str, functions: list[str] | None = None) -> None:
        """A request of this group just claimed a job on *queue*: wake the next one."""
        group = frozenset(functions) if functions is not None else None
        members = self._waiting.get(queue, {}).get(group)
        if members:
            self._wake_one(members)

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
                            SELECT DISTINCT queue, function FROM {jobs}
                            WHERE queue = ANY(%(queues)s)
                              AND status IN ('queued', 'scheduled')
                              AND scheduled_at <= NOW()
                              AND (expires_at IS NULL OR expires_at > NOW())
                            LIMIT 1000
                        """).format(jobs=self._jobs),
                        {"queues": queues},
                    )
                    rows = await cur.fetchall()
                for queue, function in rows:
                    self.wake(queue, function)
            except Exception as exc:
                logger.warning("remote long-poll tick failed: %s", exc)

    async def _listen_loop(self) -> None:
        backoff = 1.0
        prefix = f"{self._prefix}:"
        while self._running:
            try:
                async with await self._listen_connect() as conn:
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
                    return  # type: ignore[unreachable]
                logger.warning("remote long-poll listener error, reconnecting in %.1fs: %s", backoff, exc)
                # Wake everyone: notifications may have been missed while disconnected.
                for queue in list(self._waiting):
                    self.wake(queue)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
