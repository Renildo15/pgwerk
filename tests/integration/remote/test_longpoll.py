"""The shared LISTEN connection and the tick wake waiting claims."""

from __future__ import annotations

import time
import asyncio

import pytest

from pgwerk.remote.server.longpoll import Waiters

from ..tasks import noop


@pytest.fixture
async def waiters(app):
    w = Waiters(app._connect, app.prefix, app._t["jobs"], tick=0.3)
    await w.start()
    yield w
    await w.stop()


async def _waited(event, timeout):
    try:
        await asyncio.wait_for(event.wait(), timeout)
        return True
    except asyncio.TimeoutError:
        return False


class TestWaiters:
    async def test_enqueue_wakes_a_waiter_in_under_a_second(self, app):
        waiters = Waiters(app._connect, app.prefix, app._t["jobs"], tick=60)  # tick out of the way
        await waiters.start()
        try:
            async with waiters.subscribe(["lp-notify"]) as event:
                await asyncio.sleep(0.8)  # let the listener LISTEN on the new queue
                started = time.monotonic()
                await app.enqueue(noop, _queue="lp-notify")
                assert await _waited(event, 1.0)
                assert time.monotonic() - started < 1.0
        finally:
            await waiters.stop()

    async def test_tick_wakes_when_a_scheduled_job_becomes_due(self, app, waiters):
        await app.enqueue(noop, _queue="lp-tick", _delay=1)
        async with waiters.subscribe(["lp-tick"]) as event:
            assert not await _waited(event, 0.5)  # not due yet
            assert await _waited(event, 2.0)

    async def test_tick_alone_works_without_listen(self, app):
        waiters = Waiters(app._connect, app.prefix, app._t["jobs"], listen=False, tick=0.3)
        await waiters.start()
        try:
            await app.enqueue(noop, _queue="lp-nolisten")
            async with waiters.subscribe(["lp-nolisten"]) as event:
                assert await _waited(event, 2.0)
        finally:
            await waiters.stop()

    async def test_other_queues_dont_wake_and_unsubscribe_cleans_up(self, app, waiters):
        async with waiters.subscribe(["lp-a"]) as event:
            await asyncio.sleep(0.8)
            await app.enqueue(noop, _queue="lp-b")
            assert not await _waited(event, 1.0)
        assert waiters.waiting_queues() == set()

    async def test_wakes_all_waiters_on_the_queue(self, app):
        waiters = Waiters(app._connect, app.prefix, app._t["jobs"], tick=60)
        await waiters.start()
        try:
            async with waiters.subscribe(["lp-all"]) as e1, waiters.subscribe(["lp-all"]) as e2:
                await asyncio.sleep(0.8)
                await app.enqueue(noop, _queue="lp-all")
                assert await _waited(e1, 1.0) and await _waited(e2, 1.0)
        finally:
            await waiters.stop()
