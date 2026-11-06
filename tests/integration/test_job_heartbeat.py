"""Integration tests for job heartbeat functionality."""

from __future__ import annotations

import logging

import pytest

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.commons import DequeueStrategy

from .tasks import noop
from .tasks import slow_async
from .tasks import clear_callback_log
from .conftest import make_worker


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestJobHeartbeat:
    async def test_touch_updates_timestamp(self, app):
        import psycopg

        job = await app.enqueue(noop)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET status='active', touched_at='2000-01-01' WHERE id=%s").format(
                    jobs=app._t["jobs"]
                ),
                (job.id,),
            )
        await app.touch_job(job.id)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("SELECT touched_at FROM {jobs} WHERE id=%s").format(jobs=app._t["jobs"]),
                (job.id,),
            )
            row = await cur.fetchone()
        assert row[0].year > 2000

    async def test_job_heartbeat_updates_touched_at(self, app):
        """A long-running job with heartbeat_secs should update touched_at periodically."""
        import psycopg

        job = await app.enqueue(slow_async, seconds=0.4, _heartbeat=1)

        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("SELECT touched_at FROM {jobs} WHERE id = %s").format(jobs=app._t["jobs"]),
                (job.id,),
            )
            before = (await cur.fetchone())[0]

        await make_worker(app, sweep_interval=9999).run()

        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("SELECT touched_at FROM {jobs} WHERE id = %s").format(jobs=app._t["jobs"]),
                (job.id,),
            )
            after = (await cur.fetchone())[0]

        assert after is not None
        if before is not None:
            assert after > before

    async def test_lease_renewal_error_does_not_kill_job(self, app, caplog, monkeypatch):
        """A failed renewal is logged and retried; it doesn't cancel the job."""
        job = await app.enqueue(slow_async, seconds=0.8, _heartbeat=1)

        repo = app._worker_repo
        original = repo.renew_leases
        call_count = 0

        async def flaky_renew(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("renewal error")
            return await original(*args, **kwargs)

        monkeypatch.setattr(repo, "renew_leases", flaky_renew)

        with caplog.at_level(logging.WARNING, logger="pgwerk.worker.base"):
            await make_worker(app).run()

        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert call_count >= 2
        assert any("lease renewal error" in r.message for r in caplog.records)


class TestLeaseLoss:
    async def test_job_that_lost_its_lease_is_cancelled_and_writes_nothing(self, app, caplog):
        import asyncio
        import uuid

        import psycopg

        job = await app.enqueue(slow_async, seconds=5, _heartbeat=1)
        worker = make_worker(app)
        task = asyncio.create_task(worker.run())
        try:
            for _ in range(100):
                row = await app.get_job(job.id)
                if row.status == JobStatus.Active:
                    break
                await asyncio.sleep(0.05)
            original_lease = row.lease_id

            # Another attempt takes over (as if the sweep recovered it and another worker claimed it).
            new_lease = str(uuid.uuid4())
            async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
                await conn.execute(
                    SQL("UPDATE {jobs} SET lease_id = %s WHERE id = %s").format(jobs=app._t["jobs"]),
                    (new_lease, job.id),
                )

            with caplog.at_level(logging.WARNING, logger="pgwerk.worker.base"):
                await asyncio.wait_for(task, timeout=4)  # burst worker exits once the job is cancelled
        finally:
            if not task.done():
                task.cancel()

        assert any("lost its lease" in r.message for r in caplog.records)
        row = await app.get_job(job.id)
        assert (row.status, row.lease_id) == (JobStatus.Active, new_lease)  # untouched by the old attempt
        execs = {e.id: e for e in await app.get_executions(job.id)}
        assert execs[original_lease].status.value == "running"  # not settled, not requeued

    async def test_touch_job_extends_the_lease(self, app):
        import psycopg

        job = await app.enqueue(noop)
        [claimed] = await app._worker_repo.dequeue(
            await _registered_worker(app), ["default"], 1, DequeueStrategy.Priority
        )
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET lease_expires_at = NOW() - INTERVAL '1 minute' WHERE id = %s").format(
                    jobs=app._t["jobs"]
                ),
                (job.id,),
            )
        await app.touch_job(job.id)
        row = await app.get_job(job.id)
        assert row.lease_id == claimed.lease_id
        assert row.lease_expires_at > row.started_at


async def _registered_worker(app) -> str:
    import uuid

    wid = str(uuid.uuid4())
    await app._worker_repo.register(wid, "test", ["default"], "{}")
    return wid
