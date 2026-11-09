"""A worker that claims a job again while its previous attempt of that job still runs.

This happens when the job is requeued under a live attempt and the same worker claims
it before its next lease renewal notices: a pre-schema-8 worker's sweep requeues a long
job without a heartbeat after ``max_active_secs`` during a rolling deploy, or this
worker's renewals failed for longer than the lease TTL and maintenance recovered it.

The worker's bookkeeping used to be keyed by job id, so the two attempts got mixed up:
the stale attempt was never cancelled (its side effects ran twice), and when it ended it
removed the new attempt's lease, which then stopped being renewed and was lost.
"""

from __future__ import annotations

import asyncio

import psycopg

from psycopg.sql import SQL
from psycopg.sql import Identifier

from pgwerk.commons import JobStatus

from .tasks import controlled_async
from .conftest import make_worker


async def _wait(pred, timeout=5.0):
    for _ in range(int(timeout / 0.02)):
        if pred():
            return True
        await asyncio.sleep(0.02)
    return False


async def _requeue_under_the_running_attempt(app, job_id):
    """What a pre-schema-8 sweep does: back to queued, worker cleared, lease left as is."""
    async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
        await conn.execute(
            SQL(
                "UPDATE {jobs} SET status = 'queued', worker_id = NULL, started_at = NULL,"
                " touched_at = NULL, scheduled_at = NOW() WHERE id = %s"
            ).format(jobs=app._t["jobs"]),
            (job_id,),
        )
        await conn.execute(SQL("NOTIFY {ch}").format(ch=Identifier(f"{app.prefix}:default")))


class TestReclaimedAttempt:
    async def test_stale_attempt_is_cancelled_and_the_new_one_finishes(self, app, async_job_control):
        control = async_job_control
        job = await app.enqueue(controlled_async)
        # No lease renewal during the test: the worker must notice from the claim itself.
        worker = make_worker(app, burst=False, heartbeat_interval=60)
        run = asyncio.create_task(worker.run())
        try:
            assert await _wait(lambda: control.calls == 1)
            first_lease = (await app.get_job(job.id)).lease_id

            await _requeue_under_the_running_attempt(app, job.id)
            assert await _wait(lambda: control.calls == 2), "the same worker should claim it again"

            # The stale attempt is cancelled at once and writes nothing.
            assert await _wait(control.cancelled.is_set), "stale attempt kept running"
            second_lease = (await app.get_job(job.id)).lease_id
            assert second_lease != first_lease
            await asyncio.sleep(0.2)
            # The stale attempt's cleanup left the new attempt's lease alone.
            assert worker._leases.get(job.id) == second_lease

            control.release.set()
            assert await _wait(lambda: control.completed >= 1)
            row = None
            for _ in range(100):
                row = await app.get_job(job.id)
                if row.status == JobStatus.Complete:
                    break
                await asyncio.sleep(0.05)
        finally:
            worker._request_shutdown()
            await asyncio.wait_for(run, timeout=10)

        assert control.completed == 1, "the handler finished more than once"
        assert row is not None and (row.status, row.lost_count) == (JobStatus.Complete, 0)
        assert worker._leases == {} and worker._active_jobs == {}
