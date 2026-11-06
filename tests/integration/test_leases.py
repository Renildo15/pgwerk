"""Leases: every attempt is fenced by its own lease id (the execution id)."""

from __future__ import annotations

import uuid

from datetime import datetime
from datetime import timezone

import psycopg

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.commons import DequeueStrategy
from pgwerk.settlement import Fence
from pgwerk.settlement import Outcome

from .tasks import noop


async def _worker(app) -> str:
    wid = str(uuid.uuid4())
    await app._worker_repo.register(wid, "test", ["default"], "{}")
    return wid


async def _claim(app, wid, lease_ttl=300):
    return await app._worker_repo.dequeue(wid, ["default"], 10, DequeueStrategy.Priority, lease_ttl=lease_ttl)


async def _age(app, job_id, **cols):
    sets = ", ".join(f"{c} = NOW() - INTERVAL '{v}'" for c, v in cols.items())
    async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
        await conn.execute(SQL(f"UPDATE {{jobs}} SET {sets} WHERE id = %s").format(jobs=app._t["jobs"]), (job_id,))


class TestClaimMintsLease:
    async def test_lease_is_the_execution_id(self, app):
        await app.enqueue(noop)
        [job] = await _claim(app, await _worker(app))

        assert job.lease_id is not None
        [execution] = await app.get_executions(job.id)
        assert execution.id == job.lease_id

    async def test_ttl_is_heartbeat_secs_or_the_local_default(self, app):
        await app.enqueue(noop)
        await app.enqueue(noop, _heartbeat=20)
        jobs = await _claim(app, await _worker(app), lease_ttl=300)
        now = datetime.now(timezone.utc)

        ttl = {j.heartbeat_secs: (j.lease_expires_at - now).total_seconds() for j in jobs}
        assert 290 < ttl[None] <= 300
        assert 10 < ttl[20] <= 20

    async def test_run_sync_job_is_leased_and_settles(self, app):
        done = await app.enqueue(noop, _sync=True)
        assert done.status == JobStatus.Complete
        assert done.lease_id is None  # cleared on settle
        [execution] = await app.get_executions(done.id)
        assert execution.status.value == "complete"


class TestNoSteal:
    async def test_active_job_with_stale_heartbeat_is_not_claimed(self, app):
        """Fact 2: dequeue used to steal it without counting an attempt."""
        job = await app.enqueue(noop, _heartbeat=5)
        await _claim(app, await _worker(app))
        await _age(app, job.id, touched_at="1 hour", started_at="1 hour")

        assert await _claim(app, await _worker(app)) == []
        row = await app.get_job(job.id)
        assert (row.status, row.attempts) == (JobStatus.Active, 1)


class TestLeaseFence:
    async def test_late_ack_of_a_swept_attempt_cannot_settle_the_next(self, app):
        """Fact 1: the same worker reclaims its own swept job; worker_id alone can't tell
        the attempts apart, the lease can."""
        job = await app.enqueue(noop, _retry=3)
        wid = await _worker(app)
        [first] = await _claim(app, wid)
        await _age(app, job.id, started_at="2 hours")  # past max_active_secs
        assert job.id in await app.sweep()
        [second] = await _claim(app, wid)
        assert second.lease_id != first.lease_id

        stale = await app._settlement.settle(
            first, Fence(job.id, wid, first.attempts, first.lease_id), Outcome("complete", result_json='"old"')
        )
        assert stale.status is None
        assert (await app.get_job(job.id)).status == JobStatus.Active

        fresh = await app._settlement.settle(
            second, Fence(job.id, wid, second.attempts, second.lease_id), Outcome("complete", result_json='"new"')
        )
        assert fresh.status == JobStatus.Complete
        assert (await app.get_job(job.id)).result == "new"

    async def test_requeue_cancelled_with_a_stale_lease_does_nothing(self, app):
        job = await app.enqueue(noop)
        wid = await _worker(app)
        [claimed] = await _claim(app, wid)
        from dataclasses import replace

        await app._worker_repo.requeue_cancelled(wid, replace(claimed, lease_id=str(uuid.uuid4())))
        assert (await app.get_job(job.id)).status == JobStatus.Active

        await app._worker_repo.requeue_cancelled(wid, claimed)
        row = await app.get_job(job.id)
        assert (row.status, row.lease_id, row.attempts) == (JobStatus.Queued, None, 0)

    async def test_every_exit_clears_the_lease(self, app):
        ok = await app.enqueue(noop)
        bad = await app.enqueue(noop, _retry=2)
        wid = await _worker(app)
        claimed = {j.id: j for j in await _claim(app, wid)}
        for jid, outcome in ((ok.id, Outcome("complete")), (bad.id, Outcome("fail", error="x"))):
            j = claimed[jid]
            await app._settlement.settle(j, Fence(jid, wid, j.attempts, j.lease_id), outcome)

        for jid in (ok.id, bad.id):
            row = await app.get_job(jid)
            assert (row.lease_id, row.lease_expires_at) == (None, None)
