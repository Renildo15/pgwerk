"""The sweep recovers lost attempts by lease, without using retries, up to max_lost."""

from __future__ import annotations

import uuid

import pytest
import psycopg

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.commons import DequeueStrategy

from .tasks import noop
from .tasks import on_failure
from .tasks import _callback_log
from .tasks import clear_callback_log


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


async def _claim(app):
    wid = str(uuid.uuid4())
    await app._worker_repo.register(wid, "test", ["default"], "{}")
    [job] = await app._worker_repo.dequeue(wid, ["default"], 1, DequeueStrategy.Priority)
    return job


async def _set(app, job_id, **exprs):
    sets = ", ".join(f"{c} = {v}" for c, v in exprs.items())
    async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
        await conn.execute(SQL(f"UPDATE {{jobs}} SET {sets} WHERE id = %s").format(jobs=app._t["jobs"]), (job_id,))


async def _vanish(app, job_id):
    """The worker died: its lease expired and nothing touched the job since."""
    await _set(
        app,
        job_id,
        lease_expires_at="NOW() - INTERVAL '1 second'",
        touched_at="NOW() - INTERVAL '1 hour'",
        started_at="NOW() - INTERVAL '1 hour'",
    )


class TestLostAttempt:
    async def test_is_requeued_without_using_a_retry(self, app):
        job = await app.enqueue(noop, _retry=1)  # max_attempts=1: a failure would be terminal
        claimed = await _claim(app)
        await _vanish(app, job.id)

        assert job.id in await app.sweep()

        row = await app.get_job(job.id)
        assert (row.status, row.attempts, row.lost_count) == (JobStatus.Queued, 0, 1)
        assert (row.lease_id, row.worker_id) == (None, None)
        [execution] = await app.get_executions(job.id)
        assert (execution.id, execution.status.value) == (claimed.lease_id, "lost")

    async def test_recovery_follows_the_lease_not_max_active_secs(self, app):
        """A crashed worker's job comes back after the lease TTL, not after an hour."""
        job = await app.enqueue(noop)
        await _claim(app)
        await _set(
            app,
            job.id,
            lease_expires_at="NOW() - INTERVAL '1 second'",
            started_at="NOW() - INTERVAL '6 minutes'",  # well under max_active_secs (1h)
        )
        assert job.id in await app.sweep()

    async def test_live_lease_is_left_alone(self, app):
        job = await app.enqueue(noop)
        await _claim(app)
        await _set(app, job.id, started_at="NOW() - INTERVAL '2 hours'")  # old, but renewed
        assert await app.sweep() == []

    async def test_fails_after_max_lost_and_settles_like_a_failure(self, app):
        parent = await app.enqueue(noop, _max_lost=2, _on_failure=on_failure)
        child = await app.enqueue(noop, _depends_on=parent)
        for _ in range(3):
            await _claim(app)
            await _vanish(app, parent.id)
            await app.sweep()

        row = await app.get_job(parent.id)
        assert (row.status, row.error, row.lost_count) == (JobStatus.Failed, "lease_lost", 2)
        assert _callback_log == [("failure", parent.id)]
        child_row = await app.get_job(child.id)
        assert (child_row.status, child_row.error) == (JobStatus.Failed, "dependency failed")

    async def test_aborting_with_expired_lease_ends_aborted(self, app):
        job = await app.enqueue(noop)
        await _claim(app)
        await _set(app, job.id, status="'aborting'")
        await _vanish(app, job.id)
        await app.sweep()
        row = await app.get_job(job.id)
        assert (row.status, row.lost_count) == (JobStatus.Aborted, 0)


class TestTimeoutBackstop:
    async def test_is_a_failed_attempt_not_a_loss(self, app):
        job = await app.enqueue(noop, _timeout=5, _retry=3)
        await _claim(app)
        await _set(app, job.id, started_at="NOW() - INTERVAL '1 minute'")  # lease still live

        await app.sweep()

        row = await app.get_job(job.id)
        assert row.status in (JobStatus.Queued, JobStatus.Scheduled)
        assert (row.error, row.attempts, row.lost_count) == ("timeout", 1, 0)

    async def test_at_the_cap_fails_with_timeout(self, app):
        job = await app.enqueue(noop, _timeout=5, _retry=1)
        await _claim(app)
        await _set(app, job.id, started_at="NOW() - INTERVAL '1 minute'")
        await app.sweep()
        row = await app.get_job(job.id)
        assert (row.status, row.error) == (JobStatus.Failed, "timeout")


class TestRollingDeploy:
    async def test_leased_row_stolen_by_an_old_worker_is_not_swept(self, app):
        """An old worker's steal leaves the lease expired but keeps touched_at fresh."""
        job = await app.enqueue(noop, _heartbeat=30)
        await _claim(app)
        await _set(
            app,
            job.id,
            lease_expires_at="NOW() - INTERVAL '1 minute'",
            touched_at="NOW()",
            worker_id="gen_random_uuid()",
        )
        assert await app.sweep() == []

    async def test_unleased_row_from_an_old_worker_uses_the_old_rules(self, app):
        job = await app.enqueue(noop, _retry=2)
        await _set(
            app,
            job.id,
            status="'active'",
            attempts="1",
            started_at="NOW() - INTERVAL '2 hours'",
            worker_id="gen_random_uuid()",
        )
        await app.sweep()
        row = await app.get_job(job.id)
        assert (row.status, row.lost_count, row.attempts) == (JobStatus.Queued, 0, 1)
