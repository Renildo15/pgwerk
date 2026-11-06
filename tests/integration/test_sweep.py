"""Integration tests for sweep functionality."""

from __future__ import annotations

import pytest

import psycopg

from psycopg.sql import SQL

from pgwerk.commons import JobStatus

from .tasks import noop
from .tasks import clear_callback_log


@pytest.fixture(autouse=True)
def _clear_cbs():
    clear_callback_log()
    yield
    clear_callback_log()


class TestSweep:
    async def test_sweep_requeues_stuck_active_job(self, app):
        job = await app.enqueue(noop)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active', started_at = NOW() - INTERVAL '2 hour', worker_id = gen_random_uuid()
                    WHERE id = %s
                """).format(jobs=app._t["jobs"]),
                (job.id,),
            )
        swept = await app.sweep()
        assert job.id in swept
        refreshed = await app.get_job(job.id)
        assert refreshed.status == JobStatus.Queued

    async def test_sweep_fails_job_when_attempts_exhausted(self, app):
        job = await app.enqueue(noop, _retry=1)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active', attempts = 1, max_attempts = 1,
                        started_at = NOW() - INTERVAL '2 hour', worker_id = gen_random_uuid()
                    WHERE id = %s
                """).format(jobs=app._t["jobs"]),
                (job.id,),
            )
        swept = await app.sweep()
        assert job.id in swept
        refreshed = await app.get_job(job.id)
        assert refreshed.status == JobStatus.Failed

    async def test_sweep_returns_empty_when_nothing_stuck(self, app):
        await app.enqueue(noop)
        swept = await app.sweep()
        assert swept == []

    async def test_sweep_respects_heartbeat_secs(self, app):
        job = await app.enqueue(noop, _heartbeat=30)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active', started_at = NOW() - INTERVAL '1 minute',
                        touched_at = NOW() - INTERVAL '1 minute', worker_id = gen_random_uuid()
                    WHERE id = %s
                """).format(jobs=app._t["jobs"]),
                (job.id,),
            )
        swept = await app.sweep()
        assert job.id in swept

    async def test_sweep_ignores_fresh_active_job(self, app):
        job = await app.enqueue(noop)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active', started_at = NOW(), worker_id = gen_random_uuid()
                    WHERE id = %s
                """).format(jobs=app._t["jobs"]),
                (job.id,),
            )
        swept = await app.sweep()
        assert job.id not in swept

    async def test_sweep_skips_job_with_fresh_heartbeat(self, app):
        job = await app.enqueue(noop, _heartbeat=60)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active',
                        attempts = 1,
                        started_at = NOW() - INTERVAL '30 seconds',
                        touched_at = NOW()
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job.id},
            )

        swept = await app.sweep()

        assert job.id not in swept
        alive = await app.get_job(job.id)
        assert alive.status == JobStatus.Active

    async def test_sweep_recovers_stale_heartbeat_job(self, app):
        job = await app.enqueue(noop, _heartbeat=5, _retry=2)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active',
                        attempts = 1,
                        started_at = NOW() - INTERVAL '30 seconds',
                        touched_at = NOW() - INTERVAL '30 seconds'
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job.id},
            )

        swept = await app.sweep()

        assert job.id in swept
        requeued = await app.get_job(job.id)
        assert requeued.status == JobStatus.Queued

    async def test_sweep_fails_stale_job_at_max_attempts(self, app):
        job = await app.enqueue(noop, _heartbeat=5, _retry=1)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active',
                        attempts = 1,
                        max_attempts = 1,
                        started_at = NOW() - INTERVAL '30 seconds',
                        touched_at = NOW() - INTERVAL '30 seconds'
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job.id},
            )

        swept = await app.sweep()

        assert job.id in swept
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed

    async def test_sweep_skips_no_heartbeat_fresh_job(self, app):
        job = await app.enqueue(noop)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active',
                        attempts = 1,
                        started_at = NOW() - INTERVAL '5 seconds'
                    WHERE id = %(id)s
                """).format(jobs=app._t["jobs"]),
                {"id": job.id},
            )

        swept = await app.sweep()

        assert job.id not in swept


async def _make_stuck_aborting(app, job_id: str, *, attempts: int = 1, max_attempts: int = 3) -> None:
    async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
        await conn.execute(
            SQL("""
                UPDATE {jobs}
                SET status = 'aborting', attempts = %s, max_attempts = %s,
                    started_at = NOW() - INTERVAL '2 hour', worker_id = gen_random_uuid()
                WHERE id = %s
            """).format(jobs=app._t["jobs"]),
            (attempts, max_attempts, job_id),
        )
        await conn.execute(
            SQL("INSERT INTO {ex} (job_id, attempt, status) VALUES (%s, %s, 'running')").format(
                ex=app._t["executions"]
            ),
            (job_id, attempts),
        )


class TestSweepAborting:
    async def test_stuck_aborting_job_ends_aborted_not_requeued(self, app):
        job = await app.enqueue(noop)
        await _make_stuck_aborting(app, job.id)

        assert job.id in await app.sweep()

        refreshed = await app.get_job(job.id)
        assert refreshed.status == JobStatus.Aborted
        assert refreshed.error == "aborted"
        assert refreshed.worker_id is None
        assert refreshed.completed_at is not None
        [execution] = await app.get_executions(job.id)
        assert execution.status == "aborted"

    async def test_stuck_aborting_at_cap_ends_aborted_not_failed(self, app):
        job = await app.enqueue(noop, _retry=1)
        await _make_stuck_aborting(app, job.id, attempts=1, max_attempts=1)
        await app.sweep()
        assert (await app.get_job(job.id)).status == JobStatus.Aborted

    async def test_stuck_aborting_runs_on_stopped(self, app):
        from .tasks import on_stopped
        from .tasks import _callback_log

        job = await app.enqueue(noop, _on_stopped=on_stopped)
        await _make_stuck_aborting(app, job.id)
        await app.sweep()
        assert _callback_log == [("stopped", job.id)]

    async def test_stuck_aborting_fails_dependents(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)
        await _make_stuck_aborting(app, parent.id)
        await app.sweep()
        child_row = await app.get_job(child.id)
        assert child_row.status == JobStatus.Failed
        assert child_row.error == "dependency failed"

    async def test_stuck_aborting_allow_failure_dependent_is_queued(self, app):
        from pgwerk.schemas import Dependency

        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=Dependency(job=parent, allow_failure=True))
        await _make_stuck_aborting(app, parent.id)
        await app.sweep()
        assert (await app.get_job(child.id)).status == JobStatus.Queued

    async def test_stuck_aborting_failure_mode_delete(self, app):
        from pgwerk.exceptions import JobNotFound

        from .tasks import on_stopped
        from .tasks import _callback_log

        parent = await app.enqueue(noop, _failure_mode="delete", _on_stopped=on_stopped)
        child = await app.enqueue(noop, _depends_on=parent)
        await _make_stuck_aborting(app, parent.id)
        await app.sweep()

        with pytest.raises(JobNotFound):
            await app.get_job(parent.id)
        assert _callback_log == [("stopped", parent.id)]
        assert (await app.get_job(child.id)).status == JobStatus.Failed
