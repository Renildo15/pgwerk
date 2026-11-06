"""Integration tests for job TTL and expiry."""

from __future__ import annotations

from psycopg.sql import SQL

from pgwerk.commons import JobStatus

from .tasks import noop
from .tasks import fail_always
from .conftest import make_worker


class TestExpiredJobs:
    async def test_expired_queued_job_not_dequeued(self, app):
        """A job whose expires_at is in the past is skipped by the dequeue query."""
        import psycopg
        job = await app.enqueue(noop)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET expires_at = NOW() - INTERVAL '1 second' WHERE id = %(id)s").format(
                    jobs=app._t["jobs"]
                ),
                {"id": job.id},
            )

        await make_worker(app).run()

        refreshed = await app.get_job(job.id)
        assert refreshed.status == JobStatus.Queued, "Expired job should not have been dequeued"

    async def test_failure_ttl_sets_expires_at_on_failed_job(self, app):
        """After terminal failure, expires_at reflects failure_ttl."""
        job = await app.enqueue(fail_always, _retry=1, _failure_ttl=3600)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Failed
        assert done.expires_at is not None

    async def test_result_ttl_sets_expires_at_on_complete_job(self, app):
        """After successful completion, expires_at reflects result_ttl."""
        job = await app.enqueue(noop, _result_ttl=3600)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.expires_at is not None

    async def test_no_ttl_leaves_expires_at_null_on_success(self, app):
        """Without result_ttl, completed jobs have no expiry."""
        job = await app.enqueue(noop)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete
        assert done.expires_at is None

    async def test_non_expired_job_is_dequeued_normally(self, app):
        """A job with a future expires_at is dequeued and processed normally."""
        job = await app.enqueue(noop, _ttl=3600)
        await make_worker(app).run()
        done = await app.get_job(job.id)
        assert done.status == JobStatus.Complete


async def _expire_now(app, job_id: str) -> None:
    import psycopg

    async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
        await conn.execute(
            SQL("UPDATE {jobs} SET expires_at = NOW() - INTERVAL '1 second' WHERE id = %(id)s").format(
                jobs=app._t["jobs"]
            ),
            {"id": job_id},
        )


class TestExpireUnstarted:
    async def test_expired_job_ends_aborted_with_reason(self, app):
        job = await app.enqueue(noop, _ttl=60, _failure_ttl=600)
        await _expire_now(app, job.id)

        assert await app.expire_unstarted() == 1

        row = await app.get_job(job.id)
        assert (row.status, row.error) == (JobStatus.Aborted, "expired")
        assert row.completed_at is not None
        assert row.expires_at > row.completed_at  # now the failure_ttl retention
        assert await app.get_executions(job.id) == []

    async def test_runs_on_stopped(self, app):
        from .tasks import on_stopped
        from .tasks import _callback_log
        from .tasks import clear_callback_log

        clear_callback_log()
        job = await app.enqueue(noop, _ttl=60, _on_stopped=on_stopped)
        await _expire_now(app, job.id)
        await app.expire_unstarted()
        assert _callback_log == [("stopped", job.id)]
        clear_callback_log()

    async def test_dependents_fail_or_run_with_allow_failure(self, app):
        from pgwerk.schemas import Dependency

        sms = await app.enqueue(noop, _ttl=60)
        log = await app.enqueue(noop, _depends_on=sms)
        audit = await app.enqueue(noop, _depends_on=Dependency(job=sms, allow_failure=True))
        await _expire_now(app, sms.id)

        await app.expire_unstarted()

        log_row = await app.get_job(log.id)
        assert (log_row.status, log_row.error) == (JobStatus.Failed, "dependency failed")
        assert (await app.get_job(audit.id)).status == JobStatus.Queued

    async def test_failure_mode_delete_deletes(self, app):
        import pytest

        from pgwerk.exceptions import JobNotFound

        job = await app.enqueue(noop, _ttl=60, _failure_mode="delete")
        await _expire_now(app, job.id)
        await app.expire_unstarted()
        with pytest.raises(JobNotFound):
            await app.get_job(job.id)

    async def test_wait_for_returns(self, app):
        job = await app.enqueue(noop, _ttl=60)
        await _expire_now(app, job.id)
        await app.expire_unstarted()
        done = await app.wait_for(job.id, timeout=2, poll_interval=0.1)
        assert done.status == JobStatus.Aborted

    async def test_leaves_live_and_finished_jobs_alone(self, app):
        live = await app.enqueue(noop, _ttl=3600)
        finished = await app.enqueue(noop, _result_ttl=1)
        await make_worker(app).run()  # runs both
        await _expire_now(app, finished.id)  # retention time in the past

        assert await app.expire_unstarted() == 0
        assert (await app.get_job(live.id)).status == JobStatus.Complete
        assert (await app.get_job(finished.id)).status == JobStatus.Complete

    async def test_scheduled_job_expires_too(self, app):
        job = await app.enqueue(noop, _ttl=60, _delay=3600)
        await _expire_now(app, job.id)
        await app.expire_unstarted()
        assert (await app.get_job(job.id)).status == JobStatus.Aborted

    async def test_respects_limit(self, app):
        jobs = [await app.enqueue(noop, _ttl=60) for _ in range(3)]
        for j in jobs:
            await _expire_now(app, j.id)
        assert await app.expire_unstarted(limit=2) == 2
        assert await app.expire_unstarted(limit=2) == 1

    async def test_maintenance_expires_them(self, app):
        from pgwerk.maintenance import Maintenance

        job = await app.enqueue(noop, _ttl=60)
        await _expire_now(app, job.id)
        report = await Maintenance(app).run_once()
        assert report.expired == 1
        assert (await app.get_job(job.id)).status == JobStatus.Aborted
