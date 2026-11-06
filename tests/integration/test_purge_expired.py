"""Maintenance deletes finished rows whose retention ran out."""

from __future__ import annotations

import psycopg

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.schemas import Dependency
from pgwerk.exceptions import JobNotFound

from .tasks import noop
from .tasks import fail_always
from .conftest import make_worker


async def _expire(app, job_id):
    async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
        await conn.execute(
            SQL("UPDATE {jobs} SET expires_at = NOW() - INTERVAL '1 second' WHERE id = %s").format(
                jobs=app._t["jobs"]
            ),
            (job_id,),
        )


async def _gone(app, job_id) -> bool:
    try:
        await app.get_job(job_id)
    except JobNotFound:
        return True
    return False


class TestPurgeExpired:
    async def test_deletes_finished_rows_past_retention_and_keeps_the_rest(self, app):
        done = await app.enqueue(noop, _result_ttl=60)
        failed = await app.enqueue(fail_always, _failure_ttl=60)
        kept = await app.enqueue(noop, _result_ttl=3600)
        forever = await app.enqueue(noop)
        await make_worker(app).run()
        await _expire(app, done.id)
        await _expire(app, failed.id)

        assert await app.purge_expired() == 2

        assert await _gone(app, done.id) and await _gone(app, failed.id)
        assert (await app.get_job(kept.id)).status == JobStatus.Complete
        assert (await app.get_job(forever.id)).status == JobStatus.Complete
        assert await app.get_executions(done.id) == []

    async def test_never_deletes_unstarted_jobs(self, app):
        job = await app.enqueue(noop, _ttl=60)
        await _expire(app, job.id)
        assert await app.purge_expired() == 0
        assert (await app.get_job(job.id)).status == JobStatus.Queued

    async def test_respects_limit(self, app):
        jobs = [await app.enqueue(noop, _result_ttl=60) for _ in range(3)]
        await make_worker(app).run()
        for j in jobs:
            await _expire(app, j.id)
        assert await app.purge_expired(limit=2) == 2
        assert await app.purge_expired(limit=2) == 1

    async def test_dependent_of_a_purged_row_is_not_stranded(self, app):
        """A waiting dependent of a finished row settles by its real status."""
        parent = await app.enqueue(noop, _result_ttl=60)
        await make_worker(app).run()
        other = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=[Dependency(job=parent), Dependency(job=other)])
        await _expire(app, parent.id)

        await app.purge_expired()

        assert await _gone(app, parent.id)
        assert (await app.get_job(child.id)).status == JobStatus.Waiting  # still waits on `other`
        await make_worker(app).run()
        assert (await app.get_job(child.id)).status == JobStatus.Complete

    async def test_deletes_stopped_workers_past_expiry(self, app):
        await make_worker(app).run()  # registers, then deregisters as stopped
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("UPDATE {w} SET expires_at = NOW() - INTERVAL '1 second' WHERE status = 'stopped'").format(
                    w=app._t["worker"]
                )
            )
            await app.purge_expired(limit=100_000)
            cur = await conn.execute(SQL("SELECT COUNT(*) FROM {w} WHERE status = 'stopped'").format(w=app._t["worker"]))
            assert (await cur.fetchone())[0] == 0

    async def test_maintenance_skips_it_when_disabled(self, app):
        from dataclasses import replace

        from pgwerk.maintenance import Maintenance

        job = await app.enqueue(noop, _result_ttl=60)
        await make_worker(app).run()
        await _expire(app, job.id)

        app.config = replace(app.config, purge_expired=False)
        assert (await Maintenance(app).run_once()).purged == 0
        assert not await _gone(app, job.id)

        app.config = replace(app.config, purge_expired=True)
        assert (await Maintenance(app).run_once()).purged == 1
