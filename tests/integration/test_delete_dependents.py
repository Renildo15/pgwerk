"""Deleting or purging a job settles the jobs waiting on it instead of stranding them."""

from __future__ import annotations

import psycopg
import pytest

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.schemas import Dependency
from pgwerk.exceptions import JobNotFound

from .tasks import noop


async def _set_status(app, job_id: str, status: str) -> None:
    async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
        await conn.execute(
            SQL("UPDATE {jobs} SET status = %s WHERE id = %s").format(jobs=app._t["jobs"]),
            (status, job_id),
        )


class TestDeleteJob:
    async def test_dependent_of_deleted_unfinished_job_fails(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)
        grandchild = await app.enqueue(noop, _depends_on=child)

        await app.delete_job(parent.id)

        with pytest.raises(JobNotFound):
            await app.get_job(parent.id)
        for job in (child, grandchild):
            row = await app.get_job(job.id)
            assert row.status == JobStatus.Failed
            assert row.error == "dependency failed"

    async def test_allow_failure_dependent_is_queued(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=Dependency(job=parent, allow_failure=True))

        await app.delete_job(parent.id)

        assert (await app.get_job(child.id)).status == JobStatus.Queued

    async def test_maintenance_does_not_run_dependent_afterwards(self, app):
        """Regression: with dep rows gone, reconciliation used to queue the dependent."""
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)

        await app.delete_job(parent.id)
        await app.reconcile_waiting()

        assert (await app.get_job(child.id)).status == JobStatus.Failed

    async def test_finished_job_settles_dependents_by_real_status(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)
        await _set_status(app, parent.id, "complete")  # settled behind pgwerk's back

        await app.delete_job(parent.id)

        assert (await app.get_job(child.id)).status == JobStatus.Queued

    async def test_job_without_dependents_is_deleted(self, app):
        job = await app.enqueue(noop)
        await app.delete_job(job.id)
        with pytest.raises(JobNotFound):
            await app.get_job(job.id)


class TestPurgeJobs:
    async def test_purging_queued_parent_fails_dependent(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)
        other = await app.enqueue(noop, _queue="other")

        deleted = await app.purge_jobs(statuses=["queued"], queue="default")

        assert deleted == 1
        with pytest.raises(JobNotFound):
            await app.get_job(parent.id)
        assert (await app.get_job(child.id)).status == JobStatus.Failed
        assert (await app.get_job(other.id)).status == JobStatus.Queued

    async def test_age_filter(self, app):
        old = await app.enqueue(noop)
        new = await app.enqueue(noop)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET enqueued_at = NOW() - INTERVAL '10 days' WHERE id = %s").format(
                    jobs=app._t["jobs"]
                ),
                (old.id,),
            )

        assert await app.purge_jobs(statuses=["queued"], older_than_days=7) == 1
        with pytest.raises(JobNotFound):
            await app.get_job(old.id)
        assert (await app.get_job(new.id)).status == JobStatus.Queued

    async def test_purging_finished_jobs_leaves_settled_dependents_alone(self, app):
        from .conftest import make_worker

        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)
        await make_worker(app).run()

        assert await app.purge_jobs(statuses=["complete"]) == 2
        with pytest.raises(JobNotFound):
            await app.get_job(child.id)
