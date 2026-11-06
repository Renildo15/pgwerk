"""Integration tests for the stranded ``waiting`` job reconciliation."""

from __future__ import annotations

import psycopg

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.schemas import Dependency

from .tasks import noop


async def _set_status(app, job_id: str, status: str) -> None:
    """Settle a job behind pgwerk's back, so ``settle_dependents`` never fires."""
    async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
        await conn.execute(
            SQL("UPDATE {jobs} SET status = %s WHERE id = %s").format(jobs=app._t["jobs"]),
            (status, job_id),
        )


class TestReconcileWaiting:
    async def test_queues_dependent_of_completed_job(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)
        await _set_status(app, parent.id, "complete")
        assert (await app.get_job(child.id)).status == JobStatus.Waiting

        result = await app.reconcile_waiting()

        assert (result.queued, result.failed) == (1, 0)
        assert (await app.get_job(child.id)).status == JobStatus.Queued

    async def test_fails_dependent_of_failed_job_and_cascades(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)
        grandchild = await app.enqueue(noop, _depends_on=child)
        await _set_status(app, parent.id, "failed")

        result = await app.reconcile_waiting()

        assert (result.queued, result.failed) == (0, 1)
        for job in (child, grandchild):
            row = await app.get_job(job.id)
            assert row.status == JobStatus.Failed
            assert row.error == "dependency failed"

    async def test_allow_failure_dependent_is_queued(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=Dependency(job=parent, allow_failure=True))
        await _set_status(app, parent.id, "aborted")

        await app.reconcile_waiting()

        assert (await app.get_job(child.id)).status == JobStatus.Queued

    async def test_leaves_dependent_of_unsettled_job_alone(self, app):
        parent = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=parent)

        result = await app.reconcile_waiting()

        assert (result.queued, result.failed) == (0, 0)
        assert (await app.get_job(child.id)).status == JobStatus.Waiting

    async def test_waits_for_all_dependencies(self, app):
        p1 = await app.enqueue(noop)
        p2 = await app.enqueue(noop)
        child = await app.enqueue(noop, _depends_on=[p1, p2])
        await _set_status(app, p1.id, "complete")

        await app.reconcile_waiting()
        assert (await app.get_job(child.id)).status == JobStatus.Waiting

        await _set_status(app, p2.id, "complete")
        await app.reconcile_waiting()
        assert (await app.get_job(child.id)).status == JobStatus.Queued

    async def test_waiting_job_without_dep_rows_is_queued(self, app):
        """Only reachable from data written before ``insert`` became transactional."""
        job = await app.enqueue(noop)
        await _set_status(app, job.id, "waiting")

        await app.reconcile_waiting()

        assert (await app.get_job(job.id)).status == JobStatus.Queued

    async def test_respects_limit(self, app):
        parents = [await app.enqueue(noop) for _ in range(3)]
        children = [await app.enqueue(noop, _depends_on=p) for p in parents]
        for p in parents:
            await _set_status(app, p.id, "complete")

        assert (await app.reconcile_waiting(limit=2)).queued == 2
        assert (await app.reconcile_waiting(limit=2)).queued == 1
        assert all([(await app.get_job(c.id)).status == JobStatus.Queued for c in children])


class TestMaintenanceWithoutWorkers:
    async def test_run_once_recovers_stuck_and_stranded_jobs(self, app):
        from pgwerk.maintenance import Maintenance

        stuck = await app.enqueue(noop)
        parent = await app.enqueue(noop)
        stranded = await app.enqueue(noop, _depends_on=parent)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("""
                    UPDATE {jobs}
                    SET status = 'active', started_at = NOW() - INTERVAL '2 hour', worker_id = gen_random_uuid()
                    WHERE id = %s
                """).format(jobs=app._t["jobs"]),
                (stuck.id,),
            )
        await _set_status(app, parent.id, "complete")

        report = await Maintenance(app).run_once()

        assert report.errors == {}
        assert stuck.id in report.swept
        assert report.reconciled.queued == 1
        assert (await app.get_job(stuck.id)).status == JobStatus.Queued
        assert (await app.get_job(stranded.id)).status == JobStatus.Queued
