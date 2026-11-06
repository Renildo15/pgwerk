"""Settlement applied against the database: one transaction, fenced, abort wins."""

from __future__ import annotations

import uuid

import pytest
import psycopg

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.commons import DequeueStrategy
from pgwerk.schemas import Repeat
from pgwerk.settlement import Fence
from pgwerk.settlement import Outcome

from .tasks import noop


async def _claim(app, **enqueue_kwargs):
    job = await app.enqueue(noop, **enqueue_kwargs)
    wid = str(uuid.uuid4())
    await app._worker_repo.register(wid, "test", ["default"], "{}")
    [claimed] = await app._worker_repo.dequeue(wid, ["default"], 1, DequeueStrategy.Priority)
    assert claimed.id == job.id
    return claimed, Fence(claimed.id, wid, claimed.attempts, claimed.lease_id)


async def _count(app, function: str) -> int:
    async with await app._connect() as conn:
        cur = await conn.execute(
            SQL("SELECT COUNT(*) FROM {jobs} WHERE function = %s").format(jobs=app._t["jobs"]), (function,)
        )
        return (await cur.fetchone())[0]


class TestRepeatInsideTransaction:
    async def test_complete_inserts_next_run(self, app):
        job, fence = await _claim(app, _repeat=Repeat(times=2))
        settled = await app._settlement.settle(job, fence, Outcome("complete"))
        assert settled.status == JobStatus.Complete
        assert await _count(app, job.function) == 2

    async def test_next_run_keeps_failure_mode(self, app):
        job, fence = await _claim(app, _repeat=Repeat(times=2), _failure_mode="delete")
        await app._settlement.settle(job, fence, Outcome("complete"))

        async with await app._connect() as conn:
            cur = await conn.execute(
                SQL("SELECT failure_mode FROM {jobs} WHERE function = %s AND id <> %s").format(jobs=app._t["jobs"]),
                (job.function, job.id),
            )
            assert (await cur.fetchone())[0] == "delete"

    async def test_failure_after_repeat_insert_rolls_everything_back(self, app, monkeypatch):
        job, fence = await _claim(app, _repeat=Repeat(times=2))

        async def boom(cur, job_id):
            raise RuntimeError("crash mid-settle")

        monkeypatch.setattr(app._job_repo, "settle_dependents", boom)
        with pytest.raises(RuntimeError):
            await app._settlement.settle(job, fence, Outcome("complete"))

        assert (await app.get_job(job.id)).status == JobStatus.Active
        assert await _count(app, job.function) == 1

    async def test_replayed_settle_does_not_insert_a_second_run(self, app):
        job, fence = await _claim(app, _repeat=Repeat(times=3))
        first = await app._settlement.settle(job, fence, Outcome("complete"))
        replay = await app._settlement.settle(job, fence, Outcome("complete"))
        assert first.status == JobStatus.Complete
        assert replay.status is None
        assert await _count(app, job.function) == 2


class TestAbortWins:
    async def _request_abort(self, app, job_id):
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET status = 'aborting' WHERE id = %s").format(jobs=app._t["jobs"]), (job_id,)
            )

    @pytest.mark.parametrize("outcome", [Outcome("complete", result_json='"x"'), Outcome("fail", error="boom")])
    async def test_outcome_racing_an_abort_settles_as_aborted(self, app, outcome):
        job, fence = await _claim(app, _retry=3)
        await self._request_abort(app, job.id)

        settled = await app._settlement.settle(job, fence, outcome)

        assert settled.status == JobStatus.Aborted
        row = await app.get_job(job.id)
        assert row.status == JobStatus.Aborted
        assert row.result is None
        assert row.error == "aborted"


class TestFence:
    async def test_other_workers_fence_writes_nothing(self, app):
        job, fence = await _claim(app)
        stale = Fence(job.id, str(uuid.uuid4()), job.attempts, job.lease_id)

        assert (await app._settlement.settle(job, stale, Outcome("complete"))).status is None
        assert (await app.get_job(job.id)).status == JobStatus.Active

    async def test_execution_row_is_closed(self, app):
        job, fence = await _claim(app)
        await app._settlement.settle(job, fence, Outcome("fail", error="boom", retryable=False))
        [execution] = await app.get_executions(job.id)
        assert execution.status.value == "failed"
        assert execution.error == "boom"


class TestCallbackSeesSettledJob:
    """Callbacks get the job as settled (final status, error or result), not as claimed."""

    @pytest.fixture(autouse=True)
    def _clear(self):
        from .tasks import _callback_jobs

        _callback_jobs.clear()
        yield
        _callback_jobs.clear()

    async def test_on_success_sees_status_and_result(self, app):
        from .tasks import capture_job
        from .tasks import _callback_jobs

        job, fence = await _claim(app, _on_success=capture_job)
        await app._settlement.settle(job, fence, Outcome("complete", result_json='{"n": 7}'))
        [seen] = _callback_jobs
        assert (seen.status, seen.result) == (JobStatus.Complete, {"n": 7})

    async def test_on_failure_sees_status_and_error(self, app):
        from .tasks import capture_job
        from .tasks import _callback_jobs

        job, fence = await _claim(app, _on_failure=capture_job)
        await app._settlement.settle(job, fence, Outcome("fail", error="boom", retryable=False))
        [seen] = _callback_jobs
        assert (seen.status, seen.error) == (JobStatus.Failed, "boom")

    async def test_on_stopped_of_an_expired_job_sees_expired(self, app):
        from .tasks import noop
        from .tasks import capture_job
        from .tasks import _callback_jobs

        job = await app.enqueue(noop, _ttl=60, _on_stopped=capture_job)
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("UPDATE {jobs} SET expires_at = NOW() - INTERVAL '1 second' WHERE id = %s").format(
                    jobs=app._t["jobs"]
                ),
                (job.id,),
            )
        await app.expire_unstarted()
        [seen] = _callback_jobs
        assert (seen.status, seen.error) == (JobStatus.Aborted, "expired")
