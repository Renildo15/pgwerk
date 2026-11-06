"""Everything that happens after a job attempt finishes, in one place.

Local workers and the sweeper settle jobs through this module (and, later, the
remote-worker router). :func:`plan` is the pure decision table: given the job and
what happened, it decides the next status, retry schedule, expiry, whether the row
is deleted, whether a repeat is inserted, and which callback runs.
:class:`Settlement` applies that decision inside one transaction and runs the
callback after commit.
"""

from __future__ import annotations

import logging

from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from dataclasses import dataclass

from psycopg.rows import dict_row

from .utils import invoke_callback
from .commons import JobStatus
from .schemas import Job


if TYPE_CHECKING:
    from .repos import JobRepository
    from .connection import Connect


logger = logging.getLogger(__name__)

OutcomeKind = Literal["complete", "fail", "abort"]


@dataclass(frozen=True)
class Outcome:
    """What the caller observed when the attempt ended."""

    kind: OutcomeKind
    result_json: str | None = None
    """Encoded return value, for ``complete``."""
    error: str | None = None
    """Error text for ``fail`` (a traceback for local workers), or the reason for an
    ``abort`` (default ``"aborted"``)."""
    retryable: bool = True
    """``False`` fails the job terminally, even with attempts left."""


@dataclass(frozen=True)
class Fence:
    """Which row a settle may touch.

    A worker settles only a row it still owns: ``worker_id`` matches and the job is
    ``active`` or ``aborting``. The sweeper passes ``worker_id=None`` for rows it
    has already locked; the status check still applies.
    """

    job_id: str
    worker_id: str | None
    attempt: int


@dataclass(frozen=True)
class Transition:
    """The decision :func:`plan` makes for one outcome."""

    status: JobStatus
    error: str | None = None
    result_json: str | None = None
    scheduled_at: datetime | None = None
    """When a delayed retry becomes runnable; ``None`` keeps the current value."""
    expires_at: datetime | None = None
    delete: bool = False
    """Delete the row after settling dependents (``failure_mode="delete"``)."""
    repeat: bool = False
    """Insert the next run of a repeating job."""
    callback: str | None = None
    callback_timeout: int | None = None

    @property
    def terminal(self) -> bool:
        return self.status in (JobStatus.Complete, JobStatus.Failed, JobStatus.Aborted)

    @property
    def retrying(self) -> bool:
        return self.status in (JobStatus.Queued, JobStatus.Scheduled)


@dataclass(frozen=True)
class Settled:
    """What a settle did."""

    status: JobStatus | None
    """The status written, or ``None`` when the fence didn't match (the attempt no
    longer owns the row) and nothing was written."""
    transition: Transition | None = None

    @property
    def deleted(self) -> bool:
        return self.transition is not None and self.transition.delete

    @property
    def retry_at(self) -> datetime | None:
        return self.transition.scheduled_at if self.transition is not None else None


def plan(job: Job, outcome: Outcome, now: datetime | None = None) -> Transition:
    """Decide what an outcome does to *job*. Pure: no I/O.

    Args:
        job: The job as it was claimed (``attempts`` already counts this attempt).
        outcome: What happened.
        now: Reference time for retry delays and expiry; defaults to the current time.
    """
    now = now or datetime.now(timezone.utc)

    def _expiry(ttl: int | None) -> datetime | None:
        return now + timedelta(seconds=ttl) if ttl is not None else None

    if outcome.kind == "complete":
        return Transition(
            status=JobStatus.Complete,
            result_json=outcome.result_json,
            expires_at=_expiry(job.result_ttl),
            repeat=bool(job.repeat_remaining and job.repeat_remaining > 0),
            callback=job.on_success,
            callback_timeout=job.on_success_timeout,
        )

    if outcome.kind == "abort":
        return Transition(
            status=JobStatus.Aborted,
            error=outcome.error or "aborted",
            expires_at=_expiry(job.failure_ttl),
            delete=job.failure_mode == "delete",
            callback=job.on_stopped,
            callback_timeout=job.on_stopped_timeout,
        )

    if outcome.kind == "fail":
        if outcome.retryable and job.attempts < job.max_attempts:
            scheduled_at = None
            if job.retry_intervals:
                delay = job.retry_intervals[min(job.attempts - 1, len(job.retry_intervals) - 1)]
                if delay:
                    scheduled_at = now + timedelta(seconds=delay)
            return Transition(
                status=JobStatus.Scheduled if scheduled_at else JobStatus.Queued,
                error=outcome.error,
                scheduled_at=scheduled_at,
            )
        return Transition(
            status=JobStatus.Failed,
            error=outcome.error,
            expires_at=_expiry(job.failure_ttl),
            delete=job.failure_mode == "delete",
            callback=job.on_failure,
            callback_timeout=job.on_failure_timeout,
        )

    raise ValueError(f"unknown outcome kind: {outcome.kind!r}")


_EXECUTION_STATUS = {
    JobStatus.Complete: "complete",
    JobStatus.Aborted: "aborted",
    JobStatus.Failed: "failed",
    JobStatus.Queued: "failed",
    JobStatus.Scheduled: "failed",
}


class Settlement:
    """Applies :func:`plan` decisions to the database.

    Args:
        connect: Connection factory (autocommit connections).
        job_repo: Repository that owns the job tables.
    """

    def __init__(self, connect: "Connect", job_repo: "JobRepository") -> None:
        self._connect = connect
        self._jobs = job_repo

    async def apply(self, cur: Any, job: Job, fence: Fence, outcome: Outcome) -> Settled:
        """Settle *job* inside the caller's transaction. Callbacks are not run.

        The row is locked and its current status read first. If an abort was
        requested meanwhile (``aborting``), a ``complete`` or ``fail`` settles as an
        abort instead: the abort wins, and a result that raced it is dropped.

        In order: status and result/error, the execution row, the claim row,
        dependents (on a terminal status), ``failure_mode=delete``, the next repeat,
        and ``NOTIFY`` for the queues that gained runnable work.
        """
        current = await self._jobs.lock_for_settle(cur, fence.job_id, fence.worker_id)
        if current is None:
            return Settled(status=None)

        if current == JobStatus.Aborting.value and outcome.kind != "abort":
            outcome = Outcome("abort")
        t = plan(job, outcome)

        await self._jobs.write_settled(
            cur,
            job.id,
            status=t.status.value,
            error=t.error,
            result_json=t.result_json,
            scheduled_at=t.scheduled_at,
            expires_at=t.expires_at,
        )
        await self._jobs.close_execution(
            cur,
            job.id,
            fence.worker_id,
            # A worker closes its own attempt; the sweeper closes whatever is still running.
            fence.attempt if fence.worker_id is not None else None,
            status=_EXECUTION_STATUS[t.status],
            result_json=t.result_json if t.status == JobStatus.Complete else None,
            error=t.error if t.status != JobStatus.Complete else None,
        )
        await self._jobs.release_claim(cur, job.id, fence.worker_id)

        notify: set[str] = set()
        if t.terminal:
            notify.update(await self._jobs.settle_dependents(cur, job.id))
        if t.delete:
            await self._jobs.delete_settled(cur, job.id)
        if t.repeat:
            await self._jobs.insert_repeat(cur, job)
            notify.add(job.queue)
        if t.status == JobStatus.Queued:
            notify.add(job.queue)
        for queue in notify:
            await self._jobs.notify(cur, queue)

        return Settled(status=t.status, transition=t)

    async def settle(self, job: Job, fence: Fence, outcome: Outcome, *, run_callbacks: bool = True) -> Settled:
        """Settle *job* in its own transaction, then run its callback.

        The callback runs after commit, so it never sees a state that could still
        roll back. Delivery is at most once: if the process dies right after commit,
        the callback is lost.
        """
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            settled = await self.apply(cur, job, fence, outcome)
        if settled.status is None:
            logger.debug("settle fence miss for job %s [%s]; nothing written", job.function, job.id)
        elif run_callbacks:
            await self.run_callback(job, settled)
        return settled

    @staticmethod
    async def run_callback(job: Job, settled: Settled) -> None:
        """Run the callback chosen for a settled job, if any. Never raises."""
        t = settled.transition
        if t is not None and t.callback:
            await invoke_callback(t.callback, job, timeout=t.callback_timeout)

    async def sweep(self, max_active_secs: int) -> list[str]:
        """Recover stuck jobs whose worker stopped making progress.

        Each stuck job is locked, then:

        - ``aborting``: settles as aborted (dependents, ``failure_mode``,
          ``failure_ttl``, ``on_stopped``). It is never requeued.
        - ``active`` at the attempt cap: settles as a failure with
          ``error = 'swept'``, like any other failure (``failure_mode``,
          ``failure_ttl``, dependents, ``on_failure``).
        - ``active`` under the cap: requeued at once.

        Callbacks run after the transaction commits.

        Returns:
            IDs of every job the sweep touched.
        """
        settled: list[tuple[Job, Settled]] = []
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            stuck = await self._jobs.lock_stuck(cur, max_active_secs)
            requeue: list[Job] = []
            for job in stuck:
                fence = Fence(job.id, worker_id=None, attempt=job.attempts)
                if job.status == JobStatus.Aborting:
                    settled.append((job, await self.apply(cur, job, fence, Outcome("abort"))))
                elif job.attempts >= job.max_attempts:
                    settled.append((job, await self.apply(cur, job, fence, Outcome("fail", error="swept"))))
                else:
                    requeue.append(job)
            await self._jobs.requeue_stuck(cur, requeue)

        for job, result in settled:
            await self.run_callback(job, result)
        return [job.id for job in stuck]

    async def expire_unstarted(self, limit: int = 500) -> int:
        """Abort ``queued``/``scheduled`` jobs whose ``_ttl`` ran out before they started.

        Each one settles as ``aborted`` with ``error = 'expired'``: dependents settle
        (failing unless the dependency has ``allow_failure``), ``failure_mode="delete"``
        deletes the row, ``failure_ttl`` sets the new ``expires_at``, and ``on_stopped``
        runs after commit. Rows locked elsewhere are skipped.

        Args:
            limit: Maximum number of jobs to expire in one call.

        Returns:
            Number of jobs expired.
        """
        expired: list[tuple[Job, Transition]] = []
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            notify: set[str] = set()
            for job in await self._jobs.lock_expired_unstarted(cur, limit):
                t = plan(job, Outcome("abort", error="expired"))
                await self._jobs.write_settled(
                    cur,
                    job.id,
                    status=t.status.value,
                    error=t.error,
                    result_json=None,
                    scheduled_at=None,
                    expires_at=t.expires_at,
                )
                notify.update(await self._jobs.settle_dependents(cur, job.id))
                if t.delete:
                    await self._jobs.delete_settled(cur, job.id)
                expired.append((job, t))
            for queue in notify:
                await self._jobs.notify(cur, queue)

        for job, t in expired:
            await self.run_callback(job, Settled(status=t.status, transition=t))
        return len(expired)
