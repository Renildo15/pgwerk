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
import dataclasses

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
from .serializers import decode


if TYPE_CHECKING:
    from .repos import JobRepository
    from .connection import Connect


logger = logging.getLogger(__name__)

OutcomeKind = Literal["complete", "fail", "abort", "lost"]


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

    A worker settles only the attempt it still owns: ``worker_id`` and ``lease_id``
    (the attempt's execution id) match, and the job is ``active`` or ``aborting``.
    The sweeper passes neither for rows it has already locked; the status check
    still applies.
    """

    job_id: str
    worker_id: str | None
    attempt: int
    lease_id: str | None = None

    def __post_init__(self) -> None:
        if self.worker_id is not None and self.lease_id is None:
            raise ValueError("a worker's fence needs the attempt's lease_id")


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
    lost: bool = False
    """The attempt was lost (its lease expired): the execution is marked ``lost``,
    and a requeue gives the attempt back and counts it in ``lost_count``."""

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


def plan(job: Job, outcome: Outcome, now: datetime | None = None, default_max_lost: int = 3) -> Transition:
    """Decide what an outcome does to *job*. Pure: no I/O.

    Args:
        job: The job as it was claimed (``attempts`` already counts this attempt).
        outcome: What happened.
        now: Reference time for retry delays and expiry; defaults to the current time.
        default_max_lost: Cap on ``lost_count`` for jobs without ``max_lost``.
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

    if outcome.kind == "lost":
        # The worker vanished: that says nothing about the job, so it doesn't use a
        # retry. A job that keeps killing its worker looks the same, hence the cap.
        cap = job.max_lost if job.max_lost is not None else default_max_lost
        if job.lost_count < cap:
            return Transition(status=JobStatus.Queued, error="lease_lost", scheduled_at=now, lost=True)
        return Transition(
            status=JobStatus.Failed,
            error="lease_lost",
            expires_at=_expiry(job.failure_ttl),
            delete=job.failure_mode == "delete",
            callback=job.on_failure,
            callback_timeout=job.on_failure_timeout,
            lost=True,
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

    def __init__(self, connect: "Connect", job_repo: "JobRepository", default_max_lost: int = 3) -> None:
        self._connect = connect
        self._jobs = job_repo
        self._default_max_lost = default_max_lost

    async def apply(self, cur: Any, job: Job, fence: Fence, outcome: Outcome) -> Settled:
        """Settle *job* inside the caller's transaction. Callbacks are not run.

        The row is locked and its current status read first. If an abort was
        requested meanwhile (``aborting``), a ``complete`` or ``fail`` settles as an
        abort instead: the abort wins, and a result that raced it is dropped.

        In order: status and result/error, the execution row, the claim row,
        dependents (on a terminal status), ``failure_mode=delete``, the next repeat,
        and ``NOTIFY`` for the queues that gained runnable work.
        """
        current = await self._jobs.lock_for_settle(cur, fence.job_id, fence.worker_id, fence.lease_id)
        if current is None:
            return Settled(status=None)

        if current == JobStatus.Aborting.value and outcome.kind != "abort":
            outcome = Outcome("abort")
        t = plan(job, outcome, default_max_lost=self._default_max_lost)

        await self._jobs.write_settled(
            cur,
            job.id,
            status=t.status.value,
            error=t.error,
            result_json=t.result_json,
            scheduled_at=t.scheduled_at,
            expires_at=t.expires_at,
            give_back_attempt=t.lost and t.retrying,
        )
        await self._jobs.close_execution(
            cur,
            job.id,
            # A worker closes its own attempt; the sweeper closes whatever is still running.
            fence.lease_id,
            status="lost" if t.lost else _EXECUTION_STATUS[t.status],
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

    async def run_callback(self, job: Job, settled: Settled) -> None:
        """Run the callback chosen for a settled job, if any. Never raises.

        The callback gets the job as it was settled: its final ``status``, and its
        ``error`` (``"expired"``, ``"lease_lost"``, ``"timeout"``, a traceback...) or,
        on success, its ``result``. It doesn't get the job as it was when claimed.
        """
        t = settled.transition
        if t is not None and t.callback:
            await invoke_callback(t.callback, self._settled_view(job, t), timeout=t.callback_timeout)

    def _settled_view(self, job: Job, t: Transition) -> Job:
        if t.status == JobStatus.Complete:
            return dataclasses.replace(job, status=t.status, result=decode(self._jobs._serializer, t.result_json))
        return dataclasses.replace(job, status=t.status, error=t.error)

    async def sweep(self, max_active_secs: int, local_lease_ttl: int = 300) -> list[str]:
        """Recover stuck jobs whose worker stopped making progress.

        Each stuck job is locked, then settled by why it is stuck:

        - ``aborting``: an abort (dependents, ``failure_mode``, ``failure_ttl``,
          ``on_stopped``). It is never requeued.
        - past ``timeout_secs``: a failed attempt with ``error = 'timeout'``. It
          follows ``retry_intervals``, like a timeout the worker reported.
        - lease expired: the attempt was lost. The job is requeued at once without
          using a retry (``lost_count + 1``), or fails with ``lease_lost`` once
          ``max_lost`` is reached.
        - no lease (claimed by a worker older than schema 8): the old rules. It is
          requeued, or fails with ``error = 'swept'`` at the attempt cap.

        Callbacks run after the transaction commits.

        Returns:
            IDs of every job the sweep touched.
        """
        settled: list[tuple[Job, Settled]] = []
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            stuck = await self._jobs.lock_stuck(cur, max_active_secs, local_lease_ttl)
            requeue: list[Job] = []
            for job, reason in stuck:
                fence = Fence(job.id, worker_id=None, attempt=job.attempts)
                if job.status == JobStatus.Aborting:
                    outcome: Outcome | None = Outcome("abort")
                elif reason == "timeout":
                    outcome = Outcome("fail", error="timeout")
                elif reason == "lease":
                    outcome = Outcome("lost")
                elif job.attempts >= job.max_attempts:
                    outcome = Outcome("fail", error="swept")
                else:
                    outcome = None
                if outcome is None:
                    requeue.append(job)
                else:
                    settled.append((job, await self.apply(cur, job, fence, outcome)))
            await self._jobs.requeue_stuck(cur, requeue)

        for job, result in settled:
            await self.run_callback(job, result)
        return [job.id for job, _ in stuck]

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
