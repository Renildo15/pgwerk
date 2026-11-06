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

from typing import Literal
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from dataclasses import dataclass

from .commons import JobStatus
from .schemas import Job


logger = logging.getLogger(__name__)

OutcomeKind = Literal["complete", "fail", "abort"]


@dataclass(frozen=True)
class Outcome:
    """What the caller observed when the attempt ended."""

    kind: OutcomeKind
    result_json: str | None = None
    """Encoded return value, for ``complete``."""
    error: str | None = None
    """Error text for ``fail`` (a traceback for local workers)."""
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
            error="aborted",
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
