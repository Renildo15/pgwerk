"""What a remote job handler receives as ``ctx``."""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from dataclasses import field
from dataclasses import dataclass


if TYPE_CHECKING:
    from .worker import RemoteWorker


@dataclass(frozen=True)
class RemoteJob:
    """The job as the server sent it. Callbacks, retries and dependencies stay on the server."""

    id: str
    function: str
    queue: str
    kwargs: dict[str, Any]
    args: list[Any]
    attempts: int
    max_attempts: int
    timeout_secs: int | None
    meta: Any
    lease_id: str
    lease_ttl: int
    heartbeat_every: int
    lost_count: int = 0
    """Times a worker was lost running this job before (it came back without using a retry)."""

    @classmethod
    def from_claim(cls, body: dict[str, Any]) -> "RemoteJob":
        job = body["job"]
        return cls(
            id=job["id"],
            function=job["function"],
            queue=job["queue"],
            kwargs=job.get("kwargs") or {},
            args=job.get("args") or [],
            attempts=job.get("attempts", 1),
            max_attempts=job.get("max_attempts", 1),
            timeout_secs=job.get("timeout_secs"),
            meta=job.get("meta"),
            lease_id=body["lease_id"],
            lease_ttl=body["lease_ttl"],
            heartbeat_every=body["heartbeat_every"],
            lost_count=job.get("lost_count", 0),
        )


@dataclass
class RemoteContext:
    """Passed as the first argument to handlers that take ``ctx``.

    There is no ``app``: a remote job can't enqueue or query the database. Run jobs
    that need ``ctx.app`` on a local worker.
    """

    job: RemoteJob
    worker: "RemoteWorker"
    _progress: Any = field(default=None, repr=False)
    _aborted: bool = field(default=False, repr=False)

    @property
    def aborted(self) -> bool:
        """``True`` once the server asked this job to stop. Long-running sync code can poll it."""
        return self._aborted

    def heartbeat(self, progress: Any = None) -> None:
        """Report progress (any JSON value); it is sent with the next lease heartbeat.

        The lease is renewed automatically; calling this isn't needed to keep the job.
        """
        self._progress = progress
