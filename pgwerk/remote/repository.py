"""Database access for remote workers: enrollment tokens, worker sessions, revocation.

No HTTP here, so the CLI and tests can use it without Litestar.
"""

from __future__ import annotations

import hmac
import json
import uuid

from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import NoReturn
from datetime import datetime
from dataclasses import dataclass

from psycopg.sql import SQL
from psycopg.rows import dict_row

from . import tokens
from .errors import Scope
from .errors import Draining
from .errors import LeaseLost
from .errors import RateLimited
from .errors import SessionExpired
from .errors import Unauthorized
from ..schemas import JOB_COLS
from ..schemas import Job
from ..commons import JobStatus
from ..commons import DequeueStrategy
from ..settlement import Fence
from ..settlement import Outcome


if TYPE_CHECKING:
    from ..repos import JobRepository
    from ..repos import WorkerRepository
    from ..settlement import Settled
    from ..settlement import Settlement
    from ..connection import Connect


class ClaimSpent(Exception):
    """This claim id already got an attempt, and that attempt is over."""


@dataclass(frozen=True)
class Session:
    """An authenticated remote worker, for the duration of one call."""

    worker_id: str
    queues: list[str]
    functions: list[str]
    status: str


@dataclass(frozen=True)
class Claimed:
    job: Job
    lease_ttl: int


@dataclass(frozen=True)
class Reported:
    """The outcome of ``complete``/``fail``. ``status`` is what the job (or, on a
    replay, the attempt) ended as; ``settled`` holds callbacks to run after commit."""

    status: str
    retry_at: datetime | None = None
    settled: "tuple[Job, Settled] | None" = None


class RemoteRepository:
    def __init__(
        self,
        connect: "Connect",
        tables: dict[str, Any],
        settlement: "Settlement",
        job_repo: "JobRepository | None" = None,
        worker_repo: "WorkerRepository | None" = None,
    ) -> None:
        self._connect = connect
        self._t = tables
        self._settlement = settlement
        self._jobs = job_repo
        self._workers = worker_repo

    # ------------------------------------------------------------------
    # Enrollment tokens (operator side)
    # ------------------------------------------------------------------

    async def create_enrollment_token(
        self, name: str, queues: list[str], expires_at: datetime | None = None
    ) -> tuple[str, dict[str, Any]]:
        """Create an enrollment token scoped to *queues*. Returns ``(token, row)``.

        The token string is only available here; only its hash is stored.
        """
        if not queues:
            raise ValueError("an enrollment token needs at least one queue")
        if any("," in q or not q for q in queues):
            raise ValueError("queue names can't be empty or contain ','")
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT gen_random_uuid()::text AS id")
            row = await cur.fetchone()
            assert row is not None
            token, secret_hash = tokens.generate("enrollment", row["id"])
            await cur.execute(
                SQL("""
                    INSERT INTO {t} (id, name, secret_hash, queues, expires_at)
                    VALUES (%(id)s, %(name)s, %(hash)s, %(queues)s, %(expires_at)s)
                    RETURNING id::text, name, queues, created_at, expires_at, last_used_at, revoked_at
                """).format(t=self._t["enrollment_tokens"]),
                {"id": row["id"], "name": name, "hash": secret_hash, "queues": queues, "expires_at": expires_at},
            )
            created = await cur.fetchone()
            assert created is not None
            return token, created

    async def list_enrollment_tokens(self) -> list[dict[str, Any]]:
        async with await self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    SELECT t.id::text, t.name, t.queues, t.created_at, t.expires_at, t.last_used_at, t.revoked_at,
                           COUNT(w.id) FILTER (WHERE w.status IN ('active', 'draining')) AS live_workers
                    FROM {t} t
                    LEFT JOIN {w} w ON w.enrollment_token_id = t.id
                    GROUP BY t.id
                    ORDER BY t.created_at
                """).format(t=self._t["enrollment_tokens"], w=self._t["worker"])
            )
            return await cur.fetchall()

    async def revoke_enrollment_token(self, token_id: str) -> tuple[bool, list[tuple["Job", "Settled"]]]:
        """Revoke a token and, at once, every worker registered with it.

        Their tokens stop working on the next call and the jobs they hold are
        released back to the queue (not counted as lost). This is the response to a
        compromise, so it is always immediate. There is no drain.

        Returns:
            ``(revoked, released)``. ``revoked`` is ``False`` when the token doesn't
            exist or was already revoked; ``released`` lists jobs whose callbacks the
            caller should run.
        """
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                SQL("""
                    UPDATE {t} SET revoked_at = NOW()
                    WHERE id = %(id)s AND revoked_at IS NULL
                    RETURNING id
                """).format(t=self._t["enrollment_tokens"]),
                {"id": token_id},
            )
            if await cur.fetchone() is None:
                return False, []
            await cur.execute(
                SQL("""
                    UPDATE {w} SET status = 'stopped', session_hash = NULL, expires_at = NOW() + INTERVAL '1 hour'
                    WHERE enrollment_token_id = %(id)s AND status IN ('active', 'draining')
                    RETURNING id::text
                """).format(w=self._t["worker"]),
                {"id": token_id},
            )
            worker_ids = [r["id"] for r in await cur.fetchall()]
            released = await self._settlement.release_worker_jobs(cur, worker_ids, "revoked")
        return True, released

    async def revoke_worker(self, worker_id: str, drain: bool = False) -> tuple[bool, list[tuple["Job", "Settled"]]]:
        """Retire one remote worker.

        Without *drain*: its token stops working now and its jobs are released back
        to the queue (not counted as lost). With *drain*: it becomes ``draining``. It
        can't claim new jobs but may finish and report the ones it holds, then it
        stops (deregister, or idle expiry once it holds no lease).

        This doesn't stop a compromised machine, which still has its enrollment
        token: revoke the enrollment token for that.

        Returns:
            ``(revoked, released)``. ``revoked`` is ``False`` for an unknown,
            non-remote or already stopped worker.
        """
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            if drain:
                await cur.execute(
                    SQL("""
                        UPDATE {w} SET status = 'draining'
                        WHERE id = %(id)s AND role = 'remote' AND status = 'active'
                        RETURNING id
                    """).format(w=self._t["worker"]),
                    {"id": worker_id},
                )
                return await cur.fetchone() is not None, []
            await cur.execute(
                SQL("""
                    UPDATE {w} SET status = 'stopped', session_hash = NULL, expires_at = NOW() + INTERVAL '1 hour'
                    WHERE id = %(id)s AND role = 'remote' AND status IN ('active', 'draining')
                    RETURNING id::text
                """).format(w=self._t["worker"]),
                {"id": worker_id},
            )
            if await cur.fetchone() is None:
                return False, []
            released = await self._settlement.release_worker_jobs(cur, [worker_id], "revoked")
        return True, released

    # ------------------------------------------------------------------
    # Protocol side (worker calls)
    # ------------------------------------------------------------------

    async def register(
        self,
        enrollment: tokens.ParsedToken,
        *,
        name: str,
        queues: list[str],
        functions: list[str],
        labels: dict[str, Any] | None = None,
        client: dict[str, Any] | None = None,
        admit: "Callable[[], None] | None" = None,
        max_workers: int | None = None,
    ) -> tuple[str, str]:
        """Exchange an enrollment token for a new worker identity.

        *admit* runs once the secret checked out and may raise (the server's
        per-token rate limit). *max_workers* caps the token's live workers.

        Returns:
            ``(worker_id, worker_token)``. The worker token is returned only here.

        Raises:
            Unauthorized: unknown, revoked or expired enrollment token, or wrong secret.
            Scope: *queues* not within the token's queues, or no queues/functions.
            RateLimited: the token already has *max_workers* live workers.
        """
        assert self._workers is not None
        if enrollment.kind != "enrollment":
            raise Unauthorized("register needs an enrollment token")
        if not queues or not functions:
            raise Scope("declare at least one queue and one function")
        client = client or {}
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            # Check the secret before writing anything: an UPDATE first would lock the
            # token row for whoever sends its id with a wrong secret.
            await cur.execute(
                SQL("""
                    SELECT secret_hash, queues FROM {t}
                    WHERE id = %(id)s AND revoked_at IS NULL AND (expires_at IS NULL OR expires_at > NOW())
                """).format(t=self._t["enrollment_tokens"]),
                {"id": enrollment.id},
            )
            row = await cur.fetchone()
            if row is None or not hmac.compare_digest(bytes(row["secret_hash"]), enrollment.secret_hash):
                raise Unauthorized("invalid enrollment token")
            outside = sorted(set(queues) - set(row["queues"]))
            if outside:
                raise Scope(f"queues not allowed by this token: {', '.join(outside)}")
            if admit is not None:
                admit()
            await cur.execute(
                SQL("""
                    UPDATE {t} SET last_used_at = NOW()
                    WHERE id = %(id)s AND revoked_at IS NULL
                    RETURNING id
                """).format(t=self._t["enrollment_tokens"]),
                {"id": enrollment.id},
            )
            if await cur.fetchone() is None:  # revoked meanwhile
                raise Unauthorized("invalid enrollment token")
            if max_workers is not None:
                await cur.execute(
                    SQL("""
                        SELECT count(*) AS n FROM {w}
                        WHERE enrollment_token_id = %(id)s AND status IN ('active', 'draining')
                    """).format(w=self._t["worker"]),
                    {"id": enrollment.id},
                )
                live = await cur.fetchone()
                if live is not None and live["n"] >= max_workers:
                    raise RateLimited(f"this token already has {max_workers} live workers")

            worker_id = str(uuid.uuid4())
            worker_token, session_hash = tokens.generate("worker", worker_id)
            await cur.execute(
                SQL("""
                    INSERT INTO {w} (id, name, queue, status, role, metadata, heartbeat_at, functions,
                                     session_hash, enrollment_token_id)
                    VALUES (%(id)s, %(name)s, %(queue)s, 'active', 'remote', %(meta)s, NOW(), %(functions)s,
                            %(hash)s, %(token_id)s)
                """).format(w=self._t["worker"]),
                {
                    "id": worker_id,
                    "name": name,
                    "queue": ",".join(queues),
                    "meta": json.dumps({"labels": labels or {}, "client": client, "queues": queues}),
                    "functions": functions,
                    "hash": session_hash,
                    "token_id": enrollment.id,
                },
            )
            await self._workers.upsert_functions(cur, functions, str(client.get("lang") or "unknown"))
        return worker_id, worker_token

    async def _authenticate(self, cur: Any, token: tokens.ParsedToken, *, claiming: bool = False) -> Session:
        """Check a worker token and record liveness, in the caller's transaction.

        It is an ``UPDATE`` on the worker row, so it also locks it: a revoke that
        runs concurrently waits for this call's write and then sees (and releases)
        what it did. The token is checked on every call (no cache): revocation takes
        effect on the next one.
        """
        if token.kind != "worker":
            raise Unauthorized("this endpoint needs a worker token")
        await cur.execute(
            SQL("""
                UPDATE {w} w SET heartbeat_at = NOW()
                FROM {t} t
                WHERE w.id = %(id)s
                  AND w.session_hash = %(hash)s
                  AND w.role = 'remote'
                  AND w.status IN ('active', 'draining')
                  AND t.id = w.enrollment_token_id
                  AND t.revoked_at IS NULL
                RETURNING w.id::text, w.queue, w.functions, w.status
            """).format(w=self._t["worker"], t=self._t["enrollment_tokens"]),
            {"id": token.id, "hash": token.secret_hash},
        )
        row = await cur.fetchone()
        if row is None:
            await self._why_unauthorized(cur, token)
        if claiming and row["status"] == "draining":
            raise Draining("this worker is draining: finish the jobs you hold, then deregister")
        return Session(row["id"], row["queue"].split(","), row["functions"] or [], row["status"])

    async def _why_unauthorized(self, cur: Any, token: tokens.ParsedToken) -> NoReturn:
        """Raise :class:`SessionExpired` when idle expiry ended this worker's session
        (it may register again), else :class:`Unauthorized`.

        Idle expiry keeps the session hash; revocation, deregistration and the end of a
        drain clear it, so only the holder of the right token learns it expired.
        """
        await cur.execute(
            SQL("""
                SELECT w.status, w.session_hash
                FROM {w} w JOIN {t} t ON t.id = w.enrollment_token_id
                WHERE w.id = %(id)s AND w.role = 'remote' AND t.revoked_at IS NULL
            """).format(w=self._t["worker"], t=self._t["enrollment_tokens"]),
            {"id": token.id},
        )
        row = await cur.fetchone()
        if (
            row is not None
            and row["status"] == "stopped"
            and row["session_hash"] is not None
            and hmac.compare_digest(bytes(row["session_hash"]), token.secret_hash)
        ):
            raise SessionExpired("this worker's session expired (nothing heard from it for too long); register again")
        raise Unauthorized("invalid or revoked worker token")

    async def authenticate(self, token: tokens.ParsedToken, *, claiming: bool = False) -> Session:
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            return await self._authenticate(cur, token, claiming=claiming)

    async def claim(self, token: tokens.ParsedToken, claim_id: str, lease_ttl: int) -> Claimed | None:
        """Claim one job for the worker, or ``None`` if there is nothing to claim.

        A retried claim with the same *claim_id* (its response was lost) gets the same
        lease back while the attempt still owns the job. Once that attempt is over
        (settled, lost, released), the claim id is spent and :class:`ClaimSpent` is
        raised: the server answers "nothing" at once, and the client claims again
        with a fresh id.
        """
        assert self._jobs is not None
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            session = await self._authenticate(cur, token, claiming=True)
            await cur.execute(
                SQL("SELECT id::text AS lease_id, job_id::text FROM {e} WHERE claim_id = %(cid)s").format(
                    e=self._t["executions"]
                ),
                {"cid": claim_id},
            )
            previous = await cur.fetchone()
            if previous is not None:
                await cur.execute(
                    SQL(
                        "SELECT "
                        + JOB_COLS
                        + """ FROM {jobs}
                        WHERE id = %(jid)s AND lease_id = %(lease)s AND worker_id = %(wid)s
                          AND status IN ('active', 'aborting')"""
                    ).format(jobs=self._t["jobs"]),
                    {"jid": previous["job_id"], "lease": previous["lease_id"], "wid": session.worker_id},
                )
                row = await cur.fetchone()
                if row is None:
                    raise ClaimSpent()
                job = Job.from_row(row, self._jobs._serializer)
                return Claimed(job, job.heartbeat_secs or lease_ttl)

            jobs = await self._workers_dequeue(cur, session, lease_ttl)
            if not jobs:
                return None
            job = jobs[0]
            await cur.execute(
                SQL("UPDATE {e} SET claim_id = %(cid)s WHERE id = %(lease)s").format(e=self._t["executions"]),
                {"cid": claim_id, "lease": job.lease_id},
            )
            return Claimed(job, job.heartbeat_secs or lease_ttl)

    async def _workers_dequeue(self, cur: Any, session: Session, lease_ttl: int) -> list[Job]:
        assert self._workers is not None
        return await self._workers.dequeue_in(
            cur,
            session.worker_id,
            session.queues,
            1,
            DequeueStrategy.Priority,
            lease_ttl=lease_ttl,
            functions=session.functions,
        )

    async def heartbeat(
        self, token: tokens.ParsedToken, lease_id: str, lease_ttl: int, progress: Any = None
    ) -> tuple[datetime, bool]:
        """Renew one lease. Returns ``(lease_expires_at, abort_requested)``.

        Raises:
            LeaseLost: the attempt no longer owns the job; the client must stop it.
        """
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            session = await self._authenticate(cur, token)
            await cur.execute(
                SQL("""
                    UPDATE {jobs}
                    SET lease_expires_at = NOW() + COALESCE(heartbeat_secs, %(ttl)s) * INTERVAL '1 second',
                        touched_at = NOW()
                    WHERE lease_id = %(lease)s AND worker_id = %(wid)s AND status IN ('active', 'aborting')
                    RETURNING status, lease_expires_at
                """).format(jobs=self._t["jobs"]),
                {"ttl": lease_ttl, "lease": lease_id, "wid": session.worker_id},
            )
            row = await cur.fetchone()
            if row is None:
                raise LeaseLost("this attempt no longer owns the job")
            if progress is not None:
                await cur.execute(
                    SQL("UPDATE {e} SET progress = %(p)s WHERE id = %(lease)s").format(e=self._t["executions"]),
                    {"p": json.dumps(progress), "lease": lease_id},
                )
            return row["lease_expires_at"], row["status"] == JobStatus.Aborting.value

    async def report(self, token: tokens.ParsedToken, lease_id: str, outcome: Outcome) -> Reported:
        """Settle an attempt from ``complete``/``fail``. Idempotent per lease.

        - The attempt is running and still owns the job: it settles.
        - The attempt already settled (complete/failed/aborted): its status is
          returned again and nothing runs twice.
        - The attempt was lost or superseded, or belongs to another worker:
          :class:`LeaseLost`.
        - No execution row (the job was deleted, e.g. ``failure_mode="delete"``):
          ``"settled"``. Ownership can't be checked then, and nothing is revealed.
        """
        assert self._jobs is not None
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            session = await self._authenticate(cur, token)
            await cur.execute(
                SQL("SELECT job_id::text, worker_id::text, status FROM {e} WHERE id = %(lease)s FOR UPDATE").format(
                    e=self._t["executions"]
                ),
                {"lease": lease_id},
            )
            execution = await cur.fetchone()
            if execution is None:
                return Reported("settled")
            if execution["worker_id"] is not None and execution["worker_id"] != session.worker_id:
                raise LeaseLost("this attempt no longer owns the job")
            if execution["status"] in ("complete", "failed", "aborted"):
                return Reported(execution["status"])
            if execution["status"] != "running":
                raise LeaseLost("this attempt no longer owns the job")

            await cur.execute(
                SQL("SELECT " + JOB_COLS + " FROM {jobs} WHERE id = %(id)s").format(jobs=self._t["jobs"]),
                {"id": execution["job_id"]},
            )
            row = await cur.fetchone()
            if row is None:
                return Reported("settled")
            job = Job.from_row(row, self._jobs._serializer)
            fence = Fence(job.id, worker_id=session.worker_id, attempt=job.attempts, lease_id=lease_id)
            settled = await self._settlement.apply(cur, job, fence, outcome)
            if settled.status is None:
                raise LeaseLost("this attempt no longer owns the job")
            return Reported(settled.status.value, settled.retry_at, (job, settled))

    async def deregister(self, token: tokens.ParsedToken) -> list[tuple[Job, "Settled"]]:
        """Stop the worker; jobs it still holds go back to the queue (not counted as lost)."""
        async with await self._connect() as conn, conn.transaction(), conn.cursor(row_factory=dict_row) as cur:
            session = await self._authenticate(cur, token)
            released = await self._settlement.release_worker_jobs(cur, [session.worker_id], "deregistered")
            await cur.execute(
                SQL("""
                    UPDATE {w} SET status = 'stopped', session_hash = NULL, expires_at = NOW() + INTERVAL '1 hour'
                    WHERE id = %(id)s
                """).format(w=self._t["worker"]),
                {"id": session.worker_id},
            )
            return released

    async def expire_idle(self, lease_ttl: int, claim_wait: int = 0) -> int:
        """Stop remote workers that hold no live lease and are idle or draining.

        Idle means no call for ``max(2 × lease_ttl, claim_wait + lease_ttl)``. A worker
        waiting in a claim long-poll makes no call for up to ``claim_wait`` seconds, so
        the threshold always leaves a full lease TTL of slack after the longest
        long-poll. A draining worker stops as soon as it holds no live lease.

        Their tokens stop working. An idle worker's session is only expired, not
        revoked: it keeps its hash, so its next call gets ``session_expired`` and it
        registers again. Idleness is often the network's or the server's fault (an
        outage longer than the threshold), and a worker that comes back must not stay
        out for good. A drained worker's hash is cleared: it was retired on purpose.
        """
        async with await self._connect() as conn, conn.cursor() as cur:
            await cur.execute(
                SQL("""
                    UPDATE {w} w
                    SET status = 'stopped',
                        session_hash = CASE WHEN w.status = 'draining' THEN NULL ELSE w.session_hash END,
                        expires_at = NOW() + INTERVAL '1 hour'
                    WHERE w.role = 'remote'
                      AND w.status IN ('active', 'draining')
                      AND (w.status = 'draining'
                           OR w.heartbeat_at < NOW() - %(idle)s * INTERVAL '1 second')
                      AND NOT EXISTS (
                          SELECT 1 FROM {jobs} j
                          WHERE j.worker_id = w.id
                            AND j.status IN ('active', 'aborting')
                            AND j.lease_expires_at > NOW()
                      )
                """).format(w=self._t["worker"], jobs=self._t["jobs"]),
                {"idle": max(2 * lease_ttl, claim_wait + lease_ttl)},
            )
            return cur.rowcount
