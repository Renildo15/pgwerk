"""Database access for remote workers: enrollment tokens, worker sessions, revocation.

No HTTP here, so the CLI and tests can use it without Litestar.
"""

from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from datetime import datetime

from psycopg.sql import SQL
from psycopg.rows import dict_row

from . import tokens


if TYPE_CHECKING:
    from ..schemas import Job
    from ..settlement import Settled
    from ..settlement import Settlement
    from ..connection import Connect


class RemoteRepository:
    def __init__(self, connect: "Connect", tables: dict[str, Any], settlement: "Settlement") -> None:
        self._connect = connect
        self._t = tables
        self._settlement = settlement

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
