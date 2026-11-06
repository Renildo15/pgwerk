"""Enrollment tokens and revocation (operator side)."""

from __future__ import annotations

import uuid

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.commons import DequeueStrategy

from ..tasks import noop
from ..tasks import on_stopped
from ..tasks import _callback_log
from ..tasks import clear_callback_log


async def _remote_worker(app, token_id: str, queues=("default",)) -> str:
    """A registered remote worker (as register will create it)."""
    wid = str(uuid.uuid4())
    await app._worker_repo.register(wid, "remote-box", list(queues), "{}", role="remote")
    async with await app._connect() as conn:
        await conn.execute(
            SQL("UPDATE {w} SET session_hash = 'x', enrollment_token_id = %s WHERE id = %s").format(w=app._t["worker"]),
            (token_id, wid),
        )
    return wid


async def _worker_row(app, wid):
    async with await app._connect() as conn:
        cur = await conn.execute(
            SQL("SELECT status, session_hash FROM {w} WHERE id = %s").format(w=app._t["worker"]), (wid,)
        )
        return await cur.fetchone()


async def _claim(app, wid):
    return await app._worker_repo.dequeue(wid, ["default"], 10, DequeueStrategy.Priority, lease_ttl=60)


class TestEnrollmentTokens:
    async def test_create_shows_the_secret_once_and_stores_only_the_hash(self, app):
        from pgwerk.remote import tokens

        token, row = await app.create_enrollment_token("box-1", ["render"])
        parsed = tokens.parse(token)
        assert (parsed.kind, parsed.id, row["queues"]) == ("enrollment", row["id"], ["render"])
        async with await app._connect() as conn:
            cur = await conn.execute(
                SQL("SELECT secret_hash FROM {t} WHERE id = %s").format(t=app._t["enrollment_tokens"]), (row["id"],)
            )
            assert bytes((await cur.fetchone())[0]) == parsed.secret_hash
        assert "secret_hash" not in row

    async def test_list_and_expiry(self, app):
        await app.create_enrollment_token("a", ["q"], expires_in=3600)
        [row] = await app.list_enrollment_tokens()
        assert (row["name"], row["live_workers"], row["revoked_at"]) == ("a", 0, None)
        assert row["expires_at"] is not None


class TestRevocation:
    async def test_token_revoke_stops_its_workers_and_releases_jobs_without_counting_lost(self, app):
        _, tok = await app.create_enrollment_token("box", ["default"])
        wid = await _remote_worker(app, tok["id"])
        job = await app.enqueue(noop, _max_lost=0)  # one lost worker would fail it
        [claimed] = await _claim(app, wid)

        assert await app.revoke_enrollment_token(tok["id"])

        status, session = await _worker_row(app, wid)
        assert (status, session) == ("stopped", None)
        row = await app.get_job(job.id)
        assert (row.status, row.attempts, row.lost_count, row.lease_id) == (JobStatus.Queued, 0, 0, None)
        [execution] = await app.get_executions(job.id)
        assert (execution.status.value, execution.error) == ("lost", "revoked")
        assert not await app.revoke_enrollment_token(tok["id"])  # already revoked

    async def test_worker_revoke_is_immediate_by_default(self, app):
        _, tok = await app.create_enrollment_token("box", ["default"])
        wid = await _remote_worker(app, tok["id"])
        job = await app.enqueue(noop)
        await _claim(app, wid)

        assert await app.revoke_worker(wid)

        assert (await _worker_row(app, wid))[0] == "stopped"
        assert (await app.get_job(job.id)).status == JobStatus.Queued
        [tok_row] = await app.list_enrollment_tokens()
        assert tok_row["revoked_at"] is None  # the enrollment token is untouched

    async def test_worker_drain_keeps_its_jobs(self, app):
        _, tok = await app.create_enrollment_token("box", ["default"])
        wid = await _remote_worker(app, tok["id"])
        job = await app.enqueue(noop)
        await _claim(app, wid)

        assert await app.revoke_worker(wid, drain=True)

        status, session = await _worker_row(app, wid)
        assert status == "draining" and session is not None
        assert (await app.get_job(job.id)).status == JobStatus.Active
        assert not await app.revoke_worker(wid, drain=True)  # only an active worker can start draining

    async def test_releasing_an_aborting_job_ends_it_aborted(self, app):
        clear_callback_log()
        _, tok = await app.create_enrollment_token("box", ["default"])
        wid = await _remote_worker(app, tok["id"])
        job = await app.enqueue(noop, _on_stopped=on_stopped)
        await _claim(app, wid)
        await app.abort_job(job.id)

        await app.revoke_worker(wid)

        assert (await app.get_job(job.id)).status == JobStatus.Aborted
        assert _callback_log == [("stopped", job.id)]
        clear_callback_log()

    async def test_local_workers_cant_be_revoked(self, app):
        wid = str(uuid.uuid4())
        await app._worker_repo.register(wid, "local", ["default"], "{}")
        assert not await app.revoke_worker(wid)
