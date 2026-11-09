"""The remote protocol at the repository level (no HTTP)."""

from __future__ import annotations

import uuid

import pytest
import psycopg

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.remote import tokens
from pgwerk.settlement import Outcome
from pgwerk.remote.errors import Scope
from pgwerk.remote.errors import Draining
from pgwerk.remote.errors import LeaseLost
from pgwerk.remote.errors import Unauthorized

from ..tasks import add
from ..tasks import noop
from ..tasks import capture_job
from ..tasks import _callback_jobs

NOOP = "tests.integration.tasks.noop"
ADD = "tests.integration.tasks.add"


async def _enroll(app, queues=("default",), **kw):
    token, row = await app.create_enrollment_token("box", list(queues), **kw)
    return tokens.parse(token), row


async def _register(app, enrollment, queues=("default",), functions=(NOOP, ADD), lang="go"):
    wid, wtoken = await app._remote_repo.register(
        enrollment, name="box.1", queues=list(queues), functions=list(functions), client={"lang": lang}
    )
    return wid, tokens.parse(wtoken)


async def _sql(app, q, *params):
    async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
        cur = await conn.execute(SQL(q).format(w=app._t["worker"], jobs=app._t["jobs"], e=app._t["executions"]), params)
        return await cur.fetchall() if cur.description else None


class TestRegister:
    async def test_registers_within_scope_and_records_catalog(self, app):
        enrollment, _ = await _enroll(app, queues=("default", "gpu"))
        wid, wtoken = await _register(app, enrollment)
        assert wtoken.kind == "worker" and wtoken.id == wid
        [(role, status, functions)] = await _sql(app, "SELECT role, status, functions FROM {w} WHERE id = %s", wid)
        assert (role, status, functions) == ("remote", "active", [NOOP, ADD])
        assert {f["name"]: f["languages"] for f in await app.list_functions()} == {NOOP: ["go"], ADD: ["go"]}

    async def test_queues_outside_the_token_scope_are_refused(self, app):
        enrollment, _ = await _enroll(app, queues=("default",))
        with pytest.raises(Scope):
            await _register(app, enrollment, queues=("default", "admin"))

    async def test_wrong_secret_revoked_and_expired_tokens_are_refused(self, app):
        enrollment, row = await _enroll(app)
        forged = tokens.ParsedToken("enrollment", enrollment.id, "x" * 43)
        with pytest.raises(Unauthorized):
            await _register(app, forged)
        await app.revoke_enrollment_token(row["id"])
        with pytest.raises(Unauthorized):
            await _register(app, enrollment)

    async def test_expired_token_stops_registrations_but_not_existing_workers(self, app):
        enrollment, row = await _enroll(app)
        wid, wtoken = await _register(app, enrollment)
        await _sql(app, "UPDATE pgwerk._pgwerk_enrollment_tokens SET expires_at = NOW() - INTERVAL '1 s'")
        with pytest.raises(Unauthorized):
            await _register(app, enrollment)
        assert (await app._remote_repo.authenticate(wtoken)).worker_id == wid

    async def test_worker_token_is_not_an_enrollment_token(self, app):
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        with pytest.raises(Unauthorized):
            await _register(app, wtoken)
        with pytest.raises(Unauthorized):
            await app._remote_repo.authenticate(enrollment)


class TestClaim:
    async def test_claims_only_its_functions_and_replays_by_claim_id(self, app):
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment, functions=(ADD,))
        await app.enqueue(noop)
        job = await app.enqueue(add, 1, 2)

        claimed = await app._remote_repo.claim(wtoken, "11111111-1111-1111-1111-111111111111", 60)
        assert claimed.job.id == job.id and claimed.lease_ttl == 60
        again = await app._remote_repo.claim(wtoken, "11111111-1111-1111-1111-111111111111", 60)
        assert again.job.lease_id == claimed.job.lease_id  # lost response: same lease back
        assert await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60) is None  # only noop left

    async def test_spent_claim_id_returns_nothing_and_inserts_no_execution(self, app):
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        job = await app.enqueue(noop)
        cid = str(uuid.uuid4())
        claimed = await app._remote_repo.claim(wtoken, cid, 60)
        await app._remote_repo.report(wtoken, claimed.job.lease_id, Outcome("complete"))
        await app.enqueue(noop)

        from pgwerk.remote.repository import ClaimSpent

        with pytest.raises(ClaimSpent):
            await app._remote_repo.claim(wtoken, cid, 60)
        assert len(await app.get_executions(job.id)) == 1

    async def test_draining_worker_cannot_claim_but_can_report(self, app):
        enrollment, _ = await _enroll(app)
        wid, wtoken = await _register(app, enrollment)
        await app.enqueue(noop)
        claimed = await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        await app.revoke_worker(wid, drain=True)

        with pytest.raises(Draining):
            await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        await app._remote_repo.heartbeat(wtoken, claimed.job.lease_id, 60)
        assert (await app._remote_repo.report(wtoken, claimed.job.lease_id, Outcome("complete"))).status == "complete"

    async def test_revoked_worker_cannot_claim(self, app):
        enrollment, row = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        await app.revoke_enrollment_token(row["id"])
        await app.enqueue(noop)
        with pytest.raises(Unauthorized):
            await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)


class TestHeartbeat:
    async def test_renews_stores_progress_and_signals_abort(self, app):
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        job = await app.enqueue(noop)
        claimed = await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)

        _, abort = await app._remote_repo.heartbeat(wtoken, claimed.job.lease_id, 60, progress={"step": 3})
        assert abort is False
        [(progress,)] = await _sql(app, "SELECT progress FROM {e} WHERE id = %s", claimed.job.lease_id)
        assert progress == {"step": 3}

        await app.abort_job(job.id)
        assert (await app._remote_repo.heartbeat(wtoken, claimed.job.lease_id, 60))[1] is True
        assert (await app._remote_repo.report(wtoken, claimed.job.lease_id, Outcome("abort"))).status == "aborted"

    async def test_lost_lease_is_409(self, app):
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        await app.enqueue(noop)
        claimed = await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        await _sql(app, "UPDATE {jobs} SET lease_id = gen_random_uuid()")
        with pytest.raises(LeaseLost):
            await app._remote_repo.heartbeat(wtoken, claimed.job.lease_id, 60)


class TestReportIdempotency:
    async def test_complete_twice_gives_the_same_answer_and_callback_once(self, app):
        _callback_jobs.clear()
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        job = await app.enqueue(noop, _on_success=capture_job)
        claimed = await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        repo = app._remote_repo

        first = await repo.report(wtoken, claimed.job.lease_id, Outcome("complete", result_json='"ok"'))
        replay = await repo.report(wtoken, claimed.job.lease_id, Outcome("complete", result_json='"other"'))
        assert (first.status, replay.status) == ("complete", "complete")
        assert first.settled is not None and replay.settled is None  # the caller runs callbacks once
        assert (await app.get_job(job.id)).result == "ok"

    async def test_after_the_sweep_lost_it_is_409(self, app):
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        job = await app.enqueue(noop)
        claimed = await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        await _sql(
            app,
            "UPDATE {jobs} SET lease_expires_at = NOW() - INTERVAL '1 s', touched_at = NOW() - INTERVAL '1 h',"
            " started_at = NOW() - INTERVAL '1 h' WHERE id = %s",
            job.id,
        )
        await app.sweep()
        with pytest.raises(LeaseLost):
            await app._remote_repo.report(wtoken, claimed.job.lease_id, Outcome("complete"))

    async def test_after_failure_mode_delete_it_is_settled(self, app):
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        job = await app.enqueue(noop, _failure_mode="delete")
        claimed = await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        repo = app._remote_repo
        assert (await repo.report(wtoken, claimed.job.lease_id, Outcome("fail", error="x"))).status == "failed"
        assert (await repo.report(wtoken, claimed.job.lease_id, Outcome("fail", error="x"))).status == "settled"
        assert await _sql(app, "SELECT 1 FROM {jobs} WHERE id = %s", job.id) == []

    async def test_another_workers_lease_is_409(self, app):
        enrollment, _ = await _enroll(app)
        _, a = await _register(app, enrollment)
        _, b = await _register(app, enrollment)
        await app.enqueue(noop)
        claimed = await app._remote_repo.claim(a, str(uuid.uuid4()), 60)
        with pytest.raises(LeaseLost):
            await app._remote_repo.report(b, claimed.job.lease_id, Outcome("complete"))

    async def test_fail_not_retryable_is_terminal(self, app):
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        job = await app.enqueue(noop, _retry=5)
        claimed = await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        out = await app._remote_repo.report(
            wtoken, claimed.job.lease_id, Outcome("fail", error='{"type": "X"}', retryable=False)
        )
        assert out.status == "failed"
        assert (await app.get_job(job.id)).status == JobStatus.Failed


class TestDeregisterAndIdle:
    async def test_deregister_releases_without_counting_lost(self, app):
        enrollment, _ = await _enroll(app)
        wid, wtoken = await _register(app, enrollment)
        job = await app.enqueue(noop)
        await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        await app._remote_repo.deregister(wtoken)
        row = await app.get_job(job.id)
        assert (row.status, row.lost_count) == (JobStatus.Queued, 0)
        with pytest.raises(Unauthorized):
            await app._remote_repo.authenticate(wtoken)

    async def test_idle_expiry_spares_workers_with_a_live_lease(self, app):
        enrollment, _ = await _enroll(app)
        busy_id, busy = await _register(app, enrollment)
        idle_id, _ = await _register(app, enrollment)
        await app.enqueue(noop)
        await app._remote_repo.claim(busy, str(uuid.uuid4()), 60)
        await _sql(app, "UPDATE {w} SET heartbeat_at = NOW() - INTERVAL '10 minutes'")

        assert await app.expire_idle_remote_workers() == 1
        statuses = dict(await _sql(app, "SELECT id::text, status FROM {w} WHERE role = 'remote'"))
        assert (statuses[busy_id], statuses[idle_id]) == ("active", "stopped")

    async def test_draining_worker_stops_once_it_holds_nothing(self, app):
        enrollment, _ = await _enroll(app)
        wid, wtoken = await _register(app, enrollment)
        await app.enqueue(noop)
        claimed = await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        await app.revoke_worker(wid, drain=True)
        assert await app.expire_idle_remote_workers() == 0  # still holds a lease
        await app._remote_repo.report(wtoken, claimed.job.lease_id, Outcome("complete"))
        assert await app.expire_idle_remote_workers() == 1


class TestRemoteLeaseRecovery:
    async def test_lost_remote_job_comes_back_after_the_remote_ttl(self, app):
        """A vanished remote worker's job is swept once its own lease expires.

        Regression: the rolling-deploy guard also required touched_at to be older than
        local_lease_ttl (300s), so a remote job with a 60s lease came back after ~5 minutes.
        """
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        job = await app.enqueue(noop)
        await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        # Last renewal 61s ago, lease (60s) expired 1s ago: well within local_lease_ttl.
        await _sql(
            app,
            "UPDATE {jobs} SET touched_at = NOW() - INTERVAL '61 s', started_at = NOW() - INTERVAL '61 s',"
            " lease_expires_at = NOW() - INTERVAL '1 s' WHERE id = %s",
            job.id,
        )
        assert app.config.local_lease_ttl > 61

        assert job.id in await app.sweep()
        row = await app.get_job(job.id)
        assert (row.status, row.lost_count, row.attempts) == (JobStatus.Queued, 1, 0)

    async def test_live_remote_lease_is_not_swept(self, app):
        enrollment, _ = await _enroll(app)
        _, wtoken = await _register(app, enrollment)
        await app.enqueue(noop)
        await app._remote_repo.claim(wtoken, str(uuid.uuid4()), 60)
        assert await app.sweep() == []


class TestIdleExpiryDuringLongPoll:
    """Regression: idle expiry used 2 × lease TTL even when a claim long-poll lasts longer,
    so a worker waiting in a long-poll was logged out mid-wait."""

    @pytest.fixture
    def short_ttl(self, app):
        before = (app.config.remote_lease_ttl, app.config.remote_claim_wait)
        app.config.remote_lease_ttl, app.config.remote_claim_wait = 10, 30
        yield
        app.config.remote_lease_ttl, app.config.remote_claim_wait = before

    async def test_worker_inside_a_long_poll_is_not_expired(self, app, short_ttl):
        enrollment, _ = await _enroll(app)
        await _register(app, enrollment)
        # Last call 25s ago: past 2 × TTL (20s), but a 30s long-poll may still be open.
        await _sql(app, "UPDATE {w} SET heartbeat_at = NOW() - INTERVAL '25 s'")
        assert await app.expire_idle_remote_workers() == 0

    async def test_worker_silent_past_a_long_poll_plus_ttl_is_expired(self, app, short_ttl):
        enrollment, _ = await _enroll(app)
        await _register(app, enrollment)
        await _sql(app, "UPDATE {w} SET heartbeat_at = NOW() - INTERVAL '45 s'")  # > 30s + 10s
        assert await app.expire_idle_remote_workers() == 1

    async def test_default_threshold_is_still_twice_the_ttl(self, app):
        enrollment, _ = await _enroll(app)
        await _register(app, enrollment)
        assert (app.config.remote_lease_ttl, app.config.remote_claim_wait) == (60, 30)
        await _sql(app, "UPDATE {w} SET heartbeat_at = NOW() - INTERVAL '100 s'")
        assert await app.expire_idle_remote_workers() == 0
        await _sql(app, "UPDATE {w} SET heartbeat_at = NOW() - INTERVAL '125 s'")
        assert await app.expire_idle_remote_workers() == 1
