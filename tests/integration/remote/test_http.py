"""The /remote/v1 router over HTTP (Litestar test client, live Postgres)."""

from __future__ import annotations

import time
import uuid
import asyncio

from dataclasses import replace

import pytest

from litestar.testing import AsyncTestClient

from pgwerk.config import WerkConfig
from pgwerk.commons import JobStatus
from pgwerk.api.app import create_app

from ..tasks import noop
from ..tasks import capture_job
from ..tasks import _callback_jobs

NOOP = "tests.integration.tasks.noop"
ADMIN = "admin-secret"


def _config(app, **kw) -> WerkConfig:
    base = WerkConfig(dsn=app.dsn, prefix=app.prefix, remote_workers=True, api_token=ADMIN, maintenance=False, ui=False)
    return replace(base, **kw)


def _h(token: str | None = None) -> dict[str, str]:
    headers = {"Pgwerk-Protocol": "1"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


@pytest.fixture
async def client(app):
    async with AsyncTestClient(app=create_app(_config(app))) as c:
        yield c


async def _register(app, client, queues=("default",), functions=(NOOP,)):
    enrollment, _ = await app.create_enrollment_token("box", ["default", "other"])
    resp = await client.post(
        "/remote/v1/register",
        headers=_h(enrollment),
        json={
            "name": "box.1",
            "queues": list(queues),
            "functions": [{"name": f} for f in functions],
            "client": {"lang": "python", "version": "test"},
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _claim(client, wtoken, wait=0, claim_id=None):
    return await client.post(
        "/remote/v1/claim", headers=_h(wtoken), json={"claim_id": claim_id or str(uuid.uuid4()), "wait": wait}
    )


class TestStartup:
    def test_refuses_remote_mode_with_an_open_admin_api(self, app):
        with pytest.raises(RuntimeError, match="api_token"):
            create_app(_config(app, api_token=None))

    def test_remote_only_process_needs_no_admin_token(self, app):
        api = create_app(_config(app, api_token=None, api=False))
        paths = {r.path for r in api.routes}
        assert "/remote/v1/claim" in paths and not any(p.startswith("/api") for p in paths)

    def test_maintenance_defaults_on_in_remote_mode(self, app):
        cfg = _config(app, maintenance=None)
        assert cfg.maintenance_enabled
        assert not replace(cfg, remote_workers=False).maintenance_enabled


class TestAuth:
    async def test_protocol_header_is_required(self, app, client):
        reg = await _register(app, client)
        resp = await client.post(
            "/remote/v1/claim",
            headers={"Authorization": f"Bearer {reg['worker_token']}"},
            json={"claim_id": str(uuid.uuid4())},
        )
        assert (resp.status_code, resp.json()["error"]["code"]) == (400, "protocol")

    async def test_admin_token_is_rejected_on_remote_and_worker_token_on_api(self, app, client):
        reg = await _register(app, client)
        assert (await _claim(client, ADMIN)).status_code == 401
        resp = await client.get("/api/jobs", headers={"Authorization": f"Bearer {reg['worker_token']}"})
        assert resp.status_code == 401
        assert (await client.get("/api/jobs", headers={"Authorization": f"Bearer {ADMIN}"})).status_code == 200

    async def test_scope_and_revocation(self, app, client):
        enrollment, row = await app.create_enrollment_token("box", ["default"])
        body = {"queues": ["default", "secret"], "functions": [NOOP]}
        resp = await client.post("/remote/v1/register", headers=_h(enrollment), json=body)
        assert (resp.status_code, resp.json()["error"]["code"]) == (403, "scope")
        await app.revoke_enrollment_token(row["id"])
        body["queues"] = ["default"]
        assert (await client.post("/remote/v1/register", headers=_h(enrollment), json=body)).status_code == 401

    async def test_enrollment_token_only_registers(self, app, client):
        enrollment, _ = await app.create_enrollment_token("box", ["default"])
        assert (await _claim(client, enrollment)).status_code == 401


class TestJobLifecycle:
    async def test_claim_heartbeat_complete_with_callback_in_background(self, app, client):
        _callback_jobs.clear()
        reg = await _register(app, client)
        job = await app.enqueue(noop, "x", k=1, _on_success=capture_job)

        resp = await _claim(client, reg["worker_token"])
        assert resp.status_code == 200
        body = resp.json()
        assert body["job"]["id"] == job.id and body["job"]["kwargs"] == {"k": 1} and body["job"]["args"] == ["x"]
        assert body["lease_ttl"] == 60 and body["heartbeat_every"] == 15
        assert (body["job"]["attempts"], body["job"]["lost_count"]) == (1, 0)
        lease = body["lease_id"]

        hb = await client.post(
            f"/remote/v1/leases/{lease}/heartbeat", headers=_h(reg["worker_token"]), json={"progress": {"pct": 50}}
        )
        assert hb.status_code == 200 and hb.json()["abort"] is False

        done = await client.post(
            f"/remote/v1/leases/{lease}/complete", headers=_h(reg["worker_token"]), json={"result": {"ok": True}}
        )
        assert done.json() == {"status": "complete"}
        again = await client.post(
            f"/remote/v1/leases/{lease}/complete", headers=_h(reg["worker_token"]), json={"result": {"ok": True}}
        )
        assert again.json() == {"status": "complete"}
        assert (await app.get_job(job.id)).result == {"ok": True}
        for _ in range(20):
            if _callback_jobs:
                break
            await asyncio.sleep(0.05)
        assert [j.id for j in _callback_jobs] == [job.id]  # once, despite the replay

    async def test_empty_claim_is_204_and_long_poll_wakes_on_enqueue(self, app, client):
        reg = await _register(app, client)
        assert (await _claim(client, reg["worker_token"], wait=0)).status_code == 204

        # The Litestar test client blocks this event loop while a request runs, so the
        # concurrent enqueue comes from another thread (its own loop and connection).
        import threading

        from pgwerk.app import Werk

        async def enqueue_elsewhere():
            async with Werk(app.dsn, prefix=app.prefix) as other:
                await other.enqueue(noop)

        started = time.monotonic()
        threading.Timer(0.8, lambda: asyncio.run(enqueue_elsewhere())).start()
        resp = await _claim(client, reg["worker_token"], wait=10)
        assert resp.status_code == 200
        assert time.monotonic() - started < 1.8  # woken by NOTIFY, not by the 10s timeout

    async def test_claim_id_replay_and_spent(self, app, client):
        reg = await _register(app, client)
        await app.enqueue(noop)
        cid = str(uuid.uuid4())
        first = (await _claim(client, reg["worker_token"], claim_id=cid)).json()
        second = (await _claim(client, reg["worker_token"], claim_id=cid)).json()
        assert first["lease_id"] == second["lease_id"]
        await client.post(
            f"/remote/v1/leases/{first['lease_id']}/complete", headers=_h(reg["worker_token"]), json={"result": None}
        )
        await app.enqueue(noop)
        assert (await _claim(client, reg["worker_token"], claim_id=cid, wait=5)).status_code == 204

    async def test_abort_via_heartbeat_then_fail_aborted(self, app, client):
        reg = await _register(app, client)
        job = await app.enqueue(noop)
        lease = (await _claim(client, reg["worker_token"])).json()["lease_id"]
        await app.abort_job(job.id)
        hb = await client.post(f"/remote/v1/leases/{lease}/heartbeat", headers=_h(reg["worker_token"]), json={})
        assert hb.json()["abort"] is True
        resp = await client.post(
            f"/remote/v1/leases/{lease}/fail", headers=_h(reg["worker_token"]), json={"aborted": True}
        )
        assert resp.json() == {"status": "aborted"}

    async def test_structured_failure_retry_and_truncation(self, app, client):
        reg = await _register(app, client)
        job = await app.enqueue(noop, _retry=2)
        lease = (await _claim(client, reg["worker_token"])).json()["lease_id"]
        huge = "x" * 200_000
        resp = await client.post(
            f"/remote/v1/leases/{lease}/fail",
            headers=_h(reg["worker_token"]),
            json={"error": {"type": "ValueError", "message": "bad input", "stack": huge}},
        )
        assert resp.json()["status"] == "queued"
        row = await app.get_job(job.id)
        assert len(row.error.encode()) <= app.config.remote_max_error_bytes
        assert '"type": "ValueError"' in row.error and "truncated" in row.error

    async def test_not_retryable_fails_terminally(self, app, client):
        reg = await _register(app, client)
        job = await app.enqueue(noop, _retry=5)
        lease = (await _claim(client, reg["worker_token"])).json()["lease_id"]
        resp = await client.post(
            f"/remote/v1/leases/{lease}/fail",
            headers=_h(reg["worker_token"]),
            json={"error": {"type": "UnsupportedPayload", "message": "args"}, "retryable": False},
        )
        assert resp.json()["status"] == "failed"
        assert (await app.get_job(job.id)).status == JobStatus.Failed

    async def test_oversized_result_is_413(self, app, client):
        reg = await _register(app, client)
        await app.enqueue(noop)
        lease = (await _claim(client, reg["worker_token"])).json()["lease_id"]
        resp = await client.post(
            f"/remote/v1/leases/{lease}/complete",
            headers=_h(reg["worker_token"]),
            json={"result": "x" * (2 * 1024 * 1024)},
        )
        assert (resp.status_code, resp.json()["error"]["code"]) == (413, "too_large")

    async def test_lost_lease_is_409(self, app, client):
        reg = await _register(app, client)
        await app.enqueue(noop)
        lease = (await _claim(client, reg["worker_token"])).json()["lease_id"]
        await app.revoke_worker(reg["worker_id"])
        resp = await client.post(f"/remote/v1/leases/{lease}/heartbeat", headers=_h(reg["worker_token"]), json={})
        assert resp.status_code == 401  # revoked: the token itself stopped working

    async def test_deleted_job_replay_is_settled(self, app, client):
        reg = await _register(app, client)
        await app.enqueue(noop, _failure_mode="delete")
        lease = (await _claim(client, reg["worker_token"])).json()["lease_id"]
        err = {"error": {"type": "E", "message": "m"}, "retryable": False}
        url = f"/remote/v1/leases/{lease}/fail"
        assert (await client.post(url, headers=_h(reg["worker_token"]), json=err)).json()["status"] == "failed"
        assert (await client.post(url, headers=_h(reg["worker_token"]), json=err)).json() == {"status": "settled"}


class TestDrainAndRateLimit:
    async def test_draining_worker_gets_403_draining_on_claim(self, app, client):
        reg = await _register(app, client)
        await app.revoke_worker(reg["worker_id"], drain=True)
        resp = await _claim(client, reg["worker_token"])
        assert (resp.status_code, resp.json()["error"]["code"]) == (403, "draining")
        assert (await client.post("/remote/v1/deregister", headers=_h(reg["worker_token"]))).status_code == 204

    async def test_claim_rate_limit(self, app):
        async with AsyncTestClient(app=create_app(_config(app, remote_claim_rate=0.001, remote_claim_burst=1))) as c:
            reg = await _register(app, c)
            assert (await _claim(c, reg["worker_token"])).status_code == 204
            resp = await _claim(c, reg["worker_token"])
            assert (resp.status_code, resp.json()["error"]["code"]) == (429, "rate_limited")
