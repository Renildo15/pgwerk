"""Regression tests for the remote-worker hardening found by the robustness and security tests."""

from __future__ import annotations

import os
import uuid
import contextlib
import base64
import pickle

from dataclasses import replace

import pytest

from litestar.testing import AsyncTestClient

from pgwerk.app import Werk
from pgwerk.config import WerkConfig
from pgwerk.api.app import create_app
from pgwerk.serializers import PickleSerializer

from ..tasks import noop


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


async def _register(app, client, queues=("default",), functions=(NOOP,), enrollment=None):
    if enrollment is None:
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


async def _complete(app, client, result):
    reg = await _register(app, client)
    job = await app.enqueue(noop)
    claimed = (await _claim(client, reg["worker_token"])).json()
    resp = await client.post(
        f"/remote/v1/leases/{claimed['lease_id']}/complete", headers=_h(reg["worker_token"]), json={"result": result}
    )
    assert resp.status_code == 200, resp.text
    return job.id


class TestRemoteResultsNeverReachTheAppSerializer:
    """A remote worker's result must not be deserialized as code by any reader.

    Regression: results were stored as plain JSON, and decode() fed JSON strings (and
    even the raw text of objects) to the app's serializer. A process reading the job with
    PickleSerializer (the user's app, `werk jobs`, `werk dashboard`) ran pickle.loads on
    bytes chosen by the remote worker. PickleSerializer's lenient base64 decoding even
    made object-shaped results exploitable.
    """

    @staticmethod
    def _payload(marker: str) -> str:
        class _Boom:
            def __reduce__(self):
                return (os.system, (f"touch {marker}",))

        return base64.b64encode(pickle.dumps(_Boom())).decode()

    @pytest.mark.parametrize("shape", ["string", "object", "list"])
    async def test_pickle_reader_gets_the_plain_value(self, app, client, tmp_path, shape):
        marker = str(tmp_path / "pwned")
        evil = self._payload(marker)
        result = {"string": evil, "object": {"x": evil}, "list": [evil]}[shape]
        job_id = await _complete(app, client, result)

        reader = Werk(app.dsn, prefix=app.prefix, serializer=PickleSerializer(), auto_migrate=False)
        job = await reader.get_job(job_id)
        executions = await reader.get_executions(job_id)

        assert not os.path.exists(marker), "a remote result was unpickled"
        assert job.result == result
        assert [e.result for e in executions if e.result is not None] == [result]

    async def test_json_reader_and_api_see_the_value_unwrapped(self, app, client):
        job_id = await _complete(app, client, {"uri": "s3://b/k.png", "size": 3})
        assert (await app.get_job(job_id)).result == {"uri": "s3://b/k.png", "size": 3}
        resp = await client.get(f"/api/jobs/{job_id}", headers={"Authorization": f"Bearer {ADMIN}"})
        assert resp.json()["result"] == {"uri": "s3://b/k.png", "size": 3}


class TestConnectionPool:
    """Regression: every operation opened a new Postgres backend. Under concurrent
    remote requests the API process exhausted max_connections for the whole database."""

    @staticmethod
    async def _backends(dsn: str) -> int:
        import psycopg

        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as c:
            cur = await c.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"
            )
            return (await cur.fetchone())[0]

    async def test_a_pooled_werk_never_exceeds_its_pool(self, app):
        import asyncio

        job = await app.enqueue(noop)
        baseline = await self._backends(app.dsn)
        werk = Werk(app.dsn, prefix=app.prefix, auto_migrate=False, pool_size=4)
        await werk.connect()
        peak = 0
        done = asyncio.Event()

        async def sample():
            nonlocal peak
            while not done.is_set():
                peak = max(peak, await self._backends(app.dsn) - baseline)
                await asyncio.sleep(0.005)

        sampler = asyncio.create_task(sample())
        try:
            await asyncio.gather(*(werk.get_job(job.id) for _ in range(60)))
            await asyncio.gather(*(werk.enqueue(noop) for _ in range(20)))
        finally:
            done.set()
            await sampler
            await werk.disconnect()
        assert 1 <= peak <= 4, peak
        assert await self._backends(app.dsn) <= baseline  # disconnect closed them

    def test_the_api_process_uses_a_pool_by_default(self, app):
        from pgwerk.api.deps import API_POOL_SIZE

        assert API_POOL_SIZE == 20
        assert _config(app).pool_size is None  # unset: the API applies its default


@contextlib.asynccontextmanager
async def _serve(app, **cfg):
    """A real server (uvicorn in a thread): the test client can't serve concurrent requests."""
    import time
    import socket
    import asyncio
    import threading

    import uvicorn

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    srv = uvicorn.Server(
        uvicorn.Config(
            create_app(_config(app, api=False, api_token=None, **cfg)),
            host="127.0.0.1",
            port=port,
            log_level="error",
            timeout_graceful_shutdown=1,
        )
    )
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    for _ in range(100):
        if srv.started:
            break
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        srv.should_exit = True
        await asyncio.to_thread(thread.join, 10)


@pytest.fixture
async def live_server(app):
    async with _serve(app) as url:
        yield url


class TestOneWakePerGroup:
    """Regression: one enqueue woke every idle long-poll, and each ran a full claim
    transaction (and, before the pool, opened a connection): 50 idle workers cost about
    124 transactions per enqueue."""

    async def test_one_enqueue_costs_a_couple_of_claims_not_one_per_idle_worker(self, app, live_server, monkeypatch):
        import asyncio

        import httpx

        from pgwerk.remote.repository import RemoteRepository

        calls = 0
        original = RemoteRepository.claim

        async def counting(self, *a, **kw):
            nonlocal calls
            calls += 1
            return await original(self, *a, **kw)

        monkeypatch.setattr(RemoteRepository, "claim", counting)
        enrollment, _ = await app.create_enrollment_token("box", ["default"])
        async with httpx.AsyncClient(base_url=f"{live_server}/remote/v1", timeout=20) as http:
            tokens = []
            for i in range(20):
                resp = await http.post(
                    "/register",
                    headers=_h(enrollment),
                    json={"name": f"w{i}", "queues": ["default"], "functions": [{"name": NOOP}]},
                )
                assert resp.status_code == 201, resp.text
                tokens.append(resp.json()["worker_token"])

            async def poll(token):
                return await http.post("/claim", headers=_h(token), json={"claim_id": str(uuid.uuid4()), "wait": 8})

            polls = [asyncio.create_task(poll(t)) for t in tokens]
            await asyncio.sleep(1.5)  # every request made its first, empty claim and waits
            calls = 0
            await app.enqueue(noop)
            await asyncio.sleep(1.0)
            woken_claims = calls
            results = await asyncio.gather(*polls)

        assert sum(r.status_code == 200 for r in results) == 1
        assert woken_claims <= 3, f"{woken_claims} claims for one enqueue with 20 idle workers"

    async def test_a_burst_still_reaches_every_idle_worker(self, app, live_server):
        import asyncio

        import httpx

        enrollment, _ = await app.create_enrollment_token("box", ["default"])
        async with httpx.AsyncClient(base_url=f"{live_server}/remote/v1", timeout=20) as http:
            tokens = []
            for i in range(10):
                resp = await http.post(
                    "/register",
                    headers=_h(enrollment),
                    json={"name": f"w{i}", "queues": ["default"], "functions": [{"name": NOOP}]},
                )
                tokens.append(resp.json()["worker_token"])
            polls = [
                asyncio.create_task(http.post("/claim", headers=_h(t), json={"claim_id": str(uuid.uuid4()), "wait": 8}))
                for t in tokens
            ]
            await asyncio.sleep(1.0)
            for _ in range(10):
                await app.enqueue(noop)
            results = await asyncio.wait_for(asyncio.gather(*polls), 6)

        assert sum(r.status_code == 200 for r in results) == 10  # the chain fanned out to all


class TestRateLimitsAndBounds:
    """Regressions from the security review: limits keyed on unauthenticated input, and
    no bound on what a token holder could register."""

    async def test_bogus_secrets_cannot_rate_limit_a_real_worker(self, app, client):
        reg = await _register(app, client)
        prefix, _, _secret = reg["worker_token"].rpartition("_")
        bogus = f"{prefix}_{'x' * 43}"  # the victim's worker id, a wrong secret
        for _ in range(30):
            assert (await _claim(client, bogus)).status_code == 401
        assert (await _claim(client, reg["worker_token"])).status_code == 204  # not 429

    async def test_open_claims_per_worker_are_capped(self, app):
        import asyncio

        import httpx

        async with _serve(app, remote_max_claims_per_worker=2) as url:
            async with httpx.AsyncClient(base_url=f"{url}/remote/v1", timeout=10) as http:
                enrollment, _ = await app.create_enrollment_token("box", ["default"])
                reg = await http.post(
                    "/register", headers=_h(enrollment), json={"queues": ["default"], "functions": [{"name": NOOP}]}
                )
                token = reg.json()["worker_token"]

                async def poll(wait):
                    return await http.post(
                        "/claim", headers=_h(token), json={"claim_id": str(uuid.uuid4()), "wait": wait}
                    )

                held = [asyncio.create_task(poll(2)) for _ in range(2)]
                await asyncio.sleep(0.5)
                third = await poll(0)
                await asyncio.gather(*held)
                after = await poll(0)
        assert (third.status_code, third.json()["error"]["code"]) == (429, "rate_limited")
        assert after.status_code == 204  # the slots were given back

    async def test_register_rate_counts_only_valid_secrets(self, app):
        enrollment, _ = await app.create_enrollment_token("box", ["default"])
        bogus = enrollment.rpartition("_")[0] + "_" + "y" * 43
        body = {"queues": ["default"], "functions": [{"name": NOOP}]}
        async with AsyncTestClient(
            app=create_app(_config(app, remote_register_burst=2, remote_register_rate=0.01))
        ) as c:
            for _ in range(5):
                assert (await c.post("/remote/v1/register", headers=_h(bogus), json=body)).status_code == 401
            codes = [
                (await c.post("/remote/v1/register", headers=_h(enrollment), json=body)).status_code for _ in range(3)
            ]
        assert codes == [201, 201, 429]

    async def test_live_workers_per_token_are_capped(self, app):
        enrollment, _ = await app.create_enrollment_token("box", ["default"])
        body = {"queues": ["default"], "functions": [{"name": NOOP}]}
        async with AsyncTestClient(app=create_app(_config(app, remote_max_workers_per_token=2))) as c:
            codes = [
                (await c.post("/remote/v1/register", headers=_h(enrollment), json=body)).status_code for _ in range(3)
            ]
        assert codes == [201, 201, 429]

    async def test_register_bounds_and_cleans_what_a_worker_declares(self, app, client):
        enrollment, _ = await app.create_enrollment_token("box", ["default"])
        base = {"queues": ["default"], "functions": [{"name": NOOP}]}

        too_many = {**base, "functions": [{"name": f"f{i}"} for i in range(1001)]}
        resp = await client.post("/remote/v1/register", headers=_h(enrollment), json=too_many)
        assert (resp.status_code, resp.json()["error"]["code"]) == (400, "bad_request")

        big_labels = {**base, "labels": {"x": "a" * 9000}}
        assert (await client.post("/remote/v1/register", headers=_h(enrollment), json=big_labels)).status_code == 400

        evil = {**base, "name": "box\x1b]0;owned\x07\x1b[2J" + "n" * 500}
        resp = await client.post("/remote/v1/register", headers=_h(enrollment), json=evil)
        worker = await app.get_worker(resp.json()["worker_id"])
        assert "\x1b" not in worker["name"] and "\x07" not in worker["name"]
        assert len(worker["name"]) <= 200

    async def test_enrollment_token_queues_cannot_contain_commas(self, app):
        with pytest.raises(ValueError, match=","):
            await app.create_enrollment_token("x", ["a,b"])

    async def test_oversized_progress_is_dropped_but_the_lease_renewed(self, app, client):
        reg = await _register(app, client)
        await app.enqueue(noop)
        claimed = (await _claim(client, reg["worker_token"])).json()
        resp = await client.post(
            f"/remote/v1/leases/{claimed['lease_id']}/heartbeat",
            headers=_h(reg["worker_token"]),
            json={"progress": {"log": "z" * 10_000}},
        )
        assert resp.status_code == 200
        import psycopg
        from psycopg.sql import SQL

        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            cur = await conn.execute(
                SQL("SELECT progress FROM {e} WHERE id = %s").format(e=app._t["executions"]), (claimed["lease_id"],)
            )
            assert (await cur.fetchone())[0] is None

        small = {"step": 3}
        resp = await client.post(
            f"/remote/v1/leases/{claimed['lease_id']}/heartbeat",
            headers=_h(reg["worker_token"]),
            json={"progress": small},
        )
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            cur = await conn.execute(
                SQL("SELECT progress FROM {e} WHERE id = %s").format(e=app._t["executions"]), (claimed["lease_id"],)
            )
            assert (await cur.fetchone())[0] == small


def test_rate_limiter_memory_is_bounded_and_lru():
    from pgwerk.remote.server.limits import TokenBucket

    bucket = TokenBucket(rate=1, burst=1, max_keys=100)
    for i in range(1000):
        bucket.allow(f"k{i}")
        bucket.allow("hot")  # used all along: stays
    assert len(bucket) == 100
    assert not bucket.allow("hot")  # its state was kept, not reset by eviction


class TestSessionExpiry:
    """Regression: a server outage (or a network one) longer than the idle threshold made
    every idle remote worker exit for good. Idle expiry cleared the session like a
    revocation, and the client stopped on any 401."""

    @staticmethod
    async def _expire_idle(app):
        import psycopg
        from psycopg.sql import SQL

        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("UPDATE {w} SET heartbeat_at = NOW() - INTERVAL '1 hour' WHERE role = 'remote'").format(
                    w=app._t["worker"]
                )
            )
        return await app.expire_idle_remote_workers()

    async def test_idle_expired_worker_gets_session_expired(self, app, client):
        reg = await _register(app, client)
        assert await self._expire_idle(app) == 1
        resp = await _claim(client, reg["worker_token"])
        assert (resp.status_code, resp.json()["error"]["code"]) == (401, "session_expired")

        # Only the holder of the right secret learns that.
        prefix = reg["worker_token"].rpartition("_")[0]
        resp = await _claim(client, f"{prefix}_{'q' * 43}")
        assert (resp.status_code, resp.json()["error"]["code"]) == (401, "unauthorized")

    async def test_retired_revoked_and_deregistered_workers_stay_unauthorized(self, app, client):
        enrollment, row = await app.create_enrollment_token("box", ["default"])
        drained = await _register(app, client, enrollment=enrollment)
        revoked = await _register(app, client, enrollment=enrollment)
        gone = await _register(app, client, enrollment=enrollment)

        await app.revoke_worker(drained["worker_id"], drain=True)
        await app.expire_idle_remote_workers()  # the drained worker holds nothing: stopped
        await app.revoke_worker(revoked["worker_id"])
        assert (await client.post("/remote/v1/deregister", headers=_h(gone["worker_token"]))).status_code == 204

        for reg in (drained, revoked, gone):
            resp = await _claim(client, reg["worker_token"])
            assert (resp.status_code, resp.json()["error"]["code"]) == (401, "unauthorized"), reg

    async def test_expired_session_of_a_revoked_token_is_unauthorized(self, app, client):
        enrollment, row = await app.create_enrollment_token("box", ["default"])
        reg = await _register(app, client, enrollment=enrollment)
        await self._expire_idle(app)
        await app.revoke_enrollment_token(row["id"])
        resp = await _claim(client, reg["worker_token"])
        assert resp.json()["error"]["code"] == "unauthorized"

    async def test_remote_worker_registers_again_and_keeps_working(self, app):
        import asyncio

        from pgwerk.remote import RemoteWorker

        async def handler(ctx):
            return "ok"

        enrollment, _ = await app.create_enrollment_token("box", ["default"])
        async with _serve(app, remote_claim_wait=1) as url:
            worker = RemoteWorker(url, enrollment, ["default"], {NOOP: handler}, insecure=True)
            run = asyncio.create_task(worker.run())
            try:
                for _ in range(100):
                    if worker.worker_id:
                        break
                    await asyncio.sleep(0.05)
                first = worker.worker_id
                assert await self._expire_idle(app) == 1

                for _ in range(100):  # its next claim gets session_expired; it registers again
                    if worker.worker_id != first:
                        break
                    await asyncio.sleep(0.1)
                assert worker.worker_id not in (None, first)
                assert not run.done(), "the worker exited instead of registering again"

                job = await app.enqueue(noop)
                for _ in range(100):
                    row = await app.get_job(job.id)
                    if row.status.value == "complete":
                        break
                    await asyncio.sleep(0.1)
                assert row.status.value == "complete"
            finally:
                worker.stop()
                await asyncio.wait_for(run, 10)


class TestServerSideFailuresUseTheProtocolShape:
    """Regression: database outages, NUL bytes, deeply nested JSON and bugs answered a
    bare 500 `{"detail": ...}` with a traceback in the log, outside the protocol's error
    shape. A client then retried a request that could never succeed (NUL in a result)."""

    async def test_database_unavailable_is_a_retryable_503(self, app, client, monkeypatch):
        import psycopg

        from pgwerk.remote.repository import RemoteRepository

        reg = await _register(app, client)

        async def down(self, *a, **kw):
            raise psycopg.OperationalError("server closed the connection unexpectedly")

        monkeypatch.setattr(RemoteRepository, "authenticate", down)
        resp = await _claim(client, reg["worker_token"])
        assert resp.status_code == 503
        assert resp.json()["error"]["code"] == "internal"

    async def test_an_unexpected_error_is_a_500_in_the_protocol_shape(self, app, client, monkeypatch):
        from pgwerk.remote.repository import RemoteRepository

        reg = await _register(app, client)

        async def bug(self, *a, **kw):
            raise RuntimeError("a bug")

        monkeypatch.setattr(RemoteRepository, "claim", bug)
        resp = await _claim(client, reg["worker_token"])
        assert (resp.status_code, resp.json()["error"]["code"]) == (500, "internal")
        assert "a bug" not in resp.text

    async def test_nul_bytes_in_a_result_are_a_400_not_a_retry_loop(self, app, client):
        reg = await _register(app, client)
        await app.enqueue(noop)
        claimed = (await _claim(client, reg["worker_token"])).json()
        resp = await client.post(
            f"/remote/v1/leases/{claimed['lease_id']}/complete",
            headers=_h(reg["worker_token"]),
            json={"result": "a\u0000b"},
        )
        assert (resp.status_code, resp.json()["error"]["code"]) == (400, "bad_request")

    async def test_nul_bytes_in_progress_are_dropped_and_the_lease_renewed(self, app, client):
        reg = await _register(app, client)
        await app.enqueue(noop)
        claimed = (await _claim(client, reg["worker_token"])).json()
        resp = await client.post(
            f"/remote/v1/leases/{claimed['lease_id']}/heartbeat",
            headers=_h(reg["worker_token"]),
            json={"progress": {"log": "x\u0000y"}},
        )
        assert resp.status_code == 200

    async def test_deeply_nested_json_is_a_400(self, app, client):
        reg = await _register(app, client)
        body = '{"claim_id": "' + str(uuid.uuid4()) + '", "x": ' + "[" * 100_000 + "]" * 100_000 + "}"
        resp = await client.post(
            "/remote/v1/claim",
            headers={**_h(reg["worker_token"]), "Content-Type": "application/json"},
            content=body,
        )
        assert (resp.status_code, resp.json()["error"]["code"]) == (400, "bad_request")


class TestStructuredErrorsAreBounded:
    """Regression: error.type wasn't truncated (a 200 KB type was stored whole), the size
    was measured on ASCII-escaped JSON so a long non-ASCII message was cut down to just the
    marker, unknown keys were stored as is, and control characters reached terminals."""

    async def test_stored_error_fits_the_limit_and_keeps_its_text(self, app):
        import json

        async with AsyncTestClient(app=create_app(_config(app, remote_max_error_bytes=8192))) as c:
            reg = await _register(app, c)
            job = await app.enqueue(noop)
            claimed = (await _claim(c, reg["worker_token"])).json()
            error = {
                "type": "T" * 50_000,
                "message": "é" * 20_000,
                "stack": "line\n\tframe\x1b[31m\x07",
                "junk": "J" * 30_000,
            }
            resp = await c.post(
                f"/remote/v1/leases/{claimed['lease_id']}/fail",
                headers=_h(reg["worker_token"]),
                json={"error": error, "retryable": False},
            )
            assert resp.status_code == 200, resp.text
        stored = (await app.get_job(job.id)).error
        assert len(stored.encode()) <= 8192
        parsed = json.loads(stored)
        assert set(parsed) <= {"type", "message", "stack"}
        assert len(parsed["type"]) <= 256
        assert parsed["message"].startswith("é" * 1000), parsed["message"][:40]
        assert "\x1b" not in parsed["stack"] and "\x07" not in parsed["stack"] and "\n\t" in parsed["stack"]


def test_truncate_error_unit():
    import json

    from pgwerk.remote.server.limits import truncate_error

    out = truncate_error({"type": "X" * 1000, "message": "ü" * 50_000, "extra": 1}, 4096)
    assert len(json.dumps(out, ensure_ascii=False).encode()) <= 4096
    assert set(out) == {"type", "message"} and len(out["type"]) == 256
    assert out["message"].startswith("ü" * 100) and out["message"].endswith("…[truncated]")
    small = {"type": "E", "message": "ok\nnext\ttab"}
    assert truncate_error(small, 4096) == small


@pytest.mark.parametrize("url", ["HTTP://example.com", "Http://example.com", "ftp://example.com", "example.com"])
def test_client_refuses_anything_but_https(url):
    from pgwerk.remote.client.http import ProtocolClient

    with pytest.raises(ValueError):
        ProtocolClient(url)


def test_client_accepts_https_in_any_case():
    from pgwerk.remote.client.http import ProtocolClient

    ProtocolClient("HTTPS://example.com")
    ProtocolClient("http://localhost:8000", insecure=True)
