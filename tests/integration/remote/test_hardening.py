"""Regression tests for the remote-worker hardening found by the robustness and security tests."""

from __future__ import annotations

import os
import uuid
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
