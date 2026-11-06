"""End to end: a RemoteWorker against a real server (uvicorn in a thread), no DB access."""

from __future__ import annotations

import json
import time
import socket
import asyncio
import threading

import pytest
import uvicorn

from pgwerk.config import WerkConfig
from pgwerk.commons import JobStatus
from pgwerk.remote import NonRetryable
from pgwerk.remote import RemoteWorker
from pgwerk.api.app import create_app


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def server(app):
    port = _free_port()
    config = WerkConfig(
        dsn=app.dsn,
        prefix=app.prefix,
        remote_workers=True,
        api=False,
        ui=False,
        maintenance=False,
        remote_lease_ttl=4,  # heartbeat every second
    )
    srv = uvicorn.Server(
        uvicorn.Config(create_app(config), host="127.0.0.1", port=port, log_level="error", timeout_graceful_shutdown=1)
    )
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()
    for _ in range(100):
        if srv.started:
            break
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    srv.should_exit = True
    thread.join(10)


async def add(ctx, x, y):
    ctx.heartbeat({"adding": [x, y]})
    return {"sum": x + y, "attempt": ctx.job.attempts}


def shout(text):  # sync, no ctx
    return text.upper()


def boom():
    raise ValueError("bad input")


def refuse():
    raise NonRetryable("not worth retrying")


async def slow(ctx):
    for _ in range(100):
        await asyncio.sleep(0.1)


HANDLERS = {"e2e.add": add, "e2e.shout": shout, "e2e.boom": boom, "e2e.refuse": refuse, "e2e.slow": slow}


async def _wait(app, job_id, *statuses, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = await app.get_job(job_id)
        if row.status in statuses:
            return row
        await asyncio.sleep(0.1)
    raise AssertionError(f"job {job_id} stuck in {row.status}")


@pytest.fixture
async def remote(app, server):
    token, _ = await app.create_enrollment_token("e2e", ["default"])
    worker = RemoteWorker(server, token, ["default"], HANDLERS, concurrency=2, insecure=True)
    task = asyncio.create_task(worker.run())
    yield worker
    worker.stop()
    await asyncio.wait_for(task, 10)


class TestRemoteWorkerEndToEnd:
    async def test_runs_async_and_sync_handlers(self, app, remote):
        a = await app.enqueue("e2e.add", 2, y=3)
        b = await app.enqueue("e2e.shout", "hi")
        assert (await _wait(app, a.id, JobStatus.Complete)).result == {"sum": 5, "attempt": 1}
        assert (await _wait(app, b.id, JobStatus.Complete)).result == "HI"

    async def test_exceptions_become_structured_failures_and_retry(self, app, remote):
        job = await app.enqueue("e2e.boom", _retry=2)
        row = await _wait(app, job.id, JobStatus.Failed)
        error = json.loads(row.error)
        assert (error["type"], error["message"]) == ("ValueError", "bad input")
        assert "Traceback" in error["stack"] and row.attempts == 2

    async def test_non_retryable_fails_at_once(self, app, remote):
        job = await app.enqueue("e2e.refuse", _retry=5)
        row = await _wait(app, job.id, JobStatus.Failed)
        assert row.attempts == 1

    async def test_abort_reaches_the_remote_job(self, app, remote):
        job = await app.enqueue("e2e.slow")
        await _wait(app, job.id, JobStatus.Active)
        await app.abort_job(job.id)
        await _wait(app, job.id, JobStatus.Aborted)

    async def test_stop_deregisters_and_releases(self, app, server):
        token, _ = await app.create_enrollment_token("e2e", ["default"])
        worker = RemoteWorker(server, token, ["default"], HANDLERS, insecure=True)
        task = asyncio.create_task(worker.run())
        job = await app.enqueue("e2e.add", 1, y=1)
        await _wait(app, job.id, JobStatus.Complete)
        worker.stop()
        await asyncio.wait_for(task, 10)
        [row] = [w for w in await app.list_workers() if w["id"] == worker.worker_id]
        assert row["status"] == "stopped"

    async def test_revoked_worker_shuts_itself_down(self, app, server):
        token, tok = await app.create_enrollment_token("e2e", ["default"])
        worker = RemoteWorker(server, token, ["default"], HANDLERS, insecure=True)
        task = asyncio.create_task(worker.run())
        for _ in range(50):
            if worker.worker_id:
                break
            await asyncio.sleep(0.1)
        await app.revoke_enrollment_token(tok["id"])
        await app.enqueue("e2e.add", 1, y=1)  # wakes its long-poll; the next call is refused
        await asyncio.wait_for(task, 40)
        assert worker._revoked
