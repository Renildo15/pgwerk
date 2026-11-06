"""RemoteWorker against a fake transport: retry policy and protocol reactions."""

from __future__ import annotations

import json

import httpx
import pytest

from pgwerk.remote.client.worker import RemoteWorker
from pgwerk.remote.client.context import RemoteJob


def _job(**kw) -> RemoteJob:
    base = dict(
        id="j",
        function="m.f",
        queue="q",
        kwargs={},
        args=[],
        attempts=1,
        max_attempts=1,
        timeout_secs=None,
        meta=None,
        lease_id="00000000-0000-0000-0000-000000000001",
        lease_ttl=2,
        heartbeat_every=1,
    )
    base.update(kw)
    return RemoteJob(**base)


def _worker(handler) -> RemoteWorker:
    w = RemoteWorker("https://x", "pgwk_e_t", ["q"], {"m.f": lambda: None}, transport=httpx.MockTransport(handler))
    w._token = "pgwk_w_t"
    return w


def _err(status: int, code: str) -> httpx.Response:
    return httpx.Response(status, json={"error": {"code": code, "message": code}})


def test_refuses_plain_http_unless_insecure():
    with pytest.raises(ValueError):
        RemoteWorker("http://x", "t", ["q"], {"m.f": print})
    RemoteWorker("http://x", "t", ["q"], {"m.f": print}, insecure=True)


async def test_report_retries_transient_errors_until_accepted(monkeypatch):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        assert request.headers["Pgwerk-Protocol"] == "1" and request.headers["Authorization"] == "Bearer pgwk_w_t"
        if len(calls) == 1:
            raise httpx.ConnectError("down")
        if len(calls) == 2:
            return _err(503, "unavailable")
        return httpx.Response(200, json={"status": "complete"})

    monkeypatch.setattr("asyncio.sleep", _no_sleep)
    await _worker(handler)._report(_job(), "/complete", {"result": 1})
    assert calls == ["/remote/v1/leases/00000000-0000-0000-0000-000000000001/complete"] * 3


async def test_report_stops_on_lease_lost():
    calls = []

    def handler(request):
        calls.append(1)
        return _err(409, "lease_lost")

    await _worker(handler)._report(_job(), "/complete", {"result": 1})
    assert calls == [1]


async def test_too_large_result_becomes_a_non_retryable_failure():
    sent = []

    def handler(request):
        sent.append((request.url.path.rsplit("/", 1)[-1], json.loads(request.content)))
        return _err(413, "too_large") if request.url.path.endswith("complete") else httpx.Response(200, json={})

    await _worker(handler)._report(_job(), "/complete", {"result": "x"})
    assert sent[0][0] == "complete"
    assert sent[1][0] == "fail" and sent[1][1]["retryable"] is False
    assert sent[1][1]["error"]["type"] == "ResultTooLarge"


async def test_revoked_token_stops_the_worker():
    w = _worker(lambda request: _err(401, "unauthorized"))
    await w._report(_job(), "/complete", {"result": 1})
    assert w._revoked and w._stopping.is_set()


async def _no_sleep(*_args, **_kwargs):
    return None
