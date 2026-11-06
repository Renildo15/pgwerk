"""Server conformance: HTTP-only tests of protocol v1 against a running server.

Environment:
    PGWERK_CONFORMANCE_URL          base URL of the server, e.g. http://127.0.0.1:8000
    PGWERK_CONFORMANCE_TOKEN        an enrollment token scoped to the queue "conformance"
    PGWERK_CONFORMANCE_ADMIN_TOKEN  admin API token, used to create, abort and delete test jobs
"""

from __future__ import annotations

import os
import uuid

import httpx
import pytest


URL = os.environ.get("PGWERK_CONFORMANCE_URL", "")
TOKEN = os.environ.get("PGWERK_CONFORMANCE_TOKEN", "")
ADMIN = os.environ.get("PGWERK_CONFORMANCE_ADMIN_TOKEN", "")
QUEUE = "conformance"

KNOWN_CODES = {
    "bad_request",
    "protocol",
    "unauthorized",
    "scope",
    "draining",
    "not_found",
    "lease_lost",
    "too_large",
    "rate_limited",
    "internal",
}

if not (URL and TOKEN and ADMIN):
    pytest.skip(
        "set PGWERK_CONFORMANCE_URL, PGWERK_CONFORMANCE_TOKEN and PGWERK_CONFORMANCE_ADMIN_TOKEN",
        allow_module_level=True,
    )


class Remote:
    """A protocol client with no knowledge of pgwerk internals."""

    def __init__(self) -> None:
        self.http = httpx.Client(base_url=URL.rstrip("/") + "/remote/v1", timeout=60)

    def post(self, path: str, token: str | None, body: dict | None = None, *, protocol: bool = True, **kw):
        headers = {}
        if protocol:
            headers["Pgwerk-Protocol"] = "1"
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return self.http.post(path, headers=headers, json=body if body is not None else {}, **kw)

    def register(self, function: str, queues: list[str] | None = None) -> dict:
        resp = self.post(
            "/register",
            TOKEN,
            {"queues": queues or [QUEUE], "functions": [{"name": function}], "client": {"lang": "conformance"}},
        )
        assert resp.status_code == 201, resp.text
        return resp.json()

    def claim(self, worker_token: str, claim_id: str | None = None, wait: float = 0):
        return self.post("/claim", worker_token, {"claim_id": claim_id or str(uuid.uuid4()), "wait": wait})


class Admin:
    def __init__(self) -> None:
        self.http = httpx.Client(base_url=URL.rstrip("/") + "/api", headers={"Authorization": f"Bearer {ADMIN}"})

    def enqueue(self, function: str, **body) -> str:
        resp = self.http.post("/jobs", json={"function": function, "queue": QUEUE, **body})
        assert resp.status_code == 201, resp.text
        return resp.json()["id"]

    def job(self, job_id: str) -> dict:
        return self.http.get(f"/jobs/{job_id}").json()

    def abort(self, job_id: str) -> None:
        assert self.http.post(f"/jobs/{job_id}/abort").status_code in (200, 201)

    def delete(self, job_id: str) -> None:
        assert self.http.delete(f"/jobs/{job_id}").status_code == 204


@pytest.fixture
def remote() -> Remote:
    return Remote()


@pytest.fixture
def admin() -> Admin:
    return Admin()


@pytest.fixture
def function() -> str:
    """A function name unique to the test, so jobs of other tests are never claimed."""
    return f"conformance.f{uuid.uuid4().hex[:12]}"


def assert_error(resp: httpx.Response, status: int, code: str) -> None:
    assert resp.status_code == status, resp.text
    body = resp.json()
    assert set(body) == {"error"} and set(body["error"]) >= {"code", "message"}, body
    assert body["error"]["code"] == code, body
    assert body["error"]["code"] in KNOWN_CODES
