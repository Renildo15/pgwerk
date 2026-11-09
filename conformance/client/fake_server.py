"""A scripted fake pgwerk server that records every request a client under test makes."""

from __future__ import annotations

import time
import uuid
import asyncio
import socket
import threading

from typing import Any
from dataclasses import field
from dataclasses import dataclass

import uvicorn

from litestar import Litestar
from litestar import Request
from litestar import Response
from litestar import post


@dataclass
class Call:
    endpoint: str  # register | claim | heartbeat | complete | fail | deregister
    lease_id: str | None
    headers: dict[str, str]
    body: Any
    at: float


def err(status: int, code: str) -> Response:
    return Response({"error": {"code": code, "message": code}}, status_code=status)


@dataclass
class Scenario:
    """Default behaviour: register succeeds, claims hand out ``jobs`` in order, then 204.

    Subclasses override ``on_*`` hooks to script responses. Every request is recorded in
    ``calls``.
    """

    jobs: list[dict[str, Any]] = field(default_factory=list)
    lease_ttl: int = 4
    heartbeat_every: int = 1
    calls: list[Call] = field(default_factory=list)
    worker_token: str = field(default_factory=lambda: f"pgwk_w_{uuid.uuid4().hex}_{'w' * 43}")

    def of(self, endpoint: str, lease_id: str | None = None) -> list[Call]:
        return [c for c in self.calls if c.endpoint == endpoint and (lease_id is None or c.lease_id == lease_id)]

    # --- hooks ---------------------------------------------------------------

    def on_register(self, call: Call) -> Response:
        return Response(
            {
                "worker_id": str(uuid.uuid4()),
                "worker_token": self.worker_token,
                "protocol": 1,
                "lease_ttl": self.lease_ttl,
                "claim_wait": 1,
                "limits": {"max_payload_bytes": 262144, "max_result_bytes": 1024, "max_error_bytes": 65536},
            },
            status_code=201,
        )

    def on_claim(self, call: Call) -> Response | None:
        """Return ``None`` for "nothing to claim" (204 after a short hold)."""
        if not self.jobs:
            return None
        job = self.jobs.pop(0)
        return Response(self.lease(job), status_code=200)

    def lease(self, job: dict[str, Any]) -> dict[str, Any]:
        body = {
            "id": str(uuid.uuid4()),
            "queue": "conformance",
            "kwargs": {},
            "args": [],
            "attempts": 1,
            "max_attempts": 1,
            "lost_count": 0,
            "timeout_secs": None,
            "meta": None,
            **job,
        }
        return {
            "job": body,
            "lease_id": job.get("lease_id") or str(uuid.uuid4()),
            "lease_expires_at": "2099-01-01T00:00:00+00:00",
            "lease_ttl": self.lease_ttl,
            "heartbeat_every": self.heartbeat_every,
        }

    def on_heartbeat(self, call: Call) -> Response:
        return Response({"lease_expires_at": "2099-01-01T00:00:00+00:00", "abort": False}, status_code=200)

    def on_complete(self, call: Call) -> Response:
        return Response({"status": "complete"}, status_code=200)

    def on_fail(self, call: Call) -> Response:
        return Response({"status": "failed"}, status_code=200)

    def on_deregister(self, call: Call) -> Response:
        return Response(None, status_code=204)

    def on_redirected(self, call: Call) -> Response:
        """Where a redirect points: a client must never get here (its token would follow)."""
        return Response(None, status_code=204)


def _app(scenario: Scenario) -> Litestar:
    @post("/remote/v1/{path:path}")
    async def handle(request: Request, path: str) -> Response:
        try:
            body = await request.json()
        except Exception:
            body = None
        parts = path.strip("/").split("/")
        lease_id = parts[1] if parts[0] == "leases" and len(parts) == 3 else None
        endpoint = parts[2] if lease_id else parts[0]
        call = Call(endpoint, lease_id, {k.lower(): v for k, v in request.headers.items()}, body, time.monotonic())
        scenario.calls.append(call)
        response = getattr(scenario, f"on_{endpoint}")(call)
        if response is None:  # an empty claim: hold briefly, like a long-poll
            await asyncio.sleep(0.2)
            return Response(None, status_code=204)
        return response  # type: ignore[no-any-return]

    return Litestar(route_handlers=[handle])


class FakeServer:
    def __init__(self, scenario: Scenario) -> None:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(
            _app(scenario), host="127.0.0.1", port=self.port, log_level="error", timeout_graceful_shutdown=1
        )
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def __enter__(self) -> "FakeServer":
        self._thread.start()
        while not self._server.started:
            time.sleep(0.02)
        return self

    def __exit__(self, *_: Any) -> None:
        self._server.should_exit = True
        self._thread.join(5)
