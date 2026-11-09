"""The ``/remote/v1`` router: the HTTP side of the remote-worker protocol (experimental).

Mounted by :func:`pgwerk.api.app.create_app` when ``WerkConfig.remote_workers`` is on.
Every endpoint is an RPC-style ``POST``. It requires ``Pgwerk-Protocol: 1`` and a
bearer token: the enrollment token for ``register``, and the worker token it returns
for everything else. Errors are ``{"error": {"code", "message"}}``; clients branch on
``code``, never on ``message``.
"""

from __future__ import annotations

import json
import time
import uuid
import asyncio
import logging

from typing import TYPE_CHECKING
from typing import Any

from litestar import Router
from litestar import Request
from litestar import Response
from litestar import post
from litestar.di import Provide

from .. import tokens
from ..errors import Scope
from ..errors import TooLarge
from ..errors import RemoteError
from ..errors import RateLimited
from ..errors import Unauthorized
from ..errors import ProtocolMismatch
from .limits import TokenBucket
from .limits import read_body
from .limits import truncate_error
from .longpoll import Waiters
from ..repository import ClaimSpent
from ...settlement import Outcome
from ...serializers import JSONSerializer
from ...serializers import encode_plain


if TYPE_CHECKING:
    from ...app import Werk
    from ...config import WerkConfig
    from ...schemas import Job
    from ...settlement import Settled


logger = logging.getLogger(__name__)

PROTOCOL = "1"


class RemoteServer:
    """Per-process state of the remote router: long-poll waiters, rate limiter, and
    callbacks running in the background."""

    def __init__(self, werk: "Werk", config: "WerkConfig") -> None:
        if not isinstance(werk.serializer, JSONSerializer):
            # A remote worker returning a pickled result would be code execution on
            # whatever later decodes it. JSON is also the cross-language contract.
            raise RuntimeError("remote workers require JSONSerializer")
        self.werk = werk
        self.config = config
        self.repo = werk._remote_repo
        self.waiters = Waiters(
            werk._connect,
            werk.prefix,
            werk._t["jobs"],
            listen=config.listen,
            listen_connect=werk._direct_connect,
        )
        self.bucket = TokenBucket(config.remote_claim_rate, config.remote_claim_burst)
        self._background: set[asyncio.Task] = set()

    async def start(self) -> None:
        await self.waiters.start()

    async def stop(self, timeout: float = 10.0) -> None:
        await self.waiters.stop()
        if self._background:
            await asyncio.wait(self._background, timeout=timeout)

    def run_callbacks(self, settled: "list[tuple[Job, Settled]]") -> None:
        """Run callbacks in the background: a response doesn't wait for a callback whose
        timeout can be minutes. Delivery stays at most once."""
        for job, result in settled:
            task = asyncio.create_task(self.werk._settlement.run_callback(job, result))
            self._background.add(task)
            task.add_done_callback(self._background.discard)


# --- request helpers ---------------------------------------------------------


def _token(request: Request, kind: tokens.TokenKind) -> tokens.ParsedToken:
    if request.headers.get("pgwerk-protocol") != PROTOCOL:
        raise ProtocolMismatch(f"send the header Pgwerk-Protocol: {PROTOCOL}")
    auth = request.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        raise Unauthorized("missing bearer token")
    try:
        token = tokens.parse(auth[len("Bearer ") :])
    except tokens.InvalidToken:
        raise Unauthorized("invalid token") from None
    if token.kind != kind:
        raise Unauthorized(f"this endpoint needs a {kind} token")
    return token


async def _json(request: Request, limit: int) -> dict[str, Any]:
    body = await read_body(request, limit)
    if not body:
        return {}
    try:
        data = json.loads(body)
    except ValueError:
        raise RemoteError("body is not valid JSON") from None
    if not isinstance(data, dict):
        raise RemoteError("body must be a JSON object")
    return data


def _uuid(value: Any, field: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError):
        raise RemoteError(f"{field} must be a UUID") from None


def _str_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(v, str) and v for v in value):
        raise RemoteError(f"{field} must be a list of non-empty strings")
    return value


def _job_body(job: "Job") -> dict[str, Any]:
    payload = job.payload or {}
    return {
        "id": job.id,
        "function": job.function,
        "queue": job.queue,
        "kwargs": payload.get("kwargs") or {},
        "args": payload.get("args") or [],
        "attempts": job.attempts,
        "max_attempts": job.max_attempts,
        "lost_count": job.lost_count,
        "timeout_secs": job.timeout_secs,
        "meta": job.meta,
    }


def _error_response(_: Request, exc: RemoteError) -> Response:
    return Response(content={"error": {"code": exc.code, "message": exc.message}}, status_code=exc.status_code)


# --- endpoints ---------------------------------------------------------------


@post("/register", status_code=201)
async def register(request: Request, remote: RemoteServer) -> dict[str, Any]:
    cfg = remote.config
    token = _token(request, "enrollment")
    body = await _json(request, cfg.remote_max_payload_bytes)
    functions = [f["name"] if isinstance(f, dict) else f for f in body.get("functions") or []]
    queues = _str_list(body.get("queues"), "queues")
    functions = _str_list(functions, "functions")
    if not queues or not functions:
        raise Scope("declare at least one queue and one function")
    client = body.get("client") if isinstance(body.get("client"), dict) else {}
    labels = body.get("labels") if isinstance(body.get("labels"), dict) else {}
    worker_id, worker_token = await remote.repo.register(
        token, name=str(body.get("name") or "remote"), queues=queues, functions=functions, labels=labels, client=client
    )
    return {
        "worker_id": worker_id,
        "worker_token": worker_token,
        "protocol": int(PROTOCOL),
        "lease_ttl": cfg.remote_lease_ttl,
        "claim_wait": cfg.remote_claim_wait,
        "limits": {
            "max_payload_bytes": cfg.remote_max_payload_bytes,
            "max_result_bytes": cfg.remote_max_result_bytes,
            "max_error_bytes": cfg.remote_max_error_bytes,
        },
    }


@post("/deregister", status_code=204)
async def deregister(request: Request, remote: RemoteServer) -> None:
    token = _token(request, "worker")
    remote.run_callbacks(await remote.repo.deregister(token))


@post("/claim")
async def claim(request: Request, remote: RemoteServer) -> Response:
    cfg = remote.config
    token = _token(request, "worker")
    body = await _json(request, cfg.remote_max_payload_bytes)
    claim_id = _uuid(body.get("claim_id"), "claim_id")
    wait = body.get("wait", cfg.remote_claim_wait)
    wait = max(
        0.0, min(float(wait) if isinstance(wait, (int, float)) else cfg.remote_claim_wait, cfg.remote_claim_wait)
    )
    if not remote.bucket.allow(token.id):
        raise RateLimited("too many claims; slow down")

    session = await remote.repo.authenticate(token, claiming=True)
    deadline = time.monotonic() + wait
    async with remote.waiters.subscribe(session.queues, session.functions) as woken:
        while True:
            woken.clear()
            try:
                claimed = await remote.repo.claim(token, claim_id, cfg.remote_lease_ttl)
            except ClaimSpent:
                return Response(content=None, status_code=204)
            if claimed is not None:
                job = claimed.job
                # There may be more work: let the next waiting worker of this group try.
                remote.waiters.chain(job.queue, session.functions)
                return Response(
                    content={
                        "job": _job_body(job),
                        "lease_id": job.lease_id,
                        "lease_expires_at": job.lease_expires_at.isoformat() if job.lease_expires_at else None,
                        "lease_ttl": claimed.lease_ttl,
                        "heartbeat_every": max(1, claimed.lease_ttl // 4),
                    },
                    status_code=200,
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return Response(content=None, status_code=204)
            try:
                await asyncio.wait_for(woken.wait(), remaining)
            except asyncio.TimeoutError:
                return Response(content=None, status_code=204)


@post("/leases/{lease_id:str}/heartbeat", status_code=200)
async def heartbeat(request: Request, remote: RemoteServer, lease_id: str) -> dict[str, Any]:
    cfg = remote.config
    token = _token(request, "worker")
    body = await _json(request, cfg.remote_max_payload_bytes)
    expires_at, abort = await remote.repo.heartbeat(
        token, _uuid(lease_id, "lease_id"), cfg.remote_lease_ttl, progress=body.get("progress")
    )
    return {"lease_expires_at": expires_at.isoformat(), "abort": abort}


@post("/leases/{lease_id:str}/complete", status_code=200)
async def complete(request: Request, remote: RemoteServer, lease_id: str) -> dict[str, Any]:
    cfg = remote.config
    token = _token(request, "worker")
    body = await _json(request, cfg.remote_max_result_bytes + 1024)
    result = body.get("result")
    if len(json.dumps(result).encode()) > cfg.remote_max_result_bytes:
        raise TooLarge(f"result over {cfg.remote_max_result_bytes} bytes; report it as a non-retryable failure")
    # Stored in an envelope that decode() returns as is: a result from an untrusted
    # worker must never reach the app's serializer in any process that reads the job.
    outcome = Outcome("complete", encode_plain(result))
    reported = await remote.repo.report(token, _uuid(lease_id, "lease_id"), outcome)
    if reported.settled:
        remote.run_callbacks([reported.settled])
    return {"status": reported.status}


@post("/leases/{lease_id:str}/fail", status_code=200)
async def fail(request: Request, remote: RemoteServer, lease_id: str) -> dict[str, Any]:
    cfg = remote.config
    token = _token(request, "worker")
    body = await _json(request, cfg.remote_max_payload_bytes)
    if body.get("aborted"):
        outcome = Outcome("abort")
    else:
        error = body.get("error")
        if not isinstance(error, dict) or not isinstance(error.get("type"), str):
            raise RemoteError('error must be an object like {"type": ..., "message": ..., "stack": ...}')
        error = truncate_error(error, cfg.remote_max_error_bytes)
        outcome = Outcome("fail", error=json.dumps(error), retryable=body.get("retryable", True) is not False)
    reported = await remote.repo.report(token, _uuid(lease_id, "lease_id"), outcome)
    if reported.settled:
        remote.run_callbacks([reported.settled])
    response: dict[str, Any] = {"status": reported.status}
    if reported.retry_at is not None:
        response["retry_at"] = reported.retry_at.isoformat()
    return response


def make_remote_router(state: dict[str, Any]) -> Router:
    """The ``/remote/v1`` router. *state* holds the :class:`RemoteServer` under ``"remote"``."""

    def _remote() -> RemoteServer:
        return state["remote"]

    return Router(
        path="/remote/v1",
        route_handlers=[register, deregister, claim, heartbeat, complete, fail],
        dependencies={"remote": Provide(_remote, sync_to_thread=False)},
        exception_handlers={RemoteError: _error_response},
    )


def init_remote(config: "WerkConfig", state: dict[str, Any], on_startup: list, on_shutdown: list) -> None:
    """Start and stop the :class:`RemoteServer` with the app. Register after ``init_werk``."""

    async def _start() -> None:
        state["remote"] = RemoteServer(state["werk"], config)
        await state["remote"].start()

    async def _stop() -> None:
        remote = state.pop("remote", None)
        if remote is not None:
            await remote.stop()

    on_startup.append(_start)
    on_shutdown.insert(0, _stop)  # before the Werk instance disconnects
