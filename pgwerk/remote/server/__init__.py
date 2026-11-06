"""The ``/remote/v1`` router: the HTTP side of the remote-worker protocol.

Mounted by :func:`pgwerk.api.app.create_app` when ``WerkConfig.remote_workers`` is on.
Every endpoint is an RPC-style ``POST`` that requires ``Pgwerk-Protocol: 1`` and a
bearer token: the enrollment token for ``register``, and the worker token it returns
for everything else. Errors are ``{"error": {"code", "message"}}``. Clients branch on
``code``, never on ``message``. The normative description is
``docs/reference/remote-protocol.md``.
"""

from __future__ import annotations

import json
import time
import uuid
import asyncio
import logging

from typing import TYPE_CHECKING
from typing import Any

import psycopg

from litestar import Router
from litestar import Request
from litestar import Response
from litestar import post
from litestar.di import Provide
from litestar.openapi import ResponseSpec
from litestar.exceptions import HTTPException
from litestar.exceptions import ValidationException

from .. import tokens
from .models import JobBody
from .models import Limits
from .models import ErrorBody
from .models import FailRequest
from .models import ClaimRequest
from .models import ErrorResponse
from .models import ClaimResponse
from .models import ReportResponse
from .models import CompleteRequest
from .models import RegisterRequest
from .models import HeartbeatRequest
from .models import RegisterResponse
from .models import HeartbeatResponse
from .limits import MAX_NAME
from .limits import MAX_QUEUES
from .limits import MAX_FUNCTIONS
from .limits import MAX_IDENTIFIER
from .limits import MAX_CLIENT_FIELD
from .limits import MAX_LABELS_BYTES
from .limits import MAX_PROGRESS_BYTES
from .limits import TokenBucket
from .limits import clean_text
from .limits import error_json
from .limits import truncate_error
from ..errors import Scope
from ..errors import TooLarge
from ..errors import ErrorCode
from ..errors import RemoteError
from ..errors import RateLimited
from ..errors import Unauthorized
from ..errors import ProtocolMismatch
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
        self.register_bucket = TokenBucket(config.remote_register_rate, config.remote_register_burst)
        self._open_claims: dict[str, int] = {}
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


def _uuid(value: Any, field: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, TypeError):
        raise RemoteError(f"{field} must be a UUID") from None


def _bounded(values: list[str], field: str, max_items: int) -> list[str]:
    if len(values) > max_items:
        raise RemoteError(f"{field}: at most {max_items} entries")
    if any(len(v) > MAX_IDENTIFIER for v in values):
        raise RemoteError(f"{field}: each entry at most {MAX_IDENTIFIER} characters")
    return values


def _job_body(job: "Job") -> JobBody:
    payload = job.payload or {}
    return JobBody(
        id=job.id,
        function=job.function,
        queue=job.queue,
        kwargs=payload.get("kwargs") or {},
        args=payload.get("args") or [],
        attempts=job.attempts,
        max_attempts=job.max_attempts,
        lost_count=job.lost_count,
        timeout_secs=job.timeout_secs,
        meta=job.meta,
    )


def _error(status: int, code: ErrorCode, message: str) -> Response:
    return Response(content=ErrorResponse(ErrorBody(code.value, message)), status_code=status)


def _remote_error(_: Request, exc: RemoteError) -> Response:
    return _error(exc.status_code, exc.code, exc.message)


def _validation_error(_: Request, exc: ValidationException) -> Response:
    return _error(400, ErrorCode.BAD_REQUEST, str(exc.detail))


def _unexpected_error(request: Request, exc: Exception) -> Response:
    """Everything else, still in the protocol's error shape (clients branch on ``code``).

    A database outage is a retryable 503 logged as one line, not a traceback per
    request; data Postgres can't store (NUL characters) and over-deep JSON are the
    client's 400, so it doesn't retry a request that can never succeed.
    """
    if isinstance(exc, RecursionError):
        return _error(400, ErrorCode.BAD_REQUEST, "body is nested too deeply")
    if isinstance(exc, psycopg.DataError):
        return _error(400, ErrorCode.BAD_REQUEST, f"the request holds data the server can't store: {exc}")
    if isinstance(exc, psycopg.OperationalError):
        logger.warning("remote: database unavailable on %s: %s", request.url.path, exc)
        return _error(503, ErrorCode.INTERNAL, "database unavailable; retry")
    logger.exception("remote: unexpected error on %s", request.url.path, exc_info=exc)
    return _error(500, ErrorCode.INTERNAL, "internal error")


def _http_error(_: Request, exc: HTTPException) -> Response:
    if exc.status_code == 413:
        return _error(413, ErrorCode.TOO_LARGE, "request body too large")
    if exc.status_code == 404:
        return _error(404, ErrorCode.NOT_FOUND, str(exc.detail))
    if exc.status_code < 500:
        return _error(exc.status_code, ErrorCode.BAD_REQUEST, str(exc.detail))
    return _error(exc.status_code, ErrorCode.INTERNAL, "internal error")


_ERRORS = {
    400: ResponseSpec(ErrorResponse, description="bad_request or protocol"),
    401: ResponseSpec(
        ErrorResponse,
        description="unauthorized: invalid, revoked or wrong kind of token (stop); session_expired: register again",
    ),
    413: ResponseSpec(ErrorResponse, description="too_large"),
}


# --- endpoints ---------------------------------------------------------------


def _handlers(config: "WerkConfig") -> list:
    payload_limit = config.remote_max_payload_bytes
    result_limit = config.remote_max_result_bytes + 1024  # room for the envelope

    @post(
        "/register",
        status_code=201,
        request_max_body_size=payload_limit,
        responses={**_ERRORS, 403: ResponseSpec(ErrorResponse, description="scope")},
        summary="Exchange an enrollment token for a worker identity",
    )
    async def register(request: Request, data: RegisterRequest, remote: RemoteServer) -> RegisterResponse:
        token = _token(request, "enrollment")
        cfg = remote.config
        functions = [f.name for f in data.functions]
        if not data.queues or not functions or not all(data.queues) or not all(functions):
            raise Scope("declare at least one queue and one function")
        queues = _bounded(data.queues, "queues", MAX_QUEUES)
        functions = _bounded(functions, "functions", MAX_FUNCTIONS)
        labels = data.labels or {}
        if len(json.dumps(labels).encode()) > MAX_LABELS_BYTES:
            raise RemoteError(f"labels: at most {MAX_LABELS_BYTES} bytes of JSON")

        def admit() -> None:
            # Called once the token's secret checked out, so a stranger who only knows
            # the token id can't use up its registrations.
            if not remote.register_bucket.allow(token.id):
                raise RateLimited("too many registrations with this token; slow down")

        worker_id, worker_token = await remote.repo.register(
            token,
            name=clean_text(data.name or "remote", MAX_NAME) or "remote",
            queues=queues,
            functions=functions,
            labels=labels,
            client={
                "lang": clean_text(data.client.lang, MAX_CLIENT_FIELD),
                "version": clean_text(data.client.version, MAX_CLIENT_FIELD),
            },
            admit=admit,
            max_workers=cfg.remote_max_workers_per_token,
        )
        return RegisterResponse(
            worker_id=worker_id,
            worker_token=worker_token,
            protocol=int(PROTOCOL),
            lease_ttl=cfg.remote_lease_ttl,
            claim_wait=cfg.remote_claim_wait,
            limits=Limits(cfg.remote_max_payload_bytes, cfg.remote_max_result_bytes, cfg.remote_max_error_bytes),
        )

    @post(
        "/deregister",
        status_code=204,
        request_max_body_size=payload_limit,
        responses=_ERRORS,
        summary="Stop this worker; jobs it holds go back to the queue",
    )
    async def deregister(request: Request, remote: RemoteServer) -> None:
        token = _token(request, "worker")
        remote.run_callbacks(await remote.repo.deregister(token))

    @post(
        "/claim",
        request_max_body_size=payload_limit,
        responses={
            **_ERRORS,
            200: ResponseSpec(ClaimResponse, description="a job, leased to this worker"),
            204: ResponseSpec(None, description="nothing to claim within the wait, or a spent claim_id"),
            403: ResponseSpec(ErrorResponse, description="draining"),
            429: ResponseSpec(ErrorResponse, description="rate_limited"),
        },
        summary="Long-poll for one job",
    )
    async def claim(request: Request, data: ClaimRequest, remote: RemoteServer) -> Response:
        cfg = remote.config
        token = _token(request, "worker")
        claim_id = _uuid(data.claim_id, "claim_id")
        wait = cfg.remote_claim_wait if data.wait is None else max(0.0, min(data.wait, cfg.remote_claim_wait))
        session = await remote.repo.authenticate(token, claiming=True)
        # Limits are keyed on the authenticated worker: keyed on the token's id alone, a
        # stranger who knows it could drain this worker's bucket with bogus secrets.
        if not remote.bucket.allow(session.worker_id):
            raise RateLimited("too many claims; slow down")
        if remote._open_claims.get(session.worker_id, 0) >= cfg.remote_max_claims_per_worker:
            raise RateLimited(f"at most {cfg.remote_max_claims_per_worker} open claims per worker")
        remote._open_claims[session.worker_id] = remote._open_claims.get(session.worker_id, 0) + 1
        try:
            return await _long_poll(remote, token, session, claim_id, wait)
        finally:
            left = remote._open_claims.pop(session.worker_id, 1) - 1
            if left > 0:
                remote._open_claims[session.worker_id] = left

    async def _long_poll(remote: RemoteServer, token: Any, session: Any, claim_id: str, wait: float) -> Response:
        cfg = remote.config
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
                    body = ClaimResponse(
                        job=_job_body(job),
                        lease_id=job.lease_id or "",
                        lease_expires_at=job.lease_expires_at.isoformat() if job.lease_expires_at else None,
                        lease_ttl=claimed.lease_ttl,
                        heartbeat_every=max(1, claimed.lease_ttl // 4),
                    )
                    return Response(content=body, status_code=200)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return Response(content=None, status_code=204)
                try:
                    await asyncio.wait_for(woken.wait(), remaining)
                except asyncio.TimeoutError:
                    return Response(content=None, status_code=204)

    @post(
        "/leases/{lease_id:str}/heartbeat",
        status_code=200,
        request_max_body_size=payload_limit,
        responses={**_ERRORS, 409: ResponseSpec(ErrorResponse, description="lease_lost: stop the job")},
        summary="Renew a lease; learn whether the job was aborted",
    )
    async def heartbeat(
        request: Request, lease_id: str, remote: RemoteServer, data: HeartbeatRequest | None = None
    ) -> HeartbeatResponse:
        token = _token(request, "worker")
        progress = data.progress if data else None
        if progress is not None:
            dumped = json.dumps(progress)
            if len(dumped.encode()) > MAX_PROGRESS_BYTES or "\\u0000" in dumped:
                # Renew the lease anyway: failing the heartbeat would lose a healthy job.
                logger.info(
                    "remote: progress dropped for lease %s (over %d bytes or NUL)", lease_id, MAX_PROGRESS_BYTES
                )
                progress = None
        expires_at, abort = await remote.repo.heartbeat(
            token, _uuid(lease_id, "lease_id"), remote.config.remote_lease_ttl, progress=progress
        )
        return HeartbeatResponse(lease_expires_at=expires_at.isoformat(), abort=abort)

    @post(
        "/leases/{lease_id:str}/complete",
        status_code=200,
        request_max_body_size=result_limit,
        responses={**_ERRORS, 409: ResponseSpec(ErrorResponse, description="lease_lost: drop the result")},
        summary="Report success (idempotent per lease)",
    )
    async def complete(
        request: Request, lease_id: str, remote: RemoteServer, data: CompleteRequest | None = None
    ) -> ReportResponse:
        token = _token(request, "worker")
        result = data.result if data else None
        if len(json.dumps(result).encode()) > remote.config.remote_max_result_bytes:
            raise TooLarge(
                f"result over {remote.config.remote_max_result_bytes} bytes; report it as a non-retryable failure"
            )
        # Stored in an envelope that decode() returns as is: a result from an untrusted
        # worker must never reach the app's serializer in any process that reads the job.
        outcome = Outcome("complete", encode_plain(result))
        reported = await remote.repo.report(token, _uuid(lease_id, "lease_id"), outcome)
        if reported.settled:
            remote.run_callbacks([reported.settled])
        return ReportResponse(status=reported.status)

    @post(
        "/leases/{lease_id:str}/fail",
        status_code=200,
        request_max_body_size=payload_limit,
        responses={**_ERRORS, 409: ResponseSpec(ErrorResponse, description="lease_lost: drop the result")},
        summary="Report failure or abort (idempotent per lease)",
    )
    async def fail(request: Request, lease_id: str, data: FailRequest, remote: RemoteServer) -> ReportResponse:
        token = _token(request, "worker")
        if data.aborted:
            outcome = Outcome("abort")
        else:
            if data.error is None:
                raise RemoteError('error is required unless aborted: {"type": ..., "message": ..., "stack": ...}')
            error: dict[str, Any] = {"type": data.error.type, "message": data.error.message}
            if data.error.stack is not None:
                error["stack"] = data.error.stack
            error = truncate_error(error, remote.config.remote_max_error_bytes)
            outcome = Outcome("fail", error=error_json(error), retryable=data.retryable)
        reported = await remote.repo.report(token, _uuid(lease_id, "lease_id"), outcome)
        if reported.settled:
            remote.run_callbacks([reported.settled])
        return ReportResponse(
            status=reported.status, retry_at=reported.retry_at.isoformat() if reported.retry_at else None
        )

    return [register, deregister, claim, heartbeat, complete, fail]


def make_remote_router(state: dict[str, Any], config: "WerkConfig") -> Router:
    """The ``/remote/v1`` router. *state* holds the :class:`RemoteServer` under ``"remote"``."""

    def _remote() -> RemoteServer:
        remote: RemoteServer = state["remote"]
        return remote

    return Router(
        path="/remote/v1",
        route_handlers=_handlers(config),
        dependencies={"remote": Provide(_remote, sync_to_thread=False)},
        exception_handlers={
            RemoteError: _remote_error,
            ValidationException: _validation_error,
            HTTPException: _http_error,
            Exception: _unexpected_error,
        },
        tags=["remote workers v1"],
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
