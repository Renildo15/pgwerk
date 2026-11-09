"""``RemoteWorker``: run jobs from a pgwerk server over HTTPS, with no database access."""

from __future__ import annotations

import os
import json
import uuid
import signal
import socket
import asyncio
import inspect
import logging
import traceback

from typing import Any
from typing import Callable

import httpx

from .http import ProtocolError
from .http import ProtocolClient
from .context import RemoteJob
from .context import RemoteContext
from ...utils import fn_path
from ...utils import wants_context


logger = logging.getLogger(__name__)

CLIENT_VERSION = "pgwerk-python/remote-1"


class NonRetryable(Exception):
    """Raise (or wrap an error in) this to fail the job terminally, even with retries left."""


class _Stop(Exception):
    """Internal: this worker's token stopped working (revoked or expired); shut down."""


class RemoteWorker:
    """A worker that reaches pgwerk over HTTPS: register, long-poll claims, heartbeat, report.

    Args:
        url: The pgwerk server, e.g. ``https://jobs.example.com``.
        token: An enrollment token (``pgwk_e_…``) from ``werk token create``.
        queues: Queues to consume; must be within the token's scope.
        functions: Handlers to run, as ``{name: callable}`` (the name jobs are
            enqueued with) or a list of callables (named by their dotted path).
        concurrency: Jobs run at once; each slot runs its own claim loop.
        name: Shown in the dashboard; defaults to ``hostname.pid``.
        labels: Free-form metadata stored with the worker.
        insecure: Allow a plain ``http://`` URL (local testing only).
        transport: An ``httpx`` transport, for tests.
    """

    def __init__(
        self,
        url: str,
        token: str,
        queues: list[str],
        functions: dict[str, Callable] | list[Callable],
        *,
        concurrency: int = 1,
        name: str | None = None,
        labels: dict[str, Any] | None = None,
        insecure: bool = False,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.handlers: dict[str, Callable] = (
            dict(functions) if isinstance(functions, dict) else {fn_path(f): f for f in functions}
        )
        if not self.handlers:
            raise ValueError("a remote worker needs at least one function")
        self.queues = queues
        self.concurrency = concurrency
        self.name = name or f"{socket.gethostname()}.{os.getpid()}"
        self.labels = labels or {}
        self._enrollment = token
        self._client = ProtocolClient(url, insecure=insecure, transport=transport)
        self.worker_id: str | None = None
        self._token: str | None = None
        self.lease_ttl = 60
        self.claim_wait = 30
        self._stopping = asyncio.Event()
        self._draining = False
        self._revoked = False
        self._expired = False
        self._claims: set[asyncio.Task] = set()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Register, then process jobs until :meth:`stop`, SIGTERM/SIGINT, or revocation.

        If the server expires this worker's session (it heard nothing from it for too
        long, e.g. during an outage), the worker registers again and carries on. On
        the way out it finishes running jobs, then deregisters (jobs it still holds go
        back to the queue).
        """
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.stop)
            except (NotImplementedError, ValueError, RuntimeError):
                pass
        try:
            if not await self._register():
                return
            while True:
                self._expired = False
                slots = [asyncio.create_task(self._slot(i)) for i in range(self.concurrency)]
                await asyncio.gather(*slots)
                if not self._expired or self._stopping.is_set() or self._revoked:
                    break
                logger.warning(
                    "remote worker %s: the server expired this session (it heard nothing from us for too long); "
                    "registering again",
                    self.name,
                )
                self._draining = False
                if not await self._register():
                    return
        finally:
            if self.worker_id is not None and not self._revoked and not self._expired:
                try:
                    await self._client.call("/deregister", self._token or "")
                except Exception as exc:
                    logger.warning("remote worker %s: deregister failed: %s", self.name, exc)
            await self._client.close()

    def stop(self) -> None:
        """Stop claiming; running jobs finish, then the worker deregisters."""
        logger.info("remote worker %s: stopping", self.name)
        self._stopping.set()
        for task in list(self._claims):
            task.cancel()

    def _expire(self) -> None:
        """The session expired: end it (running jobs lost their leases) and register again."""
        self._expired = True
        for task in list(self._claims):
            task.cancel()

    async def _register(self) -> bool:
        """Register, retrying while the server is unreachable. ``False`` if stopped meanwhile."""
        body = {
            "name": self.name,
            "queues": self.queues,
            "functions": [{"name": n} for n in self.handlers],
            "labels": self.labels,
            "client": {"lang": "python", "version": CLIENT_VERSION},
        }
        delay = 1.0
        while True:
            if self._stopping.is_set():
                return False
            try:
                _, reg = await self._client.call("/register", self._enrollment, body)
                break
            except httpx.TransportError as exc:
                logger.warning("remote worker %s: register failed (%s), retrying in %.0fs", self.name, exc, delay)
            except ProtocolError as exc:
                if not exc.retryable:
                    raise
                logger.warning("remote worker %s: register failed (%s), retrying in %.0fs", self.name, exc, delay)
            await self._sleep(delay)
            delay = min(delay * 2, 30.0)
        self.worker_id = reg["worker_id"]
        self._token = reg["worker_token"]
        self.lease_ttl = reg.get("lease_ttl", 60)
        self.claim_wait = reg.get("claim_wait", 30)
        logger.info("remote worker %s registered as %s", self.name, self.worker_id)
        return True

    # ------------------------------------------------------------------
    # Claim loop
    # ------------------------------------------------------------------

    async def _slot(self, index: int) -> None:
        backoff = 1.0
        claim_id = str(uuid.uuid4())
        while not self._stopping.is_set() and not self._draining and not self._expired:
            task = asyncio.create_task(
                self._client.call(
                    "/claim",
                    self._token or "",
                    {"claim_id": claim_id, "wait": self.claim_wait},
                    timeout=self.claim_wait + 10,
                )
            )
            self._claims.add(task)
            try:
                status, body = await task
            except asyncio.CancelledError:
                if self._stopping.is_set() or self._expired:
                    return
                raise
            except httpx.TransportError as exc:
                # Same claim_id on retry: if the server did claim a job, we get it back.
                logger.warning("remote worker %s: claim failed (%s), retrying", self.name, exc)
                await self._sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            except ProtocolError as exc:
                if exc.code == "draining":
                    logger.info("remote worker %s is draining: finishing held jobs", self.name)
                    self._draining = True
                    return
                if exc.code == "session_expired":
                    self._expire()
                    return
                if exc.status == 401:
                    self._revoke()
                    return
                logger.warning("remote worker %s: claim error %s", self.name, exc)
                await self._sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            finally:
                self._claims.discard(task)
            backoff = 1.0
            claim_id = str(uuid.uuid4())
            if status == 200 and body:
                await self._run_job(RemoteJob.from_claim(body))

    async def _sleep(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stopping.wait(), seconds)
        except asyncio.TimeoutError:
            pass

    def _revoke(self) -> None:
        if not self._revoked:
            logger.error("remote worker %s: token no longer valid (revoked or retired); stopping", self.name)
        self._revoked = True
        self.stop()

    # ------------------------------------------------------------------
    # Running one job
    # ------------------------------------------------------------------

    async def _run_job(self, job: RemoteJob) -> None:
        ctx = RemoteContext(job=job, worker=self)
        handler = self.handlers.get(job.function)
        if handler is None:  # the server only sends declared functions; be defensive
            await self._report(job, "/fail", _error("UnknownFunction", job.function), retryable=False)
            return

        run = asyncio.create_task(self._call(handler, ctx, job))
        lost = asyncio.Event()
        beat = asyncio.create_task(self._heartbeat_loop(ctx, run, lost))
        try:
            try:
                result = (
                    await asyncio.wait_for(asyncio.shield(run), job.timeout_secs) if job.timeout_secs else await run
                )
            except asyncio.TimeoutError:
                run.cancel()
                await self._report(job, "/fail", _error("Timeout", f"exceeded {job.timeout_secs}s"))
                return
            except asyncio.CancelledError:
                if lost.is_set():
                    logger.warning("remote worker %s: job %s lost its lease; result dropped", self.name, job.id)
                    return
                if ctx.aborted:
                    await self._report(job, "/fail", None, aborted=True)
                    return
                raise
            except NonRetryable as exc:
                await self._report(job, "/fail", _exc_error(exc.__cause__ or exc), retryable=False)
                return
            except Exception as exc:
                await self._report(job, "/fail", _exc_error(exc))
                return
            if lost.is_set():
                return
            try:
                json.dumps(result)
            except (TypeError, ValueError) as exc:
                await self._report(job, "/fail", _error("UnserializableResult", str(exc)), retryable=False)
                return
            await self._report(job, "/complete", {"result": result})
        finally:
            beat.cancel()
            await asyncio.gather(beat, return_exceptions=True)

    async def _call(self, handler: Callable, ctx: RemoteContext, job: RemoteJob) -> Any:
        args = [ctx, *job.args] if wants_context(handler) else list(job.args)
        if inspect.iscoroutinefunction(handler):
            return await handler(*args, **job.kwargs)
        return await asyncio.to_thread(handler, *args, **job.kwargs)

    async def _heartbeat_loop(self, ctx: RemoteContext, run: asyncio.Task, lost: asyncio.Event) -> None:
        job = ctx.job
        while not run.done():
            await asyncio.sleep(job.heartbeat_every)
            try:
                _, body = await self._client.call(
                    f"/leases/{job.lease_id}/heartbeat", self._token or "", {"progress": ctx._progress}
                )
            except ProtocolError as exc:
                if exc.code == "lease_lost" or exc.status == 401:
                    lost.set()
                    run.cancel()
                    if exc.code == "session_expired":
                        self._expire()
                    elif exc.status == 401:
                        self._revoke()
                    return
                logger.warning("remote worker %s: heartbeat error for %s: %s", self.name, job.id, exc)
                continue
            except httpx.TransportError as exc:
                logger.warning("remote worker %s: heartbeat failed for %s: %s", self.name, job.id, exc)
                continue
            if body and body.get("abort") and not ctx.aborted:
                logger.info("remote worker %s: job %s aborted by the server", self.name, job.id)
                ctx._aborted = True
                run.cancel()

    async def _report(
        self,
        job: RemoteJob,
        path: str,
        error_or_body: dict[str, Any] | None,
        *,
        retryable: bool = True,
        aborted: bool = False,
    ) -> None:
        """Send complete/fail, retrying for at least a lease TTL; the job's effects already happened."""
        if path == "/complete":
            body = error_or_body or {}
        elif aborted:
            body = {"aborted": True}
        else:
            body = {"error": error_or_body, "retryable": retryable}
        url = f"/leases/{job.lease_id}{path}"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(job.lease_ttl, 30)
        delay = 0.5
        while True:
            try:
                await self._client.call(url, self._token or "", body)
                return
            except ProtocolError as exc:
                if exc.code == "too_large" and path == "/complete":
                    path, url = "/fail", f"/leases/{job.lease_id}/fail"
                    body = {"error": _error("ResultTooLarge", exc.message), "retryable": False}
                    continue
                if exc.code == "lease_lost":
                    logger.warning("remote worker %s: job %s lost its lease; result dropped", self.name, job.id)
                    return
                if exc.code == "session_expired":
                    logger.warning("remote worker %s: session expired; result of job %s dropped", self.name, job.id)
                    self._expire()
                    return
                if exc.status == 401:
                    self._revoke()
                    return
                if not exc.retryable:
                    logger.error("remote worker %s: reporting job %s failed: %s", self.name, job.id, exc)
                    return
            except httpx.TransportError as exc:
                logger.warning("remote worker %s: reporting job %s failed (%s), retrying", self.name, job.id, exc)
            if loop.time() > deadline:
                logger.error("remote worker %s: giving up reporting job %s", self.name, job.id)
                return
            await asyncio.sleep(delay)
            delay = min(delay * 2, 10.0)


def _error(kind: str, message: str) -> dict[str, Any]:
    return {"type": kind, "message": message}


def _exc_error(exc: BaseException) -> dict[str, Any]:
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "stack": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
    }
