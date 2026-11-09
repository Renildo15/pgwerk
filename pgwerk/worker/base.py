from __future__ import annotations

import os
import abc
import json
import uuid
import random
import signal
import socket
import asyncio
import logging
import traceback

from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from datetime import datetime
from datetime import timezone

import psycopg

from psycopg.sql import SQL
from psycopg.sql import Identifier

from ..utils import fn_path
from ..utils import call_hook
from ..commons import JobStatus
from ..commons import DequeueStrategy
from ..logging import job_id_var
from ..schemas import Job
from ..schemas import Context
from ..serializers import encode
from ..maintenance import Maintenance
from ..settlement import Fence
from ..settlement import Outcome


if TYPE_CHECKING:
    from ..app import Werk


_TRANSIENT_DB_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)

logger = logging.getLogger(__name__)


class BaseWorker(abc.ABC):
    """Abstract base worker. Subclasses implement ``_execute``."""

    def __init__(
        self,
        app: "Werk",
        queues: list[str] | None = None,
        concurrency: int = 10,
        heartbeat_interval: int | None = None,
        poll_interval: float | None = None,
        dequeue_strategy: DequeueStrategy = DequeueStrategy.Priority,
        burst: bool = False,
        before_process: list[Callable] | None = None,
        after_process: list[Callable] | None = None,
        sweep_interval: float | None = None,
        abort_interval: float | None = None,
        shutdown_timeout: float | None = None,
        listen: bool | None = None,
        maintenance: bool = True,
        functions: list[str | Callable] | None = None,
    ) -> None:
        """Initialise the worker with scheduling and concurrency settings.

        Args:
            app: Connected :class:`~wrk.app.Werk` application instance.
            queues: Queue names to consume; defaults to ``["default"]``.
            concurrency: Maximum number of jobs executed simultaneously.
            heartbeat_interval: Seconds between worker heartbeat updates;
                falls back to ``app.config.heartbeat_interval``.
            poll_interval: Seconds between dequeue polls when idle;
                falls back to ``app.config.poll_interval``.
            dequeue_strategy: Order in which queues are polled.
            burst: When ``True``, shut down once all queues are empty.
            before_process: Hooks called before each job is executed.
            after_process: Hooks called after each job finishes (success or failure).
            sweep_interval: Seconds between stuck-job sweep runs;
                falls back to ``app.config.sweep_interval``.
            abort_interval: Seconds between abort-request polls;
                falls back to ``app.config.abort_interval``.
            shutdown_timeout: Seconds to wait for active jobs to drain on shutdown;
                falls back to ``app.config.shutdown_timeout``.
            listen: Whether to open a persistent ``LISTEN`` connection for
                instant ``NOTIFY``-driven wake-up. Falls back to
                ``app.config.listen``. Set to ``False`` when using PgBouncer
                in transaction-pooling mode.
            functions: Only claim jobs for these functions (callables or dotted
                paths, matched exactly against the enqueued ``function``). ``None``
                (default) claims every function on the worker's queues. Use it to
                split one queue across deployments that can run different code.
            maintenance: Run the maintenance pass (sweep, stranded ``waiting``
                job reconciliation, rescheduling) every ``sweep_interval``.
                Set to ``False`` when ``wrk maintain`` or the API process
                already runs it.
        """
        self.app = app
        self.queues = queues or ["default"]
        self.concurrency = concurrency
        self.heartbeat_interval = (
            heartbeat_interval if heartbeat_interval is not None else app.config.heartbeat_interval
        )
        self.poll_interval = poll_interval if poll_interval is not None else app.config.poll_interval
        self.dequeue_strategy = dequeue_strategy
        self.burst = burst
        self._before_process: list[Callable] = list(before_process or [])
        self._after_process: list[Callable] = list(after_process or [])
        self._sweep_interval = sweep_interval if sweep_interval is not None else app.config.sweep_interval
        self._abort_interval = abort_interval if abort_interval is not None else app.config.abort_interval
        self.shutdown_timeout = shutdown_timeout if shutdown_timeout is not None else app.config.shutdown_timeout
        self.listen = listen if listen is not None else app.config.listen
        self.maintenance = maintenance
        if functions is not None and not functions:
            raise ValueError("functions must not be empty; pass None to handle every function")
        self.functions: list[str] | None = (
            None if functions is None else [f if isinstance(f, str) else fn_path(f) for f in functions]
        )

        self.id = uuid.uuid4().hex
        self.name = f"{socket.gethostname()}.{os.getpid()}"
        self._running = False
        self._active: set[asyncio.Task] = set()
        # The running attempt of each job ({job_id: task}). The same worker can claim a
        # job again while a stale attempt of it still runs (the job was requeued under
        # it), so per-attempt state is keyed by the attempt's task, and each attempt
        # only ever removes its own entries.
        self._active_jobs: dict[str, asyncio.Task] = {}
        self._abort_requested: set[asyncio.Task] = set()
        # Leases this worker still holds ({job_id: lease_id}); an entry is removed as
        # soon as the attempt starts settling, so renewal never races its own settle.
        self._leases: dict[str, str] = {}
        self._lease_ttls: dict[str, int] = {}
        self._lease_lost: set[asyncio.Task] = set()
        self._acknowledging: set[asyncio.Task] = set()
        self._wakeup = asyncio.Event()
        self._exception_handlers: list[Callable] = []
        self._rr_offset = 0

    # ------------------------------------------------------------------
    # Hook registration
    # ------------------------------------------------------------------

    def add_before_process(self, hook: Callable) -> None:
        """Register a hook to be called before each job is executed.

        Args:
            hook: Sync or async callable that accepts a single
                :class:`~wrk.schemas.Context` argument.
        """
        self._before_process.append(hook)

    def add_after_process(self, hook: Callable) -> None:
        """Register a hook to be called after each job finishes.

        The hook is called regardless of whether the job succeeded or failed.

        Args:
            hook: Sync or async callable that accepts a single
                :class:`~wrk.schemas.Context` argument.
        """
        self._after_process.append(hook)

    # ------------------------------------------------------------------
    # Exception handler stack
    # ------------------------------------------------------------------

    def push_exception_handler(self, handler: Callable) -> None:
        """Push an exception handler onto the handler stack.

        Handlers are called in LIFO order when a job raises an unhandled
        exception. Each handler receives the ``(job, exception)`` pair.

        Args:
            handler: Sync or async callable with signature
                ``(job: Job, exc: Exception) -> None``.
        """
        self._exception_handlers.append(handler)

    def pop_exception_handler(self) -> Callable:
        """Remove and return the most recently pushed exception handler.

        Returns:
            The callable that was on top of the exception handler stack.

        Raises:
            IndexError: If the exception handler stack is empty.
        """
        if not self._exception_handlers:
            raise IndexError("No exception handlers on stack")
        return self._exception_handlers.pop()

    async def _invoke_exception_handlers(self, job: Job, exc: Exception) -> None:
        for handler in reversed(self._exception_handlers):
            try:
                result = handler(job, exc)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.warning("Exception handler %s raised: %s", handler, e)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start the worker and block until shutdown.

        Connects to the database if not already connected, registers the worker,
        and runs the main polling loop alongside heartbeat, LISTEN/NOTIFY, abort,
        and sweep side-loops. Installs SIGTERM/SIGINT handlers to trigger a
        graceful drain before exit.
        """
        loop = asyncio.get_running_loop()
        self._running = True
        self._wakeup.clear()

        await self._setup_executor()

        try:
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self._request_shutdown)
        except (NotImplementedError, ValueError):
            logger.info("Worker %s: signal handlers not installed (non-main thread or non-Unix)", self.name)

        await self.app.connect()

        try:
            await self._register()
            side_tasks = [
                asyncio.ensure_future(self._heartbeat_loop()),
                asyncio.ensure_future(self._abort_loop()),
                asyncio.ensure_future(self._lease_loop()),
            ]
            if self.maintenance:
                side_tasks.append(asyncio.ensure_future(self._sweep_loop()))
            if self.listen:
                side_tasks.append(asyncio.ensure_future(self._listen_loop()))
            try:
                await self._main_loop()
            finally:
                for t in side_tasks:
                    t.cancel()
                await asyncio.gather(*side_tasks, return_exceptions=True)
        finally:
            await self._deregister()
            await self._teardown_executor()

    async def _setup_executor(self) -> None:
        """Override in subclasses that need executor setup."""

    async def _teardown_executor(self) -> None:
        """Override in subclasses that need executor teardown."""

    def _request_shutdown(self) -> None:
        logger.info("Worker %s: shutdown requested", self.name)
        self._running = False
        self._wakeup.set()

    # ------------------------------------------------------------------
    # Registration & heartbeat
    # ------------------------------------------------------------------

    async def _register(self) -> None:
        worker_repo = self.app._worker_repo
        await worker_repo.register(
            self.id,
            self.name,
            self.queues,
            json.dumps(
                {
                    "pid": os.getpid(),
                    "concurrency": self.concurrency,
                    "queues": self.queues,
                    "strategy": self.dequeue_strategy.value,
                    "worker_type": type(self).__name__,
                    "burst": self.burst,
                    "functions": self.functions,
                }
            ),
            functions=self.functions,
        )
        logger.info("Worker %s registered (%s)", self.name, self.id)

    async def _deregister(self) -> None:
        if not self.app._connected:
            return
        try:
            worker_repo = self.app._worker_repo
            await worker_repo.deregister(self.id)
        except Exception as exc:
            logger.warning("Worker %s: deregister failed: %s", self.name, exc)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                worker_repo = self.app._worker_repo
                await worker_repo.update_heartbeat(self.id)
            except Exception as exc:
                logger.warning("Worker %s: heartbeat error: %s", self.name, exc)
            await asyncio.sleep(self.heartbeat_interval)

    # ------------------------------------------------------------------
    # LISTEN / NOTIFY
    # ------------------------------------------------------------------

    async def _listen_loop(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                conn = await self.app._direct_connect()
            except Exception as exc:
                if not self._running:
                    return  # type: ignore[unreachable]
                logger.warning("Worker %s: listen loop error, reconnecting in %.1fs: %s", self.name, backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue

            try:
                for queue in self.queues:
                    await conn.execute(SQL("LISTEN {ch}").format(ch=Identifier(f"{self.app.prefix}:{queue}")))
                backoff = 1.0
                self._wakeup.set()
                async for _ in conn.notifies():
                    if not self._running:
                        return  # type: ignore[unreachable]
                    self._wakeup.set()
            except Exception as exc:
                if not self._running:
                    return  # type: ignore[unreachable]
                logger.warning("Worker %s: listen loop error, reconnecting in %.1fs: %s", self.name, backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            finally:
                try:
                    await conn.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Abort loop
    # ------------------------------------------------------------------

    def _lease_renew_interval(self) -> float:
        """Renew at a quarter of the shortest TTL held, at most every heartbeat_interval."""
        shortest = min(
            (ttl for job_id, ttl in self._lease_ttls.items() if job_id in self._leases),
            default=self.app.config.local_lease_ttl,
        )
        return max(0.25, min(float(self.heartbeat_interval), shortest / 4))

    async def _lease_loop(self) -> None:
        """Renew every held lease in one UPDATE; cancel the attempts that lost theirs."""
        loop = asyncio.get_running_loop()
        last_renewal = loop.time()
        while self._running:
            # Tick often and renew when due, so a job with a short TTL that starts
            # just after a renewal isn't left waiting a long interval.
            await asyncio.sleep(0.25)
            held = dict(self._leases)
            if not held or loop.time() - last_renewal < self._lease_renew_interval():
                continue
            last_renewal = loop.time()
            try:
                renewed = await self.app._worker_repo.renew_leases(self.id, held, self.app.config.local_lease_ttl)
            except Exception as exc:
                logger.warning("Worker %s: lease renewal error: %s", self.name, exc)
                continue
            for job_id in held.keys() - renewed:
                if self._leases.get(job_id) != held[job_id]:
                    continue  # started settling meanwhile; not a loss
                self._leases.pop(job_id, None)
                task = self._active_jobs.get(job_id)
                if task is not None and not task.done():
                    logger.warning(
                        "Worker %s: job %s lost its lease (recovered elsewhere); cancelling it, nothing will be written",
                        self.name,
                        job_id,
                    )
                    self._lease_lost.add(task)
                    task.cancel()

    async def _abort_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self._abort_interval)
            if not self._active_jobs:
                continue
            try:
                worker_repo = self.app._worker_repo
                aborting = await worker_repo.get_aborting(list(self._active_jobs.keys()))
                for job_id in aborting:
                    task = self._active_jobs.get(job_id)
                    # The handler has returned: let the acknowledgement transaction
                    # settle the abort instead of cancelling persistence or callbacks.
                    if task is None or task in self._abort_requested or task in self._acknowledging:
                        continue
                    if not task.done():
                        logger.info("Worker %s: aborting job %s", self.name, job_id)
                        self._abort_requested.add(task)
                        task.cancel()
            except Exception as exc:
                logger.warning("Worker %s: abort loop error: %s", self.name, exc)

    # ------------------------------------------------------------------
    # Sweep loop
    # ------------------------------------------------------------------

    async def _sweep_loop(self) -> None:
        maintenance = Maintenance(self.app, interval=self._sweep_interval)
        while self._running:
            await asyncio.sleep(self._sweep_interval)
            report = await maintenance.run_once()  # never raises; logs its own errors
            if report.swept:
                logger.info("Worker %s: swept %d job(s)", self.name, len(report.swept))

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _supersede(self, job_id: str) -> None:
        """Cancel a stale attempt of a job this worker just claimed again.

        The claim gave the job a new lease, so the attempt still running here has lost
        its own: cancel it without writing anything, exactly as when a renewal finds the
        lease gone. An attempt already acknowledging is left alone; the fence rejects
        its write.
        """
        stale = self._active_jobs.get(job_id)
        if stale is None or stale.done() or stale in self._acknowledging:
            return
        logger.warning(
            "Worker %s: claimed job %s again while a stale attempt of it was running; cancelling that attempt",
            self.name,
            job_id,
        )
        self._lease_lost.add(stale)
        stale.cancel()

    def _forget_task(self, job_id: str, task: asyncio.Task) -> None:
        if self._active_jobs.get(job_id) is task:
            del self._active_jobs[job_id]

    async def _main_loop(self) -> None:
        try:
            while self._running:
                available = self.concurrency - len(self._active)
                if available > 0:
                    jobs = await self._dequeue(limit=available)

                    if self.burst and not jobs and not self._active and not self._wakeup.is_set():
                        logger.info("Worker %s: burst complete, shutting down", self.name)
                        self._running = False
                        return

                    for job in jobs:
                        self._supersede(job.id)
                        task = asyncio.create_task(self._handle_job(job))
                        self._active.add(task)
                        task.add_done_callback(self._active.discard)
                        task.add_done_callback(lambda _: self._wakeup.set())
                        self._active_jobs[job.id] = task
                        task.add_done_callback(lambda t, jid=job.id: self._forget_task(jid, t))  # type: ignore[misc]

                try:
                    jittered = self.poll_interval * (1 + random.uniform(-0.2, 0.2))
                    await asyncio.wait_for(self._wakeup.wait(), timeout=jittered)
                except asyncio.TimeoutError:
                    pass
                self._wakeup.clear()
        except asyncio.CancelledError:
            self._running = False

        if self._active:
            logger.info(
                "Worker %s: draining %d active job(s) (timeout=%ds)",
                self.name,
                len(self._active),
                self.shutdown_timeout,
            )
            try:
                await asyncio.shield(
                    asyncio.wait_for(
                        asyncio.gather(*list(self._active), return_exceptions=True),
                        timeout=self.shutdown_timeout,
                    )
                )
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.warning("Worker %s: drain interrupted, cancelling %d job(s)", self.name, len(self._active))
                for task in list(self._active):
                    task.cancel()
                await asyncio.gather(*list(self._active), return_exceptions=True)

    # ------------------------------------------------------------------
    # Dequeue
    # ------------------------------------------------------------------

    def _ordered_queues(self) -> list[str]:
        if self.dequeue_strategy == DequeueStrategy.RoundRobin:
            q = self.queues[self._rr_offset :] + self.queues[: self._rr_offset]
            self._rr_offset = (self._rr_offset + 1) % len(self.queues)
            return q
        if self.dequeue_strategy == DequeueStrategy.Random:
            return random.sample(self.queues, len(self.queues))
        return self.queues

    async def _dequeue(self, limit: int = 1) -> list[Job]:
        ordered = self._ordered_queues()
        try:
            worker_repo = self.app._worker_repo
            jobs = await worker_repo.dequeue(
                worker_id=self.id,
                ordered_queues=ordered,
                limit=limit,
                strategy=self.dequeue_strategy,
                lease_ttl=self.app.config.local_lease_ttl,
                functions=self.functions,
            )
        except Exception as exc:
            logger.error("Worker %s: dequeue error: %s", self.name, exc)
            return []

        for job in jobs:
            logger.info(
                "Worker %s: dequeued %s [%s] (attempt %d/%d, queue=%s)",
                self.name,
                job.function,
                job.id,
                job.attempts,
                job.max_attempts,
                job.queue,
            )
        return jobs

    # ------------------------------------------------------------------
    # Job execution lifecycle
    # ------------------------------------------------------------------

    async def _handle_job(self, job: Job) -> None:
        token = job_id_var.set(job.id)
        logger.info(
            "Worker %s: starting %s [%s] (attempt %d/%d)",
            self.name,
            job.function,
            job.id,
            job.attempts,
            job.max_attempts,
        )
        ctx = Context(app=self.app, worker=self, job=job)
        me = asyncio.current_task()
        if job.lease_id is not None:
            self._leases[job.id] = job.lease_id
            self._lease_ttls[job.id] = job.heartbeat_secs or self.app.config.local_lease_ttl

        try:
            try:
                for hook in self._before_process:
                    try:
                        await call_hook(hook, ctx)
                    except Exception:
                        logger.exception("Worker %s: before_process hook error", self.name)

                result = await self._execute(job, ctx)
            except asyncio.CancelledError:
                self._release_lease(job)
                if me in self._lease_lost:
                    # The sweep recovered the job; another attempt owns it now.
                    return
                if me in self._abort_requested:
                    await self._nack(job, "aborted", aborted=True)
                else:
                    await self._requeue_cancelled(job)
                return
            except Exception as exc:
                ctx.exception = exc
                logger.error(
                    "Worker %s: failed %s [%s] (attempt %d/%d): %s",
                    self.name,
                    job.function,
                    job.id,
                    job.attempts,
                    job.max_attempts,
                    exc,
                    exc_info=True,
                )
                await self._invoke_exception_handlers(job, exc)
                self._release_lease(job)
                await self._nack(job, traceback.format_exc())
                return

            self._release_lease(job)
            if me is not None:
                self._acknowledging.add(me)
            # The handler has returned. Finish persistence even if a drain timeout
            # cancels this task; requeueing here would execute its side effects again.
            ack_task = asyncio.create_task(self._ack_with_retry(job, result))
            while True:
                try:
                    await asyncio.shield(ack_task)
                    break
                except asyncio.CancelledError:
                    if ack_task.cancelled():
                        raise
        except asyncio.CancelledError:
            if me in self._acknowledging:
                raise
            self._release_lease(job)
            if me not in self._lease_lost:
                await self._requeue_cancelled(job)
        finally:
            try:
                job_id_var.reset(token)
                for hook in self._after_process:
                    try:
                        await call_hook(hook, ctx)
                    except Exception:
                        logger.exception("Worker %s: after_process hook error", self.name)
            finally:
                if me is not None:
                    self._acknowledging.discard(me)
                    self._abort_requested.discard(me)
                    self._lease_lost.discard(me)
                self._release_lease(job, ttl=True)

    def _release_lease(self, job: Job, ttl: bool = False) -> None:
        """Stop renewing this attempt's lease, never another attempt's of the same job."""
        if job.lease_id is None or self._leases.get(job.id) == job.lease_id:
            self._leases.pop(job.id, None)
            if ttl:
                self._lease_ttls.pop(job.id, None)
        elif ttl and job.id not in self._leases:
            self._lease_ttls.pop(job.id, None)

    @abc.abstractmethod
    async def _execute(self, job: Job, ctx: Context) -> Any:
        """Run the job and return its result. Raise on failure."""

    @staticmethod
    async def _with_timeout(awt: Any, timeout: int | None) -> Any:
        return await (asyncio.wait_for(awt, timeout=timeout) if timeout else awt)

    # ------------------------------------------------------------------
    # Ack / Nack / Requeue
    # ------------------------------------------------------------------

    async def _ack_with_retry(self, job: Job, result: Any, *, max_attempts: int = 5) -> None:
        """Retry _ack on transient DB errors with exponential backoff. Correctness-critical."""
        delay = 1.0
        for attempt in range(1, max_attempts + 1):
            try:
                await self._ack(job, result)
                return
            except _TRANSIENT_DB_ERRORS as exc:
                if attempt == max_attempts:
                    logger.critical(
                        "Worker %s: ack failed after %d attempts for job %s [%s], sweep will recover: %s",
                        self.name,
                        max_attempts,
                        job.function,
                        job.id,
                        exc,
                    )
                    return
                logger.warning(
                    "Worker %s: transient ack error for job %s (attempt %d/%d), retrying in %.1fs: %s",
                    self.name,
                    job.id,
                    attempt,
                    max_attempts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30.0)

    def _fence(self, job: Job) -> Fence:
        return Fence(job_id=job.id, worker_id=self.id, attempt=job.attempts, lease_id=job.lease_id)

    async def _ack(self, job: Job, result: Any = None) -> None:
        outcome = Outcome("complete", result_json=encode(self.app.serializer, result))
        settled = await self.app._settlement.settle(job, self._fence(job), outcome)
        if settled.status is None:
            logger.warning(
                "ack race detected for job %s [%s] — row no longer owned by this worker", job.function, job.id
            )
        elif settled.status == JobStatus.Aborted:
            logger.info("Worker %s: aborted %s [%s]", self.name, job.function, job.id)
        elif settled.status == JobStatus.Complete:
            logger.info("Worker %s: completed %s [%s]", self.name, job.function, job.id)

    async def _nack(self, job: Job, error: str, *, aborted: bool = False) -> None:
        outcome = Outcome("abort") if aborted else Outcome("fail", error=error)
        settled = await self.app._settlement.settle(job, self._fence(job), outcome)
        t = settled.transition
        if t is None:
            logger.warning(
                "nack race detected for job %s [%s] — row no longer owned by this worker, skipping",
                job.function,
                job.id,
            )
            return

        if t.delete:
            logger.info(
                "Worker %s: deleted %s [%s] on terminal failure (failure_mode=delete)", self.name, job.function, job.id
            )
        elif t.retrying and t.scheduled_at:
            logger.info(
                "Worker %s: retrying %s [%s] in %.0fs (attempt %d/%d)",
                self.name,
                job.function,
                job.id,
                (t.scheduled_at - datetime.now(timezone.utc)).total_seconds(),
                job.attempts,
                job.max_attempts,
            )
        elif t.retrying:
            logger.info(
                "Worker %s: retrying %s [%s] immediately (attempt %d/%d)",
                self.name,
                job.function,
                job.id,
                job.attempts,
                job.max_attempts,
            )
        elif t.status == JobStatus.Failed:
            logger.warning(
                "Worker %s: giving up on %s [%s] after %d/%d attempts",
                self.name,
                job.function,
                job.id,
                job.attempts,
                job.max_attempts,
            )
        elif t.status == JobStatus.Aborted:
            logger.info("Worker %s: aborted %s [%s]", self.name, job.function, job.id)

    async def _requeue_cancelled(self, job: Job) -> None:
        worker_repo = self.app._worker_repo
        await worker_repo.requeue_cancelled(self.id, job)
        logger.info("Worker %s: re-queued cancelled job %s", self.name, job.id)
