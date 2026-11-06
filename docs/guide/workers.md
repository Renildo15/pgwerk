---
title: Workers — pgwerk Postgres job queue
description: Run background jobs with pgwerk workers. Choose from AsyncWorker (asyncio), ThreadWorker, ProcessWorker, or ForkWorker. Configure concurrency, queues, and lifecycle hooks.
keywords: pgwerk worker, asyncio worker python, background job worker postgres, concurrent job processing python
---

# Workers

Workers dequeue jobs from Postgres and execute them. `werk` ships with four worker types, all sharing the same base behaviour.

## Worker types

### AsyncWorker

The default. Runs handlers as coroutines on a single asyncio event loop. Best for I/O-bound work (HTTP calls, database queries, file I/O).

```python
from pgwerk import AsyncWorker

worker = AsyncWorker(app=app, queues=["default"], concurrency=20)
await worker.run()
```

### ThreadWorker

Runs each handler in a thread-pool executor. Suitable for blocking libraries that are not async-aware.

```python
from pgwerk import ThreadWorker

worker = ThreadWorker(app=app, concurrency=8)
await worker.run()
```

### ProcessWorker

Runs handlers in a process-pool executor. Use this for CPU-bound work that benefits from true parallelism (bypasses the GIL).

```python
from pgwerk import ProcessWorker

worker = ProcessWorker(app=app, concurrency=4)
await worker.run()
```

### ForkWorker

Forks a new process for each individual job. Provides maximum isolation — a crashing job cannot affect the worker process. A `SIGTERM` grace period allows the child process to finish before `SIGKILL` is sent.

```python
from pgwerk import ForkWorker

worker = ForkWorker(app=app, concurrency=4)
await worker.run()
```

## Configuration

All workers share these constructor parameters:

| Parameter | Default | Description |
|---|---|---|
| `app` | required | Connected `Werk` instance |
| `queues` | `["default"]` | Queue names to consume |
| `concurrency` | `10` | Max jobs processed simultaneously |
| `heartbeat_interval` | `10` | Seconds between worker heartbeats |
| `poll_interval` | `5.0` | Seconds between database polls when idle |
| `dequeue_strategy` | `Priority` | How queues are ordered when dequeuing |
| `burst` | `False` | Exit once the queue is empty |
| `before_process` | `[]` | Hooks called before each job starts |
| `after_process` | `[]` | Hooks called after each job finishes |
| `sweep_interval` | `60.0` | Seconds between maintenance passes |
| `abort_interval` | `1.0` | Seconds between abort-signal checks |
| `shutdown_timeout` | `30.0` | Seconds to wait for in-flight jobs during shutdown |
| `maintenance` | `True` | Run the maintenance pass in this worker (see [Maintenance](#maintenance)) |
| `functions` | `None` | Only claim jobs for these functions (see [Function routing](#function-routing)) |

## Dequeue strategies

Control how the worker picks jobs from multiple queues:

```python
from pgwerk import AsyncWorker, DequeueStrategy

# Priority (default) — higher-priority jobs run first across all queues
worker = AsyncWorker(app=app, queues=["high", "low"], dequeue_strategy=DequeueStrategy.Priority)

# Round-robin — cycle through queues evenly
worker = AsyncWorker(app=app, queues=["a", "b", "c"], dequeue_strategy=DequeueStrategy.RoundRobin)

# Random — pick a queue at random each poll
worker = AsyncWorker(app=app, queues=["a", "b", "c"], dequeue_strategy=DequeueStrategy.Random)
```

## Burst mode

The worker exits as soon as the queue drains instead of waiting for new work:

```python
worker = AsyncWorker(app=app, burst=True)
await worker.run()
# returns when the queue is empty
```

## Lifecycle hooks

Run code before or after every job:

```python
from pgwerk import Context

async def log_start(ctx: Context) -> None:
    print(f"Starting {ctx.job.function} [{ctx.job.id}]")

async def log_end(ctx: Context) -> None:
    if ctx.exception:
        print(f"Failed: {ctx.exception}")
    else:
        print(f"Completed [{ctx.job.id}]")

worker = AsyncWorker(
    app=app,
    before_process=[log_start],
    after_process=[log_end],
)

# Or add them later
worker.add_before_process(log_start)
worker.add_after_process(log_end)
```

Hooks receive a `Context` object. In `after_process` hooks, `ctx.exception` is set if the job raised.

## Exception handlers

Push handlers onto a stack to intercept job failures:

```python
async def report_error(job, exc):
    sentry_sdk.capture_exception(exc)

worker.push_exception_handler(report_error)
```

Handlers are called in reverse push order (most recently pushed first). Use `pop_exception_handler()` to remove the top handler.

## Function routing

By default a worker claims every job on its queues. Pass `functions` to claim only some of
them:

```python
# GPU image: only the rendering functions
AsyncWorker(app=app, queues=["media"], functions=[render_thumbnail, "media.tasks.transcode"])
```

```bash
werk worker myapp.tasks:app -q media -f media.tasks.render_thumbnail -f media.tasks.transcode
```

This splits one queue across deployments that can run different code, such as two images of
the same codebase, without failing imports on jobs the worker can't run. Functions are matched
exactly against the `function` stored at enqueue (the callable's dotted path). Group
concurrency (`_group`) still counts every function in the group.

## Maintenance

Every `sweep_interval` seconds a maintenance pass:

- **recovers stuck jobs** (the sweep).
  - **Lost attempt:** the lease of a running job expired, because its worker stopped renewing it
    (crash, OOM kill, eviction, lost network). The job is requeued at once **without using a
    retry**, and `lost_count` goes up. After `max_lost` losses (default 3, `_max_lost` per job) it
    fails with `error = 'lease_lost'`, like any other failure. A job that keeps killing its
    worker therefore stops instead of cycling forever.
  - **Timeout:** past `timeout_secs` (normally enforced by the worker itself), it counts as a
    failed attempt with `error = 'timeout'` and follows its retries.
  - **Abort:** an `aborting` job in either situation ends `aborted` and is never requeued.
  - Every terminal outcome settles normally: dependents settle, `failure_mode` and `failure_ttl`
    apply, and `on_failure`/`on_stopped` run.

## Leases

When a worker claims a job, that attempt gets a lease: an id (also the id of the attempt's
execution row) and an expiry. The lease lasts `heartbeat_secs` if the job has it, or
`local_lease_ttl` (300s). Each worker renews the leases of all its running jobs in one query,
at a quarter of the shortest TTL it holds and at least every `heartbeat_interval`.

Every write for an attempt is fenced on its lease. If the sweep recovered the job, the old
attempt's late result is rejected, and at its next renewal the old worker cancels the job
without writing anything. A worker that dies is noticed within one lease TTL, not after
`max_active_secs`. `max_active_secs` now only applies to jobs claimed by workers older than
schema 8, during a rolling deploy.
- **deletes rows whose retention ran out.** Finished jobs with `_result_ttl`/`_failure_ttl` are
  deleted once it passes (up to 1000 per pass, oldest first), along with their executions and
  stopped worker rows. Turn it off with `WerkConfig(purge_expired=False)`.
- **expires unstarted jobs.** A `queued` or `scheduled` job whose `_ttl` ran out ends `aborted`
  with `error = 'expired'`: dependents settle, `failure_mode="delete"` applies, `on_stopped` runs.
- **settles stranded dependents.** A `waiting` job whose dependencies have all settled, but
  that was never moved on (for example because a dependency was settled outside pgwerk), is
  queued, or failed with `error = 'dependency failed'` when a required dependency failed.
- **queues due scheduled jobs.**

By default every worker runs it. To recover jobs with no worker up, run it on its own with
[`werk maintain`](cli.md#werk-maintain), or inside the API process with `werk api --maintenance`.
Then you can turn it off on workers with `maintenance=False`.

Every query skips rows locked by another runner, so any number of them can run at once.
Callbacks of jobs settled during maintenance (such as `on_stopped`) run in the process that
ran it, so their modules must be importable there.

The Prometheus exporter reports `wrk_waiting_jobs_stalled{queue}`: jobs in `waiting` for over
an hour. Maintenance clears the ones whose dependencies settled; anything left there is
waiting on a dependency that isn't progressing.

## Signals and shutdown

Workers install handlers for `SIGTERM` and `SIGINT`. On receipt, the worker stops accepting new jobs and waits up to `shutdown_timeout` seconds for in-flight jobs to finish before cancelling them.

Once a handler has returned, the worker lets acknowledgement and its callbacks
finish, including the existing retries for transient database errors. This can
extend shutdown beyond `shutdown_timeout`; cancelling acknowledgement must not
requeue a handler that has already performed its work.

## Running alongside a web server

Workers can run in the same process as an ASGI/WSGI application using background tasks or a separate asyncio task:

```python
import asyncio
from pgwerk import AsyncWorker

async def main():
    async with app:
        worker = AsyncWorker(app=app)
        await asyncio.gather(
            run_web_server(),
            worker.run(),
        )
```
