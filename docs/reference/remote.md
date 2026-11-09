# Remote workers (experimental)

!!! warning "Experimental"
    The remote-worker protocol (`/remote/v1`) may still change without a version bump.

A remote worker runs away from the database. It only needs outbound HTTPS to a pgwerk server,
which keeps Postgres private. Use it for capacity you don't fully control: a GPU box in someone's
home, spot instances, another cloud, a customer's network.

```
 producers ──enqueue──► Postgres ◄── pgwerk server (werk api --remote-workers)
                                          ▲  HTTPS, outbound only
                                          │
                              remote workers (RemoteWorker)
```

## Server

```bash
pip install "pgwerk[api]"

# Admin API + dashboard + remote router (the admin API must have a token)
werk api --dsn "$DSN" --remote-workers --api-token "$ADMIN_TOKEN"

# Or a process that serves only remote workers
werk api --dsn "$DSN" --remote-workers --no-api
```

- **TLS:** terminate HTTPS in front of the server (load balancer, reverse proxy, tunnel).
- **Maintenance runs in this process by default** (`--no-maintenance` to turn it off). It recovers
  the jobs of remote workers that disappear and expires idle workers.
- **Callbacks** (`on_success`, `on_failure`, `on_stopped`) of jobs that remote workers settle run in
  the server process, in the background. The callbacks' modules must be importable there.
- **Producers must use the JSON serializer.** The remote protocol is JSON only.
- **Remote results are stored in an envelope**, `{"__pgwerk_json__": <result>}`, which pgwerk's readers unwrap.
  The envelope tells them never to hand the value to the app's serializer: an app or CLI that reads jobs with
  `PickleSerializer` can't be made to unpickle bytes a remote worker chose. Raw SQL readers see the envelope.
- **Scaling:** several replicas can run side by side (`SKIP LOCKED` keeps claims apart). The claim
  rate limit applies per replica.
- **Database connections:** the API process holds at most `pool_size` connections (20 by default,
  `PGWERK_POOL_SIZE`), plus one `LISTEN` connection, however many workers are connected. Waiting
  claims hold none. An enqueue wakes one waiting worker per group (workers that declared the same
  functions); a worker that gets a job wakes the next one, so a burst still reaches every idle worker.
- **Unlogged worker table:** with `ephemeral_tables=True`, worker sessions are lost if Postgres
  crashes, and remote workers must register again.

## Access

```bash
werk token create --dsn "$DSN" --name gpu-box-1 --queues render   # printed once
werk token list   --dsn "$DSN"
werk token revoke --dsn "$DSN" <token-id>      # compromise: stops all its workers now
werk workers revoke --dsn "$DSN" <worker-id>   # retire one worker now
werk workers revoke --dsn "$DSN" <worker-id> --drain   # finish what it holds, claim nothing new
```

An enrollment token is scoped to an exact list of queues. Each worker process exchanges it for its
own worker token when it registers. An expired enrollment token stops new registrations; workers
already registered keep working. Revoking releases the revoked workers' jobs back to the queue
without counting them as lost.

Limits, per server replica: a worker may claim `remote_claim_rate` times per second (burst
`remote_claim_burst`) with at most `remote_max_claims_per_worker` long-polls open; a token may register
`remote_register_rate` workers per second (burst `remote_register_burst`) and have at most
`remote_max_workers_per_token` live ones. All limits apply only after the token's secret checked out,
so a stranger who knows a worker or token id can't use them up. Names are stripped of control
characters; queues, functions and labels have size limits.

## Python client

```bash
pip install "pgwerk[remote]"
```

```python
import asyncio
from pgwerk.remote import RemoteWorker, NonRetryable

async def render(ctx, asset_id: str, upload_url: str) -> dict:
    ctx.heartbeat({"stage": "rendering"})       # optional progress, sent with the lease heartbeat
    if ctx.aborted:                            # set when the job is aborted from the server
        ...
    return {"uri": f"s3://bucket/{asset_id}.png"}

worker = RemoteWorker(
    "https://jobs.example.com",
    token="pgwk_e_…",
    queues=["render"],
    functions={"media.tasks.render": render},   # the name jobs are enqueued with
    concurrency=2,
)
asyncio.run(worker.run())   # SIGTERM: finishes running jobs, then deregisters
```

- Handlers follow the local rules: they get `ctx` first if their first parameter is named `ctx`,
  sync handlers run in a thread, and `args`/`kwargs` come from the enqueue call.
- `ctx` is a `RemoteContext` (`job`, `worker`, `heartbeat()`, `aborted`). It has no `app`: a job
  that enqueues other jobs or reads the database has to run on a local worker.
- Raising an exception fails the attempt and follows the job's retries. Raise `NonRetryable` to fail
  for good.
- `timeout_secs` is enforced by the client.
- If the worker's machine disappears, the job comes back after its lease TTL (60s), without using a
  retry, up to `_max_lost` times.
- If the server hears nothing from an idle worker for a while (`max(2 × lease TTL, claim wait + lease TTL)`,
  e.g. during an outage of either side), it expires the worker's session. The next call gets
  `401 session_expired`, and the client registers again with its enrollment token and carries on; jobs it held
  under the old session are lost and come back to the queue. A revoked or retired worker gets
  `401 unauthorized` and stops.

## Large files

pgwerk doesn't store artifacts. Put presigned URLs (or what's needed to get them) in the job's
arguments, have the worker upload directly to storage, and return a reference:

```python
upload_url = s3.generate_presigned_url("put_object", Params={"Bucket": "imgs", "Key": key}, ExpiresIn=6 * 3600)
await app.enqueue("media.tasks.render", asset_id=key, upload_url=upload_url, _queue="render", _ttl=3600)
# the worker returns {"uri": ..., "sha256": ..., "size": ...}
```

Presigned URLs expire, so give a job that can wait a long time in the queue a `_ttl` shorter than
the URL's lifetime.

## Protocol v1

JSON over HTTPS under `/remote/v1`. Every request sends `Pgwerk-Protocol: 1` and
`Authorization: Bearer <token>`: the enrollment token for `register`, the worker token for the rest.

| Endpoint | Body | Response |
|---|---|---|
| `POST /register` | `{name, queues, functions: [{name}], labels?, client: {lang, version}}` | `201 {worker_id, worker_token, protocol, lease_ttl, claim_wait, limits}` |
| `POST /claim` | `{claim_id, wait?}` | `200 {job, lease_id, lease_expires_at, lease_ttl, heartbeat_every}` or `204` |
| `POST /leases/{lease_id}/heartbeat` | `{progress?}` | `200 {lease_expires_at, abort}` |
| `POST /leases/{lease_id}/complete` | `{result}` | `200 {status}` |
| `POST /leases/{lease_id}/fail` | `{error: {type, message, stack?}, retryable?, aborted?}` | `200 {status, retry_at?}` |
| `POST /deregister` | none | `204` |

- **`claim`:**
  - It long-polls up to `wait` seconds (server maximum 30); use a client timeout of at least 40s.
  - Send a fresh `claim_id` (UUID) per claim and reuse it when retrying that claim: a lost response
    returns the same lease. Once that attempt is over, the id gets `204`.
  - `job` has `id`, `function`, `queue`, `kwargs`, `args`, `attempts`, `max_attempts`,
    `lost_count`, `timeout_secs` and `meta`. `kwargs` is the cross-language contract. A non-Python client should
    fail a job with non-empty `args` with `retryable: false`.
- **`heartbeat`:** send it every `heartbeat_every` seconds. `abort: true` means stop the job and report
  `fail` with `aborted: true`.
- **`complete`/`fail`:**
  - They are idempotent per lease: repeating one returns the same `status`, and `settled` means the
    job no longer exists.
  - Retry on network errors, `429` and `5xx` for at least `lease_ttl`.
- **Errors:** they look like `{"error": {"code", "message"}}`. Branch on `code`:

  | Code | Status | What the client does |
  |---|---|---|
  | `lease_lost` | 409 | Stop the job and drop its result: another attempt owns it. |
  | `draining` | 403 | Claim nothing new, finish the jobs held, then `deregister`. |
  | `unauthorized` | 401 | The token is invalid, revoked or expired. Register again, or stop. |
  | `scope` | 403 | Queues or functions are outside the token's scope. |
  | `too_large` | 413 | Report a `complete` that is too big as `fail` with `retryable: false`. |
  | `rate_limited` | 429 | Back off. |
  | `protocol` | 400 | The `Pgwerk-Protocol` header is missing or wrong. |
