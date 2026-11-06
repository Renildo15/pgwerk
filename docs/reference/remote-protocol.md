# Remote worker protocol v1

This is the normative description of the protocol between a pgwerk server and remote workers.
A worker client in any language that follows it, and passes the [conformance suite](#conformance),
can run pgwerk jobs. The [OpenAPI description](remote-openapi.json) is a machine-readable
convenience; where the two disagree, this document wins.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT** and **MAY** are to be interpreted
as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119).

## 1. Transport

- Every endpoint is an HTTP `POST` under the path prefix `/remote/v1`, and request and response
  bodies are JSON (`Content-Type: application/json`). An endpoint whose request body is optional
  accepts an empty body or `{}`.
- Clients **MUST** use HTTPS in production. A client **SHOULD** refuse a plain `http://` server URL
  unless the user explicitly opts in (for local testing).
- Every request **MUST** carry the header `Pgwerk-Protocol: 1`. A server **MUST** reject a request
  without it with `400` and the code `protocol`.
- A server **MUST** ignore request fields it doesn't know, and a client **MUST** ignore response
  fields it doesn't know (see [§4](#4-versioning)).

## 2. Authentication

There are two bearer tokens, sent as `Authorization: Bearer <token>`:

| Token | Format | Accepted by |
|---|---|---|
| Enrollment | `pgwk_e_<32 hex id>_<secret>` | `register` only |
| Worker | `pgwk_w_<32 hex worker id>_<secret>` | every other endpoint |

- Tokens are opaque to clients, and clients **MUST NOT** parse or construct them. The secret uses
  the URL-safe base64 alphabet, which includes `_`.
- The operator creates an enrollment token scoped to an exact list of queues. The client exchanges
  it once per process at `register` for a worker token, and **SHOULD NOT** send the enrollment
  token again.
- The client **MUST** keep the worker token in memory only, and **MUST** use it for every other call.
- The server checks the token on every call. A token that is invalid, revoked, of the wrong kind,
  or belongs to a stopped worker gets `401 unauthorized`.
- A client that gets `401` with a worker token **MUST** stop using that token. It **MAY** register
  again with its enrollment token (which fails with `401` if that token was revoked or expired).

## 3. Lifecycle

```
register ──► claim ──► (heartbeat)* ──► complete | fail ──► claim …
                                                         └─► deregister
```

### 3.1 Leases

A successful `claim` gives the worker one **attempt** of one job, identified by a `lease_id`.

- The lease lasts `lease_ttl` seconds. The client **MUST** send `heartbeat` every
  `heartbeat_every` seconds (from the claim response) while the job runs. Each heartbeat extends
  the lease by `lease_ttl`.
- If no heartbeat arrives before the lease expires, the server **MAY** take the job back at any
  time after that. It then counts the attempt as **lost**: the job is requeued without using one
  of its retries, up to a per-job cap, and fails with `lease_lost` beyond it.
- When the server answers `409 lease_lost` to a `heartbeat`, the attempt no longer owns the job.
  The client **MUST** stop running it (cancel, or at least never report it) and **MUST NOT** call
  `complete` or `fail` for it.
- The server fences on identity, not on the clock. A `complete` or `fail` for an expired lease that
  nobody took back yet is still accepted.

### 3.2 Abort

When a heartbeat response has `"abort": true`, the job was aborted. The client **SHOULD** stop the
handler as soon as possible, and **MUST** then report `fail` with `"aborted": true`. A `complete`
or `fail` that reaches the server for an aborted job settles it as `aborted` either way.

### 3.3 Draining and revocation

- A claim answered with `403 draining` means the operator is retiring this worker. The client
  **MUST NOT** claim again. It **SHOULD** finish and report the jobs it holds (heartbeats and
  reports are still accepted), then call `deregister`.
- A worker that gets `401` was revoked or expired; see [§2](#2-authentication).

## 4. Versioning

- The version is in the path (`/remote/v1`) and in the `Pgwerk-Protocol` header.
- Within v1, changes are **additive only**: new optional request fields, new response fields, new
  endpoints, and new error codes. Clients written against this document keep working.
- A breaking change means `/remote/v2`. A server that introduces v2 **MUST** keep serving v1 for at
  least two minor releases of pgwerk.
- The protocol version is independent of the pgwerk package version.

## 5. Endpoints

### 5.1 `POST /remote/v1/register` (enrollment token)

Request:

```json
{
  "queues": ["render"],
  "functions": [{"name": "media.tasks.render"}],
  "name": "gpu-box.4242",
  "labels": {"gpu": "4090"},
  "client": {"lang": "go", "version": "pgwerk-go/1.0.0"}
}
```

- `queues` (required): queues this worker consumes. They **MUST** all be within the enrollment
  token's queues, or the server answers `403 scope`.
- `functions` (required, non-empty): the functions this worker can run, by the name jobs are
  enqueued with. The server **MUST** only give this worker jobs for these functions.
- `name`, `labels` and `client` are optional. `client.lang` **SHOULD** be set; it is shown in the
  function catalog.

`201` response:

```json
{
  "worker_id": "4f6e…",
  "worker_token": "pgwk_w_…",
  "protocol": 1,
  "lease_ttl": 60,
  "claim_wait": 30,
  "limits": {"max_payload_bytes": 262144, "max_result_bytes": 1048576, "max_error_bytes": 65536}
}
```

### 5.2 `POST /remote/v1/claim` (worker token)

Request: `{"claim_id": "<uuid>", "wait": 30}`

- `claim_id` (required): a fresh UUID per claim. On a network error, `429` or `5xx`, the client
  **MUST** retry the same claim with the **same** `claim_id`, and only start a new `claim_id`
  after a `200` or `204`. If the server had already given it a job, the retry
  returns the same lease, so a lost response doesn't lose the job. A `claim_id` whose attempt is
  over gets `204`; the client then claims again with a new id.
- `wait` (optional, seconds): how long the server holds the request open when there is nothing
  to claim. The server caps it at `claim_wait`. Clients **MUST** use an HTTP timeout longer than
  the wait (wait + 10s is recommended).

`200` response:

```json
{
  "job": {
    "id": "…", "function": "media.tasks.render", "queue": "render",
    "kwargs": {"asset_id": "a1"}, "args": [],
    "attempts": 1, "max_attempts": 3, "lost_count": 0,
    "timeout_secs": 300, "meta": null
  },
  "lease_id": "…",
  "lease_expires_at": "2026-11-06T12:00:00+00:00",
  "lease_ttl": 60,
  "heartbeat_every": 15
}
```

`204`: nothing to claim within the wait (or a spent `claim_id`). The client claims again.

`claim` returns at most one job. A worker that runs N jobs at once runs N claim loops.

### 5.3 `POST /remote/v1/leases/{lease_id}/heartbeat` (worker token)

Request: `{"progress": <any JSON>}` (optional). The server stores the latest progress for the
attempt.

`200` response: `{"lease_expires_at": "…", "abort": false}`. `409 lease_lost`: see [§3.1](#31-leases).

### 5.4 `POST /remote/v1/leases/{lease_id}/complete` (worker token)

Request: `{"result": <any JSON>}`. `200` response: `{"status": "complete"}`.

- A result larger than `limits.max_result_bytes` gets `413 too_large`. The client **MUST** then
  report `fail` with `retryable: false` instead.

### 5.5 `POST /remote/v1/leases/{lease_id}/fail` (worker token)

Request:

```json
{"error": {"type": "ValueError", "message": "bad input", "stack": "…"}, "retryable": true}
```

or `{"aborted": true}` after an abort.

- `error.type` is required unless `aborted` is true. The server truncates errors longer than
  `limits.max_error_bytes`; it doesn't reject them.
- `retryable: false` fails the job for good, even with retries left. Retry scheduling is decided
  by the server, never by the client.

`200` response: `{"status": "queued" | "scheduled" | "failed" | "aborted", "retry_at": "…"}`.
`retry_at` is present when a retry is scheduled.

### 5.6 `POST /remote/v1/deregister` (worker token)

No body. `204`. The worker token stops working. Jobs the worker still holds go back to the queue
without counting as lost. A client **SHOULD** call it on graceful shutdown, after its jobs finish.

## 6. Idempotency of `complete` and `fail`

The client **MUST** retry `complete`/`fail` on network errors, `429` and `5xx`, with backoff, for
at least `lease_ttl` seconds: the job's side effects have already happened. The server answers by
the state of the attempt:

| State of the attempt | Answer |
|---|---|
| running, still owns the job | settles it; `200 {status}` |
| already settled by an earlier report | `200` with the same `status`; nothing runs twice |
| lost, taken back, or not this worker's | `409 lease_lost` |
| the job no longer exists (deleted) | `200 {"status": "settled"}` |

## 7. Payload contract

- `kwargs` (a JSON object) is the cross-language contract. Handlers in every language receive the
  job's named arguments.
- `args` (positional arguments) exists for Python producers. A non-Python client that claims a job
  with non-empty `args` **MUST** fail it with `retryable: false` and `error.type`
  `"UnsupportedPayload"`.
- Results and progress are any JSON value. Artifacts are not stored by pgwerk: pass presigned URLs
  in `kwargs` and return references.

## 8. Errors

Every error response has the shape `{"error": {"code": "<code>", "message": "<text>"}}`. Clients
**MUST** branch on `code`, and **MUST NOT** branch on `message` or on the status alone.

| Code | Status | Meaning | Client obligation |
|---|---|---|---|
| `bad_request` | 400 | The body is invalid | Fix the request; don't retry as is |
| `protocol` | 400 | Missing or unsupported `Pgwerk-Protocol` | — |
| `unauthorized` | 401 | Invalid, revoked or wrong kind of token, or the worker is stopped | Stop using the token ([§2](#2-authentication)) |
| `scope` | 403 | Queues outside the token's scope, or no queues/functions | Don't retry |
| `draining` | 403 | The worker is being retired | Stop claiming; finish held jobs; deregister |
| `not_found` | 404 | Unknown endpoint | — |
| `lease_lost` | 409 | This attempt no longer owns the job | Stop the job; drop its result |
| `too_large` | 413 | The body (or result) is over the limit | For a result: report `fail` with `retryable: false` |
| `rate_limited` | 429 | Too many claims | Back off, then retry |
| `internal` | 5xx | Server error | Retry with backoff |

New codes may be added within v1. A client **MUST** treat an unknown code by its HTTP status
class: retry on `5xx` and `429`, and don't retry other `4xx`.

## 9. Limits

`register` returns the limits in force:
- `max_payload_bytes` for any request body (default 256 KiB);
- `max_result_bytes` for results (default 1 MiB);
- `max_error_bytes` for stored errors (default 64 KiB).

The server **MUST** enforce body limits before parsing. Claims are rate limited per worker.

## Conformance

The repository's `conformance/` directory certifies implementations:

- `conformance/server`: HTTP tests that a server build **MUST** pass (run against any URL).
- `conformance/client`: a scripted fake server that checks a client's behavior (retries,
  `claim_id` reuse, heartbeat cadence, reactions to `lease_lost`, `abort`, `draining` and
  `too_large`, and `args` handling).

See `conformance/README.md`.
