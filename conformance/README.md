# Remote worker protocol v1: conformance suites

These suites certify implementations of the protocol in
[`docs/reference/remote-protocol.md`](../docs/reference/remote-protocol.md). Each test names the
section of the spec it checks.

## Server conformance (`conformance/server`)

HTTP-only tests (no pgwerk imports) run against a live server. They use the remote protocol, plus
the admin API to create, abort and delete test jobs. Each test uses its own function name, so
tests don't interfere with each other or with other jobs on the server.

```bash
werk migrate --dsn "$DSN"
werk api --dsn "$DSN" --remote-workers --api-token "$ADMIN" --no-ui --port 8000 &
export PGWERK_CONFORMANCE_URL=http://127.0.0.1:8000
export PGWERK_CONFORMANCE_ADMIN_TOKEN="$ADMIN"
export PGWERK_CONFORMANCE_TOKEN=$(werk token create --dsn "$DSN" --name conformance --queues conformance | grep '^pgwk_e_')
uv run pytest conformance/server -v
```

Without those variables the suite is skipped, so a plain `pytest` run doesn't need a server.
CI runs it on every pull request against `werk api --remote-workers`.

## Client conformance (`conformance/client`)

A scripted fake server (`fake_server.py`) that the client under test connects to. It records
every request and checks how the client behaves:
- headers and tokens: enrollment token only for `register`;
- declared functions;
- fresh vs reused `claim_id`;
- heartbeat cadence;
- `complete` retries;
- reactions to `lease_lost`, `abort`, `too_large`, `draining`, `401 unauthorized` (stop) and
  `401 session_expired` (register again, drop the held attempt);
- structured errors;
- the `args` rule for non-Python clients.

The client runs as a subprocess with `URL` and `TOKEN` in its environment. It must consume the
queue `conformance` and implement four functions:

| Function | Behaviour |
|---|---|
| `conformance.echo` | returns its `kwargs` |
| `conformance.sleep(seconds)` | sleeps, returns `null`; should stop when aborted |
| `conformance.fail(message)` | fails with `message` (a retryable error) |
| `conformance.big(size)` | returns a string of `size` characters |

```bash
# the Python RemoteWorker (default)
uv run pytest conformance/client -v

# any other client: a command that starts it
PGWERK_CONFORMANCE_CLIENT="./my-go-worker" uv run pytest conformance/client -v
```
