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
