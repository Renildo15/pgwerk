# Security

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's
[private vulnerability reporting](https://github.com/pgwerk/pgwerk/security/advisories/new) for this
repository, not in public issues. Include the version, a description, and steps to reproduce. We aim
to acknowledge reports within a few working days.

## Supported versions

Security fixes go into the latest release. pgwerk is beta (0.x); upgrade to the latest version to
receive fixes.

## Threat model: remote workers (experimental)

When `werk api --remote-workers` is on, the server faces the internet and talks to workers that
pgwerk does **not** trust:

- **Remote workers are untrusted.** A worker sees only jobs in the queues its token allows and the
  functions it declared. It can only renew and report the leases it holds, and it never gets
  database access, `ctx.app`, or other jobs' data.
- **JSON only.** The remote protocol refuses to run with a pickle serializer: a pickled result from a
  rented machine would be code execution on whatever decodes it. Results are any JSON value, and
  limits are enforced before bodies are parsed (256 KiB requests, 1 MiB results; errors are
  truncated to 64 KiB).
- **Two token layers.** An operator creates a long-lived **enrollment token** scoped to an exact list
  of queues (`werk token create`); only its sha256 hash is stored, and it is shown once. Each worker
  process exchanges it at `register` for its own **worker token**, which is checked against the
  database on every call (no cache), so revocation applies on the next call.
- **Revoke vs retire.** `werk workers revoke <id>` retires one worker, but the machine still holds the
  enrollment token and can register again. If a machine may be compromised, run
  `werk token revoke <id>`: every worker registered with that token stops at once, and their
  in-flight jobs go back to the queue.
- **The admin API is never open next to the remote router.** Remote mode refuses to start unless
  `--api-token` is set, or `--no-api` serves only the remote router. Admin tokens are rejected on
  `/remote`, and remote tokens on `/api`.
- **Claims are rate limited** per worker (token bucket, per server replica).
- **Use TLS.** Terminate HTTPS in front of the server. The Python client refuses `http://` URLs
  unless `insecure=True`.

What a malicious remote worker *can* do: return wrong results for the jobs it claims, and report
failures for them. Validate results that matter (for example, check the `sha256` of an artifact) before
acting on them.
