# @pgwerk/remote-worker

A pgwerk **remote worker** for TypeScript/JavaScript. It runs pgwerk jobs from any machine with
outbound HTTPS to a pgwerk server (`werk api --remote-workers`), with no database access.

It implements [remote worker protocol v1](../../docs/reference/remote-protocol.md) and passes the
client conformance suite. It needs Node ≥ 20.3 and has no runtime dependencies (global `fetch`).

```ts
import { RemoteWorker, NonRetryable } from "@pgwerk/remote-worker";

const worker = new RemoteWorker({
  url: "https://jobs.example.com",
  token: process.env.PGWERK_TOKEN!,   // an enrollment token from `werk token create`
  queues: ["render"],
  concurrency: 4,
});

worker.handle("media.tasks.render", async (ctx, { assetId, uploadUrl }) => {
  ctx.heartbeat({ stage: "rendering" });                    // optional progress
  const res = await fetch(String(uploadUrl), { method: "PUT", body: "…", signal: ctx.signal });
  if (res.status === 400) throw new NonRetryable("bad asset");  // fail for good
  return { uri: `s3://bucket/${assetId}.png` };
});

await worker.run();   // SIGTERM/SIGINT: finishes running jobs, then deregisters
```

- A handler gets the job's `kwargs`. A job enqueued with positional `args` (a Python-only feature)
  is failed with `UnsupportedPayload`.
- `ctx.signal` aborts when the job is aborted on the server, or when this worker loses the job's
  lease (its result would be discarded). Pass it to your I/O.
- A thrown error fails the attempt and follows the job's retries; `NonRetryable` fails it for
  good. Errors are reported as `{type, message, stack}`.
- `timeout_secs` is enforced by the client.

## Development

```bash
npm test             # unit tests (node:test; Node >= 23 runs the .ts sources directly)
npm run typecheck
npm run build        # dist/
# protocol conformance, from the repo root:
PGWERK_CONFORMANCE_CLIENT="node clients/ts/conformance/worker.ts" uv run pytest conformance/client
```
