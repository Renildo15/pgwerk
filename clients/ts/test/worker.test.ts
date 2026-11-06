import { test } from "node:test";
import assert from "node:assert/strict";

import { NonRetryable, ProtocolError, RemoteWorker } from "../src/index.ts";

type Call = { path: string; body: Record<string, unknown>; headers: Record<string, string> };

function fakeServer(respond: (path: string, n: number) => Response) {
  const calls: Call[] = [];
  const fetchImpl = (async (url: string, init: RequestInit) => {
    const path = new URL(url).pathname.replace("/remote/v1", "");
    calls.push({ path, body: JSON.parse(String(init.body)), headers: init.headers as Record<string, string> });
    return respond(path, calls.filter((c) => c.path === path).length);
  }) as unknown as typeof fetch;
  return { calls, fetchImpl };
}

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const error = (status: number, code: string) => json({ error: { code, message: code } }, status);

function worker(fetchImpl: typeof fetch) {
  const w = new RemoteWorker({ url: "https://x", token: "pgwk_e_t", queues: ["q"], fetch: fetchImpl, log: () => {} });
  (w as any).workerToken = "pgwk_w_t";
  return w;
}

const job = { id: "j", leaseId: "L", leaseTtl: 1, heartbeatEvery: 1 } as any;

test("refuses plain http unless insecure", () => {
  assert.throws(() => new RemoteWorker({ url: "http://x", token: "t", queues: ["q"] }));
  new RemoteWorker({ url: "http://x", token: "t", queues: ["q"], insecure: true });
});

test("sends the protocol header and the worker token", async () => {
  const { calls, fetchImpl } = fakeServer(() => json({ status: "complete" }));
  await (worker(fetchImpl) as any).report(job, "/complete", { result: 1 });
  assert.equal(calls[0].headers["Pgwerk-Protocol"], "1");
  assert.equal(calls[0].headers["Authorization"], "Bearer pgwk_w_t");
  assert.equal(calls[0].path, "/leases/L/complete");
});

test("retries a report on 503 until it is accepted", async () => {
  const { calls, fetchImpl } = fakeServer((_p, n) => (n < 3 ? error(503, "internal") : json({ status: "complete" })));
  await (worker(fetchImpl) as any).report(job, "/complete", { result: 1 });
  assert.equal(calls.length, 3);
});

test("stops reporting on lease_lost", async () => {
  const { calls, fetchImpl } = fakeServer(() => error(409, "lease_lost"));
  await (worker(fetchImpl) as any).report(job, "/complete", { result: 1 });
  assert.equal(calls.length, 1);
});

test("a too-large result becomes a non-retryable failure", async () => {
  const { calls, fetchImpl } = fakeServer((p) => (p.endsWith("complete") ? error(413, "too_large") : json({})));
  await (worker(fetchImpl) as any).report(job, "/complete", { result: "x" });
  assert.equal(calls[1].path, "/leases/L/fail");
  assert.equal(calls[1].body.retryable, false);
  assert.equal((calls[1].body.error as any).type, "ResultTooLarge");
});

test("a revoked token stops the worker", async () => {
  const { fetchImpl } = fakeServer(() => error(401, "unauthorized"));
  const w = worker(fetchImpl);
  await (w as any).report(job, "/complete", { result: 1 });
  assert.equal(w.revoked, true);
});

test("ProtocolError.retryable is true only for 429 and 5xx", () => {
  assert.equal(new ProtocolError(429, "rate_limited", "").retryable, true);
  assert.equal(new ProtocolError(503, "internal", "").retryable, true);
  assert.equal(new ProtocolError(400, "bad_request", "").retryable, false);
});

test("NonRetryable fails a job without retrying", async () => {
  const { calls, fetchImpl } = fakeServer(() => json({ status: "failed" }));
  const w = worker(fetchImpl).handle("f", () => {
    throw new NonRetryable("nope");
  });
  await (w as any).runJob({ ...job, function: "f", args: [], kwargs: {}, timeoutSecs: null });
  assert.equal(calls.at(-1)!.path, "/leases/L/fail");
  assert.equal(calls.at(-1)!.body.retryable, false);
});
