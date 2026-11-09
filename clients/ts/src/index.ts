/**
 * A pgwerk remote worker for protocol v1 (docs/reference/remote-protocol.md).
 *
 * It needs only outbound HTTPS to a pgwerk server: register with an enrollment token,
 * long-poll claims, renew leases, report results. Retries, dependencies, callbacks and
 * scheduling stay on the server. No runtime dependencies (global `fetch`, Node >= 20).
 */

export const PROTOCOL = "1";
export const CLIENT_VERSION = "pgwerk-ts/0.1.0";

export interface Job {
  id: string;
  function: string;
  queue: string;
  kwargs: Record<string, unknown>;
  args: unknown[];
  attempts: number;
  maxAttempts: number;
  lostCount: number;
  timeoutSecs: number | null;
  meta: unknown;
  leaseId: string;
  leaseTtl: number;
  heartbeatEvery: number;
}

export interface Context {
  readonly job: Job;
  /** Aborted when the server aborts the job or the lease is lost. Pass it to I/O. */
  readonly signal: AbortSignal;
  /** Report progress (any JSON value); sent with the next lease heartbeat. */
  heartbeat(progress: unknown): void;
}

export type Handler = (ctx: Context, kwargs: Record<string, unknown>) => unknown;

/** Throw (or wrap an error in) this to fail the job for good, even with retries left. */
export class NonRetryable extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "NonRetryable";
  }
}

/** The server answered with a protocol error ({"error": {"code", "message"}}). */
export class ProtocolError extends Error {
  readonly status: number;
  readonly code: string;
  constructor(status: number, code: string, message: string) {
    super(`${status} ${code}: ${message}`);
    this.name = "ProtocolError";
    this.status = status;
    this.code = code;
  }
  /** Worth retrying the same request: rate limited or a server error. */
  get retryable(): boolean {
    return this.status === 429 || this.status >= 500;
  }
}

export interface RemoteWorkerOptions {
  /** The pgwerk server, e.g. https://jobs.example.com */
  url: string;
  /** An enrollment token (pgwk_e_…) from `werk token create`. */
  token: string;
  queues: string[];
  /** Jobs run at once; each slot runs its own claim loop. Default 1. */
  concurrency?: number;
  name?: string;
  labels?: Record<string, unknown>;
  /** Allow a plain http:// URL (local testing only). */
  insecure?: boolean;
  /** Override fetch (tests). */
  fetch?: typeof fetch;
  /** Log sink; defaults to console.warn/error. */
  log?: (level: "info" | "warn" | "error", message: string) => void;
}

interface Registered {
  worker_id: string;
  worker_token: string;
  lease_ttl?: number;
  claim_wait?: number;
}

type Body = Record<string, unknown>;

const sleep = (ms: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener("abort", () => (clearTimeout(timer), resolve()), { once: true });
  });

class TransportError extends Error {}

export class RemoteWorker {
  readonly queues: string[];
  readonly concurrency: number;
  workerId: string | null = null;
  revoked = false;

  private readonly base: string;
  private readonly enrollment: string;
  private readonly name: string;
  private readonly labels: Record<string, unknown>;
  private readonly fetchImpl: typeof fetch;
  private readonly log: (level: "info" | "warn" | "error", message: string) => void;
  private readonly handlers = new Map<string, Handler>();
  private readonly stopping = new AbortController();
  private readonly claims = new Set<AbortController>();
  private workerToken = "";
  private leaseTtl = 60;
  private claimWait = 30;
  private draining = false;
  private expired = false;

  constructor(options: RemoteWorkerOptions) {
    const scheme = /^([a-z][a-z0-9+.-]*):\/\//i.exec(options.url)?.[1]?.toLowerCase();
    if (scheme !== "https" && scheme !== "http") {
      throw new Error(`the server URL must be https:// (got ${JSON.stringify(options.url)})`);
    }
    if (scheme === "http" && !options.insecure) {
      throw new Error("refusing a plain http:// URL; use https://, or insecure: true for local testing");
    }
    this.base = options.url.replace(/\/+$/, "") + "/remote/v1";
    this.enrollment = options.token;
    this.queues = options.queues;
    this.concurrency = options.concurrency ?? 1;
    this.name = options.name ?? `node.${globalProcess()?.pid ?? "worker"}`;
    this.labels = options.labels ?? {};
    this.fetchImpl = options.fetch ?? fetch;
    this.log = options.log ?? ((level, message) => (level === "error" ? console.error : console.warn)(message));
  }

  /** Register the handler for jobs enqueued as `name`. */
  handle(name: string, handler: Handler): this {
    this.handlers.set(name, handler);
    return this;
  }

  /**
   * Register, then process jobs until stop(), SIGTERM/SIGINT, or revocation. If the server
   * expires this worker's session (it heard nothing from it for too long, e.g. during an
   * outage), the worker registers again and carries on.
   */
  async run(): Promise<void> {
    if (this.handlers.size === 0) throw new Error("register at least one handler with handle()");
    const proc = globalProcess();
    const onSignal = () => this.stop();
    proc?.on?.("SIGTERM", onSignal);
    proc?.on?.("SIGINT", onSignal);
    try {
      if (!(await this.register())) return;
      for (;;) {
        this.expired = false;
        await Promise.all(Array.from({ length: this.concurrency }, () => this.slot()));
        if (!this.expired || this.stopping.signal.aborted || this.revoked) break;
        this.log("warn", "the server expired this session (it heard nothing from us for too long); registering again");
        this.draining = false;
        if (!(await this.register())) return;
      }
    } finally {
      proc?.off?.("SIGTERM", onSignal);
      proc?.off?.("SIGINT", onSignal);
      if (this.workerId !== null && !this.revoked && !this.expired) {
        try {
          await this.call("/deregister", this.workerToken);
        } catch (err) {
          this.log("warn", `deregister failed: ${String(err)}`);
        }
      }
    }
  }

  /** Stop claiming; running jobs finish, then the worker deregisters. */
  stop(): void {
    this.stopping.abort();
    for (const claim of this.claims) claim.abort();
  }

  // --- protocol calls ------------------------------------------------------

  private async call(path: string, token: string, body: Body = {}, timeoutMs = 30_000, signal?: AbortSignal) {
    const signals = [AbortSignal.timeout(timeoutMs), ...(signal ? [signal] : [])];
    let resp: Response;
    try {
      resp = await this.fetchImpl(this.base + path, {
        method: "POST",
        headers: { "Content-Type": "application/json", "Pgwerk-Protocol": PROTOCOL, Authorization: `Bearer ${token}` },
        body: JSON.stringify(body),
        signal: AbortSignal.any(signals),
        // Never follow a redirect: the bearer token would go along to wherever it points.
        redirect: "manual",
      });
    } catch (err) {
      if (signal?.aborted) throw err;
      throw new TransportError(String(err));
    }
    if (resp.status >= 400) {
      let code = "http_error";
      let message = resp.statusText;
      try {
        const parsed = (await resp.json()) as { error?: { code?: string; message?: string } };
        code = parsed.error?.code ?? code;
        message = parsed.error?.message ?? message;
      } catch {
        // not a protocol error body
      }
      throw new ProtocolError(resp.status, code, message);
    }
    if (resp.status === 204) return { status: 204, body: null as Body | null };
    const text = await resp.text();
    return { status: resp.status, body: text ? (JSON.parse(text) as Body) : null };
  }

  /** End an expired session: running attempts lost their leases; register again. */
  private expire(): void {
    this.expired = true;
    for (const claim of this.claims) claim.abort();
  }

  /** Register, retrying while the server is unreachable. False if stopped meanwhile. */
  private async register(): Promise<boolean> {
    const body = {
      name: this.name,
      queues: this.queues,
      functions: [...this.handlers.keys()].map((name) => ({ name })),
      labels: this.labels,
      client: { lang: "typescript", version: CLIENT_VERSION },
    };
    for (let delay = 1000; ; delay = Math.min(delay * 2, 30_000)) {
      try {
        const { body: reg } = await this.call("/register", this.enrollment, body);
        const r = reg as unknown as Registered;
        this.workerId = r.worker_id;
        this.workerToken = r.worker_token;
        this.leaseTtl = r.lease_ttl ?? 60;
        this.claimWait = r.claim_wait ?? 30;
        return true;
      } catch (err) {
        if (err instanceof ProtocolError && !err.retryable) throw err;
        this.log("warn", `register failed (${String(err)}), retrying`);
        await sleep(delay, this.stopping.signal);
        if (this.stopping.signal.aborted) return false;
      }
    }
  }

  // --- claim loop -----------------------------------------------------------

  private async slot(): Promise<void> {
    let claimId = crypto.randomUUID();
    let backoff = 1000;
    while (!this.stopping.signal.aborted && !this.draining && !this.revoked && !this.expired) {
      const controller = new AbortController();
      this.claims.add(controller);
      let result: { status: number; body: Body | null };
      try {
        result = await this.call(
          "/claim",
          this.workerToken,
          { claim_id: claimId, wait: this.claimWait },
          (this.claimWait + 10) * 1000,
          controller.signal,
        );
      } catch (err) {
        if (controller.signal.aborted) return; // stop() cancelled the long-poll
        if (err instanceof ProtocolError && err.code === "draining") {
          this.log("info", "worker is draining: finishing held jobs");
          this.draining = true;
          return;
        }
        if (err instanceof ProtocolError && err.code === "session_expired") {
          this.expire();
          return;
        }
        if (err instanceof ProtocolError && err.status === 401) {
          this.revoke();
          return;
        }
        // Same claim_id on retry: if the server did lease a job, we get it back (§5.2).
        this.log("warn", `claim failed (${String(err)}), retrying`);
        await sleep(backoff, this.stopping.signal);
        backoff = Math.min(backoff * 2, 30_000);
        continue;
      } finally {
        this.claims.delete(controller);
      }
      backoff = 1000;
      claimId = crypto.randomUUID();
      if (result.status === 200 && result.body) await this.runJob(toJob(result.body));
    }
  }

  private revoke(): void {
    if (!this.revoked) this.log("error", "worker token no longer valid (revoked or retired); stopping");
    this.revoked = true;
    this.stop();
  }

  // --- one job ----------------------------------------------------------------

  private async runJob(job: Job): Promise<void> {
    if (job.args.length > 0) {
      // kwargs is the cross-language contract; positional args are Python-only (§7).
      return this.report(job, "/fail", { error: { type: "UnsupportedPayload", message: "positional args" }, retryable: false });
    }
    const handler = this.handlers.get(job.function);
    if (!handler) {
      return this.report(job, "/fail", { error: { type: "UnknownFunction", message: job.function }, retryable: false });
    }

    const jobAbort = new AbortController();
    let progress: unknown = null;
    // An object, not a let: it changes inside the heartbeat callback.
    const state: { v: "running" | "aborted" | "lost" } = { v: "running" };
    const ctx: Context = {
      job,
      signal: jobAbort.signal,
      heartbeat: (p) => {
        progress = p;
      },
    };

    const stopped = new Promise<"stopped">((resolve) =>
      jobAbort.signal.addEventListener("abort", () => resolve("stopped"), { once: true }),
    );
    const beat = setInterval(async () => {
      try {
        const { body } = await this.call(`/leases/${job.leaseId}/heartbeat`, this.workerToken, { progress });
        if (body?.abort && state.v === "running") {
          state.v = "aborted";
          jobAbort.abort("aborted");
        }
      } catch (err) {
        if (err instanceof ProtocolError && (err.code === "lease_lost" || err.status === 401)) {
          state.v = "lost";
          jobAbort.abort("lease_lost");
          if (err.code === "session_expired") this.expire();
          else if (err.status === 401) this.revoke();
        } else {
          this.log("warn", `heartbeat failed for ${job.id}: ${String(err)}`);
        }
      }
    }, job.heartbeatEvery * 1000);

    let timeout: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<"timeout">((resolve) => {
      if (job.timeoutSecs) timeout = setTimeout(() => resolve("timeout"), job.timeoutSecs * 1000);
    });

    // Stop renewing before reporting: once the report lands the lease is gone, and a late
    // heartbeat would only get lease_lost.
    const finish = (path: "/complete" | "/fail", body: Body): Promise<void> => {
      clearInterval(beat);
      if (timeout) clearTimeout(timeout);
      return this.report(job, path, body);
    };

    try {
      const work = Promise.resolve().then(() => handler(ctx, job.kwargs));
      work.catch(() => undefined); // settled below; avoid an unhandled rejection after abort
      const outcome = await Promise.race([work.then((value) => ({ value })), stopped, timedOut]);
      if (state.v === "lost") {
        this.log("warn", `job ${job.id} lost its lease; result dropped`);
        return;
      }
      if (state.v === "aborted") return finish("/fail", { aborted: true });
      if (outcome === "timeout") {
        jobAbort.abort("timeout");
        return finish("/fail", { error: { type: "Timeout", message: `exceeded ${job.timeoutSecs}s` } });
      }
      if (outcome === "stopped") return;
      let serialized: string;
      try {
        serialized = JSON.stringify((outcome as { value: unknown }).value ?? null);
      } catch (err) {
        return finish("/fail", { error: errorBody(err, "UnserializableResult"), retryable: false });
      }
      return finish("/complete", { result: JSON.parse(serialized) });
    } catch (err) {
      if (state.v === "lost") return;
      if (state.v === "aborted") return finish("/fail", { aborted: true });
      const retryable = !(err instanceof NonRetryable);
      const reported = err instanceof NonRetryable && err.cause !== undefined ? err.cause : err;
      return finish("/fail", { error: errorBody(reported), retryable });
    } finally {
      clearInterval(beat);
      if (timeout) clearTimeout(timeout);
    }
  }

  /** complete/fail, retried for at least a lease TTL: the job's effects already happened (§6). */
  private async report(job: Job, path: "/complete" | "/fail", body: Body): Promise<void> {
    const deadline = Date.now() + Math.max(job.leaseTtl, 30) * 1000;
    for (let delay = 500; ; delay = Math.min(delay * 2, 10_000)) {
      try {
        await this.call(`/leases/${job.leaseId}${path}`, this.workerToken, body);
        return;
      } catch (err) {
        if (err instanceof ProtocolError) {
          if (err.code === "too_large" && path === "/complete") {
            path = "/fail";
            body = { error: { type: "ResultTooLarge", message: err.message }, retryable: false };
            continue;
          }
          if (err.code === "lease_lost") {
            this.log("warn", `job ${job.id} lost its lease; result dropped`);
            return;
          }
          if (err.code === "session_expired") {
            this.log("warn", `session expired; result of job ${job.id} dropped`);
            return this.expire();
          }
          if (err.status === 401) return this.revoke();
          if (!err.retryable) {
            this.log("error", `reporting job ${job.id} failed: ${err.message}`);
            return;
          }
        }
        if (Date.now() > deadline) {
          this.log("error", `giving up reporting job ${job.id}`);
          return;
        }
        await sleep(delay);
      }
    }
  }
}

function toJob(body: Body): Job {
  const job = body.job as Record<string, unknown>;
  return {
    id: String(job.id),
    function: String(job.function),
    queue: String(job.queue),
    kwargs: (job.kwargs as Record<string, unknown>) ?? {},
    args: (job.args as unknown[]) ?? [],
    attempts: Number(job.attempts ?? 1),
    maxAttempts: Number(job.max_attempts ?? 1),
    lostCount: Number(job.lost_count ?? 0),
    timeoutSecs: (job.timeout_secs as number | null) ?? null,
    meta: job.meta ?? null,
    leaseId: String(body.lease_id),
    leaseTtl: Number(body.lease_ttl ?? 60),
    heartbeatEvery: Number(body.heartbeat_every ?? 15),
  };
}

function errorBody(err: unknown, type?: string): Body {
  if (err instanceof Error) return { type: type ?? err.name, message: err.message, stack: err.stack ?? null };
  return { type: type ?? "Error", message: String(err) };
}

interface ProcessLike {
  pid?: number;
  on?(event: string, listener: () => void): void;
  off?(event: string, listener: () => void): void;
}

function globalProcess(): ProcessLike | undefined {
  return (globalThis as { process?: ProcessLike }).process;
}
