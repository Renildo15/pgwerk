"""Configuration for wrk workers, schedulers, and the API server."""

from __future__ import annotations

import os

from typing import ClassVar
from dataclasses import dataclass


@dataclass
class WerkConfig:
    """Unified configuration for wrk.

    Attributes:
        schema_version: Internal schema version; bump on each migration. Not
            user-configurable — treated as a read-only class constant.
        dsn: Postgres connection string.
        schema: PostgreSQL schema that qualifies all wrk table names (e.g.
            ``"public"``). ``None`` means no schema prefix.
        prefix: Prefix applied to every wrk table name (default ``"_pgwerk"``),
            producing tables like ``_pgwerk_jobs``, ``_pgwerk_worker``, etc.
        max_active_secs: Deprecated. How long (seconds) a job claimed by a worker
            older than schema 8 (no lease) may stay ``active`` before the sweep
            recovers it. Leased jobs are recovered when their lease expires.
        heartbeat_interval: How often (seconds) a running worker updates
            its heartbeat timestamp in ``_pgwerk_worker``.
        poll_interval: How often (seconds) the polling loop checks
            ``_pgwerk_jobs`` for new work when no LISTEN/NOTIFY wake-up
            arrives.
        abort_interval: How often (seconds) the worker checks for a
            cancellation signal on the currently running job.
        sweep_interval: How often (seconds) the maintenance sweep runs to
            requeue stalled jobs, clean up dead workers, and resolve
            dependency chains.
        shutdown_timeout: Seconds to wait for in-flight jobs to finish
            during graceful shutdown before forcibly terminating.
        sigterm_grace: Seconds ``ForkWorker`` waits between sending
            SIGTERM and SIGKILL to a timed-out subprocess.
        ephemeral_tables: Use ``UNLOGGED`` tables for ``_pgwerk_worker`` and
            ``_pgwerk_worker_jobs``. Faster writes; data is lost on a crash,
            which is safe because workers re-register on startup and the
            sweep re-establishes claims.
        listen: When ``True`` (default), workers open a persistent
            ``LISTEN`` connection for instant ``NOTIFY``-driven wake-up.
            Set to ``False`` to fall back to pure polling — required when
            routing connections through PgBouncer in transaction-pooling
            mode, which does not support ``LISTEN``/``NOTIFY``.
        metrics: Enable Prometheus metrics at ``GET /metrics``.
        metrics_interval: Metrics scrape interval in seconds.
        ui: Serve the SPA dashboard (requires built static files).
        ui_auth: Basic Auth for the SPA as ``user:password``.
        api_token: Bearer token for all ``/api/*`` routes.
        pool_size: Hold at most this many database connections and reuse them.
            ``None`` opens one per operation (workers, producers); ``werk api``
            uses 20 when unset, so it never opens a backend per request.
        maintenance: Run the maintenance pass (sweep, stranded ``waiting`` job
            reconciliation, rescheduling) inside the API process, every
            ``sweep_interval``. Lets stuck jobs recover with no worker running.
            ``None`` (default): on when ``remote_workers`` is on, off otherwise.
        purge_expired: Let maintenance delete finished jobs whose ``_result_ttl``
            or ``_failure_ttl`` ran out (and stopped worker rows past their
            expiry). Set ``False`` to keep every row.
        remote_workers: Serve the remote-worker protocol at ``/remote/v1`` from the API
            process. Requires ``api_token`` (or ``api=False``) and the
            JSON serializer, and turns maintenance on unless it's explicitly off.
        api: Serve the admin REST API at ``/api``. Turn it off to run a process that
            only serves remote workers.
        remote_claim_wait: Seconds a remote ``claim`` long-poll is held open.
        remote_claim_rate: Claims per second allowed per remote worker (token bucket,
            per server replica), with bursts up to ``remote_claim_burst``.
        remote_claim_burst: Bucket size for ``remote_claim_rate``.
        remote_max_claims_per_worker: Claim long-polls one remote worker may have open
            at once (a worker opens one per concurrency slot).
        remote_register_rate: Registrations per second allowed per enrollment token,
            with bursts up to ``remote_register_burst``. Sized so a fleet sharing one
            token comes back quickly after an outage (200 workers in about 10s).
        remote_register_burst: Bucket size for ``remote_register_rate``.
        remote_max_workers_per_token: Live (active or draining) remote workers one
            enrollment token may have.
        remote_max_payload_bytes: Largest request body accepted (256 KiB).
        remote_max_result_bytes: Largest ``complete`` body accepted (1 MiB).
        remote_max_error_bytes: Longest error stored from a remote ``fail``; longer
            errors are truncated, not rejected (64 KiB).
        local_lease_ttl: Seconds a local worker's claim on a job stays valid without
            renewal, for jobs without ``heartbeat_secs`` (which is used instead). If
            the worker stops renewing, the sweep recovers the job after this long.
        remote_lease_ttl: The same, for remote workers (used from schema 10).
        default_max_lost: How many times a job may be requeued because its worker
            vanished (its lease expired) before it fails with ``lease_lost``. Set per
            job with ``_max_lost``. Lost workers don't use up retries.
        default_retry_backoff: When ``True`` (default), passing a plain integer
            to ``_retry`` synthesizes an exponential backoff schedule rather
            than retrying immediately. An explicit :class:`Retry` always wins.
    """

    schema_version: ClassVar[int] = 10
    # This build reads and writes the columns its own schema adds, so it refuses an
    # older database instead of failing on its first query. Workers older than schema 8
    # are unaffected: what they accept is set by their own max (unbounded up to 0.1.26).
    min_compatible_db_version: ClassVar[int] = 10
    # From schema 8 on, every release declares the newest schema it tolerates, so a
    # later breaking migration can stop older code from running against it. It covers
    # the additive migrations already planned (9: function routing, 10: remote
    # workers); a breaking migration bumps past it, and its release raises this bound.
    max_compatible_db_version: ClassVar[int | None] = 10

    # Connection
    dsn: str | None = None

    # Schema / table naming
    schema: str = "pgwerk"
    prefix: str = "_pgwerk"

    # Job lifecycle
    max_active_secs: int = 3600

    # Worker polling / maintenance intervals (seconds)
    heartbeat_interval: int = 10
    poll_interval: float = 5.0
    abort_interval: float = 1.0
    sweep_interval: float = 60.0
    shutdown_timeout: float = 30.0

    # ForkWorker
    sigterm_grace: int = 5

    # Table storage
    ephemeral_tables: bool = False

    # API server
    metrics: bool = False
    metrics_interval: float = 15.0
    ui: bool = True
    ui_auth: str | None = None
    api_token: str | None = None
    maintenance: bool | None = None

    # LISTEN/NOTIFY
    listen: bool = True

    # Retry behavior
    default_retry_backoff: bool = True

    # Maintenance
    purge_expired: bool = True

    # Remote workers
    remote_workers: bool = False
    api: bool = True
    remote_claim_wait: int = 30
    remote_claim_rate: float = 5.0
    remote_claim_burst: int = 10
    remote_max_claims_per_worker: int = 32
    remote_register_rate: float = 10.0
    remote_register_burst: int = 100
    remote_max_workers_per_token: int = 1000
    remote_max_payload_bytes: int = 256 * 1024
    remote_max_result_bytes: int = 1024 * 1024
    remote_max_error_bytes: int = 64 * 1024

    # Leases
    local_lease_ttl: int = 300
    remote_lease_ttl: int = 60
    default_max_lost: int = 3

    # Dangerous operations
    allow_truncate: bool = False

    # Connections: hold at most this many and reuse them (None: one per operation).
    # `werk api` defaults to 20 when unset.
    pool_size: int | None = None

    @property
    def maintenance_enabled(self) -> bool:
        """Whether the API process runs maintenance (``maintenance``, or ``remote_workers`` if unset)."""
        return self.maintenance if self.maintenance is not None else self.remote_workers

    @classmethod
    def from_env(cls) -> "WerkConfig":

        def _bool(key: str, default: bool) -> bool:
            v = os.environ.get(key, "").lower()
            return default if not v else v in ("1", "true", "yes")

        def _float(key: str, default: float) -> float:
            return float(os.environ.get(key, default))

        def _int(key: str, default: int) -> int:
            return int(os.environ.get(key, default))

        return cls(
            dsn=os.environ.get("PGWERK_DSN"),
            schema=os.environ.get("PGWERK_SCHEMA", "pgwerk"),
            prefix=os.environ.get("PGWERK_PREFIX", "_pgwerk"),
            max_active_secs=_int("PGWERK_MAX_ACTIVE_SECS", 3600),
            heartbeat_interval=_int("PGWERK_HEARTBEAT_INTERVAL", 10),
            poll_interval=_float("PGWERK_POLL_INTERVAL", 5.0),
            abort_interval=_float("PGWERK_ABORT_INTERVAL", 1.0),
            sweep_interval=_float("PGWERK_SWEEP_INTERVAL", 60.0),
            shutdown_timeout=_float("PGWERK_SHUTDOWN_TIMEOUT", 30.0),
            sigterm_grace=_int("PGWERK_SIGTERM_GRACE", 5),
            ephemeral_tables=_bool("PGWERK_EPHEMERAL_TABLES", False),
            metrics=_bool("PGWERK_METRICS", False),
            metrics_interval=_float("PGWERK_METRICS_INTERVAL", 15.0),
            ui=not _bool("PGWERK_NO_UI", False),
            ui_auth=os.environ.get("PGWERK_UI_AUTH"),
            api_token=os.environ.get("PGWERK_API_TOKEN"),
            maintenance=_bool("PGWERK_MAINTENANCE", False) if os.environ.get("PGWERK_MAINTENANCE") else None,
            default_retry_backoff=_bool("PGWERK_DEFAULT_RETRY_BACKOFF", True),
            purge_expired=_bool("PGWERK_PURGE_EXPIRED", True),
            remote_workers=_bool("PGWERK_REMOTE_WORKERS", False),
            api=not _bool("PGWERK_NO_API", False),
            remote_claim_wait=_int("PGWERK_REMOTE_CLAIM_WAIT", 30),
            remote_claim_rate=_float("PGWERK_REMOTE_CLAIM_RATE", 5.0),
            remote_claim_burst=_int("PGWERK_REMOTE_CLAIM_BURST", 10),
            remote_max_claims_per_worker=_int("PGWERK_REMOTE_MAX_CLAIMS_PER_WORKER", 32),
            remote_register_rate=_float("PGWERK_REMOTE_REGISTER_RATE", 10.0),
            remote_register_burst=_int("PGWERK_REMOTE_REGISTER_BURST", 100),
            remote_max_workers_per_token=_int("PGWERK_REMOTE_MAX_WORKERS_PER_TOKEN", 1000),
            remote_max_payload_bytes=_int("PGWERK_REMOTE_MAX_PAYLOAD_BYTES", 256 * 1024),
            remote_max_result_bytes=_int("PGWERK_REMOTE_MAX_RESULT_BYTES", 1024 * 1024),
            remote_max_error_bytes=_int("PGWERK_REMOTE_MAX_ERROR_BYTES", 64 * 1024),
            local_lease_ttl=_int("PGWERK_LOCAL_LEASE_TTL", 300),
            remote_lease_ttl=_int("PGWERK_REMOTE_LEASE_TTL", 60),
            default_max_lost=_int("PGWERK_DEFAULT_MAX_LOST", 3),
            allow_truncate=_bool("PGWERK_ALLOW_TRUNCATE", False),
            pool_size=int(os.environ["PGWERK_POOL_SIZE"]) if os.environ.get("PGWERK_POOL_SIZE") else None,
            listen=_bool("PGWERK_LISTEN", True),
        )

    def to_env(self) -> None:

        os.environ["PGWERK_DSN"] = self.dsn or ""
        os.environ["PGWERK_SCHEMA"] = self.schema
        os.environ["PGWERK_PREFIX"] = self.prefix
        os.environ["PGWERK_MAX_ACTIVE_SECS"] = str(self.max_active_secs)
        os.environ["PGWERK_HEARTBEAT_INTERVAL"] = str(self.heartbeat_interval)
        os.environ["PGWERK_POLL_INTERVAL"] = str(self.poll_interval)
        os.environ["PGWERK_ABORT_INTERVAL"] = str(self.abort_interval)
        os.environ["PGWERK_SWEEP_INTERVAL"] = str(self.sweep_interval)
        os.environ["PGWERK_SHUTDOWN_TIMEOUT"] = str(self.shutdown_timeout)
        os.environ["PGWERK_SIGTERM_GRACE"] = str(self.sigterm_grace)
        os.environ["PGWERK_EPHEMERAL_TABLES"] = "true" if self.ephemeral_tables else "false"
        os.environ["PGWERK_METRICS"] = "true" if self.metrics else "false"
        os.environ["PGWERK_METRICS_INTERVAL"] = str(self.metrics_interval)
        os.environ["PGWERK_NO_UI"] = "true" if not self.ui else "false"
        if self.ui_auth:
            os.environ["PGWERK_UI_AUTH"] = self.ui_auth
        if self.api_token:
            os.environ["PGWERK_API_TOKEN"] = self.api_token
        if self.maintenance is None:
            os.environ.pop("PGWERK_MAINTENANCE", None)
        else:
            os.environ["PGWERK_MAINTENANCE"] = "true" if self.maintenance else "false"
        os.environ["PGWERK_DEFAULT_RETRY_BACKOFF"] = "true" if self.default_retry_backoff else "false"
        os.environ["PGWERK_PURGE_EXPIRED"] = "true" if self.purge_expired else "false"
        os.environ["PGWERK_REMOTE_WORKERS"] = "true" if self.remote_workers else "false"
        os.environ["PGWERK_NO_API"] = "true" if not self.api else "false"
        os.environ["PGWERK_REMOTE_CLAIM_WAIT"] = str(self.remote_claim_wait)
        os.environ["PGWERK_REMOTE_CLAIM_RATE"] = str(self.remote_claim_rate)
        os.environ["PGWERK_REMOTE_CLAIM_BURST"] = str(self.remote_claim_burst)
        os.environ["PGWERK_REMOTE_MAX_CLAIMS_PER_WORKER"] = str(self.remote_max_claims_per_worker)
        os.environ["PGWERK_REMOTE_REGISTER_RATE"] = str(self.remote_register_rate)
        os.environ["PGWERK_REMOTE_REGISTER_BURST"] = str(self.remote_register_burst)
        os.environ["PGWERK_REMOTE_MAX_WORKERS_PER_TOKEN"] = str(self.remote_max_workers_per_token)
        os.environ["PGWERK_REMOTE_MAX_PAYLOAD_BYTES"] = str(self.remote_max_payload_bytes)
        os.environ["PGWERK_REMOTE_MAX_RESULT_BYTES"] = str(self.remote_max_result_bytes)
        os.environ["PGWERK_REMOTE_MAX_ERROR_BYTES"] = str(self.remote_max_error_bytes)
        os.environ["PGWERK_LOCAL_LEASE_TTL"] = str(self.local_lease_ttl)
        os.environ["PGWERK_REMOTE_LEASE_TTL"] = str(self.remote_lease_ttl)
        os.environ["PGWERK_DEFAULT_MAX_LOST"] = str(self.default_max_lost)
        os.environ["PGWERK_ALLOW_TRUNCATE"] = "true" if self.allow_truncate else "false"
        if self.pool_size is not None:
            os.environ["PGWERK_POOL_SIZE"] = str(self.pool_size)
        os.environ["PGWERK_LISTEN"] = "true" if self.listen else "false"
