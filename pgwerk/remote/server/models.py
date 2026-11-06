"""Request and response bodies of protocol v1.

Unknown request fields are ignored, and clients must ignore unknown response fields:
within v1, changes are additive only.
"""

from __future__ import annotations

from typing import Any

import msgspec


class FunctionDecl(msgspec.Struct):
    name: str


class ClientInfo(msgspec.Struct):
    lang: str = "unknown"
    version: str = ""


class RegisterRequest(msgspec.Struct):
    queues: list[str]
    functions: list[FunctionDecl]
    name: str = "remote"
    labels: dict[str, Any] = msgspec.field(default_factory=dict)
    client: ClientInfo = msgspec.field(default_factory=ClientInfo)


class Limits(msgspec.Struct):
    max_payload_bytes: int
    max_result_bytes: int
    max_error_bytes: int


class RegisterResponse(msgspec.Struct):
    worker_id: str
    worker_token: str
    protocol: int
    lease_ttl: int
    claim_wait: int
    limits: Limits


class ClaimRequest(msgspec.Struct):
    claim_id: str
    wait: float | None = None


class JobBody(msgspec.Struct):
    id: str
    function: str
    queue: str
    kwargs: dict[str, Any]
    args: list[Any]
    attempts: int
    max_attempts: int
    lost_count: int
    timeout_secs: int | None
    meta: Any


class ClaimResponse(msgspec.Struct):
    job: JobBody
    lease_id: str
    lease_expires_at: str | None
    lease_ttl: int
    heartbeat_every: int


class HeartbeatRequest(msgspec.Struct):
    progress: Any = None


class HeartbeatResponse(msgspec.Struct):
    lease_expires_at: str
    abort: bool


class CompleteRequest(msgspec.Struct):
    result: Any = None


class ErrorDetail(msgspec.Struct):
    type: str
    message: str = ""
    stack: str | None = None


class FailRequest(msgspec.Struct):
    error: ErrorDetail | None = None
    retryable: bool = True
    aborted: bool = False


class ReportResponse(msgspec.Struct, omit_defaults=True):
    status: str
    retry_at: str | None = None


class ErrorBody(msgspec.Struct):
    code: str
    message: str


class ErrorResponse(msgspec.Struct):
    error: ErrorBody
