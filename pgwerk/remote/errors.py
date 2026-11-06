"""Protocol errors. Each maps to one HTTP status and a stable ``code`` that clients branch on.

:class:`ErrorCode` is the frozen list of v1 codes (see ``docs/reference/remote-protocol.md``).
Adding a code is an additive change; renaming or removing one needs a new protocol version.
"""

from __future__ import annotations

import enum


class ErrorCode(str, enum.Enum):
    BAD_REQUEST = "bad_request"
    PROTOCOL = "protocol"
    UNAUTHORIZED = "unauthorized"
    SESSION_EXPIRED = "session_expired"
    SCOPE = "scope"
    DRAINING = "draining"
    NOT_FOUND = "not_found"
    LEASE_LOST = "lease_lost"
    TOO_LARGE = "too_large"
    RATE_LIMITED = "rate_limited"
    INTERNAL = "internal"


class RemoteError(Exception):
    """An error the remote protocol reports as ``{"error": {"code", "message"}}``."""

    status_code = 400
    code = ErrorCode.BAD_REQUEST

    def __init__(self, message: str = "") -> None:
        super().__init__(message or self.code.value)
        self.message = message or self.code.value


class Unauthorized(RemoteError):
    """Invalid, revoked or retired credentials. Stop using the token."""

    status_code, code = 401, ErrorCode.UNAUTHORIZED


class SessionExpired(Unauthorized):
    """The server stopped this worker after hearing nothing from it for too long (for
    example, while either side was unreachable). Not a revocation: register again with
    the enrollment token and carry on. Jobs held under the old session are lost."""

    code = ErrorCode.SESSION_EXPIRED


class Scope(RemoteError):
    """The request goes beyond what the token allows (e.g. queues outside its scope)."""

    status_code, code = 403, ErrorCode.SCOPE


class Draining(RemoteError):
    """The worker is being retired: finish what you hold, claim nothing new."""

    status_code, code = 403, ErrorCode.DRAINING


class LeaseLost(RemoteError):
    """This attempt no longer owns the job. Stop it and drop its result."""

    status_code, code = 409, ErrorCode.LEASE_LOST


class TooLarge(RemoteError):
    status_code, code = 413, ErrorCode.TOO_LARGE


class RateLimited(RemoteError):
    status_code, code = 429, ErrorCode.RATE_LIMITED


class ProtocolMismatch(RemoteError):
    status_code, code = 400, ErrorCode.PROTOCOL
