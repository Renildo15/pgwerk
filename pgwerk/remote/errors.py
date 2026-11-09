"""Protocol errors. Each maps to one HTTP status and a stable ``code`` clients branch on."""

from __future__ import annotations


class RemoteError(Exception):
    """An error the remote protocol reports as ``{"error": {"code", "message"}}``."""

    status_code = 400
    code = "bad_request"

    def __init__(self, message: str = "") -> None:
        super().__init__(message or self.code)
        self.message = message or self.code


class Unauthorized(RemoteError):
    """Invalid, revoked or retired credentials. Stop using the token."""

    status_code, code = 401, "unauthorized"


class SessionExpired(Unauthorized):
    """The server stopped this worker after hearing nothing from it for too long (for
    example, while either side was unreachable). Not a revocation: register again with
    the enrollment token and carry on. Jobs held under the old session are lost."""

    code = "session_expired"


class Scope(RemoteError):
    """The request goes beyond what the token allows (e.g. queues outside its scope)."""

    status_code, code = 403, "scope"


class Draining(RemoteError):
    """The worker is being retired: finish what you hold, claim nothing new."""

    status_code, code = 403, "draining"


class LeaseLost(RemoteError):
    """This attempt no longer owns the job. Stop it and drop its result."""

    status_code, code = 409, "lease_lost"


class TooLarge(RemoteError):
    status_code, code = 413, "too_large"


class RateLimited(RemoteError):
    status_code, code = 429, "rate_limited"


class ProtocolMismatch(RemoteError):
    status_code, code = 400, "protocol"
