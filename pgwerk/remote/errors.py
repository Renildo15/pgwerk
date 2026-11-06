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
    status_code, code = 401, "unauthorized"


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
