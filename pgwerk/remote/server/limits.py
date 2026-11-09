"""Request size limits and the per-worker claim rate limiter."""

from __future__ import annotations

import re
import time

from typing import Any
from collections import OrderedDict

from litestar import Request

from ..errors import TooLarge


async def read_body(request: Request, limit: int) -> bytes:
    """Read the request body, refusing it (413) past *limit* bytes.

    ``Content-Length`` is checked before anything is read, and a chunked body is cut
    off as soon as it passes the limit, so an oversized body is never buffered whole.
    """
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        raise TooLarge(f"body over {limit} bytes")
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise TooLarge(f"body over {limit} bytes")
        chunks.append(chunk)
    return b"".join(chunks)


def truncate_error(error: dict[str, Any], limit: int) -> dict[str, Any]:
    """Shrink a structured error to fit *limit* bytes of JSON; errors are truncated, not rejected."""
    import json

    error = dict(error)
    for field in ("stack", "message"):
        while len(json.dumps(error)) > limit and error.get(field):
            text = str(error[field])
            cut = max(0, len(text) - (len(json.dumps(error)) - limit) - 32)
            error[field] = text[:cut] + "…[truncated]"
            if cut == 0:
                error[field] = "…[truncated]"
                break
    return error


class TokenBucket:
    """Per-key token bucket. In-process: each server replica limits on its own.

    Keys must be authenticated identities (a worker id, an enrollment token id after
    its secret was checked): keyed on something a caller can make up, a stranger could
    drain someone else's bucket. At most *max_keys* buckets are kept; the least
    recently used is evicted in O(1).
    """

    def __init__(self, rate: float, burst: int, max_keys: int = 10_000) -> None:
        self.rate = rate
        self.burst = burst
        self.max_keys = max_keys
        self._state: OrderedDict[str, tuple[float, float]] = OrderedDict()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        tokens, last = self._state.pop(key, (float(self.burst), now))
        tokens = min(float(self.burst), tokens + (now - last) * self.rate)
        allowed = tokens >= 1.0
        self._state[key] = (tokens - 1.0 if allowed else tokens, now)
        while len(self._state) > self.max_keys:
            self._state.popitem(last=False)
        return allowed

    def __len__(self) -> int:
        return len(self._state)


#: Bounds on what a worker declares at register, and on the progress it reports.
MAX_NAME = 200
MAX_QUEUES = 100
MAX_FUNCTIONS = 1000
MAX_IDENTIFIER = 300
MAX_LABELS_BYTES = 8 * 1024
MAX_CLIENT_FIELD = 64
MAX_PROGRESS_BYTES = 8 * 1024

_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f]")


def clean_text(value: Any, limit: int) -> str:
    """A display string from an untrusted worker: no control characters, at most *limit* chars.

    Control characters (terminal escapes among them) would reach operators' terminals
    through the CLI and logs.
    """
    return _CONTROL.sub("", str(value))[:limit]
