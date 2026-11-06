"""Request size limits and the per-worker claim rate limiter."""

from __future__ import annotations

import time

from typing import Any

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
    """Per-key token bucket. In-process: each server replica limits on its own."""

    def __init__(self, rate: float, burst: int) -> None:
        self.rate = rate
        self.burst = burst
        self._state: dict[str, tuple[float, float]] = {}

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        tokens, last = self._state.get(key, (float(self.burst), now))
        tokens = min(float(self.burst), tokens + (now - last) * self.rate)
        if tokens < 1.0:
            self._state[key] = (tokens, now)
            return False
        self._state[key] = (tokens - 1.0, now)
        if len(self._state) > 10_000:  # bound memory: forget idle keys
            self._state = {k: v for k, v in self._state.items() if now - v[1] < 300}
        return True
