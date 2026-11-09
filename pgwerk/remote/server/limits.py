"""Request size limits and the per-worker claim rate limiter."""

from __future__ import annotations

import re
import json
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


MAX_ERROR_TYPE = 256
_TRUNCATED = "…[truncated]"
# Control characters, except tab and newlines (stack traces keep their layout).
_ERROR_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]")


def error_json(error: dict[str, Any]) -> str:
    """How a structured error is stored: UTF-8 JSON, the form its size limit applies to."""
    return json.dumps(error, ensure_ascii=False)


def truncate_error(error: dict[str, Any], limit: int) -> dict[str, Any]:
    """Shrink a structured error to fit *limit* bytes of stored JSON; errors are truncated, not rejected.

    Only ``type``, ``message`` and ``stack`` are kept, ``type`` is cut at 256 characters,
    and control characters other than tab and newlines are removed (they would reach
    operators' terminals). ``message`` and ``stack`` are cut to fit, measured in UTF-8
    bytes of the stored form; when both are long, each keeps about half.
    """
    out: dict[str, Any] = {}
    for field in ("type", "message", "stack"):
        if error.get(field) is not None:
            out[field] = _ERROR_CONTROL.sub("", str(error[field]))
    out["type"] = out.get("type", "")[:MAX_ERROR_TYPE]

    def content_bytes(text: str) -> int:  # stored size of a string, without its quotes
        return len(json.dumps(text, ensure_ascii=False).encode()) - 2

    def cut(text: str, budget: int) -> str:
        """The longest prefix of *text* that, with the marker, takes at most *budget* bytes."""
        if content_bytes(text) <= budget:
            return text
        lo, hi = 0, len(text)
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if content_bytes(text[:mid] + _TRUNCATED) <= budget:
                lo = mid
            else:
                hi = mid - 1
        return text[:lo] + _TRUNCATED if content_bytes(_TRUNCATED) <= budget else ""

    message, stack = out.get("message"), out.get("stack")
    if len(error_json(out).encode()) > limit:
        overhead = len(error_json({**out, **{k: "" for k in ("message", "stack") if k in out}}).encode())
        room = max(0, limit - overhead)
        need_m = content_bytes(message) if message else 0
        need_s = content_bytes(stack) if stack else 0
        # Each gets up to half; what one doesn't need goes to the other.
        give_m = min(need_m, max(room // 2, room - need_s))
        give_s = room - give_m
        if message:
            out["message"] = cut(message, give_m)
        if stack:
            out["stack"] = cut(stack, give_s)
    return out


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
