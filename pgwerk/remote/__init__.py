"""Remote workers: workers that reach pgwerk over HTTPS instead of connecting to Postgres.

Protocol v1 (``/remote/v1``) is stable: within v1, changes are additive only. See
``docs/reference/remote-protocol.md``.
The client needs ``httpx`` (``pip install 'pgwerk[remote]'``) and is imported lazily, so
the server side doesn't require it.
"""

from __future__ import annotations

from typing import Any


__all__ = ["NonRetryable", "RemoteContext", "RemoteJob", "RemoteWorker"]


def __getattr__(name: str) -> Any:
    if name in __all__:
        from . import client

        return getattr(client, name)
    raise AttributeError(name)
