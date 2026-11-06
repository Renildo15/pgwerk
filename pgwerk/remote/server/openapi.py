"""Export the OpenAPI description of ``/remote/v1``.

    python -m pgwerk.remote.server.openapi > docs/reference/remote-openapi.json

The committed file guards the wire format: a test regenerates it and fails on any
difference, so a protocol change can't slip in unnoticed. The normative description is
``docs/reference/remote-protocol.md``; this file is a machine-readable convenience.
"""

from __future__ import annotations

import sys
import json

from typing import Any


def remote_openapi() -> dict[str, Any]:
    from litestar.serialization import encode_json

    from ...config import WerkConfig
    from ...api.app import create_app

    config = WerkConfig(dsn="postgresql://unused/unused", remote_workers=True, api=False, ui=False, maintenance=False)
    schema: dict[str, Any] = json.loads(encode_json(create_app(config).openapi_schema.to_schema()))
    schema["paths"] = {p: v for p, v in schema.get("paths", {}).items() if p.startswith("/remote/v1/")}
    schema["info"] = {
        "title": "pgwerk remote workers",
        "version": "1",
        "description": "Protocol v1. Normative spec: docs/reference/remote-protocol.md.",
    }
    schema.pop("servers", None)
    return _without_examples(schema)


def _without_examples(node: Any) -> Any:
    """Drop generated examples: they are random (they depend on the process's random
    state), aren't part of the wire format, and would make the guard flaky."""
    if isinstance(node, dict):
        return {k: _without_examples(v) for k, v in node.items() if k not in ("example", "examples")}
    if isinstance(node, list):
        return [_without_examples(v) for v in node]
    return node


def dumps() -> str:
    return json.dumps(remote_openapi(), indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    sys.stdout.write(dumps())
