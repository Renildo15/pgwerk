"""The committed OpenAPI file must match the router: the wire format can't change silently."""

from __future__ import annotations

from pathlib import Path

from pgwerk.remote.server.openapi import dumps


COMMITTED = Path(__file__).resolve().parents[2] / "docs" / "reference" / "remote-openapi.json"


def test_remote_openapi_is_up_to_date():
    assert COMMITTED.read_text() == dumps(), (
        "The /remote/v1 wire format changed. If that's intended (additive only within v1), "
        "regenerate with: just openapi"
    )


def test_it_describes_only_the_remote_router():
    import json

    paths = json.loads(dumps())["paths"]
    assert paths and all(p.startswith("/remote/v1/") for p in paths)
