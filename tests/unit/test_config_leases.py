from __future__ import annotations

import os

from pgwerk.config import WerkConfig


def test_lease_settings_defaults():
    cfg = WerkConfig()
    assert (cfg.local_lease_ttl, cfg.remote_lease_ttl, cfg.default_max_lost) == (300, 60, 3)


def test_lease_settings_round_trip_through_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith("PGWERK_"):
            monkeypatch.delenv(key)
    WerkConfig(dsn="postgresql://x/y", local_lease_ttl=120, remote_lease_ttl=30, default_max_lost=5).to_env()
    cfg = WerkConfig.from_env()
    assert (cfg.local_lease_ttl, cfg.remote_lease_ttl, cfg.default_max_lost) == (120, 30, 5)


def test_compat_window_covers_planned_additive_migrations():
    assert WerkConfig.schema_version >= 8
    assert WerkConfig.max_compatible_db_version is not None
    assert WerkConfig.max_compatible_db_version >= WerkConfig.schema_version
