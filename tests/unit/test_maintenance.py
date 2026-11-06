from __future__ import annotations

import asyncio

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

from pgwerk.repos import ReconcileResult
from pgwerk.maintenance import Maintenance


def _app(**overrides):
    app = MagicMock()
    app.config.sweep_interval = 60.0
    app.sweep = AsyncMock(return_value=["a", "b"])
    app.expire_unstarted = AsyncMock(return_value=4)
    app.purge_expired = AsyncMock(return_value=5)
    app.config.purge_expired = True
    app.reconcile_waiting = AsyncMock(return_value=ReconcileResult(queued=2, failed=1))
    app.reschedule_stuck = AsyncMock(return_value=3)
    for name, value in overrides.items():
        setattr(app, name, value)
    return app


class TestRunOnce:
    async def test_reports_every_task(self):
        report = await Maintenance(_app()).run_once()
        assert report.swept == ["a", "b"]
        assert report.reconciled == ReconcileResult(queued=2, failed=1)
        assert report.rescheduled == 3
        assert report.expired == 4
        assert report.purged == 5
        assert report.errors == {}

    async def test_failing_task_does_not_stop_the_others(self):
        app = _app(sweep=AsyncMock(side_effect=RuntimeError("db down")))
        report = await Maintenance(app).run_once()
        assert report.errors == {"sweep": "db down"}
        assert report.swept == []
        app.expire_unstarted.assert_awaited_once()
        app.reconcile_waiting.assert_awaited_once()
        app.reschedule_stuck.assert_awaited_once()


class TestRun:
    def test_interval_defaults_to_sweep_interval(self):
        assert Maintenance(_app()).interval == 60.0
        assert Maintenance(_app(), interval=5).interval == 5

    async def test_runs_until_stopped(self):
        m = Maintenance(_app(), interval=0.01)
        task = asyncio.create_task(m.run())
        await asyncio.sleep(0.05)
        m.stop()
        await asyncio.wait_for(task, timeout=1.0)
        assert m.app.sweep.await_count >= 2


async def test_purge_is_skipped_when_disabled():
    app = _app()
    app.config.purge_expired = False
    report = await Maintenance(app).run_once()
    app.purge_expired.assert_not_awaited()
    assert report.purged == 0
