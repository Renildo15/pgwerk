"""Periodic maintenance that keeps the queue healthy without depending on workers.

The same pass runs from three places:

- inside every worker (``BaseWorker`` side loop, on by default);
- standalone, with ``wrk maintain``;
- inside the API process, when ``WerkConfig.maintenance`` is set.

Every query takes its rows with ``FOR UPDATE SKIP LOCKED``, so any number of
runners can operate on the same database at once.

Callbacks of jobs settled here (e.g. ``on_stopped`` for an expired abort) run in
the maintenance process, so their modules must be importable there.
"""

from __future__ import annotations

import asyncio
import logging

from typing import TYPE_CHECKING
from dataclasses import field
from dataclasses import dataclass

from .repos import ReconcileResult


if TYPE_CHECKING:
    from .app import Werk


logger = logging.getLogger(__name__)


@dataclass
class MaintenanceReport:
    """What one :meth:`Maintenance.run_once` pass did."""

    swept: list[str] = field(default_factory=list)
    """IDs of stuck jobs recovered by the sweep."""
    reconciled: ReconcileResult = field(default_factory=lambda: ReconcileResult(queued=0, failed=0))
    """Stranded ``waiting`` jobs that were settled."""
    expired: int = 0
    """Unstarted jobs aborted because their ``_ttl`` ran out."""
    rescheduled: int = 0
    """Due ``scheduled`` jobs moved to ``queued``."""
    errors: dict[str, str] = field(default_factory=dict)
    """Task name → error message, for tasks that raised."""


class Maintenance:
    """Runs sweep, ``_ttl`` expiry, waiting-job reconciliation and rescheduling on an interval.

    Args:
        app: Connected :class:`~pgwerk.app.Werk` instance.
        interval: Seconds between passes; defaults to ``app.config.sweep_interval``.
    """

    def __init__(self, app: "Werk", interval: float | None = None) -> None:
        self.app = app
        self.interval = interval if interval is not None else app.config.sweep_interval
        self._stopped = asyncio.Event()

    async def run_once(self) -> MaintenanceReport:
        """Run every maintenance task once. A failing task doesn't stop the others."""
        report = MaintenanceReport()

        try:
            report.swept = await self.app.sweep()
        except Exception as exc:
            report.errors["sweep"] = str(exc)
            logger.warning("maintenance: sweep failed: %s", exc)

        try:
            report.expired = await self.app.expire_unstarted()
        except Exception as exc:
            report.errors["expire_unstarted"] = str(exc)
            logger.warning("maintenance: expiring unstarted jobs failed: %s", exc)

        try:
            report.reconciled = await self.app.reconcile_waiting()
        except Exception as exc:
            report.errors["reconcile_waiting"] = str(exc)
            logger.warning("maintenance: waiting-job reconciliation failed: %s", exc)

        try:
            report.rescheduled = await self.app.reschedule_stuck()
        except Exception as exc:
            report.errors["reschedule_stuck"] = str(exc)
            logger.warning("maintenance: reschedule failed: %s", exc)

        return report

    async def run(self) -> None:
        """Run passes every ``interval`` seconds until :meth:`stop` is called."""
        self._stopped.clear()
        logger.info("maintenance: running every %.0fs", self.interval)
        while not self._stopped.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                pass

    def stop(self) -> None:
        """Ask :meth:`run` to return after the current pass."""
        self._stopped.set()
