from __future__ import annotations

import signal
import asyncio

from dataclasses import replace

import click

from ..logging import configure_logging


@click.command()
@click.option("--dsn", envvar="PGWERK_DSN", required=True, help="Postgres connection string.")
@click.option("--schema", default=None, envvar="PGWERK_SCHEMA", help="Postgres schema for wrk tables.")
@click.option("--prefix", default=None, envvar="PGWERK_PREFIX", help="Table-name prefix.")
@click.option(
    "--interval",
    type=float,
    default=None,
    envvar="PGWERK_SWEEP_INTERVAL",
    help="Seconds between passes.  [default: 60]",
)
@click.option("--once", is_flag=True, default=False, help="Run a single pass and exit.")
@click.option(
    "--log-level",
    "-l",
    default="INFO",
    show_default=True,
    envvar="PGWERK_LOG_LEVEL",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
)
@click.option(
    "--log-format",
    default="text",
    show_default=True,
    envvar="PGWERK_LOG_FORMAT",
    type=click.Choice(["text", "json"], case_sensitive=False),
)
@click.option("--no-color", is_flag=True, default=False, envvar="PGWERK_NO_COLOR", help="Disable colored log output.")
def maintain(
    dsn: str,
    schema: str | None,
    prefix: str | None,
    interval: float | None,
    once: bool,
    log_level: str,
    log_format: str,
    no_color: bool,
) -> None:
    """Run the maintenance pass without a worker.

    Recovers stuck jobs (sweep), settles dependents stranded in ``waiting`` and
    queues due scheduled jobs. Safe to run alongside workers and other
    ``wrk maintain`` processes.

    Callbacks of jobs settled here (e.g. ``on_stopped`` for an expired abort) run in
    this process, so their modules must be importable from it.
    """
    configure_logging(level=log_level, format=log_format, color=False if no_color else None)

    from ..app import Werk
    from ..config import WerkConfig
    from ..maintenance import Maintenance

    config = replace(WerkConfig.from_env(), dsn=dsn, schema=schema or "pgwerk", prefix=prefix or "_pgwerk")
    if interval is not None:
        config = replace(config, sweep_interval=interval)

    async def _run() -> None:
        async with Werk(dsn, schema=config.schema, prefix=config.prefix, config=config) as app:
            maintenance = Maintenance(app)
            if once:
                report = await maintenance.run_once()
                click.echo(
                    f"swept={len(report.swept)} "
                    f"reconciled_queued={report.reconciled.queued} "
                    f"reconciled_failed={report.reconciled.failed} "
                    f"rescheduled={report.rescheduled}"
                )
                if report.errors:
                    raise click.ClickException(f"maintenance errors: {report.errors}")
                return

            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                try:
                    loop.add_signal_handler(sig, maintenance.stop)
                except (NotImplementedError, ValueError):
                    pass
            await maintenance.run()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass
