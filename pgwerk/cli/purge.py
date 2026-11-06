from __future__ import annotations

import asyncio

import click

from .utils import load_app


@click.command()
@click.argument("app")
@click.option("--status", "-s", default="complete,failed,aborted", help="Comma-separated statuses to delete.")
@click.option("--queue", "-q", default=None, help="Limit to a specific queue.")
@click.option(
    "--older-than", type=click.IntRange(min=0), default=None, help="Only delete jobs enqueued more than N days ago."
)
@click.confirmation_option(prompt="Purge matching jobs?")
def purge(app: str, status: str, queue: str | None, older_than: int | None) -> None:
    """Delete jobs from the database.

    Jobs waiting on a purged job fail with ``dependency failed`` (or are queued when
    the dependency allows failure), instead of running as if it had succeeded.
    """

    wrk_app = load_app(app)
    statuses = [s.strip() for s in status.split(",")]

    async def _run() -> None:
        async with wrk_app:
            deleted = await wrk_app.purge_jobs(statuses=statuses, older_than_days=older_than, queue=queue)
            click.echo(f"Purged {deleted} job(s).")

    asyncio.run(_run())
