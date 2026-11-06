"""``werk token`` and ``werk workers``: manage remote-worker access over a DSN."""

from __future__ import annotations

import asyncio

from typing import Any
from typing import Callable
from functools import wraps

import click


def _db_options(f: Callable) -> Callable:
    f = click.option("--prefix", default=None, envvar="PGWERK_PREFIX", help="Table-name prefix.")(f)
    f = click.option("--schema", default=None, envvar="PGWERK_SCHEMA", help="Postgres schema for wrk tables.")(f)
    f = click.option("--dsn", envvar="PGWERK_DSN", required=True, help="Postgres connection string.")(f)
    return f


def _with_werk(f: Callable) -> Callable:
    """Run an async command body with a connected Werk built from --dsn/--schema/--prefix."""

    @wraps(f)
    def wrapper(dsn: str, schema: str | None, prefix: str | None, **kwargs: Any) -> Any:
        from ..app import Werk

        async def _run() -> Any:
            async with Werk(dsn, schema=schema or "pgwerk", prefix=prefix or "_pgwerk") as werk:
                return await f(werk, **kwargs)

        return asyncio.run(_run())

    return wrapper


@click.group()
def token() -> None:
    """Enrollment tokens that let remote workers register (experimental)."""


@token.command("create")
@_db_options
@click.option("--name", required=True, help="Label for the machine or fleet the token is for.")
@click.option("--queues", "-q", required=True, help="Comma-separated queues its workers may consume.")
@click.option(
    "--expires-in", type=click.IntRange(min=1), default=None, help="Days until it stops accepting new registrations."
)
@_with_werk
async def token_create(werk: Any, name: str, queues: str, expires_in: int | None) -> None:
    """Create a token. It is printed once and can't be shown again."""
    queue_list = [q.strip() for q in queues.split(",") if q.strip()]
    secret, row = await werk.create_enrollment_token(
        name, queue_list, expires_in=expires_in * 86400 if expires_in else None
    )
    click.echo(f"id:      {row['id']}")
    click.echo(f"queues:  {', '.join(row['queues'])}")
    click.echo(f"expires: {row['expires_at'] or 'never'}")
    click.echo("")
    click.echo(secret)
    click.echo("")
    click.echo("Store this token now; it is not shown again.", err=True)


@token.command("list")
@_db_options
@_with_werk
async def token_list(werk: Any) -> None:
    """List tokens (never their secrets)."""
    rows = await werk.list_enrollment_tokens()
    if not rows:
        click.echo("No enrollment tokens.")
        return
    for r in rows:
        state = "revoked" if r["revoked_at"] else "active"
        click.echo(
            f"{r['id']}  {r['name']:<20} {state:<8} queues={','.join(r['queues'])}  "
            f"workers={r['live_workers']}  last_used={r['last_used_at'] or 'never'}"
        )


@token.command("revoke")
@_db_options
@click.argument("token_id")
@_with_werk
async def token_revoke(werk: Any, token_id: str) -> None:
    """Revoke a token and every worker registered with it, at once (no drain).

    Their jobs go back to the queue. Use this when a machine may be compromised.
    """
    if not await werk.revoke_enrollment_token(token_id):
        raise click.ClickException(f"no active enrollment token {token_id}")
    click.echo(f"Revoked {token_id} and its workers.")


@click.group()
def workers() -> None:
    """Manage registered remote workers (experimental)."""


@workers.command("revoke")
@_db_options
@click.argument("worker_id")
@click.option("--drain", is_flag=True, default=False, help="Let it finish the jobs it holds, but claim no more.")
@_with_werk
async def workers_revoke(werk: Any, worker_id: str, drain: bool) -> None:
    """Retire one remote worker.

    This doesn't stop a compromised machine, which can register again with its
    enrollment token; use `werk token revoke` for that.
    """
    if not await werk.revoke_worker(worker_id, drain=drain):
        raise click.ClickException(f"no active remote worker {worker_id}")
    click.echo(f"Worker {worker_id} {'is draining' if drain else 'revoked'}.")
