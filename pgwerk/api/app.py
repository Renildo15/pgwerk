from __future__ import annotations

from litestar import Router
from litestar import Litestar
from litestar.openapi import OpenAPIConfig
from litestar.exceptions import NotFoundException

from .auth import make_bearer_guard
from .auth import make_basic_auth_guard
from .spa import init_spa
from .deps import init_werk
from .deps import init_maintenance
from .routes import MetricsController
from .routes import make_router
from .exporter import init_exporter
from .handlers import server_error_handler
from ..config import WerkConfig


def create_app(config: WerkConfig | None = None) -> Litestar:
    """Create the Litestar observability app.

    Args:
        config: Full wrk configuration. Defaults to ``WerkConfig()`` when omitted.

    Returns:
        A configured Litestar application instance.
    """
    if config is None:
        config = WerkConfig.from_env()

    state: dict = {}
    dependencies: dict = {}
    on_startup: list = []
    on_shutdown: list = []

    if config.remote_workers and config.api and not config.api_token:
        # Mounting /remote puts this process on the internet; an open admin API next
        # to it would be too.
        raise RuntimeError(
            "remote_workers=True requires api_token (PGWERK_API_TOKEN / --api-token), "
            "or api=False (--no-api) to serve only the remote router"
        )

    route_handlers: list = []
    api_guards: list = []
    if config.api:
        if config.api_token:
            api_guards.append(make_bearer_guard(config.api_token))
        route_handlers.append(make_router(guards=api_guards))

    init_werk(config, state, dependencies, on_startup, on_shutdown)

    if config.remote_workers:
        from ..remote.server import init_remote
        from ..remote.server import make_remote_router

        route_handlers.append(make_remote_router(state))
        init_remote(config, state, on_startup, on_shutdown)

    if config.maintenance_enabled:
        init_maintenance(state, on_startup, on_shutdown)

    if config.metrics:
        init_exporter(config.metrics_interval, state, on_startup, on_shutdown)
        route_handlers.append(MetricsController)

    if config.ui and config.api:  # the dashboard is a client of /api
        spa_guard = None
        if config.ui_auth:
            user, _, password = config.ui_auth.partition(":")
            spa_guard = make_basic_auth_guard(user, password)
        init_spa(route_handlers, guard=spa_guard)

    return Litestar(
        route_handlers=route_handlers,
        dependencies=dependencies,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        exception_handlers={Exception: server_error_handler},
        openapi_config=_openapi_config(config, api_guards if config.api else None),
    )


def _no_schema(*_: object) -> None:
    raise NotFoundException()


def _openapi_config(config: WerkConfig, api_guards: list | None) -> OpenAPIConfig:
    """``/schema`` lists every route, so it is guarded like ``/api``: behind the admin
    token when there is one, and not served at all by a process without the admin API
    (a remote-only server). The schema is still built in-process either way."""
    guards = api_guards if api_guards is not None else [_no_schema]
    return OpenAPIConfig(
        title="Litestar API",
        version="1.0.0",
        openapi_router=Router(path="/schema", route_handlers=[], guards=guards),
    )
