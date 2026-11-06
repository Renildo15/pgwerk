"""Workers that declare the functions they handle claim only those."""

from __future__ import annotations

import uuid

import pytest

from psycopg.sql import SQL

from pgwerk.commons import JobStatus
from pgwerk.commons import DequeueStrategy

from .tasks import add
from .tasks import noop
from .conftest import make_worker


class TestFunctionFilter:
    async def test_worker_with_functions_runs_only_those(self, app):
        a = await app.enqueue(noop)
        b = await app.enqueue(add, 1, 2)

        await make_worker(app, functions=[noop]).run()

        assert (await app.get_job(a.id)).status == JobStatus.Complete
        assert (await app.get_job(b.id)).status == JobStatus.Queued

    async def test_dotted_paths_work_like_callables(self, app):
        b = await app.enqueue(add, 1, 2)
        await make_worker(app, functions=["tests.integration.tasks.add"]).run()
        assert (await app.get_job(b.id)).result == 3

    async def test_worker_without_functions_runs_everything(self, app):
        a = await app.enqueue(noop)
        b = await app.enqueue(add, 1, 2)
        await make_worker(app).run()
        assert {(await app.get_job(j.id)).status for j in (a, b)} == {JobStatus.Complete}

    async def test_mixed_queue_split_across_two_deployments(self, app):
        jobs = [await app.enqueue(noop) for _ in range(3)] + [await app.enqueue(add, 1, 1) for _ in range(3)]
        await make_worker(app, functions=[add]).run()
        await make_worker(app, functions=[noop]).run()
        assert {(await app.get_job(j.id)).status for j in jobs} == {JobStatus.Complete}

    async def test_registers_its_functions(self, app):
        wid = str(uuid.uuid4())
        await app._worker_repo.register(wid, "w", ["default"], "{}", functions=["m.f"])
        async with await app._connect() as conn:
            cur = await conn.execute(SQL("SELECT functions FROM {w} WHERE id = %s").format(w=app._t["worker"]), (wid,))
            assert (await cur.fetchone())[0] == ["m.f"]

    async def test_repo_filter_and_none_means_all(self, app):
        await app.enqueue(noop)
        await app.enqueue(add, 1, 2)
        wid = str(uuid.uuid4())
        await app._worker_repo.register(wid, "w", ["default"], "{}")
        claim = app._worker_repo.dequeue
        got = await claim(wid, ["default"], 10, DequeueStrategy.Priority, functions=["nope.nothing"])
        assert got == []
        got = await claim(wid, ["default"], 10, DequeueStrategy.Priority, functions=None)
        assert len(got) == 2


def test_empty_function_list_is_rejected():
    from unittest.mock import MagicMock

    from pgwerk.worker import AsyncWorker

    with pytest.raises(ValueError):
        AsyncWorker(app=MagicMock(), functions=[])


class TestCatalog:
    async def test_registration_upserts_and_merges_languages(self, app):
        await app._worker_repo.register(str(uuid.uuid4()), "py", ["default"], "{}", functions=["m.f", "m.g"])
        await app._worker_repo.register(str(uuid.uuid4()), "go", ["default"], "{}", functions=["m.f"], lang="go")
        await app._worker_repo.register(str(uuid.uuid4()), "py2", ["default"], "{}", functions=["m.f"])

        catalog = {r["name"]: r["languages"] for r in await app.list_functions()}
        assert catalog == {"m.f": ["go", "python"], "m.g": ["python"]}

    async def test_worker_without_functions_adds_nothing(self, app):
        await make_worker(app).run()
        assert await app.list_functions() == []

    async def test_running_worker_registers_its_functions(self, app):
        await make_worker(app, functions=[noop]).run()
        assert [r["name"] for r in await app.list_functions()] == ["tests.integration.tasks.noop"]

    async def test_catalog_is_informational_unknown_functions_still_enqueue(self, app):
        await app._worker_repo.register(str(uuid.uuid4()), "w", ["default"], "{}", functions=["m.only"])
        job = await app.enqueue("some.unregistered.function")
        assert job is not None and job.status == JobStatus.Queued

    async def test_api_lists_the_catalog(self, app):
        from litestar.testing import AsyncTestClient

        from pgwerk.api.app import create_app
        from pgwerk.config import WerkConfig

        await app._worker_repo.register(str(uuid.uuid4()), "w", ["default"], "{}", functions=["m.f"])
        api = create_app(WerkConfig(dsn=app.dsn, prefix=app.prefix))
        async with AsyncTestClient(app=api) as client:
            resp = await client.get("/api/functions")
        assert resp.status_code == 200
        [entry] = resp.json()
        assert (entry["name"], entry["languages"]) == ("m.f", ["python"])
