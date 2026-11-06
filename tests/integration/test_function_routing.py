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


class TestUnroutable:
    """Each test uses its own queue: worker rows from other tests aren't truncated."""

    async def _live(self, app, queues, functions=None, role="worker"):
        await app._worker_repo.register(str(uuid.uuid4()), "w", queues, "{}", role=role, functions=functions)

    def _mine(self, rows, q):
        return [(r["function"], r["count"]) for r in rows if r["queue"] == q]

    async def test_reports_a_function_no_live_worker_declared(self, app):
        q = f"q-{uuid.uuid4()}"
        await app.enqueue(noop, _queue=q)
        await app.enqueue(add, 1, 2, _queue=q)
        await self._live(app, [q], functions=["tests.integration.tasks.noop"])

        assert self._mine(await app.unroutable_jobs(), q) == [("tests.integration.tasks.add", 1)]

    async def test_catch_all_worker_routes_everything(self, app):
        q = f"q-{uuid.uuid4()}"
        await app.enqueue(add, 1, 2, _queue=q)
        await self._live(app, [q], functions=["tests.integration.tasks.noop"])
        await self._live(app, ["other", q])
        assert self._mine(await app.unroutable_jobs(), q) == []

    async def test_queue_with_no_worker_and_schedulers_dont_count(self, app):
        q = f"q-{uuid.uuid4()}"
        await app.enqueue(noop, _queue=q)
        await self._live(app, [q], role="scheduler")
        assert self._mine(await app.unroutable_jobs(), q) == [("tests.integration.tasks.noop", 1)]

    async def test_stale_workers_dont_count(self, app):
        import psycopg

        q = f"q-{uuid.uuid4()}"
        await app.enqueue(noop, _queue=q)
        wid = str(uuid.uuid4())
        await app._worker_repo.register(wid, "w", [q], "{}")
        async with await psycopg.AsyncConnection.connect(app.dsn, autocommit=True) as conn:
            await conn.execute(
                SQL("UPDATE {w} SET heartbeat_at = NOW() - INTERVAL '5 minutes' WHERE id = %s").format(
                    w=app._t["worker"]
                ),
                (wid,),
            )
        assert self._mine(await app.unroutable_jobs(), q) == [("tests.integration.tasks.noop", 1)]

    async def test_future_jobs_are_not_counted(self, app):
        q = f"q-{uuid.uuid4()}"
        await app.enqueue(noop, _queue=q, _delay=3600)
        assert self._mine(await app.unroutable_jobs(), q) == []

    async def test_exporter_gauge(self, app):
        pytest.importorskip("prometheus_client")
        from pgwerk.exporter import WerkExporter

        q = f"q-{uuid.uuid4()}"
        await app.enqueue(add, 1, 2, _queue=q)
        await self._live(app, [q], functions=["tests.integration.tasks.noop"])
        exporter = WerkExporter(app)

        def mine():
            return {f: g._value.get() for (queue, f), g in exporter._unroutable._metrics.items() if queue == q}

        await exporter.collect()
        assert mine() == {"tests.integration.tasks.add": 1}

        await self._live(app, [q])
        await exporter.collect()
        assert mine() == {}
