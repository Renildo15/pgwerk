"""The Python RemoteWorker as a client under test. It implements the conformance functions.

URL=… TOKEN=… python -m conformance.client.python_client
"""

from __future__ import annotations

import os
import asyncio
import logging

from pgwerk.remote import RemoteWorker


async def echo(**kwargs):
    return kwargs


async def sleep(seconds: float):
    await asyncio.sleep(seconds)


async def fail(message: str):
    raise RuntimeError(message)


async def big(size: int):
    return "x" * size


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    worker = RemoteWorker(
        os.environ["URL"],
        os.environ["TOKEN"],
        ["conformance"],
        {"conformance.echo": echo, "conformance.sleep": sleep, "conformance.fail": fail, "conformance.big": big},
        insecure=True,
    )
    asyncio.run(worker.run())
