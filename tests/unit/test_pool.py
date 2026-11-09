"""The bounded connection pool, with fake connections."""

from __future__ import annotations

import asyncio

import psycopg
import pytest

from psycopg.pq import TransactionStatus

from pgwerk.pool import PoolTimeout
from pgwerk.pool import ConnectionPool


class FakeInfo:
    def __init__(self) -> None:
        self.transaction_status = TransactionStatus.IDLE


class FakeConn:
    opened = 0

    def __init__(self) -> None:
        FakeConn.opened += 1
        self.closed = False
        self.info = FakeInfo()

    async def close(self) -> None:
        self.closed = True


async def _connect() -> FakeConn:
    return FakeConn()


@pytest.fixture(autouse=True)
def _reset():
    FakeConn.opened = 0


async def test_never_holds_more_than_max_size_and_reuses_idle():
    pool = ConnectionPool(_connect, 3)
    busy = 0
    peak = 0

    async def op():
        nonlocal busy, peak
        async with await pool() as conn:
            assert isinstance(conn, FakeConn)
            busy += 1
            peak = max(peak, busy)
            await asyncio.sleep(0.01)
            busy -= 1

    await asyncio.gather(*(op() for _ in range(50)))
    assert peak == 3
    assert FakeConn.opened == 3  # reused, not reopened
    assert pool.idle == 3


async def test_connection_left_in_a_transaction_or_broken_is_not_reused():
    pool = ConnectionPool(_connect, 2)
    async with await pool() as conn:
        conn.info.transaction_status = TransactionStatus.INERROR
    assert conn.closed and pool.idle == 0

    with pytest.raises(psycopg.OperationalError):
        async with await pool() as conn2:
            raise psycopg.OperationalError("server closed the connection")
    assert conn2.closed and pool.idle == 0


async def test_stale_idle_connection_is_replaced():
    pool = ConnectionPool(_connect, 1, max_idle=0.0)
    async with await pool() as first:
        pass
    async with await pool() as second:
        pass
    assert first.closed and second is not first


async def test_acquire_times_out_when_all_are_busy():
    pool = ConnectionPool(_connect, 1, acquire_timeout=0.05)
    async with await pool():
        with pytest.raises(PoolTimeout):
            await pool()
    async with await pool():  # the slot came back
        pass


async def test_failed_connect_gives_the_slot_back():
    async def broken():
        raise psycopg.OperationalError("refused")

    pool = ConnectionPool(broken, 1, acquire_timeout=0.05)
    for _ in range(3):
        with pytest.raises(psycopg.OperationalError, match="refused"):
            await pool()


async def test_close_closes_idle_connections_and_refuses_new_ones():
    pool = ConnectionPool(_connect, 2)
    async with await pool() as conn:
        pass
    await pool.close()
    assert conn.closed
    with pytest.raises(psycopg.OperationalError):
        await pool()
