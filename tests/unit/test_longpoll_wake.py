"""Who a long-poll wake-up reaches: one waiting request per worker group, in waiting order."""

from __future__ import annotations

from contextlib import AsyncExitStack

from pgwerk.remote.server.longpoll import Waiters


def _waiters() -> Waiters:
    return Waiters(connect=None, prefix="p", jobs_table=None)


async def _subscribe(stack, w, n, queues=("q",), functions=("f",)):
    return [await stack.enter_async_context(w.subscribe(list(queues), list(functions))) for _ in range(n)]


async def test_notify_wakes_one_waiter_per_group_not_everyone():
    w = _waiters()
    async with AsyncExitStack() as stack:
        gpu = await _subscribe(stack, w, 10, functions=("render",))
        cpu = await _subscribe(stack, w, 5, functions=("resize", "crop"))
        w.wake("q")
        assert [e.is_set() for e in gpu] == [True] + [False] * 9  # the longest waiting
        assert [e.is_set() for e in cpu] == [True] + [False] * 4


async def test_a_successful_claim_chains_to_the_next_waiter_of_its_group():
    w = _waiters()
    async with AsyncExitStack() as stack:
        gpu = await _subscribe(stack, w, 3, functions=("render",))
        cpu = await _subscribe(stack, w, 2, functions=("resize",))
        w.wake("q")
        w.chain("q", ["render"])
        assert [e.is_set() for e in gpu] == [True, True, False]
        assert [e.is_set() for e in cpu] == [True, False]


async def test_a_waiter_that_cleared_its_event_can_be_woken_again():
    w = _waiters()
    async with AsyncExitStack() as stack:
        a, b = await _subscribe(stack, w, 2)
        w.wake("q")
        assert (a.is_set(), b.is_set()) == (True, False)
        a.clear()  # it claimed nothing and waits again
        w.wake("q")
        assert (a.is_set(), b.is_set()) == (True, False)


async def test_tick_with_a_function_only_wakes_groups_that_declared_it():
    w = _waiters()
    async with AsyncExitStack() as stack:
        gpu = await _subscribe(stack, w, 2, functions=("render",))
        cpu = await _subscribe(stack, w, 2, functions=("resize",))
        anyfn = [await stack.enter_async_context(w.subscribe(["q"])) for _ in range(2)]
        w.wake("q", "resize")
        assert not any(e.is_set() for e in gpu)
        assert [e.is_set() for e in cpu] == [True, False]
        assert [e.is_set() for e in anyfn] == [True, False]  # no declared functions: any


async def test_unsubscribing_cleans_up_empty_groups_and_queues():
    w = _waiters()
    async with w.subscribe(["q1", "q2"], ["f"]):
        assert w.waiting_queues() == {"q1", "q2"}
    assert w.waiting_queues() == set()
    w.wake("q1")  # nothing to wake, no error
