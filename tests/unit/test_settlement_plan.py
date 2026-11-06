"""The settlement decision table, tested without a database."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from datetime import timedelta
from dataclasses import replace

import pytest

from pgwerk.commons import JobStatus
from pgwerk.schemas import Job
from pgwerk.settlement import Outcome
from pgwerk.settlement import plan


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _job(**overrides) -> Job:
    job = Job(
        id="j",
        function="m.f",
        queue="default",
        status=JobStatus.Active,
        priority=0,
        attempts=1,
        max_attempts=3,
        scheduled_at=NOW,
        enqueued_at=NOW,
        on_success="cb.ok",
        on_failure="cb.fail",
        on_stopped="cb.stop",
        on_success_timeout=1,
        on_failure_timeout=2,
        on_stopped_timeout=3,
    )
    return replace(job, **overrides)


def _at(secs: int) -> datetime:
    return NOW + timedelta(seconds=secs)


class TestComplete:
    def test_completes_with_result_and_success_callback(self):
        t = plan(_job(), Outcome("complete", result_json='{"x": 1}'), NOW)
        assert t.status == JobStatus.Complete
        assert t.result_json == '{"x": 1}'
        assert (t.callback, t.callback_timeout) == ("cb.ok", 1)
        assert t.terminal and not t.retrying
        assert not t.delete and not t.repeat

    def test_expiry_from_result_ttl(self):
        assert plan(_job(result_ttl=60), Outcome("complete"), NOW).expires_at == _at(60)
        assert plan(_job(), Outcome("complete"), NOW).expires_at is None

    @pytest.mark.parametrize("remaining, expected", [(None, False), (0, False), (2, True)])
    def test_repeat_only_with_runs_remaining(self, remaining, expected):
        assert plan(_job(repeat_remaining=remaining), Outcome("complete"), NOW).repeat is expected

    def test_failure_mode_delete_does_not_delete_on_success(self):
        assert plan(_job(failure_mode="delete"), Outcome("complete"), NOW).delete is False


class TestFail:
    def test_retries_immediately_without_intervals(self):
        t = plan(_job(attempts=1, max_attempts=3), Outcome("fail", error="boom"), NOW)
        assert t.status == JobStatus.Queued
        assert t.scheduled_at is None
        assert t.error == "boom"
        assert t.callback is None
        assert t.retrying and not t.terminal

    @pytest.mark.parametrize("attempts, delay", [(1, 5), (2, 30), (3, 30)])
    def test_retry_delay_uses_interval_for_attempt_reusing_the_last(self, attempts, delay):
        job = _job(attempts=attempts, max_attempts=5, retry_intervals=[5, 30])
        t = plan(job, Outcome("fail"), NOW)
        assert t.status == JobStatus.Scheduled
        assert t.scheduled_at == _at(delay)

    def test_zero_interval_retries_immediately(self):
        t = plan(_job(retry_intervals=[0]), Outcome("fail"), NOW)
        assert (t.status, t.scheduled_at) == (JobStatus.Queued, None)

    def test_fails_at_the_attempt_cap(self):
        t = plan(_job(attempts=3, max_attempts=3, failure_ttl=10), Outcome("fail", error="boom"), NOW)
        assert t.status == JobStatus.Failed
        assert t.expires_at == _at(10)
        assert (t.callback, t.callback_timeout) == ("cb.fail", 2)
        assert t.terminal and not t.delete

    def test_not_retryable_fails_with_attempts_left(self):
        t = plan(_job(attempts=1, max_attempts=3), Outcome("fail", retryable=False), NOW)
        assert t.status == JobStatus.Failed

    def test_failure_mode_delete_deletes_terminal_failure_only(self):
        assert plan(_job(attempts=3, failure_mode="delete"), Outcome("fail"), NOW).delete is True
        assert plan(_job(attempts=1, failure_mode="delete"), Outcome("fail"), NOW).delete is False

    def test_retry_does_not_repeat(self):
        assert plan(_job(repeat_remaining=2), Outcome("fail"), NOW).repeat is False


class TestAbort:
    def test_aborts_with_stopped_callback(self):
        t = plan(_job(failure_ttl=10), Outcome("abort"), NOW)
        assert t.status == JobStatus.Aborted
        assert t.error == "aborted"
        assert t.expires_at == _at(10)
        assert (t.callback, t.callback_timeout) == ("cb.stop", 3)
        assert t.terminal and not t.repeat

    def test_abort_never_retries_even_with_attempts_left(self):
        assert plan(_job(attempts=1, max_attempts=3), Outcome("abort"), NOW).status == JobStatus.Aborted

    def test_failure_mode_delete_deletes(self):
        assert plan(_job(failure_mode="delete"), Outcome("abort"), NOW).delete is True


def test_unknown_outcome_kind_raises():
    with pytest.raises(ValueError):
        plan(_job(), Outcome("bogus"), NOW)  # type: ignore[arg-type]


def test_worker_fence_requires_a_lease():
    from pgwerk.settlement import Fence

    with pytest.raises(ValueError):
        Fence("j", "worker", 1)
    Fence("j", "worker", 1, "lease")
    Fence("j", None, 1)  # the sweeper, on rows it already locked


class TestLost:
    def test_requeues_now_without_using_a_retry(self):
        t = plan(_job(attempts=1, max_attempts=1, lost_count=0, max_lost=3, retry_intervals=[60]), Outcome("lost"), NOW)
        assert (t.status, t.scheduled_at, t.error) == (JobStatus.Queued, NOW, "lease_lost")
        assert t.lost and t.retrying and t.callback is None

    def test_fails_at_max_lost_like_any_failure(self):
        job = _job(lost_count=3, max_lost=3, failure_mode="delete", failure_ttl=10)
        t = plan(job, Outcome("lost"), NOW)
        assert (t.status, t.error) == (JobStatus.Failed, "lease_lost")
        assert t.delete and t.expires_at == _at(10)
        assert (t.callback, t.callback_timeout) == ("cb.fail", 2)

    def test_default_cap_applies_without_max_lost(self):
        assert plan(_job(lost_count=2, max_lost=None), Outcome("lost"), NOW, default_max_lost=3).retrying
        assert plan(_job(lost_count=3, max_lost=None), Outcome("lost"), NOW, default_max_lost=3).terminal

    def test_max_lost_zero_fails_the_first_loss(self):
        assert plan(_job(lost_count=0, max_lost=0), Outcome("lost"), NOW).status == JobStatus.Failed
