"""Protocol v1 client conformance: how a client must behave (docs/reference/remote-protocol.md)."""

from __future__ import annotations

import time

import pytest

from .conftest import ENROLLMENT
from .conftest import run_client
from .conftest import client_lang
from .fake_server import Call
from .fake_server import Scenario
from .fake_server import err


def test_registers_with_the_enrollment_token_and_declares_its_functions(scenario):  # §2, §5.1
    run_client(scenario, lambda s: bool(s.of("claim")))
    [reg] = scenario.of("register")
    assert reg.headers["pgwerk-protocol"] == "1"
    assert reg.headers["authorization"] == f"Bearer {ENROLLMENT}"
    assert reg.body["queues"] == ["conformance"]
    names = {f["name"] for f in reg.body["functions"]}
    assert names >= {"conformance.echo", "conformance.sleep", "conformance.fail", "conformance.big"}
    assert reg.body["client"]["lang"]


def test_uses_the_worker_token_and_protocol_header_after_register(scenario):  # §2
    run_client(scenario, lambda s: len(s.of("claim")) >= 2)
    for call in scenario.calls[1:]:
        assert call.headers["authorization"] == f"Bearer {scenario.worker_token}"
        assert call.headers["pgwerk-protocol"] == "1"


def test_runs_a_job_and_completes_it_with_its_result(scenario):  # §5.4
    scenario.jobs = [{"function": "conformance.echo", "kwargs": {"a": 1, "b": [2]}, "lease_id": "L-echo"}]
    run_client(scenario, lambda s: bool(s.of("complete")))
    [done] = scenario.of("complete", "L-echo")
    assert done.body == {"result": {"a": 1, "b": [2]}}


def test_empty_claims_continue_with_fresh_claim_ids(scenario):  # §5.2
    run_client(scenario, lambda s: len(s.of("claim")) >= 3)
    ids = [c.body["claim_id"] for c in scenario.of("claim")]
    assert len(set(ids)) == len(ids)
    assert all(isinstance(c.body.get("wait"), (int, float)) for c in scenario.of("claim"))


def test_retries_a_failed_claim_with_the_same_claim_id(scenario):  # §5.2
    class S(Scenario):
        def on_claim(self, call: Call):
            if len(self.of("claim")) == 1:
                return err(503, "internal")
            return super().on_claim(call)

    s = S()
    run_client(s, lambda s: len(s.of("claim")) >= 3, timeout=20)
    first, second = s.of("claim")[:2]
    assert first.body["claim_id"] == second.body["claim_id"]


def test_heartbeats_at_the_requested_cadence(scenario):  # §3.1
    scenario.jobs = [{"function": "conformance.sleep", "kwargs": {"seconds": 3.5}, "lease_id": "L-hb"}]
    run_client(scenario, lambda s: bool(s.of("complete")))
    beats = scenario.of("heartbeat", "L-hb")
    assert len(beats) >= 2, "expected a heartbeat about every second during a 3.5s job"
    assert scenario.of("complete", "L-hb")


def test_retries_complete_until_accepted(scenario):  # §6
    class S(Scenario):
        def on_complete(self, call: Call):
            return err(503, "internal") if len(self.of("complete")) <= 2 else super().on_complete(call)

    s = S(jobs=[{"function": "conformance.echo", "kwargs": {"v": 1}, "lease_id": "L-retry"}])
    run_client(s, lambda s: len(s.of("complete")) >= 3, timeout=20)
    calls = s.of("complete", "L-retry")
    assert len(calls) == 3 and all(c.body == calls[0].body for c in calls)


def test_stops_and_never_reports_after_lease_lost(scenario):  # §3.1
    class S(Scenario):
        def on_heartbeat(self, call: Call):
            return err(409, "lease_lost")

    s = S(jobs=[{"function": "conformance.sleep", "kwargs": {"seconds": 2}, "lease_id": "L-lost"}])
    run_client(s, lambda s: bool(s.of("heartbeat")) and time.monotonic() - s.of("heartbeat")[0].at > 3)
    assert s.of("heartbeat", "L-lost")
    assert not s.of("complete", "L-lost") and not s.of("fail", "L-lost")


def test_reports_an_abort_signalled_by_heartbeat(scenario):  # §3.2
    class S(Scenario):
        def on_heartbeat(self, call: Call):
            from litestar import Response

            return Response({"lease_expires_at": "2099-01-01T00:00:00+00:00", "abort": True}, status_code=200)

    s = S(jobs=[{"function": "conformance.sleep", "kwargs": {"seconds": 30}, "lease_id": "L-abort"}])
    run_client(s, lambda s: bool(s.of("fail")))
    [fail] = s.of("fail", "L-abort")
    assert fail.body.get("aborted") is True
    assert fail.at - s.of("heartbeat")[0].at < 5, "should stop promptly, not run the whole job"


def test_reports_failures_with_a_structured_error(scenario):  # §5.5
    scenario.jobs = [{"function": "conformance.fail", "kwargs": {"message": "boom"}, "lease_id": "L-fail"}]
    run_client(scenario, lambda s: bool(s.of("fail")))
    [fail] = scenario.of("fail", "L-fail")
    assert fail.body["error"]["type"] and "boom" in fail.body["error"]["message"]
    assert fail.body.get("retryable", True) is True


def test_too_large_result_becomes_a_non_retryable_failure(scenario):  # §5.4
    class S(Scenario):
        def on_complete(self, call: Call):
            return err(413, "too_large")

    s = S(jobs=[{"function": "conformance.big", "kwargs": {"size": 5000}, "lease_id": "L-big"}])
    run_client(s, lambda s: bool(s.of("fail")))
    [fail] = s.of("fail", "L-big")
    assert fail.body["retryable"] is False


def test_draining_stops_claiming_and_deregisters(scenario):  # §3.3
    class S(Scenario):
        def on_claim(self, call: Call):
            return err(403, "draining")

    s = S()
    proc = run_client(s, lambda s: bool(s.of("deregister")), timeout=15)
    assert s.of("deregister")
    assert len(s.of("claim")) == 1
    assert proc.wait(10) is not None


def test_unauthorized_stops_the_client(scenario):  # §2
    class S(Scenario):
        def on_claim(self, call: Call):
            return err(401, "unauthorized")

    s = S()
    proc = run_client(s, lambda s: False, timeout=8)
    assert proc.returncode is not None
    assert len(s.of("claim")) <= 2, "a client must stop using a token the server refuses"
    assert len(s.of("register")) == 1, "unauthorized means retired: the client must not register again"


class _Expiring(Scenario):
    """Each register hands out a new worker token; the first session is expired."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.tokens: list[str] = []

    def on_register(self, call: Call):
        import uuid

        self.worker_token = f"pgwk_w_{uuid.uuid4().hex}_{'w' * 43}"
        self.tokens.append(self.worker_token)
        return super().on_register(call)

    def first_session(self, call: Call) -> bool:
        return call.headers.get("authorization") == f"Bearer {self.tokens[0]}"


def test_session_expired_registers_again_and_carries_on(scenario):  # §2, §8
    class S(_Expiring):
        def on_claim(self, call: Call):
            if self.first_session(call):
                return err(401, "session_expired")
            return super().on_claim(call)

    s = S(jobs=[{"function": "conformance.echo", "kwargs": {"x": 1}, "lease_id": "L-after"}])
    run_client(s, lambda s: bool(s.of("complete", "L-after")), timeout=20)
    registers = s.of("register")
    assert len(registers) == 2, "a client must register again after session_expired"
    assert registers[1].headers["authorization"] == f"Bearer {ENROLLMENT}"
    [complete] = s.of("complete", "L-after")
    assert complete.headers["authorization"] == f"Bearer {s.tokens[1]}"


def test_session_expired_drops_the_attempt_it_held(scenario):  # §2
    class S(_Expiring):
        def on_heartbeat(self, call: Call):
            if self.first_session(call):
                return err(401, "session_expired")
            return super().on_heartbeat(call)

    s = S(jobs=[{"function": "conformance.sleep", "kwargs": {"seconds": 4}, "lease_id": "L-old"}])
    run_client(s, lambda s: len(s.of("register")) >= 2, timeout=20)
    time.sleep(5)  # longer than the job: an attempt that kept running would report by now
    assert not s.of("complete", "L-old") and not s.of("fail", "L-old"), "attempts of an expired session are lost"


def test_positional_args(scenario):  # §7
    scenario.jobs = [{"function": "conformance.echo", "args": [1, 2], "kwargs": {}, "lease_id": "L-args"}]
    run_client(scenario, lambda s: bool(s.of("complete")) or bool(s.of("fail")))
    if client_lang(scenario) == "python":
        pytest.skip("Python clients may pass args positionally")
    [fail] = scenario.of("fail", "L-args")
    assert fail.body["retryable"] is False and fail.body["error"]["type"] == "UnsupportedPayload"
