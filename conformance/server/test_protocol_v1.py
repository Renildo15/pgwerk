"""Protocol v1 server conformance. Each test cites the section of
docs/reference/remote-protocol.md it checks."""

from __future__ import annotations

import time
import uuid
import threading

from .conftest import ADMIN
from .conftest import QUEUE
from .conftest import TOKEN
from .conftest import assert_error


# --- §1 transport, §2 authentication ---------------------------------------


def test_protocol_header_is_required(remote, function):  # §1
    worker = remote.register(function)
    assert_error(
        remote.post("/claim", worker["worker_token"], {"claim_id": str(uuid.uuid4())}, protocol=False), 400, "protocol"
    )


def test_invalid_and_wrong_kind_tokens_are_unauthorized(remote, function):  # §2
    assert_error(remote.claim("not-a-token"), 401, "unauthorized")
    assert_error(remote.claim(TOKEN), 401, "unauthorized")  # enrollment token on claim
    assert_error(remote.claim(ADMIN), 401, "unauthorized")  # admin token on /remote
    worker = remote.register(function)
    assert_error(
        remote.post("/register", worker["worker_token"], {"queues": [QUEUE], "functions": [{"name": function}]}),
        401,
        "unauthorized",
    )


def test_unknown_request_fields_are_ignored(remote, function):  # §1
    resp = remote.post(
        "/register",
        TOKEN,
        {
            "queues": [QUEUE],
            "functions": [{"name": function, "x-future": 1}],
            "x-future": {"a": 1},
        },
    )
    assert resp.status_code == 201


# --- §5.1 register --------------------------------------------------------------


def test_register_returns_a_worker_identity(remote, function):
    body = remote.register(function)
    assert body["protocol"] == 1
    assert body["worker_token"].startswith("pgwk_w_") and body["worker_id"]
    assert body["lease_ttl"] > 0 and body["claim_wait"] > 0
    assert set(body["limits"]) >= {"max_payload_bytes", "max_result_bytes", "max_error_bytes"}


def test_register_outside_the_token_scope_is_scope(remote, function):
    resp = remote.post("/register", TOKEN, {"queues": [QUEUE, "not-in-scope"], "functions": [{"name": function}]})
    assert_error(resp, 403, "scope")


def test_register_without_functions_is_refused(remote):
    resp = remote.post("/register", TOKEN, {"queues": [QUEUE], "functions": []})
    assert resp.status_code in (400, 403) and resp.json()["error"]["code"] in ("bad_request", "scope")


# --- §5.2 claim ---------------------------------------------------------------------


def test_empty_claim_is_204(remote, function):
    worker = remote.register(function)
    assert remote.claim(worker["worker_token"]).status_code == 204


def test_claim_returns_a_leased_job(remote, admin, function):
    worker = remote.register(function)
    job_id = admin.enqueue(function, kwargs={"x": 1}, max_attempts=3)
    resp = remote.claim(worker["worker_token"])
    assert resp.status_code == 200
    body = resp.json()
    job = body["job"]
    assert job["id"] == job_id and job["function"] == function and job["queue"] == QUEUE
    assert job["kwargs"] == {"x": 1} and job["args"] == []
    assert (job["attempts"], job["max_attempts"], job["lost_count"]) == (1, 3, 0)
    assert body["lease_id"] and body["lease_ttl"] > 0 and 0 < body["heartbeat_every"] <= body["lease_ttl"]


def test_claim_only_returns_declared_functions(remote, admin, function):
    worker = remote.register(function)
    admin.enqueue(function + ".other")
    assert remote.claim(worker["worker_token"]).status_code == 204


def test_claim_id_retry_returns_the_same_lease_and_a_spent_id_gets_204(remote, admin, function):
    worker = remote.register(function)
    admin.enqueue(function)
    cid = str(uuid.uuid4())
    first = remote.claim(worker["worker_token"], cid).json()
    again = remote.claim(worker["worker_token"], cid).json()
    assert again["lease_id"] == first["lease_id"]
    remote.post(f"/leases/{first['lease_id']}/complete", worker["worker_token"], {"result": None})
    admin.enqueue(function)
    assert remote.claim(worker["worker_token"], cid).status_code == 204


def test_long_poll_returns_when_a_job_arrives(remote, admin, function):
    worker = remote.register(function)
    threading.Timer(1.0, lambda: admin.enqueue(function)).start()
    started = time.monotonic()
    resp = remote.claim(worker["worker_token"], wait=20)
    assert resp.status_code == 200
    assert time.monotonic() - started < 6  # woken by the enqueue, not by the 20s wait


# --- §5.3 heartbeat, §3.2 abort ------------------------------------------------------


def test_heartbeat_renews_and_accepts_progress(remote, admin, function):
    worker = remote.register(function)
    admin.enqueue(function)
    lease = remote.claim(worker["worker_token"]).json()["lease_id"]
    resp = remote.post(f"/leases/{lease}/heartbeat", worker["worker_token"], {"progress": {"pct": 10}})
    assert resp.status_code == 200 and resp.json()["abort"] is False and resp.json()["lease_expires_at"]


def test_abort_is_signalled_by_heartbeat_and_reported_as_aborted(remote, admin, function):
    worker = remote.register(function)
    job_id = admin.enqueue(function)
    lease = remote.claim(worker["worker_token"]).json()["lease_id"]
    admin.abort(job_id)
    assert remote.post(f"/leases/{lease}/heartbeat", worker["worker_token"]).json()["abort"] is True
    resp = remote.post(f"/leases/{lease}/fail", worker["worker_token"], {"aborted": True})
    assert resp.json()["status"] == "aborted"


def test_another_workers_lease_is_lease_lost(remote, admin, function):  # §3.1, §6
    a = remote.register(function)
    b = remote.register(function)
    admin.enqueue(function)
    lease = remote.claim(a["worker_token"]).json()["lease_id"]
    assert_error(remote.post(f"/leases/{lease}/heartbeat", b["worker_token"]), 409, "lease_lost")
    assert_error(remote.post(f"/leases/{lease}/complete", b["worker_token"], {"result": 1}), 409, "lease_lost")


# --- §5.4 complete, §5.5 fail, §6 idempotency -----------------------------------------


def test_complete_is_idempotent(remote, admin, function):
    worker = remote.register(function)
    job_id = admin.enqueue(function)
    lease = remote.claim(worker["worker_token"]).json()["lease_id"]
    url = f"/leases/{lease}/complete"
    assert remote.post(url, worker["worker_token"], {"result": {"v": 1}}).json() == {"status": "complete"}
    assert remote.post(url, worker["worker_token"], {"result": {"v": 2}}).json() == {"status": "complete"}
    assert admin.job(job_id)["result"] == {"v": 1}


def test_fail_retries_on_the_server_and_retryable_false_is_final(remote, admin, function):
    worker = remote.register(function)
    admin.enqueue(function, max_attempts=3)
    lease = remote.claim(worker["worker_token"]).json()["lease_id"]
    error = {"type": "ValueError", "message": "bad", "stack": "trace"}
    resp = remote.post(f"/leases/{lease}/fail", worker["worker_token"], {"error": error})
    assert resp.json()["status"] in ("queued", "scheduled")

    lease = remote.claim(worker["worker_token"], wait=5).json()["lease_id"]
    resp = remote.post(f"/leases/{lease}/fail", worker["worker_token"], {"error": error, "retryable": False})
    assert resp.json()["status"] == "failed"


def test_fail_needs_an_error_unless_aborted(remote, admin, function):
    worker = remote.register(function)
    admin.enqueue(function)
    lease = remote.claim(worker["worker_token"]).json()["lease_id"]
    resp = remote.post(f"/leases/{lease}/fail", worker["worker_token"], {"retryable": False})
    assert_error(resp, 400, "bad_request")


def test_oversized_result_is_too_large(remote, admin, function):  # §5.4, §9
    worker = remote.register(function)
    admin.enqueue(function)
    claimed = remote.claim(worker["worker_token"]).json()
    limit = worker["limits"]["max_result_bytes"]
    resp = remote.post(
        f"/leases/{claimed['lease_id']}/complete", worker["worker_token"], {"result": "x" * (limit + 10)}
    )
    assert_error(resp, 413, "too_large")


def test_report_for_a_deleted_job_is_settled(remote, admin, function):  # §6
    worker = remote.register(function)
    job_id = admin.enqueue(function)
    lease = remote.claim(worker["worker_token"]).json()["lease_id"]
    admin.delete(job_id)
    resp = remote.post(f"/leases/{lease}/complete", worker["worker_token"], {"result": 1})
    assert (resp.status_code, resp.json()) == (200, {"status": "settled"})


# --- §5.6 deregister -------------------------------------------------------------------


def test_deregister_ends_the_worker_token(remote, function):
    worker = remote.register(function)
    assert remote.post("/deregister", worker["worker_token"]).status_code == 204
    assert_error(remote.claim(worker["worker_token"]), 401, "unauthorized")
