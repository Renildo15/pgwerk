"""Client conformance harness: run the client under test against a scripted fake server.

The client is started as a subprocess with ``URL`` and ``TOKEN`` in its environment. It
must consume the queue ``conformance`` and implement:

    conformance.echo(**kwargs)      returns its kwargs
    conformance.sleep(seconds)      sleeps, returns null (should be cancellable)
    conformance.fail(message)       fails with that message (a retryable error)
    conformance.big(size)           returns a string of `size` characters

PGWERK_CONFORMANCE_CLIENT sets the command; it defaults to the Python RemoteWorker.
"""

from __future__ import annotations

import os
import sys
import time
import shlex
import uuid
import signal
import subprocess

from typing import Callable
from pathlib import Path

import pytest

from .fake_server import Scenario
from .fake_server import FakeServer


ROOT = Path(__file__).resolve().parents[2]
COMMAND = shlex.split(os.environ.get("PGWERK_CONFORMANCE_CLIENT", "")) or [
    sys.executable,
    "-m",
    "conformance.client.python_client",
]
ENROLLMENT = f"pgwk_e_{uuid.uuid4().hex}_{'e' * 43}"


def run_client(scenario: Scenario, until: Callable[[Scenario], bool], timeout: float = 15.0) -> subprocess.Popen:
    """Run the client until ``until(scenario)`` holds (or *timeout*), then SIGTERM it."""
    with FakeServer(scenario) as server:
        env = {**os.environ, "URL": server.url, "TOKEN": ENROLLMENT, "PYTHONPATH": str(ROOT)}
        proc = subprocess.Popen(COMMAND, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + timeout
        try:
            while time.monotonic() < deadline and proc.poll() is None and not until(scenario):
                time.sleep(0.05)
        finally:
            if proc.poll() is None:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(10)
                except subprocess.TimeoutExpired:
                    proc.kill()
        return proc


def client_lang(scenario: Scenario) -> str:
    register = scenario.of("register")
    return (register[0].body or {}).get("client", {}).get("lang", "") if register else ""


@pytest.fixture
def scenario() -> Scenario:
    return Scenario()
