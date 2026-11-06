from __future__ import annotations

import uuid

import pytest

from pgwerk.remote import tokens


def test_round_trip_both_kinds():
    for kind in ("enrollment", "worker"):
        tid = uuid.uuid4()
        token, secret_hash = tokens.generate(kind, tid)
        parsed = tokens.parse(token)
        assert (parsed.kind, parsed.id) == (kind, str(tid))
        assert parsed.secret_hash == secret_hash
        assert tokens.kind_of(token) == kind


def test_prefixes():
    tid = uuid.uuid4()
    assert tokens.generate("enrollment", tid)[0].startswith(f"pgwk_e_{tid.hex}_")
    assert tokens.generate("worker", tid)[0].startswith(f"pgwk_w_{tid.hex}_")


def test_secret_containing_underscores_and_dashes():
    tid = uuid.uuid4()
    secret = "ab_cd-ef_gh__ij-kl_mnopqrstuv"
    parsed = tokens.parse(f"pgwk_w_{tid.hex}_{secret}")
    assert (parsed.id, parsed.secret) == (str(tid), secret)


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "Bearer xyz",
        "pgwk_x_" + "0" * 32 + "_" + "s" * 30,  # unknown kind
        "pgwk_w_" + "0" * 31 + "_" + "s" * 30,  # short id
        "pgwk_w_" + "z" * 32 + "_" + "s" * 30,  # id not hex
        "pgwk_w_" + "0" * 32 + "_short",  # secret too short
        "pgwk_w_" + "0" * 32 + "_" + "s" * 29 + "!",  # bad character
        "pgwk_w_" + "0" * 32,  # no secret
        "admin-api-token",
    ],
)
def test_rejects_malformed(bad):
    with pytest.raises(tokens.InvalidToken):
        tokens.parse(bad)


def test_kind_of_ignores_other_bearers():
    assert tokens.kind_of("some-admin-token") is None
    assert tokens.kind_of("pgwk_q_whatever") is None


def test_hash_is_sha256_of_secret():
    import hashlib

    assert tokens.hash_secret("abc") == hashlib.sha256(b"abc").digest()
