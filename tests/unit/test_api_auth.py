from __future__ import annotations

import base64

from types import SimpleNamespace

import pytest

from litestar.exceptions import NotAuthorizedException

from pgwerk.api.auth import make_bearer_guard
from pgwerk.api.auth import make_basic_auth_guard


def _conn(authorization: str | None = None) -> SimpleNamespace:
    headers = {} if authorization is None else {"Authorization": authorization}
    return SimpleNamespace(headers=headers)


def _basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


class TestBearerGuard:
    def test_accepts_correct_token(self):
        make_bearer_guard("secret")(_conn("Bearer secret"), None)

    @pytest.mark.parametrize(
        "header",
        [
            None,
            "",
            "Bearer wrong!",
            "Bearer secre",
            "Bearer secret-and-more",
            "secret",
            "Basic secret",
            "Bearer séçret",
        ],
    )
    def test_rejects_wrong_or_missing_token(self, header):
        with pytest.raises(NotAuthorizedException):
            make_bearer_guard("secret")(_conn(header), None)


class TestBasicAuthGuard:
    def test_accepts_correct_credentials(self):
        make_basic_auth_guard("admin", "pw")(_conn(_basic("admin", "pw")), None)

    @pytest.mark.parametrize(
        "header",
        [None, "", _basic("admin", "wrong"), _basic("admin", "p"), "Bearer xyz", "Basic ", "Basic çç"],
    )
    def test_rejects_wrong_or_missing_credentials(self, header):
        with pytest.raises(NotAuthorizedException):
            make_basic_auth_guard("admin", "pw")(_conn(header), None)
