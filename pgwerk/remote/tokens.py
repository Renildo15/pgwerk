"""Remote-worker tokens: format, parsing and hashing.

Two kinds, both opaque bearer strings:

- **enrollment** ``pgwk_e_<id>_<secret>``: long-lived, created by an operator for a
  machine or fleet, scoped to a list of queues. Accepted only by ``register``.
- **worker** ``pgwk_w_<worker_id>_<secret>``: issued by ``register`` to one worker
  process. Accepted by every other endpoint, and it stops working when the worker
  deregisters, is revoked, or expires idle.

Only ``sha256(secret)`` is stored. A worker token is checked by an equality on that
hash inside the same SQL statement as the write it authorizes: comparing hashes
doesn't leak the secret through timing, because an attacker can't steer the hash
byte by byte, and doing it in the write's statement means revocation applies on the
very next call (no cache). The enrollment token is looked up by id and its hash
compared with :func:`hmac.compare_digest`.
"""

from __future__ import annotations

import re
import uuid
import hashlib
import secrets

from typing import Literal
from dataclasses import dataclass


TokenKind = Literal["enrollment", "worker"]

_PREFIX = "pgwk"
_KIND_CODES: dict[str, TokenKind] = {"e": "enrollment", "w": "worker"}
_CODES: dict[TokenKind, str] = {v: k for k, v in _KIND_CODES.items()}
_SECRET_RE = re.compile(r"^[A-Za-z0-9_-]{20,}$")


class InvalidToken(ValueError):
    """The bearer string isn't a well-formed pgwerk remote token."""


@dataclass(frozen=True)
class ParsedToken:
    kind: TokenKind
    id: str
    """The enrollment token's id, or the worker's id, as a canonical UUID string."""
    secret: str

    @property
    def secret_hash(self) -> bytes:
        return hash_secret(self.secret)


def hash_secret(secret: str) -> bytes:
    return hashlib.sha256(secret.encode()).digest()


def generate(kind: TokenKind, token_id: str | uuid.UUID) -> tuple[str, bytes]:
    """Mint a token for *token_id*. Returns ``(token, secret_hash)``.

    The token is shown once; only the hash is stored.
    """
    secret = secrets.token_urlsafe(32)
    tid = uuid.UUID(str(token_id))
    return f"{_PREFIX}_{_CODES[kind]}_{tid.hex}_{secret}", hash_secret(secret)


def parse(token: str) -> ParsedToken:
    """Parse a bearer string strictly. Raises :class:`InvalidToken` on anything malformed.

    The secret's alphabet includes ``_``, so the string is split at most three times
    and everything after the id is the secret.
    """
    parts = token.split("_", 3)
    if len(parts) != 4 or parts[0] != _PREFIX or parts[1] not in _KIND_CODES:
        raise InvalidToken("malformed token")
    _, code, raw_id, secret = parts
    if len(raw_id) != 32:
        raise InvalidToken("malformed token")
    try:
        token_id = str(uuid.UUID(hex=raw_id))
    except ValueError:
        raise InvalidToken("malformed token") from None
    if not _SECRET_RE.match(secret):
        raise InvalidToken("malformed token")
    return ParsedToken(kind=_KIND_CODES[code], id=token_id, secret=secret)


def kind_of(bearer: str) -> TokenKind | None:
    """The kind a bearer string claims to be, from its prefix alone (no validation)."""
    for code, kind in _KIND_CODES.items():
        if bearer.startswith(f"{_PREFIX}_{code}_"):
            return kind
    return None
