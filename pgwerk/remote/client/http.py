"""Thin HTTP layer of the remote client: protocol header, bearer auth, error decoding."""

from __future__ import annotations

from typing import Any

import httpx


class ProtocolError(Exception):
    """The server answered with a protocol error (``{"error": {"code", "message"}}``)."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message

    @property
    def retryable(self) -> bool:
        """Worth retrying the same request: rate limited or a server-side error."""
        return self.status == 429 or self.status >= 500


class ProtocolClient:
    def __init__(self, url: str, *, insecure: bool = False, transport: httpx.AsyncBaseTransport | None = None) -> None:
        if url.startswith("http://") and not insecure:
            raise ValueError("refusing a plain http:// URL; use https://, or insecure=True for local testing")
        self._http = httpx.AsyncClient(
            base_url=url.rstrip("/") + "/remote/v1",
            headers={"Pgwerk-Protocol": "1"},
            transport=transport,
            timeout=httpx.Timeout(30.0),
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def call(
        self, path: str, token: str, body: dict[str, Any] | None = None, *, timeout: float | None = None
    ) -> tuple[int, Any]:
        """POST to *path*. Returns ``(status, json_or_None)``; raises :class:`ProtocolError` on 4xx/5xx."""
        resp = await self._http.post(
            path,
            json=body or {},
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout if timeout is not None else httpx.USE_CLIENT_DEFAULT,
        )
        if resp.status_code >= 400:
            try:
                err = resp.json()["error"]
                code, message = err["code"], err.get("message", "")
            except Exception:
                code, message = "http_error", resp.text[:200]
            raise ProtocolError(resp.status_code, code, message)
        if resp.status_code == 204 or not resp.content:
            return resp.status_code, None
        return resp.status_code, resp.json()
