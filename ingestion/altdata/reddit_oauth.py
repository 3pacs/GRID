"""Reddit Data API access via application-only OAuth (free tier).

Why
---
Every unauthenticated ``https://www.reddit.com/...json`` request from
grid-svr now returns ``403 Blocked`` (seen daily in grid-intelligence and
grid-scheduler logs, e.g. ``reddit_options_pulse: forbidden (403)`` and
``SmartMoney: Reddit pull failed for r/options: 403 Client Error:
Blocked``). Reddit's robots.txt is ``Disallow: /`` for all agents and its
Public Content Policy routes programmatic access through the official Data
API, which requires a registered OAuth app. So GRID must stop scraping
www.reddit.com and use ``https://oauth.reddit.com`` with an app token.

Owner setup (one-time, free, cannot be done by an agent)
--------------------------------------------------------
1. Sign in to Reddit with the account GRID should be registered under and
   create an app at https://www.reddit.com/prefs/apps (type "script";
   redirect URI can be ``http://localhost:8080``). Accept the Data API
   terms. If Reddit asks for Data API access approval for new apps, submit
   that request (non-commercial research use).
2. Put three values in grid-svr's .env (names only shown here):
   ``REDDIT_CLIENT_ID``, ``REDDIT_CLIENT_SECRET``, ``REDDIT_USERNAME``
   (the account name, used only in the User-Agent string Reddit requires).
3. Restart grid-intelligence / grid-scheduler per the normal activation
   workflow so the processes pick up the new environment.

Until those names are present, every caller gets ``RedditNotConfigured``
and reports FAILED -- no request is sent to Reddit and nothing is written.

The client uses the ``client_credentials`` grant (application-only: no user
password is ever needed or stored). Tokens live ~24h and are cached in
memory. Free-tier limit is 100 queries/minute per client; GRID's pullers
make a handful of calls per day. Secrets are never logged.
"""

from __future__ import annotations

import os
import time
from typing import Any

import requests
from loguru import logger as log

TOKEN_URL: str = "https://www.reddit.com/api/v1/access_token"
API_BASE: str = "https://oauth.reddit.com"
REQUEST_TIMEOUT_S: int = 30

ENV_CLIENT_ID: str = "REDDIT_CLIENT_ID"
ENV_CLIENT_SECRET: str = "REDDIT_CLIENT_SECRET"
ENV_USERNAME: str = "REDDIT_USERNAME"


class RedditNotConfigured(RuntimeError):
    """Raised when the Reddit OAuth credential names are absent."""


class RedditAPIError(RuntimeError):
    """Raised on a non-200 answer from the Reddit Data API."""

    def __init__(self, status: int, path: str) -> None:
        super().__init__(f"Reddit API HTTP {status} on {path}")
        self.status = status
        self.path = path


def _setting(name: str) -> str:
    """Read a credential by NAME from GRID settings, falling back to env."""
    try:
        from config import settings  # local import: keep module importable in tests

        val = getattr(settings, name, "") or ""
    except Exception:  # noqa: BLE001
        val = ""
    return (val or os.environ.get(name, "")).strip()


def reddit_credentials_configured() -> bool:
    return bool(_setting(ENV_CLIENT_ID) and _setting(ENV_CLIENT_SECRET))


class RedditAppClient:
    """Minimal application-only OAuth client for the Reddit Data API."""

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        username: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self._client_id = client_id if client_id is not None else _setting(ENV_CLIENT_ID)
        self._client_secret = (
            client_secret if client_secret is not None else _setting(ENV_CLIENT_SECRET)
        )
        if not self._client_id or not self._client_secret:
            raise RedditNotConfigured(
                f"{ENV_CLIENT_ID}/{ENV_CLIENT_SECRET} are not set; Reddit's "
                "Data API requires a registered OAuth app (see "
                "ingestion/altdata/reddit_oauth.py)."
            )
        user = username if username is not None else _setting(ENV_USERNAME)
        # Reddit's required UA format: <platform>:<app id>:<version> (by /u/<user>)
        self.user_agent = f"linux:grid-intelligence:1.0 (by /u/{user or 'unknown'})"
        self._session = session or requests.Session()
        self._token: str | None = None
        self._token_expiry: float = 0.0

    def _ensure_token(self) -> str:
        if self._token and time.monotonic() < self._token_expiry - 60:
            return self._token
        resp = self._session.post(
            TOKEN_URL,
            auth=(self._client_id, self._client_secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": self.user_agent},
            timeout=REQUEST_TIMEOUT_S,
        )
        if resp.status_code != 200:
            raise RedditAPIError(resp.status_code, "/api/v1/access_token")
        payload = resp.json()
        token = payload.get("access_token")
        if not token:
            raise RedditAPIError(resp.status_code, "/api/v1/access_token (no token)")
        self._token = str(token)
        self._token_expiry = time.monotonic() + float(payload.get("expires_in", 3600))
        log.info("reddit_oauth: obtained application token")
        return self._token

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """GET ``https://oauth.reddit.com<path>`` and return parsed JSON."""
        token = self._ensure_token()
        if not path.startswith("/"):
            path = "/" + path
        resp = self._session.get(
            f"{API_BASE}{path}",
            params={**(params or {}), "raw_json": 1},
            headers={"Authorization": f"bearer {token}", "User-Agent": self.user_agent},
            timeout=REQUEST_TIMEOUT_S,
        )
        if resp.status_code != 200:
            raise RedditAPIError(resp.status_code, path)
        return resp.json()


__all__ = [
    "RedditAppClient",
    "RedditAPIError",
    "RedditNotConfigured",
    "reddit_credentials_configured",
    "ENV_CLIENT_ID",
    "ENV_CLIENT_SECRET",
    "ENV_USERNAME",
]
