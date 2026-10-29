"""GRID Schwab Trader API market-data client (read-only).

Thin client for the Schwab Trader API app "GRID" (product: Market Data
Production, OAuth scope ``readonly``). It exposes **only** GET requests
against ``https://api.schwabapi.com/marketdata/v1``: quotes, option chains,
expiration chains, price history and market hours. There are no account,
position, order or trading methods in this module and none may be added
here; account-side endpoints belong to a separate, explicitly authorised
module if GRID ever needs them.

Operational constraints (set by the operator on 2026-10-29):

* **Weekly re-login.** Schwab refresh tokens expire 7 days after issue.
  This client refreshes the short-lived access token automatically, but it
  cannot mint a new refresh token. When the refresh token has expired the
  client raises :class:`SchwabAuthError` and the operator must re-run the
  interactive login helper (``~/dev/grid-schwab/schwab_auth.py login``),
  which rewrites the token file. Nothing in GRID performs that login.
* **No redistribution.** Schwab's terms prohibit redistributing its market
  data. Nothing returned by this client may be published to a public page
  (e.g. the GEX page at gex.stepdad.finance) or exported to a third party
  without the operator's explicit say-so. Callers that persist or display
  Schwab data must keep it behind GRID's authenticated surfaces.
* **Secrets never leave the 0600 files.** The app key, app secret, access
  token and refresh token are read from ``SCHWAB_APP_KEY`` /
  ``SCHWAB_APP_SECRET`` (or the fallback credentials file) and the token
  file. They are never logged, never included in exception messages and
  never written anywhere except the token file (rewritten atomically at
  mode 0600). Request headers are never logged.
* **No DB writes, no scheduling.** This slice is a client only. It is not
  registered in ``ingestion/scheduler.py`` and does not touch ``raw_series``.

Token file shape (written by the login helper and by :meth:`refresh`)::

    {"access_token": ..., "refresh_token": ..., "token_type": "Bearer",
     "expires_in": 1800, "scope": "api", "obtained_at": <epoch>,
     "access_expires_at": <epoch>, "refresh_expires_at": <epoch>}

Transient failures (429 / 5xx / connection errors) are retried with
exponential backoff and logged at ``warning`` level; only programming
errors reach ``log.error``.
"""

from __future__ import annotations

import json
import os
import stat
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from config import log, settings

TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
MARKET_BASE = "https://api.schwabapi.com/marketdata/v1"

#: Every path this module is allowed to request. Tests assert that the
#: public surface maps onto exactly these GET paths and nothing else.
READ_ONLY_PATHS: tuple[str, ...] = (
    "/quotes",
    "/chains",
    "/expirationchain",
    "/pricehistory",
    "/markets",
)

DEFAULT_TIMEOUT = 20.0
DEFAULT_MAX_RETRIES = 4
DEFAULT_BACKOFF_BASE = 1.0
DEFAULT_BACKOFF_CAP = 30.0
#: Refresh the access token when it expires within this many seconds.
ACCESS_SKEW_SECONDS = 60
REFRESH_TOKEN_LIFETIME_SECONDS = 7 * 24 * 3600

_CRED_KEYS = ("SCHWAB_APP_KEY", "SCHWAB_APP_SECRET")


class SchwabError(RuntimeError):
    """Base class for errors raised by this module. Never carries secrets."""


class SchwabConfigError(SchwabError):
    """App key / secret or token file not available or not private."""


class SchwabAuthError(SchwabError):
    """Refresh token rejected or expired; operator must re-run the login helper."""


class SchwabTransientError(SchwabError):
    """Retries exhausted on 429 / 5xx / network errors."""


@dataclass(frozen=True)
class SchwabQuote:
    """Parsed subset of a Schwab ``/quotes`` entry. ``raw`` keeps the full body."""

    symbol: str
    asset_main_type: str | None
    realtime: bool | None
    last_price: float | None
    bid_price: float | None
    ask_price: float | None
    bid_size: int | None
    ask_size: int | None
    total_volume: int | None
    quote_time_ms: int | None
    trade_time_ms: int | None
    raw: Mapping[str, Any]


# ---------------------------------------------------------------------------
# Credentials and token-file helpers
# ---------------------------------------------------------------------------

def _require_private(path: Path) -> None:
    """Refuse to read a secret file that is readable by group or others."""
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise SchwabConfigError(
            f"refusing to read {path}: mode {oct(mode)} is not private; run chmod 600"
        )


def _parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def load_credentials(
    app_key: str | None = None,
    app_secret: str | None = None,
    credentials_file: str | os.PathLike[str] | None = None,
) -> tuple[str, str]:
    """Resolve the app key and secret.

    Order: explicit arguments → ``settings.SCHWAB_APP_KEY`` /
    ``settings.SCHWAB_APP_SECRET`` → the fallback credentials file (default
    ``settings.SCHWAB_CREDENTIALS_FILE``, must be mode 0600). Raises
    :class:`SchwabConfigError` when neither source is filled in. The values
    are returned to the caller and never logged.
    """
    key = (app_key if app_key is not None else settings.SCHWAB_APP_KEY).strip()
    secret = (app_secret if app_secret is not None else settings.SCHWAB_APP_SECRET).strip()
    if key and secret:
        return key, secret

    cred_path = Path(
        credentials_file if credentials_file is not None else settings.SCHWAB_CREDENTIALS_FILE
    ).expanduser()
    if cred_path.exists():
        _require_private(cred_path)
        values = _parse_env_file(cred_path)
        key = key or values.get("SCHWAB_APP_KEY", "").strip()
        secret = secret or values.get("SCHWAB_APP_SECRET", "").strip()
        if key.startswith("PASTE_"):
            key = ""
        if secret.startswith("PASTE_"):
            secret = ""
        if key and secret:
            return key, secret

    raise SchwabConfigError(
        "Schwab app key/secret not configured: set SCHWAB_APP_KEY and "
        f"SCHWAB_APP_SECRET or fill in {cred_path}"
    )


def is_configured() -> bool:
    """True when credentials and a token file are available; warns (no crash) otherwise."""
    try:
        load_credentials()
    except SchwabConfigError as exc:
        log.warning("Schwab market data disabled: {msg}", msg=str(exc))
        return False
    token_path = Path(settings.SCHWAB_TOKEN_FILE).expanduser()
    if not token_path.exists():
        log.warning(
            "Schwab market data disabled: token file {path} missing (run the login helper)",
            path=str(token_path),
        )
        return False
    return True


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class SchwabMarketClient:
    """Read-only Schwab market-data client with transparent access-token refresh.

    Parameters are injectable for tests: ``session`` must offer
    ``get``/``post`` with the :mod:`requests` signature, ``sleep`` and
    ``now`` replace :func:`time.sleep` / :func:`time.time`.
    """

    def __init__(
        self,
        *,
        app_key: str | None = None,
        app_secret: str | None = None,
        token_file: str | os.PathLike[str] | None = None,
        credentials_file: str | os.PathLike[str] | None = None,
        session: Any | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        backoff_cap: float = DEFAULT_BACKOFF_CAP,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], float] = time.time,
    ) -> None:
        self._app_key, self._app_secret = load_credentials(
            app_key, app_secret, credentials_file
        )
        self._token_file = Path(
            token_file if token_file is not None else settings.SCHWAB_TOKEN_FILE
        ).expanduser()
        self._session = session if session is not None else requests.Session()
        self._timeout = timeout
        self._max_retries = max(0, int(max_retries))
        self._backoff_base = backoff_base
        self._backoff_cap = backoff_cap
        self._sleep = sleep
        self._now = now
        self._tokens: dict[str, Any] | None = None

    # -- token file ---------------------------------------------------------

    def _load_tokens(self) -> dict[str, Any]:
        if not self._token_file.exists():
            raise SchwabConfigError(
                f"Schwab token file {self._token_file} missing; run the login helper"
            )
        _require_private(self._token_file)
        try:
            data = json.loads(self._token_file.read_text(encoding="utf-8"))
        except ValueError as exc:  # corrupt file; message carries no secret
            raise SchwabConfigError(
                f"Schwab token file {self._token_file} is not valid JSON"
            ) from exc
        if not isinstance(data, dict) or "access_token" not in data:
            raise SchwabConfigError(
                f"Schwab token file {self._token_file} has no access_token"
            )
        self._tokens = data
        return data

    def _write_tokens(self, payload: dict[str, Any]) -> None:
        """Atomically replace the token file at mode 0600."""
        tmp = self._token_file.with_name(self._token_file.name + ".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        os.replace(tmp, self._token_file)
        os.chmod(self._token_file, 0o600)
        self._tokens = payload

    def token_status(self) -> dict[str, Any]:
        """Non-secret token status: expiry epochs, scope, validity flags."""
        t = self._tokens or self._load_tokens()
        now = self._now()
        return {
            "token_file": str(self._token_file),
            "scope": t.get("scope"),
            "access_expires_at": t.get("access_expires_at"),
            "refresh_expires_at": t.get("refresh_expires_at"),
            "access_valid": float(t.get("access_expires_at") or 0) > now,
            "refresh_valid": float(t.get("refresh_expires_at") or 0) > now,
        }

    # -- OAuth refresh ------------------------------------------------------

    def refresh(self) -> None:
        """Exchange the stored refresh token for a new access token and persist it."""
        old = self._tokens or self._load_tokens()
        refresh_token = old.get("refresh_token")
        if not refresh_token:
            raise SchwabAuthError("no refresh token stored; re-run the login helper")
        refresh_expires_at = float(old.get("refresh_expires_at") or 0)
        if refresh_expires_at and refresh_expires_at <= self._now():
            raise SchwabAuthError(
                "Schwab refresh token expired (7-day lifetime); re-run the login helper"
            )

        try:
            resp = self._session.post(
                TOKEN_URL,
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                auth=HTTPBasicAuth(self._app_key, self._app_secret),
                headers={"Accept": "application/json"},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            log.warning("Schwab token refresh network error: {err}", err=type(exc).__name__)
            raise SchwabTransientError("Schwab token refresh failed (network)") from exc

        status = int(resp.status_code)
        if status in (400, 401):
            log.warning(
                "Schwab token refresh rejected with HTTP {status}; weekly login required",
                status=status,
            )
            raise SchwabAuthError(
                f"Schwab refresh token rejected (HTTP {status}); re-run the login helper"
            )
        if status != 200:
            log.warning("Schwab token refresh returned HTTP {status}", status=status)
            raise SchwabTransientError(f"Schwab token refresh returned HTTP {status}")

        payload = dict(resp.json())
        if "access_token" not in payload:
            raise SchwabAuthError("Schwab token refresh response had no access_token")
        now = int(self._now())
        payload["obtained_at"] = now
        payload["access_expires_at"] = now + int(payload.get("expires_in", 1800))
        if payload.get("refresh_token"):
            payload["refresh_expires_at"] = now + REFRESH_TOKEN_LIFETIME_SECONDS
        else:
            payload["refresh_token"] = refresh_token
            payload["refresh_expires_at"] = old.get("refresh_expires_at")
        self._write_tokens(payload)
        log.info("Schwab access token refreshed (expires in {s}s)", s=payload["expires_in"])

    def _access_token(self, force_refresh: bool = False) -> str:
        t = self._tokens or self._load_tokens()
        expires_at = float(t.get("access_expires_at") or 0)
        if force_refresh or expires_at - ACCESS_SKEW_SECONDS <= self._now():
            self.refresh()
            t = self._tokens or self._load_tokens()
        return str(t["access_token"])

    # -- HTTP ---------------------------------------------------------------

    def _backoff(self, attempt: int, retry_after: str | None) -> float:
        if retry_after:
            try:
                return min(float(retry_after), self._backoff_cap)
            except ValueError:
                pass
        return min(self._backoff_base * (2 ** attempt), self._backoff_cap)

    def _get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        """GET ``MARKET_BASE + path`` with bearer auth, 401 re-auth and backoff."""
        if path not in READ_ONLY_PATHS:
            raise SchwabError(f"path {path!r} is not a read-only market-data path")
        url = MARKET_BASE + path
        reauthed = False
        attempt = 0
        while True:
            token = self._access_token()
            try:
                resp = self._session.get(
                    url,
                    params=dict(params or {}),
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=self._timeout,
                )
            except requests.RequestException as exc:
                if attempt >= self._max_retries:
                    raise SchwabTransientError(
                        f"Schwab GET {path} failed after {attempt + 1} attempts (network)"
                    ) from exc
                delay = self._backoff(attempt, None)
                log.warning(
                    "Schwab GET {path} network error ({err}); retry {n} in {d:.1f}s",
                    path=path, err=type(exc).__name__, n=attempt + 1, d=delay,
                )
                self._sleep(delay)
                attempt += 1
                continue

            status = int(resp.status_code)
            if status == 200:
                return resp.json()
            if status == 401 and not reauthed:
                reauthed = True
                log.warning("Schwab GET {path} returned 401; refreshing access token once", path=path)
                self._access_token(force_refresh=True)
                continue
            if status == 429 or 500 <= status < 600:
                if attempt >= self._max_retries:
                    raise SchwabTransientError(
                        f"Schwab GET {path} returned HTTP {status} after {attempt + 1} attempts"
                    )
                delay = self._backoff(attempt, resp.headers.get("Retry-After"))
                log.warning(
                    "Schwab GET {path} returned HTTP {status}; retry {n} in {d:.1f}s",
                    path=path, status=status, n=attempt + 1, d=delay,
                )
                self._sleep(delay)
                attempt += 1
                continue
            # 4xx other than 401/429: caller error, no retry. Body is not a secret
            # but keep it short; never echo headers.
            raise SchwabError(f"Schwab GET {path} returned HTTP {status}")

    # -- public read-only surface -----------------------------------------

    def quotes(
        self, symbols: str | Iterable[str], fields: str | None = "quote"
    ) -> dict[str, SchwabQuote]:
        """GET /quotes for one or more symbols, parsed into :class:`SchwabQuote`."""
        if isinstance(symbols, str):
            symbol_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
        else:
            symbol_list = [str(s).strip().upper() for s in symbols if str(s).strip()]
        if not symbol_list:
            return {}
        params: dict[str, Any] = {"symbols": ",".join(symbol_list)}
        if fields:
            params["fields"] = fields
        body = self._get("/quotes", params)
        return parse_quotes(body)

    def option_chain(self, symbol: str, **params: Any) -> dict[str, Any]:
        """GET /chains for ``symbol``; extra kwargs are passed through as query params
        (``contractType``, ``strikeCount``, ``fromDate``, ``toDate``, ``strategy`` ...)."""
        query: dict[str, Any] = {"symbol": symbol.strip().upper()}
        query.update(params)
        return self._get("/chains", query)

    def expiration_chain(self, symbol: str) -> dict[str, Any]:
        """GET /expirationchain for ``symbol``."""
        return self._get("/expirationchain", {"symbol": symbol.strip().upper()})

    def price_history(self, symbol: str, **params: Any) -> dict[str, Any]:
        """GET /pricehistory for ``symbol``; extra kwargs are query params
        (``periodType``, ``period``, ``frequencyType``, ``frequency``,
        ``startDate``, ``endDate``, ``needExtendedHoursData`` ...)."""
        query: dict[str, Any] = {"symbol": symbol.strip().upper()}
        query.update(params)
        return self._get("/pricehistory", query)

    def market_hours(
        self, markets: str | Iterable[str], date: str | None = None
    ) -> dict[str, Any]:
        """GET /markets for one or more market ids (equity, option, bond, future, forex)."""
        if isinstance(markets, str):
            market_list = [m.strip().lower() for m in markets.split(",") if m.strip()]
        else:
            market_list = [str(m).strip().lower() for m in markets if str(m).strip()]
        params: dict[str, Any] = {"markets": ",".join(market_list)}
        if date:
            params["date"] = date
        return self._get("/markets", params)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

def _as_float(v: Any) -> float | None:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _as_int(v: Any) -> int | None:
    try:
        return None if v is None else int(v)
    except (TypeError, ValueError):
        return None


def parse_quotes(body: Mapping[str, Any]) -> dict[str, SchwabQuote]:
    """Parse the ``{"SPY": {"quote": {...}, ...}, ...}`` body of GET /quotes.

    Symbols Schwab could not resolve come back under an ``errors`` key and
    are skipped with a warning; everything else must carry a ``quote`` map.
    """
    out: dict[str, SchwabQuote] = {}
    if not isinstance(body, Mapping):
        return out
    errors = body.get("errors")
    if errors:
        log.warning("Schwab quotes returned errors for some symbols: {e}", e=errors)
    for symbol, entry in body.items():
        if symbol == "errors" or not isinstance(entry, Mapping):
            continue
        q = entry.get("quote") or {}
        if not isinstance(q, Mapping):
            q = {}
        out[str(symbol)] = SchwabQuote(
            symbol=str(entry.get("symbol") or symbol),
            asset_main_type=entry.get("assetMainType"),
            realtime=entry.get("realtime"),
            last_price=_as_float(q.get("lastPrice")),
            bid_price=_as_float(q.get("bidPrice")),
            ask_price=_as_float(q.get("askPrice")),
            bid_size=_as_int(q.get("bidSize")),
            ask_size=_as_int(q.get("askSize")),
            total_volume=_as_int(q.get("totalVolume")),
            quote_time_ms=_as_int(q.get("quoteTime")),
            trade_time_ms=_as_int(q.get("tradeTime")),
            raw=entry,
        )
    return out


__all__ = [
    "MARKET_BASE",
    "READ_ONLY_PATHS",
    "TOKEN_URL",
    "SchwabAuthError",
    "SchwabConfigError",
    "SchwabError",
    "SchwabMarketClient",
    "SchwabQuote",
    "SchwabTransientError",
    "is_configured",
    "load_credentials",
    "parse_quotes",
]
