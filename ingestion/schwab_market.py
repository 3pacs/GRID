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
  file. They are never logged, never included in exception messages or
  chained causes, and never written anywhere except the token file. Request
  headers are never logged.
* **No DB writes, no scheduling.** This slice is a client only. It is not
  registered in ``ingestion/scheduler.py`` and does not touch ``raw_series``.

Secret-file handling (POSIX only; the client refuses to run where
``fcntl`` is unavailable because the guarantees below cannot be met):

* Every secret file is opened once with ``O_NOFOLLOW`` and validated on the
  open descriptor (``fstat``): regular file, owned by the effective uid, no
  group/other permission bits. The data is read from that same descriptor,
  so there is no check-then-read window. Symlinks, FIFOs and foreign-owned
  files are refused. The parent directory must be owned by the effective
  uid and not group/other writable.
* The token file is rewritten through a uniquely named temporary file
  created exclusively (``mkstemp``, 0600) in the same directory, fsynced and
  then atomically renamed over the destination. A destination that is not a
  regular file (e.g. a symlink) is refused.
* Read-refresh-write runs under an exclusive ``flock`` on
  ``<token file>.lock`` so several clients or processes sharing one token
  file never refresh concurrently; after taking the lock the file is
  re-read and a refresh another holder already performed is reused.
* ``refresh_expires_at`` is only extended when the provider returns a
  refresh token that differs from the stored one; an unchanged or omitted
  refresh token keeps its existing expiry.

Token file shape (written by the login helper and by :meth:`refresh`)::

    {"access_token": ..., "refresh_token": ..., "token_type": "Bearer",
     "expires_in": 1800, "scope": "api", "obtained_at": <epoch>,
     "access_expires_at": <epoch>, "refresh_expires_at": <epoch>}

Transient failures (429 / 5xx / connection errors) are retried with
exponential backoff and logged at ``warning`` level with the exception
class name only; only programming errors reach ``log.error``.
"""

from __future__ import annotations

import errno
import json
import os
import stat
import tempfile
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

import requests
from requests.auth import HTTPBasicAuth

from config import log, settings

try:  # POSIX advisory locks; absent on Windows.
    import fcntl
except ImportError:  # pragma: no cover - exercised only on non-POSIX hosts
    fcntl = None  # type: ignore[assignment]

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
#: Used when the token response omits or mangles ``expires_in``.
DEFAULT_ACCESS_LIFETIME_SECONDS = 1800
REFRESH_TOKEN_LIFETIME_SECONDS = 7 * 24 * 3600
#: The only ``errors`` categories of GET /quotes whose *names* may be logged
#: (documented Schwab shape). Any other key is counted, never echoed, so a
#: provider- or request-controlled key cannot reach the log.
ERROR_CATEGORY_ALLOWLIST: tuple[str, ...] = (
    "invalidSymbols",
    "invalidCusips",
    "invalidSSIDs",
)

_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_CLOEXEC = getattr(os, "O_CLOEXEC", 0)
_O_NONBLOCK = getattr(os, "O_NONBLOCK", 0)


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
# Private-file primitives
# ---------------------------------------------------------------------------

def _expected_uid() -> int | None:
    return os.geteuid() if hasattr(os, "geteuid") else None


def _require_posix() -> None:
    if fcntl is None:
        raise SchwabConfigError(
            "Schwab client requires a POSIX host (fcntl locks and 0600 semantics)"
        )


def _check_private_dir(directory: Path) -> None:
    """The directory holding secret files must be ours and not group/other writable."""
    try:
        st = os.lstat(directory)
    except OSError as exc:
        raise SchwabConfigError(
            f"cannot stat directory {directory}: {exc.strerror}"
        ) from None
    if not stat.S_ISDIR(st.st_mode):
        raise SchwabConfigError(f"refusing: {directory} is not a directory")
    uid = _expected_uid()
    if uid is not None and st.st_uid != uid:
        raise SchwabConfigError(
            f"refusing: directory {directory} is owned by uid {st.st_uid}, expected {uid}"
        )
    if stat.S_IMODE(st.st_mode) & 0o022:
        raise SchwabConfigError(
            f"refusing: directory {directory} is group/other writable "
            f"(mode {oct(stat.S_IMODE(st.st_mode))})"
        )


def _check_private_fd(fd: int, path: Path) -> os.stat_result:
    """Validate an *open* descriptor: regular file, owned by us, mode 0600 or tighter."""
    st = os.fstat(fd)
    if not stat.S_ISREG(st.st_mode):
        raise SchwabConfigError(f"refusing to read {path}: not a regular file")
    uid = _expected_uid()
    if uid is not None and st.st_uid != uid:
        raise SchwabConfigError(
            f"refusing to read {path}: owned by uid {st.st_uid}, expected {uid}"
        )
    mode = stat.S_IMODE(st.st_mode)
    if mode & 0o077:
        raise SchwabConfigError(
            f"refusing to read {path}: mode {oct(mode)} is not private; run chmod 600"
        )
    return st


def _read_private(path: Path) -> str:
    """Open ``path`` once without following links, validate the descriptor, read it.

    The check and the read use the same descriptor, so a swap of the path
    between them cannot change what is validated.
    """
    _require_posix()
    _check_private_dir(path.parent)
    try:
        fd = os.open(path, os.O_RDONLY | _O_NOFOLLOW | _O_CLOEXEC | _O_NONBLOCK)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise SchwabConfigError(
                f"refusing to read {path}: it is a symbolic link"
            ) from None
        if exc.errno == errno.ENOENT:
            raise FileNotFoundError(str(path)) from None
        raise SchwabConfigError(f"cannot open {path}: {exc.strerror}") from None
    try:
        _check_private_fd(fd, path)
    except BaseException:
        os.close(fd)
        raise
    with os.fdopen(fd, "r", encoding="utf-8") as fh:
        return fh.read()


class _TokenFileLock:
    """Exclusive advisory lock on ``<token file>.lock`` (same directory, 0600)."""

    def __init__(self, token_file: Path) -> None:
        self.path = token_file.with_name(token_file.name + ".lock")
        self._fd: int | None = None

    def __enter__(self) -> Self:
        _require_posix()
        _check_private_dir(self.path.parent)
        try:
            fd = os.open(
                self.path,
                os.O_RDWR | os.O_CREAT | _O_NOFOLLOW | _O_CLOEXEC,
                0o600,
            )
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise SchwabConfigError(
                    f"refusing: lock file {self.path} is a symbolic link"
                ) from None
            raise SchwabConfigError(
                f"cannot open lock file {self.path}: {exc.strerror}"
            ) from None
        try:
            st = os.fstat(fd)
            if not stat.S_ISREG(st.st_mode):
                raise SchwabConfigError(f"refusing: lock file {self.path} is not a regular file")
            uid = _expected_uid()
            if uid is not None and st.st_uid != uid:
                raise SchwabConfigError(
                    f"refusing: lock file {self.path} is owned by uid {st.st_uid}"
                )
            if stat.S_IMODE(st.st_mode) & 0o077:
                os.fchmod(fd, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX)
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd
        return self

    def __exit__(self, *exc: object) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def _parse_env_file(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in text.splitlines():
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
    ``settings.SCHWAB_CREDENTIALS_FILE``; must be a regular file owned by us
    at mode 0600, reached without following a symlink). Raises
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
    try:
        text = _read_private(cred_path)
    except FileNotFoundError:
        text = None
    if text is not None:
        values = _parse_env_file(text)
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

    def _lock(self) -> _TokenFileLock:
        return _TokenFileLock(self._token_file)

    def _load_tokens(self) -> dict[str, Any]:
        try:
            text = _read_private(self._token_file)
        except FileNotFoundError:
            raise SchwabConfigError(
                f"Schwab token file {self._token_file} missing; run the login helper"
            ) from None
        try:
            data = json.loads(text)
        except ValueError:  # corrupt file; message carries no secret
            raise SchwabConfigError(
                f"Schwab token file {self._token_file} is not valid JSON"
            ) from None
        if not isinstance(data, dict) or "access_token" not in data:
            raise SchwabConfigError(
                f"Schwab token file {self._token_file} has no access_token"
            )
        self._tokens = data
        return data

    def _write_tokens(self, payload: dict[str, Any]) -> None:
        """Atomically replace the token file via an exclusive 0600 temp file."""
        _require_posix()
        directory = self._token_file.parent
        _check_private_dir(directory)
        try:
            dst = os.lstat(self._token_file)
        except FileNotFoundError:
            dst = None
        if dst is not None and not stat.S_ISREG(dst.st_mode):
            raise SchwabConfigError(
                f"refusing to replace {self._token_file}: not a regular file"
            )
        fd, tmp_name = tempfile.mkstemp(prefix=".tokens-", suffix=".tmp", dir=directory)
        tmp = Path(tmp_name)
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._token_file)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
        try:
            dfd = os.open(directory, os.O_RDONLY | _O_CLOEXEC)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
        self._tokens = payload

    def token_status(self) -> dict[str, Any]:
        """Non-secret token status: expiry epochs, scope, validity flags."""
        t = self._load_tokens()
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
        with self._lock():
            self._refresh_locked(self._load_tokens())

    def _refresh_locked(self, old: dict[str, Any]) -> None:
        """Perform the refresh. Caller must hold the token-file lock."""
        old_refresh = old.get("refresh_token")
        if not old_refresh:
            raise SchwabAuthError("no refresh token stored; re-run the login helper")
        refresh_expires_at = float(old.get("refresh_expires_at") or 0)
        if refresh_expires_at and refresh_expires_at <= self._now():
            raise SchwabAuthError(
                "Schwab refresh token expired (7-day lifetime); re-run the login helper"
            )

        try:
            resp = self._session.post(
                TOKEN_URL,
                data={"grant_type": "refresh_token", "refresh_token": old_refresh},
                auth=HTTPBasicAuth(self._app_key, self._app_secret),
                headers={"Accept": "application/json"},
                timeout=self._timeout,
            )
        except requests.RequestException as exc:
            # ``from None``: the transport exception text may echo request
            # details and must not surface in tracebacks.
            log.warning("Schwab token refresh network error: {err}", err=type(exc).__name__)
            raise SchwabTransientError("Schwab token refresh failed (network)") from None

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

        try:
            body = resp.json()
        except ValueError:
            raise SchwabTransientError("Schwab token refresh returned non-JSON body") from None
        if not isinstance(body, Mapping) or not body.get("access_token"):
            raise SchwabAuthError("Schwab token refresh response had no access_token")

        # Normalise everything *before* persisting so nothing can fail after
        # the file has changed.
        payload: dict[str, Any] = dict(body)
        expires_in = _normalise_expires_in(payload.get("expires_in"))
        payload["expires_in"] = expires_in
        now = int(self._now())
        payload["obtained_at"] = now
        payload["access_expires_at"] = now + expires_in
        new_refresh = payload.get("refresh_token")
        rotated = bool(new_refresh) and new_refresh != old_refresh
        if rotated:
            payload["refresh_expires_at"] = now + REFRESH_TOKEN_LIFETIME_SECONDS
        else:
            payload["refresh_token"] = old_refresh
            payload["refresh_expires_at"] = old.get("refresh_expires_at")
        self._write_tokens(payload)
        log.info(
            "Schwab access token refreshed (expires in {s}s, refresh token rotated={r})",
            s=expires_in, r=rotated,
        )

    def _access_token(self, stale_token: str | None = None) -> str:
        """Return a usable access token, refreshing under the file lock if needed.

        ``stale_token`` is the token a request just got a 401 for: the file
        is re-read under the lock and a refresh is only performed if the
        stored token is still that one (another holder may have refreshed).
        """
        with self._lock():
            t = self._load_tokens()
            expires_at = float(t.get("access_expires_at") or 0)
            expiring = expires_at - ACCESS_SKEW_SECONDS <= self._now()
            still_stale = stale_token is not None and t.get("access_token") == stale_token
            if expiring or still_stale:
                self._refresh_locked(t)
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
        stale: str | None = None
        while True:
            token = self._access_token(stale_token=stale)
            stale = None
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
                        f"Schwab GET {path} failed after {attempt + 1} attempts "
                        f"(network: {type(exc).__name__})"
                    ) from None
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
                try:
                    return resp.json()
                except ValueError:
                    raise SchwabTransientError(
                        f"Schwab GET {path} returned a non-JSON body"
                    ) from None
            if status == 401 and not reauthed:
                reauthed = True
                stale = token
                log.warning("Schwab GET {path} returned 401; refreshing access token once", path=path)
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
            # Other 4xx: caller error, no retry. Never echo the body or headers.
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

def _normalise_expires_in(value: Any) -> int:
    try:
        seconds = int(value) if value is not None else DEFAULT_ACCESS_LIFETIME_SECONDS
    except (TypeError, ValueError):
        seconds = DEFAULT_ACCESS_LIFETIME_SECONDS
    return seconds if seconds > 0 else DEFAULT_ACCESS_LIFETIME_SECONDS


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


def _summarise_errors(errors: Any) -> str:
    """Bounded summary of a provider ``errors`` object for logging.

    Emits only counts. The only names that appear are the fixed literals in
    :data:`ERROR_CATEGORY_ALLOWLIST`; every other key is folded into an
    ``other_keys`` count. Neither keys outside the allowlist nor any value
    is ever echoed, so provider- or request-controlled text cannot reach
    the log.
    """
    if isinstance(errors, Mapping):
        parts: list[str] = []
        other = 0
        for key, value in errors.items():
            if key in ERROR_CATEGORY_ALLOWLIST:
                count = len(value) if isinstance(value, (list, tuple, Mapping, str)) else 1
                parts.append(f"{key}={count}")
            else:
                other += 1
        if other:
            parts.append(f"other_keys={other}")
        return ", ".join(parts) or "empty"
    if isinstance(errors, (list, tuple)):
        return f"list={len(errors)}"
    return "unrecognised"


def parse_quotes(body: Mapping[str, Any]) -> dict[str, SchwabQuote]:
    """Parse the ``{"SPY": {"quote": {...}, ...}, ...}`` body of GET /quotes.

    Symbols Schwab could not resolve come back under an ``errors`` key and
    are skipped with a bounded warning; everything else must carry a
    ``quote`` map.
    """
    out: dict[str, SchwabQuote] = {}
    if not isinstance(body, Mapping):
        return out
    errors = body.get("errors")
    if errors:
        log.warning("Schwab quotes returned errors: {e}", e=_summarise_errors(errors))
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
    "ERROR_CATEGORY_ALLOWLIST",
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
