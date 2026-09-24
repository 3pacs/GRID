"""
Robinhood Crypto trading integration (official key-signed API).

Robinhood's one programmatic trading surface is the Crypto Trading API
(https://docs.robinhood.com/crypto/trading). Every request carries an API key
and an Ed25519 signature over ``api_key + timestamp + path + method + body``.
There is no official equities API, so this connector is crypto-spot only and
never touches the brokerage login or MFA flow.

Architecture (mirrors trading/hyperliquid.py):
  GRID Signal -> Position Sizing -> Robinhood order -> Confirmation -> Journal

Safety rails:
  * ``ROBINHOOD_LIVE_TRADING`` defaults to False. Orders are built, sized and
    risk-checked exactly as live, then returned with ``status="dry_run"``
    instead of being sent. Read endpoints work as soon as keys are set.
  * Per-order notional cap (``ROBINHOOD_MAX_POSITION_USD``) and an equity
    drawdown halt (``ROBINHOOD_MAX_DRAWDOWN_PCT``), same as Hyperliquid.
  * Drawdown high-water mark, start-of-day equity and the order-rate counter
    are PERSISTED (see ``trading/robinhood_risk_store.py``) rather than kept
    on this object — ``get_robinhood_trader()`` builds a fresh instance on
    every call, so in-memory state used to reset (and the drawdown halt could
    never trip) every request. A ``RiskStore`` is the only thing that fixes
    that; see that module's docstring.
  * Daily loss cap (``ROBINHOOD_MAX_DAILY_LOSS_PCT``) blocks new BUYS once
    today's loss from start-of-day equity is reached; sells/closes stay
    allowed so a position can still be de-risked.
  * Order-rate cap (``ROBINHOOD_MAX_ORDERS_PER_DAY``) blocks new BUYS once
    today's opened-order count is reached.
  * Idempotent submission: a caller-supplied (or generated) ``client_order_id``
    is checked against the persisted order log before anything is built —
    a repeat of the same key is reported as ``status="duplicate"`` rather
    than resubmitted.
  * Stale-quote and spread guards (``ROBINHOOD_MAX_QUOTE_AGE_S`` /
    ``ROBINHOOD_MAX_SPREAD_BPS``) reject a quote that is too old or too wide
    before it prices an order.
  * Marketable limit orders by default (``ROBINHOOD_USE_LIMIT_ORDERS``) —
    buy at ask*(1+slip), sell at bid*(1-slip),
    ``ROBINHOOD_LIMIT_SLIPPAGE_BPS`` — with Robinhood's only documented
    ``time_in_force`` ("gtc"). ``reconcile_stale_orders()`` is the explicit
    cancel-after handling that stands in for a short time-in-force: it runs
    automatically before every LIVE (non-simulated) order, and blocks that
    new order (``guard="reconcile_failed"``) if it can't confirm the book is
    clean of stale resting orders first. It should ALSO be wired into a
    periodic job independent of new orders — see docs/ROBINHOOD_SETUP.md's
    go-live checklist.
  * A wallet gate (``wallet_lookup``) blocks every order when there is no
    ACTIVE ``trading_wallets`` row for this venue — wired by
    ``get_robinhood_trader()`` for every production caller (API routes,
    ``scripts/live_rotation_trader.py``, ``trading/signal_executor.py``'s
    ``VenueTag``), since all three call into this one connector.
  * Crypto spot has no shorts: SHORT sells held quantity only.

``simulate_order`` / ``simulate_close`` run every guard exactly as a real
order would (the same read-only network calls open_position()/close_position()
make — account, holdings, quote), but never POST/cancel an order and never
touch the persisted order-rate counter, idempotency log or wallet P&L — the
API a forward paper log should call (see their docstrings).

Account set-up (see docs/ROBINHOOD_SETUP.md):
  1. ``python -m trading.robinhood keygen`` — prints a base64 Ed25519 keypair.
  2. Robinhood app → Account → API → create credential, paste the PUBLIC key.
  3. Put the issued API key and your PRIVATE key in the server ``.env`` as
     ``ROBINHOOD_API_KEY`` / ``ROBINHOOD_PRIVATE_KEY_B64`` (never commit them).
  4. ``GET /api/v1/trading/robinhood/status`` should report the account.
"""

from __future__ import annotations

import base64
import json
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any, Callable
from urllib.parse import parse_qs, urlencode, urlparse

import requests
from loguru import logger as log

from trading.robinhood_risk_store import InMemoryRiskStore, RiskStore, utc_today

ROBINHOOD_BASE_URL: str = "https://trading.robinhood.com"

_API = "/api/v1/crypto"
PATH_ACCOUNT = f"{_API}/trading/accounts/"
PATH_HOLDINGS = f"{_API}/trading/holdings/"
PATH_TRADING_PAIRS = f"{_API}/trading/trading_pairs/"
PATH_ORDERS = f"{_API}/trading/orders/"
PATH_BEST_BID_ASK = f"{_API}/marketdata/best_bid_ask/"

#: Quantity precision when a trading pair does not advertise an increment.
_DEFAULT_INCREMENT = Decimal("0.00000001")

#: Pair statuses Robinhood uses for "you can trade this right now".
_TRADABLE_STATUSES = frozenset({"tradable", "active"})

#: Order states reconcile_stale_orders() treats as "still resting".
_OPEN_ORDER_STATES = frozenset({"open", "unconfirmed", "placed"})

#: Price-field and quote suffixes that trail a crypto asset code in GRID
#: feature names (``btc_close``, ``eth_usd_full``, ``sol_usd_full``).
_FEATURE_SUFFIXES = frozenset({
    "FULL", "CLOSE", "OPEN", "HIGH", "LOW", "PRICE", "VOLUME", "ADJ", "MID",
    "LAST", "SPOT", "INDEX", "USD", "USDT", "USDC",
})

#: Quote currencies glued onto an asset code without a separator (``BTCUSDT``).
_GLUED_QUOTES = ("USDT", "USDC", "USD")


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested without network)
# ---------------------------------------------------------------------------


def normalize_symbol(ticker: str) -> str:
    """Map GRID ticker spellings to a Robinhood crypto pair.

    Examples:
        BTC       -> BTC-USD
        btc-usd   -> BTC-USD
        ETH-PERP  -> ETH-USD
        SOL/USDT  -> SOL-USD
    """
    base = (ticker or "").strip().upper()
    for sep in ("-", "/", ":"):
        base = base.split(sep)[0]
    if not base:
        raise ValueError(f"Cannot derive a crypto symbol from {ticker!r}")
    return f"{base}-USD"


def crypto_asset_code(name: str) -> str | None:
    """Best-effort crypto asset code from a GRID feature name or ticker.

    ``btc_close`` → ``BTC``; ``eth_usd_full`` → ``ETH``; ``BTC-USD`` → ``BTC``;
    ``BTCUSDT`` → ``BTC``. Returns ``None`` when *name* carries no plausible
    asset code. A non-``None`` answer is a *candidate* only — the caller must
    confirm it against :meth:`RobinhoodCryptoTrader.tradable_assets`, which is
    the authority on what Robinhood will actually trade (``sp500_close``
    yields ``SP500``, and no such pair exists).
    """
    tokens = [t for t in re.split(r"[^A-Za-z0-9]+", (name or "").upper()) if t]
    while tokens and tokens[-1] in _FEATURE_SUFFIXES:
        tokens.pop()
    if not tokens:
        return None
    candidate = tokens[0]
    for quote in _GLUED_QUOTES:
        if candidate.endswith(quote) and len(candidate) > len(quote):
            candidate = candidate[: -len(quote)]
            break
    if not re.fullmatch(r"[A-Z][A-Z0-9]{1,9}", candidate):
        return None
    return candidate


def cursor_from_next(next_link: Any) -> str | None:
    """Pull the ``cursor`` query parameter out of a paginated ``next`` link.

    Robinhood pages its list endpoints with a full URL in ``next``; the only
    part that matters is the opaque cursor. Returns ``None`` when there is no
    further page.
    """
    if not isinstance(next_link, str) or not next_link:
        return None
    values = parse_qs(urlparse(next_link).query).get("cursor") or []
    return values[0] if values and values[0] else None


def load_signing_key(private_key_b64: str):
    """Return an Ed25519 private key from its base64 seed (32 or 64 bytes)."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    raw = base64.b64decode(private_key_b64)
    if len(raw) not in (32, 64):
        raise ValueError("ROBINHOOD_PRIVATE_KEY_B64 must decode to a 32-byte Ed25519 seed")
    return Ed25519PrivateKey.from_private_bytes(raw[:32])


def generate_keypair() -> tuple[str, str]:
    """Return ``(private_key_b64, public_key_b64)`` for a fresh Ed25519 key.

    The PUBLIC half goes into the Robinhood API-credential form; the PRIVATE
    half goes into the server ``.env`` only.
    """
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    key = Ed25519PrivateKey.generate()
    private_raw = key.private_bytes(
        serialization.Encoding.Raw,
        serialization.PrivateFormat.Raw,
        serialization.NoEncryption(),
    )
    public_raw = key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw,
    )
    return base64.b64encode(private_raw).decode(), base64.b64encode(public_raw).decode()


def sign_request(signing_key, api_key: str, timestamp: int, path: str, method: str, body: str) -> str:
    """Base64 Ed25519 signature over ``api_key + timestamp + path + method + body``.

    *path* includes the query string; *body* is the exact JSON text sent (empty
    for GET). This is the message layout Robinhood's docs specify.
    """
    message = f"{api_key}{timestamp}{path}{method.upper()}{body}"
    return base64.b64encode(signing_key.sign(message.encode("utf-8"))).decode()


def round_down_to_increment(quantity: float | Decimal, increment: str | float | None) -> Decimal:
    """Floor *quantity* to a multiple of the pair's quantity increment."""
    qty = Decimal(str(quantity))
    if qty <= 0:
        return Decimal("0")
    inc = Decimal(str(increment)) if increment else _DEFAULT_INCREMENT
    if inc <= 0:
        inc = _DEFAULT_INCREMENT
    return (qty // inc) * inc


def _format_decimal(value: Decimal) -> str:
    """Plain-decimal string (no exponent, no trailing zeros) for the API."""
    text = format(value.quantize(_DEFAULT_INCREMENT, rounding=ROUND_DOWN), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def format_quantity(quantity: Decimal) -> str:
    """Plain-decimal string (no exponent, no trailing zeros) for the API."""
    return _format_decimal(quantity)


def format_price(price: float | Decimal) -> str:
    """Plain-decimal string for a limit price — same convention as
    :func:`format_quantity` (no exponent, no trailing zeros, floored to 8dp).
    Sent as a string (like ``asset_quantity``) to avoid float/JSON precision
    drift on the wire."""
    return _format_decimal(Decimal(str(price)))


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_iso(value: Any) -> datetime | None:
    """Parse an RFC3339/ISO8601 timestamp (Robinhood's own format, e.g.
    ``2026-09-10T10:00:00Z``). ``None`` when absent or unparseable."""
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _parse_quote_timestamp(value: Any, fallback: datetime) -> datetime:
    """Robinhood's ``best_bid_ask`` rows carry a ``timestamp`` field; fall
    back to the local fetch time when it is absent or unparseable — see
    ``ROBINHOOD_MAX_QUOTE_AGE_S``."""
    return _parse_iso(value) or fallback


#: Fixed namespace (derived once from a readable name, itself a uuid5 of
#: uuid.NAMESPACE_DNS) so deterministic_client_order_id() below always
#: derives the same key from the same inputs, in any process, forever.
_ORDER_ID_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_DNS, "grid.robinhood.order-idempotency")


def deterministic_client_order_id(source: str, decision_id: str, asset: str, side: str, decision_date: Any) -> str:
    """Stable idempotency key for one automated order decision.

    The same ``(source, decision_id, asset, side, decision_date)`` always
    derives the SAME key, so a retry of the identical decision — the signal
    executor's next cycle re-evaluating a still-open signal, a rotation
    rebalance run twice — is recognized as a duplicate by
    ``RiskStore.is_duplicate()`` and reported ``status="duplicate"`` rather
    than resubmitted. Any different input (a new *decision_date*, a
    different *side*) derives a different key, i.e. a different decision.
    Used by ``trading/signal_executor.py`` (``VenueTag.submit``) and
    ``scripts/live_rotation_trader.py``; manual/API calls normally omit
    ``client_order_id`` instead and are not deduplicated.
    """
    parts = "|".join(str(p) for p in (source, decision_id, asset, side, decision_date))
    return str(uuid.uuid5(_ORDER_ID_NAMESPACE, parts))


# ---------------------------------------------------------------------------
# Trader
# ---------------------------------------------------------------------------


class RobinhoodCryptoTrader:
    """Trade crypto spot on Robinhood through the official API.

    GRID-specific risk controls (per-order cap, persisted drawdown halt,
    daily loss cap, order-rate cap, idempotency, stale-quote and spread
    guards, wallet gate) and a dry-run default sit in front of every order.
    """

    def __init__(
        self,
        api_key: str = "",
        private_key_b64: str = "",
        live: bool = False,
        max_position_usd: float = 100.0,
        max_drawdown_pct: float = 0.20,
        base_url: str = ROBINHOOD_BASE_URL,
        session: Any | None = None,
        timeout: int = 15,
        *,
        venue: str = "robinhood",
        risk_store: RiskStore | None = None,
        max_daily_loss_pct: float = 0.05,
        max_orders_per_day: int = 6,
        max_quote_age_s: float = 30.0,
        max_spread_bps: float = 50.0,
        use_limit_orders: bool = True,
        limit_slippage_bps: float = 25.0,
        limit_cancel_after_s: float = 15.0,
        wallet_lookup: Callable[[], dict[str, Any] | None] | None = None,
        alert_fn: Callable[[str, str, str], Any] | None = None,
        wallet_pnl_fn: Callable[[str, float, bool], Any] | None = None,
        alert_on_dry_run: bool = False,
    ) -> None:
        self.api_key = api_key or ""
        self.configured = bool(api_key and private_key_b64)
        self._signing_key = load_signing_key(private_key_b64) if self.configured else None
        self.live = bool(live)
        self.max_position_usd = float(max_position_usd)
        self.max_drawdown_pct = float(max_drawdown_pct)
        self.base_url = base_url.rstrip("/")
        self._session = session or requests.Session()
        self.timeout = timeout
        self._tradable_assets: set[str] | None = None

        self.venue = venue
        self.risk_store: RiskStore = risk_store or InMemoryRiskStore()
        self.max_daily_loss_pct = float(max_daily_loss_pct)
        self.max_orders_per_day = int(max_orders_per_day)
        self.max_quote_age_s = float(max_quote_age_s)
        self.max_spread_bps = float(max_spread_bps)
        self.use_limit_orders = bool(use_limit_orders)
        self.limit_slippage_bps = float(limit_slippage_bps)
        self.limit_cancel_after_s = float(limit_cancel_after_s)
        #: None means "not wired" -- no gate is enforced (used by callers
        #: that build a trader directly, e.g. tests and ad-hoc scripts).
        #: get_robinhood_trader() always wires one for production traffic.
        self.wallet_lookup = wallet_lookup
        self.alert_fn = alert_fn
        self.wallet_pnl_fn = wallet_pnl_fn
        self.alert_on_dry_run = bool(alert_on_dry_run)

        log.info(
            "RobinhoodCryptoTrader initialized — mode={mode} cap=${cap} dd={dd:.0%} "
            "daily_loss={dl:.0%} orders/day={od} limit_orders={lo}",
            mode=self.mode, cap=self.max_position_usd, dd=self.max_drawdown_pct,
            dl=self.max_daily_loss_pct, od=self.max_orders_per_day, lo=self.use_limit_orders,
        )

    @property
    def mode(self) -> str:
        if not self.configured:
            return "UNCONFIGURED"
        return "LIVE" if self.live else "DRY_RUN"

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | list[tuple[str, Any]] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        if not self.configured:
            return {"error": "Robinhood API credentials not configured "
                             "(ROBINHOOD_API_KEY / ROBINHOOD_PRIVATE_KEY_B64)."}
        full_path = path + (f"?{urlencode(params, doseq=True)}" if params else "")
        payload = json.dumps(body) if body is not None else ""
        timestamp = int(time.time())
        headers = {
            "x-api-key": self.api_key,
            "x-signature": sign_request(self._signing_key, self.api_key, timestamp, full_path, method, payload),
            "x-timestamp": str(timestamp),
            "Content-Type": "application/json",
        }
        try:
            resp = self._session.request(
                method.upper(), self.base_url + full_path, headers=headers,
                data=payload if body is not None else None, timeout=self.timeout,
            )
        except Exception as exc:  # noqa: BLE001 — network failure is data, not a crash
            log.warning("Robinhood {m} {p} failed: {e}", m=method, p=path, e=str(exc))
            return {"error": f"request failed: {exc}"}
        if resp.status_code >= 400:
            log.warning("Robinhood {m} {p} -> HTTP {s}", m=method, p=path, s=resp.status_code)
            return {"error": f"HTTP {resp.status_code}: {getattr(resp, 'text', '')[:200]}"}
        try:
            return resp.json() if getattr(resp, "content", b"") else {}
        except ValueError:
            return {"error": "non-JSON response"}

    def _paged_results(
        self,
        path: str,
        params: list[tuple[str, Any]] | None = None,
        max_pages: int = 10,
    ) -> list[dict[str, Any]]:
        """Read every page of a list endpoint, following Robinhood's ``next``.

        Bounded at *max_pages* so a broken cursor cannot spin. A truncated
        read is logged rather than raised — callers treat the result as "what
        Robinhood reported", never as a closed-world guarantee.
        """
        base = list(params or [])
        rows: list[dict[str, Any]] = []
        page = base
        for _ in range(max_pages):
            payload = self._request("GET", path, params=page or None)
            rows.extend(self._results(payload))
            cursor = cursor_from_next(payload.get("next") if isinstance(payload, dict) else None)
            if not cursor:
                return rows
            page = base + [("cursor", cursor)]
        log.warning("Robinhood {p}: stopped after {n} pages, more remain", p=path, n=max_pages)
        return rows

    @staticmethod
    def _results(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            if "error" in payload:
                return []
            results = payload.get("results")
            return list(results) if isinstance(results, list) else []
        return list(payload) if isinstance(payload, list) else []

    # ------------------------------------------------------------------
    # Read methods
    # ------------------------------------------------------------------

    def get_account(self) -> dict[str, Any]:
        """Account number, status and buying power."""
        data = self._request("GET", PATH_ACCOUNT)
        if "error" in data:
            return data
        number = str(data.get("account_number", ""))
        return {
            "account_number": f"…{number[-4:]}" if number else "",
            "status": data.get("status"),
            "buying_power_usd": round(_to_float(data.get("buying_power")), 2),
            "currency": data.get("buying_power_currency", "USD"),
        }

    def get_holdings(self) -> list[dict[str, Any]]:
        """Raw holdings (asset code, total and tradable quantity)."""
        return self._results(self._request("GET", PATH_HOLDINGS))

    def get_best_bid_ask(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        """``{symbol: {bid, ask, mid, timestamp}}`` for the requested pairs.

        ``timestamp`` is Robinhood's own quote timestamp when the response
        carries one, else the local time this call completed — see
        :meth:`_quote_for` / ``ROBINHOOD_MAX_QUOTE_AGE_S``.
        """
        if not symbols:
            return {}
        data = self._request("GET", PATH_BEST_BID_ASK, params=[("symbol", s) for s in symbols])
        fetched_at = datetime.now(timezone.utc)
        quotes: dict[str, dict[str, Any]] = {}
        for row in self._results(data):
            sym = row.get("symbol")
            if not sym:
                continue
            bid = _to_float(row.get("bid_inclusive_of_sell_spread"))
            ask = _to_float(row.get("ask_inclusive_of_buy_spread"))
            mid = _to_float(row.get("price")) or ((bid + ask) / 2 if bid and ask else 0.0)
            quotes[sym] = {
                "bid": bid, "ask": ask, "mid": mid,
                "timestamp": _parse_quote_timestamp(row.get("timestamp"), fetched_at),
            }
        return quotes

    def get_trading_pair(self, symbol: str) -> dict[str, Any]:
        """Pair metadata: min/max order size, quantity increment, status."""
        rows = self._results(self._request("GET", PATH_TRADING_PAIRS, params={"symbol": symbol}))
        return rows[0] if rows else {}

    def get_trading_pairs(self) -> list[dict[str, Any]]:
        """Every trading pair Robinhood lists for this account, all pages."""
        return self._paged_results(PATH_TRADING_PAIRS)

    def tradable_assets(self, refresh: bool = False) -> set[str]:
        """Asset codes Robinhood currently reports tradable (``{"BTC", "ETH", …}``).

        Cached on the instance: the venue routing paths ask once per run
        instead of once per signal. An empty set means "Robinhood told us
        nothing" (unconfigured, or the call failed) — callers must treat that
        as "we do not know", never as "everything allowed" and never as "the
        universe is empty". An empty answer is deliberately not cached, so a
        transient failure is retried rather than pinned for the run.
        """
        if self._tradable_assets and not refresh:
            return self._tradable_assets
        assets: set[str] = set()
        for pair in self.get_trading_pairs():
            symbol = str(pair.get("symbol", ""))
            status = str(pair.get("status", "")).lower()
            if not symbol or status not in _TRADABLE_STATUSES:
                continue
            code = symbol.split("-")[0].strip().upper()
            if code:
                assets.add(code)
        if assets:
            self._tradable_assets = assets
        return assets

    def is_tradable(self, ticker: str) -> bool:
        """True when *ticker* maps to a pair Robinhood reports tradable.

        Accepts GRID feature spellings (``btc_close``) as well as plain
        tickers (``BTC``, ``BTC-USD``).
        """
        code = crypto_asset_code(ticker)
        return bool(code) and code in self.tradable_assets()

    def get_positions(self) -> list[dict[str, Any]]:
        """Holdings with quantity > 0, valued at the current mid."""
        holdings = [h for h in self.get_holdings() if _to_float(h.get("total_quantity")) > 0]
        if not holdings:
            return []
        symbols = [f"{h['asset_code']}-USD" for h in holdings if h.get("asset_code")]
        quotes = self.get_best_bid_ask(symbols)
        positions = []
        for h in holdings:
            code = h.get("asset_code", "")
            qty = _to_float(h.get("total_quantity"))
            mid = quotes.get(f"{code}-USD", {}).get("mid", 0.0)
            positions.append({
                "coin": code,
                "symbol": f"{code}-USD",
                "direction": "LONG",
                "size": qty,
                "available": _to_float(h.get("quantity_available_for_trading"), qty),
                "mid_price": round(mid, 6),
                "size_usd": round(qty * mid, 2),
            })
        return positions

    def get_balance(self) -> dict[str, Any]:
        """Buying power + holdings value = equity.

        No longer tracks the high-water mark itself — :meth:`check_risk_limits`
        does that through ``self.risk_store`` so it survives a fresh instance.
        """
        account = self.get_account()
        if "error" in account:
            return account
        positions = self.get_positions()
        holdings_value = sum(p["size_usd"] for p in positions)
        equity = account["buying_power_usd"] + holdings_value
        state = self.risk_store.get_state(self.venue)
        hwm = max(state.peak_equity, equity) if state else equity
        return {
            "account_number": account["account_number"],
            "mode": self.mode,
            "buying_power_usd": account["buying_power_usd"],
            "holdings_value_usd": round(holdings_value, 2),
            "equity_usd": round(equity, 2),
            "open_positions": len(positions),
            "high_water_mark": round(hwm, 2) if hwm else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_orders(self, limit: int = 50) -> list[dict[str, Any]]:
        """Most recent orders (newest first as returned by the API)."""
        rows = self._results(self._request("GET", PATH_ORDERS, params={"limit": int(limit)}))
        return [
            {
                "id": r.get("id"),
                "symbol": r.get("symbol"),
                "side": r.get("side"),
                "type": r.get("type"),
                "state": r.get("state"),
                "filled_quantity": r.get("filled_asset_quantity"),
                "average_price": r.get("average_price"),
                "created_at": r.get("created_at"),
            }
            for r in rows[:limit]
        ]

    # ------------------------------------------------------------------
    # Risk management
    # ------------------------------------------------------------------

    def check_risk_limits(self) -> dict[str, Any]:
        """Drawdown, daily-loss and order-rate checks against persisted state.

        Every call refreshes ``self.risk_store``'s row for this venue (peak
        equity, day rollover) but does NOT count an order — callers that are
        about to actually submit call ``self.risk_store.touch(..., True)``
        themselves once every other guard has passed.
        """
        balance = self.get_balance()
        if "error" in balance:
            return {"error": balance["error"], "drawdown_breached": True}
        equity = balance["equity_usd"]
        today = utc_today()
        state = self.risk_store.touch(self.venue, equity, today, increment_order=False)

        hwm = state.peak_equity
        drawdown = (hwm - equity) / hwm if hwm > 0 else 0.0
        drawdown_breached = drawdown >= self.max_drawdown_pct
        if drawdown_breached:
            log.warning("DRAWDOWN BREACH: {dd:.1%} >= {mx:.1%} — Robinhood trading halted",
                        dd=drawdown, mx=self.max_drawdown_pct)

        day_start = state.day_start_equity
        daily_loss_pct = (day_start - equity) / day_start if day_start > 0 else 0.0
        daily_loss_breached = daily_loss_pct >= self.max_daily_loss_pct
        if daily_loss_breached:
            log.warning("DAILY LOSS CAP BREACH: {dl:.1%} >= {mx:.1%} — new Robinhood buys halted",
                        dl=daily_loss_pct, mx=self.max_daily_loss_pct)

        order_rate_breached = state.orders_today >= self.max_orders_per_day
        if order_rate_breached:
            log.warning("ORDER-RATE CAP REACHED: {n}/{mx} orders today — new Robinhood buys halted",
                        n=state.orders_today, mx=self.max_orders_per_day)

        return {
            "equity_usd": round(equity, 2),
            "high_water_mark": round(hwm, 2),
            "current_drawdown_pct": round(drawdown, 4),
            "max_drawdown_pct": self.max_drawdown_pct,
            "drawdown_breached": drawdown_breached,
            "day_start_equity": round(day_start, 2),
            "daily_loss_pct": round(daily_loss_pct, 4),
            "max_daily_loss_pct": self.max_daily_loss_pct,
            "daily_loss_breached": daily_loss_breached,
            "orders_today": state.orders_today,
            "max_orders_per_day": self.max_orders_per_day,
            "order_rate_breached": order_rate_breached,
            "max_position_usd": self.max_position_usd,
            "mode": self.mode,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _wallet_gate(self) -> tuple[bool, dict[str, Any] | None]:
        """``(blocked, wallet)``.

        ``blocked`` is True only when ``wallet_lookup`` is wired and it
        returns no ACTIVE wallet (including a lookup failure — a risk gate
        that can't confirm ACTIVE must fail closed, not open). ``wallet_lookup
        is None`` (nothing wired) never blocks — that is the state every
        directly-constructed trader (tests, ad-hoc scripts) is in unless it
        opts in; ``get_robinhood_trader()`` always wires one in production.
        """
        if self.wallet_lookup is None:
            return False, None
        try:
            wallet = self.wallet_lookup()
        except Exception as exc:  # noqa: BLE001 — fail closed, never let a lookup error open the gate
            log.warning("Robinhood wallet lookup failed: {e}", e=str(exc))
            return True, None
        if not wallet or wallet.get("status") != "ACTIVE":
            return True, None
        return False, wallet

    def _send_alert(self, subject: str, body: str, severity: str = "info") -> None:
        if self.alert_fn is None:
            return
        try:
            self.alert_fn(subject, body, severity)
        except Exception as exc:  # noqa: BLE001 — alerting must never block or crash an order decision
            log.warning("Robinhood alert failed ({s}): {e}", s=subject, e=str(exc))

    def _alert_guard_trip(self, guard: str, message: str) -> None:
        self._send_alert(f"[GRID] Robinhood guard tripped: {guard}", message, severity="warning")

    def _settle_wallet_pnl(self, wallet: dict[str, Any] | None, symbol: str,
                           qty: Decimal, fill_price: float, simulated: bool) -> None:
        """Best-effort realized P&L on a SELL fill, fed to
        ``WalletManager.update_pnl`` via ``self.wallet_pnl_fn``.

        Robinhood's API exposes no cost basis for a spot holding (see
        :func:`status` — positions carry no ``unrealized_pnl`` field), so this
        estimates cost basis as the volume-weighted average of GRID's own
        prior BUY fills for (venue, symbol) in the persisted order log since
        the last SELL. That is an ESTIMATE from GRID's own history, not
        Robinhood's authoritative basis, and is skipped (no call at all) when
        there is no ACTIVE wallet, no ``wallet_pnl_fn`` wired, or no cost
        history to compute from (e.g. a position that predates this
        connector's order log). BUY fills never call this — there is no
        realized P&L to record until a sale closes some of the position.
        """
        if wallet is None or self.wallet_pnl_fn is None or fill_price <= 0:
            return
        try:
            avg_cost = self.risk_store.average_cost(self.venue, symbol)
        except Exception as exc:  # noqa: BLE001 — a P&L estimate must never block a settled order
            log.warning("Robinhood avg-cost lookup failed for {s}: {e}", s=symbol, e=str(exc))
            return
        if not avg_cost or avg_cost <= 0:
            return
        pnl = (float(fill_price) - avg_cost) * float(qty)
        try:
            self.wallet_pnl_fn(wallet["id"], pnl, pnl > 0)
            log.info("Robinhood wallet {w} P&L settled: {p:+.2f}{sim}",
                     w=wallet["id"], p=pnl, sim=" (simulated)" if simulated else "")
        except Exception as exc:  # noqa: BLE001 — a P&L write failure must not fail the order
            log.warning("Robinhood update_pnl failed for wallet {w}: {e}", w=wallet.get("id"), e=str(exc))

    # ------------------------------------------------------------------
    # Quoting
    # ------------------------------------------------------------------

    def _quote_for(self, symbol: str, side: str) -> dict[str, Any]:
        """Fresh quote for *symbol*/*side*, gated by the stale-quote and
        spread guards. Always re-fetches (never reuses an earlier quote in
        the same call) so the price checked is the price about to be used.

        On success: ``bid``, ``ask``, ``mid``, ``reference_price`` (ask for a
        buy / bid for a sell — unpadded, what quantity is sized against),
        ``executable_price`` (the marketable-limit price including slippage
        when ``use_limit_orders``, else equal to ``reference_price``),
        ``spread_bps``, ``age_s``. On failure: ``error`` (plus ``status`` /
        ``guard`` for the two guarded rejections).
        """
        quotes = self.get_best_bid_ask([symbol])
        quote = quotes.get(symbol)
        if not quote:
            return {"error": f"No quote for {symbol} on Robinhood."}
        bid, ask, mid = quote["bid"], quote["ask"], quote["mid"]
        price = ask if side == "buy" else bid
        price = price or mid
        if price <= 0:
            return {"error": f"Invalid price for {symbol}: {price}"}

        age_s = max(0.0, (datetime.now(timezone.utc) - quote["timestamp"]).total_seconds())
        if age_s > self.max_quote_age_s:
            msg = (f"Quote for {symbol} is {age_s:.0f}s old (max {self.max_quote_age_s:.0f}s) — "
                   "refusing a stale price.")
            self._alert_guard_trip("stale_quote", msg)
            return {"error": msg, "status": "blocked", "guard": "stale_quote"}

        spread_bps = ((ask - bid) / mid * 10000.0) if (mid and ask and bid) else 0.0
        if spread_bps > self.max_spread_bps:
            msg = (f"{symbol} spread {spread_bps:.1f}bps exceeds the {self.max_spread_bps:.1f}bps cap "
                   f"(bid {bid} / ask {ask}) — refusing a dislocated quote.")
            self._alert_guard_trip("wide_spread", msg)
            return {"error": msg, "status": "blocked", "guard": "spread"}

        slip = self.limit_slippage_bps / 10000.0
        limit_price = price * (1 + slip) if side == "buy" else price * (1 - slip)
        return {
            "bid": bid, "ask": ask, "mid": mid, "reference_price": price,
            "executable_price": limit_price if self.use_limit_orders else price,
            "spread_bps": round(spread_bps, 2), "age_s": round(age_s, 1),
        }

    def _build_order(self, symbol: str, side: str, qty: Decimal,
                     client_order_id: str, quote: dict[str, Any]) -> dict[str, Any]:
        """Market or marketable-limit order payload for Robinhood's
        ``POST /orders/`` (see https://docs.robinhood.com/crypto/trading/).
        Limit orders use ``time_in_force="gtc"`` — Robinhood's only
        documented value; see ``reconcile_stale_orders`` for the explicit
        cancel-after handling that stands in for a shorter TIF."""
        order: dict[str, Any] = {"client_order_id": client_order_id, "side": side, "symbol": symbol}
        if self.use_limit_orders:
            order["type"] = "limit"
            order["limit_order_config"] = {
                "asset_quantity": format_quantity(qty),
                "limit_price": format_price(quote["executable_price"]),
                "time_in_force": "gtc",
            }
        else:
            order["type"] = "market"
            order["market_order_config"] = {"asset_quantity": format_quantity(qty)}
        return order

    # ------------------------------------------------------------------
    # Trade methods
    # ------------------------------------------------------------------

    def _submit(self, order: dict[str, Any], context: dict[str, Any], force_dry_run: bool = False) -> dict[str, Any]:
        """Send *order* when live; otherwise return it as a dry run.

        ``force_dry_run`` overrides ``self.live`` for one call without
        mutating shared state — how :meth:`simulate_order` /
        :meth:`simulate_close` guarantee no network side effect regardless
        of the connector's configured mode.
        """
        if not self.live or force_dry_run:
            log.info("DRY RUN Robinhood order (ROBINHOOD_LIVE_TRADING=false): {o}", o=order)
            return {"status": "dry_run", "order": order, **context,
                    "note": "ROBINHOOD_LIVE_TRADING is false — order built and risk-checked, not sent",
                    "timestamp": datetime.now(timezone.utc).isoformat()}
        result = self._request("POST", PATH_ORDERS, body=order)
        if "error" in result:
            log.error("Robinhood order failed: {e}", e=result["error"])
            return {"status": "rejected", "order": order, **context, "error": result["error"]}
        log.info("Robinhood order accepted: id={i} state={s}", i=result.get("id"), s=result.get("state"))
        self._send_alert(
            "[GRID] LIVE Robinhood order submitted",
            f"{context.get('direction')} {context.get('symbol')} ${context.get('size_usd')} "
            f"— order_id={result.get('id')} state={result.get('state')}",
            severity="warning",
        )
        return {"status": "submitted", "order_id": result.get("id"), "state": result.get("state"),
                "order": order, **context, "raw_response": result,
                "timestamp": datetime.now(timezone.utc).isoformat()}

    def _record_and_settle(self, *, wallet: dict[str, Any] | None, symbol: str, side: str,
                           direction: str, size_usd: float, qty: Decimal, quote: dict[str, Any],
                           spread_cost_usd: float, order: dict[str, Any], result: dict[str, Any]) -> None:
        """Persist the order-log row and (for a sell) settle wallet P&L.
        Called once per attempt that got far enough to be built — guard
        rejections earlier in open_position/close_position return before
        this and are not persisted (they are fully visible in the returned
        response, the loguru warning, and the guard-trip alert already
        sent)."""
        status = result.get("status", "error")
        fill_price = quote.get("executable_price", 0.0)
        if status == "submitted":
            avg = _to_float((result.get("raw_response") or {}).get("average_price"), 0.0)
            if avg > 0:
                fill_price = avg

        # Settle P&L BEFORE logging this fill — average_cost() scans the
        # order log for PRIOR fills of this ticker, and this fill (a SELL,
        # when we get here) would otherwise be its own most-recent row and
        # short-circuit the scan to "no prior BUY history" every time.
        if side == "sell" and status in ("dry_run", "submitted"):
            self._settle_wallet_pnl(wallet, symbol, qty, fill_price, simulated=(status == "dry_run"))

        self.risk_store.log_order(
            self.venue, order.get("client_order_id", ""),
            wallet_id=(wallet or {}).get("id"), ticker=symbol, side=side, direction=direction,
            size_usd=round(size_usd, 2), quantity=format_quantity(qty),
            bid=quote.get("bid"), ask=quote.get("ask"), mid=quote.get("mid"),
            executable_price=quote.get("executable_price"), spread_bps=quote.get("spread_bps"),
            spread_cost_usd=spread_cost_usd, order_type=order.get("type", "market"), status=status,
            fill_price=fill_price,
            guard_results={"wallet": "ok", "drawdown": "ok", "daily_loss": "ok",
                          "order_rate": "ok", "stale_quote": "ok", "spread": "ok"},
            error=result.get("error"), raw_response=result.get("raw_response"),
            simulated=(status == "dry_run"),
        )
        # LIVE orders already alert unconditionally from _submit(). A dry-run
        # that passed every guard only alerts when opted in — guard trips
        # (which never reach this method) always alert regardless.
        if status == "dry_run" and self.alert_on_dry_run:
            self._send_alert(
                "[GRID] Robinhood dry-run order",
                f"{direction} {symbol} ${size_usd:.2f} @ ~{fill_price:.6f} (simulated, not sent)",
                severity="info",
            )

    def open_position(
        self,
        ticker: str,
        direction: str,
        size_usd: float,
        client_order_id: str | None = None,
        *,
        force_dry_run: bool = False,
    ) -> dict[str, Any]:
        """Order for *size_usd* notional — a marketable limit by default,
        a plain market order when ``use_limit_orders`` is off.

        LONG buys. SHORT sells held quantity (crypto spot cannot go net
        short). *client_order_id*, when given, is the idempotency key
        checked against the persisted order log (pass a deterministic
        uuid5 from an automated caller — see ``trading/signal_executor.py``
        and ``scripts/live_rotation_trader.py``); omitted, a fresh uuid4 is
        used and the call is not deduplicated. *force_dry_run* is what
        :meth:`simulate_order` uses to guarantee no network call and no
        persisted order-rate/idempotency side effect regardless of
        ``self.live``.
        """
        direction = (direction or "").upper()
        if direction not in ("LONG", "SHORT"):
            return {"error": f"Invalid direction: {direction}. Must be LONG or SHORT."}
        if size_usd <= 0:
            return {"error": "size_usd must be positive."}
        if size_usd > self.max_position_usd:
            return {"error": f"Size ${size_usd} exceeds max_position_usd ${self.max_position_usd}. "
                             "Increase ROBINHOOD_MAX_POSITION_USD or reduce size."}
        if not self.configured:
            return {"error": "Robinhood API credentials not configured."}

        reconcile_block = self._reconcile_guard(force_dry_run)
        if reconcile_block is not None:
            return reconcile_block

        blocked, wallet = self._wallet_gate()
        if blocked:
            msg = "No ACTIVE robinhood wallet — trading blocked. Create or resume a wallet first."
            log.warning("Robinhood order blocked: {m}", m=msg)
            if not force_dry_run:
                self._alert_guard_trip("wallet", msg)
            return {"error": msg, "status": "blocked", "guard": "wallet"}

        risk = self.check_risk_limits()
        if "error" in risk:
            return {"error": risk["error"]}
        if risk["drawdown_breached"]:
            msg = (f"Max drawdown breached: {risk['current_drawdown_pct']:.1%} "
                   f">= {self.max_drawdown_pct:.1%}. Trading halted.")
            if not force_dry_run:
                self._alert_guard_trip("drawdown", msg)
            return {"error": msg, "status": "blocked", "guard": "drawdown"}
        if direction == "LONG" and risk["daily_loss_breached"]:
            msg = (f"Daily loss cap breached: {risk['daily_loss_pct']:.1%} "
                   f">= {self.max_daily_loss_pct:.1%} from start-of-day equity. New buys halted "
                   "(sells/closes still allowed).")
            if not force_dry_run:
                self._alert_guard_trip("daily_loss", msg)
            return {"error": msg, "status": "blocked", "guard": "daily_loss"}
        if direction == "LONG" and risk["order_rate_breached"]:
            msg = (f"Order-rate cap reached: {risk['orders_today']}/{self.max_orders_per_day} "
                   "orders placed today.")
            if not force_dry_run:
                self._alert_guard_trip("order_rate", msg)
            return {"error": msg, "status": "blocked", "guard": "order_rate"}

        try:
            symbol = normalize_symbol(ticker)
        except ValueError as exc:
            return {"error": str(exc)}

        side = "buy" if direction == "LONG" else "sell"
        key = client_order_id or str(uuid.uuid4())
        if not force_dry_run and self.risk_store.is_duplicate(self.venue, key):
            log.info("Robinhood order {k} for {s} is a duplicate — not resubmitted", k=key, s=symbol)
            return {"status": "duplicate", "client_order_id": key, "symbol": symbol, "direction": direction,
                    "note": "An order with this idempotency key already completed (dry-run or submitted)."}

        quote = self._quote_for(symbol, side)
        if "error" in quote:
            return quote

        pair = self.get_trading_pair(symbol)
        if pair and str(pair.get("status", "tradable")).lower() not in ("tradable", "active"):
            return {"error": f"{symbol} is not tradable right now ({pair.get('status')})."}
        qty = round_down_to_increment(size_usd / quote["reference_price"], pair.get("quantity_increment"))

        if side == "sell":
            held = next((p for p in self.get_positions() if p["symbol"] == symbol), None)
            if not held or held["available"] <= 0:
                return {"error": f"SHORT on Robinhood spot sells holdings only, and none of {symbol} is held."}
            qty = min(qty, round_down_to_increment(held["available"], pair.get("quantity_increment")))

        min_size = _to_float(pair.get("min_order_size"), 0.0)
        if qty <= 0 or (min_size and qty < Decimal(str(min_size))):
            return {"error": f"Quantity {format_quantity(qty)} below the {symbol} minimum order size {min_size}."}

        order = self._build_order(symbol, side, qty, key, quote)
        spread_cost_usd = round(float(qty) * (quote["ask"] - quote["bid"]) / 2, 6) if quote["ask"] and quote["bid"] else 0.0
        context = {
            "symbol": symbol, "direction": direction, "size_usd": round(size_usd, 2),
            "quantity": format_quantity(qty), "reference_price": round(quote["reference_price"], 6),
            "bid": quote["bid"], "ask": quote["ask"], "mid": quote["mid"],
            "executable_price": round(quote["executable_price"], 6), "spread_bps": quote["spread_bps"],
            "spread_cost_usd": spread_cost_usd, "order_type": order["type"], "client_order_id": key,
        }
        log.info("Robinhood {d} {s} — ${u} ({q} @ ${p:.2f}) [{m}]",
                 d=direction, s=symbol, u=size_usd, q=context["quantity"], p=quote["reference_price"], m=self.mode)

        if not force_dry_run:
            self.risk_store.touch(self.venue, risk["equity_usd"], utc_today(), increment_order=True)

        result = self._submit(order, context, force_dry_run=force_dry_run)

        if not force_dry_run:
            self._record_and_settle(wallet=wallet, symbol=symbol, side=side, direction=direction,
                                    size_usd=size_usd, qty=qty, quote=quote,
                                    spread_cost_usd=spread_cost_usd, order=order, result=result)
        return result

    def close_position(
        self,
        ticker: str,
        client_order_id: str | None = None,
        *,
        force_dry_run: bool = False,
    ) -> dict[str, Any]:
        """Sell the whole tradable holding of *ticker*.

        Not blocked by the drawdown, daily-loss or order-rate guards — a
        close only reduces exposure — but the wallet gate, idempotency and
        stale-quote/spread guards still apply, exactly as for
        :meth:`open_position`.
        """
        try:
            symbol = normalize_symbol(ticker)
        except ValueError as exc:
            return {"error": str(exc)}
        if not self.configured:
            return {"error": "Robinhood API credentials not configured."}

        reconcile_block = self._reconcile_guard(force_dry_run)
        if reconcile_block is not None:
            return reconcile_block

        blocked, wallet = self._wallet_gate()
        if blocked:
            msg = "No ACTIVE robinhood wallet — trading blocked. Create or resume a wallet first."
            log.warning("Robinhood close blocked: {m}", m=msg)
            if not force_dry_run:
                self._alert_guard_trip("wallet", msg)
            return {"error": msg, "status": "blocked", "guard": "wallet"}

        # Refreshes the persisted peak/day-start from current equity (best
        # effort) without gating the close on any of its guard flags.
        self.check_risk_limits()

        held = next((p for p in self.get_positions() if p["symbol"] == symbol), None)
        if not held or held["available"] <= 0:
            return {"error": f"No open position for {symbol}."}

        key = client_order_id or str(uuid.uuid4())
        if not force_dry_run and self.risk_store.is_duplicate(self.venue, key):
            log.info("Robinhood close {k} for {s} is a duplicate — not resubmitted", k=key, s=symbol)
            return {"status": "duplicate", "client_order_id": key, "symbol": symbol, "direction": "CLOSE",
                    "note": "An order with this idempotency key already completed (dry-run or submitted)."}

        quote = self._quote_for(symbol, "sell")
        if "error" in quote:
            return quote

        pair = self.get_trading_pair(symbol)
        qty = round_down_to_increment(held["available"], pair.get("quantity_increment"))
        if qty <= 0:
            return {"error": f"Holding of {symbol} is below the tradable increment."}

        order = self._build_order(symbol, "sell", qty, key, quote)
        spread_cost_usd = round(float(qty) * (quote["ask"] - quote["bid"]) / 2, 6) if quote["ask"] and quote["bid"] else 0.0
        context = {
            "symbol": symbol, "direction": "CLOSE", "quantity": format_quantity(qty),
            "size_usd": round(held["size_usd"], 2), "bid": quote["bid"], "ask": quote["ask"],
            "mid": quote["mid"], "executable_price": round(quote["executable_price"], 6),
            "spread_bps": quote["spread_bps"], "spread_cost_usd": spread_cost_usd,
            "order_type": order["type"], "client_order_id": key,
        }
        log.info("Robinhood close {s} — {q} [{m}]", s=symbol, q=context["quantity"], m=self.mode)

        result = self._submit(order, context, force_dry_run=force_dry_run)

        if not force_dry_run:
            self._record_and_settle(wallet=wallet, symbol=symbol, side="sell", direction="CLOSE",
                                    size_usd=held["size_usd"], qty=qty, quote=quote,
                                    spread_cost_usd=spread_cost_usd, order=order, result=result)
        return result

    def simulate_order(self, ticker: str, direction: str, size_usd: float,
                       client_order_id: str | None = None) -> dict[str, Any]:
        """Full guard-evaluated decision record for a would-be
        :meth:`open_position` call — the API a forward paper log should call.

        Every guard (wallet, drawdown, daily loss, order rate, quote
        staleness, spread) still runs against real, current data — this
        still makes the same read-only network calls open_position() would
        (account/holdings/quote) — and is reflected in the result exactly as
        it would be live. What's guaranteed never to happen: an order is
        never POSTed/cancelled, the persisted order-rate counter is never
        incremented, and nothing is written to the idempotency/order log or
        to wallet P&L. (The persisted equity high-water mark IS still
        refreshed from the real balance read, same as a plain
        ``check_risk_limits()`` call — that's an accurate observation, not a
        simulated one, and every other guard's evaluation depends on it
        being current.)"""
        return self.open_position(ticker, direction, size_usd, client_order_id, force_dry_run=True)

    def simulate_close(self, ticker: str, client_order_id: str | None = None) -> dict[str, Any]:
        """:meth:`simulate_order`'s counterpart for :meth:`close_position`."""
        return self.close_position(ticker, client_order_id, force_dry_run=True)

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        """Cancel an open order. Not wallet-gated — cancelling only reduces
        risk, so it stays available even for a KILLED/PAUSED wallet."""
        if not self.live:
            return {"status": "dry_run", "order_id": order_id,
                    "note": "ROBINHOOD_LIVE_TRADING is false — nothing to cancel"}
        result = self._request("POST", f"{PATH_ORDERS}{order_id}/cancel/", body={})
        if "error" in result:
            return {"status": "rejected", "order_id": order_id, "error": result["error"]}
        return {"status": "cancel_requested", "order_id": order_id, "raw_response": result}

    def reconcile_stale_orders(self) -> dict[str, Any]:
        """Cancel our own LIVE orders that have been resting past
        ``ROBINHOOD_LIMIT_CANCEL_AFTER_S``.

        Robinhood crypto limit orders only support ``time_in_force="gtc"``
        (no IOC/FOK/expiry) — see :meth:`_build_order` — so a marketable
        limit that doesn't fill immediately would otherwise rest
        indefinitely. This is the explicit cancel-after handling that stands
        in for a short time-in-force. A no-op (``error=None``) in DRY_RUN or
        when market orders are configured.

        Called automatically at the start of every LIVE (non-simulated)
        :meth:`open_position` / :meth:`close_position` via
        :meth:`_reconcile_guard` — a caller placing a new order is exactly
        the moment a stale resting order from a previous attempt matters
        most. ``error`` is set (and the new order is blocked by the caller)
        when the order list couldn't be fetched, or a stale order couldn't
        be confirmed cancelled — a new order must not be layered on top of
        a book we can't currently prove is clean. This method is ALSO meant
        to be wired into a periodic job independent of new orders (see
        docs/ROBINHOOD_SETUP.md's go-live checklist) — the pre-order call
        only reconciles at the moment of the next order, which could be a
        long time after a resting order actually went stale.
        """
        if not self.live or not self.use_limit_orders:
            return {"checked": 0, "cancelled": [], "error": None}
        payload = self._request("GET", PATH_ORDERS, params={"limit": 50})
        if isinstance(payload, dict) and "error" in payload:
            msg = f"could not list orders to reconcile: {payload['error']}"
            log.warning("Robinhood reconcile: {m}", m=msg)
            return {"checked": 0, "cancelled": [], "error": msg}
        cutoff = datetime.now(timezone.utc).timestamp() - self.limit_cancel_after_s
        checked = 0
        cancelled: list[str] = []
        cancel_errors: list[str] = []
        for row in self._results(payload):
            state = str(row.get("state") or "").lower()
            if state not in _OPEN_ORDER_STATES:
                continue
            created = _parse_iso(row.get("created_at"))
            if created is None:
                continue
            checked += 1
            if created.timestamp() < cutoff:
                order_id = row.get("id")
                result = self.cancel_order(order_id)
                if result.get("status") == "cancel_requested":
                    cancelled.append(order_id)
                else:
                    cancel_errors.append(f"{order_id}: {result.get('error', 'cancel not confirmed')}")
        if cancelled:
            log.warning("Robinhood reconcile: cancelled {n} stale resting order(s): {ids}",
                        n=len(cancelled), ids=cancelled)
        if cancel_errors:
            msg = "failed to cancel stale order(s): " + "; ".join(cancel_errors)
            log.warning("Robinhood reconcile: {m}", m=msg)
            return {"checked": checked, "cancelled": cancelled, "error": msg}
        return {"checked": checked, "cancelled": cancelled, "error": None}

    def _reconcile_guard(self, force_dry_run: bool) -> dict[str, Any] | None:
        """Run :meth:`reconcile_stale_orders` before a LIVE order, and turn a
        reconcile failure into a blocked result rather than letting a new
        order stack on top of a book we can't currently prove is clean.
        Returns ``None`` to proceed (dry-run, force_dry_run, or a clean
        reconcile), else the blocked result to return immediately."""
        if not self.live or force_dry_run:
            return None
        reconcile = self.reconcile_stale_orders()
        if reconcile.get("error"):
            msg = f"Could not reconcile stale resting orders before placing a new one: {reconcile['error']}"
            log.warning("Robinhood order blocked: {m}", m=msg)
            self._alert_guard_trip("reconcile_failed", msg)
            return {"error": msg, "status": "blocked", "guard": "reconcile_failed"}
        return None

    def status(self) -> dict[str, Any]:
        """Connector status for the API and the health page."""
        out: dict[str, Any] = {
            "exchange": "robinhood_crypto",
            "mode": self.mode,
            "configured": self.configured,
            "live": self.live,
            "max_position_usd": self.max_position_usd,
            "max_drawdown_pct": self.max_drawdown_pct,
            "max_daily_loss_pct": self.max_daily_loss_pct,
            "max_orders_per_day": self.max_orders_per_day,
            "max_quote_age_s": self.max_quote_age_s,
            "max_spread_bps": self.max_spread_bps,
            "use_limit_orders": self.use_limit_orders,
            "limit_slippage_bps": self.limit_slippage_bps,
            "base_url": self.base_url,
        }
        if self.configured:
            out["account"] = self.get_account()
        return out


# ---------------------------------------------------------------------------
# Factory — build from GRID config
# ---------------------------------------------------------------------------


def get_robinhood_trader() -> RobinhoodCryptoTrader:
    """Instantiate RobinhoodCryptoTrader from GRID Settings (env vars).

    Wires the production dependencies every call site (API routes,
    ``scripts/live_rotation_trader.py``, ``trading/signal_executor.py``)
    shares: a Postgres-backed risk store, the venue wallet gate, and email
    alerts. All three are lazy — building a trader here does no I/O (the
    unauthenticated ``/system/health`` check depends on that), only calling
    ``open_position``/``close_position`` does.
    """
    from config import settings

    def _wallet_lookup() -> dict[str, Any] | None:
        from db import get_engine
        from trading.wallet_manager import resolve_active_wallet

        return resolve_active_wallet(get_engine(), "robinhood")

    def _alert_fn(subject: str, body: str, severity: str = "info") -> None:
        from alerts.email import send_alert

        send_alert(subject, body, severity)

    def _wallet_pnl_fn(wallet_id: str, pnl: float, is_win: bool) -> None:
        from db import get_engine
        from trading.wallet_manager import WalletManager

        WalletManager(get_engine()).update_pnl(wallet_id, pnl, is_win)

    from db import get_engine
    from trading.robinhood_risk_store import PostgresRiskStore

    return RobinhoodCryptoTrader(
        api_key=getattr(settings, "ROBINHOOD_API_KEY", ""),
        private_key_b64=getattr(settings, "ROBINHOOD_PRIVATE_KEY_B64", ""),
        live=bool(getattr(settings, "ROBINHOOD_LIVE_TRADING", False)),
        max_position_usd=float(getattr(settings, "ROBINHOOD_MAX_POSITION_USD", 100.0)),
        max_drawdown_pct=float(getattr(settings, "ROBINHOOD_MAX_DRAWDOWN_PCT", 0.20)),
        base_url=getattr(settings, "ROBINHOOD_BASE_URL", ROBINHOOD_BASE_URL),
        risk_store=PostgresRiskStore(get_engine()),
        max_daily_loss_pct=float(getattr(settings, "ROBINHOOD_MAX_DAILY_LOSS_PCT", 0.05)),
        max_orders_per_day=int(getattr(settings, "ROBINHOOD_MAX_ORDERS_PER_DAY", 6)),
        max_quote_age_s=float(getattr(settings, "ROBINHOOD_MAX_QUOTE_AGE_S", 30.0)),
        max_spread_bps=float(getattr(settings, "ROBINHOOD_MAX_SPREAD_BPS", 50.0)),
        use_limit_orders=bool(getattr(settings, "ROBINHOOD_USE_LIMIT_ORDERS", True)),
        limit_slippage_bps=float(getattr(settings, "ROBINHOOD_LIMIT_SLIPPAGE_BPS", 25.0)),
        limit_cancel_after_s=float(getattr(settings, "ROBINHOOD_LIMIT_CANCEL_AFTER_S", 15.0)),
        wallet_lookup=_wallet_lookup,
        alert_fn=_alert_fn,
        wallet_pnl_fn=_wallet_pnl_fn,
        alert_on_dry_run=bool(getattr(settings, "ROBINHOOD_ALERT_ON_DRY_RUN", False)),
    )


def _cli(argv: list[str]) -> int:
    cmd = argv[1] if len(argv) > 1 else "status"
    if cmd == "keygen":
        private_b64, public_b64 = generate_keypair()
        print("ROBINHOOD_PRIVATE_KEY_B64 (server .env only, never commit):")
        print(f"  {private_b64}")
        print("Public key (paste into Robinhood → Account → API credentials):")
        print(f"  {public_b64}")
        return 0
    if cmd == "status":
        print(json.dumps(get_robinhood_trader().status(), indent=2, default=str))
        return 0
    print("usage: python -m trading.robinhood [keygen|status]")
    return 2


if __name__ == "__main__":
    sys.exit(_cli(sys.argv))
