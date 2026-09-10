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
  * Crypto spot has no shorts: SHORT sells held quantity only.

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
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests
from loguru import logger as log

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


def format_quantity(quantity: Decimal) -> str:
    """Plain-decimal string (no exponent, no trailing zeros) for the API."""
    text = format(quantity.quantize(_DEFAULT_INCREMENT, rounding=ROUND_DOWN), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Trader
# ---------------------------------------------------------------------------


class RobinhoodCryptoTrader:
    """Trade crypto spot on Robinhood through the official API.

    GRID-specific risk controls (per-order cap, drawdown halt) and a dry-run
    default sit in front of every order.
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
        self._high_water_mark: float | None = None
        self._tradable_assets: set[str] | None = None

        log.info(
            "RobinhoodCryptoTrader initialized — mode={mode} cap=${cap} dd={dd:.0%}",
            mode=self.mode, cap=self.max_position_usd, dd=self.max_drawdown_pct,
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

    def get_best_bid_ask(self, symbols: list[str]) -> dict[str, dict[str, float]]:
        """``{symbol: {bid, ask, mid}}`` for the requested pairs."""
        if not symbols:
            return {}
        data = self._request("GET", PATH_BEST_BID_ASK, params=[("symbol", s) for s in symbols])
        quotes: dict[str, dict[str, float]] = {}
        for row in self._results(data):
            sym = row.get("symbol")
            if not sym:
                continue
            bid = _to_float(row.get("bid_inclusive_of_sell_spread"))
            ask = _to_float(row.get("ask_inclusive_of_buy_spread"))
            mid = _to_float(row.get("price")) or ((bid + ask) / 2 if bid and ask else 0.0)
            quotes[sym] = {"bid": bid, "ask": ask, "mid": mid}
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
        """Buying power + holdings value = equity; tracks the high-water mark."""
        account = self.get_account()
        if "error" in account:
            return account
        positions = self.get_positions()
        holdings_value = sum(p["size_usd"] for p in positions)
        equity = account["buying_power_usd"] + holdings_value
        if self._high_water_mark is None or equity > self._high_water_mark:
            self._high_water_mark = equity
        return {
            "account_number": account["account_number"],
            "mode": self.mode,
            "buying_power_usd": account["buying_power_usd"],
            "holdings_value_usd": round(holdings_value, 2),
            "equity_usd": round(equity, 2),
            "open_positions": len(positions),
            "high_water_mark": round(self._high_water_mark, 2) if self._high_water_mark else None,
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
        """Drawdown from the high-water mark versus ``max_drawdown_pct``."""
        balance = self.get_balance()
        if "error" in balance:
            return {"error": balance["error"], "drawdown_breached": True}
        equity = balance["equity_usd"]
        if not self._high_water_mark:
            self._high_water_mark = equity
        hwm = self._high_water_mark
        drawdown = (hwm - equity) / hwm if hwm > 0 else 0.0
        breached = drawdown >= self.max_drawdown_pct
        if breached:
            log.warning("DRAWDOWN BREACH: {dd:.1%} >= {mx:.1%} — Robinhood trading halted",
                        dd=drawdown, mx=self.max_drawdown_pct)
        return {
            "equity_usd": round(equity, 2),
            "high_water_mark": round(hwm, 2),
            "current_drawdown_pct": round(drawdown, 4),
            "max_drawdown_pct": self.max_drawdown_pct,
            "drawdown_breached": breached,
            "max_position_usd": self.max_position_usd,
            "mode": self.mode,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Trade methods
    # ------------------------------------------------------------------

    def _submit(self, order: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        """Send *order* when live; otherwise return it as a dry run."""
        if not self.live:
            log.info("DRY RUN Robinhood order (ROBINHOOD_LIVE_TRADING=false): {o}", o=order)
            return {"status": "dry_run", "order": order, **context,
                    "note": "ROBINHOOD_LIVE_TRADING is false — order built and risk-checked, not sent",
                    "timestamp": datetime.now(timezone.utc).isoformat()}
        result = self._request("POST", PATH_ORDERS, body=order)
        if "error" in result:
            log.error("Robinhood order failed: {e}", e=result["error"])
            return {"status": "rejected", "order": order, **context, "error": result["error"]}
        log.info("Robinhood order accepted: id={i} state={s}", i=result.get("id"), s=result.get("state"))
        return {"status": "submitted", "order_id": result.get("id"), "state": result.get("state"),
                "order": order, **context, "raw_response": result,
                "timestamp": datetime.now(timezone.utc).isoformat()}

    def open_position(self, ticker: str, direction: str, size_usd: float) -> dict[str, Any]:
        """Market order for *size_usd* notional.

        LONG buys. SHORT sells held quantity (crypto spot cannot go net short).
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

        risk = self.check_risk_limits()
        if risk.get("drawdown_breached"):
            return {"error": f"Max drawdown breached: {risk.get('current_drawdown_pct', 0):.1%} "
                             f">= {self.max_drawdown_pct:.1%}. Trading halted."}

        try:
            symbol = normalize_symbol(ticker)
        except ValueError as exc:
            return {"error": str(exc)}

        quote = self.get_best_bid_ask([symbol]).get(symbol)
        if not quote:
            return {"error": f"No quote for {symbol} on Robinhood."}
        side = "buy" if direction == "LONG" else "sell"
        price = quote["ask"] if side == "buy" else quote["bid"]
        price = price or quote["mid"]
        if price <= 0:
            return {"error": f"Invalid price for {symbol}: {price}"}

        pair = self.get_trading_pair(symbol)
        if pair and str(pair.get("status", "tradable")).lower() not in ("tradable", "active"):
            return {"error": f"{symbol} is not tradable right now ({pair.get('status')})."}
        qty = round_down_to_increment(size_usd / price, pair.get("quantity_increment"))

        if side == "sell":
            held = next((p for p in self.get_positions() if p["symbol"] == symbol), None)
            if not held or held["available"] <= 0:
                return {"error": f"SHORT on Robinhood spot sells holdings only, and none of {symbol} is held."}
            qty = min(qty, round_down_to_increment(held["available"], pair.get("quantity_increment")))

        min_size = _to_float(pair.get("min_order_size"), 0.0)
        if qty <= 0 or (min_size and qty < Decimal(str(min_size))):
            return {"error": f"Quantity {format_quantity(qty)} below the {symbol} minimum order size {min_size}."}

        order = {
            "client_order_id": str(uuid.uuid4()),
            "side": side,
            "type": "market",
            "symbol": symbol,
            "market_order_config": {"asset_quantity": format_quantity(qty)},
        }
        context = {"symbol": symbol, "direction": direction, "size_usd": round(size_usd, 2),
                   "quantity": format_quantity(qty), "reference_price": round(price, 6)}
        log.info("Robinhood {d} {s} — ${u} ({q} @ ${p:.2f}) [{m}]",
                 d=direction, s=symbol, u=size_usd, q=context["quantity"], p=price, m=self.mode)
        return self._submit(order, context)

    def close_position(self, ticker: str) -> dict[str, Any]:
        """Sell the whole tradable holding of *ticker*."""
        try:
            symbol = normalize_symbol(ticker)
        except ValueError as exc:
            return {"error": str(exc)}
        if not self.configured:
            return {"error": "Robinhood API credentials not configured."}
        held = next((p for p in self.get_positions() if p["symbol"] == symbol), None)
        if not held or held["available"] <= 0:
            return {"error": f"No open position for {symbol}."}
        pair = self.get_trading_pair(symbol)
        qty = round_down_to_increment(held["available"], pair.get("quantity_increment"))
        if qty <= 0:
            return {"error": f"Holding of {symbol} is below the tradable increment."}
        order = {
            "client_order_id": str(uuid.uuid4()),
            "side": "sell",
            "type": "market",
            "symbol": symbol,
            "market_order_config": {"asset_quantity": format_quantity(qty)},
        }
        context = {"symbol": symbol, "direction": "CLOSE", "quantity": format_quantity(qty),
                   "size_usd": round(held["size_usd"], 2)}
        log.info("Robinhood close {s} — {q} [{m}]", s=symbol, q=context["quantity"], m=self.mode)
        return self._submit(order, context)

    def cancel_order(self, order_id: str) -> dict[str, Any]:
        """Cancel an open order."""
        if not self.live:
            return {"status": "dry_run", "order_id": order_id,
                    "note": "ROBINHOOD_LIVE_TRADING is false — nothing to cancel"}
        result = self._request("POST", f"{PATH_ORDERS}{order_id}/cancel/", body={})
        if "error" in result:
            return {"status": "rejected", "order_id": order_id, "error": result["error"]}
        return {"status": "cancel_requested", "order_id": order_id, "raw_response": result}

    def status(self) -> dict[str, Any]:
        """Connector status for the API and the health page."""
        out: dict[str, Any] = {
            "exchange": "robinhood_crypto",
            "mode": self.mode,
            "configured": self.configured,
            "live": self.live,
            "max_position_usd": self.max_position_usd,
            "max_drawdown_pct": self.max_drawdown_pct,
            "base_url": self.base_url,
        }
        if self.configured:
            out["account"] = self.get_account()
        return out


# ---------------------------------------------------------------------------
# Factory — build from GRID config
# ---------------------------------------------------------------------------


def get_robinhood_trader() -> RobinhoodCryptoTrader:
    """Instantiate RobinhoodCryptoTrader from GRID Settings (env vars)."""
    from config import settings

    return RobinhoodCryptoTrader(
        api_key=getattr(settings, "ROBINHOOD_API_KEY", ""),
        private_key_b64=getattr(settings, "ROBINHOOD_PRIVATE_KEY_B64", ""),
        live=bool(getattr(settings, "ROBINHOOD_LIVE_TRADING", False)),
        max_position_usd=float(getattr(settings, "ROBINHOOD_MAX_POSITION_USD", 100.0)),
        max_drawdown_pct=float(getattr(settings, "ROBINHOOD_MAX_DRAWDOWN_PCT", 0.20)),
        base_url=getattr(settings, "ROBINHOOD_BASE_URL", ROBINHOOD_BASE_URL),
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
