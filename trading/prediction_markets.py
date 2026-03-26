"""
GRID Prediction Market Integration — Polymarket + Kalshi.

Two exchange connectors for prediction market trading:
  - PolymarketTrader: Uses py-clob-client for Polymarket's CLOB API
  - KalshiTrader: REST API with JWT authentication

EXCH-02 (Polymarket) and EXCH-03 (Kalshi) from the exchange integration roadmap.

NOTE: Requires `pip install py-clob-client` for Polymarket support.
"""

from __future__ import annotations

import time
from typing import Any

import httpx
from loguru import logger as log

from config import settings


# ---------------------------------------------------------------------------
# Risk limits shared across prediction market traders
# ---------------------------------------------------------------------------
MAX_SINGLE_TRADE_USD = 500.0
MAX_PORTFOLIO_USD = 5000.0


# ---------------------------------------------------------------------------
# Polymarket — CLOB API via py-clob-client
# ---------------------------------------------------------------------------

class PolymarketTrader:
    """Polymarket prediction market trader using the CLOB API.

    Requires:
      pip install py-clob-client

    Env vars:
      POLYMARKET_API_KEY — API key from Polymarket
      POLYMARKET_PRIVATE_KEY — Ethereum private key for signing
    """

    def __init__(
        self,
        api_key: str | None = None,
        private_key: str | None = None,
    ) -> None:
        self.api_key = api_key or settings.POLYMARKET_API_KEY
        self.private_key = private_key or settings.POLYMARKET_PRIVATE_KEY

        self._client = None
        if self.api_key and self.private_key:
            self._init_client()

    def _init_client(self) -> None:
        """Lazy-init the py-clob-client ClobClient."""
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds

            host = "https://clob.polymarket.com"
            chain_id = 137  # Polygon mainnet

            self._client = ClobClient(
                host,
                key=self.private_key,
                chain_id=chain_id,
                creds=ApiCreds(
                    api_key=self.api_key,
                    api_secret="",       # populated by derive_api_key if needed
                    api_passphrase="",
                ),
            )
            log.info("Polymarket CLOB client initialised")
        except ImportError:
            log.warning(
                "py-clob-client not installed — run: pip install py-clob-client"
            )
            self._client = None
        except Exception as exc:
            log.error("Failed to init Polymarket client: {e}", e=exc)
            self._client = None

    def _require_client(self) -> Any:
        if self._client is None:
            raise RuntimeError(
                "Polymarket client not initialised. "
                "Set POLYMARKET_API_KEY and POLYMARKET_PRIVATE_KEY, "
                "and install py-clob-client."
            )
        return self._client

    # ---- Market discovery ------------------------------------------------

    def get_markets(self, query: str | None = None, limit: int = 20) -> list[dict]:
        """Search or list active Polymarket markets.

        Uses the Gamma API (public REST) for market discovery since the
        CLOB client is primarily for order execution.
        """
        params: dict[str, Any] = {
            "limit": limit,
            "active": True,
            "closed": False,
        }
        if query:
            params["tag"] = query

        try:
            resp = httpx.get(
                "https://gamma-api.polymarket.com/markets",
                params=params,
                timeout=15,
            )
            resp.raise_for_status()
            markets = resp.json()
            return [
                {
                    "condition_id": m.get("conditionId", m.get("condition_id", "")),
                    "question": m.get("question", ""),
                    "description": m.get("description", "")[:200],
                    "outcomes": m.get("outcomes", []),
                    "outcome_prices": m.get("outcomePrices", m.get("outcome_prices", [])),
                    "volume": m.get("volume", 0),
                    "liquidity": m.get("liquidity", 0),
                    "end_date": m.get("endDate", m.get("end_date", "")),
                    "active": m.get("active", True),
                }
                for m in (markets if isinstance(markets, list) else [])
            ][:limit]
        except Exception as exc:
            log.error("Polymarket get_markets failed: {e}", e=exc)
            return []

    def get_market(self, condition_id: str) -> dict:
        """Get single market details with current prices."""
        try:
            resp = httpx.get(
                f"https://gamma-api.polymarket.com/markets/{condition_id}",
                timeout=15,
            )
            resp.raise_for_status()
            m = resp.json()
            return {
                "condition_id": m.get("conditionId", m.get("condition_id", "")),
                "question": m.get("question", ""),
                "description": m.get("description", ""),
                "outcomes": m.get("outcomes", []),
                "outcome_prices": m.get("outcomePrices", m.get("outcome_prices", [])),
                "volume": m.get("volume", 0),
                "liquidity": m.get("liquidity", 0),
                "end_date": m.get("endDate", m.get("end_date", "")),
                "active": m.get("active", True),
            }
        except Exception as exc:
            log.error("Polymarket get_market failed: {e}", e=exc)
            return {"error": str(exc)}

    # ---- Positions -------------------------------------------------------

    def get_position(self, condition_id: str) -> dict:
        """Get current position and P&L for a specific market."""
        client = self._require_client()
        try:
            # py-clob-client does not have a direct position endpoint;
            # query open orders and balances via the REST layer.
            orders = client.get_orders(
                params={"market": condition_id, "open": True}
            )
            return {
                "condition_id": condition_id,
                "open_orders": orders if isinstance(orders, list) else [],
            }
        except Exception as exc:
            log.error("Polymarket get_position failed: {e}", e=exc)
            return {"condition_id": condition_id, "error": str(exc)}

    def get_portfolio(self) -> dict:
        """Get all positions with total portfolio value."""
        client = self._require_client()
        try:
            orders = client.get_orders()
            positions = orders if isinstance(orders, list) else []
            return {
                "positions": positions,
                "total_positions": len(positions),
            }
        except Exception as exc:
            log.error("Polymarket get_portfolio failed: {e}", e=exc)
            return {"positions": [], "error": str(exc)}

    # ---- Trading ---------------------------------------------------------

    def buy(
        self,
        condition_id: str,
        outcome: str,
        amount_usd: float,
    ) -> dict:
        """Buy outcome shares on a Polymarket market.

        Args:
            condition_id: The market condition ID.
            outcome: 'Yes' or 'No'.
            amount_usd: Dollar amount to spend.

        Returns dict with order details or error.
        """
        # Risk guard
        if amount_usd > MAX_SINGLE_TRADE_USD:
            return {
                "error": f"Amount ${amount_usd} exceeds single-trade limit "
                         f"${MAX_SINGLE_TRADE_USD}"
            }
        if amount_usd <= 0:
            return {"error": "Amount must be positive"}

        client = self._require_client()
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY

            # Resolve token_id for the outcome
            market = self.get_market(condition_id)
            outcomes = market.get("outcomes", [])
            prices = market.get("outcome_prices", [])

            if outcome not in ("Yes", "No"):
                return {"error": f"Invalid outcome '{outcome}', must be 'Yes' or 'No'"}

            idx = 0 if outcome == "Yes" else 1
            if idx >= len(prices):
                return {"error": "Could not resolve price for outcome"}

            price = float(prices[idx])
            if price <= 0 or price >= 1:
                return {"error": f"Invalid price {price} for outcome {outcome}"}

            size = round(amount_usd / price, 2)

            order_args = OrderArgs(
                price=price,
                size=size,
                side=BUY,
                token_id=condition_id,
            )
            signed_order = client.create_order(order_args)
            result = client.post_order(signed_order, OrderType.GTC)

            log.info(
                "Polymarket BUY: {outcome} on {cid} — ${amt} ({sz} shares @ {p})",
                outcome=outcome, cid=condition_id[:12],
                amt=amount_usd, sz=size, p=price,
            )
            return {
                "status": "filled",
                "condition_id": condition_id,
                "outcome": outcome,
                "amount_usd": amount_usd,
                "price": price,
                "size": size,
                "order": result,
            }
        except ImportError:
            return {"error": "py-clob-client not installed"}
        except Exception as exc:
            log.error("Polymarket buy failed: {e}", e=exc)
            return {"error": str(exc)}

    def sell(
        self,
        condition_id: str,
        outcome: str,
        amount: float,
    ) -> dict:
        """Sell outcome shares on a Polymarket market.

        Args:
            condition_id: The market condition ID.
            outcome: 'Yes' or 'No'.
            amount: Number of shares to sell.
        """
        if amount <= 0:
            return {"error": "Amount must be positive"}

        client = self._require_client()
        try:
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import SELL

            market = self.get_market(condition_id)
            prices = market.get("outcome_prices", [])
            idx = 0 if outcome == "Yes" else 1
            price = float(prices[idx]) if idx < len(prices) else 0.5

            order_args = OrderArgs(
                price=price,
                size=amount,
                side=SELL,
                token_id=condition_id,
            )
            signed_order = client.create_order(order_args)
            result = client.post_order(signed_order, OrderType.GTC)

            log.info(
                "Polymarket SELL: {outcome} on {cid} — {amt} shares @ {p}",
                outcome=outcome, cid=condition_id[:12], amt=amount, p=price,
            )
            return {
                "status": "filled",
                "condition_id": condition_id,
                "outcome": outcome,
                "amount": amount,
                "price": price,
                "order": result,
            }
        except ImportError:
            return {"error": "py-clob-client not installed"}
        except Exception as exc:
            log.error("Polymarket sell failed: {e}", e=exc)
            return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Kalshi — REST API with JWT auth
# ---------------------------------------------------------------------------

class KalshiTrader:
    """Kalshi prediction market trader using REST API v2.

    Uses JWT authentication (email/password login).

    Env vars:
      KALSHI_EMAIL — Kalshi account email
      KALSHI_PASSWORD — Kalshi account password
    """

    BASE_URL = "https://trading-api.kalshi.com/trade-api/v2"

    def __init__(
        self,
        email: str | None = None,
        password: str | None = None,
    ) -> None:
        self.email = email or settings.KALSHI_EMAIL
        self.password = password or settings.KALSHI_PASSWORD
        self._token: str | None = None
        self._token_expiry: float = 0.0
        self._http = httpx.Client(
            base_url=self.BASE_URL,
            timeout=20,
        )

    # ---- Auth ------------------------------------------------------------

    def _authenticate(self) -> str:
        """POST /login to get JWT token. Caches until expiry."""
        if self._token and time.time() < self._token_expiry:
            return self._token

        if not self.email or not self.password:
            raise RuntimeError(
                "Kalshi credentials not set. "
                "Set KALSHI_EMAIL and KALSHI_PASSWORD in .env."
            )

        resp = self._http.post(
            "/login",
            json={"email": self.email, "password": self.password},
        )
        resp.raise_for_status()
        data = resp.json()

        self._token = data.get("token", "")
        # Kalshi tokens last ~24h; refresh at 23h
        self._token_expiry = time.time() + 23 * 3600

        log.info("Kalshi authenticated as {email}", email=self.email)
        return self._token

    def _headers(self) -> dict[str, str]:
        """Return auth headers with current JWT."""
        token = self._authenticate()
        return {"Authorization": f"Bearer {token}"}

    # ---- Event discovery -------------------------------------------------

    def get_events(
        self, category: str | None = None, limit: int = 20,
    ) -> list[dict]:
        """Get active events/contracts from Kalshi."""
        try:
            params: dict[str, Any] = {"limit": limit, "status": "open"}
            if category:
                params["series_ticker"] = category

            resp = self._http.get(
                "/events", headers=self._headers(), params=params,
            )
            resp.raise_for_status()
            data = resp.json()

            events = data.get("events", [])
            return [
                {
                    "event_ticker": e.get("event_ticker", ""),
                    "title": e.get("title", ""),
                    "category": e.get("category", ""),
                    "series_ticker": e.get("series_ticker", ""),
                    "markets": [
                        {
                            "ticker": m.get("ticker", ""),
                            "subtitle": m.get("subtitle", ""),
                            "yes_bid": m.get("yes_bid", 0),
                            "yes_ask": m.get("yes_ask", 0),
                            "no_bid": m.get("no_bid", 0),
                            "no_ask": m.get("no_ask", 0),
                            "volume": m.get("volume", 0),
                        }
                        for m in e.get("markets", [])
                    ],
                }
                for e in events
            ][:limit]
        except Exception as exc:
            log.error("Kalshi get_events failed: {e}", e=exc)
            return []

    def get_event(self, event_ticker: str) -> dict:
        """Get single event with market details."""
        try:
            resp = self._http.get(
                f"/events/{event_ticker}", headers=self._headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            e = data.get("event", data)
            return {
                "event_ticker": e.get("event_ticker", ""),
                "title": e.get("title", ""),
                "category": e.get("category", ""),
                "series_ticker": e.get("series_ticker", ""),
                "markets": e.get("markets", []),
            }
        except Exception as exc:
            log.error("Kalshi get_event failed: {e}", e=exc)
            return {"error": str(exc)}

    # ---- Positions -------------------------------------------------------

    def get_position(self, event_ticker: str) -> dict:
        """Get current position for a specific event."""
        try:
            resp = self._http.get(
                "/portfolio/positions",
                headers=self._headers(),
                params={"event_ticker": event_ticker},
            )
            resp.raise_for_status()
            data = resp.json()
            positions = data.get("market_positions", [])
            return {
                "event_ticker": event_ticker,
                "positions": positions,
            }
        except Exception as exc:
            log.error("Kalshi get_position failed: {e}", e=exc)
            return {"event_ticker": event_ticker, "error": str(exc)}

    def get_portfolio(self) -> dict:
        """Get all positions across all events."""
        try:
            resp = self._http.get(
                "/portfolio/positions", headers=self._headers(),
            )
            resp.raise_for_status()
            data = resp.json()
            positions = data.get("market_positions", [])
            return {
                "positions": positions,
                "total_positions": len(positions),
            }
        except Exception as exc:
            log.error("Kalshi get_portfolio failed: {e}", e=exc)
            return {"positions": [], "error": str(exc)}

    def get_balance(self) -> dict:
        """Get account balance."""
        try:
            resp = self._http.get(
                "/portfolio/balance", headers=self._headers(),
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            log.error("Kalshi get_balance failed: {e}", e=exc)
            return {"error": str(exc)}

    # ---- Trading ---------------------------------------------------------

    def buy(
        self,
        event_ticker: str,
        side: str,
        contracts: int,
        price_cents: int,
    ) -> dict:
        """Buy contracts on a Kalshi event.

        Args:
            event_ticker: The market ticker (e.g. 'KXBTCD-26MAR28-T50000').
            side: 'yes' or 'no'.
            contracts: Number of contracts to buy.
            price_cents: Limit price in cents (1-99).
        """
        # Validate
        if side.lower() not in ("yes", "no"):
            return {"error": f"Invalid side '{side}', must be 'yes' or 'no'"}
        if contracts <= 0:
            return {"error": "Contracts must be positive"}
        if not (1 <= price_cents <= 99):
            return {"error": f"Price {price_cents}c out of range (1-99)"}

        # Risk guard
        cost_usd = contracts * price_cents / 100
        if cost_usd > MAX_SINGLE_TRADE_USD:
            return {
                "error": f"Cost ${cost_usd:.2f} exceeds single-trade limit "
                         f"${MAX_SINGLE_TRADE_USD}"
            }

        try:
            body = {
                "ticker": event_ticker,
                "action": "buy",
                "side": side.lower(),
                "count": contracts,
                "type": "limit",
                "yes_price": price_cents if side.lower() == "yes" else None,
                "no_price": price_cents if side.lower() == "no" else None,
            }
            # Remove None values
            body = {k: v for k, v in body.items() if v is not None}

            resp = self._http.post(
                "/portfolio/orders", headers=self._headers(), json=body,
            )
            resp.raise_for_status()
            data = resp.json()

            log.info(
                "Kalshi BUY: {side} {n}x {tk} @ {p}c (${cost:.2f})",
                side=side, n=contracts, tk=event_ticker,
                p=price_cents, cost=cost_usd,
            )
            return {
                "status": "submitted",
                "event_ticker": event_ticker,
                "side": side,
                "contracts": contracts,
                "price_cents": price_cents,
                "cost_usd": cost_usd,
                "order": data.get("order", data),
            }
        except Exception as exc:
            log.error("Kalshi buy failed: {e}", e=exc)
            return {"error": str(exc)}

    def sell(
        self,
        event_ticker: str,
        contracts: int,
        price_cents: int,
    ) -> dict:
        """Sell contracts on a Kalshi event.

        Args:
            event_ticker: The market ticker.
            contracts: Number of contracts to sell.
            price_cents: Limit price in cents (1-99).
        """
        if contracts <= 0:
            return {"error": "Contracts must be positive"}
        if not (1 <= price_cents <= 99):
            return {"error": f"Price {price_cents}c out of range (1-99)"}

        try:
            body = {
                "ticker": event_ticker,
                "action": "sell",
                "side": "yes",
                "count": contracts,
                "type": "limit",
                "yes_price": price_cents,
            }

            resp = self._http.post(
                "/portfolio/orders", headers=self._headers(), json=body,
            )
            resp.raise_for_status()
            data = resp.json()

            log.info(
                "Kalshi SELL: {n}x {tk} @ {p}c",
                n=contracts, tk=event_ticker, p=price_cents,
            )
            return {
                "status": "submitted",
                "event_ticker": event_ticker,
                "contracts": contracts,
                "price_cents": price_cents,
                "order": data.get("order", data),
            }
        except Exception as exc:
            log.error("Kalshi sell failed: {e}", e=exc)
            return {"error": str(exc)}
