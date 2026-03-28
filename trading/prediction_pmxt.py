"""
GRID Unified Prediction Market Trader via pmxt SDK.

Wraps the pmxt SDK for unified trading across all supported prediction
market platforms (Polymarket, Kalshi, Limitless, Probable Markets,
Myriad Markets, Opinion Trade).

Enforces the same risk limits as the per-platform traders:
  MAX_SINGLE_TRADE_USD = 500
  MAX_PORTFOLIO_USD    = 5000

Falls back gracefully if pmxt is not installed.
"""

from __future__ import annotations

from typing import Any

from loguru import logger as log

from config import settings

# ── Graceful import of pmxt SDK ──────────────────────────────────────────

try:
    import pmxt

    _PMXT_AVAILABLE = True
except ImportError:
    pmxt = None  # type: ignore[assignment]
    _PMXT_AVAILABLE = False
    log.info("pmxt SDK not installed — run: pip install pmxt")


# ── Risk Limits ──────────────────────────────────────────────────────────

MAX_SINGLE_TRADE_USD: float = 500.0
MAX_PORTFOLIO_USD: float = 5000.0


# ── Trader ───────────────────────────────────────────────────────────────


class PmxtTrader:
    """Unified prediction market trader using the pmxt SDK.

    Provides market discovery, portfolio viewing, and order execution
    across all pmxt-supported platforms with consistent risk guardrails.

    Attributes:
        _configured: Whether at least one platform has credentials.
    """

    def __init__(self) -> None:
        """Initialise the pmxt trader with credentials from config."""
        self._configured = False

        if not _PMXT_AVAILABLE:
            log.warning("PmxtTrader: pmxt SDK not available")
            return

        try:
            self._configure_auth()
        except Exception as exc:
            log.error("PmxtTrader: auth configuration failed: {e}", e=exc)

    def _configure_auth(self) -> None:
        """Configure platform authentication from GRID settings.

        Reads PMXT_POLYMARKET_PRIVATE_KEY, PMXT_KALSHI_API_KEY, and
        PMXT_KALSHI_PRIVATE_KEY_PATH from config and passes them to the
        pmxt SDK.
        """
        if not _PMXT_AVAILABLE:
            return

        configured_any = False

        # Polymarket
        poly_key = settings.PMXT_POLYMARKET_PRIVATE_KEY
        if poly_key:
            try:
                pmxt.configure(
                    platform="polymarket",
                    private_key=poly_key,
                )
                configured_any = True
                log.info("PmxtTrader: Polymarket configured")
            except Exception as exc:
                log.warning("PmxtTrader: Polymarket config failed: {e}", e=exc)

        # Kalshi
        kalshi_api = settings.PMXT_KALSHI_API_KEY
        kalshi_pk_path = settings.PMXT_KALSHI_PRIVATE_KEY_PATH
        if kalshi_api and kalshi_pk_path:
            try:
                pmxt.configure(
                    platform="kalshi",
                    api_key=kalshi_api,
                    rsa_key=kalshi_pk_path,
                )
                configured_any = True
                log.info("PmxtTrader: Kalshi configured")
            except Exception as exc:
                log.warning("PmxtTrader: Kalshi config failed: {e}", e=exc)

        self._configured = configured_any

    def _require_pmxt(self) -> None:
        """Raise RuntimeError if pmxt is not available.

        Raises:
            RuntimeError: If pmxt SDK is not installed.
        """
        if not _PMXT_AVAILABLE:
            raise RuntimeError(
                "pmxt SDK not installed. Run: pip install pmxt"
            )

    # ------------------------------------------------------------------ #
    # Market Discovery
    # ------------------------------------------------------------------ #

    def get_markets(
        self,
        query: str,
        platform: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Search for prediction markets across platforms.

        Parameters:
            query: Search query string.
            platform: Optional platform filter.
            limit: Maximum results to return.

        Returns:
            List of market dicts with normalised fields, or empty list on error.
        """
        if not _PMXT_AVAILABLE:
            return []

        try:
            kwargs: dict[str, Any] = {"query": query}
            if platform:
                kwargs["platform"] = platform

            events = pmxt.fetch_events(**kwargs)
            if not events:
                return []

            results: list[dict[str, Any]] = []
            for event in events:
                markets = getattr(event, "markets", []) or []
                for market in markets:
                    outcomes = getattr(market, "outcomes", []) or []
                    outcome_data = []
                    for outcome in outcomes:
                        outcome_data.append({
                            "name": getattr(outcome, "name", ""),
                            "price": getattr(outcome, "yes_price", None)
                            or getattr(outcome, "price", None),
                        })

                    results.append({
                        "event_id": getattr(event, "id", ""),
                        "title": getattr(event, "title", ""),
                        "platform": getattr(event, "platform", ""),
                        "market_id": getattr(market, "id", ""),
                        "outcomes": outcome_data,
                        "volume": getattr(market, "volume", 0),
                    })

                    if len(results) >= limit:
                        break
                if len(results) >= limit:
                    break

            return results[:limit]

        except Exception as exc:
            log.error("PmxtTrader get_markets failed: {e}", e=exc)
            return []

    def get_market(
        self,
        event_id: str,
        platform: str | None = None,
    ) -> dict[str, Any]:
        """Get details for a specific market/event.

        Parameters:
            event_id: The event identifier.
            platform: Optional platform hint.

        Returns:
            Market detail dict, or error dict on failure.
        """
        if not _PMXT_AVAILABLE:
            return {"error": "pmxt SDK not installed"}

        try:
            kwargs: dict[str, Any] = {"query": event_id}
            if platform:
                kwargs["platform"] = platform

            events = pmxt.fetch_events(**kwargs)
            if not events:
                return {"error": f"Event {event_id} not found"}

            event = events[0]
            markets = getattr(event, "markets", []) or []

            return {
                "event_id": getattr(event, "id", ""),
                "title": getattr(event, "title", ""),
                "description": getattr(event, "description", ""),
                "platform": getattr(event, "platform", ""),
                "markets": [
                    {
                        "market_id": getattr(m, "id", ""),
                        "outcomes": [
                            {
                                "name": getattr(o, "name", ""),
                                "price": getattr(o, "yes_price", None)
                                or getattr(o, "price", None),
                            }
                            for o in (getattr(m, "outcomes", []) or [])
                        ],
                        "volume": getattr(m, "volume", 0),
                    }
                    for m in markets
                ],
            }

        except Exception as exc:
            log.error("PmxtTrader get_market failed: {e}", e=exc)
            return {"error": str(exc)}

    # ------------------------------------------------------------------ #
    # Portfolio
    # ------------------------------------------------------------------ #

    def get_portfolio(self) -> dict[str, Any]:
        """Get portfolio balance and positions across configured platforms.

        Returns:
            Portfolio dict with balance and positions, or error dict.
        """
        if not _PMXT_AVAILABLE:
            return {"error": "pmxt SDK not installed", "positions": []}

        if not self._configured:
            return {"error": "No platforms configured", "positions": []}

        try:
            balance = pmxt.fetch_balance()
            return {
                "balance": getattr(balance, "total", 0) if balance else 0,
                "available": getattr(balance, "available", 0) if balance else 0,
                "positions": getattr(balance, "positions", []) if balance else [],
            }
        except Exception as exc:
            log.error("PmxtTrader get_portfolio failed: {e}", e=exc)
            return {"error": str(exc), "positions": []}

    # ------------------------------------------------------------------ #
    # Trading
    # ------------------------------------------------------------------ #

    def buy(
        self,
        event_id: str,
        outcome: str,
        amount_usd: float,
        platform: str | None = None,
    ) -> dict[str, Any]:
        """Buy outcome shares on a prediction market.

        Parameters:
            event_id: The event/market identifier.
            outcome: Outcome name (e.g. 'Yes', 'No').
            amount_usd: Dollar amount to spend.
            platform: Optional platform hint.

        Returns:
            Order result dict, or error dict on failure.
        """
        if not _PMXT_AVAILABLE:
            return {"error": "pmxt SDK not installed"}

        if not self._configured:
            return {"error": "No platforms configured for trading"}

        # Risk guards
        if amount_usd <= 0:
            return {"error": "Amount must be positive"}

        if amount_usd > MAX_SINGLE_TRADE_USD:
            return {
                "error": f"Amount ${amount_usd:.2f} exceeds single-trade limit "
                         f"${MAX_SINGLE_TRADE_USD:.2f}"
            }

        # Portfolio limit check
        portfolio = self.get_portfolio()
        current_value = portfolio.get("balance", 0) or 0
        if current_value + amount_usd > MAX_PORTFOLIO_USD:
            return {
                "error": f"Trade would exceed portfolio limit "
                         f"${MAX_PORTFOLIO_USD:.2f} "
                         f"(current: ${current_value:.2f})"
            }

        try:
            order = pmxt.create_order(
                event_id=event_id,
                outcome=outcome,
                side="buy",
                amount=amount_usd,
                platform=platform,
            )

            log.info(
                "PmxtTrader BUY: {outcome} on {eid} — ${amt:.2f}",
                outcome=outcome,
                eid=event_id[:20],
                amt=amount_usd,
            )

            return {
                "status": "submitted",
                "event_id": event_id,
                "outcome": outcome,
                "amount_usd": amount_usd,
                "platform": platform,
                "order": order if isinstance(order, dict) else str(order),
            }

        except Exception as exc:
            log.error("PmxtTrader buy failed: {e}", e=exc)
            return {"error": str(exc)}

    def sell(
        self,
        event_id: str,
        outcome: str,
        amount_usd: float,
        platform: str | None = None,
    ) -> dict[str, Any]:
        """Sell outcome shares on a prediction market.

        Parameters:
            event_id: The event/market identifier.
            outcome: Outcome name (e.g. 'Yes', 'No').
            amount_usd: Dollar amount worth of shares to sell.
            platform: Optional platform hint.

        Returns:
            Order result dict, or error dict on failure.
        """
        if not _PMXT_AVAILABLE:
            return {"error": "pmxt SDK not installed"}

        if not self._configured:
            return {"error": "No platforms configured for trading"}

        if amount_usd <= 0:
            return {"error": "Amount must be positive"}

        if amount_usd > MAX_SINGLE_TRADE_USD:
            return {
                "error": f"Amount ${amount_usd:.2f} exceeds single-trade limit "
                         f"${MAX_SINGLE_TRADE_USD:.2f}"
            }

        try:
            order = pmxt.create_order(
                event_id=event_id,
                outcome=outcome,
                side="sell",
                amount=amount_usd,
                platform=platform,
            )

            log.info(
                "PmxtTrader SELL: {outcome} on {eid} — ${amt:.2f}",
                outcome=outcome,
                eid=event_id[:20],
                amt=amount_usd,
            )

            return {
                "status": "submitted",
                "event_id": event_id,
                "outcome": outcome,
                "amount_usd": amount_usd,
                "platform": platform,
                "order": order if isinstance(order, dict) else str(order),
            }

        except Exception as exc:
            log.error("PmxtTrader sell failed: {e}", e=exc)
            return {"error": str(exc)}
