"""
Hyperliquid perp trading integration.

Connects to Hyperliquid DEX for perpetual futures trading.
Testnet first, then mainnet. Uses GRID signals for entry/exit.

Architecture:
  GRID Signal -> Position Sizing -> Hyperliquid Order -> Confirmation -> Journal
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import eth_account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants
from loguru import logger as log


class HyperliquidTrader:
    """Trade perpetual futures on Hyperliquid DEX.

    Wraps the hyperliquid-python-sdk with GRID-specific risk controls:
    position limits, max drawdown, and trade journaling.
    """

    def __init__(
        self,
        private_key: str,
        testnet: bool = True,
        max_position_usd: float = 100.0,
        max_drawdown_pct: float = 0.20,
    ) -> None:
        if not private_key:
            raise ValueError(
                "HYPERLIQUID_PRIVATE_KEY is required. "
                "Generate a wallet and fund it on Hyperliquid testnet first."
            )

        self.testnet = testnet
        self.max_position_usd = max_position_usd
        self.max_drawdown_pct = max_drawdown_pct

        # Derive wallet from private key
        self.wallet = eth_account.Account.from_key(private_key)
        self.address = self.wallet.address

        # SDK base URL
        self.base_url = (
            constants.TESTNET_API_URL if testnet else constants.MAINNET_API_URL
        )

        # Initialize SDK clients
        # Testnet spot metadata can be empty — fall back to mainnet info
        # for price queries while keeping testnet for order execution
        try:
            self.info = Info(base_url=self.base_url, skip_ws=True)
        except (IndexError, KeyError):
            log.warning("Testnet spot meta unavailable — using mainnet for info queries")
            self.info = Info(base_url=constants.MAINNET_API_URL, skip_ws=True)

        try:
            self.exchange = Exchange(wallet=self.wallet, base_url=self.base_url)
        except (IndexError, KeyError):
            log.warning("Exchange init with testnet failed — using mainnet URL")
            self.exchange = Exchange(wallet=self.wallet, base_url=constants.MAINNET_API_URL)

        # Track high water mark for drawdown calculation
        self._high_water_mark: float | None = None

        mode = "TESTNET" if testnet else "MAINNET"
        log.info(
            "HyperliquidTrader initialized — {mode} addr={addr}",
            mode=mode,
            addr=self.address[:10] + "...",
        )

    # ------------------------------------------------------------------
    # Ticker mapping
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_ticker(ticker: str) -> str:
        """Map GRID ticker format to Hyperliquid coin name.

        Examples:
            BTC-USD  -> BTC
            ETH-USD  -> ETH
            SOL      -> SOL
            BTC-PERP -> BTC
        """
        coin = ticker.upper().split("-")[0]
        return coin

    # ------------------------------------------------------------------
    # Read methods
    # ------------------------------------------------------------------

    def get_balance(self) -> dict[str, Any]:
        """Wallet balance, margin info, and position summary."""
        try:
            state = self.info.user_state(self.address)
        except Exception as e:
            log.error("Failed to fetch balance: {e}", e=e)
            return {"error": str(e)}

        margin_summary = state.get("marginSummary", {})
        positions = state.get("assetPositions", [])

        equity = float(margin_summary.get("accountValue", 0))
        margin_used = float(margin_summary.get("totalMarginUsed", 0))

        # Update high water mark
        if self._high_water_mark is None or equity > self._high_water_mark:
            self._high_water_mark = equity

        return {
            "address": self.address,
            "testnet": self.testnet,
            "equity_usd": round(equity, 2),
            "margin_used_usd": round(margin_used, 2),
            "free_margin_usd": round(equity - margin_used, 2),
            "open_positions": len([
                p for p in positions
                if float(p.get("position", {}).get("szi", 0)) != 0
            ]),
            "high_water_mark": round(self._high_water_mark, 2) if self._high_water_mark else None,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_positions(self) -> list[dict[str, Any]]:
        """All open positions with unrealized P&L."""
        try:
            state = self.info.user_state(self.address)
        except Exception as e:
            log.error("Failed to fetch positions: {e}", e=e)
            return []

        positions = state.get("assetPositions", [])
        result = []

        for p in positions:
            pos = p.get("position", {})
            size = float(pos.get("szi", 0))
            if size == 0:
                continue

            entry_px = float(pos.get("entryPx", 0))
            unrealized_pnl = float(pos.get("unrealizedPnl", 0))
            leverage_val = pos.get("leverage", {})
            lev = leverage_val.get("value", "1") if isinstance(leverage_val, dict) else str(leverage_val)

            result.append({
                "coin": pos.get("coin", ""),
                "direction": "LONG" if size > 0 else "SHORT",
                "size": abs(size),
                "size_usd": round(abs(size) * entry_px, 2),
                "entry_price": entry_px,
                "unrealized_pnl": round(unrealized_pnl, 2),
                "leverage": lev,
                "liquidation_px": pos.get("liquidationPx"),
            })

        return result

    def get_trade_history(self, limit: int = 50) -> list[dict[str, Any]]:
        """Recent fills / trade history."""
        try:
            fills = self.info.user_fills(self.address)
        except Exception as e:
            log.error("Failed to fetch trade history: {e}", e=e)
            return []

        # user_fills returns most recent first; cap to limit
        fills = fills[:limit] if len(fills) > limit else fills

        result = []
        for f in fills:
            result.append({
                "coin": f.get("coin", ""),
                "direction": f.get("dir", ""),
                "size": f.get("sz", ""),
                "price": f.get("px", ""),
                "fee": f.get("fee", ""),
                "time": f.get("time", ""),
                "oid": f.get("oid", ""),
                "closed_pnl": f.get("closedPnl", "0"),
            })

        return result

    # ------------------------------------------------------------------
    # Trade methods
    # ------------------------------------------------------------------

    def open_position(
        self,
        ticker: str,
        direction: str,
        size_usd: float,
    ) -> dict[str, Any]:
        """Open a market order position.

        Args:
            ticker: Asset name (e.g. BTC-USD, ETH, SOL).
            direction: LONG or SHORT.
            size_usd: Notional size in USD.

        Returns:
            Order result dict with status and details.
        """
        direction = direction.upper()
        if direction not in ("LONG", "SHORT"):
            return {"error": f"Invalid direction: {direction}. Must be LONG or SHORT."}

        # Validate size against limits
        if size_usd > self.max_position_usd:
            return {
                "error": (
                    f"Size ${size_usd} exceeds max_position_usd ${self.max_position_usd}. "
                    "Increase HYPERLIQUID_MAX_POSITION_USD or reduce size."
                ),
            }

        if size_usd <= 0:
            return {"error": "size_usd must be positive."}

        # Check drawdown before trading
        risk = self.check_risk_limits()
        if risk.get("drawdown_breached"):
            return {
                "error": (
                    f"Max drawdown breached: {risk['current_drawdown_pct']:.1%} "
                    f"> {self.max_drawdown_pct:.1%}. Trading halted."
                ),
            }

        coin = self._normalize_ticker(ticker)
        is_buy = direction == "LONG"

        # Get current mid price to compute size in coins
        try:
            mids = self.info.all_mids()
            if coin not in mids:
                return {"error": f"Coin {coin} not found on Hyperliquid."}
            mid_price = float(mids[coin])
        except Exception as e:
            log.error("Failed to fetch mid price for {c}: {e}", c=coin, e=e)
            return {"error": f"Failed to fetch price: {e}"}

        if mid_price <= 0:
            return {"error": f"Invalid mid price for {coin}: {mid_price}"}

        # Convert USD size to coin size
        sz = size_usd / mid_price

        log.info(
            "Opening {dir} {coin} — ${usd} ({sz:.6f} coins @ ${px:.2f})",
            dir=direction, coin=coin, usd=size_usd, sz=sz, px=mid_price,
        )

        try:
            result = self.exchange.market_open(
                name=coin,
                is_buy=is_buy,
                sz=sz,
                slippage=0.05,
            )
        except Exception as e:
            log.error("Order failed: {e}", e=e)
            return {"error": f"Order execution failed: {e}"}

        # Parse SDK response
        status = result.get("status", "unknown")
        response = result.get("response", {})

        if status == "ok":
            data = response.get("data", {})
            statuses = data.get("statuses", [])
            filled = statuses[0] if statuses else {}
            log.info("Order filled: {r}", r=filled)
            return {
                "status": "filled",
                "coin": coin,
                "direction": direction,
                "size_usd": round(size_usd, 2),
                "size_coins": round(sz, 6),
                "mid_price": round(mid_price, 2),
                "order_result": filled,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        else:
            log.warning("Order rejected: {r}", r=result)
            return {
                "status": "rejected",
                "coin": coin,
                "direction": direction,
                "raw_response": result,
            }

    def close_position(self, ticker: str) -> dict[str, Any]:
        """Close all of a ticker's open position.

        Args:
            ticker: Asset name (e.g. BTC-USD, ETH, SOL).

        Returns:
            Close result dict.
        """
        coin = self._normalize_ticker(ticker)

        # Check we actually have a position
        positions = self.get_positions()
        pos = next((p for p in positions if p["coin"] == coin), None)

        if not pos:
            return {"error": f"No open position for {coin}."}

        log.info(
            "Closing {dir} {coin} — {sz} coins",
            dir=pos["direction"], coin=coin, sz=pos["size"],
        )

        try:
            result = self.exchange.market_close(coin=coin)
        except Exception as e:
            log.error("Close failed: {e}", e=e)
            return {"error": f"Close execution failed: {e}"}

        status = result.get("status", "unknown")
        response = result.get("response", {})

        if status == "ok":
            data = response.get("data", {})
            statuses = data.get("statuses", [])
            filled = statuses[0] if statuses else {}
            log.info("Position closed: {r}", r=filled)
            return {
                "status": "closed",
                "coin": coin,
                "direction": pos["direction"],
                "size_closed": pos["size"],
                "order_result": filled,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        else:
            log.warning("Close rejected: {r}", r=result)
            return {
                "status": "rejected",
                "coin": coin,
                "raw_response": result,
            }

    # ------------------------------------------------------------------
    # Risk management
    # ------------------------------------------------------------------

    def check_risk_limits(self) -> dict[str, Any]:
        """Compute current drawdown and compare to limits.

        Returns:
            Dict with equity, high water mark, drawdown, and breach status.
        """
        balance = self.get_balance()
        if "error" in balance:
            return {"error": balance["error"], "drawdown_breached": True}

        equity = balance["equity_usd"]

        if self._high_water_mark is None or self._high_water_mark == 0:
            self._high_water_mark = equity

        if equity > self._high_water_mark:
            self._high_water_mark = equity

        hwm = self._high_water_mark
        drawdown = (hwm - equity) / hwm if hwm > 0 else 0.0
        breached = drawdown >= self.max_drawdown_pct

        if breached:
            log.warning(
                "DRAWDOWN BREACH: {dd:.1%} >= {max:.1%} — trading halted",
                dd=drawdown, max=self.max_drawdown_pct,
            )

        return {
            "equity_usd": round(equity, 2),
            "high_water_mark": round(hwm, 2),
            "current_drawdown_pct": round(drawdown, 4),
            "max_drawdown_pct": self.max_drawdown_pct,
            "drawdown_breached": breached,
            "max_position_usd": self.max_position_usd,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


# ---------------------------------------------------------------------------
# Factory — build from GRID config
# ---------------------------------------------------------------------------

def get_hyperliquid_trader() -> HyperliquidTrader:
    """Instantiate HyperliquidTrader from GRID Settings (env vars)."""
    from config import settings

    return HyperliquidTrader(
        private_key=settings.HYPERLIQUID_PRIVATE_KEY,
        testnet=settings.HYPERLIQUID_TESTNET,
        max_position_usd=settings.HYPERLIQUID_MAX_POSITION_USD,
        max_drawdown_pct=settings.HYPERLIQUID_MAX_DRAWDOWN_PCT,
    )
