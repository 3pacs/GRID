#!/usr/bin/env python3
"""
Live Rotation Trader — bridges GRID rotation signals to a crypto venue.

Maps the Adaptive Rotation regime to crypto allocations:
  - risk-on:  80% BTC, 20% ETH (aggressive)
  - neutral:  50% BTC, 50% cash (defensive)
  - risk-off: 100% cash (flat)

Two venues share the same target-weight logic:
  - ``hyperliquid`` (default) — perps, testnet unless ``--mainnet``.
  - ``robinhood`` — crypto **spot**: long only, risk-off sells to cash, and
    every order stays dry-run until ``ROBINHOOD_LIVE_TRADING`` is true.

Phase A: Testnet ($100 fake money)
Phase B: Mainnet ($100 real money)

Usage:
    python3 scripts/live_rotation_trader.py                    # Execute trades
    python3 scripts/live_rotation_trader.py --status           # Check positions
    python3 scripts/live_rotation_trader.py --mainnet          # Use real money (Phase B)
    python3 scripts/live_rotation_trader.py --venue robinhood  # Robinhood spot
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log
from sqlalchemy import text

from db import get_engine
from alpha_research.strategies.adaptive_rotation import run_rotation

# trading.hyperliquid pulls in the eth_account/hyperliquid SDKs at import time.
# They are only needed for the perp venue, so the import lives in _get_trader —
# a Robinhood-only run must not require the Hyperliquid stack to be installed.


# ── Regime → Crypto Allocation Map ──────────────────────────────────

REGIME_ALLOCATIONS: dict[str, dict[str, float]] = {
    "risk-on": {"BTC": 0.60, "ETH": 0.25, "SOL": 0.15},
    "neutral": {"BTC": 0.50},
    "risk-off": {},  # 100% cash
}

MAX_POSITION_USD = 100.0  # Per-coin max
TOTAL_CAPITAL = 100.0     # Total wallet capital

#: Venues the rotation trader can target. Hyperliquid stays the default.
VENUES: tuple[str, ...] = ("hyperliquid", "robinhood")

#: Venues that hold the asset itself: long only, and a rebalance trades the
#: delta instead of closing and re-opening the whole position.
SPOT_VENUES: frozenset[str] = frozenset({"robinhood"})

#: Skip a rebalance whose delta is under this notional — not worth the spread.
_MIN_REBALANCE_USD = 1.0


def _get_trader(mainnet: bool = False, venue: str = "hyperliquid"):
    """Build the venue's trader from env config."""
    from config import settings

    if venue == "robinhood":
        from trading.robinhood import get_robinhood_trader

        trader = get_robinhood_trader()
        if not trader.configured:
            raise ValueError(
                "ROBINHOOD_API_KEY / ROBINHOOD_PRIVATE_KEY_B64 not set in .env. "
                "Run 'python3 -m trading.robinhood keygen' and see docs/ROBINHOOD_SETUP.md."
            )
        return trader

    from trading.hyperliquid import HyperliquidTrader

    private_key = settings.HYPERLIQUID_PRIVATE_KEY
    if not private_key:
        raise ValueError(
            "HYPERLIQUID_PRIVATE_KEY not set in .env. "
            "Generate a wallet and fund it first."
        )

    return HyperliquidTrader(
        private_key=private_key,
        testnet=not mainnet,
        max_position_usd=MAX_POSITION_USD,
        max_drawdown_pct=0.20,
    )


def _mode_label(venue: str, mainnet: bool, trader: Any) -> str:
    """Human-readable execution mode for logs and the summary payload."""
    if venue in SPOT_VENUES:
        return getattr(trader, "mode", "UNKNOWN")  # UNCONFIGURED / DRY_RUN / LIVE
    return "MAINNET" if mainnet else "TESTNET"


def _log_trade_to_journal(engine, coin: str, direction: str, size_usd: float,
                          regime: str, result: dict, venue: str = "hyperliquid") -> None:
    """Log every live trade to the decision journal for audit."""
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO journal_entries "
                "(entry_type, ticker, direction, confidence, reasoning, metadata, created_at) "
                "VALUES ('LIVE_TRADE', :ticker, :dir, :conf, :reason, :meta, NOW())"
            ), {
                "ticker": coin,
                "dir": direction,
                "conf": 0.7,
                "reason": (f"Rotation regime={regime} → {direction} {coin} "
                           f"${size_usd:.2f} on {venue}"),
                "meta": str(result)[:500],
            })
    except Exception:
        pass  # Journal is optional — don't block trading


def _tradable_targets(trader: Any, target: dict[str, float], venue: str) -> dict[str, float]:
    """Drop target coins the venue does not report tradable.

    Robinhood lists a different universe than Hyperliquid perps, so a weight
    on a pair it does not carry has to be dropped rather than sent and
    rejected. Weights of the survivors are left untouched — the shortfall
    stays in cash, which is the conservative reading of the regime.
    """
    if venue not in SPOT_VENUES:
        return target
    tradable = trader.tradable_assets()
    if not tradable:
        log.warning("{v} reported no tradable pairs — allocating nothing", v=venue)
        return {}
    kept = {c: w for c, w in target.items() if c.upper() in tradable}
    for coin in target:
        if coin not in kept:
            log.warning("{v} does not list {c} as tradable — dropped from the target",
                        v=venue, c=coin)
    return kept


def _rebalance_spot(trader: Any, engine, target: dict[str, float], regime: str,
                    current: dict[str, dict], venue: str) -> list[dict]:
    """Long-only spot rebalance: trade the delta, risk-off sells to cash."""
    results: list[dict] = []
    cap = float(getattr(trader, "max_position_usd", MAX_POSITION_USD))

    # Sell anything the regime no longer wants — risk-off empties the book.
    for coin, pos in current.items():
        if coin not in target:
            log.info("Selling {c} to cash (not in target for {r} regime)", c=coin, r=regime)
            result = trader.close_position(coin)
            results.append({"action": "CLOSE", "coin": coin, "result": result})
            _log_trade_to_journal(engine, coin, "CLOSE", pos["size_usd"], regime, result, venue)

    # Buy up / trim down to the target notional.
    for coin, weight in target.items():
        target_usd = min(TOTAL_CAPITAL * weight, cap)
        held_usd = current.get(coin, {}).get("size_usd", 0.0)
        delta = target_usd - held_usd

        if abs(delta) < _MIN_REBALANCE_USD or (
            held_usd > 0 and abs(delta) / max(target_usd, 1) < 0.10
        ):
            log.info("{c} already at target (${cur:.2f} ≈ ${tgt:.2f})",
                     c=coin, cur=held_usd, tgt=target_usd)
            continue

        direction = "LONG" if delta > 0 else "SHORT"  # SHORT = sell held quantity
        size_usd = min(abs(delta), cap)
        log.info("{a} {c} — ${usd:.2f} (target {w:.0%}, held ${held:.2f})",
                 a="Buying" if delta > 0 else "Trimming", c=coin, usd=size_usd,
                 w=weight, held=held_usd)
        result = trader.open_position(ticker=coin, direction=direction, size_usd=size_usd)
        results.append({"action": "OPEN" if delta > 0 else "TRIM", "coin": coin,
                        "size_usd": size_usd, "result": result})
        _log_trade_to_journal(engine, coin, direction, size_usd, regime, result, venue)

    return results


def _rebalance_perps(trader: Any, engine, target: dict[str, float], regime: str,
                     current_positions: list[dict], venue: str) -> list[dict]:
    """Perp rebalance: close what fell out of target, re-open at the new size."""
    results: list[dict] = []
    current_coins = {p["coin"]: p for p in current_positions}

    for pos in current_positions:
        if pos["coin"] not in target:
            log.info("Closing {d} {c} (not in target for {r} regime)",
                     d=pos["direction"], c=pos["coin"], r=regime)
            close_result = trader.close_position(pos["coin"])
            results.append({"action": "CLOSE", "coin": pos["coin"], "result": close_result})
            _log_trade_to_journal(engine, pos["coin"], "CLOSE", pos["size_usd"], regime,
                                  close_result, venue)

    for coin, weight in target.items():
        target_usd = min(TOTAL_CAPITAL * weight, MAX_POSITION_USD)

        if coin in current_coins:
            current_usd = current_coins[coin]["size_usd"]
            # Skip if within 10% of target
            if abs(current_usd - target_usd) / max(target_usd, 1) < 0.10:
                log.info("{c} already at target (${cur:.2f} ≈ ${tgt:.2f})",
                         c=coin, cur=current_usd, tgt=target_usd)
                continue
            # Close and re-open at new size
            log.info("Adjusting {c}: ${cur:.2f} → ${tgt:.2f}",
                     c=coin, cur=current_usd, tgt=target_usd)
            trader.close_position(coin)

        if target_usd < 1.0:
            continue  # Too small to trade

        log.info("Opening LONG {c} — ${usd:.2f} ({w:.0%})",
                 c=coin, usd=target_usd, w=weight)
        open_result = trader.open_position(
            ticker=coin,
            direction="LONG",
            size_usd=target_usd,
        )
        results.append({"action": "OPEN", "coin": coin, "size_usd": target_usd,
                        "result": open_result})
        _log_trade_to_journal(engine, coin, "LONG", target_usd, regime, open_result, venue)

    return results


def execute_rotation_live(mainnet: bool = False, venue: str = "hyperliquid") -> dict[str, Any]:
    """Run rotation, map regime to crypto allocation, execute on *venue*."""
    venue = (venue or "hyperliquid").strip().lower()
    if venue not in VENUES:
        raise ValueError(f"Unknown venue {venue!r}. Choose from: {', '.join(VENUES)}")

    engine = get_engine()
    trader = _get_trader(mainnet, venue=venue)

    mode = _mode_label(venue, mainnet, trader)
    log.info("═══ Live Rotation Trader — {v} {m} ═══", v=venue, m=mode)

    # 1. Get current regime from rotation strategy
    try:
        rotation = run_rotation(engine, as_of_date=date.today())
        regime = rotation.regime.label
    except Exception as e:
        log.error("Rotation strategy failed: {e}", e=str(e))
        return {"status": "ERROR", "error": str(e)}

    log.info("Regime: {r} (SPY trend={t:.4f}, VIX z={v:.2f})",
             r=regime, t=rotation.regime.spy_trend, v=rotation.regime.vix_zscore)

    # 2. Get target crypto allocation (venue may not list every coin)
    target = _tradable_targets(trader, REGIME_ALLOCATIONS.get(regime, {}), venue)
    log.info("Target allocation: {a}", a={k: f"{v:.0%}" for k, v in target.items()} or "100% CASH")

    # 3. Check current balance
    balance = trader.get_balance()
    if "error" in balance:
        log.error("Failed to get balance: {e}", e=balance["error"])
        return {"status": "ERROR", "error": balance["error"]}

    equity = balance["equity_usd"]
    log.info("Wallet equity: ${e:.2f} ({m})", e=equity, m=mode)

    # 4. Check risk limits
    risk = trader.check_risk_limits()
    if risk.get("drawdown_breached"):
        log.warning("Drawdown breached — all trading halted")
        return {"status": "RISK_HALT", "risk": risk}

    # 5. Get current positions
    current_positions = trader.get_positions()
    current_coins = {p["coin"]: p for p in current_positions}
    log.info("Current positions: {p}",
             p=[f"{p['coin']} {p['direction']} ${p['size_usd']}" for p in current_positions] or "none")

    # 6-7. Rebalance to the target allocation. Spot holds the asset, so it
    #      trades the delta and never goes short; perps close and re-open.
    if venue in SPOT_VENUES:
        results = _rebalance_spot(trader, engine, target, regime, current_coins, venue)
    else:
        results = _rebalance_perps(trader, engine, target, regime, current_positions, venue)

    # 8. Final state
    final_balance = trader.get_balance()
    final_positions = trader.get_positions()

    summary = {
        "status": "OK",
        "venue": venue,
        "mode": mode,
        "regime": regime,
        "target_allocation": target,
        "trades": results,
        "final_equity": final_balance.get("equity_usd", 0),
        "final_positions": final_positions,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    log.info("")
    log.info("═══ LIVE ROTATION SUMMARY ═══")
    log.info("  Venue:    {v}", v=venue)
    log.info("  Mode:     {m}", m=mode)
    log.info("  Regime:   {r}", r=regime)
    log.info("  Trades:   {n}", n=len(results))
    log.info("  Equity:   ${e:.2f}", e=final_balance.get("equity_usd", 0))
    for p in final_positions:
        # Spot holdings carry no unrealized PnL field — the venue only knows
        # the current mark, not the cost basis.
        log.info("  Position: {d} {c} ${sz:.2f} (PnL: {pnl})",
                 d=p["direction"], c=p["coin"], sz=p["size_usd"],
                 pnl=(f"${p['unrealized_pnl']:+.2f}" if "unrealized_pnl" in p else "n/a"))

    return summary


def show_status(mainnet: bool = False, venue: str = "hyperliquid") -> None:
    """Show the current wallet/account status for *venue*."""
    venue = (venue or "hyperliquid").strip().lower()
    if venue not in VENUES:
        raise ValueError(f"Unknown venue {venue!r}. Choose from: {', '.join(VENUES)}")

    trader = _get_trader(mainnet, venue=venue)
    mode = _mode_label(venue, mainnet, trader)

    balance = trader.get_balance()
    positions = trader.get_positions()
    risk = trader.check_risk_limits()

    log.info("\n{}", '=' * 50)
    log.info("{} WALLET STATUS — {}", venue.upper(), mode)
    log.info("{}", '=' * 50)
    log.info("  Account:    {}", balance.get('address') or balance.get('account_number', 'N/A'))
    log.info("  Equity:     ${:.2f}", balance.get('equity_usd', 0))
    log.info("  Free:       ${:.2f}",
             balance.get('free_margin_usd', balance.get('buying_power_usd', 0)))
    log.info("  Drawdown:   {:.2%}", risk.get('current_drawdown_pct', 0))
    log.info("  DD Limit:   {:.2%}", risk.get('max_drawdown_pct', 0))
    if positions:
        log.info("POSITIONS:")
        for p in positions:
            log.info("  {:5s} {:6s} ${:8.2f} @ {:10.2f} PnL: {} Lev: {}x",
                     p['direction'], p['coin'], p['size_usd'],
                     p.get('entry_price', p.get('mid_price', 0)),
                     (f"${p['unrealized_pnl']:+.2f}" if "unrealized_pnl" in p else "n/a"),
                     p.get('leverage', '1'))
    else:
        log.info("No open positions.")

    if hasattr(trader, "get_trade_history"):
        history = trader.get_trade_history(limit=10)
        if history:
            log.info("RECENT TRADES (last {}):", len(history))
            for t in history[:5]:
                log.info("  {:5s} {:6s} sz={} @ {} PnL={}",
                         t['dir'], t['coin'], t['size'], t['price'], t['closed_pnl'])
    else:
        orders = trader.get_orders(limit=10)
        if orders:
            log.info("RECENT ORDERS (last {}):", len(orders))
            for o in orders[:5]:
                log.info("  {:5s} {:9s} qty={} @ {} [{}]",
                         str(o.get('side', '')), str(o.get('symbol', '')),
                         o.get('filled_quantity'), o.get('average_price'), o.get('state'))


def main() -> None:
    parser = argparse.ArgumentParser(description="Live Rotation Trader")
    parser.add_argument("--status", action="store_true", help="Show wallet status only")
    parser.add_argument("--mainnet", action="store_true", help="Use mainnet (real money)")
    parser.add_argument("--venue", choices=VENUES, default="hyperliquid",
                        help="Execution venue (robinhood = crypto spot, long only)")
    args = parser.parse_args()

    if args.status:
        show_status(mainnet=args.mainnet, venue=args.venue)
    else:
        result = execute_rotation_live(mainnet=args.mainnet, venue=args.venue)
        if result["status"] != "OK":
            log.warning("Live trading ended with status: {s}", s=result["status"])


if __name__ == "__main__":
    main()
