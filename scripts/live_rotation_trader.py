#!/usr/bin/env python3
"""
Live Rotation Trader — bridges GRID rotation signals to Hyperliquid perps.

Maps the Adaptive Rotation regime to crypto allocations:
  - risk-on:  80% BTC, 20% ETH (aggressive)
  - neutral:  50% BTC, 50% cash (defensive)
  - risk-off: 100% cash (flat)

Phase A: Testnet ($100 fake money)
Phase B: Mainnet ($100 real money)

Usage:
    python3 scripts/live_rotation_trader.py                # Execute trades
    python3 scripts/live_rotation_trader.py --status        # Check positions
    python3 scripts/live_rotation_trader.py --mainnet       # Use real money (Phase B)
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
from trading.hyperliquid import HyperliquidTrader
from trading.wallet_manager import WalletManager


# ── Regime → Crypto Allocation Map ──────────────────────────────────

REGIME_ALLOCATIONS: dict[str, dict[str, float]] = {
    "risk-on": {"BTC": 0.60, "ETH": 0.25, "SOL": 0.15},
    "neutral": {"BTC": 0.50},
    "risk-off": {},  # 100% cash
}

MAX_POSITION_USD = 100.0  # Per-coin max
TOTAL_CAPITAL = 100.0     # Total wallet capital


def _get_trader(mainnet: bool = False) -> HyperliquidTrader:
    """Build trader from env config."""
    from config import settings

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


def _log_trade_to_journal(engine, coin: str, direction: str, size_usd: float,
                          regime: str, result: dict) -> None:
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
                "reason": f"Rotation regime={regime} → {direction} {coin} ${size_usd:.2f}",
                "meta": str(result)[:500],
            })
    except Exception:
        pass  # Journal is optional — don't block trading


def execute_rotation_live(mainnet: bool = False) -> dict[str, Any]:
    """Run rotation, map regime to crypto allocation, execute on Hyperliquid."""
    engine = get_engine()
    trader = _get_trader(mainnet)

    mode = "MAINNET" if mainnet else "TESTNET"
    log.info("═══ Live Rotation Trader — {m} ═══", m=mode)

    # 1. Get current regime from rotation strategy
    try:
        rotation = run_rotation(engine, as_of_date=date.today())
        regime = rotation.regime.label
    except Exception as e:
        log.error("Rotation strategy failed: {e}", e=str(e))
        return {"status": "ERROR", "error": str(e)}

    log.info("Regime: {r} (SPY trend={t:.4f}, VIX z={v:.2f})",
             r=regime, t=rotation.regime.spy_trend, v=rotation.regime.vix_zscore)

    # 2. Get target crypto allocation
    target = REGIME_ALLOCATIONS.get(regime, {})
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

    results: list[dict] = []

    # 6. Close positions not in target
    for pos in current_positions:
        if pos["coin"] not in target:
            log.info("Closing {d} {c} (not in target for {r} regime)",
                     d=pos["direction"], c=pos["coin"], r=regime)
            close_result = trader.close_position(pos["coin"])
            results.append({"action": "CLOSE", "coin": pos["coin"], "result": close_result})
            _log_trade_to_journal(engine, pos["coin"], "CLOSE", pos["size_usd"], regime, close_result)

    # 7. Open/adjust positions for target allocation
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
        results.append({"action": "OPEN", "coin": coin, "size_usd": target_usd, "result": open_result})
        _log_trade_to_journal(engine, coin, "LONG", target_usd, regime, open_result)

    # 8. Final state
    final_balance = trader.get_balance()
    final_positions = trader.get_positions()

    summary = {
        "status": "OK",
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
    log.info("  Mode:     {m}", m=mode)
    log.info("  Regime:   {r}", r=regime)
    log.info("  Trades:   {n}", n=len(results))
    log.info("  Equity:   ${e:.2f}", e=final_balance.get("equity_usd", 0))
    for p in final_positions:
        log.info("  Position: {d} {c} ${sz:.2f} (PnL: ${pnl:+.2f})",
                 d=p["direction"], c=p["coin"], sz=p["size_usd"], pnl=p["unrealized_pnl"])

    return summary


def show_status(mainnet: bool = False) -> None:
    """Show current Hyperliquid wallet status."""
    trader = _get_trader(mainnet)
    mode = "MAINNET" if mainnet else "TESTNET"

    balance = trader.get_balance()
    positions = trader.get_positions()
    risk = trader.check_risk_limits()

    print(f"\n{'=' * 50}")
    print(f"HYPERLIQUID WALLET STATUS — {mode}")
    print(f"{'=' * 50}")
    print(f"  Address:    {balance.get('address', 'N/A')}")
    print(f"  Equity:     ${balance.get('equity_usd', 0):.2f}")
    print(f"  Free:       ${balance.get('free_margin_usd', 0):.2f}")
    print(f"  Drawdown:   {risk.get('current_drawdown_pct', 0):.2%}")
    print(f"  DD Limit:   {risk.get('max_drawdown_pct', 0):.2%}")
    print()

    if positions:
        print("POSITIONS:")
        for p in positions:
            print(f"  {p['direction']:5s} {p['coin']:6s} "
                  f"${p['size_usd']:8.2f} @ {p['entry_price']:10.2f} "
                  f"PnL: ${p['unrealized_pnl']:+.2f} "
                  f"Lev: {p.get('leverage', '1')}x")
    else:
        print("No open positions.")

    history = trader.get_trade_history(limit=10)
    if history:
        print(f"\nRECENT TRADES (last {len(history)}):")
        for t in history[:5]:
            print(f"  {t['dir']:5s} {t['coin']:6s} sz={t['size']} @ {t['price']} "
                  f"PnL={t['closed_pnl']}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live Rotation Trader — Hyperliquid")
    parser.add_argument("--status", action="store_true", help="Show wallet status only")
    parser.add_argument("--mainnet", action="store_true", help="Use mainnet (real money)")
    args = parser.parse_args()

    if args.status:
        show_status(mainnet=args.mainnet)
    else:
        result = execute_rotation_live(mainnet=args.mainnet)
        if result["status"] != "OK":
            log.warning("Live trading ended with status: {s}", s=result["status"])


if __name__ == "__main__":
    main()
