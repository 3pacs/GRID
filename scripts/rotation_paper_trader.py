#!/usr/bin/env python3
"""
Live paper trading runner for the Adaptive Rotation Strategy.

Runs daily after market close. Executes the rotation strategy,
compares target weights to current positions, and opens/closes
paper trades to match the allocation.

Usage:
    python3 scripts/rotation_paper_trader.py              # Run once
    python3 scripts/rotation_paper_trader.py --dashboard   # Show dashboard
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine
from alpha_research.strategies.adaptive_rotation import run_rotation
from trading.paper_engine import PaperTradingEngine
from trading.wallet_manager import WalletManager


WALLET_EXCHANGE = "grid_rotation"
WALLET_TYPE = "paper"
PAPER_CAPITAL = 10_000.0
STRATEGY_ID = "adaptive_rotation_live"
MAX_DRAWDOWN_LIMIT = 0.20
RISK_LIMIT_PCT = 0.05


def _get_or_create_wallet(wm: WalletManager) -> str:
    """Get existing rotation wallet or create one."""
    wallets = wm.get_all_wallets(exchange=WALLET_EXCHANGE)
    active = [w for w in wallets if w["status"] == "ACTIVE"]

    if active:
        wallet_id = active[0]["id"]
        log.info("Using existing wallet: {id} (capital={c})",
                 id=wallet_id, c=active[0]["current_capital"])
        return wallet_id

    wallet_id = wm.create_wallet(
        exchange=WALLET_EXCHANGE,
        wallet_type=WALLET_TYPE,
        initial_capital=PAPER_CAPITAL,
        risk_limit_pct=RISK_LIMIT_PCT,
        max_drawdown_limit=MAX_DRAWDOWN_LIMIT,
    )
    log.info("Created rotation paper wallet: {id} (capital={c})",
             id=wallet_id, c=PAPER_CAPITAL)
    return wallet_id


def _get_latest_price(engine, ticker: str) -> float | None:
    """Get the latest close price for a ticker from resolved_series."""
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT rs.value FROM resolved_series rs "
            "JOIN feature_registry fr ON rs.feature_id = fr.id "
            "WHERE fr.name = :name "
            "ORDER BY rs.obs_date DESC, rs.vintage_date DESC LIMIT 1"
        ), {"name": f"{ticker.lower()}_full"}).fetchone()

    if row and row[0] and float(row[0]) > 0:
        return float(row[0])
    return None


def _get_open_positions(engine) -> dict[str, dict]:
    """Get all open paper trades for the rotation strategy."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, ticker, direction, entry_price, position_size, entry_date "
            "FROM paper_trades "
            "WHERE strategy_id = :sid AND status = 'OPEN'"
        ), {"sid": STRATEGY_ID}).fetchall()

    positions = {}
    for r in rows:
        positions[r[1]] = {
            "trade_id": r[0],
            "ticker": r[1],
            "direction": r[2],
            "entry_price": r[3],
            "position_size": r[4],
            "entry_date": r[5],
        }
    return positions


def _ensure_strategy(engine) -> None:
    """Register the rotation strategy if it doesn't exist."""
    with engine.begin() as conn:
        existing = conn.execute(text(
            "SELECT id FROM paper_strategies WHERE id = :id"
        ), {"id": STRATEGY_ID}).fetchone()

        if not existing:
            conn.execute(text(
                "INSERT INTO paper_strategies "
                "(id, hypothesis_id, leader, follower, description, capital, high_water_mark, status) "
                "VALUES (:id, NULL, 'regime', 'multi_asset', :desc, :cap, :cap, 'ACTIVE')"
            ), {
                "id": STRATEGY_ID,
                "desc": "Adaptive Rotation — Sharpe 1.56 walk-forward. Daily rebalance.",
                "cap": PAPER_CAPITAL,
            })
            log.info("Registered paper strategy: {s}", s=STRATEGY_ID)


def run_paper_trading(engine) -> dict:
    """Execute one rotation cycle: compute weights, adjust positions."""
    pe = PaperTradingEngine(engine, initial_capital=PAPER_CAPITAL)
    wm = WalletManager(engine)

    wallet_id = _get_or_create_wallet(wm)
    _ensure_strategy(engine)

    # Check wallet health
    risk = wm.check_risk(wallet_id)
    if risk.get("status") == "KILLED":
        log.warning("Rotation wallet KILLED — skipping. Reason: {r}",
                     r=risk.get("kill_reason", "unknown"))
        return {"status": "KILLED", "risk": risk}

    wallet = wm.get_wallet(wallet_id)
    current_capital = wallet["current_capital"]

    # Run rotation strategy
    today = date.today()
    try:
        rotation = run_rotation(engine, as_of_date=today)
    except Exception as e:
        log.error("Rotation strategy failed: {e}", e=str(e))
        return {"status": "ERROR", "error": str(e)}

    target_weights = rotation.weights
    regime = rotation.regime
    stopped = rotation.stopped_tickers

    log.info("Regime: {l} (SPY trend={t}, VIX z={v:.2f})",
             l=regime.label, t=regime.spy_trend, v=regime.vix_zscore)
    log.info("Target weights: {w}", w={k: f"{v:.1%}" for k, v in target_weights.items()})

    if stopped:
        log.info("Stopped tickers: {s}", s=stopped)

    # Get current open positions
    current_positions = _get_open_positions(engine)
    log.info("Current open positions: {n}", n=len(current_positions))

    trades_opened = 0
    trades_closed = 0

    # Close positions not in target or stopped
    tickers_to_close = set(current_positions.keys()) - set(target_weights.keys())
    tickers_to_close |= set(stopped) & set(current_positions.keys())

    for ticker in tickers_to_close:
        pos = current_positions[ticker]
        exit_price = _get_latest_price(engine, ticker)
        if exit_price is None:
            log.warning("No price for {t}, skipping close", t=ticker)
            continue

        result = pe.close_trade(
            trade_id=pos["trade_id"],
            exit_price=exit_price,
            notes=f"Rotation rebalance — {'stopped' if ticker in stopped else 'dropped from target'}",
        )

        if "error" not in result:
            trades_closed += 1
            pnl = result.get("pnl", 0)
            wm.update_pnl(wallet_id, pnl, pnl > 0)
            log.info("Closed {t}: P&L={pnl:+.2f}", t=ticker, pnl=pnl)

    # Open/adjust positions to match target weights
    for ticker, weight in target_weights.items():
        if weight <= 0.01:
            continue  # Skip negligible allocations

        if ticker in current_positions:
            continue  # Already positioned — rotation holds until next rebalance

        entry_price = _get_latest_price(engine, ticker)
        if entry_price is None:
            log.warning("No price for {t}, skipping open", t=ticker)
            continue

        position_size = weight  # Weight = fraction of capital
        trade_id = pe.open_trade(
            strategy_id=STRATEGY_ID,
            ticker=ticker,
            direction="LONG",
            entry_price=entry_price,
            position_size=position_size,
            signal_strength=weight,
            hypothesis_id=None,
            threshold_used=0.0,
        )

        if trade_id > 0:
            trades_opened += 1
            log.info("Opened LONG {t} @ {p:.2f} (weight={w:.1%})",
                     t=ticker, p=entry_price, w=weight)

    # Summary
    final_wallet = wm.get_wallet(wallet_id)
    summary = {
        "status": "OK",
        "date": str(today),
        "regime": regime.label,
        "target_weights": target_weights,
        "trades_opened": trades_opened,
        "trades_closed": trades_closed,
        "wallet_capital": final_wallet["current_capital"],
        "wallet_pnl": final_wallet["total_pnl"],
        "wallet_drawdown": final_wallet["max_drawdown"],
        "stopped_tickers": stopped,
    }

    log.info("")
    log.info("=" * 50)
    log.info("ROTATION PAPER TRADING SUMMARY")
    log.info("=" * 50)
    log.info("  Date:         {d}", d=today)
    log.info("  Regime:       {r}", r=regime.label)
    log.info("  Opened:       {n} trades", n=trades_opened)
    log.info("  Closed:       {n} trades", n=trades_closed)
    log.info("  Capital:      ${c:,.2f}", c=final_wallet["current_capital"])
    log.info("  Total P&L:    ${p:+,.2f}", p=final_wallet["total_pnl"])
    log.info("  Max Drawdown: {d:.2%}", d=final_wallet["max_drawdown"])

    return summary


def show_dashboard(engine) -> None:
    """Print current rotation paper trading dashboard."""
    wm = WalletManager(engine)
    pe = PaperTradingEngine(engine)

    wallets = wm.get_all_wallets(exchange=WALLET_EXCHANGE)
    if not wallets:
        log.info("No rotation wallets found.")
        return

    wallet = wallets[0]
    positions = _get_open_positions(engine)

    log.info("=" * 60)
    log.info("ADAPTIVE ROTATION PAPER TRADING DASHBOARD")
    log.info("=" * 60)
    log.info("  Wallet:       {}", wallet['id'])
    log.info("  Status:       {}", wallet['status'])
    log.info("  Capital:      ${:,.2f}", wallet['current_capital'])
    log.info("  Initial:      ${:,.2f}", wallet['initial_capital'])
    log.info("  Total P&L:    ${:+,.2f}", wallet['total_pnl'])
    log.info("  Win/Loss:     {}/{}", wallet['win_count'], wallet['loss_count'])
    log.info("  Max Drawdown: {:.2%}", wallet['max_drawdown'])

    if positions:
        log.info("OPEN POSITIONS:")
        for ticker, pos in sorted(positions.items()):
            current = _get_latest_price(engine, ticker)
            if current and pos["entry_price"]:
                pnl_pct = (current - pos["entry_price"]) / pos["entry_price"]
                log.info("  {:6s} {:5s} @ {:8.2f} -> {:8.2f} ({:+.2%}) size={:.1%}",
                         ticker, pos['direction'], pos['entry_price'], current, pnl_pct, pos['position_size'])
            else:
                log.info("  {:6s} {:5s} @ {:8.2f}", ticker, pos['direction'], pos['entry_price'])
    else:
        log.info("No open positions.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Rotation Paper Trader")
    parser.add_argument("--dashboard", action="store_true", help="Show dashboard only")
    args = parser.parse_args()

    engine = get_engine()

    if args.dashboard:
        show_dashboard(engine)
    else:
        result = run_paper_trading(engine)
        if result["status"] != "OK":
            log.warning("Paper trading cycle ended with status: {s}", s=result["status"])


if __name__ == "__main__":
    main()
