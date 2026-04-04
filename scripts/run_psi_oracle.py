#!/usr/bin/env python3
"""
Run the PSI Oracle and persist predictions to AstroGrid.

Usage:
  python3 scripts/run_psi_oracle.py              # Evaluate and show signals
  python3 scripts/run_psi_oracle.py --persist     # Evaluate and save to DB
  python3 scripts/run_psi_oracle.py --backtest    # Backtest over historical data
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

from loguru import logger as log

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine
from oracle.psi_oracle import evaluate_psi_signals, run_psi_oracle


def show_signals() -> None:
    """Evaluate and display current PSI oracle signals."""
    engine = get_engine()
    signals = evaluate_psi_signals(engine)

    if not signals:
        log.info("No PSI oracle signals triggered right now")
        return

    log.info("\n" + "=" * 60)
    log.info("PSI ORACLE — CURRENT SIGNALS")
    log.info("=" * 60)

    for s in signals:
        log.info(
            "\n  Symbol:     {sym}\n"
            "  Direction:  {dir}\n"
            "  Confidence: {conf:.1%}\n"
            "  PSI:        {psi:.2f}\n"
            "  VIX:        {vix}\n"
            "  Config:     {cfg} (historical Sharpe {sr:.2f})\n"
            "  Reasoning:  {reason}",
            sym=s.symbol,
            dir=s.direction,
            conf=s.confidence,
            psi=s.psi_value,
            vix=f"{s.vix_value:.1f}" if s.vix_value else "N/A",
            cfg=s.config_name,
            sr=s.config_sharpe,
            reason=s.reasoning,
        )


def persist_signals() -> None:
    """Evaluate signals and save to AstroGrid database."""
    engine = get_engine()
    payloads = run_psi_oracle(engine)

    if not payloads:
        log.info("No signals to persist")
        return

    from store.astrogrid import AstroGridStore

    store = AstroGridStore(engine)

    for payload in payloads:
        result = store.save_prediction(payload)
        if result:
            log.info(
                "Saved prediction: {sym} {call} (id={pid})",
                sym=payload["target_symbols"][0],
                call=payload["call"],
                pid=result.get("prediction_id", "?"),
            )
        else:
            log.error("Failed to save prediction for {sym}", sym=payload["target_symbols"][0])


def main() -> None:
    parser = argparse.ArgumentParser(description="PSI Oracle")
    parser.add_argument("--persist", action="store_true", help="Save predictions to DB")
    args = parser.parse_args()

    if args.persist:
        persist_signals()
    else:
        show_signals()


if __name__ == "__main__":
    main()
