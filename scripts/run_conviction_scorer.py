#!/usr/bin/env python3
"""Run the conviction scorer across all tickers."""

from __future__ import annotations
import os, sys
from pathlib import Path

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log
from db import get_engine
from alpha_research.conviction_scorer import scan_all, print_report


def main() -> None:
    engine = get_engine()

    log.info("=" * 70)
    log.info("CONVICTION SCORER — 98% CONFIDENCE TRADE DETECTOR")
    log.info("=" * 70)

    reports = scan_all(engine, min_score=15)

    conviction = [r for r in reports if r.alert_level == "CONVICTION"]
    fire = [r for r in reports if r.alert_level == "FIRE"]
    scale = [r for r in reports if r.alert_level == "SCALE"]
    pilot = [r for r in reports if r.alert_level == "PILOT"]
    watch = [r for r in reports if r.alert_level == "WATCH"]

    if conviction:
        log.info("\n" + "!" * 70)
        log.info("CONVICTION (Score 85+, 80%+ data coverage)")
        log.info("!" * 70)
        for r in conviction:
            print_report(r)

    if fire:
        log.info("\n" + "=" * 70)
        log.info("FIRE (Score 70+)")
        log.info("=" * 70)
        for r in fire:
            print_report(r)

    if scale:
        log.info("\n--- SCALE (Score 55+) ---")
        for r in scale:
            print_report(r)

    if pilot:
        log.info("\n--- PILOT (Score 40+) ---")
        for r in pilot[:10]:
            print_report(r)

    if watch:
        log.info("\n--- WATCH (Score 25+) ---")
        for r in watch[:10]:
            print_report(r)

    log.info("\n" + "=" * 70)
    log.info("SUMMARY: CONVICTION={c} FIRE={f} SCALE={s} PILOT={p} WATCH={w} PASS={x}",
             c=len(conviction), f=len(fire), s=len(scale), p=len(pilot), w=len(watch),
             x=len(reports) - len(conviction) - len(fire) - len(scale) - len(pilot) - len(watch))


if __name__ == "__main__":
    main()
