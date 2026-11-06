"""Fee-aware, frozen-rule Astrogrid research; no inference or ledger writes.

Reuses the live receipt validator/rule constants and GRID's Wilson primitive.
Historical archive replay is explicitly latest-vintage, never prospective PIT.
"""
from __future__ import annotations

import csv
import json
import zipfile
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from binance_close_contract import FIVE_MINUTES_MS, completed_spot_flow_payload
from evals.e2.scoring import wilson_interval
from oracle.astrogrid_order_flow import (
    CONTRACT_VERSION,
    MIN_IMBALANCE,
    MIN_QUOTE,
    MIN_TRADES,
    MODEL_VERSION,
    THRESHOLD,
)

VERSION = "astrogrid-spot-flow-edge-v1"
PROTOCOL = "docs/research/astrogrid-spot-flow-edge-v1-protocol.md"
UTC = timezone.utc
START = datetime(2025, 11, 1, tzinfo=UTC)
SPLIT = datetime(2026, 8, 1, tzinfo=UTC)
END = datetime(2026, 11, 1, tzinfo=UTC)
STEP = FIVE_MINUTES_MS
SYMBOLS = ("BTC", "ETH", "SOL")
STRATEGIES = ("flow", "cash", "always_long", "always_short", "price_momentum",
              "random_expected", "reverse_flow", "long_only_flow")
COMPARISONS = ("cash", "always_long", "always_short", "price_momentum", "random_expected")


@dataclass(frozen=True)
class Bar:
    end: int
    opening: float
    close: float
    quote: float
    buy_quote: float
    trades: int


def parse_archive(path: Path, symbol: str) -> list[Bar]:
    """Validate full provider bars; explicitly normalize documented microseconds."""
    out = []
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        if len(names) != 1 or not names[0].endswith(".csv"):
            raise ValueError("Archive must contain one CSV")
        lines = archive.read(names[0]).decode().splitlines()
    for row in csv.reader(lines):
        if len(row) != 12:
            raise ValueError("Expected 12 kline fields")
        raw = list(row)
        raw[0], raw[6], raw[8] = int(row[0]), int(row[6]), int(row[8])
        if raw[0] >= 10**14:
            if raw[0] % 1000 or raw[6] % 1000 != 999:
                raise ValueError("Unexpected microsecond boundary")
            raw[0] //= 1000
            raw[6] //= 1000
        end = raw[0] + STEP
        # This is a hypothetical archive validation clock, not known-at evidence.
        p = completed_spot_flow_payload(raw, symbol + "USDT", datetime.fromtimestamp(end / 1000, UTC))
        if p is None:
            raise ValueError("Incomplete archive bar")
        out.append(Bar(end, p["open"], p["close"], p["quote_volume"],
                       p["taker_buy_quote"], p["trade_count"]))
    return out


def load_archives(cache: Path, manifest: list[dict]) -> tuple[dict[str, list[Bar]], dict]:
    import hashlib

    all_bars = {s: [] for s in SYMBOLS}
    seen = set()
    for item in manifest:
        symbol, month = item["symbol"], item["month"]
        if symbol not in SYMBOLS or (symbol, month) in seen:
            raise ValueError("Unsupported or repeated archive")
        seen.add((symbol, month))
        path = cache / item["url"].rsplit("/", 1)[-1]
        if hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError("Archive changed after checksum verification")
        bars = parse_archive(path, symbol)
        for bar in bars:
            opened = datetime.fromtimestamp((bar.end - STEP) / 1000, UTC)
            if opened.strftime("%Y-%m") != month:
                raise ValueError("Bar outside named archive month")
        all_bars[symbol].extend(bars)
    expected_months = {"2025-11", "2025-12"} | {f"2026-{m:02d}" for m in range(1, 11)}
    if seen != {(s, m) for s in SYMBOLS for m in expected_months}:
        raise ValueError("Incomplete declared archive sample")
    quality = {}
    for symbol, bars in all_bars.items():
        bars.sort(key=lambda b: b.end)
        if len({b.end for b in bars}) != len(bars):
            raise ValueError("Duplicate bar timestamp")
        expected = int((END - START).total_seconds() / 300)
        if any(not START.timestamp() * 1000 < b.end <= END.timestamp() * 1000 for b in bars):
            raise ValueError("Bar outside declared sample")
        quality[symbol] = {"rows": len(bars), "expected_rows": expected,
                           "missing_bars": expected - len(bars), "duplicates": 0,
                           "first_bar_end": iso(bars[0].end), "last_bar_end": iso(bars[-1].end)}
    return all_bars, quality


def iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat()


def replay_events(bars_by_symbol: dict[str, list[Bar]], start: datetime,
                  end: datetime) -> tuple[list[dict], dict]:
    events, counts = [], Counter()
    lower, upper = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
    for symbol, bars in bars_by_symbol.items():
        lookup = {b.end: b for b in bars}
        busy_until = 0
        for bar in bars:
            if not lower <= bar.end < upper:
                continue
            imbalance = (2 * bar.buy_quote - bar.quote) / bar.quote if bar.quote else 0
            if bar.quote < MIN_QUOTE or bar.trades < MIN_TRADES or abs(imbalance) < MIN_IMBALANCE:
                continue
            counts["qualified_bars"] += 1
            if bar.end < busy_until:
                counts["overlapping_skipped"] += 1
                continue
            # 15s issuance delay means first 5m close after due is +35m.
            outcome_end = bar.end + 7 * STEP
            busy_until = outcome_end  # never selected using forward returns
            if outcome_end > upper:
                counts["boundary_purged"] += 1
                continue
            window = [lookup.get(bar.end + i * STEP) for i in range(1, 8)]
            if any(b is None for b in window):
                counts["missing_window"] += 1
                continue
            entry, outcome = window[0], window[-1]
            direction = 1 if imbalance > 0 else -1
            events.append({"symbol": symbol, "signal_end": iso(bar.end),
                           "outcome_end": iso(outcome_end), "day": iso(bar.end)[:10],
                           "signal_price": bar.close, "entry_price": entry.close,
                           "outcome_price": outcome.close, "direction": direction,
                           "momentum": int(np.sign(bar.close - bar.opening)),
                           "imbalance": imbalance, "quote_volume": bar.quote,
                           "trades": bar.trades,
                           "return": outcome.close / entry.close - 1,
                           "forecast_return": direction * (outcome.close / bar.close - 1)})
    events.sort(key=lambda e: (e["signal_end"], e["symbol"]))
    counts["retained"] = len(events)
    return events, dict(counts)


def strategy_returns(events: list[dict], total_bps: float = 24) -> np.ndarray:
    """Returns per fixed entry notional, costs charged on both leg notionals."""
    if not np.isfinite(total_bps) or total_bps < 0:
        raise ValueError("Invalid roundtrip cost scenario")
    raw = np.asarray([e["return"] for e in events], dtype=float)
    flow = np.asarray([e["direction"] for e in events], dtype=float)
    momentum = np.asarray([e["momentum"] for e in events], dtype=float)
    sides = np.column_stack([flow, np.zeros(len(events)), np.ones(len(events)),
                             -np.ones(len(events)), momentum, np.zeros(len(events)),
                             -flow, np.maximum(flow, 0)])
    costs = (total_bps / 2 / 10000) * (2 + raw)
    returns = sides * raw[:, None] - (sides != 0) * costs[:, None]
    # Analytic expected random long/short gross=0, always pays trading costs.
    returns[:, STRATEGIES.index("random_expected")] = -costs
    return returns


def daily_bootstrap(events: list[dict], values: np.ndarray, *, n_boot: int = 1999,
                    seed: int = 20261106, block: int = 7) -> np.ndarray:
    """Resample joint daily cells; retain all assets together and event weights."""
    if not events:
        return np.empty((0, values.shape[1]))
    first = datetime.fromisoformat(events[0]["day"])
    last = datetime.fromisoformat(events[-1]["day"])
    days = (last - first).days + 1
    sums, n = np.zeros((days, values.shape[1])), np.zeros(days)
    for event, row in zip(events, values, strict=True):
        i = (datetime.fromisoformat(event["day"]) - first).days
        sums[i] += row
        n[i] += 1
    rng = np.random.default_rng(seed)
    starts = rng.integers(0, days, (n_boot, int(np.ceil(days / block))))
    indexes = (starts[:, :, None] + np.arange(block)) % days
    indexes = indexes.reshape(n_boot, -1)[:, :days]
    return sums[indexes].sum(axis=1) / n[indexes].sum(axis=1)[:, None]


def sample_summary(events: list[dict], start: datetime, end: datetime) -> dict:
    assets = {s: sum(e["symbol"] == s for e in events) for s in SYMBOLS}
    dates = len({e["day"] for e in events})
    return {"start": start.isoformat(), "end_exclusive": end.isoformat(),
            "events": len(events), "utc_dates": dates, "per_asset": assets,
            "minimum_sample_met": len(events) >= 500 and dates >= 60 and min(assets.values()) >= 100}


def summarize(events: list[dict], start: datetime, end: datetime, *, inference: bool = True) -> dict:
    sample = sample_summary(events, start, end)
    if not events:
        return {"sample": sample, "strategies": {}, "paired_advantages": {}}
    values = strategy_returns(events)
    means = values.mean(axis=0)
    boot = daily_bootstrap(events, values) if inference else None
    strategies, paired = {}, {}
    for i, name in enumerate(STRATEGIES):
        ci = np.quantile(boot[:, i], [.025, .975]).tolist() if inference else None
        strategies[name] = {"mean_net_bps": float(means[i] * 10000),
                            "mean_net_95ci_bps": [v * 10000 for v in ci] if ci else None,
                            "net_positive_share": float((values[:, i] > 0).mean())}
        if name in COMPARISONS:
            diff = values[:, 0] - values[:, i]
            dist = boot[:, 0] - boot[:, i] if inference else None
            paired[name] = {"mean_advantage_bps": float(diff.mean() * 10000),
                            "advantage_95ci_bps": (np.quantile(dist, [.025, .975]) * 10000).tolist()
                            if inference else None,
                            "simultaneous_lower_bps": float(np.quantile(dist, .05 / len(COMPARISONS)) * 10000)
                            if inference else None}
    forecast = np.asarray([e["forecast_return"] for e in events])
    hits = int((forecast >= THRESHOLD).sum())
    signed_raw = np.asarray([e["direction"] * e["return"] for e in events])
    return {"sample": sample, "strategies": strategies, "paired_advantages": paired,
            "flow_gross_mean_bps": float(signed_raw.mean() * 10000),
            "flow_fees_only_mean_bps": float(strategy_returns(events, 20)[:, 0].mean() * 10000),
            "forecast_close_basis": {"hit_rate": hits / len(events), "hits": hits,
                                     "direction_accuracy": float((forecast > 0).mean()),
                                     "mean_signed_bps": float(forecast.mean() * 10000),
                                     "naive_hit_wilson_95": wilson_interval(hits, len(events)),
                                     "basis": "Original signal close to +35m outcome; no fill/cost credit"},
            "cost_sensitivity": [{"roundtrip_bps": c,
                                   "flow_mean_net_bps": float(strategy_returns(events, c)[:, 0].mean() * 10000)}
                                  for c in (0, 8, 20, 24, 40)]}


def historical_report(cache: Path, manifest: list[dict]) -> dict[str, Any]:
    bars, quality = load_archives(cache, manifest)
    reference, ref_counts = replay_events(bars, START, SPLIT)
    events, counts = replay_events(bars, SPLIT, END)
    test = summarize(events, SPLIT, END)
    if not test["sample"]["minimum_sample_met"]:
        verdict = "INSUFFICIENT_SAMPLE"
    elif test["strategies"]["flow"]["mean_net_95ci_bps"][1] <= 0:
        verdict = "NO_POSITIVE_NET_RETURN"
    elif all(p["simultaneous_lower_bps"] > 0 for p in test["paired_advantages"].values()):
        verdict = "HISTORICAL_SUPPORT_ONLY"
    else:
        verdict = "NO_DEMONSTRATED_EDGE"
    # Selected timestamps are retained outside the repository for independent audit.
    (cache.parent / "test-events.json").write_text(json.dumps(events, indent=2) + "\n")
    return {"version": VERSION, "model_version": MODEL_VERSION, "contract_version": CONTRACT_VERSION,
            "origin": "historical_archive_replay_not_prospective", "protocol_path": PROTOCOL,
            "verdict": verdict, "test": test, "reference": summarize(reference, START, SPLIT, inference=False),
            "selection": {"test": counts, "reference": ref_counts}, "data_quality": quality,
            "per_asset": {s: summarize([e for e in events if e["symbol"] == s], SPLIT, END, inference=False)
                          for s in SYMBOLS},
            "per_month": {m: summarize([e for e in events if e["day"].startswith(m)], SPLIT, END, inference=False)
                          for m in ("2026-08", "2026-09", "2026-10")},
            "costs": {"fee_bps_per_side": 10, "slippage_bps_per_side": 2,
                      "basis": "Scenario; current regular-user spot rate, not historical/account-specific fees",
                      "shorts": "Hypothetical; borrowing/funding/short availability excluded",
                      "entry": "Next completed 5m close after signal", "exit": "First completed close after due (+35m)"},
            "inference": {"bootstrap": "Joint UTC-day circular moving blocks", "block_days": 7,
                          "replicates": 1999, "seed": 20261106, "paired_comparisons": len(COMPARISONS),
                          "simultaneous_alpha": .05, "minimum_events": 500, "minimum_dates": 60,
                          "minimum_events_per_asset": 100},
            "sources": [{"title": "Binance public data schema and checksums",
                         "url": "https://github.com/binance/binance-public-data"},
                        {"title": "Binance spot trading fee schedule",
                         "url": "https://www.binance.com/en/fee/trading"}],
            "archive_manifest": manifest}
