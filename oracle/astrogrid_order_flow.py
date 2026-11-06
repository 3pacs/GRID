"""Separate, unvalidated 30m spot taker-flow research contract for Astrogrid.

The fixed gates below are declared before live outcomes, not calibrated odds:
>=100k USDT quote turnover, >=100 trades, >=10% signed taker imbalance in
the latest completed 5m bar. Hypothesis: that direction moves >=0.10% over
30m. Anonymous buyer/seller cohorts are not identified institutional actors.
Entry and outcome both use recorded Binance GLOBAL spot REST receipts; the
observed stream candles and 24h actor-flow cohort are never substituted.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from binance_close_contract import FIVE_MINUTES_MS, verified_spot_flow_payload

MODEL_VERSION = "astrogrid-spot-flow-v1"
CONTRACT_VERSION = "binance_spot_taker_30m_v1"
HORIZON = timedelta(minutes=30)
THRESHOLD = 0.001
MIN_QUOTE = 100_000
MIN_TRADES = 100
MIN_IMBALANCE = 0.10
MAX_AGE = timedelta(minutes=10)
OUTCOME_GRACE = timedelta(minutes=15)
LABEL = "30m spot taker-flow research"


def _digest(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _receipt(row: dict, cutoff: datetime) -> dict | None:
    p = verified_spot_flow_payload(row.get("payload"), row["available_at"], cutoff)
    if (p is None or row.get("symbol") not in {"BTC", "ETH", "SOL"}
            or p["instrument"] != row["symbol"] + "USDT"
            or row.get("source_id") != f"binance_spot:5m:{row['symbol']}:{p['kline'][0]}"):
        return None
    return p


def qualification(row: dict | None, now: datetime) -> dict:
    """Explain every gate; the newest neutral bar cannot fall back to an older buy."""
    if row is None:
        return {"qualified": False, "reason": "no_verified_spot_flow_receipt"}
    p = _receipt(row, now)
    if p is None:
        return {"qualified": False, "reason": "invalid_spot_flow_receipt"}
    quote = p["quote_volume"]
    net = p["taker_buy_quote"] - p["taker_sell_quote"]
    imbalance = net / quote if quote else 0.0
    report = {"qualified": False, "reason": "balanced_taker_flow", "receipt_id": row["id"],
              "observed_at": p["bar_end"], "available_at": row["available_at"].isoformat(),
              "quote_volume": quote, "net_taker_quote": net, "imbalance": imbalance,
              "trade_count": p["trade_count"], "unit": "USDT", "venue": p["venue"],
              "rule": {"min_quote_volume": MIN_QUOTE, "min_trades": MIN_TRADES,
                       "min_absolute_imbalance": MIN_IMBALANCE}}
    if not timedelta(0) <= now - datetime.fromisoformat(p["bar_end"]) <= MAX_AGE:
        report["reason"] = "stale_spot_flow"
    elif quote < MIN_QUOTE:
        report["reason"] = "insufficient_quote_turnover"
    elif p["trade_count"] < MIN_TRADES:
        report["reason"] = "insufficient_trades"
    elif abs(imbalance) >= MIN_IMBALANCE:
        report.update(qualified=True, reason="measured_taker_imbalance", direction="buy" if net > 0 else "sell")
    return report


def latest_receipts(engine: Any, now: datetime) -> dict[str, dict]:
    from store.pit import PITStore

    latest = {}
    for row in PITStore(engine).get_spot_order_flow_receipts(as_of=now):
        p = _receipt(row, now)
        if p is not None and (row["symbol"] not in latest or
                p["bar_end"] > latest[row["symbol"]]["payload"]["bar_end"]):
            latest[row["symbol"]] = row
    return latest


def _entry(row: dict) -> dict:
    p = row["payload"]
    return {"receipt_id": row["id"], "source": "binance_spot_rest", "instrument": p["instrument"],
            "interval": "5m", "ts": datetime.fromtimestamp(p["kline"][0] / 1000, timezone.utc).isoformat(),
            "price": p["close"], "available_at": row["available_at"].isoformat(),
            "source_sha256": _digest(p)}


def build_forecast(row: dict, issued_at: datetime) -> dict:
    q = qualification(row, issued_at)
    if not q["qualified"]:
        raise ValueError(q["reason"])
    symbol, p, direction = row["symbol"], row["payload"], q["direction"]
    sign = 1 if direction == "buy" else -1
    cohort = "buyers" if direction == "buy" else "sellers"
    actor = f"Binance {p['instrument']} anonymous spot taker {cohort}"
    setup = (f"{actor} initiated net {abs(q['net_taker_quote']):,.2f} USDT against resting orders; "
             f"signed taker imbalance {q['imbalance']:+.2%} across {p['trade_count']:,} trades. "
             "Measured exchange cohort; individual identities unknown. Unvalidated continuation hypothesis.")
    contract = {"version": CONTRACT_VERSION, "label": LABEL, "symbol": symbol,
                "entry": _entry(row), "issued_at": issued_at.isoformat(),
                "due_at": (issued_at + HORIZON).isoformat(), "horizon_minutes": 30,
                "direction": direction, "threshold": THRESHOLD,
                "target_price": p["close"] * (1 + sign * THRESHOLD), "benchmark": "cash",
                "outcome_rule": "first_recorded_completed_5m_close_at_or_after_due_within_15m",
                "qualification": q, "basis": "unvalidated 30m taker-flow continuation; fees excluded"}
    evidence = {"kind": "forecast", "symbol": symbol, "available": True,
                "as_of": p["bar_end"], "price": p["close"], "entry": contract["entry"],
                "change_24h": None, "coverage": None, "forecast": contract,
                "trigger": {"id": row["id"], "source": "exchange_flow", "source_id": row["source_id"],
                            "details": {"actor": actor, "action": "aggressive spot buying" if sign == 1 else "aggressive spot selling",
                                        "liquidity_valve": f"{p['venue']} {p['instrument']} order book",
                                        "flow_direction": direction, "flow_amount": abs(q["net_taker_quote"]), "flow_unit": "USDT"}},
                "confidence": None, "validation": "prospective research; no demonstrated edge",
                "provenance": {"source": "signal_sources", "venue": p["venue"], "measurement": p["measurement"]}}
    pid = hashlib.sha256(f"{MODEL_VERSION}|{row['source_id']}".encode()).hexdigest()[:36]
    return {"prediction_id": pid, "as_of_ts": issued_at.isoformat(), "horizon_label": "swing",
            "target_universe": "crypto", "scoring_class": "liquid_market", "target_symbols": [symbol],
            "question": f"Does measured {symbol} spot taker flow continue over 30 minutes?",
            "call": f"{symbol}: {'bullish' if sign == 1 else 'bearish'} 30m taker-flow hypothesis; 0.10% threshold",
            "timing": contract["due_at"], "setup": setup,
            "invalidation": "Miss if the signed move is below 0.10% at the declared 30m outcome; fees excluded",
            "note": "Separate 30m research cohort; no demonstrated edge or calibrated probability.",
            "seer_summary": "", "market_overlay_snapshot": {}, "mystical_feature_payload": {},
            "grid_feature_payload": {"insight": evidence}, "weight_version": "astrogrid-v1",
            "model_version": MODEL_VERSION, "live_or_local": "live", "status": "pending"}


def produce_forecasts(engine: Any, store: Any, *, as_of: datetime | None = None) -> dict:
    rows = latest_receipts(engine, as_of or datetime.now(timezone.utc))
    issued = as_of or datetime.now(timezone.utc)  # after reading the inputs
    result = {"persisted": 0, "unchanged": 0, "errors": [], "qualification": {}}
    for symbol in ("BTC", "ETH", "SOL"):
        row = rows.get(symbol)
        gate = qualification(row, issued)
        result["qualification"][symbol] = gate
        if not gate["qualified"]:
            continue
        payload = build_forecast(row, issued)
        if store.get_prediction(payload["prediction_id"]):
            result["unchanged"] += 1
        elif store.save_prediction(payload):
            result["persisted"] += 1
        else:
            result["errors"].append(f"Forecast persistence failed for {symbol}")
    return result


def score_contract(engine: Any, *, symbols: list[str], evidence: dict, issued_at: datetime,
                   created_at: datetime, cutoff: datetime) -> dict:
    def unscored(reason):
        return {"unscored": {"reason": reason}}
    try:
        c = evidence["forecast"]
        symbol, entry = c["symbol"], c["entry"]
        due = datetime.fromisoformat(c["due_at"])
        if (c["version"] != CONTRACT_VERSION or symbol not in {"BTC", "ETH", "SOL"} or symbols != [symbol]
                or any(t.tzinfo is None for t in (issued_at, created_at, cutoff, due))
                or c["issued_at"] != issued_at.isoformat() or due != issued_at + HORIZON
                or c["horizon_minutes"] != 30 or c["threshold"] != THRESHOLD
                or c["benchmark"] != "cash"
                or c["outcome_rule"] != "first_recorded_completed_5m_close_at_or_after_due_within_15m"
                or abs((created_at - issued_at).total_seconds()) > 300
                or type(entry["receipt_id"]) is not int):
            return unscored("invalid_order_flow_contract")
    except (KeyError, TypeError, ValueError, OverflowError):
        return unscored("invalid_order_flow_contract")
    if cutoff < due:
        return {"not_mature": True}
    with engine.connect() as conn:
        source = conn.execute(text("""
            SELECT id, ticker, signal_value, created_at, source_id FROM signal_sources
            WHERE id=:id AND source_type='exchange_flow' AND signal_type='spot_taker_flow'
        """), {"id": entry["receipt_id"]}).fetchone()
        if source is None:
            return unscored("unverified_order_flow_entry")
        row = dict(zip(("id", "symbol", "payload", "available_at", "source_id"), source))
        gate = qualification(row, issued_at)
        if (not gate["qualified"] or row["symbol"] != symbol or _entry(row) != entry
                or c.get("direction") != gate["direction"] or c.get("qualification") != gate
                or isinstance(c.get("target_price"), bool) or not isinstance(c.get("target_price"), (int, float))
                or not math.isclose(c["target_price"], entry["price"] *
                                    (1 + THRESHOLD if gate["direction"] == "buy" else 1 - THRESHOLD))):
            return unscored("unverified_order_flow_entry")
        # Enumerate exact own-asset bars, avoiding unsafe JSON timestamp casts
        # and any fallback to stream candles, another venue, coin or SPY.
        first_end = math.ceil(due.timestamp() * 1000 / FIVE_MINUTES_MS) * FIVE_MINUTES_MS
        ids = [f"binance_spot:5m:{symbol}:{end - FIVE_MINUTES_MS}"
               for end in range(first_end, int((due + OUTCOME_GRACE).timestamp() * 1000) + 1, FIVE_MINUTES_MS)]
        outcomes = conn.execute(text("""
            SELECT id, ticker, signal_value, created_at, source_id FROM signal_sources
            WHERE source_type='exchange_flow' AND signal_type='spot_taker_flow'
              AND ticker=:symbol AND source_id=ANY(:ids) AND created_at <= :cutoff
        """), {"symbol": symbol, "ids": ids, "cutoff": cutoff}).fetchall()
    valid = []
    for source in outcomes:
        row = dict(zip(("id", "symbol", "payload", "available_at", "source_id"), source))
        p = _receipt(row, cutoff)
        if (p is not None and row["symbol"] == symbol and row["source_id"] in ids
                and due <= datetime.fromisoformat(p["bar_end"]) <= min(cutoff, due + OUTCOME_GRACE)):
            valid.append(row)
    if not valid:
        return unscored("missing_order_flow_outcome")
    outcome = min(valid, key=lambda r: r["payload"]["bar_end"])
    realized = outcome["payload"]["close"] / entry["price"] - 1
    signed = realized * (1 if c["direction"] == "buy" else -1)
    hit = signed >= THRESHOLD - 1e-12
    return {"benchmark_symbol": "cash", "benchmark_return": 0.0, "realized_return": realized,
            "alpha_vs_benchmark": signed, "verdict": "hit" if hit else "miss",
            "invalidation_status": "respected" if hit else "violated",
            "max_favorable_excursion": None, "max_adverse_excursion": None, "regime_context": {},
            "attribution_grid": ["measured_spot_taker_flow"], "attribution_mystical": [],
            "attribution_noise": ["unvalidated_rule", "fees_not_included", "anonymous_cohort"],
            "raw_payload": {"contract_version": CONTRACT_VERSION, "symbols": symbols, "entry": entry,
                            "outcome": {**_entry(outcome), "bar_end": outcome["payload"]["bar_end"]},
                            "signed_return": signed, "direction": c["direction"], "due_at": due.isoformat(),
                            "horizon_minutes": 30, "threshold": THRESHOLD}}
