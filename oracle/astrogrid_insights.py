"""Evidence-backed AstroGrid observations and prospective forecast contracts.

Extends the existing crypto producer and AstroGrid ledger. Market conditions
are observations, never causes. A forecast additionally needs an identifiable
actor and liquidity action; absent one we publish an explicit abstention.
"""
from __future__ import annotations

import hashlib
import math
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

MODEL_VERSION = "astrogrid-evidence-v2"
CONTRACT_VERSION = "binance_observed_5m_forward_v1"
SYMBOLS = ("BTC", "ETH", "SOL")
BAR = timedelta(minutes=5)
MAX_AGE = timedelta(minutes=20)
HORIZON = timedelta(hours=24)
OUTCOME_GRACE = timedelta(minutes=15)


def _positive(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def read_candles(engine: Any, symbol: str, as_of: datetime) -> list[dict[str, Any]]:
    from store.pit import PITStore

    return PITStore(engine).get_closed_crypto_candles(symbol, as_of=as_of)


def read_triggers(engine: Any, as_of: datetime) -> list[dict[str, Any]]:
    from store.pit import PITStore

    return PITStore(engine).get_crypto_actor_actions(as_of=as_of)


def causal_trigger(rows: list[dict[str, Any]], symbol: str) -> dict[str, Any] | None:
    for row in rows:
        detail = row.get("details")
        if row.get("symbol") != symbol or not isinstance(detail, dict):
            continue
        # A specific actor, action, valve and measured directional flow are
        # mandatory. No inference from source_type or generic BUY/SELL labels.
        if (all(isinstance(detail.get(k), str) and detail[k].strip()
                for k in ("actor", "action", "liquidity_valve"))
                and detail.get("flow_direction") in {"buy", "sell"}
                and _positive(detail.get("flow_amount"))
                and detail.get("flow_unit") in {symbol, "USD", "USDT"}):
            return {k: row[k] for k in ("id", "symbol", "source", "source_id", "event_date", "available_at")} | {
                "details": {k: detail[k] for k in
                            ("actor", "action", "liquidity_valve", "flow_direction", "flow_amount", "flow_unit")}}
    return None


def observe(symbol: str, candles: list[dict[str, Any]], as_of: datetime) -> dict[str, Any]:
    """Describe measured change and coverage; never fill missing data with zero."""
    result: dict[str, Any] = {"symbol": symbol, "available": False, "as_of": None,
                              "reason": "No completed Binance trade candles", "price": None,
                              "change_24h": None, "recorded_volume_24h": None, "coverage": None}
    valid = [c for c in candles if _positive(c.get("close"))
             and c["ts"] + BAR <= as_of and c["available_at"] <= as_of]
    if not valid:
        return result
    valid.sort(key=lambda c: c["ts"])
    latest = valid[-1]
    ended = latest["ts"] + BAR
    if as_of - ended > MAX_AGE:
        result.update(reason="Binance trade candles are stale", last_observed_at=ended.isoformat())
        return result
    target = latest["ts"] - HORIZON
    baseline = next((c for c in valid if c["ts"] == target), None)
    window = [c for c in valid if target < c["ts"] <= latest["ts"]]
    coverage = len({c["ts"] for c in window}) / 288
    complete = baseline is not None and coverage >= 0.95
    volumes = [c.get("volume") for c in window]
    volume_known = all(isinstance(v, (int, float)) and math.isfinite(v) and v >= 0 for v in volumes)
    result.update(available=True, as_of=ended.isoformat(), reason=None, price=latest["close"],
                  change_24h=latest["close"] / baseline["close"] - 1 if complete else None,
                  volume_24h=sum(volumes) if complete and volume_known else None,
                  coverage=round(min(coverage, 1), 4),
                  entry={"source": "binance", "instrument": symbol + "USDT", "interval": "5m",
                         "ts": latest["ts"].isoformat(), "price": latest["close"],
                         "available_at": latest["available_at"].isoformat()},
                  provenance={"source": "realtime_candles", "basis": "observed Binance USDT trade buckets",
                              "provider_certified_final": False, "price_unit": "USDT"})
    return result


def build_payload(observation: dict[str, Any], trigger: dict[str, Any] | None,
                  as_of: datetime) -> dict[str, Any]:
    symbol, price = observation["symbol"], observation["price"]
    change = observation.get("change_24h")
    forecast = trigger is not None and change is not None
    direction = trigger["details"]["flow_direction"] if forecast else None
    kind = "forecast" if forecast else "observation"
    contract = None
    if forecast:
        # A fixed, declared research rule, not a calibrated probability.
        threshold = 0.01
        sign = 1 if direction == "buy" else -1
        contract = {"version": CONTRACT_VERSION, "symbol": symbol,
                    "entry": observation["entry"], "issued_at": as_of.isoformat(),
                    "due_at": (as_of + HORIZON).isoformat(), "direction": direction,
                    "threshold": threshold, "target_price": price * (1 + sign * threshold),
                    "benchmark": "cash", "basis": "unvalidated 24h directional flow hypothesis; 1% threshold"}
        call = f"{symbol}: {'bullish' if sign == 1 else 'bearish'} flow hypothesis; 1% move over 24h"
        detail = trigger["details"]
        setup = (f"{detail['actor']} {detail['action']}; {detail['flow_amount']:g} {detail['flow_unit']} "
                 f"through {detail['liquidity_valve']}. Observed 24h price change {change:+.2%}.")
    else:
        call = (f"{symbol} {change:+.2%} over 24h" if change is not None
                else f"{symbol}: ${price:,.2f}; incomplete 24h coverage")
        relative = observation.get("relative_return_vs_btc")
        if relative is not None:
            call += f"; {relative:+.2%} relative to BTC"
        setup = "No verified actor-driven liquidity trigger. No directional forecast issued."
    evidence = {**observation, "kind": kind, "trigger": trigger, "forecast": contract,
                "confidence": None, "validation": "prospective research; no demonstrated edge"}
    event_key = str(trigger["id"]) if forecast else observation["as_of"]
    key = f"{MODEL_VERSION}|{symbol}|{kind}|{event_key}"
    return {"prediction_id": hashlib.sha256(key.encode()).hexdigest()[:36],
            "as_of_ts": as_of.isoformat(), "horizon_label": "swing", "target_universe": "crypto",
            "scoring_class": "liquid_market" if forecast else "unscored_experimental",
            "target_symbols": [symbol], "question": f"What does current evidence show for {symbol}?",
            "call": call, "timing": contract["due_at"] if contract else observation["as_of"],
            "setup": setup, "invalidation": "Miss if the declared 1% move is absent at the 24h outcome" if forecast
            else "Observation only; no win/loss grade",
            "note": "Evidence-backed research; celestial context is separate and unvalidated.",
            "seer_summary": "", "market_overlay_snapshot": {}, "mystical_feature_payload": {},
            "grid_feature_payload": {"insight": evidence}, "weight_version": "astrogrid-v1",
            "model_version": MODEL_VERSION, "live_or_local": "live", "status": "pending"}


def produce_insights(engine: Any, store: Any, *, as_of: datetime | None = None,
                     persist: bool = True) -> dict[str, Any]:
    now = as_of or datetime.now(timezone.utc)
    triggers = read_triggers(engine, now)
    result: dict[str, Any] = {"observations": [], "persisted": 0, "unchanged": 0, "errors": []}
    recorded = {symbol: read_candles(engine, symbol, now) for symbol in SYMBOLS}
    # Forward issuance happens after the inputs were actually read. An explicit
    # clock is only for deterministic/disposable tests, never a replay claim.
    issued_at = as_of or datetime.now(timezone.utc)
    observations = [observe(symbol, recorded[symbol], issued_at) for symbol in SYMBOLS]
    btc = observations[0]
    for observation in observations:
        symbol = observation["symbol"]
        change, btc_change = observation.get("change_24h"), btc.get("change_24h")
        if (symbol != "BTC" and observation.get("as_of") == btc.get("as_of")
                and change is not None and btc_change is not None):
            observation["relative_return_vs_btc"] = (1 + change) / (1 + btc_change) - 1
        else:
            observation["relative_return_vs_btc"] = None
        result["observations"].append(observation)
        if not observation["available"]:
            continue
        payloads = [build_payload(observation, None, issued_at)]
        trigger = causal_trigger(triggers, symbol)
        if trigger is not None and observation["change_24h"] is not None:
            payloads.append(build_payload(observation, trigger, issued_at))
        for payload in payloads:
            if persist:
                if store.get_prediction(payload["prediction_id"]):
                    result["unchanged"] += 1
                elif store.save_prediction(payload):
                    result["persisted"] += 1
                else:
                    result["errors"].append(f"Persistence failed for {symbol}")
    return result


def score_contract(engine: Any, *, symbols: list[str], evidence: dict[str, Any],
                   issued_at: datetime, created_at: datetime, cutoff: datetime) -> dict[str, Any]:
    """Grade only a matured, verified entry on its own asset; never substitutes.

    The outcome is the first closed 5m bucket at/after the due time, within
    fifteen minutes. Missing outcomes remain pending. Original forecasts and
    source rows are never modified.
    """
    contract = evidence.get("forecast")
    def unscored(reason: str) -> dict[str, Any]:
        return {"unscored": {"reason": reason}}
    if not isinstance(contract, dict) or contract.get("version") != CONTRACT_VERSION:
        return unscored("invalid_insight_contract")
    try:
        symbol = contract["symbol"]
        entry = contract["entry"]
        due = datetime.fromisoformat(contract["due_at"])
        declared_issue = datetime.fromisoformat(contract["issued_at"])
        ts = datetime.fromisoformat(entry["ts"])
        available = datetime.fromisoformat(entry["available_at"])
        if (symbol not in SYMBOLS or symbols != [symbol] or entry["source"] != "binance"
                or entry["instrument"] != symbol + "USDT" or entry["interval"] != "5m"
                or any(t.tzinfo is None for t in (due, ts, available, declared_issue, issued_at, created_at, cutoff))
                or declared_issue != issued_at or due != issued_at + HORIZON
                or abs((created_at - issued_at).total_seconds()) > 300
                or not timedelta(0) <= issued_at - (ts + BAR) <= MAX_AGE
                or available > issued_at or contract["direction"] not in {"buy", "sell"}
                or contract["threshold"] != 0.01 or not _positive(entry["price"])
                or not _positive(contract.get("target_price"))
                or not math.isclose(contract["target_price"], entry["price"] *
                                    (1.01 if contract["direction"] == "buy" else 0.99))):
            return unscored("invalid_insight_contract")
    except (KeyError, ValueError, TypeError, OverflowError):
        return unscored("invalid_insight_contract")
    if cutoff < due:
        return {"not_mature": True}
    with engine.connect() as conn:
        entry_row = conn.execute(text("""
            SELECT close, created_at FROM realtime_candles
            WHERE source = :source AND symbol = :instrument AND interval = :interval AND ts = :ts
        """), {"source": "binance", "instrument": symbol + "USDT", "interval": "5m", "ts": ts}).fetchone()
        if not entry_row or entry_row[0] != entry["price"] or entry_row[1] != available:
            return unscored("unverified_insight_entry")
        outcome = conn.execute(text("""
            SELECT ts, close, created_at FROM realtime_candles
            WHERE source = :source AND symbol = :instrument AND interval = :interval
              AND ts + INTERVAL '5 minutes' >= :due
              AND ts + INTERVAL '5 minutes' <= :last_outcome
              AND ts + INTERVAL '5 minutes' <= :cutoff AND created_at <= :cutoff
            ORDER BY ts LIMIT 1
        """), {"source": "binance", "instrument": symbol + "USDT", "interval": "5m",
               "due": due, "last_outcome": due + OUTCOME_GRACE, "cutoff": cutoff}).fetchone()
    if not outcome or not _positive(outcome[1]):
        return unscored("missing_insight_outcome")
    realized = outcome[1] / entry["price"] - 1
    signed = realized * (1 if contract["direction"] == "buy" else -1)
    return {"benchmark_symbol": "cash", "benchmark_return": 0.0,
            "realized_return": realized, "alpha_vs_benchmark": signed,
            "verdict": "hit" if signed >= contract["threshold"] - 1e-12 else "miss",
            "invalidation_status": "respected" if signed >= contract["threshold"] - 1e-12 else "violated",
            "max_favorable_excursion": None, "max_adverse_excursion": None,
            "regime_context": {}, "attribution_grid": ["verified_actor_flow"],
            "attribution_mystical": [], "attribution_noise": ["unvalidated_rule", "fees_not_included"],
            "raw_payload": {"contract_version": CONTRACT_VERSION, "symbols": symbols,
                            "entry": entry, "outcome": {"ts": outcome[0].isoformat(),
                            "price": outcome[1], "available_at": outcome[2].isoformat()},
                            "direction": contract["direction"], "signed_return": signed,
                            "due_at": due.isoformat(), "threshold": contract["threshold"]}}


def public_insights(engine: Any, now: datetime | None = None) -> dict[str, Any]:
    """A bounded read of the published evidence ledger, not a generator on GET."""
    from config import settings
    from store.astrogrid import _safe_schema_name

    now = now or datetime.now(timezone.utc)
    with engine.connect() as conn:
        conn.execute(text("SELECT set_config('search_path', :path, true)"),
                     {"path": '"' + _safe_schema_name(settings.ASTROGRID_DB_SCHEMA) + '", public'})
        rows = conn.execute(text("""
            SELECT DISTINCT ON (pr.target_symbols->>0)
                   pr.prediction_id, pr.as_of_ts, pr.call, pr.setup, pr.grid_feature_payload,
                   ps.verdict, ps.raw_payload, ps.scored_at
            FROM prediction_run pr
            LEFT JOIN prediction_score ps ON ps.prediction_run_id = pr.id
            WHERE pr.model_version = :model
              AND pr.grid_feature_payload->'insight'->>'kind' = 'observation'
            ORDER BY pr.target_symbols->>0, pr.created_at DESC, pr.id DESC
        """), {"model": MODEL_VERSION}).fetchall()
        forecasts = conn.execute(text("""
            SELECT pr.prediction_id, pr.as_of_ts, pr.call, pr.setup, pr.grid_feature_payload,
                   ps.verdict, ps.raw_payload, ps.scored_at
            FROM prediction_run pr
            LEFT JOIN prediction_score ps ON ps.prediction_run_id = pr.id
            WHERE pr.model_version = :model
              AND pr.grid_feature_payload->'insight'->>'kind' = 'forecast'
            ORDER BY pr.created_at DESC LIMIT 12
        """), {"model": MODEL_VERSION}).fetchall()
        scores = conn.execute(text("""
            SELECT count(*), count(*) FILTER (WHERE ps.verdict = 'hit'), max(ps.scored_at)
            FROM prediction_score ps
            JOIN prediction_run pr ON pr.id = ps.prediction_run_id
            WHERE pr.model_version = :model AND ps.raw_payload->>'contract_version' = :contract
        """), {"model": MODEL_VERSION, "contract": CONTRACT_VERSION}).fetchone()
    items = []
    for row in [*rows, *forecasts]:
        data = row[4].get("insight", {}) if isinstance(row[4], dict) else {}
        stamp = data.get("as_of")
        stale = not stamp or now - datetime.fromisoformat(stamp) > MAX_AGE
        items.append({"prediction_id": row[0], "issued_at": row[1].isoformat(),
                      "call": row[2], "setup": row[3], "evidence": data, "stale": stale,
                      "status": row[5] or ("pending" if data.get("forecast") else "observation"),
                      "score": row[6], "scored_at": row[7].isoformat() if row[7] else None})
    published = {item["evidence"].get("symbol") for item in items}
    for symbol in SYMBOLS:
        if symbol not in published:
            items.append({"prediction_id": "", "issued_at": None, "call": f"{symbol}: evidence unavailable",
                          "setup": "No usable market evidence has been published for this asset.",
                          "evidence": {"symbol": symbol, "available": False, "as_of": None},
                          "stale": True, "status": "unavailable", "score": None, "scored_at": None})
    return {"model_version": MODEL_VERSION, "checked_at": now.isoformat(), "items": items,
            "performance": {"scored": scores[0], "hits": scores[1],
                            "accuracy": scores[1] / scores[0] if scores[0] else None,
                            "last_scored_at": scores[2].isoformat() if scores[2] else None,
                            "basis": "prospective actor-flow forecasts only; observations and legacy templates excluded"}}


def answer_from_insights(feed: dict[str, Any], requested: list[str], *,
                         now: datetime | None = None) -> dict[str, Any]:
    """Answer from published server evidence; never trust a client's overlay."""
    now = now or datetime.now(timezone.utc)
    matches = [i for i in feed.get("items", [])
               if not requested or i.get("evidence", {}).get("symbol") in requested]
    observations = [i for i in matches if i.get("status") == "observation" and not i.get("stale")]
    forecasts = [i for i in matches if i.get("status") == "pending"
                 and i.get("evidence", {}).get("forecast")
                 and datetime.fromisoformat(i["evidence"]["forecast"]["due_at"]) > now]
    return {"call": "; ".join(i["call"] for i in forecasts) or "No qualified directional forecast",
            "timing": "24h prospective outcome" if forecasts else "Latest measured market conditions",
            "setup": "; ".join(i["call"] for i in observations)
                     or "No fresh published observations for the requested asset.",
            "invalidation": "Grade the declared 1% directional move at the 24h outcome" if forecasts
            else "Observation only; no trade call or win/loss grade",
            "horizon": "24h" if forecasts else "observation", "target_group": "crypto" if observations or forecasts else None,
            "target_symbols": sorted({i["evidence"]["symbol"] for i in observations + forecasts}),
            "evidence_ids": [i["prediction_id"] for i in observations + forecasts],
            "confidence": None, "disclaimer": "Research hypotheses; no demonstrated trading edge."}
