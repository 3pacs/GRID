"""Seed AstroGrid with a high-value question corpus and prediction records.

This script bootstraps the learning loop with market questions people
actually ask, then persists structured AstroGrid predictions so they can
be scored, backtested, and reviewed later.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from loguru import logger as log
from pydantic import BaseModel

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.dependencies import get_db_engine
from api.routers import astrogrid as astro
from api.routers import intelligence, regime
from api.routers.watchlist import _batch_fetch_prices
from oracle.astrogrid_universe import (
    enrich_astrogrid_scoreable_universe,
    get_astrogrid_scoreable_universe,
)

SEED_LENS_IDS = ["western", "vedic", "hermetic", "taoist"]
SEED_UNIVERSE = get_astrogrid_scoreable_universe()


@dataclass(frozen=True)
class QuestionTemplate:
    question: str
    symbols: tuple[str, ...]
    horizon: str
    kind: str
    target_universe: str = "hybrid"
    scoring_class: str = "liquid_market"


def default_question_templates() -> list[QuestionTemplate]:
    return [
        QuestionTemplate(
            question="What crypto should I buy right now: BTC, ETH, or SOL?",
            symbols=("BTC", "ETH", "SOL"),
            horizon="swing",
            kind="relative_best",
        ),
        QuestionTemplate(
            question="Which stock is the best buy right now: Google, Apple, or Microsoft?",
            symbols=("GOOGL", "AAPL", "MSFT"),
            horizon="swing",
            kind="relative_best",
        ),
        QuestionTemplate(
            question="Is NVDA worth buying right now or is it too late?",
            symbols=("NVDA",),
            horizon="swing",
            kind="direct_buy",
        ),
        QuestionTemplate(
            question="When should I buy Meta?",
            symbols=("META",),
            horizon="swing",
            kind="timing",
        ),
        QuestionTemplate(
            question="What should I long for the next month: BTC, QQQ, or GLD?",
            symbols=("BTC", "QQQ", "GLD"),
            horizon="macro",
            kind="relative_best",
        ),
        QuestionTemplate(
            question="Should I buy SPY or QQQ over the next month?",
            symbols=("SPY", "QQQ"),
            horizon="macro",
            kind="relative_best",
        ),
        QuestionTemplate(
            question="Should I buy TLT as a macro hedge right now?",
            symbols=("TLT",),
            horizon="macro",
            kind="direct_buy",
        ),
        QuestionTemplate(
            question="Should I rotate into gold or stay in tech?",
            symbols=("GLD", "QQQ"),
            horizon="macro",
            kind="relative_best",
        ),
        QuestionTemplate(
            question="What should I avoid buying right now: ETH, SOL, or NVDA?",
            symbols=("ETH", "SOL", "NVDA"),
            horizon="swing",
            kind="avoid",
        ),
        QuestionTemplate(
            question="Should I buy the dollar here or fade it?",
            symbols=("DXY",),
            horizon="macro",
            kind="direct_buy",
        ),
    ]


def _regime_direction(regime_state: str | None) -> int:
    state = str(regime_state or "").upper()
    if state in {"GROWTH", "RISK_ON", "BULL", "BULLISH"}:
        return 1
    if state in {"FRAGILE", "CRISIS", "RISK_OFF", "BEAR", "BEARISH"}:
        return -1
    return 0


def _thesis_direction(thesis_payload: dict[str, Any]) -> int:
    raw = str(
        thesis_payload.get("overall_direction")
        or thesis_payload.get("bias")
        or thesis_payload.get("stance")
        or ""
    ).upper()
    if "BULL" in raw or "RISK_ON" in raw:
        return 1
    if "BEAR" in raw or "RISK_OFF" in raw:
        return -1
    return 0


def _score_asset(item: dict[str, Any], regime_bias: int, thesis_bias: int) -> float:
    momentum = float(item.get("momentum_score") or 0.0)
    confidence = float(item.get("confidence") or 0.0)
    change_20d = float(item.get("change_20d_pct") or 0.0) / 100.0
    return round(momentum + change_20d + (regime_bias * 0.12) + (thesis_bias * 0.15) + (confidence * 0.08), 6)


def _stop_pct(item: dict[str, Any]) -> float:
    change_5d = abs(float(item.get("change_5d_pct") or 0.0))
    change_20d = abs(float(item.get("change_20d_pct") or 0.0))
    stop = max(3.5, min(12.0, (change_5d * 0.75) + (change_20d * 0.2)))
    return round(stop, 1)


def _timing_line(snapshot: dict[str, Any], horizon: str) -> str:
    events = list(snapshot.get("events") or [])
    first_event = events[0] if events else {}
    event_name = first_event.get("name") or snapshot.get("lunar", {}).get("phase_name") or "current window"
    if horizon == "macro":
        return f"{snapshot.get('date')} -> {event_name} / 30d frame"
    return f"{snapshot.get('date')} -> {event_name} / 7d frame"


def _seer_note(snapshot: dict[str, Any]) -> str:
    seer = snapshot.get("seer") or {}
    return str(seer.get("reading") or seer.get("prediction") or "geometry leads.")


def _build_relative_prediction(
    template: QuestionTemplate,
    ranked_items: list[dict[str, Any]],
    snapshot: dict[str, Any],
    regime_payload: dict[str, Any],
    thesis_payload: dict[str, Any],
) -> dict[str, Any]:
    leader = ranked_items[0]
    runner_up = ranked_items[1] if len(ranked_items) > 1 else None
    leader_symbol = leader["symbol"]
    leader_score = float(leader["seed_score"])
    spread = leader_score - float(runner_up["seed_score"]) if runner_up else leader_score
    stop = _stop_pct(leader)
    if leader_score >= 0.28 and spread >= 0.08:
        call = f"buy {leader_symbol}"
        setup = f"{leader_symbol} leads its field with {leader['trend']} momentum"
        note = f"{_seer_note(snapshot)} Favor the leader while spread over peers holds."
    elif leader_score >= 0.12:
        call = f"accumulate {leader_symbol}"
        setup = f"{leader_symbol} has the cleanest relative bid, but conviction is not broad yet"
        note = f"{_seer_note(snapshot)} Enter in pieces and let the window confirm."
    else:
        call = f"wait {leader_symbol}"
        setup = f"{leader_symbol} is first in a weak field; relative edge exists but absolute edge is thin"
        note = f"{_seer_note(snapshot)} Hold cash until the field resolves."
    invalidation = (
        f"break if {leader_symbol} closes {stop}% below seed or regime flips against the setup"
    )
    return {
        "call": call,
        "timing": _timing_line(snapshot, template.horizon),
        "setup": setup,
        "invalidation": invalidation,
        "note": note,
        "target_symbols": [leader_symbol] + [item["symbol"] for item in ranked_items[1:3]],
    }


def _build_direct_prediction(
    template: QuestionTemplate,
    item: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    symbol = item["symbol"]
    score = float(item["seed_score"])
    stop = _stop_pct(item)
    stretched = abs(float(item.get("change_5d_pct") or 0.0)) > 7.5
    if score >= 0.3 and not stretched:
        call = f"buy {symbol}"
        setup = f"{symbol} is in {item['trend']} with clean follow-through"
        note = f"{_seer_note(snapshot)} Press while the tape stays orderly."
    elif score >= 0.18:
        call = f"buy {symbol} on weakness"
        setup = f"{symbol} has positive edge but entry is stretched"
        note = f"{_seer_note(snapshot)} Wait for a pullback or quieter session."
    elif score <= -0.15:
        call = f"avoid {symbol}"
        setup = f"{symbol} trend and field quality are both hostile"
        note = f"{_seer_note(snapshot)} There is cleaner risk elsewhere."
    else:
        call = f"wait {symbol}"
        setup = f"{symbol} edge is mixed"
        note = f"{_seer_note(snapshot)} Stand down until the next cleaner cut."
    invalidation = f"break if {symbol} closes {stop}% through the wrong side of the setup"
    return {
        "call": call,
        "timing": _timing_line(snapshot, template.horizon),
        "setup": setup,
        "invalidation": invalidation,
        "note": note,
        "target_symbols": [symbol],
    }


def _build_timing_prediction(
    template: QuestionTemplate,
    item: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    symbol = item["symbol"]
    stop = _stop_pct(item)
    phase = str((snapshot.get("lunar") or {}).get("phase_name") or "window")
    trend = item["trend"]
    if float(item["seed_score"]) >= 0.18:
        call = f"buy {symbol} on pullback"
        setup = f"{symbol} still has edge, but timing matters more than direction here"
        note = f"{_seer_note(snapshot)} Use the {phase.lower()} window to scale in, not chase."
    else:
        call = f"wait {symbol}"
        setup = f"{symbol} does not have a clean entry window yet"
        note = f"{_seer_note(snapshot)} Let the {trend} state resolve first."
    invalidation = f"break if {symbol} loses {stop}% from the trigger zone"
    return {
        "call": call,
        "timing": _timing_line(snapshot, template.horizon),
        "setup": setup,
        "invalidation": invalidation,
        "note": note,
        "target_symbols": [symbol],
    }


def _build_avoid_prediction(
    template: QuestionTemplate,
    ranked_items: list[dict[str, Any]],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    worst = ranked_items[-1]
    worst_symbol = worst["symbol"]
    stop = _stop_pct(worst)
    call = f"avoid {worst_symbol}"
    setup = f"{worst_symbol} is the weakest name in the set with {worst['trend']} structure"
    note = f"{_seer_note(snapshot)} Do not force the weak leg when stronger alternatives exist."
    invalidation = f"break the avoid call only if {worst_symbol} reclaims momentum and survives a {stop}% test"
    return {
        "call": call,
        "timing": _timing_line(snapshot, template.horizon),
        "setup": setup,
        "invalidation": invalidation,
        "note": note,
        "target_symbols": [worst_symbol],
    }


def _build_market_overlay(
    *,
    scorecard: dict[str, Any],
    regime_payload: dict[str, Any],
    thesis_payload: dict[str, Any],
) -> dict[str, Any]:
    items = list(scorecard.get("items") or [])
    leaders = sorted(items, key=lambda item: float(item.get("seed_score") or 0.0), reverse=True)[:3]
    laggards = sorted(items, key=lambda item: float(item.get("seed_score") or 0.0))[:3]
    return {
        "regime": {
            "state": regime_payload.get("state"),
            "confidence": regime_payload.get("confidence"),
            "transition_probability": regime_payload.get("transition_probability"),
        },
        "thesis": {
            "overall_direction": thesis_payload.get("overall_direction"),
            "conviction": thesis_payload.get("conviction"),
            "narrative": thesis_payload.get("narrative"),
        },
        "scorecard": {
            "leaders": leaders,
            "laggards": laggards,
            "items": items,
        },
    }


def _coerce_thesis_payload(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _coerce_model_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, BaseModel):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    return {}


async def _build_snapshot(as_of_date: date) -> dict[str, Any]:
    return await astro.get_snapshot(date_str=as_of_date.isoformat())


async def _safe_regime_payload(as_of_date: date) -> dict[str, Any]:
    if as_of_date != date.today():
        try:
            state, _ = astro._get_market_regime(get_db_engine(), as_of_date)
            return {"state": state, "confidence": None, "transition_probability": None}
        except Exception as exc:
            log.warning("Seed corpus historical regime unavailable: {e}", e=str(exc))
            return {"state": None, "confidence": None, "transition_probability": None}
    try:
        return _coerce_model_dict(await regime.get_current())
    except Exception as exc:
        log.warning("Seed corpus live regime unavailable: {e}", e=str(exc))
        return {"state": None, "confidence": None, "transition_probability": None}


async def _safe_thesis_payload(as_of_date: date, regime_payload: dict[str, Any]) -> dict[str, Any]:
    if as_of_date != date.today():
        direction = _regime_direction(regime_payload.get("state"))
        return {
            "overall_direction": "BULLISH" if direction > 0 else "BEARISH" if direction < 0 else "NEUTRAL",
            "conviction": 40,
            "narrative": "Historical seed thesis derived from regime state and relative strength.",
        }
    try:
        return _coerce_thesis_payload(await intelligence.get_unified_thesis())
    except Exception as exc:
        log.warning("Seed corpus live thesis unavailable: {e}", e=str(exc))
        direction = _regime_direction(regime_payload.get("state"))
        return {
            "overall_direction": "BULLISH" if direction > 0 else "BEARISH" if direction < 0 else "NEUTRAL",
            "conviction": 35,
            "narrative": "Live thesis unavailable; using regime fallback.",
        }


def _build_scorecard(as_of_date: date) -> dict[str, Any]:
    history_start = as_of_date - timedelta(days=120)
    use_live_quotes = as_of_date == date.today()
    lookup_tickers = [asset["lookup_ticker"] for asset in SEED_UNIVERSE]
    try:
        live_quotes = _batch_fetch_prices(lookup_tickers) if use_live_quotes else {}
    except Exception as exc:
        log.debug("Seed corpus live-price fallback unavailable: {e}", e=str(exc))
        live_quotes = {}

    items: list[dict[str, Any]] = []
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            for asset in enrich_astrogrid_scoreable_universe(conn):
                feature_name, candidates = astro._resolve_scorecard_feature(conn, asset)
                history = astro._load_scorecard_history(conn, feature_name, history_start) if feature_name else []
                history = [(obs_date, value) for obs_date, value in history if obs_date <= as_of_date]
                live_quote = live_quotes.get(asset["lookup_ticker"]) if use_live_quotes else None
                item = astro._build_scorecard_item(asset, feature_name, candidates, history, live_quote)
                items.append(item)
    except Exception as exc:
        log.warning("Seed corpus DB scorecard unavailable: {e}", e=str(exc))
        for asset in SEED_UNIVERSE:
            live_quote = live_quotes.get(asset["lookup_ticker"]) if use_live_quotes else None
            item = astro._build_scorecard_item(asset, None, [], [], live_quote)
            items.append(item)
    return {
        "generated_at": datetime.now().isoformat(),
        "items": items,
    }


def build_prediction_request(
    *,
    template: QuestionTemplate,
    snapshot: dict[str, Any],
    scorecard: dict[str, Any],
    regime_payload: dict[str, Any],
    thesis_payload: dict[str, Any],
    as_of_date: date,
) -> astro.AstrogridPredictionRequest:
    regime_bias = _regime_direction(regime_payload.get("state"))
    thesis_bias = _thesis_direction(thesis_payload)
    item_map = {item["symbol"]: dict(item) for item in scorecard["items"]}
    candidates = []
    for symbol in template.symbols:
        item = dict(item_map.get(symbol, {"symbol": symbol, "trend": "mixed", "confidence": 0.0}))
        item["seed_score"] = _score_asset(item, regime_bias, thesis_bias)
        candidates.append(item)
    ranked = sorted(candidates, key=lambda item: float(item.get("seed_score") or 0.0), reverse=True)
    if template.kind == "relative_best":
        directive = _build_relative_prediction(template, ranked, snapshot, regime_payload, thesis_payload)
    elif template.kind == "timing":
        directive = _build_timing_prediction(template, ranked[0], snapshot)
    elif template.kind == "avoid":
        directive = _build_avoid_prediction(template, ranked, snapshot)
    else:
        directive = _build_direct_prediction(template, ranked[0], snapshot)

    overlay = _build_market_overlay(scorecard=scorecard, regime_payload=regime_payload, thesis_payload=thesis_payload)
    overlay["scorecard"]["leaders"] = [
        {key: item.get(key) for key in ("symbol", "label", "group", "bias", "trend", "confidence", "seed_score")}
        for item in ranked[:3]
    ]
    overlay["scorecard"]["laggards"] = [
        {key: item.get(key) for key in ("symbol", "label", "group", "bias", "trend", "confidence", "seed_score")}
        for item in list(reversed(ranked[-3:]))
    ]
    target_items = [item_map.get(symbol, {}) for symbol in directive["target_symbols"]]
    target_statuses = [str(item.get("status") or "unscored") for item in target_items]
    scoreable_now = bool(target_statuses) and all(status == "scoreable_now" for status in target_statuses)
    scoring_class = template.scoring_class if scoreable_now else "unscored_experimental"
    overlay["scorecard"]["target_statuses"] = [
        {
            "symbol": symbol,
            "status": str(item_map.get(symbol, {}).get("status") or "unscored"),
            "scoreable_now": bool(item_map.get(symbol, {}).get("scoreable_now")),
            "reason_if_not": item_map.get(symbol, {}).get("reason_if_not"),
        }
        for symbol in directive["target_symbols"]
    ]

    return astro.AstrogridPredictionRequest(
        question=template.question,
        call=directive["call"],
        timing=directive["timing"],
        setup=directive["setup"],
        invalidation=directive["invalidation"],
        as_of_ts=f"{as_of_date.isoformat()}T12:00:00+00:00",
        note=directive["note"],
        mode="chorus",
        lens_ids=SEED_LENS_IDS,
        snapshot=snapshot,
        seer=snapshot.get("seer") or {},
        engine_outputs=[{"engine_id": lens_id} for lens_id in SEED_LENS_IDS],
        market_overlay_snapshot=overlay,
        target_universe=template.target_universe,
        scoring_class=scoring_class,
        target_symbols=directive["target_symbols"],
        horizon_label=template.horizon,
        weight_version="astrogrid-v1",
        model_version="astrogrid-seed-v1",
        live_or_local="live" if as_of_date_is_live(snapshot) else "archive",
        publish_oracle=True,
    )


def as_of_date_is_live(snapshot: dict[str, Any]) -> bool:
    snapshot_date = snapshot.get("date")
    return snapshot_date == date.today().isoformat()


async def seed_prediction_corpus(
    *,
    start_date: date,
    end_date: date,
    step_days: int,
    dry_run: bool,
    export_path: Path | None,
) -> dict[str, Any]:
    templates = default_question_templates()
    results: list[dict[str, Any]] = []
    current = start_date
    while current <= end_date:
        snapshot = await _build_snapshot(current)
        scorecard = _build_scorecard(current)
        regime_payload = await _safe_regime_payload(current)
        thesis_payload = await _safe_thesis_payload(current, regime_payload)
        for template in templates:
            req = build_prediction_request(
                template=template,
                snapshot=snapshot,
                scorecard=scorecard,
                regime_payload=regime_payload,
                thesis_payload=thesis_payload,
                as_of_date=current,
            )
            if dry_run:
                record = {
                    "as_of_date": current.isoformat(),
                    "question": req.question,
                    "call": req.call,
                    "timing": req.timing,
                    "setup": req.setup,
                    "invalidation": req.invalidation,
                    "target_symbols": req.target_symbols,
                }
            else:
                record = await astro.create_prediction(req)
                record["as_of_date"] = current.isoformat()
            results.append(record)
        current += timedelta(days=step_days)
    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "step_days": step_days,
        "question_count": len(default_question_templates()),
        "records": results,
    }
    if export_path:
        export_path.parent.mkdir(parents=True, exist_ok=True)
        export_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed AstroGrid prediction corpus.")
    parser.add_argument("--start-date", default=date.today().isoformat())
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--step-days", type=int, default=7)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--export", type=Path, default=None)
    return parser.parse_args()


async def _main() -> int:
    args = _parse_args()
    start_date = date.fromisoformat(args.start_date)
    end_date = date.fromisoformat(args.end_date)
    result = await seed_prediction_corpus(
        start_date=start_date,
        end_date=end_date,
        step_days=max(1, args.step_days),
        dry_run=args.dry_run,
        export_path=args.export,
    )
    print(json.dumps({
        "generated_at": result["generated_at"],
        "records": len(result["records"]),
        "question_count": result["question_count"],
        "start_date": result["start_date"],
        "end_date": result["end_date"],
        "dry_run": args.dry_run,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
