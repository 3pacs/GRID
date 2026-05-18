"""GRID Intelligence — Deep Cross-Reference Engine v2.

Goes deeper than v1: backtests signals, finds hidden chains,
tracks money flows through actor networks, identifies
asymmetric information edges.
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timedelta, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_engine
from loguru import logger as log
from sqlalchemy import text


CONNECTED_DOT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS connected_dot_cards (
    id BIGSERIAL PRIMARY KEY,
    dot_key TEXT NOT NULL UNIQUE,
    dot_type TEXT NOT NULL,
    ticker TEXT,
    direction TEXT NOT NULL DEFAULT 'watch',
    horizon TEXT NOT NULL DEFAULT 'watch',
    title TEXT NOT NULL,
    summary TEXT NOT NULL,
    catalyst TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    invalidation TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0.35,
    next_check_at TIMESTAMPTZ,
    state TEXT NOT NULL DEFAULT 'new',
    previous_state TEXT,
    state_signature TEXT NOT NULL,
    state_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    validation JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""


def _iso(value: datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


def _money(value: float | int | str | None) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def normalize_actor_name(value: str | None) -> str:
    """Collapse cosmetic actor-name variants before scoring records."""
    raw = re.sub(r"[^A-Za-z0-9 ]+", " ", str(value or "").lower())
    tokens = [
        token
        for token in raw.split()
        if token not in {"jr", "sr", "ii", "iii", "iv", "hon", "the"}
    ]
    return " ".join(tokens)


def _merge_unique_strings(*values: list[str] | tuple[str, ...] | set[str]) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for group in values:
        for value in group:
            text_value = str(value or "").strip()
            if not text_value or text_value in seen:
                continue
            seen.add(text_value)
            merged.append(text_value)
    return sorted(merged)


def dedupe_actor_track_records(records: list[dict]) -> list[dict]:
    """Collapse duplicate public-actor aliases without double-counting trades."""
    deduped: dict[str, dict] = {}
    for record in records:
        member = str(record.get("member") or "").strip()
        key = normalize_actor_name(member)
        if not key:
            continue
        current = deduped.get(key)
        candidate = dict(record)
        candidate["traded"] = _merge_unique_strings(candidate.get("traded") or [])
        candidate["aliases"] = list(candidate.get("aliases") or [])
        if current is None:
            deduped[key] = candidate
            continue

        current_trades = int(_money(current.get("trades")))
        candidate_trades = int(_money(candidate.get("trades")))
        winner, alias = (candidate, current) if candidate_trades > current_trades else (current, candidate)
        aliases = _merge_unique_strings(
            winner.get("aliases") or [],
            alias.get("aliases") or [],
            [str(alias.get("member") or "")],
        )
        winner = dict(winner)
        winner["aliases"] = [name for name in aliases if name != winner.get("member")]
        winner["traded"] = _merge_unique_strings(current.get("traded") or [], candidate.get("traded") or [])
        winner["tickers"] = max(int(_money(current.get("tickers"))), int(_money(candidate.get("tickers"))), len(winner["traded"]))
        deduped[key] = winner

    return sorted(
        deduped.values(),
        key=lambda item: (_money(item.get("avg_return_30d")), _money(item.get("win_rate")), _money(item.get("trades"))),
        reverse=True,
    )


def dedupe_money_trails(trails: list[dict]) -> list[dict]:
    """Collapse duplicate actor->entity money trails without inflating totals."""
    deduped: dict[tuple[str, str], dict] = {}
    for trail in trails:
        key = (str(trail.get("from") or ""), str(trail.get("to") or ""))
        current = deduped.get(key)
        candidate = dict(trail)
        implication = str(candidate.get("implication") or "").strip()
        candidate["implications"] = [implication] if implication else []
        candidate["implication_count"] = len(candidate["implications"])
        if current is None:
            deduped[key] = candidate
            continue
        if _money(candidate.get("total")) > _money(current.get("total")):
            winner, loser = candidate, current
        else:
            winner, loser = current, candidate
        winner = dict(winner)
        winner["implications"] = _merge_unique_strings(winner.get("implications") or [], loser.get("implications") or [])
        winner["implication_count"] = len(winner["implications"])
        winner["count"] = max(int(_money(winner.get("count"))), int(_money(loser.get("count"))))
        deduped[key] = winner

    return sorted(deduped.values(), key=lambda item: _money(item.get("total")), reverse=True)


def _direction_from_bias(value: str | None) -> str:
    raw = str(value or "").lower()
    if raw in {"bull", "bullish", "buy", "long"}:
        return "bullish"
    if raw in {"bear", "bearish", "sell", "short"}:
        return "bearish"
    return "watch"


def _validation_stub(dot_type: str, horizon_days: int) -> dict:
    return {
        "dot_type": dot_type,
        "samples": 0,
        "hit_rate": None,
        "avg_return_pct": None,
        "horizon_days": horizon_days,
        "status": "pending_forward_validation",
    }


def _validation_for(
    dot_type: str,
    horizon_days: int,
    validation_stats: dict[str, dict] | None = None,
    ticker: str | None = None,
) -> dict:
    stats: dict = {}
    matched_key = dot_type
    if validation_stats:
        keys = [dot_type]
        if ticker:
            keys.insert(0, f"{dot_type}:{str(ticker).upper()}")
        for key in keys:
            if validation_stats.get(key):
                stats = dict(validation_stats[key])
                matched_key = key
                break
    if not stats:
        return _validation_stub(dot_type, horizon_days)
    stats.setdefault("dot_type", dot_type)
    stats.setdefault("horizon_days", horizon_days)
    stats.setdefault("status", "historical_forward_validated")
    if ticker and matched_key != dot_type:
        stats.setdefault("ticker", str(ticker).upper())
        stats.setdefault("scope", "ticker")
    else:
        stats.setdefault("scope", "family")
    return stats


def _evidence(source: str, label: str, detail: str, generated_at: datetime, weight: float = 0.5) -> dict:
    return {
        "source": source,
        "label": label,
        "detail": detail,
        "timestamp": _iso(generated_at),
        "weight": round(_clamp(weight), 4),
    }


def _base_card(
    *,
    dot_key: str,
    dot_type: str,
    ticker: str,
    direction: str,
    horizon: str,
    title: str,
    summary: str,
    catalyst: str,
    evidence: list[dict],
    invalidation: str,
    confidence: float,
    generated_at: datetime,
    horizon_days: int,
    state_signature: str,
    source_payload: dict,
    quality: dict | None = None,
    validation: dict | None = None,
    validation_stats: dict[str, dict] | None = None,
) -> dict:
    return {
        "dot_key": dot_key,
        "dot_type": dot_type,
        "ticker": ticker.upper(),
        "direction": direction,
        "horizon": horizon,
        "title": title,
        "summary": summary,
        "catalyst": catalyst,
        "evidence": evidence,
        "invalidation": invalidation,
        "confidence": round(_clamp(confidence), 4),
        "next_check_at": generated_at + timedelta(days=max(1, horizon_days // 2)),
        "state_signature": state_signature,
        "validation": validation or _validation_for(dot_type, horizon_days, validation_stats, ticker=ticker),
        "quality": quality or {"warnings": []},
        "source_payload": source_payload,
        "updated_at": generated_at,
    }


def _event_chain_is_usable(chain: dict, generated_at: datetime) -> bool:
    date_b = str(chain.get("date_b") or "")
    try:
        event_dt = datetime.fromisoformat(date_b).replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    if generated_at - event_dt > timedelta(days=21):
        return False
    text_blob = f"{chain.get('headline_a') or ''} {chain.get('headline_b') or ''}".lower()
    noisy = (
        "main stock index" in text_blob
        or "digg cuts jobs" in text_blob
        or len(str(chain.get("headline_a") or "")) < 30
        or len(str(chain.get("headline_b") or "")) < 30
    )
    return not noisy


def filter_event_chains(chains: list[dict], generated_at: datetime) -> list[dict]:
    filtered: list[dict] = []
    seen: set[tuple[str, str, str, str]] = set()
    for chain in chains:
        if not _event_chain_is_usable(chain, generated_at):
            continue
        key = (
            str(chain.get("ticker_a") or ""),
            str(chain.get("ticker_b") or ""),
            str(chain.get("event_a") or ""),
            str(chain.get("headline_b") or "")[:80],
        )
        if key in seen:
            continue
        seen.add(key)
        filtered.append(chain)
    return filtered


def _resolved_profile_value(value: object) -> bool:
    text_value = str(value or "").strip()
    return bool(text_value and text_value.lower() not in {"?", "none", "null", "nan", "unknown"})


def filter_hidden_gems(gems: list[dict]) -> list[dict]:
    return [
        gem for gem in gems
        if _resolved_profile_value(gem.get("name")) and _resolved_profile_value(gem.get("sector"))
    ]


def _build_whale_reversal(row: dict, generated_at: datetime, validation_stats: dict[str, dict] | None = None) -> dict:
    ticker = str(row.get("ticker") or "").upper()
    direction = _direction_from_bias(row.get("to"))
    bull = _money(row.get("bull"))
    bear = _money(row.get("bear"))
    total = bull + bear
    dominant = max(bull, bear)
    confidence = 0.45 + (dominant / total * 0.35 if total else 0.0)
    return _base_card(
        dot_key=f"whale_reversal:{ticker}",
        dot_type="whale_reversal",
        ticker=ticker,
        direction=direction,
        horizon="swing",
        title=f"{ticker} whale flow flipped {direction}",
        summary=f"Whale/options flow flipped {row.get('from')} -> {row.get('to')} with ${bull/1e9:.2f}B bullish vs ${bear/1e9:.2f}B bearish.",
        catalyst=f"Whale/options flow flipped {row.get('from')} -> {row.get('to')} during week {row.get('week')}.",
        evidence=[
            _evidence(
                "whale_reversal",
                "Flow flip",
                f"Bullish premium ${bull:,.0f}; bearish premium ${bear:,.0f}.",
                generated_at,
                confidence,
            )
        ],
        invalidation=f"Invalidate if {ticker} flow flips back or fresh options premium stops confirming {direction}.",
        confidence=confidence,
        generated_at=generated_at,
        horizon_days=10,
        state_signature=f"{row.get('from')}->{row.get('to')}:{row.get('week')}",
        source_payload=row,
        quality={"source_count": 3, "warnings": []},
        validation_stats=validation_stats,
    )


def _build_smart_money_divergence(row: dict, generated_at: datetime, validation_stats: dict[str, dict] | None = None) -> dict:
    ticker = str(row.get("ticker") or "").upper()
    divergence = str(row.get("divergence") or "")
    direction = "bullish" if "WHALE BUY" in divergence else "bearish" if "WHALE SELL" in divergence else "watch"
    whale_net = _money(row.get("whale_net"))
    insider_net = _money(row.get("insider_net"))
    confidence = 0.50 + min(0.28, abs(whale_net) / 20_000_000_000)
    return _base_card(
        dot_key=f"smart_money_divergence:{ticker}",
        dot_type="smart_money_divergence",
        ticker=ticker,
        direction=direction,
        horizon="swing",
        title=f"{ticker} whale flow diverges from insiders",
        summary=f"{divergence}: whale net ${whale_net/1e9:.2f}B while insider net is ${insider_net:,.0f}.",
        catalyst=f"{ticker} has a live smart-money divergence: {divergence}.",
        evidence=[
            _evidence("smart_money_divergence", "Whale net", f"Whale/options net ${whale_net:,.0f}.", generated_at, 0.7),
            _evidence("smart_money_divergence", "Insider net", f"Insider net ${insider_net:,.0f}.", generated_at, 0.45),
        ],
        invalidation="Invalidate if insider selling accelerates into weakness or whale flow reverses before price confirms.",
        confidence=confidence,
        generated_at=generated_at,
        horizon_days=10,
        state_signature=f"{divergence}:{round(whale_net, -6)}:{round(insider_net, -5)}",
        source_payload=row,
        quality={"source_count": 2, "warnings": ["conflicting_actor_classes"]},
        validation_stats=validation_stats,
    )


def _build_hidden_gem(row: dict, generated_at: datetime, validation_stats: dict[str, dict] | None = None) -> dict:
    ticker = str(row.get("ticker") or "").upper()
    bias = str(row.get("bias") or "MIXED")
    direction = _direction_from_bias(bias)
    signals = int(_money(row.get("signals")))
    source_count = int(_money(row.get("types")))
    missing_profile = not row.get("name")
    confidence = 0.32 + min(0.30, source_count * 0.04) + min(0.16, signals / 1000)
    if missing_profile:
        confidence -= 0.08
    return _base_card(
        dot_key=f"hidden_gem:{ticker}",
        dot_type="hidden_gem",
        ticker=ticker,
        direction=direction,
        horizon="swing",
        title=f"{ticker} {bias.lower()} multi-source cluster",
        summary=f"{signals} signals across {source_count} source types; bull={row.get('bull')}, bear={row.get('bear')}.",
        catalyst=f"{ticker} crossed the hidden-gem density threshold with {source_count} independent source types.",
        evidence=[
            _evidence("hidden_gem", "Signal density", f"{signals} total signals across {source_count} types.", generated_at, 0.65),
            _evidence("hidden_gem", "Bias", f"Bias is {bias}: bull={row.get('bull')}, bear={row.get('bear')}.", generated_at, 0.55),
        ],
        invalidation="Invalidate if source diversity collapses below three types or bias becomes mixed.",
        confidence=confidence,
        generated_at=generated_at,
        horizon_days=7,
        state_signature=f"{bias}:{source_count}:{signals // 10}",
        source_payload=row,
        quality={
            "source_count": source_count,
            "warnings": ["missing_company_profile"] if missing_profile else [],
        },
        validation_stats=validation_stats,
    )


def _build_money_trail(row: dict, generated_at: datetime, validation_stats: dict[str, dict] | None = None) -> dict | None:
    target = str(row.get("to") or "")
    if not target.startswith("corp_"):
        return None
    ticker = target.replace("corp_", "", 1).upper()
    total = _money(row.get("total"))
    implication = str(row.get("implication") or "capital flow")
    return _base_card(
        dot_key=f"money_trail:{ticker}",
        dot_type="money_trail",
        ticker=ticker,
        direction="bullish",
        horizon="multi_month",
        title=f"{ticker} capital trail detected",
        summary=f"{implication}; aggregate observed flow ${total/1e9:.2f}B.",
        catalyst=f"Large actor-money trail into {ticker}: {implication}.",
        evidence=[
            _evidence("money_trail", "Capital trail", f"{row.get('from')} -> {row.get('to')} total ${total:,.0f}.", generated_at, 0.6)
        ],
        invalidation="Invalidate if the contract/flow is non-recurring, already priced, or fails to map to revenue sensitivity.",
        confidence=0.52 + min(0.22, total / 25_000_000_000),
        generated_at=generated_at,
        horizon_days=45,
        state_signature=f"{round(total, -6)}:{implication[:80]}",
        source_payload=row,
        quality={"source_count": 1, "warnings": ["single_source_contract_flow"]},
        validation_stats=validation_stats,
    )


def _build_insider_backtest(row: dict, generated_at: datetime, validation_stats: dict[str, dict] | None = None) -> dict | None:
    ticker = str(row.get("ticker") or "").upper()
    action = str(row.get("direction") or "").lower()
    pct_move = _money(row.get("pct_move"))
    expected_direction = "bullish" if action in {"buy", "purchase"} else "bearish" if action in {"sell", "sale"} else "watch"
    aligned = (expected_direction == "bullish" and pct_move > 0) or (expected_direction == "bearish" and pct_move < 0)
    if not aligned:
        return None
    value = _money(row.get("value"))
    fallback_validation = {
        "dot_type": "insider_cluster",
        "samples": 1,
        "hit_rate": 1.0,
        "avg_return_pct": pct_move,
        "horizon_days": 14,
        "status": "single_cluster_forward_checked",
    }
    return _base_card(
        dot_key=f"insider_cluster:{ticker}",
        dot_type="insider_cluster",
        ticker=ticker,
        direction=expected_direction,
        horizon="swing",
        title=f"{ticker} insider cluster validated {expected_direction}",
        summary=f"{row.get('insiders')} insiders {action} ${value:,.0f}; 2-week move was {pct_move:+.2f}%.",
        catalyst=f"Recent insider cluster aligned with a {pct_move:+.2f}% 2-week forward move.",
        evidence=[
            _evidence("insider_cluster", "Cluster", f"{row.get('insiders')} insiders {action} ${value:,.0f}.", generated_at, 0.6),
            _evidence("insider_cluster", "Forward move", f"Price moved {pct_move:+.2f}% over two weeks.", generated_at, 0.65),
        ],
        invalidation="Invalidate if the next insider cluster stops matching forward price action.",
        confidence=0.48 + min(0.24, abs(pct_move) / 25),
        generated_at=generated_at,
        horizon_days=14,
        state_signature=f"{action}:{round(value, -5)}:{round(pct_move, 1)}",
        source_payload=row,
        validation=_validation_for("insider_cluster", 14, validation_stats, ticker=ticker) if validation_stats else fallback_validation,
        quality={"source_count": 2, "warnings": ["single_event_validation"]},
    )


def build_connected_dot_cards(
    results: dict,
    generated_at: datetime | None = None,
    validation_stats: dict[str, dict] | None = None,
) -> list[dict]:
    generated_at = generated_at or datetime.now(timezone.utc)
    cards: list[dict] = []

    for row in results.get("whale_reversals") or []:
        cards.append(_build_whale_reversal(row, generated_at, validation_stats))
    for row in results.get("smart_money_divergences") or []:
        cards.append(_build_smart_money_divergence(row, generated_at, validation_stats))
    for row in results.get("hidden_gems") or []:
        cards.append(_build_hidden_gem(row, generated_at, validation_stats))
    for row in results.get("money_trails") or []:
        card = _build_money_trail(row, generated_at, validation_stats)
        if card:
            cards.append(card)
    for row in results.get("insider_backtest") or []:
        card = _build_insider_backtest(row, generated_at, validation_stats)
        if card:
            cards.append(card)

    # Event chains are intentionally not surfaced until they clear freshness and
    # headline-quality checks; the raw JSON remains available for diagnostics.
    for row in filter_event_chains(results.get("event_chains") or [], generated_at):
        ticker = str(row.get("ticker_b") or row.get("ticker_a") or "").upper()
        if not ticker:
            continue
        cards.append(_base_card(
            dot_key=f"event_chain:{ticker}:{row.get('event_a')}:{row.get('event_b')}",
            dot_type="event_chain",
            ticker=ticker,
            direction="watch",
            horizon="swing",
            title=f"{ticker} event chain",
            summary=f"{row.get('event_a')} -> {row.get('event_b')} in {row.get('days')} day(s).",
            catalyst=f"{row.get('headline_a')} -> {row.get('headline_b')}",
            evidence=[
                _evidence("event_chain", "Event A", str(row.get("headline_a")), generated_at, 0.45),
                _evidence("event_chain", "Event B", str(row.get("headline_b")), generated_at, 0.45),
            ],
            invalidation="Invalidate if the chain is duplicate, stale, or lacks causal linkage to price.",
            confidence=0.38,
            generated_at=generated_at,
            horizon_days=7,
            state_signature=f"{row.get('date_a')}:{row.get('date_b')}:{row.get('headline_b')}",
            source_payload=row,
            quality={"source_count": 1, "warnings": ["needs_causal_review"]},
            validation_stats=validation_stats,
        ))

    deduped: dict[str, dict] = {}
    for card in cards:
        existing = deduped.get(card["dot_key"])
        if existing is None or card["confidence"] > existing["confidence"]:
            deduped[card["dot_key"]] = card
    return sorted(deduped.values(), key=lambda item: (item["confidence"], item["dot_key"]), reverse=True)


def _table_exists(conn, table_name: str) -> bool:
    return bool(conn.execute(text("SELECT to_regclass(:name)"), {"name": table_name}).scalar())


def _validation_summary(
    rows: list,
    dot_type: str,
    horizon_days: int,
    generated_at: datetime,
    lookback_days: int,
    *,
    scope: str = "family",
    ticker: str | None = None,
) -> dict:
    returns: list[float] = []
    hits = 0
    for row in rows:
        data = row._mapping if hasattr(row, "_mapping") else row
        signed_return = data.get("signed_return_pct")
        if signed_return is None:
            continue
        signed_return_float = _money(signed_return)
        returns.append(signed_return_float)
        if signed_return_float > 0:
            hits += 1
    if not returns:
        summary = _validation_stub(dot_type, horizon_days)
        summary["scope"] = scope
        if ticker:
            summary["ticker"] = ticker.upper()
        return summary
    summary = {
        "dot_type": dot_type,
        "samples": len(returns),
        "hit_rate": round(hits / len(returns), 4),
        "avg_return_pct": round(sum(returns) / len(returns), 2),
        "horizon_days": horizon_days,
        "lookback_days": lookback_days,
        "status": "historical_forward_validated",
        "as_of": _iso(generated_at),
        "scope": scope,
    }
    if ticker:
        summary["ticker"] = ticker.upper()
    return summary


def _validation_summaries(
    rows: list,
    dot_type: str,
    horizon_days: int,
    generated_at: datetime,
    lookback_days: int,
) -> dict[str, dict]:
    summaries = {
        dot_type: _validation_summary(rows, dot_type, horizon_days, generated_at, lookback_days)
    }
    by_ticker: dict[str, list] = defaultdict(list)
    for row in rows:
        data = row._mapping if hasattr(row, "_mapping") else row
        ticker = str(data.get("ticker") or "").upper()
        if ticker:
            by_ticker[ticker].append(row)
    for ticker, ticker_rows in by_ticker.items():
        summaries[f"{dot_type}:{ticker}"] = _validation_summary(
            ticker_rows,
            dot_type,
            horizon_days,
            generated_at,
            lookback_days,
            scope="ticker",
            ticker=ticker,
        )
    return summaries


def _validation_health(validation: dict | str | None) -> str:
    if isinstance(validation, str):
        try:
            validation = json.loads(validation)
        except json.JSONDecodeError:
            validation = {}
    validation = validation or {}
    samples = int(_money(validation.get("samples")))
    hit_rate = validation.get("hit_rate")
    avg_return = validation.get("avg_return_pct")
    if samples < 5 or hit_rate is None:
        return "pending"
    hit_rate_float = _money(hit_rate)
    avg_return_float = _money(avg_return)
    if hit_rate_float >= 0.58 and (avg_return is None or avg_return_float > 0):
        return "validated"
    if hit_rate_float <= 0.50 or (avg_return is not None and avg_return_float <= 0):
        return "invalidated"
    return "weak"


def prepare_operator_results(results: dict, validation_stats: dict[str, dict] | None = None) -> dict:
    prepared = {
        key: list(value) if isinstance(value, list) else value
        for key, value in results.items()
    }
    diagnostics = dict(prepared.get("diagnostics") or {})
    validation_stats = validation_stats or {}
    invalidated_families = {
        "hidden_gems": "hidden_gem",
        "money_trails": "money_trail",
        "insider_backtest": "insider_cluster",
    }
    for family, validation_key in invalidated_families.items():
        rows = list(prepared.get(family) or [])
        if rows and _validation_health(validation_stats.get(validation_key)) == "invalidated":
            diagnostics[family] = rows
            prepared[family] = []

    contagion = list(prepared.get("signal_contagion") or [])
    if contagion:
        diagnostics["signal_contagion"] = contagion
        prepared["signal_contagion"] = []

    if diagnostics:
        prepared["diagnostics"] = diagnostics
    return prepared


def _validation_signature(validation: dict | str | None) -> str:
    if isinstance(validation, str):
        try:
            validation = json.loads(validation)
        except json.JSONDecodeError:
            validation = {}
    validation = validation or {}
    return ":".join(
        str(validation.get(key))
        for key in ("status", "samples", "hit_rate", "avg_return_pct", "horizon_days")
    )


def _run_validation_query(
    conn,
    *,
    dot_type: str,
    horizon_days: int,
    lookback_days: int,
    generated_at: datetime,
    sql: str,
    limit: int = 250,
) -> dict[str, dict]:
    try:
        rows = conn.execute(
            text(sql),
            {"horizon_days": horizon_days, "lookback_days": lookback_days, "limit": limit},
        ).fetchall()
        return _validation_summaries(rows, dot_type, horizon_days, generated_at, lookback_days)
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log.warning("Forward validation failed for {dot}: {e}", dot=dot_type, e=str(exc))
        return {dot_type: _validation_stub(dot_type, horizon_days)}


def compute_dot_type_forward_validation(conn, generated_at: datetime | None = None) -> dict[str, dict]:
    generated_at = generated_at or datetime.now(timezone.utc)
    if not _table_exists(conn, "raw_series") or not _table_exists(conn, "signal_data"):
        return {}

    stats: dict[str, dict] = {}
    validation_sql = {
        "whale_reversal": {
            "horizon_days": 10,
            "lookback_days": 240,
            "limit": 200,
            "sql": """
                WITH weekly_flows AS (
                    SELECT ticker,
                           DATE_TRUNC('week', signal_date)::date AS event_date,
                           SUM(CASE WHEN direction IN ('buy','bullish','call') THEN magnitude ELSE 0 END) AS bull,
                           SUM(CASE WHEN direction IN ('sell','bearish','put') THEN magnitude ELSE 0 END) AS bear
                    FROM signal_data
                    WHERE signal_type IN ('whale_flow', 'whale_options', 'unusual_options', 'options_flow')
                      AND signal_date >= CURRENT_DATE - CAST(:lookback_days AS integer)
                      AND signal_date < CURRENT_DATE - CAST(:horizon_days AS integer)
                      AND ticker IS NOT NULL
                    GROUP BY ticker, DATE_TRUNC('week', signal_date)
                ),
                with_bias AS (
                    SELECT *,
                           CASE WHEN bull > bear * 2 THEN 'BULL'
                                WHEN bear > bull * 2 THEN 'BEAR'
                                ELSE 'NEUTRAL' END AS bias,
                           LAG(CASE WHEN bull > bear * 2 THEN 'BULL'
                                    WHEN bear > bull * 2 THEN 'BEAR'
                                    ELSE 'NEUTRAL' END)
                               OVER (PARTITION BY ticker ORDER BY event_date) AS prev_bias
                    FROM weekly_flows
                    WHERE bull + bear > 1000000
                ),
                events AS (
                    SELECT ticker, event_date, bias
                    FROM with_bias
                    WHERE bias != prev_bias
                      AND prev_bias IS NOT NULL
                      AND bias != 'NEUTRAL'
                      AND prev_bias != 'NEUTRAL'
                    ORDER BY event_date DESC
                    LIMIT :limit
                )
                SELECT e.ticker AS ticker,
                       CASE WHEN e.bias = 'BULL'
                            THEN (p2.price - p1.price) / NULLIF(p1.price, 0) * 100
                            ELSE (p1.price - p2.price) / NULLIF(p1.price, 0) * 100
                       END AS signed_return_pct
                FROM events e
                JOIN LATERAL (
                    SELECT value AS price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), e.ticker, chr(58), 'close')
                      AND obs_date >= e.event_date
                      AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p1 ON true
                JOIN LATERAL (
                    SELECT value AS price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), e.ticker, chr(58), 'close')
                      AND obs_date >= e.event_date + CAST(:horizon_days AS integer)
                      AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p2 ON true
            """,
        },
        "smart_money_divergence": {
            "horizon_days": 10,
            "lookback_days": 210,
            "limit": 250,
            "sql": """
                WITH weekly AS (
                    SELECT ticker,
                           DATE_TRUNC('week', signal_date)::date AS event_date,
                           SUM(CASE WHEN signal_type IN ('insider', 'quiverquant:insider')
                                     AND direction IN ('buy', 'purchase') THEN magnitude
                                    WHEN signal_type IN ('insider', 'quiverquant:insider')
                                     AND direction IN ('sell', 'sale') THEN -magnitude
                                    ELSE 0 END) AS insider_net,
                           SUM(CASE WHEN signal_type IN ('whale_flow', 'whale_options', 'unusual_options', 'options_flow')
                                     AND direction IN ('buy','bullish','call') THEN magnitude
                                    WHEN signal_type IN ('whale_flow', 'whale_options', 'unusual_options', 'options_flow')
                                     AND direction IN ('sell','bearish','put') THEN -magnitude
                                    ELSE 0 END) AS whale_net,
                           COUNT(*) AS total_signals
                    FROM signal_data
                    WHERE signal_date >= CURRENT_DATE - CAST(:lookback_days AS integer)
                      AND signal_date < CURRENT_DATE - CAST(:horizon_days AS integer)
                      AND ticker IS NOT NULL
                    GROUP BY ticker, DATE_TRUNC('week', signal_date)
                    HAVING COUNT(*) >= 10
                ),
                events AS (
                    SELECT ticker, event_date,
                           CASE WHEN insider_net < 0 AND whale_net > 0 THEN 'BULL'
                                WHEN insider_net > 0 AND whale_net < 0 THEN 'BEAR'
                                ELSE NULL END AS bias
                    FROM weekly
                    WHERE (insider_net < 0 AND whale_net > 0)
                       OR (insider_net > 0 AND whale_net < 0)
                    ORDER BY ABS(whale_net) DESC
                    LIMIT :limit
                )
                SELECT e.ticker AS ticker,
                       CASE WHEN e.bias = 'BULL'
                            THEN (p2.price - p1.price) / NULLIF(p1.price, 0) * 100
                            ELSE (p1.price - p2.price) / NULLIF(p1.price, 0) * 100
                       END AS signed_return_pct
                FROM events e
                JOIN LATERAL (
                    SELECT value AS price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), e.ticker, chr(58), 'close')
                      AND obs_date >= e.event_date
                      AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p1 ON true
                JOIN LATERAL (
                    SELECT value AS price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), e.ticker, chr(58), 'close')
                      AND obs_date >= e.event_date + CAST(:horizon_days AS integer)
                      AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p2 ON true
            """,
        },
        "hidden_gem": {
            "horizon_days": 7,
            "lookback_days": 180,
            "limit": 250,
            "sql": """
                WITH density AS (
                    SELECT ticker,
                           DATE_TRUNC('week', signal_date)::date AS event_date,
                           COUNT(*) AS signals,
                           COUNT(DISTINCT signal_type) AS types,
                           SUM(CASE WHEN direction IN ('buy','bullish','call') THEN 1 ELSE 0 END) AS bull,
                           SUM(CASE WHEN direction IN ('sell','bearish','put') THEN 1 ELSE 0 END) AS bear
                    FROM signal_data
                    WHERE signal_date >= CURRENT_DATE - CAST(:lookback_days AS integer)
                      AND signal_date < CURRENT_DATE - CAST(:horizon_days AS integer)
                      AND ticker IS NOT NULL
                      AND ticker != ''
                    GROUP BY ticker, DATE_TRUNC('week', signal_date)
                ),
                events AS (
                    SELECT ticker, event_date,
                           CASE WHEN bull > bear * 2 THEN 'BULL'
                                WHEN bear > bull * 2 THEN 'BEAR'
                                ELSE NULL END AS bias
                    FROM density
                    WHERE signals BETWEEN 3 AND 500
                      AND types >= 2
                      AND (bull > bear * 2 OR bear > bull * 2)
                    ORDER BY types DESC, signals DESC
                    LIMIT :limit
                )
                SELECT e.ticker AS ticker,
                       CASE WHEN e.bias = 'BULL'
                            THEN (p2.price - p1.price) / NULLIF(p1.price, 0) * 100
                            ELSE (p1.price - p2.price) / NULLIF(p1.price, 0) * 100
                       END AS signed_return_pct
                FROM events e
                JOIN LATERAL (
                    SELECT value AS price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), e.ticker, chr(58), 'close')
                      AND obs_date >= e.event_date
                      AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p1 ON true
                JOIN LATERAL (
                    SELECT value AS price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), e.ticker, chr(58), 'close')
                      AND obs_date >= e.event_date + CAST(:horizon_days AS integer)
                      AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p2 ON true
            """,
        },
        "insider_cluster": {
            "horizon_days": 14,
            "lookback_days": 180,
            "limit": 250,
            "sql": """
                WITH clusters AS (
                    SELECT ticker,
                           DATE_TRUNC('week', signal_date)::date AS event_date,
                           mode() WITHIN GROUP (ORDER BY direction) AS direction,
                           COUNT(DISTINCT actor) AS insiders,
                           SUM(magnitude) AS total_value
                    FROM signal_data
                    WHERE signal_type IN ('insider', 'quiverquant:insider')
                      AND signal_date >= CURRENT_DATE - CAST(:lookback_days AS integer)
                      AND signal_date < CURRENT_DATE - CAST(:horizon_days AS integer)
                      AND ticker IS NOT NULL
                      AND magnitude > 10000
                    GROUP BY ticker, DATE_TRUNC('week', signal_date)
                    HAVING COUNT(DISTINCT actor) >= 2
                    ORDER BY SUM(magnitude) DESC
                    LIMIT :limit
                )
                SELECT c.ticker AS ticker,
                       CASE WHEN c.direction IN ('buy', 'purchase')
                            THEN (p2.price - p1.price) / NULLIF(p1.price, 0) * 100
                            ELSE (p1.price - p2.price) / NULLIF(p1.price, 0) * 100
                       END AS signed_return_pct
                FROM clusters c
                JOIN LATERAL (
                    SELECT value AS price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), c.ticker, chr(58), 'close')
                      AND obs_date >= c.event_date
                      AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p1 ON true
                JOIN LATERAL (
                    SELECT value AS price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), c.ticker, chr(58), 'close')
                      AND obs_date >= c.event_date + CAST(:horizon_days AS integer)
                      AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p2 ON true
            """,
        },
    }

    for dot_type, spec in validation_sql.items():
        stats.update(
            _run_validation_query(
                conn,
                dot_type=dot_type,
                horizon_days=spec["horizon_days"],
                lookback_days=spec["lookback_days"],
                generated_at=generated_at,
                sql=spec["sql"],
                limit=spec["limit"],
            )
        )

    if _table_exists(conn, "wealth_flows"):
        stats.update(
            _run_validation_query(
                conn,
                dot_type="money_trail",
                horizon_days=45,
                lookback_days=365,
                generated_at=generated_at,
                limit=200,
                sql="""
                WITH events AS (
                    SELECT replace(to_entity, 'corp_', '') AS ticker,
                           flow_date::date AS event_date,
                           SUM(amount_estimate) AS total_flow
                    FROM wealth_flows
                    WHERE amount_estimate > 100000
                      AND flow_date >= CURRENT_DATE - CAST(:lookback_days AS integer)
                      AND flow_date < CURRENT_DATE - CAST(:horizon_days AS integer)
                      AND to_entity LIKE 'corp_%'
                    GROUP BY replace(to_entity, 'corp_', ''), flow_date::date
                    HAVING SUM(amount_estimate) > 1000000
                    ORDER BY SUM(amount_estimate) DESC
                    LIMIT :limit
                )
                SELECT e.ticker AS ticker,
                       (p2.price - p1.price) / NULLIF(p1.price, 0) * 100 AS signed_return_pct
                FROM events e
                JOIN LATERAL (
                    SELECT value AS price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), e.ticker, chr(58), 'close')
                      AND obs_date >= e.event_date
                      AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p1 ON true
                JOIN LATERAL (
                    SELECT value AS price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), e.ticker, chr(58), 'close')
                      AND obs_date >= e.event_date + CAST(:horizon_days AS integer)
                      AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p2 ON true
            """,
            )
        )

    return stats


def merge_card_state(existing: dict | None, card: dict, now: datetime) -> dict:
    merged = dict(card)
    merged["state_changed_at"] = now
    validation_health = _validation_health(card.get("validation"))
    if not existing:
        merged["state"] = "invalidated" if validation_health == "invalidated" else "new"
        merged["previous_state"] = None
        return merged

    previous_state = existing.get("state") or "active"
    validation_changed = _validation_signature(existing.get("validation")) != _validation_signature(card.get("validation"))

    if validation_health == "invalidated":
        merged["state"] = "invalidated"
        merged["previous_state"] = previous_state if previous_state != "invalidated" else existing.get("previous_state")
        if previous_state == "invalidated" and not validation_changed:
            merged["state_changed_at"] = existing.get("state_changed_at") or now
        return merged

    if validation_health == "validated":
        merged["state"] = "validated"
        merged["previous_state"] = previous_state if previous_state != "validated" else existing.get("previous_state")
        if previous_state == "validated" and not validation_changed:
            merged["state_changed_at"] = existing.get("state_changed_at") or now
        return merged

    if existing.get("state_signature") != card.get("state_signature"):
        merged["state"] = "changed"
        merged["previous_state"] = previous_state
        return merged
    merged["state"] = "active"
    merged["previous_state"] = existing.get("previous_state")
    merged["state_changed_at"] = existing.get("state_changed_at") or now
    return merged


def ensure_connected_dot_cards_table(conn) -> None:
    conn.execute(text(CONNECTED_DOT_TABLE_SQL))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_connected_dot_cards_type ON connected_dot_cards(dot_type)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_connected_dot_cards_ticker ON connected_dot_cards(ticker)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_connected_dot_cards_updated ON connected_dot_cards(updated_at DESC)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_connected_dot_cards_state ON connected_dot_cards(state, state_changed_at DESC)"))


def persist_connected_dot_cards(
    engine,
    results: dict,
    generated_at: datetime | None = None,
    validation_stats: dict[str, dict] | None = None,
) -> int:
    generated_at = generated_at or datetime.now(timezone.utc)
    if validation_stats is None:
        validation_stats = {}
        try:
            with engine.connect() as conn:
                conn.execute(text("SET statement_timeout = '20s'"))
                validation_stats = compute_dot_type_forward_validation(conn, generated_at=generated_at)
        except Exception as exc:
            log.warning("connected-dot forward validation unavailable: {e}", e=str(exc))
    cards = build_connected_dot_cards(results, generated_at=generated_at, validation_stats=validation_stats)
    if not cards:
        return 0
    with engine.begin() as conn:
        ensure_connected_dot_cards_table(conn)
        existing_rows = conn.execute(
            text(
                """
                SELECT dot_key, state, previous_state, state_signature, state_changed_at
                     , validation
                FROM connected_dot_cards
                WHERE dot_key = ANY(:keys)
                """
            ),
            {"keys": [card["dot_key"] for card in cards]},
        ).fetchall()
        existing = {row._mapping["dot_key"]: dict(row._mapping) for row in existing_rows}
        for raw_card in cards:
            card = merge_card_state(existing.get(raw_card["dot_key"]), raw_card, generated_at)
            conn.execute(
                text(
                    """
                    INSERT INTO connected_dot_cards (
                        dot_key, dot_type, ticker, direction, horizon, title, summary,
                        catalyst, evidence, invalidation, confidence, next_check_at,
                        state, previous_state, state_signature, state_changed_at,
                        validation, quality, source_payload, updated_at
                    )
                    VALUES (
                        :dot_key, :dot_type, :ticker, :direction, :horizon, :title, :summary,
                        :catalyst, CAST(:evidence AS jsonb), :invalidation, :confidence, :next_check_at,
                        :state, :previous_state, :state_signature, :state_changed_at,
                        CAST(:validation AS jsonb), CAST(:quality AS jsonb), CAST(:source_payload AS jsonb), :updated_at
                    )
                    ON CONFLICT (dot_key) DO UPDATE SET
                        dot_type = EXCLUDED.dot_type,
                        ticker = EXCLUDED.ticker,
                        direction = EXCLUDED.direction,
                        horizon = EXCLUDED.horizon,
                        title = EXCLUDED.title,
                        summary = EXCLUDED.summary,
                        catalyst = EXCLUDED.catalyst,
                        evidence = EXCLUDED.evidence,
                        invalidation = EXCLUDED.invalidation,
                        confidence = EXCLUDED.confidence,
                        next_check_at = EXCLUDED.next_check_at,
                        state = EXCLUDED.state,
                        previous_state = EXCLUDED.previous_state,
                        state_signature = EXCLUDED.state_signature,
                        state_changed_at = EXCLUDED.state_changed_at,
                        validation = EXCLUDED.validation,
                        quality = EXCLUDED.quality,
                        source_payload = EXCLUDED.source_payload,
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                {
                    **card,
                    "evidence": json.dumps(card["evidence"], default=str),
                    "validation": json.dumps(card["validation"], default=str),
                    "quality": json.dumps(card["quality"], default=str),
                    "source_payload": json.dumps(card["source_payload"], default=str),
                },
            )
    return len(cards)


def main() -> dict:
    engine = get_engine()
    results = {}
    generated_at = datetime.now(timezone.utc)

    def set_timeout(conn, seconds: int = 20) -> None:
        conn.execute(text(f"SET statement_timeout = '{seconds}s'"))

    def to_float(value, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    # 1. INSIDER CLUSTER -> PRICE MOVE backtesting
    # Did coordinated insider selling actually predict drops?
    log.info("DOT 1: Insider cluster -> price move backtest...")
    try:
        with engine.connect() as conn:
            set_timeout(conn)
            rows = conn.execute(text("""
                WITH clusters AS MATERIALIZED (
                    SELECT ticker,
                           DATE_TRUNC('week', signal_date)::date as week,
                           COUNT(DISTINCT actor) as insiders,
                           mode() WITHIN GROUP (ORDER BY direction) as dominant_dir,
                           SUM(magnitude) as total_value
                    FROM signal_data
                    WHERE signal_type IN ('insider', 'quiverquant:insider')
                    AND signal_date >= CURRENT_DATE - 60
                    AND ticker IS NOT NULL AND magnitude > 10000
                    GROUP BY ticker, DATE_TRUNC('week', signal_date)
                    HAVING COUNT(DISTINCT actor) >= 2
                    ORDER BY SUM(magnitude) DESC
                    LIMIT 80
                )
                SELECT c.ticker, c.week, c.insiders, c.dominant_dir, c.total_value,
                       p1.price as price_at_cluster,
                       p2.price as price_2w_later,
                       CASE WHEN p1.price > 0 THEN
                           ROUND(((p2.price - p1.price) / p1.price * 100)::numeric, 2)
                       END as pct_move_2w
                FROM clusters c
                LEFT JOIN LATERAL (
                    SELECT value as price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), c.ticker, chr(58), 'close')
                    AND obs_date >= c.week
                    AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p1 ON true
                LEFT JOIN LATERAL (
                    SELECT value as price
                    FROM raw_series
                    WHERE series_id = concat('YF', chr(58), c.ticker, chr(58), 'close')
                    AND obs_date >= c.week + 14
                    AND value > 0
                    ORDER BY obs_date ASC LIMIT 1
                ) p2 ON true
                WHERE p1.price IS NOT NULL AND p2.price IS NOT NULL
                ORDER BY c.total_value DESC
                LIMIT 20
            """)).fetchall()
            backtests = []
            for r in rows:
                b = {
                    "ticker": r[0], "week": str(r[1]), "insiders": r[2],
                    "direction": r[3], "value": float(r[4] or 0),
                    "price_at": float(r[5] or 0), "price_2w": float(r[6] or 0),
                    "pct_move": float(r[7]) if r[7] is not None else None,
                }
                backtests.append(b)
                arrow = "v" if (b["pct_move"] or 0) < 0 else "^"
                log.info("  {t}: {n} insiders {d} ${v:,.0f} -> {pct}% {a} (${p1:.0f} -> ${p2:.0f})",
                         t=b["ticker"], n=b["insiders"], d=b["direction"],
                         v=b["value"], pct=b["pct_move"] or 0, a=arrow,
                         p1=b["price_at"], p2=b["price_2w"])
            results["insider_backtest"] = backtests
    except Exception as e:
        log.warning("Insider backtest: {e}", e=str(e))

    # 2. CONGRESS MEMBER TRACK RECORDS — who has the best returns?
    log.info("DOT 2: Congress member track records...")
    try:
        with engine.connect() as conn:
            set_timeout(conn)
            rows = conn.execute(text("""
                WITH congress_trades AS (
                    SELECT actor, ticker, direction, signal_date, magnitude
                    FROM signal_data
                    WHERE signal_type IN ('congressional', 'quiverquant:house', 'quiverquant:senate')
                    AND signal_date >= CURRENT_DATE - 90
                    AND ticker IS NOT NULL
                    AND actor NOT IN ('qq_house_trading', 'qq_senate_trading')
                    AND actor IS NOT NULL AND actor != ''
                ),
                with_prices AS (
                    SELECT ct.*,
                           p1.price as entry_price,
                           p2.price as price_30d
                    FROM congress_trades ct
                    LEFT JOIN LATERAL (
                        SELECT value as price
                        FROM raw_series
                        WHERE series_id = concat('YF', chr(58), ct.ticker, chr(58), 'close')
                        AND obs_date >= ct.signal_date
                        AND value > 0
                        ORDER BY obs_date ASC LIMIT 1
                    ) p1 ON true
                    LEFT JOIN LATERAL (
                        SELECT value as price
                        FROM raw_series
                        WHERE series_id = concat('YF', chr(58), ct.ticker, chr(58), 'close')
                        AND obs_date >= ct.signal_date + 30
                        AND value > 0
                        ORDER BY obs_date ASC LIMIT 1
                    ) p2 ON true
                    WHERE p1.price IS NOT NULL AND p2.price IS NOT NULL
                )
                SELECT actor,
                       COUNT(*) as trades,
                       COUNT(DISTINCT ticker) as tickers,
                       ROUND(AVG(CASE WHEN direction IN ('buy', 'purchase')
                           THEN (price_30d - entry_price) / NULLIF(entry_price, 0) * 100
                           ELSE (entry_price - price_30d) / NULLIF(entry_price, 0) * 100
                       END)::numeric, 2) as avg_return_30d,
                       SUM(CASE WHEN
                           (direction IN ('buy', 'purchase') AND price_30d > entry_price) OR
                           (direction IN ('sell', 'sale') AND price_30d < entry_price)
                       THEN 1 ELSE 0 END) as winners,
                       array_agg(DISTINCT ticker ORDER BY ticker) as traded_tickers
                FROM with_prices
                GROUP BY actor
                HAVING COUNT(*) >= 3
                ORDER BY AVG(CASE WHEN direction IN ('buy', 'purchase')
                    THEN (price_30d - entry_price) / NULLIF(entry_price, 0) * 100
                    ELSE (entry_price - price_30d) / NULLIF(entry_price, 0) * 100
                END) DESC NULLS LAST
                LIMIT 15
            """)).fetchall()
            congress = []
            for r in rows:
                win_rate = (r[4] / r[1] * 100) if r[1] > 0 else 0
                c = {
                    "member": r[0], "trades": r[1], "tickers": r[2],
                    "avg_return_30d": float(r[3]) if r[3] is not None else None,
                    "winners": r[4], "win_rate": round(win_rate, 1),
                    "traded": list(r[5])[:8] if r[5] else [],
                }
                congress.append(c)
            congress = dedupe_actor_track_records(congress)
            for c in congress:
                log.info("  {m}: {t} trades, {wr:.0f}% win rate, avg return {r}%, tickers={tk}",
                         m=c["member"][:25], t=c["trades"], wr=c["win_rate"],
                         r=c["avg_return_30d"] or 0, tk=c["traded"][:5])
            results["congress_track_records"] = congress
    except Exception as e:
        log.warning("Congress records: {e}", e=str(e))

    # 3. WHALE FLOW REVERSALS — when did smart money flip?
    log.info("DOT 3: Whale flow reversals...")
    try:
        with engine.connect() as conn:
            set_timeout(conn)
            rows = conn.execute(text("""
                WITH weekly_flows AS (
                    SELECT ticker,
                           DATE_TRUNC('week', signal_date)::date as week,
                           SUM(CASE WHEN direction IN ('buy','bullish','call') THEN magnitude ELSE 0 END) as bull,
                           SUM(CASE WHEN direction IN ('sell','bearish','put') THEN magnitude ELSE 0 END) as bear
                    FROM signal_data
                    WHERE signal_type IN ('whale_flow', 'whale_options', 'unusual_options', 'options_flow')
                    AND signal_date >= CURRENT_DATE - 30
                    AND ticker IS NOT NULL
                    GROUP BY ticker, DATE_TRUNC('week', signal_date)
                ),
                with_bias AS (
                    SELECT *,
                           CASE WHEN bull > bear * 2 THEN 'BULL'
                                WHEN bear > bull * 2 THEN 'BEAR'
                                ELSE 'NEUTRAL' END as bias,
                           LAG(CASE WHEN bull > bear * 2 THEN 'BULL'
                                    WHEN bear > bull * 2 THEN 'BEAR'
                                    ELSE 'NEUTRAL' END)
                               OVER (PARTITION BY ticker ORDER BY week) as prev_bias
                    FROM weekly_flows
                    WHERE bull + bear > 1000000
                )
                SELECT ticker, week, prev_bias, bias, bull, bear
                FROM with_bias
                WHERE bias != prev_bias
                AND prev_bias IS NOT NULL
                AND bias != 'NEUTRAL' AND prev_bias != 'NEUTRAL'
                ORDER BY (bull + bear) DESC
                LIMIT 15
            """)).fetchall()
            reversals = []
            for r in rows:
                rv = {
                    "ticker": r[0], "week": str(r[1]),
                    "from": r[2], "to": r[3],
                    "bull": float(r[4] or 0), "bear": float(r[5] or 0),
                }
                reversals.append(rv)
                log.info("  FLIP: {t} {f} -> {to} week={w} bull=${b:,.0f}M bear=${br:,.0f}M",
                         t=rv["ticker"], f=rv["from"], to=rv["to"],
                         w=rv["week"], b=rv["bull"]/1e6, br=rv["bear"]/1e6)
            results["whale_reversals"] = reversals
    except Exception as e:
        log.warning("Whale reversals: {e}", e=str(e))

    # 4. SUPPLY CHAIN CONTAGION — which companies share suppliers/signals
    log.info("DOT 4: Supply chain signal contagion...")
    try:
        with engine.connect() as conn:
            set_timeout(conn)
            rows = conn.execute(text("""
                WITH top_tickers AS MATERIALIZED (
                    SELECT ticker
                    FROM signal_data
                    WHERE signal_date >= CURRENT_DATE - 30
                      AND ticker IS NOT NULL
                      AND signal_type IN ('whale_flow', 'whale_options', 'unusual_options', 'options_flow', 'insider')
                    GROUP BY ticker
                    ORDER BY COUNT(*) DESC
                    LIMIT 150
                ),
                recent AS MATERIALIZED (
                    SELECT sd.signal_date, sd.ticker, sd.signal_type, SUM(sd.magnitude) AS magnitude
                    FROM signal_data sd
                    JOIN top_tickers tt ON tt.ticker = sd.ticker
                    WHERE sd.signal_date >= CURRENT_DATE - 30
                      AND sd.ticker IS NOT NULL
                      AND sd.signal_type IN ('whale_flow', 'whale_options', 'unusual_options', 'options_flow', 'insider')
                      AND sd.magnitude > 0
                    GROUP BY sd.signal_date, sd.ticker, sd.signal_type
                )
                SELECT s1.ticker as ticker_a, s2.ticker as ticker_b,
                       COUNT(*) as shared_days,
                       CORR(s1.magnitude, s2.magnitude) as signal_correlation,
                       array_agg(DISTINCT s1.signal_type) as shared_types
                FROM recent s1
                JOIN recent s2 ON s1.signal_date = s2.signal_date
                    AND s1.signal_type = s2.signal_type
                    AND s1.ticker < s2.ticker
                GROUP BY s1.ticker, s2.ticker
                HAVING COUNT(*) >= 10
                AND CORR(s1.magnitude, s2.magnitude) > 0.3
                ORDER BY CORR(s1.magnitude, s2.magnitude) DESC NULLS LAST
                LIMIT 20
            """)).fetchall()
            contagion = []
            for r in rows:
                c = {
                    "ticker_a": r[0], "ticker_b": r[1],
                    "shared_days": r[2],
                    "correlation": round(float(r[3]), 3) if r[3] else None,
                    "types": list(r[4])[:5] if r[4] else [],
                }
                contagion.append(c)
                log.info("  PAIR: {a} <-> {b} corr={c} shared={d}d types={t}",
                         a=c["ticker_a"], b=c["ticker_b"],
                         c=c["correlation"], d=c["shared_days"], t=c["types"])
            results["signal_contagion"] = contagion
    except Exception as e:
        log.warning("Contagion: {e}", e=str(e))

    # 5. MONEY TRAIL — wealth flows between connected actors
    log.info("DOT 5: Money trails through actor network...")
    try:
        with engine.connect() as conn:
            set_timeout(conn)
            rows = conn.execute(text("""
                SELECT wf.from_actor, wf.to_entity, wf.implication,
                       SUM(wf.amount_estimate) as total_flow,
                       COUNT(*) as flow_count,
                       a1.category as from_category,
                       a2.category as to_category
                FROM wealth_flows wf
                LEFT JOIN actors a1 ON wf.from_actor = a1.id
                LEFT JOIN actors a2 ON wf.to_entity = a2.id
                WHERE wf.amount_estimate > 100000
                AND wf.flow_date >= CURRENT_DATE - 90
                GROUP BY wf.from_actor, wf.to_entity, wf.implication,
                         a1.category, a2.category
                HAVING SUM(wf.amount_estimate) > 1000000
                ORDER BY SUM(wf.amount_estimate) DESC
                LIMIT 20
            """)).fetchall()
            trails = []
            for r in rows:
                t = {
                    "from": r[0][:30] if r[0] else "?",
                    "to": r[1][:30] if r[1] else "?",
                    "implication": r[2],
                    "total": float(r[3] or 0),
                    "count": r[4],
                    "from_cat": r[5], "to_cat": r[6],
                }
                trails.append(t)
            trails = dedupe_money_trails(trails)
            for t in trails:
                log.info("  FLOW: {f} [{fc}] --${t:,.0f}M--> {to} [{tc}] ({imp})",
                         f=t["from"], fc=t["from_cat"] or "?",
                         t=t["total"]/1e6, to=t["to"],
                         tc=t["to_cat"] or "?", imp=t["implication"] or "?")
            results["money_trails"] = trails
    except Exception as e:
        log.warning("Money trails: {e}", e=str(e))

    # 6. DIVERGENCE SCANNER — insider vs whale vs congress direction mismatches
    log.info("DOT 6: Smart money divergences...")
    try:
        with engine.connect() as conn:
            set_timeout(conn)
            rows = conn.execute(text("""
                WITH actor_positions AS (
                    SELECT ticker,
                        -- Insider net direction
                        SUM(CASE WHEN signal_type IN ('insider', 'quiverquant:insider')
                            AND direction IN ('buy', 'purchase') THEN magnitude
                            WHEN signal_type IN ('insider', 'quiverquant:insider')
                            AND direction IN ('sell', 'sale') THEN -magnitude
                            ELSE 0 END) as insider_net,
                        -- Congress net direction
                        SUM(CASE WHEN signal_type IN ('congressional', 'quiverquant:house', 'quiverquant:senate')
                            AND direction IN ('buy', 'purchase') THEN 1
                            WHEN signal_type IN ('congressional', 'quiverquant:house', 'quiverquant:senate')
                            AND direction IN ('sell', 'sale') THEN -1
                            ELSE 0 END) as congress_net,
                        -- Whale net direction
                        SUM(CASE WHEN signal_type IN ('whale_flow', 'whale_options', 'unusual_options', 'options_flow')
                            AND direction IN ('buy', 'bullish', 'call') THEN magnitude
                            WHEN signal_type IN ('whale_flow', 'whale_options', 'unusual_options', 'options_flow')
                            AND direction IN ('sell', 'bearish', 'put') THEN -magnitude
                            ELSE 0 END) as whale_net,
                        COUNT(*) as total_signals
                    FROM signal_data
                    WHERE signal_date >= CURRENT_DATE - 14
                    AND ticker IS NOT NULL
                    GROUP BY ticker
                    HAVING COUNT(*) >= 10
                )
                SELECT ticker, insider_net, congress_net, whale_net, total_signals,
                    CASE
                        WHEN insider_net < 0 AND whale_net > 0 THEN 'INSIDER SELL / WHALE BUY'
                        WHEN insider_net > 0 AND whale_net < 0 THEN 'INSIDER BUY / WHALE SELL'
                        WHEN congress_net < 0 AND whale_net > 0 THEN 'CONGRESS SELL / WHALE BUY'
                        WHEN congress_net > 0 AND insider_net < 0 THEN 'CONGRESS BUY / INSIDER SELL'
                        ELSE NULL
                    END as divergence_type
                FROM actor_positions
                WHERE (insider_net < 0 AND whale_net > 0)
                   OR (insider_net > 0 AND whale_net < 0)
                   OR (congress_net != 0 AND SIGN(congress_net) != SIGN(insider_net) AND insider_net != 0)
                ORDER BY ABS(whale_net) DESC
                LIMIT 20
            """)).fetchall()
            divs = []
            for r in rows:
                d = {
                    "ticker": r[0],
                    "insider_net": float(r[1] or 0),
                    "congress_net": int(r[2] or 0),
                    "whale_net": float(r[3] or 0),
                    "signals": r[4],
                    "divergence": r[5],
                }
                divs.append(d)
                log.info("  DIVERGE: {t} [{div}] insider=${i:,.0f} congress={c} whale=${w:,.0f}M",
                         t=d["ticker"], div=d["divergence"] or "?",
                         i=d["insider_net"], c=d["congress_net"],
                         w=d["whale_net"]/1e6)
            results["smart_money_divergences"] = divs
    except Exception as e:
        log.warning("Divergences: {e}", e=str(e))

    # 7. EVENT CHAINS — A causes B causes C
    log.info("DOT 7: Event chains (A -> B -> C)...")
    try:
        with engine.connect() as conn:
            set_timeout(conn)
            rows = conn.execute(text("""
                SELECT b1.category as event_a, b1.tickers[1] as ticker_a,
                       b2.category as event_b, b2.tickers[1] as ticker_b,
                       b1.published_at::date as date_a, b2.published_at::date as date_b,
                       (b2.published_at::date - b1.published_at::date) as days_between,
                       LEFT(b1.headline, 50) as headline_a,
                       LEFT(b2.headline, 50) as headline_b
                FROM business_events b1
                JOIN business_events b2
                    ON b1.tickers && b2.tickers
                    AND b2.published_at > b1.published_at
                    AND b2.published_at <= b1.published_at + INTERVAL '14 days'
                    AND b1.event_id != b2.event_id
                WHERE array_length(b1.tickers, 1) > 0
                AND array_length(b2.tickers, 1) > 0
                ORDER BY b1.published_at DESC
                LIMIT 20
            """)).fetchall()
            chains = []
            for r in rows:
                c = {
                    "event_a": r[0], "ticker_a": r[1],
                    "event_b": r[2], "ticker_b": r[3],
                    "date_a": str(r[4]), "date_b": str(r[5]),
                    "days": r[6],
                    "headline_a": r[7], "headline_b": r[8],
                }
                chains.append(c)
            chains = filter_event_chains(chains, generated_at)
            for c in chains:
                log.info("  CHAIN: [{ea}] {ta} ({da}) -> [{eb}] {tb} ({db}) [{d}d]",
                         ea=c["event_a"], ta=c["ticker_a"] or "?", da=c["date_a"],
                         eb=c["event_b"], tb=c["ticker_b"] or "?", db=c["date_b"],
                         d=c["days"])
                log.info("    A: {a}", a=c["headline_a"])
                log.info("    B: {b}", b=c["headline_b"])
            results["event_chains"] = chains
    except Exception as e:
        log.warning("Event chains: {e}", e=str(e))

    # 8. HIDDEN GEMS — small caps with unusual signal density
    log.info("DOT 8: Hidden gem scanner...")
    try:
        with engine.connect() as conn:
            set_timeout(conn)
            rows = conn.execute(text("""
                WITH signal_density AS (
                    SELECT ticker,
                           COUNT(*) as signals,
                           COUNT(DISTINCT signal_type) as types,
                           SUM(CASE WHEN direction IN ('buy','bullish','call') THEN 1 ELSE 0 END) as bull,
                           SUM(CASE WHEN direction IN ('sell','bearish','put') THEN 1 ELSE 0 END) as bear,
                           MAX(magnitude) as max_mag,
                           MAX(confidence) as max_conf
                    FROM signal_data
                    WHERE signal_date >= CURRENT_DATE - 14
                    AND ticker IS NOT NULL AND ticker != ''
                    GROUP BY ticker
                ),
                with_mcap AS (
                    SELECT sd.*,
                           cp.sector,
                           cp.name as company_name
                    FROM signal_density sd
                    LEFT JOIN company_profiles cp ON cp.ticker = sd.ticker
                )
                SELECT ticker, company_name, sector, signals, types,
                       bull, bear, max_mag, max_conf,
                       CASE WHEN bull > bear * 2 THEN 'BULLISH'
                            WHEN bear > bull * 2 THEN 'BEARISH'
                            ELSE 'MIXED' END as bias
                FROM with_mcap
                WHERE signals BETWEEN 3 AND 500
                AND types >= 2
                AND (bull > bear * 2 OR bear > bull * 2)
                ORDER BY types DESC, signals DESC
                LIMIT 25
            """)).fetchall()
            gems = []
            for r in rows:
                g = {
                    "ticker": r[0], "name": r[1], "sector": r[2],
                    "signals": r[3], "types": r[4],
                    "bull": r[5], "bear": r[6],
                    "max_mag": to_float(r[7]), "max_conf": to_float(r[8]),
                    "bias": r[9],
                }
                gems.append(g)
            gems = filter_hidden_gems(gems)
            for g in gems:
                log.info("  GEM: {t} ({n}) [{s}] {sig} signals, {ty} types, {b} [{bi}]",
                         t=g["ticker"], n=(g["name"] or "?")[:25],
                         s=(g["sector"] or "?")[:15],
                         sig=g["signals"], ty=g["types"],
                         b="%d bull / %d bear" % (g["bull"], g["bear"]),
                         bi=g["bias"])
            results["hidden_gems"] = gems
    except Exception as e:
        log.warning("Hidden gems: {e}", e=str(e))

    validation_stats: dict[str, dict] = {}
    try:
        with engine.connect() as conn:
            conn.execute(text("SET statement_timeout = '20s'"))
            validation_stats = compute_dot_type_forward_validation(conn, generated_at=generated_at)
    except Exception as exc:
        log.warning("connected-dot forward validation unavailable: {e}", e=str(exc))

    operator_results = prepare_operator_results(results, validation_stats)

    # Summary
    log.info("=== V2 DOT CONNECTION COMPLETE ===")
    for key, val in operator_results.items():
        log.info("  {k}: {n} items", k=key, n=len(val) if isinstance(val, list) else "?")

    output_path = Path(__file__).parent.parent / "output" / "dots_v2.json"
    output_path.parent.mkdir(exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(operator_results, f, indent=2, default=str)
    log.info("Results written to {p}", p=output_path)
    try:
        persisted = persist_connected_dot_cards(
            engine,
            results,
            generated_at=generated_at,
            validation_stats=validation_stats,
        )
        log.info("Persisted {n} connected-dot Surfacer cards", n=persisted)
    except Exception as exc:
        log.warning("connected-dot persistence failed: {e}", e=str(exc))

    return operator_results


if __name__ == "__main__":
    main()
