"""
GRID Surfacer API.

Ranks fresh alpha candidates from the existing oracle, signal, and discovery
tables. Canvas can remain a sandbox; this router returns compact, actionable
cards with evidence, invalidation, and freshness built in.
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(
    prefix="/api/v1/surfacer",
    tags=["surfacer"],
    dependencies=[Depends(require_auth)],
)


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def _unit(value: Any) -> float:
    parsed = _safe_float(value)
    if parsed > 1:
        parsed = parsed / 100
    return _clamp(parsed, 0.0, 1.0)


def _safe_json(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return stripped
    return value


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _age_hours(value: Any) -> float | None:
    if not isinstance(value, datetime):
        return None
    timestamp = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - timestamp.astimezone(timezone.utc)).total_seconds() / 3600)


def _freshness(value: Any) -> dict[str, Any]:
    hours = _age_hours(value)
    if hours is None:
        label = "unknown"
    elif hours <= 24:
        label = "fresh"
    elif hours <= 96:
        label = "aging"
    else:
        label = "stale"
    return {"age_hours": hours, "label": label, "last_seen": _iso(value)}


def _table_exists(conn: Any, table_name: str) -> bool:
    try:
        return bool(conn.execute(text("SELECT to_regclass(:table_name)"), {"table_name": table_name}).scalar())
    except Exception as exc:
        log.debug("surfacer table check failed for {table}: {err}", table=table_name, err=exc)
        return False


def _slug(value: Any) -> str:
    return "-".join(str(value or "candidate").lower().replace(":", "-").split())


def _clean_label(value: Any, fallback: str = "Signal") -> str:
    label = " ".join(str(value or "").replace("_", " ").replace("-", " ").split())
    return label.title() if label else fallback


def _humanize_signal(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "market signal"

    text = raw
    replacements = {
        "snap:llm_task_": "AI research ",
        "snap:": "",
        "sig:": "",
        "dex_": "DEX ",
    }
    for before, after in replacements.items():
        text = text.replace(before, after)
    text = text.replace("_", " ").replace("-", " ").replace(":", " ")
    text = re.sub(r"\s+", " ", text).strip()

    known = {
        "insider": "insider activity",
        "darkpool": "dark-pool activity",
        "geopolitical tone": "geopolitical tone",
        "options flow": "options flow",
        "congressional": "congressional trading",
    }
    lower = text.lower()
    if lower in known:
        return known[lower]
    if lower.startswith("ai research "):
        return f"{text[0].upper()}{text[1:]} activity"
    return text


def _sanitize_text(value: Any, limit: int = 500) -> str:
    text_value = _compact_payload(value, limit * 2)
    text_value = re.sub(r"\bsnap:llm_task_([a-z0-9_]+)", lambda m: f"AI research {m.group(1).replace('_', ' ')}", text_value)
    text_value = re.sub(r"\bsnap:([a-z0-9_]+)", lambda m: m.group(1).replace("_", " "), text_value)
    text_value = re.sub(r"\bsig:([a-z0-9_]+)", lambda m: m.group(1).replace("_", " "), text_value)
    text_value = re.sub(r"\bdarkpool\b", "dark-pool activity", text_value, flags=re.IGNORECASE)
    text_value = re.sub(r"\s+", " ", text_value).strip()
    return text_value[:limit]


def _compact_payload(value: Any, limit: int = 260) -> str:
    parsed = _safe_json(value)
    if isinstance(parsed, dict):
        pieces = []
        for key, item in list(parsed.items())[:6]:
            if isinstance(item, dict):
                inner = item.get("name") or item.get("direction") or item.get("detail") or item.get("value") or item
            elif isinstance(item, list):
                inner = ", ".join(_compact_payload(entry, 80) for entry in item[:4])
                if len(item) > 4:
                    inner = f"{inner}, +{len(item) - 4} more"
            elif key in {"signal_a", "signal_b", "watch_signal", "expect_signal"}:
                inner = _humanize_signal(item)
            else:
                inner = item
            pieces.append(f"{_clean_label(key)}: {inner}")
        return "; ".join(pieces)[:limit]
    if isinstance(parsed, list):
        pieces = [_compact_payload(item, 80) for item in parsed[:4]]
        if len(parsed) > 4:
            pieces.append(f"+{len(parsed) - 4} more")
        return "; ".join(pieces)[:limit]
    return str(parsed or "")[:limit]


def _format_hypothesis_title(data: dict[str, Any], evidence: Any) -> str:
    evidence_payload = _safe_json(evidence)
    evidence_item = evidence_payload[0] if isinstance(evidence_payload, list) and evidence_payload else evidence_payload
    if isinstance(evidence_item, dict):
        signal_a = evidence_item.get("signal_a") or evidence_item.get("watch_signal")
        signal_b = evidence_item.get("signal_b") or evidence_item.get("expect_signal")
        lag_days = evidence_item.get("lag_days")
        correlation = _safe_float(evidence_item.get("correlation"), default=0.0)
        if signal_a and signal_b:
            verb = "leads"
            if correlation < -0.05:
                verb = "fades before"
            elif correlation > 0.05:
                verb = "leads"
            lag = f" within {int(_safe_float(lag_days))}d" if lag_days is not None else ""
            return f"{_humanize_signal(signal_a)} {verb} {_humanize_signal(signal_b)}{lag}"

    raw_title = str(data.get("thesis") or data.get("pattern_type") or "Discovered hypothesis")
    return _sanitize_text(raw_title, 120)


def _format_hypothesis_summary(data: dict[str, Any]) -> str:
    criteria = _safe_json(data.get("test_criteria"))
    if isinstance(criteria, dict):
        watch = criteria.get("watch_signal")
        expect = criteria.get("expect_signal")
        direction = str(criteria.get("expected_direction") or "move").replace("_", " ")
        direction = {
            "increases": "increase",
            "decreases": "decrease",
            "rises": "rise",
            "falls": "fall",
        }.get(direction.lower(), direction)
        lag = criteria.get("lag_days")
        if watch and expect:
            lag_text = f" within {int(_safe_float(lag))} day{'s' if int(_safe_float(lag)) != 1 else ''}" if lag is not None else ""
            return f"Watch {_humanize_signal(watch)}. Expect {_humanize_signal(expect)} to {direction}{lag_text}."
    if criteria:
        return _sanitize_text(criteria, 240)
    return "Research hypothesis needs a fresh confirmation trigger before it becomes a trade."


def _format_invalidation(value: Any) -> str:
    text_value = _sanitize_text(value or "Drop if the next validation run fails the test criteria.", 500)
    text_value = text_value.replace("does NOT", "does not")
    text_value = re.sub(r"does not increases\b", "does not increase", text_value, flags=re.IGNORECASE)
    text_value = re.sub(r"does not decreases\b", "does not decrease", text_value, flags=re.IGNORECASE)
    text_value = re.sub(r"1 days\b", "1 day", text_value)
    return text_value


def _direction_label(*values: Any) -> str:
    haystack = " ".join(str(v or "").lower() for v in values)
    words = set(re.findall(r"[a-z]+", haystack))
    if words.intersection({"put", "short", "bear", "bearish", "sell", "down"}):
        return "bearish"
    if words.intersection({"call", "long", "bull", "bullish", "buy", "up"}):
        return "bullish"
    return "watch"


def _horizon(expiry: Any = None, created_at: Any = None) -> str:
    if isinstance(expiry, datetime) and isinstance(created_at, datetime):
        days = (expiry - created_at).days
        if days <= 10:
            return "swing"
        if days <= 45:
            return "multi_week"
        return "multi_month"
    if isinstance(expiry, date):
        days = (expiry - date.today()).days
        if days <= 10:
            return "swing"
        if days <= 45:
            return "multi_week"
        return "multi_month"
    return "watch"


def _trade_expression(ticker: str | None, direction: str, instrument: Any = None) -> str:
    symbol = (ticker or "basket").upper()
    kind = str(instrument or "").upper()
    if ":" in symbol:
        return f"Watch {symbol}; require liquidity depth, holder quality, and venue access before sizing"
    if kind in {"CALL", "PUT"}:
        return f"{symbol} {kind} bias; confirm liquidity and spread before entry"
    if direction == "bullish":
        return f"Long bias in {symbol}; enter only on confirmed follow-through"
    if direction == "bearish":
        return f"Short or put bias in {symbol}; invalidate on reversal strength"
    return f"Watch {symbol}; require a confirming catalyst before sizing"


def _freshness_points(created_at: Any) -> float:
    hours = _age_hours(created_at)
    if hours is None:
        return 4
    if hours <= 12:
        return 10
    if hours <= 48:
        return 8
    if hours <= 96:
        return 5
    return 2


def _evidence_from_payload(payload: Any, source: str, timestamp: Any, limit: int = 5) -> list[dict[str, Any]]:
    parsed = _safe_json(payload)
    evidence: list[dict[str, Any]] = []

    if isinstance(parsed, list):
        for idx, item in enumerate(parsed[:limit]):
            if isinstance(item, dict):
                label = item.get("label") or item.get("type") or item.get("source") or f"{source} {idx + 1}"
                detail = item.get("detail") or item.get("text") or item.get("description") or item.get("reason") or _compact_payload(item)
                weight = _unit(item.get("weight") or item.get("confidence") or 0.5)
            else:
                label = f"{source} {idx + 1}"
                detail = _compact_payload(item)
                weight = 0.5
            evidence.append({
                "source": source,
                "label": _clean_label(label, source),
                "detail": _sanitize_text(detail, 500),
                "timestamp": _iso(timestamp),
                "weight": weight,
            })
    elif isinstance(parsed, dict):
        for idx, (key, value) in enumerate(parsed.items()):
            if idx >= limit:
                break
            evidence.append({
                "source": source,
                "label": _clean_label(key, source),
                "detail": _sanitize_text(value, 500),
                "timestamp": _iso(timestamp),
                "weight": 0.5,
            })
    elif parsed:
        evidence.append({
            "source": source,
            "label": _clean_label(source, source),
            "detail": _sanitize_text(parsed, 500),
            "timestamp": _iso(timestamp),
            "weight": 0.5,
        })

    return evidence


def _oracle_candidate(row: Any) -> dict[str, Any]:
    data = row._mapping
    ticker = str(data.get("ticker") or "").upper() or None
    direction = _direction_label(data.get("prediction_type"), data.get("direction"))
    confidence = _unit(data.get("confidence"))
    signal_strength = _unit(data.get("signal_strength"))
    coherence = _unit(data.get("coherence"))
    move_pct = abs(_safe_float(data.get("expected_move_pct")))
    move_score = _clamp(move_pct * 2.0, 0.0, 20.0)
    backtest = 70 if str(data.get("verdict") or "").lower() in {"hit", "partial"} else 45
    risk_penalty = 8 if confidence < 0.35 else 0
    score = _clamp(
        confidence * 35
        + signal_strength * 20
        + coherence * 15
        + move_score
        + _freshness_points(data.get("created_at"))
        + (5 if data.get("flow_context") else 0)
        - risk_penalty
    )

    evidence = _evidence_from_payload(data.get("signals"), "oracle", data.get("created_at"))
    evidence.extend(_evidence_from_payload(data.get("flow_context"), "flow", data.get("created_at"), limit=2))
    anti = _evidence_from_payload(data.get("anti_signals"), "anti-signal", data.get("created_at"), limit=3)

    return {
        "id": f"oracle-{data.get('id')}",
        "title": f"{ticker or 'Market'} {_clean_label(direction)} setup",
        "summary": f"{ticker or 'Market'} has a {direction} oracle read with {round(confidence * 100)}% confidence.",
        "why_now": "Fresh model prediction with supporting signal stack and flow context.",
        "alpha_score": round(score, 1),
        "score_parts": {
            "signal": round(signal_strength * 100, 1),
            "freshness": round(_freshness_points(data.get("created_at")) * 10, 1),
            "confidence": round(confidence * 100, 1),
            "backtest": backtest,
            "tradability": 80 if ticker else 45,
            "risk_penalty": risk_penalty,
        },
        "confidence": confidence,
        "direction": direction,
        "horizon": _horizon(data.get("expiry"), data.get("created_at")),
        "tickers": [ticker] if ticker else [],
        "trade_expression": _trade_expression(ticker, direction, data.get("prediction_type")),
        "status": "new" if _freshness(data.get("created_at"))["label"] == "fresh" else "watch",
        "freshness": _freshness(data.get("created_at")),
        "evidence": evidence[:7],
        "contradictions": [item["detail"] for item in anti[:3]],
        "invalidation": "Kill the setup if anti-signals dominate or price rejects the expected move window.",
        "next_update": _iso(data.get("expiry")),
        "source_modules": ["oracle", "signal_data"],
    }


def _signal_candidate(row: Any) -> dict[str, Any]:
    data = row._mapping
    ticker = str(data.get("ticker") or data.get("actor") or "").upper() or None
    signal_type = data.get("signal_type")
    direction = _direction_label(data.get("direction"), signal_type)
    raw_confidence = data.get("confidence")
    confidence_known = raw_confidence is not None and _safe_float(raw_confidence) > 0
    confidence = _unit(raw_confidence if confidence_known else 0.30)
    magnitude = abs(_safe_float(data.get("magnitude")))
    magnitude_score = _clamp(math.log10(magnitude + 1) * 10, 0.0, 28.0)
    fresh_points = _freshness_points(data.get("created_at"))
    speculative = bool(ticker and ":" in ticker) or "dex" in str(signal_type or "").lower()
    risk_penalty = (18 if speculative else 5) + (8 if not confidence_known else 0)
    tradability = 35 if speculative else 65 if ticker else 30
    score = _clamp(confidence * 30 + magnitude_score + fresh_points + 12 - risk_penalty)
    evidence = _evidence_from_payload(data.get("data"), "signal", data.get("created_at"))
    description = data.get("description") or f"{_clean_label(signal_type)} with magnitude {magnitude:.2f}"

    return {
        "id": f"signal-{data.get('id')}",
        "title": f"{ticker or 'Market'} {_clean_label(signal_type)}",
        "summary": _compact_payload(description, 240),
        "why_now": f"New {str(signal_type or 'signal').replace('_', ' ')} event ranked by recency and magnitude.",
        "alpha_score": round(score, 1),
        "score_parts": {
            "signal": round(magnitude_score / 35 * 100, 1),
            "freshness": round(fresh_points * 10, 1),
            "confidence": round(confidence * 100, 1),
            "backtest": 35,
            "tradability": tradability,
            "risk_penalty": risk_penalty,
        },
        "confidence": confidence,
        "direction": direction,
        "horizon": "swing",
        "tickers": [ticker] if ticker else [],
        "trade_expression": _trade_expression(ticker, direction),
        "status": "needs_research" if speculative or not confidence_known else (
            "new" if _freshness(data.get("created_at"))["label"] == "fresh" else "watch"
        ),
        "freshness": _freshness(data.get("created_at")),
        "evidence": evidence or [{
            "source": "signal",
            "label": _clean_label(signal_type),
            "detail": _compact_payload(description, 500),
            "timestamp": _iso(data.get("created_at")),
            "weight": confidence,
        }],
        "contradictions": [],
        "invalidation": "Do not size until price, flow, or news confirms the signal instead of fading it.",
        "next_update": _iso(data.get("signal_date")),
        "source_modules": ["signal_data"],
    }


def _hypothesis_candidate(row: Any) -> dict[str, Any]:
    data = row._mapping
    raw_confidence = data.get("confidence")
    confidence = _unit(raw_confidence if raw_confidence is not None else 0.35)
    tested = max(0, int(_safe_float(data.get("times_tested"))))
    correct = max(0, int(_safe_float(data.get("times_correct"))))
    accuracy = correct / tested if tested else 0.0
    tested_bonus = min(15, tested * 2)
    unscored_penalty = 10 if tested == 0 else 0
    score = _clamp(
        confidence * 38
        + accuracy * 25
        + tested_bonus
        + _freshness_points(data.get("created_at"))
        - unscored_penalty
    )
    title = _format_hypothesis_title(data, data.get("evidence"))

    return {
        "id": f"hypothesis-{data.get('id')}",
        "title": title[:140],
        "summary": _format_hypothesis_summary(data),
        "why_now": f"{_clean_label(data.get('pattern_type'), 'Pattern')} pattern is active in the hypothesis book.",
        "alpha_score": round(score, 1),
        "score_parts": {
            "signal": round(confidence * 100, 1),
            "freshness": round(_freshness_points(data.get("created_at")) * 10, 1),
            "confidence": round(confidence * 100, 1),
            "backtest": round(accuracy * 100, 1),
            "tradability": 45,
            "risk_penalty": 10 if tested < 3 else 3,
        },
        "confidence": confidence,
        "direction": "watch",
        "horizon": "multi_week",
        "tickers": [],
        "trade_expression": "Convert to a ticker basket only after evidence refresh and invalidation review",
        "status": "unscored" if tested == 0 else str(data.get("status") or "testing"),
        "freshness": _freshness(data.get("created_at")),
        "evidence": _evidence_from_payload(data.get("evidence"), "discovery", data.get("created_at")),
        "contradictions": [],
        "invalidation": _format_invalidation(data.get("invalidation")),
        "next_update": _iso(data.get("last_tested")),
        "source_modules": ["discovery", "hypotheses"],
    }


def _fetch_oracle_candidates(conn: Any, limit: int) -> list[dict[str, Any]]:
    if not _table_exists(conn, "oracle_predictions"):
        return []
    rows = conn.execute(
        text(
            """
            SELECT *
            FROM (
                SELECT DISTINCT ON (ticker, direction)
                    id, created_at, ticker, prediction_type, direction, expiry,
                    confidence, expected_move_pct, signal_strength, coherence,
                    model_name, signals, anti_signals, flow_context, verdict
                FROM oracle_predictions
                WHERE created_at >= NOW() - (:hours * interval '1 hour')
                ORDER BY ticker, direction, created_at DESC
            ) latest
            ORDER BY created_at DESC
            LIMIT :limit
            """
        ),
        {"hours": 24 * 21, "limit": limit},
    ).fetchall()
    return [_oracle_candidate(row) for row in rows]


def _fetch_signal_candidates(conn: Any, limit: int) -> list[dict[str, Any]]:
    if not _table_exists(conn, "signal_data"):
        return []
    rows = conn.execute(
        text(
            """
            SELECT id, signal_type, signal_date, ticker, actor, direction, magnitude,
                   description, data, confidence, source_id, created_at
            FROM signal_data
            WHERE created_at >= NOW() - (:hours * interval '1 hour')
            ORDER BY created_at DESC, ABS(COALESCE(magnitude, 0)) DESC
            LIMIT :limit
            """
        ),
        {"hours": 24 * 7, "limit": limit},
    ).fetchall()
    return [_signal_candidate(row) for row in rows]


def _fetch_hypothesis_candidates(conn: Any, limit: int) -> list[dict[str, Any]]:
    if not _table_exists(conn, "discovered_hypotheses"):
        return []
    rows = conn.execute(
        text(
            """
            SELECT id, thesis, pattern_type, evidence, test_criteria, invalidation,
                   confidence, status, times_tested, times_correct, created_at,
                   last_tested, role
            FROM discovered_hypotheses
            WHERE LOWER(COALESCE(status, 'active')) NOT IN
                ('killed', 'rejected', 'inactive', 'archived', 'duplicate')
            ORDER BY COALESCE(last_tested, created_at) DESC NULLS LAST, created_at DESC
            LIMIT :limit
            """
        ),
        {"limit": limit},
    ).fetchall()
    return [_hypothesis_candidate(row) for row in rows]


def _fetch_thesis_snapshot(conn: Any) -> dict[str, Any] | None:
    if not _table_exists(conn, "thesis_snapshots"):
        return None
    row = conn.execute(
        text(
            """
            SELECT timestamp, overall_direction, conviction, key_drivers,
                   risk_factors, narrative
            FROM thesis_snapshots
            ORDER BY timestamp DESC
            LIMIT 1
            """
        )
    ).fetchone()
    if not row:
        return None
    data = row._mapping
    return {
        "timestamp": _iso(data.get("timestamp")),
        "overall_direction": data.get("overall_direction"),
        "conviction": _unit(data.get("conviction")),
        "key_drivers": _safe_json(data.get("key_drivers")) or [],
        "risk_factors": _safe_json(data.get("risk_factors")) or [],
        "narrative": data.get("narrative"),
    }


def _dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        tickers = ",".join(candidate.get("tickers") or [])
        key = f"{tickers}:{_slug(candidate.get('title'))}:{candidate.get('direction')}"
        existing = best.get(key)
        if existing is None or candidate.get("alpha_score", 0) > existing.get("alpha_score", 0):
            best[key] = candidate
    return sorted(best.values(), key=lambda item: item.get("alpha_score", 0), reverse=True)


def _candidate_meta(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    sources = Counter(source for item in candidates for source in item.get("source_modules", []))
    fresh = sum(1 for item in candidates if item.get("freshness", {}).get("label") == "fresh")
    avg_score = sum(item.get("alpha_score", 0) for item in candidates) / len(candidates) if candidates else 0
    return {
        "count": len(candidates),
        "fresh_count": fresh,
        "average_score": round(avg_score, 1),
        "sources": dict(sources),
        "mode": "alpha_triage",
    }


@router.get("/candidates")
def list_candidates(
    limit: int = Query(16, ge=1, le=50),
    fresh_only: bool = Query(False),
    horizon: str = Query("all", pattern="^(all|swing|multi_week|multi_month|watch)$"),
    engine: Engine = Depends(get_db_engine),
) -> dict[str, Any]:
    """Return ranked alpha candidates with evidence and invalidation."""
    candidates: list[dict[str, Any]] = []
    thesis = None
    try:
        with engine.connect() as conn:
            per_source_limit = max(limit, 12)
            candidates.extend(_fetch_oracle_candidates(conn, per_source_limit))
            candidates.extend(_fetch_signal_candidates(conn, per_source_limit))
            candidates.extend(_fetch_hypothesis_candidates(conn, max(8, limit // 2)))
            thesis = _fetch_thesis_snapshot(conn)
    except Exception as exc:
        log.warning("surfacer candidate query unavailable: {e}", e=str(exc))

    deduped = _dedupe_candidates(candidates)
    if fresh_only:
        deduped = [
            item for item in deduped
            if item.get("freshness", {}).get("label") in {"fresh", "aging"}
        ]
    if horizon != "all":
        deduped = [item for item in deduped if item.get("horizon") == horizon]
    selected = deduped[:limit]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidates": selected,
        "thesis": thesis,
        "meta": _candidate_meta(selected),
    }
