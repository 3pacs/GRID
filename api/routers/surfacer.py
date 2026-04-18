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


def _walk_strings(value: Any) -> list[str]:
    parsed = _safe_json(value)
    if isinstance(parsed, dict):
        strings: list[str] = []
        for key, item in parsed.items():
            strings.append(str(key))
            strings.extend(_walk_strings(item))
        return strings
    if isinstance(parsed, list):
        strings = []
        for item in parsed:
            strings.extend(_walk_strings(item))
        return strings
    if parsed is None:
        return []
    return [str(parsed)]


def _is_internal_signal(value: Any) -> bool:
    raw = str(value or "").strip().lower()
    if not raw:
        return False
    if "snap:llm_task_" in raw or "llm_task_" in raw:
        return True
    if raw.startswith("snap:") and any(
        token in raw
        for token in (
            "anomaly_detection",
            "expectation_tracking",
            "feature_interpretation",
            "hypothesis",
            "research_task",
            "task_",
        )
    ):
        return True
    return False


def _is_internal_hypothesis(data: dict[str, Any]) -> bool:
    fields = ("thesis", "pattern_type", "evidence", "test_criteria", "invalidation", "role")
    return any(_is_internal_signal(item) for field in fields for item in _walk_strings(data.get(field)))


_TICKER_STOPWORDS = {
    "AI",
    "API",
    "BPS",
    "CEO",
    "CFO",
    "CPI",
    "DEX",
    "ETF",
    "EU",
    "FED",
    "FOMC",
    "GDP",
    "IPO",
    "LLM",
    "SEC",
    "US",
    "USD",
}


def _extract_tickers(*values: Any) -> list[str]:
    tickers: list[str] = []
    for value in values:
        for text_value in _walk_strings(value):
            for match in re.findall(r"(?<![A-Za-z0-9])\$?([A-Z]{1,5})(?![A-Za-z0-9])", text_value):
                if match in _TICKER_STOPWORDS or match in tickers:
                    continue
                tickers.append(match)
                if len(tickers) >= 6:
                    return tickers
    return tickers


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


def _horizon_days(candidate: dict[str, Any]) -> int:
    horizon = str(candidate.get("horizon") or "").lower()
    if horizon == "swing":
        return 7
    if horizon == "multi_week":
        return 30
    if horizon == "multi_month":
        return 90
    return 14


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


def _gate(name: str, score: float, weight: float, status: str, detail: str) -> dict[str, Any]:
    return {
        "name": name,
        "score": round(_clamp(score, 0, weight), 1),
        "weight": weight,
        "status": status,
        "detail": detail,
    }


def _unusable_information_flag(candidate: dict[str, Any]) -> str | None:
    haystack = " ".join(_walk_strings({
        "title": candidate.get("title"),
        "summary": candidate.get("summary"),
        "why_now": candidate.get("why_now"),
        "evidence": candidate.get("evidence"),
        "invalidation": candidate.get("invalidation"),
    })).lower()
    patterns = (
        "material nonpublic",
        "mnpi",
        "inside information",
        "insider tip",
        "leaked earnings",
        "leaked merger",
        "confidential deal",
        "not yet public",
        "non-public",
        "nonpublic",
    )
    for pattern in patterns:
        if pattern in haystack:
            return pattern
    return None


def _fetch_options_context(conn: Any, ticker: str) -> dict[str, Any]:
    if not ticker or not _table_exists(conn, "options_daily_signals"):
        return {}
    try:
        row = conn.execute(
            text(
                """
                SELECT signal_date, put_call_ratio, max_pain, iv_skew,
                       total_oi, total_volume, near_expiry, spot_price, iv_atm
                FROM options_daily_signals
                WHERE ticker = :ticker
                ORDER BY signal_date DESC NULLS LAST
                LIMIT 1
                """
            ),
            {"ticker": ticker.upper()},
        ).fetchone()
    except Exception as exc:
        log.debug("surfacer options context failed for {t}: {e}", t=ticker, e=exc)
        return {}
    if not row:
        return {}
    data = row._mapping
    return {
        "signal_date": _iso(data.get("signal_date")),
        "put_call_ratio": _safe_float(data.get("put_call_ratio"), default=None),
        "max_pain": _safe_float(data.get("max_pain"), default=None),
        "iv_skew": _safe_float(data.get("iv_skew"), default=None),
        "total_oi": _safe_float(data.get("total_oi"), default=None),
        "total_volume": _safe_float(data.get("total_volume"), default=None),
        "near_expiry": data.get("near_expiry"),
        "spot_price": _safe_float(data.get("spot_price"), default=None),
        "iv_atm": _safe_float(data.get("iv_atm"), default=None),
    }


def _fetch_track_record(conn: Any, ticker: str, direction: str | None, model_name: str | None = None) -> dict[str, Any]:
    if not ticker or not _table_exists(conn, "oracle_predictions"):
        return {"samples": 0}
    clauses = ["ticker = :ticker", "verdict IN ('hit', 'miss', 'partial')"]
    params: dict[str, Any] = {"ticker": ticker.upper()}
    if direction and direction != "watch":
        clauses.append("LOWER(COALESCE(direction, prediction_type, '')) LIKE :direction")
        params["direction"] = f"%{direction.lower().replace('bullish', 'up').replace('bearish', 'down')}%"
    if model_name:
        clauses.append("model_name = :model_name")
        params["model_name"] = model_name
    where_sql = " AND ".join(clauses)
    try:
        row = conn.execute(
            text(
                "SELECT COUNT(*) AS samples, "
                "COUNT(*) FILTER (WHERE verdict = 'hit') AS hits, "
                "COUNT(*) FILTER (WHERE verdict = 'partial') AS partials, "
                "COUNT(*) FILTER (WHERE verdict = 'miss') AS misses, "
                "AVG(pnl_pct) AS avg_pnl_pct, AVG(confidence) AS avg_confidence "
                "FROM oracle_predictions WHERE " + where_sql
            ),
            params,
        ).fetchone()
    except Exception as exc:
        log.debug("surfacer track record failed for {t}: {e}", t=ticker, e=exc)
        return {"samples": 0}
    if not row:
        return {"samples": 0}
    data = row._mapping
    samples = int(data.get("samples") or 0)
    hits = int(data.get("hits") or 0)
    partials = int(data.get("partials") or 0)
    hit_rate = (hits + partials * 0.5) / samples if samples else None
    return {
        "samples": samples,
        "hits": hits,
        "partials": partials,
        "misses": int(data.get("misses") or 0),
        "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
        "avg_pnl_pct": round(_safe_float(data.get("avg_pnl_pct")), 2) if data.get("avg_pnl_pct") is not None else None,
        "avg_confidence": round(_safe_float(data.get("avg_confidence")), 4) if data.get("avg_confidence") is not None else None,
    }


def _fetch_signal_confirmation(conn: Any, ticker: str, direction: str | None) -> dict[str, Any]:
    if not ticker or not _table_exists(conn, "signal_data"):
        return {"samples": 0, "aligned": 0, "opposed": 0}
    try:
        rows = conn.execute(
            text(
                """
                SELECT signal_type, direction, confidence, created_at
                FROM signal_data
                WHERE ticker = :ticker
                  AND created_at >= NOW() - interval '14 days'
                ORDER BY created_at DESC
                LIMIT 30
                """
            ),
            {"ticker": ticker.upper()},
        ).fetchall()
    except Exception as exc:
        log.debug("surfacer signal confirmation failed for {t}: {e}", t=ticker, e=exc)
        return {"samples": 0, "aligned": 0, "opposed": 0}
    wanted = str(direction or "watch").lower()
    aligned = 0
    opposed = 0
    signals: list[str] = []
    for row in rows:
        data = row._mapping
        row_dir = _direction_label(data.get("direction"), data.get("signal_type"))
        if wanted != "watch" and row_dir == wanted:
            aligned += 1
        elif wanted != "watch" and row_dir in {"bullish", "bearish"} and row_dir != wanted:
            opposed += 1
        label = _clean_label(data.get("signal_type"), "Signal")
        if label not in signals:
            signals.append(label)
    return {
        "samples": len(rows),
        "aligned": aligned,
        "opposed": opposed,
        "signals": signals[:5],
    }


def _build_conviction_gate(
    candidate: dict[str, Any],
    *,
    options: dict[str, Any] | None = None,
    track_record: dict[str, Any] | None = None,
    confirmation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    options = options or {}
    track_record = track_record or {"samples": 0}
    confirmation = confirmation or {"samples": 0, "aligned": 0, "opposed": 0}
    gates: list[dict[str, Any]] = []
    ticker = (candidate.get("tickers") or [None])[0]
    direction = candidate.get("direction") or "watch"
    blocked = _unusable_information_flag(candidate)
    if blocked:
        return {
            "score": 0,
            "label": "blocked",
            "action": "Blocked",
            "summary": f"Do not trade: evidence references {blocked}.",
            "gates": [_gate("legal provenance", 0, 100, "blocked", f"Evidence references {blocked}.")],
            "missing": [],
            "expectation_gap": None,
            "track_record": track_record,
        }

    if ticker:
        gates.append(_gate("target", 12, 12, "pass", f"Tradable target: {ticker}."))
    else:
        gates.append(_gate("target", 0, 12, "missing", "No concrete ticker, basket, or instrument."))

    confidence = _unit(candidate.get("confidence"))
    evidence_count = len(candidate.get("evidence") or [])
    evidence_score = min(15, confidence * 10 + min(5, evidence_count))
    gates.append(_gate(
        "evidence",
        evidence_score,
        15,
        "pass" if evidence_score >= 10 else "weak",
        f"{evidence_count} evidence item{'s' if evidence_count != 1 else ''}; stated confidence {round(confidence * 100)}%.",
    ))

    modeled_move = abs(_safe_float(candidate.get("expected_move_pct")))
    iv_atm = options.get("iv_atm")
    implied_move = None
    edge = None
    if iv_atm and iv_atm > 0:
        implied_move = iv_atm * math.sqrt(_horizon_days(candidate) / 365) * 100
    if modeled_move and implied_move:
        edge = modeled_move - implied_move
        exp_score = 15 if edge >= 3 else 10 if edge > 0 else 4
        exp_status = "pass" if edge >= 1 else "weak"
        exp_detail = f"Modeled {modeled_move:.1f}% vs market-implied {implied_move:.1f}%."
    elif modeled_move:
        exp_score = 8
        exp_status = "weak"
        exp_detail = f"Modeled move {modeled_move:.1f}%; missing options-implied move."
    else:
        exp_score = 0
        exp_status = "missing"
        exp_detail = "No modeled move or market-implied expectation."
    gates.append(_gate("expectation gap", exp_score, 15, exp_status, exp_detail))

    samples = int(track_record.get("samples") or 0)
    hit_rate = track_record.get("hit_rate")
    avg_pnl = track_record.get("avg_pnl_pct")
    if samples >= 10 and hit_rate is not None:
        history_score = min(18, max(0, hit_rate * 18 + (2 if avg_pnl and avg_pnl > 0 else -2)))
        history_status = "pass" if hit_rate >= 0.58 and (avg_pnl is None or avg_pnl > 0) else "weak"
        history_detail = f"{samples} scored analogs; hit rate {round(hit_rate * 100)}%; avg PnL {avg_pnl if avg_pnl is not None else 'n/a'}%."
    elif samples:
        history_score = min(8, samples)
        history_status = "weak"
        history_detail = f"Only {samples} scored analogs."
    else:
        history_score = 0
        history_status = "missing"
        history_detail = "No scored analogs yet."
    gates.append(_gate("track record", history_score, 18, history_status, history_detail))

    contradictions = len(candidate.get("contradictions") or [])
    contradiction_score = max(0, 15 - contradictions * 5)
    gates.append(_gate(
        "contradictions",
        contradiction_score,
        15,
        "pass" if contradictions == 0 else "weak" if contradictions <= 2 else "blocked",
        f"{contradictions} anti-signal{'s' if contradictions != 1 else ''} attached.",
    ))

    aligned = int(confirmation.get("aligned") or 0)
    opposed = int(confirmation.get("opposed") or 0)
    confirm_score = max(0, min(15, aligned * 5 - opposed * 4 + (3 if confirmation.get("samples") else 0)))
    gates.append(_gate(
        "fresh confirmation",
        confirm_score,
        15,
        "pass" if aligned >= 2 and opposed == 0 else "weak" if confirmation.get("samples") else "missing",
        f"{aligned} aligned, {opposed} opposed recent target signals.",
    ))

    liquidity_score = 0
    if options.get("total_oi") or options.get("total_volume"):
        oi = _safe_float(options.get("total_oi"))
        vol = _safe_float(options.get("total_volume"))
        liquidity_score = 10 if oi >= 5000 or vol >= 1000 else 6 if oi >= 1000 or vol >= 100 else 3
        liquidity_detail = f"Options OI {int(oi):,}; volume {int(vol):,}."
    elif ticker and ":" not in str(ticker):
        liquidity_score = 5
        liquidity_detail = "Equity target present; options liquidity not confirmed."
    else:
        liquidity_detail = "Liquidity not confirmed."
    gates.append(_gate("execution", liquidity_score, 10, "pass" if liquidity_score >= 8 else "weak", liquidity_detail))

    source_modules = set(candidate.get("source_modules") or [])
    provenance_score = 10 if source_modules.intersection({"oracle", "signal_data"}) else 4
    gates.append(_gate(
        "provenance",
        provenance_score,
        10,
        "pass" if provenance_score >= 8 else "weak",
        "Uses persisted oracle/signal data." if provenance_score >= 8 else "Derived research row; needs source lineage.",
    ))

    total_weight = sum(gate["weight"] for gate in gates)
    score = round(sum(gate["score"] for gate in gates) / total_weight * 100, 1) if total_weight else 0.0
    missing = [gate["name"] for gate in gates if gate["status"] == "missing"]
    blocked_gates = [gate["name"] for gate in gates if gate["status"] == "blocked"]
    if blocked_gates:
        label = "blocked"
        action = "Blocked"
    elif score >= 82 and not missing:
        label = "play"
        action = "Actionable"
    elif score >= 62:
        label = "watch"
        action = "Watch"
    else:
        label = "research"
        action = "Research"

    summary = {
        "play": "Clears the first conviction gate; still require execution confirmation before sizing.",
        "watch": "Promising, but one or more gates need confirmation before sizing.",
        "research": "Not enough verified edge yet. Keep it in research.",
        "blocked": "Do not trade.",
    }[label]
    return {
        "score": score,
        "label": label,
        "action": action,
        "summary": summary,
        "gates": gates,
        "missing": missing,
        "expectation_gap": {
            "modeled_move_pct": round(modeled_move, 2) if modeled_move else None,
            "market_implied_move_pct": round(implied_move, 2) if implied_move else None,
            "edge_pct": round(edge, 2) if edge is not None else None,
            "iv_atm": round(iv_atm, 4) if iv_atm else None,
        },
        "track_record": track_record,
        "confirmation": confirmation,
        "options": options,
    }


def _attach_conviction(conn: Any, candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    options_cache: dict[str, dict[str, Any]] = {}
    track_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
    confirmation_cache: dict[tuple[str, str], dict[str, Any]] = {}
    enriched: list[dict[str, Any]] = []
    for candidate in candidates:
        ticker = (candidate.get("tickers") or [None])[0]
        direction = str(candidate.get("direction") or "watch")
        model_name = str(candidate.get("model_name") or "")
        options = {}
        track_record = {"samples": 0}
        confirmation = {"samples": 0, "aligned": 0, "opposed": 0}
        if ticker:
            options = options_cache.setdefault(ticker, _fetch_options_context(conn, ticker))
            track_key = (ticker, direction, model_name)
            track_record = track_cache.setdefault(track_key, _fetch_track_record(conn, ticker, direction, model_name or None))
            confirmation_key = (ticker, direction)
            confirmation = confirmation_cache.setdefault(confirmation_key, _fetch_signal_confirmation(conn, ticker, direction))
        candidate = {
            **candidate,
            "conviction": _build_conviction_gate(
                candidate,
                options=options,
                track_record=track_record,
                confirmation=confirmation,
            ),
        }
        enriched.append(candidate)
    return enriched


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
        "model_name": data.get("model_name"),
        "expected_move_pct": move_pct,
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
        "candidate_type": "oracle",
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
        "expected_move_pct": None,
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
        "candidate_type": "signal",
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
    is_internal = _is_internal_hypothesis(data)
    tickers = _extract_tickers(data.get("thesis"), data.get("test_criteria"), data.get("evidence"))
    research_only = not is_internal and not tickers
    if is_internal:
        confidence = min(confidence, 0.20)
        score = min(score, 12)
        title = "Internal telemetry correlation"
    elif research_only:
        score = min(score, 34)

    return {
        "id": f"hypothesis-{data.get('id')}",
        "title": title[:140],
        "summary": (
            "System activity correlated with another stream. Keep this in diagnostics; it is not market alpha."
            if is_internal
            else "Research-only hypothesis. Needs a concrete ticker, basket, or instrument before it belongs on the front page."
            if research_only
            else _format_hypothesis_summary(data)
        ),
        "why_now": (
            "Internal diagnostic row from hypothesis discovery; keep off trade surfacing."
            if is_internal
            else "Hypothesis row has no concrete tradable target yet."
            if research_only
            else f"{_clean_label(data.get('pattern_type'), 'Pattern')} pattern is active in the hypothesis book."
        ),
        "alpha_score": round(score, 1),
        "score_parts": {
            "signal": round(confidence * 100, 1),
            "freshness": round(_freshness_points(data.get("created_at")) * 10, 1),
            "confidence": round(confidence * 100, 1),
            "backtest": round(accuracy * 100, 1),
            "tradability": 0 if is_internal else 15 if research_only else 55,
            "risk_penalty": 80 if is_internal else 35 if research_only else 10 if tested < 3 else 3,
        },
        "confidence": confidence,
        "expected_move_pct": None,
        "direction": "watch",
        "horizon": "multi_week",
        "tickers": tickers,
        "trade_expression": (
            "Internal diagnostic only; not a trade candidate"
            if is_internal
            else f"Research {', '.join(tickers)} basket; require fresh confirming evidence before sizing"
            if tickers
            else "Convert to a ticker basket only after evidence refresh and invalidation review"
        ),
        "status": (
            "internal_telemetry"
            if is_internal
            else "research_only"
            if research_only
            else "unscored" if tested == 0 else str(data.get("status") or "testing")
        ),
        "freshness": _freshness(data.get("created_at")),
        "evidence": _evidence_from_payload(data.get("evidence"), "discovery", data.get("created_at")),
        "contradictions": [],
        "invalidation": (
            "No trade thesis. Only useful for monitoring the research pipeline."
            if is_internal
            else "Do not promote until the research row names a tradable target and gets fresh confirmation."
            if research_only
            else _format_invalidation(data.get("invalidation"))
        ),
        "next_update": _iso(data.get("last_tested")),
        "source_modules": ["diagnostic", "discovery", "hypotheses"] if is_internal else ["discovery", "hypotheses"],
        "front_page": not is_internal and not research_only,
        "diagnostic": is_internal,
        "research_only": research_only,
        "candidate_type": "hypothesis",
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
    return sorted(
        best.values(),
        key=lambda item: (
            item.get("conviction", {}).get("score", -1),
            item.get("alpha_score", 0),
        ),
        reverse=True,
    )


def _select_candidates(
    candidates: list[dict[str, Any]],
    *,
    include_diagnostics: bool,
    fresh_only: bool,
    horizon: str,
    limit: int,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    deduped = _dedupe_candidates(candidates)
    diagnostics_filtered = 0
    research_filtered = 0
    blocked_filtered = 0
    front_page_filtered = 0
    if not include_diagnostics:
        visible = []
        for item in deduped:
            if item.get("diagnostic"):
                diagnostics_filtered += 1
                continue
            if item.get("conviction", {}).get("label") == "blocked":
                blocked_filtered += 1
                continue
            if item.get("front_page") is False:
                front_page_filtered += 1
                if item.get("research_only"):
                    research_filtered += 1
                continue
            visible.append(item)
        deduped = visible
    if fresh_only:
        deduped = [
            item for item in deduped
            if item.get("freshness", {}).get("label") in {"fresh", "aging"}
        ]
    if horizon != "all":
        deduped = [item for item in deduped if item.get("horizon") == horizon]
    return deduped[:limit], {
        "diagnostics_filtered": diagnostics_filtered,
        "research_filtered": research_filtered,
        "blocked_filtered": blocked_filtered,
        "front_page_filtered": diagnostics_filtered + blocked_filtered + front_page_filtered,
    }


def _candidate_meta(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    sources = Counter(source for item in candidates for source in item.get("source_modules", []))
    fresh = sum(1 for item in candidates if item.get("freshness", {}).get("label") == "fresh")
    avg_score = sum(item.get("alpha_score", 0) for item in candidates) / len(candidates) if candidates else 0
    avg_conviction = sum(item.get("conviction", {}).get("score", 0) for item in candidates) / len(candidates) if candidates else 0
    actionable = sum(1 for item in candidates if item.get("conviction", {}).get("label") == "play")
    diagnostics = sum(1 for item in candidates if item.get("diagnostic"))
    return {
        "count": len(candidates),
        "fresh_count": fresh,
        "average_score": round(avg_score, 1),
        "average_conviction": round(avg_conviction, 1),
        "actionable_count": actionable,
        "sources": dict(sources),
        "diagnostic_count": diagnostics,
        "mode": "alpha_triage",
    }


@router.get("/candidates")
def list_candidates(
    limit: int = Query(16, ge=1, le=50),
    fresh_only: bool = Query(False),
    include_diagnostics: bool = Query(False),
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
            candidates.extend(_fetch_hypothesis_candidates(conn, max(40, limit * 2)))
            candidates = _attach_conviction(conn, candidates)
            thesis = _fetch_thesis_snapshot(conn)
    except Exception as exc:
        log.warning("surfacer candidate query unavailable: {e}", e=str(exc))

    selected, filtered_meta = _select_candidates(
        candidates,
        include_diagnostics=include_diagnostics,
        fresh_only=fresh_only,
        horizon=horizon,
        limit=limit,
    )
    meta = _candidate_meta(selected)
    meta.update(filtered_meta)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidates": selected,
        "thesis": thesis,
        "meta": meta,
    }
