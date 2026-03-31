"""AstroGrid sub-router: overview, snapshot, scorecard, universe, interpret."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log

from api.auth import require_auth
from api.dependencies import get_astrogrid_store, get_db_engine
from api.routers.astrogrid_helpers import (
    AstroEphemeris,
    AstrogridInterpretRequest,
    build_astrological_ephemeris,
    enrich_astrogrid_scoreable_universe,
    get_astrogrid_scoreable_universe,
    _build_objects,
    _build_scorecard_evaluation,
    _build_scorecard_item,
    _build_scorecard_summary,
    _build_signal_field,
    _build_snapshot_events,
    _build_snapshot_seer,
    _classify_prediction_scoreability,
    _compact_engine_outputs,
    _compute_full_ephemeris,
    _fallback_interpretation,
    _build_interpret_messages,
    _get_latest_resolved,
    _get_market_regime,
    _group_scorecard_items,
    _interpret_kp,
    _llm_backend_name,
    _MERCURY_RETROGRADES,
    _parse_json_response,
    _parse_snapshot_date,
    _phase_name,
    _resolve_scorecard_feature,
    _load_scorecard_history,
    _solar_cycle_phase,
    _zodiac_index,
    _element_index,
    _ZODIAC_ANIMALS,
    _ELEMENTS,
)

router = APIRouter(tags=["astrogrid"])


@router.get("/overview")
async def get_overview(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Current state of all celestial systems with interpretations."""
    today = date.today()

    try:
        engine = get_db_engine()
        ephemeris = _compute_full_ephemeris(today)
        phase = ephemeris["lunar_phase"]
        illum = ephemeris["lunar_illumination"]
        phase_name = _phase_name(phase)

        active_retros: list[dict] = []
        for start, end in _MERCURY_RETROGRADES:
            if start <= today <= end:
                active_retros.append({
                    "planet": "Mercury",
                    "start": str(start),
                    "end": str(end),
                })

        kp_val, kp_date = _get_latest_resolved(engine, "geomagnetic_kp_index")
        sunspot_val, _ = _get_latest_resolved(engine, "sunspot_number")
        wind_val, _ = _get_latest_resolved(engine, "solar_wind_speed")

        categories = {
            "lunar": {
                "moon_phase": phase_name,
                "illumination_pct": round(illum, 1),
                "phase_fraction": round(phase, 4),
                "days_to_new_moon": ephemeris["days_to_new_moon"],
                "days_to_full_moon": ephemeris["days_to_full_moon"],
                "interpretation": f"{phase_name}, {illum:.0f}% illuminated",
            },
            "planetary": {
                "mercury_retrograde": bool(ephemeris["mercury_retrograde"]),
                "jupiter_saturn_angle": ephemeris["jupiter_saturn_angle"],
                "mars_volatility_index": ephemeris["mars_volatility_index"],
                "planetary_stress_index": int(ephemeris["planetary_stress_index"]),
                "venus_cycle_phase": ephemeris["venus_cycle_phase"],
                "active_retrogrades": active_retros,
                "interpretation": (
                    f"Mercury retrograde active since {active_retros[0]['start']}"
                    if active_retros
                    else (
                        f"No retrogrades active. "
                        f"{int(ephemeris['planetary_stress_index'])} hard aspects."
                    )
                ),
            },
            "solar": {
                "kp_index": kp_val,
                "sunspot_number": sunspot_val,
                "solar_wind_speed_kms": wind_val,
                "solar_cycle_phase": ephemeris["solar_cycle_phase"],
                "geomagnetic_status": _interpret_kp(kp_val),
                "interpretation": (
                    f"Geomagnetic: {_interpret_kp(kp_val)}. "
                    f"Sunspots: {int(sunspot_val) if sunspot_val else 'N/A'}. "
                    f"Solar wind: {wind_val:.0f} km/s"
                    if wind_val
                    else "Solar data limited"
                ),
            },
            "vedic": {
                "nakshatra_index": int(ephemeris["nakshatra_index"]),
                "nakshatra_name": ephemeris["nakshatra_name"],
                "nakshatra_quality": int(ephemeris["nakshatra_quality"]),
                "nakshatra_quality_name": (
                    ["Fixed", "Movable", "Dual"][int(ephemeris["nakshatra_quality"])]
                ),
                "tithi": int(ephemeris["tithi"]),
                "rahu_ketu_axis": ephemeris["rahu_ketu_axis"],
                "dasha_cycle_phase": ephemeris["dasha_cycle_phase"],
                "interpretation": (
                    f"Nakshatra: {ephemeris['nakshatra_name']} "
                    f"({'Fixed' if ephemeris['nakshatra_quality'] == 0 else 'Movable' if ephemeris['nakshatra_quality'] == 1 else 'Dual'}). "
                    f"Tithi: {int(ephemeris['tithi'])}."
                ),
            },
            "chinese": {
                "zodiac_animal": ephemeris["chinese_zodiac_animal"],
                "element": ephemeris["chinese_element_name"],
                "yin_yang": "Yang" if ephemeris["chinese_yin_yang"] == 0 else "Yin",
                "flying_star": int(ephemeris["feng_shui_flying_star"]),
                "lunar_month": int(ephemeris["chinese_lunar_month"]),
                "iching_hexagram": int(ephemeris["iching_hexagram_of_day"]),
                "interpretation": (
                    f"Year of the {ephemeris['chinese_element_name']} "
                    f"{ephemeris['chinese_zodiac_animal']} "
                    f"({'Yang' if ephemeris['chinese_yin_yang'] == 0 else 'Yin'}). "
                    f"Flying Star Period {int(ephemeris['feng_shui_flying_star'])}."
                ),
            },
        }

        return {
            "as_of": str(today),
            "categories": categories,
            "active_retrogrades": active_retros,
            "current_moon_phase": phase_name,
            "solar_activity_level": _interpret_kp(kp_val),
            "current_nakshatra": ephemeris["nakshatra_name"],
            "chinese_year": (
                f"{ephemeris['chinese_element_name']} "
                f"{ephemeris['chinese_zodiac_animal']}"
            ),
        }

    except Exception as exc:
        log.warning("AstroGrid overview failed: {e}", e=str(exc))
        return {"error": str(exc), "as_of": str(today)}


@router.get("/snapshot")
async def get_snapshot(
    date_str: str | None = Query(
        alias="date", default=None, description="YYYY-MM-DD or ISO datetime"
    ),
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Unified AstroGrid state payload for the standalone UI."""
    try:
        target = _parse_snapshot_date(date_str)
    except ValueError as exc:
        return {"error": str(exc)}

    engine = get_db_engine()
    ephemeris = AstroEphemeris()
    full_ephemeris = build_astrological_ephemeris(target)
    objects = _build_objects(target, ephemeris, full_ephemeris)

    solar_features: dict[str, Any] = {
        "geomagnetic_kp_index": None,
        "sunspot_number": None,
        "solar_wind_speed": None,
        "solar_cycle_phase": None,
    }
    for feature_name in solar_features:
        solar_features[feature_name], _ = _get_latest_resolved(engine, feature_name)

    if solar_features["solar_cycle_phase"] is None:
        solar_features["solar_cycle_phase"] = round(_solar_cycle_phase(target), 6)

    market_regime, market_bias = _get_market_regime(engine, target)
    signals, signal_field = _build_signal_field(
        full_ephemeris["lunar_phase"],
        full_ephemeris["nakshatra"],
        full_ephemeris["aspects"],
        objects,
        solar_features,
        market_regime,
        market_bias,
        full_ephemeris["void_of_course"],
    )
    events = _build_snapshot_events(
        target,
        full_ephemeris["lunar_phase"],
        full_ephemeris["nakshatra"],
        full_ephemeris["aspects"],
        full_ephemeris["void_of_course"],
    )
    seer = _build_snapshot_seer(
        full_ephemeris["lunar_phase"],
        full_ephemeris["nakshatra"],
        signals,
        events,
    )

    source_parts = ["analysis.ephemeris"]
    if any(value is not None for value in solar_features.values()):
        source_parts.append("resolved_series")
    if market_regime is not None:
        source_parts.append("regime_history")

    snapshot = {
        "date": str(target),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "+".join(source_parts),
        "objects": objects,
        "bodies": objects,
        "positions": full_ephemeris["positions"],
        "aspects": full_ephemeris["aspects"],
        "lunar": full_ephemeris["lunar_phase"],
        "nakshatra": full_ephemeris["nakshatra"],
        "void_of_course": full_ephemeris["void_of_course"],
        "retrograde_planets": full_ephemeris["retrograde_planets"],
        "summary": full_ephemeris["summary"],
        "signals": signals,
        "signal_field": signal_field,
        "events": events,
        "seer": seer,
        "grid": {
            "market_regime": market_regime,
            "market_regime_bias": market_bias,
            "solar": solar_features,
        },
    }
    try:
        get_astrogrid_store().save_snapshot(snapshot)
    except Exception as exc:
        log.warning("AstroGrid snapshot store unavailable: {e}", e=str(exc))
    return snapshot


@router.get("/scorecard")
async def get_scorecard(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Hybrid AstroGrid market scorecard using existing GRID read paths."""
    from api.routers.watchlist import _batch_fetch_prices, _cache_price_to_db

    engine = get_db_engine()
    history_start = date.today() - timedelta(days=120)

    scoreable_universe = get_astrogrid_scoreable_universe()
    lookup_tickers = [asset["lookup_ticker"] for asset in scoreable_universe]
    try:
        live_quotes = _batch_fetch_prices(lookup_tickers)
    except Exception as exc:
        log.debug("AstroGrid scorecard live-price fallback unavailable: {e}", e=str(exc))
        live_quotes = {}

    items: list[dict[str, Any]] = []
    source_parts: set[str] = set()

    with engine.connect() as conn:
        scoreable_universe = enrich_astrogrid_scoreable_universe(conn)
        for asset in scoreable_universe:
            feature_name, candidate_features = _resolve_scorecard_feature(conn, asset)
            history = (
                _load_scorecard_history(conn, feature_name, history_start)
                if feature_name
                else []
            )
            live_quote = live_quotes.get(asset["lookup_ticker"])
            if live_quote and live_quote.get("price") is not None:
                _cache_price_to_db(
                    engine, asset["lookup_ticker"], float(live_quote["price"]), date.today()
                )
            item = _build_scorecard_item(
                asset, feature_name, candidate_features, history, live_quote
            )
            items.append(item)
            if feature_name and history:
                source_parts.add("resolved_series")
            if live_quote:
                source_parts.add("watchlist_live")

    groups = _group_scorecard_items(items)
    leaders = sorted(
        [item for item in items if item.get("latest") is not None],
        key=lambda item: float(item["momentum_score"]),
        reverse=True,
    )[:3]
    laggards = sorted(
        [item for item in items if item.get("latest") is not None],
        key=lambda item: float(item["momentum_score"]),
    )[:3]
    evaluation = _build_scorecard_evaluation(engine, scoreable_universe)
    if evaluation["overall"]["total_predictions"] > 0:
        source_parts.add("oracle_predictions")

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "+".join(sorted(source_parts)) if source_parts else "none",
        "universe": {
            "id": "hybrid_v1",
            "ranked_horizons": ["macro", "swing", "intraday"],
            "groups": ["crypto", "macro"],
            "assets": [
                {
                    "symbol": asset["symbol"],
                    "asset_class": asset["asset_class"],
                    "price_feature": asset["price_feature"],
                    "benchmark_symbol": asset["benchmark_symbol"],
                    "status": asset.get("status", "unscored"),
                    "scoreable_now": bool(asset.get("scoreable_now")),
                    "reason_if_not": asset.get("reason_if_not"),
                }
                for asset in scoreable_universe
            ],
        },
        "items": items,
        "groups": groups,
        "leaders": leaders,
        "laggards": laggards,
        "summary": _build_scorecard_summary(items, groups, evaluation),
        "evaluation": evaluation,
    }


@router.get("/universe")
async def get_scoreable_universe(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Return the canonical AstroGrid scoreable-universe contract."""
    engine = get_db_engine()
    try:
        with engine.connect() as conn:
            assets = enrich_astrogrid_scoreable_universe(conn)
    except Exception as exc:
        log.warning("AstroGrid universe contract fallback used: {e}", e=str(exc))
        assets = get_astrogrid_scoreable_universe()
        for asset in assets:
            asset["status"] = "unknown"
            asset["scoreable_now"] = False
            asset["reason_if_not"] = "coverage check unavailable"
            asset["history_points"] = None
            asset["latest_obs_date"] = None
    counts = Counter(str(asset.get("status") or "unknown") for asset in assets)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe_id": "hybrid_v1",
        "assets": assets,
        "counts": {
            "scoreable_now": int(counts.get("scoreable_now", 0)),
            "degraded": int(counts.get("degraded", 0)),
            "unscored": int(counts.get("unscored", 0)),
            "unknown": int(counts.get("unknown", 0)),
            "total": len(assets),
        },
    }


@router.post("/interpret")
async def interpret_snapshot(
    req: AstrogridInterpretRequest,
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Run a model-backed interpretation over deterministic AstroGrid state."""
    fallback = _fallback_interpretation(req)

    try:
        from ollama.client import get_client

        client = get_client()
        backend = _llm_backend_name(client)
        model = getattr(client, "model", None)
        if not getattr(client, "is_available", False):
            fallback["backend"] = backend
            fallback["model"] = model
            return fallback

        messages = _build_interpret_messages(req)
        raw = client.chat(
            messages=messages,
            temperature=0.2,
            num_predict=1200,
        )
        parsed = _parse_json_response(raw)
        if not parsed:
            fallback["backend"] = backend
            fallback["model"] = model
            return fallback

        summary = str(parsed.get("summary") or fallback["summary"])
        seer = (
            parsed.get("seer")
            if isinstance(parsed.get("seer"), dict)
            else fallback["seer"]
        )
        threads = (
            parsed.get("threads")
            if isinstance(parsed.get("threads"), list)
            else fallback["threads"]
        )
        engine_notes = (
            parsed.get("engine_notes")
            if isinstance(parsed.get("engine_notes"), list)
            else fallback["engine_notes"]
        )
        tone_notes = (
            parsed.get("tone_notes")
            if isinstance(parsed.get("tone_notes"), list)
            else fallback["tone_notes"]
        )

        result = {
            "summary": summary,
            "seer": {
                "reading": str(seer.get("reading") or fallback["seer"]["reading"]),
                "prediction": str(seer.get("prediction") or fallback["seer"]["prediction"]),
                "why": list(seer.get("why") or fallback["seer"]["why"])[:6],
                "warnings": list(seer.get("warnings") or fallback["seer"]["warnings"])[:6],
            },
            "threads": threads[:12],
            "engine_notes": engine_notes[:8],
            "tone_notes": tone_notes[:6],
            "used_llm": True,
            "backend": backend,
            "model": model,
            "raw_length": len(raw or ""),
        }
        try:
            get_astrogrid_store().save_interpretation(req.model_dump(), result)
        except Exception as persist_exc:
            log.warning(
                "AstroGrid interpret store unavailable: {e}", e=str(persist_exc)
            )
        return result
    except Exception as exc:
        log.warning("AstroGrid interpretation failed: {e}", e=str(exc))
        fallback["error"] = str(exc)
        try:
            get_astrogrid_store().save_interpretation(req.model_dump(), fallback)
        except Exception as persist_exc:
            log.warning(
                "AstroGrid fallback store unavailable: {e}", e=str(persist_exc)
            )
        return fallback
