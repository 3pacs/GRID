"""
DerivativesGrid Dealer Flow Narrative Synthesis.

Generates model-based dealer-flow briefings from GRID options snapshots.
The sign convention is assumed; open interest does not measure dealer books.

Outputs describe conditional hedging under an assumed position model.
"""

from __future__ import annotations

import calendar
import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.availability import unavailable

SPOT_CONTRACT = "spy_receipt_chain_batch_pit_v5"


def valid_spy_gex_profile(profile: Any, briefing_date: date) -> bool:
    """Validate the dated source pair before saving or serving a narrative."""
    if not isinstance(profile, dict):
        return False
    spot = profile.get("spot")
    if (profile.get("spot_source") != "spy_close_receipt"
            or profile.get("spot_basis") != "prior_completed_unadjusted_close"
            or not isinstance(spot, (int, float)) or isinstance(spot, bool)
            or not math.isfinite(spot) or spot <= 0
            or profile.get("estimated") is not True
            or profile.get("basis") != "options_open_interest_with_assumed_dealer_sign_and_black_scholes"
            or not isinstance(profile.get("spot_receipt_id"), int)
            or profile["spot_receipt_id"] <= 0
            or not isinstance(profile.get("chain_capture_ordinal"), int)
            or isinstance(profile["chain_capture_ordinal"], bool)
            or profile["chain_capture_ordinal"] <= 0):
        return False
    try:
        chain_date = date.fromisoformat(profile["chain_snap_date"])
        spot_date = date.fromisoformat(profile["spot_obs_date"])
        available_at = datetime.fromisoformat(profile["spot_available_at"])
        receipt_created_at = datetime.fromisoformat(profile["spot_receipt_created_at"])
        release_date = date.fromisoformat(profile["spot_release_date"])
        vintage_date = date.fromisoformat(profile["spot_vintage_date"])
        chain_first = datetime.fromisoformat(profile["chain_created_at"])
        chain_last = datetime.fromisoformat(profile["chain_created_at_max"])
        chain_started = datetime.fromisoformat(profile["chain_capture_started_at"])
        chain_completed = datetime.fromisoformat(profile["chain_capture_completed_at"])
        UUID(profile["chain_batch_id"])
    except (KeyError, TypeError, ValueError, AttributeError):
        return False
    if any(ts.tzinfo is None for ts in (
        available_at, receipt_created_at, chain_first, chain_last,
        chain_started, chain_completed,
    )):
        return False
    return (
        chain_date == briefing_date == date.today()
        and profile.get("snap_date") == chain_date.isoformat()
        and 1 <= (chain_date - spot_date).days <= 4
        and available_at >= datetime.combine(
            spot_date + timedelta(days=1), datetime.min.time(), timezone.utc
        )
        and release_date <= chain_date and vintage_date <= chain_date
        and available_at <= receipt_created_at <= chain_completed
        and chain_started <= chain_completed <= datetime.now(timezone.utc)
        and chain_first <= chain_last <= datetime.now(timezone.utc)
        and chain_started.astimezone(timezone.utc).date() == chain_date
        and chain_first.astimezone(timezone.utc).date() == chain_date
        and chain_last.astimezone(timezone.utc).date() == chain_date
        and chain_completed.astimezone(timezone.utc).date() == chain_date
    )

# ── DB table DDL ──────────────────────────────────────────────────────

_ENSURE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dealer_flow_briefings (
    id SERIAL PRIMARY KEY,
    briefing_date DATE NOT NULL UNIQUE,
    content TEXT NOT NULL,
    positioning_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def _ensure_table(engine: Engine) -> None:
    """Create the dealer_flow_briefings table if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text(_ENSURE_TABLE_SQL))


# ── LLM client (same pattern as celestial_briefing.py) ───────────────

def _get_llm_client() -> Any:
    """Return the best available shared LLM client."""
    try:
        from ollama.client import get_client
        return get_client()
    except Exception:
        return None


# ── OpEx calendar ─────────────────────────────────────────────────────

def _next_monthly_opex(from_date: date | None = None) -> date:
    """Compute the next monthly OpEx (3rd Friday of the month).

    If today IS the 3rd Friday, returns today (it hasn't expired yet
    if we're computing pre-close).
    """
    d = from_date or date.today()
    year, month = d.year, d.month

    # 3rd Friday = first Friday + 14 days
    # calendar.monthcalendar returns weeks (Mon=0 ... Sun=6)
    # We need the 3rd occurrence of Friday (weekday 4)
    cal = calendar.monthcalendar(year, month)
    fridays = [week[4] for week in cal if week[4] != 0]
    third_friday = date(year, month, fridays[2])

    if third_friday >= d:
        return third_friday

    # Move to next month
    if month == 12:
        year, month = year + 1, 1
    else:
        month += 1

    cal = calendar.monthcalendar(year, month)
    fridays = [week[4] for week in cal if week[4] != 0]
    return date(year, month, fridays[2])


def _days_to_opex(from_date: date | None = None) -> int:
    d = from_date or date.today()
    return (_next_monthly_opex(d) - d).days


# ── Data gathering ────────────────────────────────────────────────────

def _gather_positioning_data(engine: Engine) -> dict[str, Any]:
    """Gather all dealer positioning data needed for the briefing.

    Returns a dict with:
    - gex: per-ticker GEX profiles (SPY, QQQ, IWM)
    - vix: current VIX level
    - returns: 1d and 5d SPY returns
    - opex: days to next monthly OpEx + date
    - top_signals: top tickers from options_daily_signals
    """
    data: dict[str, Any] = {
        "date": date.today().isoformat(),
        "gex": {},
        "vix": None,
        "returns": {"1d": None, "5d": None},
        "opex": {
            "date": _next_monthly_opex().isoformat(),
            "days": _days_to_opex(),
        },
        "top_signals": [],
    }

    # ── GEX profiles for index ETFs ──────────────────────────────────
    try:
        from physics.dealer_gamma import DealerGammaEngine
        dge = DealerGammaEngine(db_engine=engine)

        for ticker in ("SPY", "QQQ", "IWM"):
            try:
                profile = dge.compute_gex_profile(ticker)
                if "error" not in profile:
                    # Strip the heavy profile/per_strike arrays for the
                    # positioning_data JSONB (keep them for prompt building)
                    data["gex"][ticker] = profile
            except Exception as exc:
                log.debug("GEX for {t} failed: {e}", t=ticker, e=str(exc))
    except Exception as exc:
        log.warning("DealerGammaEngine unavailable: {e}", e=str(exc))

    # ── VIX ──────────────────────────────────────────────────────────
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT value, obs_date FROM raw_series "
                "WHERE series_id = 'YF:^VIX:close' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()
            if row:
                data["vix"] = round(float(row[0]), 2)
    except Exception as exc:
        log.debug("VIX fetch failed: {e}", e=str(exc))

    # ── Recent SPY returns ───────────────────────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT value, obs_date FROM raw_series "
                "WHERE series_id = 'YF:^GSPC:close' "
                "ORDER BY obs_date DESC LIMIT 6"
            )).fetchall()
            if rows and len(rows) >= 2:
                latest = float(rows[0][0])
                prev_1d = float(rows[1][0])
                data["returns"]["1d"] = round((latest / prev_1d - 1) * 100, 2)
                if len(rows) >= 6:
                    prev_5d = float(rows[5][0])
                    data["returns"]["5d"] = round((latest / prev_5d - 1) * 100, 2)
    except Exception as exc:
        log.debug("SPY returns fetch failed: {e}", e=str(exc))

    # ── Top ticker options signals ───────────────────────────────────
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT ticker, put_call_ratio, iv_skew, iv_atm, "
                "       total_oi, spot_price "
                "FROM options_daily_signals "
                "WHERE signal_date = ("
                "  SELECT MAX(signal_date) FROM options_daily_signals"
                ") "
                "ORDER BY total_oi DESC "
                "LIMIT 10"
            )).fetchall()

            for row in rows:
                data["top_signals"].append({
                    "ticker": row[0],
                    "pcr": round(float(row[1]), 3) if row[1] else None,
                    "iv_skew": round(float(row[2]), 4) if row[2] else None,
                    "iv_atm": round(float(row[3]), 4) if row[3] else None,
                    "total_oi": int(row[4]) if row[4] else None,
                    "spot": round(float(row[5]), 2) if row[5] else None,
                })
    except Exception as exc:
        log.debug("Options signals fetch failed: {e}", e=str(exc))

    return data


# ── Prompt construction ───────────────────────────────────────────────

def _fmt_dollar(value: float | None) -> str:
    """Format a dollar-gamma value into human-readable $B/$M/$K."""
    if value is None:
        return "N/A"
    abs_val = abs(value)
    sign = "-" if value < 0 else ""
    if abs_val >= 1e9:
        return f"{sign}${abs_val / 1e9:.2f}B"
    if abs_val >= 1e6:
        return f"{sign}${abs_val / 1e6:.1f}M"
    if abs_val >= 1e3:
        return f"{sign}${abs_val / 1e3:.0f}K"
    return f"{sign}${abs_val:.0f}"


def _fmt_ticker_block(ticker: str, gex_data: dict[str, Any]) -> str:
    """Format a single ticker's GEX data into a prompt block."""
    spot = gex_data.get("spot", "?")
    gex = gex_data.get("gex_aggregate", 0)
    flip = gex_data.get("gamma_flip")
    put_wall = gex_data.get("put_wall")
    call_wall = gex_data.get("call_wall")
    delta = gex_data.get("dealer_delta", 0)
    vanna = gex_data.get("vanna_exposure", 0)
    charm = gex_data.get("charm_exposure", 0)
    regime = gex_data.get("regime", "UNKNOWN")

    # The nearest zero crossing has no universal sign direction. The engine
    # classifies the regime from modeled GEX at the verified reference spot.
    territory = regime.replace("_", " ") if isinstance(regime, str) else "UNKNOWN"

    lines = [
        f"### {ticker}",
        f"- Modeled GEX Aggregate: {_fmt_dollar(gex)} (assumed dealer sign)",
        f"- Modeled Gamma Flip: {flip if flip else 'N/A'}",
        f"- Flip crossings in search range: {gex_data.get('gamma_flip_crossings', 'N/A')}",
        f"- Prior verified close ({gex_data.get('spot_obs_date')}): {spot} --> Modeled {territory}",
        f"- Modeled Put Wall: {put_wall if put_wall else 'N/A'}",
        f"- Modeled Call Wall: {call_wall if call_wall else 'N/A'}",
        f"- Modeled Vanna Exposure: {_fmt_dollar(vanna)}",
        f"- Modeled Charm Exposure: {_fmt_dollar(charm)}",
        f"- Modeled Net Dealer-Side Delta: {_fmt_dollar(delta)}",
        "",
    ]
    return "\n".join(lines)


def _fmt_top_signals(
    signals: list[dict[str, Any]],
    gex_map: dict[str, dict[str, Any]],
) -> str:
    """Format top ticker signals with their GEX regime if available."""
    if not signals:
        return "No options signal data available."

    lines: list[str] = []
    for sig in signals[:7]:
        ticker = sig["ticker"]
        gex_info = gex_map.get(ticker, {})
        regime = gex_info.get("regime", "N/A")
        gex_val = _fmt_dollar(gex_info.get("gex_aggregate")) if gex_info else "N/A"

        parts = [f"- **{ticker}**:"]
        if sig.get("pcr") is not None:
            parts.append(f"PCR={sig['pcr']:.2f}")
        if sig.get("iv_skew") is not None:
            parts.append(f"IV_skew={sig['iv_skew']:.3f}")
        if sig.get("iv_atm") is not None:
            parts.append(f"IV_ATM={sig['iv_atm']:.1%}")
        parts.append(f"GEX={gex_val}")
        parts.append(f"Regime={regime}")

        lines.append(" | ".join(parts))

    return "\n".join(lines)


def _build_prompt(data: dict[str, Any]) -> tuple[str, str]:
    """Build system and user prompts for the dealer flow briefing.

    Returns:
        (system_prompt, user_prompt)
    """
    system_prompt = (
        "You are a derivatives market analyst explaining an estimated options "
        "exposure model. Dealer signs are assumed from open interest, not observed "
        "positions. The input spot is a prior verified close, not a live quote. "
        "Do not claim provider-sourced dealer positioning, current dealer books, "
        "forced trades, or certain support and resistance.\n\n"

        "Your job is NOT to predict direction. Your job is to explain the "
        "model's conditional implications, uncertainty and key levels.\n\n"

        "KEY PRINCIPLES:\n"
        "- The model assumes a dealer sign for options open interest.\n"
        "- Under the assumed positions, long gamma can imply dampening hedges; "
        "short gamma can imply amplifying hedges. This is conditional.\n"
        "- Vanna describes model sensitivity of delta to implied volatility; "
        "it is not a measured hedge order.\n"
        "- Charm: as time passes, option delta decays. Near OpEx, gamma concentrates. "
        "Post-OpEx, gamma unwinds and vol can expand.\n"
        "- A computed gamma flip is a model zero crossing, not a verified regime boundary; "
        "either side can have either GEX sign, and there may be multiple crossings.\n"
        "- Put and call walls are modeled exposure concentrations.\n"
        "- Expiry changes modeled exposure; direction and size of actual flows "
        "are unknown.\n\n"

        "STYLE:\n"
        "- Be technical but accessible. Use specific dollar amounts.\n"
        "- No vague hand-waving. Every claim must reference a number.\n"
        "- Describe conditional hedging under the assumed position model.\n"
        "- Keep the briefing under 800 words."
    )

    # Build data context
    gex = data.get("gex", {})
    vix = data.get("vix")
    returns = data.get("returns", {})
    opex = data.get("opex", {})
    signals = data.get("top_signals", [])

    context_lines: list[str] = [
        "## Estimated Dealer Exposure From Dated Inputs",
        "",
    ]

    # Main index GEX blocks
    for ticker in ("SPY", "QQQ", "IWM"):
        if ticker in gex:
            context_lines.append(_fmt_ticker_block(ticker, gex[ticker]))

    # Market-wide context
    context_lines.append("### Market-Wide Context")
    context_lines.append(f"- VIX: {vix if vix else 'N/A'}")
    context_lines.append(
        f"- Days to Monthly OpEx: {opex.get('days', '?')} "
        f"(expires {opex.get('date', '?')})"
    )
    context_lines.append(
        f"- SPY 1d Return: {returns.get('1d', 'N/A')}%"
    )
    context_lines.append(
        f"- SPY 5d Return: {returns.get('5d', 'N/A')}%"
    )

    # PCR from top signals for SPY/QQQ if available
    spy_sig = next((s for s in signals if s["ticker"] == "SPY"), None)
    qqq_sig = next((s for s in signals if s["ticker"] == "QQQ"), None)
    if spy_sig and spy_sig.get("pcr") is not None:
        context_lines.append(f"- SPY Put/Call Ratio: {spy_sig['pcr']:.2f}")
    if qqq_sig and qqq_sig.get("pcr") is not None:
        context_lines.append(f"- QQQ Put/Call Ratio: {qqq_sig['pcr']:.2f}")
    context_lines.append("")

    # Top ticker signals
    context_lines.append("### Top Ticker Signals (by open interest)")
    context_lines.append(_fmt_top_signals(signals, gex))
    context_lines.append("")

    data_block = "\n".join(context_lines)

    user_prompt = (
        f"Generate a Dealer Flow Briefing for {date.today().isoformat()}.\n\n"
        f"{data_block}\n\n"

        "## Generate a Dealer Flow Briefing with these sections:\n\n"

        "### Regime Assessment\n"
        "One paragraph: Does the assumed model estimate long or short gamma? "
        "What does this mean mechanically? Use specific numbers. Explain whether "
        "the model conditionally implies damping or amplification. Do not "
        "infer observed realized volatility from GEX alone.\n\n"

        "### Key Levels\n"
        "Gamma flip, put wall, call wall for SPY (and QQQ if different story). "
        "Explain these as conditional model levels; do not present a floor as certain.\n\n"

        "### Vanna & Charm Dynamics\n"
        "Explain the vol-sensitivity and time-sensitivity of current positioning. "
        "Quote the model outputs, but do not translate them into forced dollar "
        "trades or forecasted price moves without a calibrated conversion.\n\n"

        "### OpEx Dynamics\n"
        "Days to OpEx and conditional changes to modeled exposure as contracts "
        "expire. Do not assert an amount of actual dealer gamma will unwind "
        "or forecast realized volatility without evidence.\n\n"

        "### Flow Outlook\n"
        "Synthesis: given all the above, what are the mechanical forces saying? "
        "Which conditional model scenario matters? What observations would "
        "change the picture? Recompute GEX at a new verified spot and chain; "
        "crossing a flip price alone does not establish a regime direction."
    )

    return system_prompt, user_prompt


# ── Main entry points ─────────────────────────────────────────────────

def generate_dealer_flow_briefing(engine: Engine) -> dict[str, Any]:
    """Generate and store a daily dealer flow narrative briefing.

    Parameters:
        engine: SQLAlchemy engine for database access.

    Returns:
        dict with keys: content, positioning_data, briefing_date, created_at.
    """
    # Gather data
    positioning = _gather_positioning_data(engine)

    spy = positioning.get("gex", {}).get("SPY", {})
    if not valid_spy_gex_profile(spy, date.today()):
        return unavailable(
            "SPY GEX profile lacks a dated verified close and chain pair",
            source="spy_close_receipt",
            content=None,
            positioning_data=None,
            briefing_date=None,
            created_at=None,
        )
    positioning["spot_contract"] = SPOT_CONTRACT

    _ensure_table(engine)

    # Build prompt
    system_prompt, user_prompt = _build_prompt(positioning)

    # Call LLM
    client = _get_llm_client()
    content: str | None = None

    if client is not None:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        content = client.chat(
            messages=messages,
            temperature=0.4,
            num_predict=1200,
        )

    if content is None:
        content = _generate_fallback(positioning)
        log.warning("LLM unavailable -- using fallback dealer flow briefing")

    # Store in DB (strip heavy profile/per_strike from JSONB to keep it lean)
    today = date.today()
    lean_positioning = _strip_heavy_fields(positioning)
    positioning_json = json.dumps(lean_positioning, default=str)

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO dealer_flow_briefings "
                    "(briefing_date, content, positioning_data) "
                    "VALUES (:d, :c, CAST(:p AS jsonb)) "
                    "ON CONFLICT (briefing_date) DO UPDATE "
                    "SET content = EXCLUDED.content, "
                    "    positioning_data = EXCLUDED.positioning_data, "
                    "    created_at = NOW()"
                ),
                {"d": today, "c": content, "p": positioning_json},
            )
        log.info("Dealer flow briefing stored for {d}", d=today)
    except Exception as exc:
        log.warning("Could not store dealer flow briefing: {err}", err=str(exc))

    return {
        "content": content,
        "positioning_data": lean_positioning,
        "briefing_date": today.isoformat(),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def get_latest_flow_briefing(engine: Engine) -> dict[str, Any]:
    """Return the latest dealer flow briefing from the database.

    If no briefing exists for today, returns the most recent one with a
    staleness indicator.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        dict with keys: content, positioning_data, briefing_date,
                        created_at, stale.
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT briefing_date, content, positioning_data, created_at "
                    "FROM dealer_flow_briefings "
                    "ORDER BY briefing_date DESC "
                    "LIMIT 1"
                )
            ).fetchone()

        if row is None:
            return {
                "content": None,
                "positioning_data": None,
                "briefing_date": None,
                "created_at": None,
                "stale": True,
                "note": "No dealer flow briefings generated yet.",
            }

        briefing_date = row[0]
        is_stale = briefing_date < date.today()
        positioning = row[2]
        saved_spy = (
            positioning.get("gex", {}).get("SPY")
            if isinstance(positioning, dict) and isinstance(positioning.get("gex"), dict)
            else None
        )
        if (briefing_date != date.today()
                or not isinstance(positioning, dict)
                or positioning.get("spot_contract") != SPOT_CONTRACT
                or not valid_spy_gex_profile(saved_spy, briefing_date)):
            return {
                "content": None, "positioning_data": None,
                "briefing_date": str(briefing_date),
                "created_at": str(row[3]) if row[3] else None,
                "stale": True,
                "note": "Saved dealer flow briefing lacks the current dated-source contract.",
            }

        result: dict[str, Any] = {
            "content": row[1],
            "positioning_data": positioning,
            "briefing_date": str(briefing_date),
            "created_at": str(row[3]) if row[3] else None,
            "stale": is_stale,
        }

        if is_stale:
            result["note"] = (
                f"Briefing is from {briefing_date}, not today. "
                "Today's briefing has not been generated yet."
            )

        return result

    except Exception as exc:
        log.warning("Could not fetch dealer flow briefing: {err}", err=str(exc))
        return {
            "content": None,
            "positioning_data": None,
            "briefing_date": None,
            "created_at": None,
            "stale": True,
            "error": str(exc),
        }


# ── Helpers ───────────────────────────────────────────────────────────

def _strip_heavy_fields(positioning: dict[str, Any]) -> dict[str, Any]:
    """Remove per_strike and profile arrays from GEX data for lean JSONB storage."""
    clean = dict(positioning)
    clean_gex = {}
    for ticker, gex_data in positioning.get("gex", {}).items():
        slim = {k: v for k, v in gex_data.items() if k not in ("profile", "per_strike")}
        clean_gex[ticker] = slim
    clean["gex"] = clean_gex
    return clean


def _generate_fallback(positioning: dict[str, Any]) -> str:
    """Generate a data-only briefing when LLM is unavailable."""
    lines = [
        f"# Dealer Flow Briefing -- {positioning.get('date', date.today().isoformat())}",
        "",
        "**Note: AI analysis unavailable. Data summary only.**",
        "",
    ]

    for ticker in ("SPY", "QQQ", "IWM"):
        gex_data = positioning.get("gex", {}).get(ticker)
        if gex_data:
            lines.append(f"## {ticker}")
            lines.append(f"- Prior verified close ({gex_data.get('spot_obs_date')}): {gex_data.get('spot')}")
            lines.append(f"- GEX: {_fmt_dollar(gex_data.get('gex_aggregate'))}")
            lines.append(f"- Regime: **{gex_data.get('regime')}**")
            lines.append(f"- Gamma Flip: {gex_data.get('gamma_flip')}")
            lines.append(f"- Put Wall: {gex_data.get('put_wall')}")
            lines.append(f"- Call Wall: {gex_data.get('call_wall')}")
            lines.append(f"- Vanna: {_fmt_dollar(gex_data.get('vanna_exposure'))}")
            lines.append(f"- Charm: {_fmt_dollar(gex_data.get('charm_exposure'))}")
            lines.append(f"- Dealer Delta: {_fmt_dollar(gex_data.get('dealer_delta'))}")
            lines.append("")

    vix = positioning.get("vix")
    if vix:
        lines.append("## Market Context")
        lines.append(f"- VIX: {vix}")

    opex = positioning.get("opex", {})
    lines.append(f"- Days to OpEx: {opex.get('days', '?')} ({opex.get('date', '?')})")

    returns = positioning.get("returns", {})
    if returns.get("1d") is not None:
        lines.append(f"- SPY 1d: {returns['1d']}%")
    if returns.get("5d") is not None:
        lines.append(f"- SPY 5d: {returns['5d']}%")

    lines.append("")
    lines.append("---")
    lines.append("*LLM offline -- set OPENAI_API_KEY or start a local model for AI-powered dealer flow analysis*")

    return "\n".join(lines)
