"""
GRID 100x Bundled Digest — every 4 hours.

Scans all tickers, bundles 100x opportunities with full strike data,
cross-verifies against live market data before sending, and opportunistically
scrapes extra useful data while validating.

Pipeline:
  1. Run options scanner → get all opportunities
  2. For each opportunity, pull live verification:
     - Current spot price (yfinance)
     - Live options chain (verify strikes exist, IV is real)
     - Cross-check P/C ratio from multiple sources
  3. If verification fails → flag, scrape correct data, note source
  4. While scraping, grab any useful bonus data (volume spikes, unusual OI)
  5. Bundle everything into a beautifully formatted email
  6. Only send if at least 1 verified opportunity exists
"""

from __future__ import annotations

import html as _html
import json
import math
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from alerts.email import _render_html, _section_text, _send_in_thread


def _get_engine():
    from db import get_engine
    return get_engine()


# ── Nullable formatting ─────────────────────────────────────────────────────

MISSING = "N/A"


def _fmt(value: Any, spec: str = "", missing: str = MISSING) -> str:
    """Format a nullable number for the digest; None/NaN render as ``missing``.

    ``options_daily_signals`` columns are nullable and the scanner preserves
    that (``MispricingOpportunity.iv_atm`` is ``float | None`` since #476),
    so an f-string such as ``f"{opp.get('iv_atm', 0):.1%}"`` receives
    ``None`` whenever the key is present with a NULL value — ``dict.get``'s
    default only covers an *absent* key — and raises ``TypeError:
    unsupported format string passed to NoneType.__format__``. That took the
    whole 100x digest down (grid-hermes journal 2026-10-20 04:28:43Z,
    ``run_cycle:3856``). Route every nullable field through here instead.
    """
    if value is None:
        return missing
    try:
        if isinstance(value, float) and value != value:  # NaN
            return missing
        return format(value, spec)
    except (TypeError, ValueError):
        return missing


def _opportunity_summary_line(index: int, opp: dict) -> str:
    """One line of the LLM sanity-review prompt for a single opportunity.

    Never raises on a missing field: a row with ``iv_atm``/``spot_price``
    NULL is annotated as ``N/A`` so the LLM can weigh it, rather than
    aborting the review for every other opportunity in the batch.
    """
    thesis = (opp.get("thesis") or "")[:150]
    return (
        f"{index}. {opp.get('ticker', '?')} {opp.get('direction', '?')}S — "
        f"score {_fmt(opp.get('score'), '.1f')}/10, "
        f"payoff est {_fmt(opp.get('payoff_multiple'), '.0f')}x, "
        f"spot ${_fmt(opp.get('spot_price'), '.2f')}, "
        f"IV ATM {_fmt(opp.get('iv_atm'), '.1%')}, thesis: {thesis}"
    )


# ── Data Quality Sanity Checks ──────────────────────────────────────────────

def _sanity_check(opp) -> list[str]:
    """Pre-flight sanity check on an opportunity. Returns list of issues (empty = ok)."""
    issues = []

    # Max pain should be within 30% of spot — anything more is garbage data
    spot = opp.spot_price or 0
    max_pain = 0
    if hasattr(opp, 'signals') and isinstance(opp.signals, dict):
        mp_sig = opp.signals.get("max_pain_div", {})
        max_pain = mp_sig.get("value") or 0
    if not max_pain and hasattr(opp, 'strikes') and opp.strikes:
        max_pain = opp.strikes[0] if opp.strikes else 0

    if spot > 0 and max_pain > 0:
        divergence = abs(spot - max_pain) / spot
        if divergence > 0.30:
            issues.append(f"max_pain ${max_pain:.0f} vs spot ${spot:.0f} ({divergence:.0%} divergence — likely stale expiry data)")

    # IV ATM should be between 5% and 200% — outside is garbage
    iv = opp.iv_atm or 0
    if iv > 0 and (iv < 0.03 or iv > 2.5):
        issues.append(f"IV ATM {iv:.4f} outside sane range (3%-250%)")

    # IV skew should be between 0.3 and 5.0 — anything above is broken data
    skew = 0
    if hasattr(opp, 'signals') and isinstance(opp.signals, dict):
        skew = opp.signals.get("iv_skew", {}).get("value") or 0
    if skew > 5.0:
        issues.append(f"IV skew {skew:.2f} impossibly high (>5.0)")

    # Total OI should be at least 1000 for any liquid ticker
    # (near-expiry garbage has OI < 100)
    if hasattr(opp, 'signals') and isinstance(opp.signals, dict):
        opp.signals.get("oi_concentration", {})
        # Try to get total_oi from the raw signals
    # Check from the MispricingOpportunity attributes
    if hasattr(opp, 'signals') and 'total_oi' in str(opp.signals):
        pass  # Already checked via scanner
    # Use spot as proxy — if ticker has zero IV it's garbage
    if iv > 0 and iv < 0.03:
        issues.append(f"IV {iv:.4f} near zero — likely illiquid/expiring chain")

    # PCR should be between 0.01 and 50 — GOOGL had 457
    pcr = 0
    if hasattr(opp, 'signals') and isinstance(opp.signals, dict):
        pcr = opp.signals.get("pcr", {}).get("value") or 0
    if pcr > 20:
        issues.append(f"PCR {pcr:.1f} impossibly high (>20)")

    # Score of exactly 5.0 for everything means the scoring is degenerate
    # (all signals at threshold, no differentiation)
    if opp.score == 5.0:
        issues.append("Score exactly 5.0 — no signal differentiation, likely threshold artifact")

    return issues


def _llm_sanity_review(opportunities: list[dict], engine) -> list[dict]:
    """Have the local LLM review opportunities for coherence.

    Returns opportunities with an 'llm_verdict' field added.
    Only passes through opportunities the LLM considers plausible.
    """
    try:
        import requests as req

        # Check if LLM is available
        from config import settings
        props = req.get(f"{settings.LLAMACPP_BASE_URL}/props", timeout=3)
        if props.status_code != 200:
            log.warning("LLM not available for sanity review — passing all through")
            for opp in opportunities:
                opp["llm_verdict"] = "UNREVIEWED"
            return opportunities
    except Exception:
        for opp in opportunities:
            opp["llm_verdict"] = "UNREVIEWED"
        return opportunities

    reviewed = []
    # Batch all opportunities into one prompt for efficiency
    opp_summaries = []
    for i, opp in enumerate(opportunities):
        try:
            opp_summaries.append(_opportunity_summary_line(i + 1, opp))
        except Exception as exc:  # pragma: no cover - defensive; _fmt never raises
            log.warning(
                "100x digest: summary for {t} unavailable — {e}",
                t=opp.get("ticker", "?"), e=str(exc),
            )
            opp_summaries.append(f"{i + 1}. {opp.get('ticker', '?')} — summary unavailable (missing fields)")

    prompt = f"""You are a senior options strategist reviewing automated scan results.
For each opportunity below, respond with ONLY a JSON array of verdicts.
Each verdict: {{"index": N, "verdict": "PASS"|"FAIL"|"SUSPECT", "reason": "one sentence"}}

PASS = plausible trade setup worth investigating
FAIL = data quality issue, impossible values, or incoherent thesis
SUSPECT = might be real but needs manual verification

Opportunities:
{chr(10).join(opp_summaries)}

Rules:
- Max pain >30% from spot = likely stale expiry data → FAIL
- IV ATM under 5% or over 200% = garbage → FAIL
- PCR over 20 = broken data → FAIL
- Payoff estimate of exactly 1000x for everything = scoring bug → SUSPECT
- A thesis that only says "IV skew dislocated" with no context → SUSPECT
- Score of exactly 5.0 for everything = no real signal → FAIL

Respond with ONLY the JSON array, no other text."""

    try:
        resp = req.post(
            f"{settings.LLAMACPP_BASE_URL}/v1/chat/completions",
            json={
                "model": "default",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
                "max_tokens": 1000,
            },
            timeout=60,
        )
        if resp.status_code == 200:
            content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
            # Parse JSON from response
            import re
            json_match = re.search(r'\[.*\]', content, re.DOTALL)
            if json_match:
                verdicts = json.loads(json_match.group())
                verdict_map = {v["index"]: v for v in verdicts}

                for i, opp in enumerate(opportunities):
                    v = verdict_map.get(i + 1, {"verdict": "UNREVIEWED", "reason": "not in LLM response"})
                    opp["llm_verdict"] = v.get("verdict", "UNREVIEWED")
                    opp["llm_reason"] = v.get("reason", "")
                    if v.get("verdict") != "FAIL":
                        reviewed.append(opp)
                    else:
                        log.info("LLM rejected {t}: {r}", t=opp["ticker"], r=v.get("reason", ""))
                return reviewed
    except Exception as e:
        log.warning("LLM review failed: {e}", e=str(e))

    # Fallback: pass everything through unreviewed
    for opp in opportunities:
        opp["llm_verdict"] = "UNREVIEWED"
    return opportunities


# ── Selection gates (measured; see module constants) ───────────────────────
#
# Every threshold here was set from a backfill of what this digest would have
# emailed 2026-09-04..2026-11-03: 325 options_mispricing_scans rows that pass
# _sanity_check (290 distinct calls), each priced against options_snapshots
# and settled on options_daily_signals spot. Results in the commit message.

# Contract structure. The old pick (nearest expiry >= 3 days, 5-15% OTM, top
# OI) had median 14 DTE / 9.8% OTM / $85 a contract: 6.9% of 247 priced calls
# ended above entry, median return -100%, 62.8% lost the whole premium. The
# same calls on the nearest expiry >= 21 days, strike nearest 2% OTM: 21.1%
# above entry, median -60%, 18.1% total losses (n=237).
MIN_DTE_DAYS = 21
MAX_DTE_DAYS = 75          # backfill priced expiries out to 75 days
EXPIRIES_TRIED = 3         # first expiries >= MIN_DTE_DAYS searched for a quote
STRIKE_TARGET_OTM = 0.02
CALL_STRIKE_BAND = (0.98, 1.10)   # strike / spot
PUT_STRIKE_BAND = (0.90, 1.02)

# Widest bid/ask spread (as a share of mid) a named contract may have. On
# the >= 21 DTE structure, contracts quoted wider than this ended above the
# entry ask 13.6% of the time (n=125, mean -49.8%) against 29.5% (n=112,
# mean -17.0%) for tighter ones. Backfill quotes are end-of-day snapshots,
# which run wider than the intraday quotes the digest prices on.
MAX_SPREAD_PCT = 0.60

# Directions not emailed. CALL calls went the right way over 5 trading days
# 42.0% of the time (n=143; 10 days 41.7%, n=132), below a coin flip in both
# September (47.7%) and October (37.2%) and in both large caps (39.1%) and
# small caps (43.3%). 3.6% of 140 priced CALL contracts ended above entry.
# PUT calls are still sent: 58.8% at 5 days (n=102), but that edge is
# September-only (10-day 87.8% Sep vs 42.9% Oct), so it is labelled
# unproven and the track record carries the evidence.
# 2026-11-04 (owner): send CALLs again, labelled unproven, rather than
# withholding them. The gate stays available: add a direction here to
# withhold it again.
SUPPRESSED_DIRECTIONS: frozenset[str] = frozenset()

# Shown on every card of that direction until the persisted track record
# (options_digest_calls) says otherwise. Measured 2026-09-04..11-03.
DIRECTION_CAVEATS = {
    "CALL": ("UNPROVEN: in the Sep-Nov backfill CALLs went the called direction "
             "42.0% of the time over 5 days (n=143) and 84% expired worthless"),
    "PUT": ("UNPROVEN: the PUT edge was September-only (10-day direction hit "
            "87.8% in Sep vs 42.9% in Oct)"),
}

# The is_100x tier ("estimated payoff >= 100x"): 0 of 7 calls ended above
# entry, all 7 lost the premium, and the 30-100x bucket was 0 of 39. The
# payoff estimate is a formula on IV and max-pain distance, not a price.
SUPPRESS_100X = True

# A call whose signal row is older than this is not sent (the scanner also
# refuses such rows; this guards callers that hand in their own rows).
MAX_SIGNAL_AGE_DAYS = 4

# The digest runs about every 4 hours. The same contract is not re-emailed
# inside this window; with nothing new, nothing is sent.
RESEND_COOLDOWN_HOURS = 24

# Rows of the track record shown in the email (stats use a wider window).
TRACK_RECORD_ROWS = 10

VERIFY_CAP = 15

_GATE_TEXT = {
    "lottery_tier": "100x tier withheld (0/7 ended above entry in backfill)",
    "direction_suppressed": "CALL withheld (5-day direction hit 42.0%, n=143)",
    "stale_signal": f"signal row older than {MAX_SIGNAL_AGE_DAYS} days",
    "no_executable_contract": (f"no contract >= {MIN_DTE_DAYS} DTE quoted two-sided with spread "
                               f"<= {MAX_SPREAD_PCT:.0%} of mid"),
    "already_emailed": f"same contract emailed in the last {RESEND_COOLDOWN_HOURS}h",
}


def _selection_gate(opp: Any, today: date) -> str | None:
    """Return the gate that withholds ``opp`` before verification, or None."""
    if SUPPRESS_100X and bool(getattr(opp, "is_100x", False)):
        return "lottery_tier"
    if getattr(opp, "direction", None) in SUPPRESSED_DIRECTIONS:
        return "direction_suppressed"
    sig_date = getattr(opp, "signal_date", None)
    if isinstance(sig_date, date) and (today - sig_date).days > MAX_SIGNAL_AGE_DAYS:
        return "stale_signal"
    return None


# ── Call economics ─────────────────────────────────────────────────────────

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _call_economics(
    direction: str, strike: float, bid: float, ask: float, spot: float | None,
    iv: float | None, expiry: date, today: date,
) -> dict[str, Any]:
    """Breakeven, max loss, required move and IV-implied odds for a long option.

    Entry is the ask (what a buyer pays). Probabilities are lognormal,
    zero-drift, at the contract's own IV — the market's pricing, not a GRID
    forecast. Anything that needs a missing input is None, never guessed.
    """
    dte = (expiry - today).days
    breakeven = strike + ask if direction == "CALL" else strike - ask
    out: dict[str, Any] = {
        "entry": ask, "mid": (bid + ask) / 2.0, "spread_pct": (ask - bid) / ((bid + ask) / 2.0),
        "breakeven": breakeven, "max_loss": ask * 100.0, "dte": dte,
        "required_move": None, "prob_beyond_breakeven": None, "prob_itm": None,
        "invalidation": None,
    }
    if not spot or spot <= 0:
        return out
    out["required_move"] = breakeven / spot - 1.0
    if not iv or iv <= 0 or dte <= 0 or breakeven <= 0:
        return out
    t = dte / 365.0
    vol_t = iv * math.sqrt(t)

    def _p_above(level: float) -> float:
        return _norm_cdf((math.log(spot / level) - 0.5 * iv * iv * t) / vol_t)

    if direction == "CALL":
        out["prob_beyond_breakeven"] = _p_above(breakeven)
        out["prob_itm"] = _p_above(strike)
        # One-sigma, one-week adverse move at the contract IV.
        out["invalidation"] = spot * math.exp(-iv * math.sqrt(5 / 252))
    else:
        out["prob_beyond_breakeven"] = 1.0 - _p_above(breakeven)
        out["prob_itm"] = 1.0 - _p_above(strike)
        out["invalidation"] = spot * math.exp(iv * math.sqrt(5 / 252))
    return out


# ── Cross-Verification ─────────────────────────────────────────────────────

def _float_or_none(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if out != out else out


def _pick_contract(side: pd.DataFrame, spot: float, direction: str) -> dict | None:
    """The one contract the call names: strike nearest 2% OTM inside the band.

    Only rows with a positive bid, an ask at or above it and a spread no
    wider than MAX_SPREAD_PCT of mid qualify — a contract nobody is quoting
    cannot be entered, and its "price" would be invented.
    """
    if side is None or side.empty or not spot:
        return None
    lo, hi = CALL_STRIKE_BAND if direction == "CALL" else PUT_STRIKE_BAND
    target = spot * (1 + STRIKE_TARGET_OTM if direction == "CALL" else 1 - STRIKE_TARGET_OTM)
    best: tuple[float, int, dict] | None = None
    for _, row in side.iterrows():
        strike = _float_or_none(row.get("strike"))
        bid = _float_or_none(row.get("bid"))
        ask = _float_or_none(row.get("ask"))
        if strike is None or bid is None or ask is None or bid <= 0 or ask < bid:
            continue
        if (ask - bid) / ((ask + bid) / 2.0) > MAX_SPREAD_PCT:
            continue
        if not (spot * lo <= strike <= spot * hi):
            continue
        oi = int(_float_or_none(row.get("openInterest")) or 0)
        last_trade = row.get("lastTradeDate")
        cand = {
            "strike": strike, "bid": bid, "ask": ask,
            "iv": _float_or_none(row.get("impliedVolatility")),
            "oi": oi, "volume": int(_float_or_none(row.get("volume")) or 0),
            "last_trade": str(last_trade) if last_trade is not None and str(last_trade) != "NaT" else None,
        }
        key = (abs(strike - target), -oi, cand)
        if best is None or key[:2] < best[:2]:
            best = key
    return best[2] if best else None


def _verify_opportunity(ticker: str, opp: dict, engine: Engine) -> dict:
    """Price the call against the live chain and name one contract.

    Returns the verification dict: live spot (with which yfinance field it
    came from), the chosen contract with its quote time, and any issues.
    ``contract`` is None when no contract >= MIN_DTE_DAYS has a two-sided
    quote — the caller withholds such a call.
    """
    import yfinance as yf

    verification: dict[str, Any] = {
        "verified": False,
        "issues": [],
        "corrections": [],
        "bonus_data": [],
        "live_spot": None,
        "spot_source": None,
        "spot_at": None,
        "live_chain": None,
        "contract": None,
        "recommended_strikes": [],
    }

    try:
        stock = yf.Ticker(ticker)
        info = stock.info or {}

        # 1. Spot — record which field answered: previousClose is yesterday.
        live_spot = None
        for field_name in ("regularMarketPrice", "previousClose"):
            live_spot = _float_or_none(info.get(field_name))
            if live_spot:
                verification["spot_source"] = f"yfinance info.{field_name}"
                break
        if live_spot:
            verification["live_spot"] = live_spot
            verification["spot_at"] = datetime.now(timezone.utc)
            stored_spot = opp.get("spot_price") or 0
            if stored_spot and abs(live_spot - stored_spot) / stored_spot > 0.03:
                verification["issues"].append(
                    f"Spot price drift: stored={stored_spot:.2f}, live={live_spot:.2f} "
                    f"({(live_spot-stored_spot)/stored_spot:+.1%})"
                )
                verification["corrections"].append({
                    "field": "spot_price", "old": stored_spot, "new": live_spot,
                    "source": verification["spot_source"],
                })
                _update_spot_in_db(engine, ticker, live_spot)

        # 2. Chain: nearest expiry at least MIN_DTE_DAYS out. No fallback to
        #    a nearer expiry — that is the lottery-ticket structure the
        #    backfill measured at a 6.9% hit rate.
        try:
            expirations = stock.options or ()
            today = datetime.now(timezone.utc).date()
            valid_exps = [e for e in expirations
                          if (datetime.strptime(e, "%Y-%m-%d").date() - today).days >= MIN_DTE_DAYS]
            if not valid_exps:
                verification["issues"].append(f"No listed expiry >= {MIN_DTE_DAYS} days")
            else:
                expiry = valid_exps[0]
                chain = stock.option_chain(expiry)
                quote_at = datetime.now(timezone.utc)

                live_call_vol = chain.calls["volume"].sum() if "volume" in chain.calls else 0
                live_put_vol = chain.puts["volume"].sum() if "volume" in chain.puts else 0
                live_pcr = live_put_vol / live_call_vol if live_call_vol > 0 else 0

                # Scanner rows carry the raw value under "value" (the old
                # "raw_value" key never existed, so this check never ran).
                stored_pcr = _float_or_none(((opp.get("signals") or {}).get("pcr") or {}).get("value"))
                if stored_pcr and abs(live_pcr - stored_pcr) > 0.5:
                    verification["issues"].append(
                        f"PCR diverged: stored={stored_pcr:.2f}, live={live_pcr:.2f} ({expiry} chain)"
                    )

                direction = opp.get("direction", "PUT")
                if live_spot:
                    tried: list[str] = []
                    candidates = [e for e in valid_exps
                                  if (datetime.strptime(e, "%Y-%m-%d").date() - today).days
                                  <= MAX_DTE_DAYS][:EXPIRIES_TRIED]
                    for exp_str in candidates:
                        if exp_str == expiry:
                            exp_chain, exp_quote_at = chain, quote_at
                        else:
                            time.sleep(0.5)  # Rate limit yfinance
                            exp_chain = stock.option_chain(exp_str)
                            exp_quote_at = datetime.now(timezone.utc)
                        side = exp_chain.calls if direction == "CALL" else exp_chain.puts
                        tried.append(exp_str)
                        contract = _pick_contract(side, live_spot, direction)
                        if contract:
                            contract.update(expiry=exp_str, quote_at=exp_quote_at,
                                            quote_source="yfinance option_chain")
                            verification["contract"] = contract
                            verification["recommended_strikes"] = [contract]
                            break
                    if verification["contract"] is None:
                        lo, hi = CALL_STRIKE_BAND if direction == "CALL" else PUT_STRIKE_BAND
                        where = ", ".join(tried) or f"any expiry <= {MAX_DTE_DAYS}d"
                        verification["issues"].append(
                            f"No {direction.lower()} on {where} with strike {lo:.0%}-{hi:.0%} "
                            f"of spot quoted two-sided within {MAX_SPREAD_PCT:.0%} spread"
                        )

                # Unusual volume (>5x mean OI) is stored for later study; it
                # is not shown on the card.
                import numpy as np
                oi_arr = [x for x in chain.calls["openInterest"].tolist()
                          + chain.puts["openInterest"].tolist() if x and x > 0]
                if oi_arr:
                    mean_oi = np.mean(oi_arr)
                    for kind, frame in (("unusual_call_volume", chain.calls),
                                        ("unusual_put_volume", chain.puts)):
                        for _, row in frame.iterrows():
                            if row.get("volume", 0) and row["volume"] > mean_oi * 5:
                                verification["bonus_data"].append({
                                    "type": kind, "strike": float(row["strike"]),
                                    "volume": int(row["volume"]),
                                    "oi": int(row.get("openInterest", 0)),
                                    "ratio": float(row["volume"] / mean_oi),
                                })
                if verification["bonus_data"]:
                    _store_bonus_data(engine, ticker, verification["bonus_data"])

                verification["live_chain"] = {
                    "expiry": expiry,
                    "call_count": len(chain.calls),
                    "put_count": len(chain.puts),
                    "total_call_oi": int(chain.calls["openInterest"].sum()),
                    "total_put_oi": int(chain.puts["openInterest"].sum()),
                    "pcr": round(live_pcr, 3),
                }

        except Exception as e:
            verification["issues"].append(f"Chain verification failed: {str(e)[:100]}")

        critical_issues = [i for i in verification["issues"] if "drift" not in i.lower()]
        verification["verified"] = len(critical_issues) == 0

    except Exception as e:
        verification["issues"].append(f"Verification failed: {str(e)[:200]}")

    return verification


def _update_spot_in_db(engine: Engine, ticker: str, spot: float):
    """Update the latest spot price in options_daily_signals."""
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE options_daily_signals SET spot_price = :spot "
                "WHERE ticker = :t AND signal_date = CURRENT_DATE"
            ), {"spot": spot, "t": ticker})
    except Exception:
        log.warning("Failed to update spot price in DB for {t}", t=ticker, exc_info=True)


def _store_bonus_data(engine: Engine, ticker: str, bonus: list[dict]):
    """Store bonus scraped data (unusual activity) to raw_series."""
    try:
        with engine.begin() as conn:
            for item in bonus[:10]:  # Cap at 10
                series_id = f"BONUS:{ticker}:{item['type']}:{int(item['strike'])}"
                conn.execute(text(
                    "INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status) "
                    "VALUES (:sid, 1, CURRENT_DATE, :val, :payload, 'SUCCESS') "
                    "ON CONFLICT DO NOTHING"
                ), {
                    "sid": series_id,
                    "val": float(item.get("volume", 0)),
                    "payload": json.dumps(item),
                })
    except Exception as e:
        log.warning("Bonus data store failed: {e}", e=str(e))


# ── Signal descriptions (values, not labels) ───────────────────────────────

def _signal_text(name: str, sig: dict, spot: float | None) -> str | None:
    """One signal as its measured value against the scanner's rule."""
    v = sig.get("value")
    num = _float_or_none(v)
    if name == "pcr" and num is not None:
        return f"put/call volume ratio {num:.2f} (rule: >=1.80 votes CALL, <=0.40 votes PUT)"
    if name == "iv_skew" and num is not None:
        return f"OTM-put/ATM IV skew {num:.2f} (rule: >=1.60 votes CALL, <=0.85 votes PUT)"
    if name == "max_pain_div" and num is not None:
        if spot:
            return (f"max pain ${num:,.2f} vs spot ${spot:,.2f} ({num / spot - 1:+.1%}; "
                    f">=8% apart votes toward max pain)")
        return f"max pain ${num:,.2f} (spot missing)"
    if name == "term_structure" and num is not None:
        return f"IV term slope {num:+.3f} (inverted below -0.05)"
    if name == "oi_concentration" and num is not None:
        return f"{num:.1%} of open interest at one strike (flag at >=15%)"
    if name == "iv_percentile" and num is not None:
        return f"ATM IV {num:.1%}, outside its 10th-95th percentile band (252d)"
    if name == "gamma_squeeze":
        return "OI concentration >=10% with spot >=5% from max pain and OI >=50k"
    if name == "vol_surface":
        meta = v if isinstance(v, dict) else {}
        bits = []
        if meta.get("butterfly_violations"):
            bits.append(f"{meta['butterfly_violations']} butterfly")
        if meta.get("calendar_violations"):
            bits.append(f"{meta['calendar_violations']} calendar")
        fs = meta.get("front_skew")
        tail = f", front 25-delta skew {fs:+.3f}" if isinstance(fs, (int, float)) else ""
        # Measured 2026-09-04..11-03: violations on 99.85% of scanned rows
        # (median 21), so this fires on nearly every name.
        return (f"SVI surface: {' / '.join(bits) or 'no'} arbitrage violations{tail} "
                f"(fires on ~all names; low information)")
    if name in ("vanna", "charm") and num is not None:
        return f"dealer {name} exposure {num:,.0f}"
    return None


def _signal_lines(signals: dict, direction: str, spot: float | None) -> tuple[list[str], list[str]]:
    """Top 3 signals voting for ``direction`` (with values) and the names voting against."""
    active = [(n, s) for n, s in (signals or {}).items()
              if isinstance(s, dict) and (_float_or_none(s.get("score")) or 0) >= 3]
    active.sort(key=lambda x: -(_float_or_none(x[1].get("score")) or 0))
    support, against = [], []
    for name, sig in active:
        d = sig.get("direction") or ""
        if d and d != direction:
            against.append(name)
            continue
        if len(support) >= 3:
            continue
        desc = _signal_text(name, sig, spot)
        if desc:
            vote = f"votes {d}" if d else "no direction"
            support.append(f"{desc} — score {_fmt(_float_or_none(sig.get('score')), '.1f')}/10, {vote}")
    return support, against


# ── Catalyst ──────────────────────────────────────────────────────────────

def _catalyst_before_expiry(engine: Engine, ticker: str, today: date, expiry: date) -> dict:
    """Nearest ticker-specific earnings/clinical event in [today, expiry]."""
    try:
        from intelligence.catalyst_aggregator import (
            CATALYST_CLINICAL,
            CATALYST_EARNINGS,
            events_for_window,
        )
        events = events_for_window(
            engine, start=today, end=expiry, ticker=ticker,
            event_types=[CATALYST_EARNINGS, CATALYST_CLINICAL],
        )
    except Exception as exc:
        return {"status": "unavailable", "detail": str(exc)[:120]}
    events = [e for e in events if e.ticker]
    if not events:
        return {"status": "none_on_file",
                "detail": "earnings_calendar, earnings_events, catalyst_calendar checked"}
    e = events[0]
    return {"status": "found", "type": e.event_type, "date": e.event_date.isoformat(),
            "source": e.source, "notes": e.notes}


# ── Ledger (options_digest_calls) ──────────────────────────────────────────

def _load_recent_calls(engine: Engine, since: datetime) -> list[dict]:
    """Calls emailed since ``since``. Raises if the ledger cannot be read."""
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT ticker, direction, expiry, strike, emailed_at "
            "FROM options_digest_calls WHERE emailed_at >= :since "
            "ORDER BY emailed_at DESC LIMIT 2000"
        ), {"since": since}).fetchall()
    return [{"ticker": r[0], "direction": r[1], "expiry": r[2], "strike": float(r[3]),
             "emailed_at": r[4]} for r in rows]


def _contract_key(ticker: str, direction: str, expiry: Any, strike: float) -> tuple:
    exp = expiry.isoformat() if isinstance(expiry, date) else str(expiry)
    return (ticker, direction, exp, round(float(strike), 4))


def _persist_calls(engine: Engine, run_id: str, calls: list[dict]) -> int:
    """Insert every emailed call. Raises on failure: no ledger row, no email."""
    if not calls:
        return 0
    with engine.begin() as conn:
        for c in calls:
            k, econ = c["contract"], c["economics"]
            conn.execute(text(
                "INSERT INTO options_digest_calls ("
                "digest_run_id, ticker, direction, expiry, strike, bid, ask, quote_at, "
                "quote_source, contract_iv, contract_oi, underlying_spot, spot_source, "
                "breakeven, prob_beyond_breakeven, invalidation_level, score, signal_date, "
                "signals, catalyst, llm_verdict, provenance"
                ") VALUES ("
                ":run, :ticker, :dir, :expiry, :strike, :bid, :ask, :quote_at, "
                ":quote_source, :iv, :oi, :spot, :spot_source, "
                ":be, :prob, :inval, :score, :sig_date, "
                "CAST(:signals AS JSONB), CAST(:catalyst AS JSONB), :llm, CAST(:prov AS JSONB))"
            ), {
                "run": run_id, "ticker": c["ticker"], "dir": c["direction"],
                "expiry": k["expiry"], "strike": k["strike"], "bid": k["bid"], "ask": k["ask"],
                "quote_at": k["quote_at"], "quote_source": k["quote_source"],
                "iv": k.get("iv"), "oi": k.get("oi"),
                "spot": c["spot"], "spot_source": c["spot_source"],
                "be": econ["breakeven"], "prob": econ["prob_beyond_breakeven"],
                "inval": econ["invalidation"], "score": c["score"],
                "sig_date": c.get("signal_date"),
                "signals": json.dumps(c.get("signals") or {}, default=str),
                "catalyst": json.dumps(c.get("catalyst") or {}, default=str),
                "llm": c.get("llm_verdict"),
                "prov": json.dumps(c.get("provenance") or {}, default=str),
            })
    return len(calls)


def _load_track_record(engine: Engine, as_of: date) -> dict:
    from trading.options_tracker import load_digest_track_record
    return load_digest_track_record(engine, as_of=as_of,
                                    emailed_before=datetime.now(timezone.utc))


# ── Email Formatting ────────────────────────────────────────────────────────

_STRIKE_CSS = """
.call-line { font-size:16px; font-weight:800; color:#E8F0F8; font-family:monospace; margin:4px 0 10px; }
.kv { width:100%; border-collapse:collapse; margin:6px 0; }
.kv td { padding:4px 4px; font-size:13px; color:#C8D8E8; border-bottom:1px solid #0D1520; vertical-align:top; }
.kv td.k { color:#5A7A96; width:34%; font-size:11px; text-transform:uppercase; letter-spacing:0.5px; }
.kv td.v { font-family:monospace; }
.tr-table { width:100%; border-collapse:collapse; margin:8px 0; }
.tr-table th { text-align:left; padding:4px; font-size:10px; color:#5A7A96; border-bottom:1px solid #1A2A3A;
  text-transform:uppercase; }
.tr-table td { padding:4px; font-size:12px; color:#C8D8E8; border-bottom:1px solid #0D1520; font-family:monospace; }
.win { color:#22C55E; } .loss { color:#EF4444; }
"""


def _esc(value: Any) -> str:
    return _html.escape(str(value), quote=False)


def _ts(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%MZ")
    return MISSING if value is None else str(value)


def _plain(html_text: str) -> str:
    """Readable plain-text part from a card body (tags dropped, rows kept)."""
    t = re.sub(r"</(tr|div|p|li)>|<br\s*/?>", "\n", html_text)
    t = re.sub(r"</t[dh]>\s*<t[dh][^>]*>", "  ", t)
    t = re.sub(r"<[^>]+>", "", t)
    t = _html.unescape(t)
    return "\n".join(line.strip() for line in t.splitlines() if line.strip())


def _kv_table(rows: list[tuple[str, str]]) -> str:
    body = "".join(f'<tr><td class="k">{_esc(k)}</td><td class="v">{v}</td></tr>' for k, v in rows)
    return f'<table class="kv">{body}</table>'


def _format_opportunity_card(opp: dict, verification: dict, index: int) -> dict:
    """One call: the exact contract, its quote, the math, and why — with dates.

    Every number is either measured (with its source and time) or rendered
    as N/A. Nothing is filled in.
    """
    ticker = opp["ticker"]
    direction = opp["direction"]
    today = opp.get("as_of") or datetime.now(timezone.utc).date()
    spot = verification.get("live_spot") or opp.get("spot_price")
    spot_source = verification.get("spot_source") or (
        "options_daily_signals.spot_price" if opp.get("spot_price") else None)
    contract = verification.get("contract")
    econ = opp.get("economics")
    if contract and econ is None:
        exp = datetime.strptime(contract["expiry"], "%Y-%m-%d").date()
        econ = _call_economics(direction, contract["strike"], contract["bid"], contract["ask"],
                               spot, contract.get("iv"), exp, today)

    rows: list[tuple[str, str]] = []
    if contract:
        call_line = (f"BUY {ticker} {contract['expiry']} ${contract['strike']:,.2f} "
                     f"{direction} @ ${contract['ask']:.2f} (ask)")
        rows.append(("Quote", f"bid ${contract['bid']:.2f} / ask ${contract['ask']:.2f} / "
                              f"mid ${econ['mid']:.2f} (spread {econ['spread_pct']:.0%} of mid)"))
        rows.append(("Quote time", f"{_ts(contract.get('quote_at'))} via {_esc(contract.get('quote_source'))}; "
                                   f"last trade {_esc(contract.get('last_trade') or MISSING)}"))
        rows.append(("Contract IV / OI", f"{_fmt(contract.get('iv'), '.1%')} / "
                                         f"{_fmt(contract.get('oi'), ',')} open, "
                                         f"{_fmt(contract.get('volume'), ',')} traded today"))
        rows.append(("Breakeven at expiry",
                     f"${econ['breakeven']:,.2f} — needs {_fmt(econ['required_move'], '+.1%')} "
                     f"from spot in {econ['dte']} days"))
        rows.append(("Max loss", f"${econ['max_loss']:,.0f} per contract (the premium)"))
        rows.append(("Market-implied odds",
                     f"{_fmt(econ['prob_beyond_breakeven'], '.0%')} of finishing past breakeven, "
                     f"{_fmt(econ['prob_itm'], '.0%')} in the money "
                     f"(lognormal at contract IV — the market's price, not a GRID forecast)"))
        rows.append(("Invalidation",
                     (f"exit if {ticker} trades {'below' if direction == 'CALL' else 'above'} "
                      f"${econ['invalidation']:,.2f} (one-sigma one-week move against the call "
                      f"at contract IV — a rule, not a backtested level)")
                     if econ.get("invalidation") else MISSING))
    else:
        call_line = f"{ticker} {direction} — no executable contract quoted"
        rows.append(("Contract", MISSING))

    rows.append(("Underlying", f"${_fmt(spot, ',.2f')} ({_esc(spot_source or MISSING)}, "
                               f"{_ts(verification.get('spot_at'))})"))

    cat = opp.get("catalyst") or {}
    if cat.get("status") == "found":
        cat_txt = f"{_esc(cat['type'])} {cat['date']} ({_esc(cat.get('source'))}) {_esc(cat.get('notes') or '')}"
    elif cat.get("status") == "none_on_file":
        cat_txt = f"none on file before expiry ({_esc(cat.get('detail'))})"
    else:
        cat_txt = f"{MISSING} (catalyst lookup unavailable)"
    rows.append(("Catalyst before expiry", cat_txt))

    support, against = _signal_lines(opp.get("signals") or {}, direction, spot)
    why = "<br>".join(_esc(s) for s in support) or MISSING
    if against:
        why += f"<br>Against: {_esc(', '.join(against))}"
    rows.append(("Signals", why))

    sig_date = opp.get("signal_date")
    if isinstance(sig_date, date):
        age = (today - sig_date).days
        sig_txt = f"options_daily_signals row {sig_date.isoformat()} ({age}d old)"
        if opp.get("signal_captured_at") is not None:
            sig_txt += f", captured {_ts(opp['signal_captured_at'])}"
    else:
        sig_txt = MISSING
    rows.append(("Signal data as of", sig_txt))
    rows.append(("Composite score", f"{_fmt(opp.get('score'), '.2f')}/10 "
                                    f"(no measured relation to outcome — see Track Record)"))
    caveat = DIRECTION_CAVEATS.get(direction)
    if caveat:
        rows.insert(0, ("Track record", caveat))

    llm_verdict = opp.get("llm_verdict", "UNREVIEWED")
    if llm_verdict not in ("PASS", "UNREVIEWED"):
        rows.append(("LLM review", f"{_esc(llm_verdict)}: {_esc(opp.get('llm_reason') or '')}"))
    if opp.get("previous_direction"):
        rows.append(("Note", _esc(opp["previous_direction"])))
    for issue in verification.get("issues") or []:
        rows.append(("Data issue", _esc(issue)))

    body = f'<div class="call-line">{_esc(call_line)}</div>{_kv_table(rows)}'
    label = " (UNPROVEN)" if direction in DIRECTION_CAVEATS else ""
    return {"title": f"#{index} — {ticker} {direction}S{label}", "body": body,
            "accent": "red" if direction == "PUT" else "green"}


def _format_track_record(record: dict | None, error: str | None = None) -> dict:
    """Last calls with their outcome so far, and the running hit rate."""
    if error is not None:
        return {"title": "Track Record", "accent": "amber",
                "body": f"Track record unavailable: {_esc(error)}"}
    calls = (record or {}).get("calls") or []
    if not calls:
        return {"title": "Track Record", "accent": "amber",
                "body": "No emailed calls on record yet — the call ledger "
                        "(options_digest_calls) starts with this digest."}
    hit = record.get("settled_hit_rate")
    head = (f"Last {len(calls)} calls: {record['settled']} settled, "
            f"{record['settled_wins']} above entry"
            + (f" (hit rate {hit:.0%}, median {_fmt(record.get('settled_median_return'), '+.0%')})"
               if hit is not None else "")
            + f"; {record['open']} open (median mark {_fmt(record.get('open_median_return'), '+.0%')})"
            + (f"; {record['unpriced']} unpriced" if record.get("unpriced") else "")
            + ". Settled = intrinsic at the options_daily_signals spot on or just before expiry "
              "(intraday capture); open = latest snapshot mid vs entry ask.")
    trs = ""
    for c in calls[:TRACK_RECORD_ROWS]:
        ret = c.get("return")
        cls = "win" if ret is not None and ret > 0 else "loss" if ret is not None else ""
        emailed = c["emailed_at"].date().isoformat() if isinstance(c.get("emailed_at"), datetime) else str(c.get("emailed_at"))
        exp = c["expiry"].isoformat() if isinstance(c.get("expiry"), date) else str(c.get("expiry"))
        trs += (f"<tr><td>{emailed}</td><td>{_esc(c['ticker'])} {exp} ${c['strike']:,.2f} "
                f"{c['direction']}</td><td>${_fmt(c.get('entry'), '.2f')}</td>"
                f"<td>{c['status']}</td><td class=\"{cls}\">{_fmt(ret, '+.0%')}</td></tr>")
    body = (f"<div>{_esc(head)}</div><table class=\"tr-table\"><thead><tr><th>Emailed</th>"
            f"<th>Contract</th><th>Entry</th><th>Status</th><th>Return</th></tr></thead>"
            f"<tbody>{trs}</tbody></table>")
    return {"title": "Track Record", "body": body, "accent": "purple"}


def _format_summary_section(opportunities: list[dict], verifications: list[dict],
                            gated: dict[str, list[str]] | None = None) -> dict:
    """What was sent, and what was withheld and why."""
    lines = [f"{len(opportunities)} call(s) below, each with an executable quote."]
    for reason, tickers in (gated or {}).items():
        if tickers:
            lines.append(f"Withheld {len(tickers)} — {_GATE_TEXT.get(reason, reason)}: "
                         f"{', '.join(sorted(set(tickers)))}")
    body = "<br>".join(_esc(x) for x in lines)
    return {"title": "Scan Summary", "body": body}


# ── Main Digest Function ───────────────────────────────────────────────────

_warned: set[tuple[str, str]] = set()


def _warn_once(key: tuple[str, str], message: str, **kw: Any) -> None:
    """Warn once per process per key; repeats go to INFO (same data every 4h)."""
    if key in _warned:
        log.info(message, **kw)
        return
    _warned.add(key)
    log.warning(message, **kw)


def run_100x_digest(force: bool = False) -> dict[str, Any]:
    """Scan, gate, price, record and email options calls.

    1. Scanner rows sorted by score; ``_sanity_check`` drops garbage data.
    2. ``_selection_gate`` withholds the measured losers (100x tier, CALLs,
       stale signal rows) before any network call.
    3. Up to 15 are priced on the live chain; a call with no quoted contract
       >= MIN_DTE_DAYS is withheld.
    4. LLM review (FAIL drops).
    5. Contracts emailed in the last RESEND_COOLDOWN_HOURS are withheld.
    6. Each remaining call is written to options_digest_calls, then emailed
       with the track record of earlier calls. No ledger, no email.

    Returns summary dict.
    """
    engine = _get_engine()
    now = datetime.now(timezone.utc)
    today = now.date()  # UTC, like grid-svr's clock and the ledger timestamps

    log.info("100x Digest: Running options scanner...")
    from discovery.options_scanner import OptionsScanner
    scanner = OptionsScanner(db_engine=engine)

    try:
        opportunities = scanner.scan_all()
    except Exception as e:
        log.error("Scanner failed: {e}", e=str(e))
        opportunities = []

    all_opps = sorted(opportunities, key=lambda o: -o.score)
    log.info("100x Digest: {total} scanned, {hx} flagged 100x",
             total=len(opportunities), hx=sum(1 for o in opportunities if o.is_100x))

    if not all_opps and not force:
        log.info("No notable opportunities — skipping digest")
        return {"sent": False, "reason": "no_opportunities", "scanned": len(opportunities)}

    # Step 1: data-quality gate.
    clean_opps, rejected = [], []
    for opp in all_opps:
        issues = _sanity_check(opp)
        if issues:
            rejected.append({"ticker": opp.ticker, "issues": issues})
            _warn_once((opp.ticker, issues[0].split(" ")[0]),
                       "Rejected {t}: {i}", t=opp.ticker, i=issues)
        else:
            clean_opps.append(opp)
    log.info("Sanity check: {c} passed, {r} rejected", c=len(clean_opps), r=len(rejected))

    # Step 2: measured selection gates.
    gated: dict[str, list[str]] = {}
    selected = []
    for opp in clean_opps:
        reason = _selection_gate(opp, today)
        if reason:
            gated.setdefault(reason, []).append(opp.ticker)
        else:
            selected.append(opp)

    # Step 3: price each on the live chain.
    verified_opps, verifications = [], []
    for opp in selected[:VERIFY_CAP]:
        log.info("Verifying {t} {d}...", t=opp.ticker, d=opp.direction)
        opp_dict = {
            "ticker": opp.ticker,
            "direction": opp.direction,
            "score": opp.score,
            "payoff_multiple": opp.estimated_payoff_multiple,
            "thesis": opp.thesis,
            "spot_price": opp.spot_price,
            "iv_atm": opp.iv_atm,
            "signals": opp.signals,
            "strikes": opp.strikes,
            "expiry": opp.expiry,
            "confidence": opp.confidence,
            "is_100x": opp.is_100x,
            "signal_date": getattr(opp, "signal_date", None),
            "signal_captured_at": getattr(opp, "signal_captured_at", None),
            "as_of": today,
        }
        v = _verify_opportunity(opp.ticker, opp_dict, engine)
        time.sleep(0.5)  # Rate limit yfinance
        if not v.get("contract"):
            gated.setdefault("no_executable_contract", []).append(opp.ticker)
            continue
        verified_opps.append(opp_dict)
        verifications.append(v)

    # Step 4: LLM review.
    log.info("100x Digest: LLM reviewing {n} opportunities...", n=len(verified_opps))
    reviewed_opps = _llm_sanity_review(verified_opps, engine)
    reviewed_ids = {id(o) for o in reviewed_opps}
    pairs = [(o, v) for o, v in zip(verified_opps, verifications) if id(o) in reviewed_ids]
    log.info("LLM review: {n} passed", n=len(pairs))

    # Step 5: the ledger — re-send gate and direction-flip note.
    try:
        recent = _load_recent_calls(engine, now - timedelta(days=30))
    except Exception as exc:
        _warn_once(("ledger", "read"), "100x digest: call ledger unreadable, not sending — {e}",
                   e=str(exc)[:200])
        return {"sent": False, "reason": "call_ledger_unavailable", "scanned": len(opportunities)}
    cutoff = now - timedelta(hours=RESEND_COOLDOWN_HOURS)
    recent_keys = {
        _contract_key(c["ticker"], c["direction"], c["expiry"], c["strike"])
        for c in recent
        if c["emailed_at"] is not None and c["emailed_at"] >= cutoff
    }
    last_dir: dict[str, dict] = {}
    for c in recent:  # newest first
        last_dir.setdefault(c["ticker"], c)

    calls: list[tuple[dict, dict]] = []
    for opp, v in pairs:
        k = v["contract"]
        if _contract_key(opp["ticker"], opp["direction"], k["expiry"], k["strike"]) in recent_keys:
            gated.setdefault("already_emailed", []).append(opp["ticker"])
            continue
        prev = last_dir.get(opp["ticker"])
        if prev and prev["direction"] != opp["direction"]:
            opp["previous_direction"] = (
                f"direction flipped: {prev['direction']} emailed {_ts(prev['emailed_at'])}")
        exp = datetime.strptime(k["expiry"], "%Y-%m-%d").date()
        spot = v.get("live_spot") or opp.get("spot_price")
        opp["spot_used"] = spot
        opp["spot_source_used"] = v.get("spot_source") or (
            "options_daily_signals.spot_price (signal row)" if spot else None)
        opp["economics"] = _call_economics(opp["direction"], k["strike"], k["bid"], k["ask"],
                                           spot, k.get("iv"), exp, today)
        opp["catalyst"] = _catalyst_before_expiry(engine, opp["ticker"], today, exp)
        calls.append((opp, v))

    log.info("100x Digest: {n} call(s) to send; withheld {g}",
             n=len(calls), g={k: len(v) for k, v in gated.items()})

    if not calls and not force:
        return {"sent": False, "reason": "no_new_calls", "scanned": len(opportunities),
                "rejected_sanity": len(rejected), "gated": {k: len(v) for k, v in gated.items()}}

    # Step 6: record, then send.
    try:
        track = _load_track_record(engine, today)
        track_error = None
    except Exception as exc:
        track, track_error = None, str(exc)[:200]

    run_id = uuid.uuid4().hex
    ledger_rows = []
    for opp, v in calls:
        ledger_rows.append({
            "ticker": opp["ticker"], "direction": opp["direction"], "contract": v["contract"],
            "economics": opp["economics"], "spot": opp.get("spot_used"),
            "spot_source": opp.get("spot_source_used"), "score": float(opp.get("score") or 0),
            "signal_date": opp.get("signal_date"),
            "signals": {n: {"score": s.get("score"), "direction": s.get("direction", ""),
                            "value": s.get("value")}
                        for n, s in (opp.get("signals") or {}).items() if isinstance(s, dict)},
            "catalyst": opp.get("catalyst"), "llm_verdict": opp.get("llm_verdict"),
            "provenance": {
                "scanner": "discovery.options_scanner.OptionsScanner.scan_all",
                "signal_source": "options_daily_signals",
                "signal_captured_at": opp.get("signal_captured_at"),
                "spot_at": v.get("spot_at"),
                "gates": {"min_dte": MIN_DTE_DAYS, "suppressed": sorted(SUPPRESSED_DIRECTIONS),
                          "suppress_100x": SUPPRESS_100X},
            },
        })
    try:
        _persist_calls(engine, run_id, ledger_rows)
    except Exception as exc:
        _warn_once(("ledger", "write"), "100x digest: call ledger write failed, not sending — {e}",
                   e=str(exc)[:200])
        return {"sent": False, "reason": "call_ledger_write_failed", "scanned": len(opportunities)}

    sections = [_format_summary_section([o for o, _ in calls], [v for _, v in calls], gated),
                _format_track_record(track, track_error)]

    calls.sort(key=lambda x: -float(x[0].get("score") or 0))
    for i, (opp, v) in enumerate(calls, 1):
        try:
            sections.append(_format_opportunity_card(opp, v, i))
        except Exception as exc:
            ticker = opp.get("ticker", "?")
            log.warning("100x digest: card for {t} not rendered — {e}", t=ticker, e=str(exc))
            sections.append(_section_text(
                f"#{i} — {ticker} {opp.get('direction', '?')}S",
                f"Card not rendered (missing/invalid fields): {_esc(exc)}",
                accent="amber",
            ))

    names = ", ".join(f"{o['ticker']} {o['direction']}" for o, _ in calls) or "none"
    subject = f"GRID Options — {len(calls)} call{'s' if len(calls) != 1 else ''}: {names}"

    html = _render_html(subject, sections)
    html = html.replace("</style>", _STRIKE_CSS + "</style>")
    plain = "\n\n".join(f"[{s['title']}]\n{_plain(s.get('body', ''))}" for s in sections)

    _send_in_thread(subject, html, plain)

    result = {
        "sent": True,
        "run_id": run_id,
        "total_scanned": len(opportunities),
        "opportunities": len(calls),
        "100x_count": 0,
        "rejected_sanity": len(rejected),
        "gated": {k: len(v) for k, v in gated.items()},
        "verified": sum(1 for _, v in calls if v.get("verified")),
    }
    log.info("100x Digest sent: {r}", r=result)
    return result


# ── Scheduler Integration ───────────────────────────────────────────────────

def schedule_100x_digest(interval_hours: int = 4):
    """Run the 100x digest on a recurring schedule.

    Called from hermes_operator.py or standalone.
    """
    import threading

    def _loop():
        while True:
            try:
                log.info("100x Digest: Starting scheduled scan...")
                result = run_100x_digest()
                log.info("100x Digest result: {r}", r=result)
            except Exception as e:
                log.error("100x Digest failed: {e}", e=str(e))
            time.sleep(interval_hours * 3600)

    t = threading.Thread(target=_loop, daemon=True, name="100x-digest")
    t.start()
    log.info("100x Digest scheduler started — every {h}h", h=interval_hours)
    return t


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true", help="Send even if no opportunities")
    parser.add_argument("--schedule", type=int, default=0, help="Run on schedule (hours)")
    args = parser.parse_args()

    if args.schedule:
        schedule_100x_digest(args.schedule)
        import signal
        signal.pause()
    else:
        result = run_100x_digest(force=args.force)
        print(json.dumps(result, indent=2))
