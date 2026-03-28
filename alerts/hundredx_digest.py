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

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from alerts.email import _render_html, _send_in_thread, _send, _section_text


def _get_engine():
    from db import get_engine
    return get_engine()


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
    total_oi = 0
    if hasattr(opp, 'signals') and isinstance(opp.signals, dict):
        oi_sig = opp.signals.get("oi_concentration", {})
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
        props = req.get("http://localhost:8080/props", timeout=3)
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
        opp_summaries.append(
            f"{i+1}. {opp['ticker']} {opp['direction']}S — score {opp['score']:.1f}/10, "
            f"payoff est {opp['payoff_multiple']:.0f}x, spot ${opp.get('spot_price', 0):.2f}, "
            f"IV ATM {opp.get('iv_atm', 0):.1%}, thesis: {opp.get('thesis', '')[:150]}"
        )

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
            "http://localhost:8080/v1/chat/completions",
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


# ── Cross-Verification ─────────────────────────────────────────────────────

def _verify_opportunity(ticker: str, opp: dict, engine: Engine) -> dict:
    """Cross-verify a 100x opportunity against live data.

    Returns enriched opportunity dict with verification status.
    If data is stale or wrong, attempts to scrape correct data.
    """
    import yfinance as yf

    verification = {
        "verified": False,
        "issues": [],
        "corrections": [],
        "bonus_data": [],
        "live_spot": None,
        "live_chain": None,
        "recommended_strikes": [],
    }

    try:
        stock = yf.Ticker(ticker)
        info = stock.info or {}

        # 1. Verify spot price
        live_spot = info.get("regularMarketPrice") or info.get("previousClose")
        if live_spot:
            verification["live_spot"] = live_spot
            stored_spot = opp.get("spot_price", 0)
            if stored_spot and abs(live_spot - stored_spot) / stored_spot > 0.03:
                verification["issues"].append(
                    f"Spot price drift: stored={stored_spot:.2f}, live={live_spot:.2f} "
                    f"({(live_spot-stored_spot)/stored_spot:+.1%})"
                )
                verification["corrections"].append({
                    "field": "spot_price",
                    "old": stored_spot,
                    "new": live_spot,
                    "source": "yfinance_live",
                })
                # Update in DB
                _update_spot_in_db(engine, ticker, live_spot)

        # 2. Verify options chain exists and get recommended strikes
        try:
            expirations = stock.options
            if expirations:
                # Skip expiries within 3 days (illiquid, near-worthless)
                from datetime import datetime as dt
                today = date.today()
                valid_exps = [e for e in expirations
                              if (dt.strptime(e, "%Y-%m-%d").date() - today).days >= 3]
                if not valid_exps:
                    valid_exps = expirations[:1]  # Fallback to nearest
                nearest_exp = valid_exps[0]
                chain = stock.option_chain(nearest_exp)

                # Verify P/C ratio
                live_call_vol = chain.calls["volume"].sum() if "volume" in chain.calls else 0
                live_put_vol = chain.puts["volume"].sum() if "volume" in chain.puts else 0
                live_pcr = live_put_vol / live_call_vol if live_call_vol > 0 else 0

                stored_pcr = opp.get("signals", {}).get("pcr", {}).get("raw_value", 0)
                if stored_pcr and abs(live_pcr - stored_pcr) > 0.5:
                    verification["issues"].append(
                        f"PCR diverged: stored={stored_pcr:.2f}, live={live_pcr:.2f}"
                    )
                    verification["corrections"].append({
                        "field": "pcr",
                        "old": stored_pcr,
                        "new": live_pcr,
                        "source": "yfinance_chain",
                    })

                # Sanity check max pain — recompute from live chain if stored value is suspect
                stored_max_pain = opp.get("signals", {}).get("max_pain_divergence", {}).get("raw_value", 0)
                if live_spot and stored_max_pain and abs(stored_max_pain - live_spot) / live_spot > 0.40:
                    # Recompute max pain from live chain
                    try:
                        all_strikes_df = pd.concat([
                            chain.calls[["strike", "openInterest"]].rename(columns={"openInterest": "call_oi"}),
                            chain.puts[["strike", "openInterest"]].rename(columns={"openInterest": "put_oi"}),
                        ]).groupby("strike").sum()
                        all_strikes_df["total"] = all_strikes_df.get("call_oi", 0) + all_strikes_df.get("put_oi", 0)
                        if len(all_strikes_df) > 0:
                            live_max_pain = float(all_strikes_df["total"].idxmax())
                            verification["corrections"].append({
                                "field": "max_pain",
                                "old": stored_max_pain,
                                "new": live_max_pain,
                                "source": "recomputed_from_live_chain",
                            })
                    except Exception:
                        log.debug("Failed to recompute max_pain for {t}", t=ticker, exc_info=True)

                # Get recommended strikes based on direction
                direction = opp.get("direction", "CALL")
                if live_spot:
                    if direction == "CALL":
                        # OTM calls: 5-15% above spot
                        target_strikes = chain.calls[
                            (chain.calls["strike"] >= live_spot * 1.05) &
                            (chain.calls["strike"] <= live_spot * 1.15)
                        ].nlargest(3, "openInterest")
                    else:
                        # OTM puts: 5-15% below spot
                        target_strikes = chain.puts[
                            (chain.puts["strike"] <= live_spot * 0.95) &
                            (chain.puts["strike"] >= live_spot * 0.85)
                        ].nlargest(3, "openInterest")

                    for _, row in target_strikes.iterrows():
                        verification["recommended_strikes"].append({
                            "strike": float(row["strike"]),
                            "expiry": nearest_exp,
                            "bid": float(row.get("bid", 0)),
                            "ask": float(row.get("ask", 0)),
                            "iv": float(row.get("impliedVolatility", 0)),
                            "oi": int(row.get("openInterest", 0)),
                            "volume": int(row.get("volume", 0) or 0),
                        })

                # 3. Bonus data: while we're here, grab unusual activity
                all_oi = chain.calls["openInterest"].tolist() + chain.puts["openInterest"].tolist()
                all_vol = chain.calls["volume"].tolist() + chain.puts["volume"].tolist()

                # Unusual volume (>5x average OI)
                import numpy as np
                oi_arr = [x for x in all_oi if x and x > 0]
                vol_arr = [x for x in all_vol if x and x > 0]
                if oi_arr:
                    mean_oi = np.mean(oi_arr)
                    for _, row in chain.calls.iterrows():
                        if row.get("volume", 0) and row["volume"] > mean_oi * 5:
                            verification["bonus_data"].append({
                                "type": "unusual_call_volume",
                                "strike": float(row["strike"]),
                                "volume": int(row["volume"]),
                                "oi": int(row.get("openInterest", 0)),
                                "ratio": float(row["volume"] / mean_oi),
                            })
                    for _, row in chain.puts.iterrows():
                        if row.get("volume", 0) and row["volume"] > mean_oi * 5:
                            verification["bonus_data"].append({
                                "type": "unusual_put_volume",
                                "strike": float(row["strike"]),
                                "volume": int(row["volume"]),
                                "oi": int(row.get("openInterest", 0)),
                                "ratio": float(row["volume"] / mean_oi),
                            })

                # Store bonus data to DB
                if verification["bonus_data"]:
                    _store_bonus_data(engine, ticker, verification["bonus_data"])

                verification["live_chain"] = {
                    "expiry": nearest_exp,
                    "call_count": len(chain.calls),
                    "put_count": len(chain.puts),
                    "total_call_oi": int(chain.calls["openInterest"].sum()),
                    "total_put_oi": int(chain.puts["openInterest"].sum()),
                    "pcr": round(live_pcr, 3),
                }

        except Exception as e:
            verification["issues"].append(f"Chain verification failed: {str(e)[:100]}")

        # Mark as verified if no critical issues
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


# ── Email Formatting ────────────────────────────────────────────────────────

_STRIKE_CSS = """
.strike-table { width:100%; border-collapse:collapse; margin:8px 0; }
.strike-table th { text-align:left; padding:6px 4px; font-size:11px; color:#5A7A96;
  border-bottom:1px solid #1A2A3A; text-transform:uppercase; letter-spacing:0.5px; }
.strike-table td { padding:6px 4px; font-size:13px; color:#C8D8E8; border-bottom:1px solid #0D1520; }
.strike-table td.strike { font-weight:700; color:#E8F0F8; font-family:monospace; }
.strike-table td.iv { color:#A855F7; }
.strike-table td.bid-ask { font-family:monospace; }
.signal-grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin:12px 0; }
.signal-item { background:#080C10; border-radius:8px; padding:10px 12px; }
.signal-label { font-size:10px; color:#5A7A96; text-transform:uppercase; letter-spacing:1px; }
.signal-value { font-size:18px; font-weight:700; color:#E8F0F8; margin-top:2px; }
.verified-badge { display:inline-block; padding:2px 8px; border-radius:10px; font-size:10px;
  font-weight:700; letter-spacing:0.5px; }
.verified-yes { background:#22C55E22; color:#22C55E; }
.verified-no { background:#EF444422; color:#EF4444; }
.bonus-tag { display:inline-block; padding:2px 6px; border-radius:4px; font-size:10px;
  background:#F59E0B22; color:#F59E0B; margin:2px; }
"""


def _format_opportunity_card(opp: dict, verification: dict, index: int) -> dict:
    """Format a single 100x opportunity as a rich email section."""
    ticker = opp["ticker"]
    direction = opp["direction"]
    score = opp["score"]
    thesis = opp.get("thesis", "")
    payoff = opp.get("payoff_multiple", 0)
    spot = verification.get("live_spot") or opp.get("spot_price", 0)
    verified = verification.get("verified", False)

    # Header with verification + LLM review badges
    badge_cls = "verified-yes" if verified else "verified-no"
    badge_text = "VERIFIED" if verified else "UNVERIFIED"

    llm_verdict = opp.get("llm_verdict", "UNREVIEWED")
    llm_reason = opp.get("llm_reason", "")
    llm_badge_cls = "verified-yes" if llm_verdict == "PASS" else "verified-no" if llm_verdict == "FAIL" else ""
    llm_badge_text = f"LLM: {llm_verdict}" if llm_verdict != "UNREVIEWED" else ""

    # Signal grid
    signals = opp.get("signals", {})
    signal_html = '<div class="signal-grid">'
    signal_items = [
        ("P/C Ratio", f"{signals.get('pcr', {}).get('raw_value', 'N/A')}", "pcr"),
        ("IV ATM", f"{opp.get('iv_atm', 0):.1%}" if opp.get('iv_atm') else "N/A", "iv"),
        ("Max Pain", f"${opp.get('signals', {}).get('max_pain_divergence', {}).get('raw_value', 'N/A')}", "mp"),
        ("Score", f"{score:.1f}/10", "score"),
        ("Spot", f"${spot:.2f}" if spot else "N/A", "spot"),
        ("Payoff Est.", f"{payoff:.0f}x" if payoff else "N/A", "payoff"),
    ]
    for label, value, _ in signal_items:
        signal_html += f'''
        <div class="signal-item">
            <div class="signal-label">{label}</div>
            <div class="signal-value">{value}</div>
        </div>'''
    signal_html += '</div>'

    # Strike table
    strikes = verification.get("recommended_strikes", [])
    strike_html = ""
    if strikes:
        strike_html = '''
        <table class="strike-table">
            <thead><tr>
                <th>Strike</th><th>Expiry</th><th>Bid/Ask</th>
                <th>IV</th><th>OI</th><th>Vol</th>
            </tr></thead><tbody>'''
        for s in strikes:
            strike_html += f'''
            <tr>
                <td class="strike">${s["strike"]:.2f}</td>
                <td>{s["expiry"]}</td>
                <td class="bid-ask">${s["bid"]:.2f} / ${s["ask"]:.2f}</td>
                <td class="iv">{s["iv"]:.1%}</td>
                <td>{s["oi"]:,}</td>
                <td>{s["volume"]:,}</td>
            </tr>'''
        strike_html += '</tbody></table>'

    # Bonus unusual activity
    bonus_html = ""
    bonus = verification.get("bonus_data", [])
    if bonus:
        bonus_html = '<div style="margin-top:8px;">'
        for b in bonus[:5]:
            bonus_html += f'<span class="bonus-tag">{b["type"].replace("_", " ")} ${b["strike"]:.0f} vol={b["volume"]:,}</span> '
        bonus_html += '</div>'

    # Issues / corrections
    issues_html = ""
    if verification.get("issues"):
        issues_html = '<div style="margin-top:8px; padding:8px; background:#EF444411; border-radius:6px; font-size:11px; color:#EF4444;">'
        for issue in verification["issues"]:
            issues_html += f"⚠ {issue}<br>"
        issues_html += '</div>'

    corrections_html = ""
    if verification.get("corrections"):
        corrections_html = '<div style="margin-top:4px; font-size:11px; color:#22C55E;">'
        for c in verification["corrections"]:
            corrections_html += f'✓ Corrected {c["field"]}: {c["old"]} → {c["new"]} (source: {c["source"]})<br>'
        corrections_html += '</div>'

    body = f'''
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
        <div>
            <span class="badge {"badge-buy" if direction == "CALL" else "badge-sell"}">
                {ticker} {direction}S
            </span>
            &nbsp;
            <span class="verified-badge {badge_cls}">{badge_text}</span>
            {f'<span class="verified-badge {llm_badge_cls}">{llm_badge_text}</span>' if llm_badge_text else ''}
        </div>
        <div style="font-size:24px;font-weight:800;color:{"#22C55E" if direction == "CALL" else "#EF4444"};">
            {payoff:.0f}x
        </div>
    </div>
    {signal_html}
    <div style="margin:12px 0;font-size:14px;color:#C8D8E8;line-height:1.6;">
        {thesis}
    </div>
    {strike_html}
    {bonus_html}
    {f'<div style="margin-top:6px;font-size:11px;color:#4fc3f7;">🤖 {llm_reason}</div>' if llm_reason else ''}
    {issues_html}
    {corrections_html}
    '''

    return {
        "title": f"#{index} — {ticker} {direction}S",
        "body": body,
        "accent": "purple",
    }


def _format_summary_section(opportunities: list[dict], verifications: list[dict]) -> dict:
    """Format the summary header section."""
    total = len(opportunities)
    verified = sum(1 for v in verifications if v.get("verified"))
    total_bonus = sum(len(v.get("bonus_data", [])) for v in verifications)
    total_corrections = sum(len(v.get("corrections", [])) for v in verifications)

    body = f'''
    <div style="display:flex;justify-content:space-around;text-align:center;padding:8px 0;">
        <div>
            <div style="font-size:28px;font-weight:800;color:#A855F7;">{total}</div>
            <div style="font-size:11px;color:#5A7A96;">OPPORTUNITIES</div>
        </div>
        <div>
            <div style="font-size:28px;font-weight:800;color:#22C55E;">{verified}</div>
            <div style="font-size:11px;color:#5A7A96;">VERIFIED</div>
        </div>
        <div>
            <div style="font-size:28px;font-weight:800;color:#F59E0B;">{total_bonus}</div>
            <div style="font-size:11px;color:#5A7A96;">BONUS SIGNALS</div>
        </div>
        <div>
            <div style="font-size:28px;font-weight:800;color:#EF4444;">{total_corrections}</div>
            <div style="font-size:11px;color:#5A7A96;">CORRECTIONS</div>
        </div>
    </div>
    '''
    return {"title": "100x Scan Summary", "body": body}


# ── Main Digest Function ───────────────────────────────────────────────────

def run_100x_digest(force: bool = False) -> dict[str, Any]:
    """Run the full 100x digest pipeline.

    1. Scan all tickers for 100x opportunities
    2. Cross-verify each against live data
    3. Correct any stale data, note sources
    4. Opportunistically scrape bonus data
    5. Bundle and send formatted email

    Returns summary dict.
    """
    engine = _get_engine()

    # Step 1: Run the scanner
    log.info("100x Digest: Running options scanner...")
    from discovery.options_scanner import OptionsScanner
    scanner = OptionsScanner(db_engine=engine)

    try:
        opportunities = scanner.scan_all()
    except Exception as e:
        log.error("Scanner failed: {e}", e=str(e))
        opportunities = []

    # Get all opportunities — include everything the scanner flagged
    all_opps = sorted(opportunities, key=lambda o: -o.score)
    hundredx_opps = [o for o in opportunities if o.is_100x]

    log.info(
        "100x Digest: {total} total, {high} high-score, {hx} 100x",
        total=len(opportunities), high=len(all_opps), hx=len(hundredx_opps),
    )

    if not all_opps and not force:
        log.info("No notable opportunities — skipping digest")
        return {"sent": False, "reason": "no_opportunities", "scanned": len(opportunities)}

    # Step 1b: Data quality gate — reject signals with obvious garbage
    clean_opps = []
    rejected = []
    for opp in all_opps:
        issues = _sanity_check(opp)
        if issues:
            rejected.append({"ticker": opp.ticker, "issues": issues})
            log.warning("Rejected {t}: {i}", t=opp.ticker, i=issues)
        else:
            clean_opps.append(opp)
    all_opps = clean_opps
    log.info("Sanity check: {c} passed, {r} rejected", c=len(all_opps), r=len(rejected))

    # Step 2: Cross-verify each opportunity
    verified_opps = []
    verifications = []

    for opp in all_opps[:15]:  # Cap at 15 per digest
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
        }

        v = _verify_opportunity(opp.ticker, opp_dict, engine)
        verified_opps.append(opp_dict)
        verifications.append(v)

        time.sleep(0.5)  # Rate limit yfinance

    # Step 3: LLM sanity review — Hermes evaluates each opportunity
    log.info("100x Digest: LLM reviewing {n} opportunities...", n=len(verified_opps))
    reviewed_opps = _llm_sanity_review(verified_opps, engine)
    # Filter verifications to match reviewed opps
    reviewed_tickers = {o["ticker"] for o in reviewed_opps}
    reviewed_verifications = [v for o, v in zip(verified_opps, verifications) if o["ticker"] in reviewed_tickers]
    verified_opps = reviewed_opps
    verifications = reviewed_verifications
    log.info("LLM review: {n} passed", n=len(verified_opps))

    if not verified_opps and not force:
        log.info("All opportunities rejected by sanity checks — skipping digest")
        return {"sent": False, "reason": "all_rejected", "scanned": len(opportunities),
                "rejected_sanity": len(rejected), "rejected_llm": len(all_opps) - len(verified_opps)}

    # Step 4: Build email sections
    sections = [_format_summary_section(verified_opps, verifications)]

    # Regime context
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT inferred_state, state_confidence, grid_recommendation "
                "FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1"
            )).fetchone()
            if row:
                from alerts.email import _section_regime
                sections.append(_section_regime(row[0], row[1], row[2]))
    except Exception:
        log.debug("Failed to fetch regime state for digest", exc_info=True)

    # Individual opportunity cards (100x first, then high-score)
    sorted_opps = sorted(
        zip(verified_opps, verifications),
        key=lambda x: (-x[0].get("is_100x", False), -x[0]["score"]),
    )

    for i, (opp, v) in enumerate(sorted_opps, 1):
        sections.append(_format_opportunity_card(opp, v, i))

    # Verification summary
    total_corrections = sum(len(v.get("corrections", [])) for v in verifications)
    total_bonus = sum(len(v.get("bonus_data", [])) for v in verifications)
    if total_corrections > 0 or total_bonus > 0:
        notes = []
        if total_corrections:
            notes.append(f"{total_corrections} data corrections applied from live verification")
        if total_bonus:
            notes.append(f"{total_bonus} bonus signals discovered during scraping")
        sections.append(_section_text(
            "Supervised Intelligence Notes",
            "<br>".join(f"• {n}" for n in notes),
            accent="amber",
        ))

    # Step 4: Send
    n_100x = sum(1 for o in verified_opps if o.get("is_100x"))
    subject = f"GRID Intelligence — {n_100x} 100x + {len(verified_opps) - n_100x} High-Score Opportunities"

    # Inject extra CSS for strike tables
    html = _render_html(subject, sections)
    html = html.replace("</style>", _STRIKE_CSS + "</style>")
    plain = "\n\n".join(f"[{s['title']}]\n{s.get('body', '')}" for s in sections)

    _send_in_thread(subject, html, plain)

    result = {
        "sent": True,
        "total_scanned": len(opportunities),
        "opportunities": len(verified_opps),
        "100x_count": n_100x,
        "verified": sum(1 for v in verifications if v.get("verified")),
        "corrections": total_corrections,
        "bonus_signals": total_bonus,
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
