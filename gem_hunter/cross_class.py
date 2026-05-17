"""Cross-class confirmation validator for gem-hunter (Task #117, +#167).

Single-class signals = noise. Real tradeable gems must align across >=2 of
{insider activity, congress disclosures, news catalyst, technical signal,
filings} on the same ticker within a recent window.

Task #167 (2026-05-17): added `filings` lane sourced from earnings_events
(SEC 10-K/10-Q/8-K + EARNINGS_CALL transcripts). Material disclosure inside
the window contributes up to 1.0 to composite, so a clean insider+filing+
technical setup (no news yet) can clear composite >= 1.5 with min_classes=2.

Public API:
    cross_class_score(cur, ticker, asof_ts, window_days=14) -> dict
        Returns per-class scores for {insider, congress, news, technical,
        filings} plus composite + classes_confirming.
    extract_tickers_from_gem(cur, g) -> list[str]
    validate_cross_class(cur, g, window_days=14,
                         min_classes=2, min_composite=1.5) -> tuple[bool, dict]

Wired into gem_hunter.insert_gems() as a pre-file validator (Step 3 of #117).

Design notes:
  * absence of data => 0.0 (not 1.0). A ticker with no insider rows in the
    window scores 0 for that class — NOT a free pass.
  * subjects without an extractable ticker (actor_pair country, signal_pair,
    etc.) BYPASS the cross-class gate — we mark validators["cross_class"] =
    {"bypassed": "<reason>"} and the existing kill-predictor + LLM judge
    still run.
  * uses a single psycopg2 cursor passed by caller; no new connection.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

# ---------------------------------------------------------------------------
# Ticker extraction
# ---------------------------------------------------------------------------

# US tickers: 1-5 uppercase letters, optionally with a single dot or dash
# (BRK.B, RDS-A). We exclude pure stopwords + common 3-5 letter false positives.
_TICKER_RE = re.compile(r"(?:^|[\s_|])([A-Z]{1,5}(?:[.\-][A-Z]{1,2})?)(?=[\s_|]|$)")

# Words shaped like tickers but never tickers in our corpus
_TICKER_STOPWORDS = {
    "A", "I", "AM", "PM", "AN", "AS", "AT", "BE", "BY", "DO", "GO", "IF",
    "IN", "IS", "IT", "ME", "MY", "NO", "OF", "ON", "OR", "SO", "TO", "UP",
    "US", "WE",
    "AND", "ARE", "BUT", "CAN", "FOR", "GET", "HAD", "HAS", "HER", "HIM",
    "HIS", "HOW", "ITS", "MAY", "NEW", "NOT", "NOW", "OUR", "OUT", "PUT",
    "SAW", "SAY", "SEE", "SHE", "THE", "TWO", "WAS", "WAY", "WHO", "WHY",
    "YES", "YET", "YOU",
    "BEEN", "FROM", "INTO", "LIKE", "MORE", "MOST", "ONLY", "OVER", "SOME",
    "SUCH", "THAN", "THAT", "THEM", "THEN", "THEY", "THIS", "WERE", "WHAT",
    "WHEN", "WILL", "WITH", "YOUR",
    "ABOUT", "AFTER", "BEING", "COULD", "EVERY", "FIRST", "FOUND", "GREAT",
    "MIGHT", "OTHER", "PEOPLE", "SHALL", "SHOULD", "SINCE", "STILL",
    "THEIR", "THESE", "THINK", "THOSE", "THREE", "UNDER", "UNTIL", "WHERE",
    "WHICH", "WHILE", "WOULD",
    # noise from our pipeline
    "CALL", "PUT", "NONE", "NULL", "TRUE", "FALSE", "REAL", "JSON",
    "MACRO", "GRID", "ETF", "USD", "EUR", "JPY", "GBP", "CHF", "CAD",
    "GDP", "CPI", "PPI", "FED", "ECB", "BOJ", "OPEC", "WSB", "DEX",
    "PCR", "ATM", "ITM", "OTM", "IV", "OI",
    "BUY", "SELL", "HOLD", "LONG", "SHORT", "EXIT", "ENTRY",
    # Agency-name fragments that leak through underscore-bounded scan (Task #145)
    "SPACE", "URBAN", "STATE", "HEALTH", "LABOR", "ENERGY", "AFFAIRS",
    "DEFENSE", "JUSTICE", "INTERIOR", "NATIONAL", "FEDERAL", "GENERAL",
    "COMMERCE", "TREASURY", "AGRICULTURE", "EDUCATION", "VETERANS",
    "HOUSING", "TRANSPORT", "SECURITY",
}


def _scan_text_for_tickers(text: str) -> set[str]:
    if not text:
        return set()
    found = set()
    for m in _TICKER_RE.finditer(text):
        sym = m.group(1)
        # Drop pure single letters (too noisy) and stopwords
        base = sym.split(".")[0].split("-")[0]
        if base in _TICKER_STOPWORDS:
            continue
        if len(base) < 2:
            # Allow well-known single-char tickers if explicit in dotted form
            continue
        found.add(sym)
    return found


def _maybe_value_tickers(v: Any) -> set[str]:
    out: set[str] = set()
    if v is None:
        return out
    if isinstance(v, str):
        out |= _scan_text_for_tickers(v)
    elif isinstance(v, (list, tuple)):
        for item in v:
            out |= _maybe_value_tickers(item)
    elif isinstance(v, dict):
        for k, vv in v.items():
            # Trust explicit "ticker"/"tickers"/"symbol" keys
            if isinstance(k, str) and k.lower() in {"ticker", "tickers", "symbol", "symbols"}:
                if isinstance(vv, str):
                    out.add(vv.upper())
                elif isinstance(vv, (list, tuple)):
                    for s in vv:
                        if isinstance(s, str):
                            out.add(s.upper())
            else:
                out |= _maybe_value_tickers(vv)
    return out


def extract_tickers_from_gem(cur, g: dict) -> list[str]:
    """Pull plausible US ticker symbols from a gem dict.

    Order of preference (highest signal first):
      1. evidence["ticker"]/["tickers"]/["symbol"]/["symbols"]
      2. related_ids entries that look like tickers
      3. subject_id text scan
      4. For hypothesis subjects: thesis + evidence of discovered_hypotheses
    """
    tickers: set[str] = set()

    ev = g.get("evidence") or {}
    if isinstance(ev, str):
        try:
            ev = json.loads(ev)
        except Exception:
            ev = {"_raw": ev}

    tickers |= _maybe_value_tickers(ev)

    related = g.get("related_ids") or []
    if isinstance(related, str):
        try:
            related = json.loads(related)
        except Exception:
            related = [related]
    for r in related or []:
        if isinstance(r, str):
            tickers |= _scan_text_for_tickers(r)

    sid = g.get("subject_id")
    if isinstance(sid, str):
        tickers |= _scan_text_for_tickers(sid)

    # Hypothesis lookup: gems on subject_kind="hypothesis" point at
    # discovered_hypotheses rows whose thesis usually names tickers.
    if g.get("subject_kind") == "hypothesis" and isinstance(sid, str) and cur is not None:
        try:
            cur.execute(
                "SELECT thesis, evidence FROM discovered_hypotheses WHERE id = %s",
                (sid,),
            )
            row = cur.fetchone()
            if row:
                thesis, h_ev = row[0], row[1]
                if thesis:
                    tickers |= _scan_text_for_tickers(thesis)
                if h_ev:
                    tickers |= _maybe_value_tickers(h_ev)
        except Exception:
            pass

    # Final cleanup: drop stopwords (in case they snuck through the explicit
    # key path), filter to 1-5 alphas
    cleaned = []
    for t in tickers:
        t = t.upper().strip()
        base = t.split(".")[0].split("-")[0]
        if not base or base in _TICKER_STOPWORDS:
            continue
        if len(base) < 2 or len(base) > 5:
            continue
        if not base.isalpha():
            continue
        cleaned.append(t)
    return sorted(set(cleaned))


# ---------------------------------------------------------------------------
# Per-class scorers
# ---------------------------------------------------------------------------

def _as_window(asof_ts, window_days: int) -> tuple[datetime, datetime]:
    if asof_ts is None:
        asof = datetime.now(timezone.utc)
    elif isinstance(asof_ts, str):
        try:
            asof = datetime.fromisoformat(asof_ts.replace("Z", "+00:00"))
        except Exception:
            asof = datetime.now(timezone.utc)
    elif isinstance(asof_ts, datetime):
        asof = asof_ts if asof_ts.tzinfo else asof_ts.replace(tzinfo=timezone.utc)
    else:
        asof = datetime.now(timezone.utc)
    start = asof - timedelta(days=window_days)
    return start, asof


def _score_insider(cur, ticker: str, start, end) -> tuple[float, dict]:
    """0.0-1.0 based on count of insider trades in window.
    1 trade -> 0.4, 3 trades -> 0.7, >=5 trades -> 1.0.
    Cluster-buy boolean adds +0.2 (capped at 1.0)."""
    cur.execute(
        """SELECT COUNT(*) AS n,
                  COUNT(*) FILTER (WHERE is_cluster_buy) AS n_cluster,
                  COALESCE(SUM(CASE WHEN trade_type ILIKE 'P%%' THEN 1
                                    WHEN trade_type ILIKE 'S%%' THEN -1
                                    ELSE 0 END), 0) AS net_dir
             FROM insider_trades
            WHERE ticker = %s AND trade_date >= %s AND trade_date <= %s""",
        (ticker, start.date(), end.date()),
    )
    row = cur.fetchone()
    n = int(row[0] or 0)
    n_cluster = int(row[1] or 0)
    net_dir = int(row[2] or 0)
    if n == 0:
        return 0.0, {"n": 0}
    if n >= 5:
        base = 1.0
    elif n >= 3:
        base = 0.7
    elif n >= 2:
        base = 0.5
    else:
        base = 0.4
    if n_cluster > 0:
        base = min(1.0, base + 0.2)
    return base, {"n": n, "n_cluster": n_cluster, "net_dir": net_dir}


def _score_congress(cur, ticker: str, start, end) -> tuple[float, dict]:
    """Same shape as insider. Congress trades are sparser; n>=3 -> 1.0."""
    cur.execute(
        """SELECT COUNT(*) AS n,
                  COUNT(DISTINCT representative) AS n_reps,
                  COALESCE(SUM(CASE WHEN transaction_type ILIKE '%%purchase%%' THEN 1
                                    WHEN transaction_type ILIKE '%%sale%%' THEN -1
                                    ELSE 0 END), 0) AS net_dir
             FROM congressional_trades
            WHERE ticker = %s
              AND COALESCE(transaction_date, disclosure_date) >= %s
              AND COALESCE(transaction_date, disclosure_date) <= %s""",
        (ticker, start.date(), end.date()),
    )
    row = cur.fetchone()
    n = int(row[0] or 0)
    n_reps = int(row[1] or 0)
    net_dir = int(row[2] or 0)
    if n == 0:
        return 0.0, {"n": 0}
    if n_reps >= 3:
        base = 1.0
    elif n_reps == 2:
        base = 0.7
    else:
        base = 0.4 if n == 1 else 0.5
    return base, {"n": n, "n_reps": n_reps, "net_dir": net_dir}


def _score_news(cur, ticker: str, start, end) -> tuple[float, dict]:
    """Average sentiment confidence as magnitude proxy, scaled by article count.
    >=10 articles -> 1.0 base, 5 -> 0.7, 2 -> 0.5, 1 -> 0.3.
    Multiply by max(|sentiment_score|, 0.3) so a flood of NEUTRAL doesn't
    fake-confirm. Returns 0 if no articles."""
    cur.execute(
        """SELECT COUNT(*) AS n,
                  COUNT(*) FILTER (WHERE sentiment ILIKE 'POS%%'
                                    OR sentiment ILIKE 'BULL%%') AS n_pos,
                  COUNT(*) FILTER (WHERE sentiment ILIKE 'NEG%%'
                                    OR sentiment ILIKE 'BEAR%%') AS n_neg,
                  COALESCE(AVG(confidence), 0) AS avg_conf
             FROM news_articles
            WHERE %s = ANY(tickers)
              AND published_at >= %s
              AND published_at <= %s""",
        (ticker, start, end),
    )
    row = cur.fetchone()
    n = int(row[0] or 0)
    n_pos = int(row[1] or 0)
    n_neg = int(row[2] or 0)
    avg_conf = float(row[3] or 0.0)
    if n == 0:
        return 0.0, {"n": 0}
    if n >= 10:
        base = 1.0
    elif n >= 5:
        base = 0.7
    elif n >= 2:
        base = 0.5
    else:
        base = 0.3
    # Sentiment skew: directional articles vs total. ALL-NEUTRAL coverage
    # is treated as zero signal (the model never tagged the catalyst); a
    # mixed pile gets dampened proportionally.
    directional = n_pos + n_neg
    if directional == 0:
        return 0.0, {"n": n, "n_pos": 0, "n_neg": 0, "avg_conf": avg_conf,
                     "note": "all_neutral_no_signal"}
    skew = directional / max(n, 1)
    score = base * max(0.3, skew)
    # avg_conf is a quality multiplier (0.5 = neutral, >0.7 = high)
    if avg_conf > 0:
        score *= min(1.2, 0.5 + avg_conf)
    score = max(0.0, min(1.0, score))
    return score, {"n": n, "n_pos": n_pos, "n_neg": n_neg, "avg_conf": avg_conf}


def _score_technical(cur, ticker: str, start, end) -> tuple[float, dict]:
    """Two-leg technical:
      A) recent close price vs 50-day SMA (from ticker_metrics_daily) - if data
      B) volume of whale_options + unusual_options + options_flow signals
         on this ticker in the window (from signal_data)
    Either leg can confirm. Score = max(legA, legB).
    """
    # Leg A: price vs 50dma
    leg_a = 0.0
    pa_info: dict[str, Any] = {}
    cur.execute(
        """SELECT obs_date, close_price FROM ticker_metrics_daily
            WHERE ticker = %s AND obs_date <= %s
            ORDER BY obs_date DESC LIMIT 60""",
        (ticker, end.date()),
    )
    rows = cur.fetchall()
    if rows and len(rows) >= 20:
        closes = [float(r[1]) for r in rows if r[1] is not None]
        if closes:
            last = closes[0]
            sma = sum(closes[:50]) / min(50, len(closes))
            if sma > 0:
                pct = (last - sma) / sma
                pa_info = {"last_close": last, "sma50": sma, "pct_vs_sma50": pct, "n_bars": len(closes)}
                # >=5% above 50dma -> 0.7, >=10% -> 1.0; symmetric for below
                a = min(1.0, abs(pct) / 0.10)
                if abs(pct) >= 0.05:
                    leg_a = max(0.4, a)
                else:
                    leg_a = 0.0

    # Leg B: options/whale activity volume in signal_data
    cur.execute(
        """SELECT COUNT(*) AS n,
                  COALESCE(AVG(magnitude), 0) AS avg_mag
             FROM signal_data
            WHERE upper(ticker) = upper(%s)
              AND signal_date >= %s AND signal_date <= %s
              AND signal_type IN ('whale_options', 'unusual_options',
                                  'options_flow', 'darkpool', 'smart_money')""",
        (ticker, start.date(), end.date()),
    )
    row = cur.fetchone()
    n_tech = int(row[0] or 0)
    avg_mag = float(row[1] or 0.0)
    pb_info: dict[str, Any] = {"n": n_tech, "avg_mag": avg_mag}
    leg_b = 0.0
    if n_tech >= 20:
        leg_b = 1.0
    elif n_tech >= 10:
        leg_b = 0.7
    elif n_tech >= 5:
        leg_b = 0.5
    elif n_tech >= 1:
        leg_b = 0.3

    score = max(leg_a, leg_b)
    return score, {"price_vs_sma": pa_info, "options_volume": pb_info,
                   "leg_a": leg_a, "leg_b": leg_b}


def _score_filings(cur, ticker: str, start, end) -> tuple[float, dict]:
    """Task #167: SEC filings + transcript activity in window from earnings_events.

    Score formula (per task spec):
      * 0.0 if no events
      * 0.4 base if any 10-K / 10-Q in window (material disclosure)
      * +0.2 if any 8-K with material event flag (item codes 1.01, 2.01,
              2.02, 4.01, 5.02, 8.01 — acquisition/results/auditor/exec/other)
      * +0.2 if EARNINGS_CALL with sentiment magnitude > 0.5
      * cap at 1.0
    """
    cur.execute(
        """SELECT event_type, raw_payload, sentiment, confidence
             FROM earnings_events
            WHERE upper(ticker) = upper(%s)
              AND filing_date >= %s
              AND filing_date <= %s""",
        (ticker, start.date(), end.date()),
    )
    rows = cur.fetchall() or []
    if not rows:
        return 0.0, {"n": 0}

    n_10kq = 0
    n_8k = 0
    n_8k_material = 0
    n_call = 0
    n_call_loaded = 0
    n_other = 0
    max_sent_mag = 0.0
    # 8-K item codes considered "material" (price-moving). Per SEC Form 8-K
    # instructions: 1.01 entry/material agreement, 2.01 acquisition, 2.02
    # results, 4.01 auditor change, 5.02 exec/director change, 8.01 other.
    MATERIAL_ITEMS = {"1.01", "2.01", "2.02", "4.01", "5.02", "8.01"}

    for etype, raw, sentiment, conf in rows:
        et = (etype or "").upper().strip()
        if et in ("10-K", "10-Q"):
            n_10kq += 1
        elif et == "8-K":
            n_8k += 1
            # raw_payload.items is a comma-separated string like "2.02,9.01"
            items_field = ""
            try:
                if isinstance(raw, dict):
                    items_field = str(raw.get("items") or "")
                elif isinstance(raw, str):
                    rj = json.loads(raw)
                    items_field = str(rj.get("items") or "")
            except Exception:
                items_field = ""
            items = {i.strip() for i in items_field.split(",") if i.strip()}
            if items & MATERIAL_ITEMS:
                n_8k_material += 1
        elif et in ("EARNINGS_CALL", "TRANSCRIPT"):
            n_call += 1
            # sentiment magnitude — prefer numeric sentiment if present,
            # else fall back to confidence as proxy when sentiment is the
            # textual BULL/BEAR/NEUTRAL tag.
            mag = 0.0
            try:
                if sentiment is not None:
                    s = str(sentiment).strip().upper()
                    if s in ("BULL", "POS", "POSITIVE"):
                        mag = float(conf or 0.0)
                    elif s in ("BEAR", "NEG", "NEGATIVE"):
                        mag = float(conf or 0.0)
                    else:
                        # try numeric
                        try:
                            mag = abs(float(sentiment))
                        except Exception:
                            mag = 0.0
            except Exception:
                mag = 0.0
            if mag > 0.5:
                n_call_loaded += 1
            if mag > max_sent_mag:
                max_sent_mag = mag
        else:
            n_other += 1

    score = 0.0
    components: dict[str, Any] = {
        "n_10kq": n_10kq, "n_8k": n_8k, "n_8k_material": n_8k_material,
        "n_call": n_call, "n_call_sentiment_gt_0_5": n_call_loaded,
        "n_other": n_other, "max_sent_mag": max_sent_mag,
    }
    if n_10kq > 0:
        score += 0.4
        components["bonus_10kq"] = 0.4
    if n_8k_material > 0:
        score += 0.2
        components["bonus_8k_material"] = 0.2
    if n_call_loaded > 0:
        score += 0.2
        components["bonus_call_loaded"] = 0.2

    # Floor for any event so a lone non-material 8-K still registers above
    # the 0.4 CONFIRMING_THRESHOLD when material; otherwise 0.0 if nothing
    # qualifies. Don't fabricate a score where the spec says 0.0.
    if score == 0.0 and (n_8k > 0 or n_call > 0 or n_other > 0):
        # Some filings exist but none meet the score-bearing criteria.
        # Keep score 0.0 per spec (no material disclosure), but record info.
        components["note"] = "events_present_but_none_material"

    score = max(0.0, min(1.0, score))
    components["score"] = round(score, 4)
    components["n"] = n_10kq + n_8k + n_call + n_other
    return score, components


# ---------------------------------------------------------------------------
# Public scorer
# ---------------------------------------------------------------------------

CONFIRMING_THRESHOLD = 0.4


def cross_class_score(cur, ticker: str, asof_ts=None, window_days: int = 14,
                      window_days_per_class: Optional[dict] = None) -> dict:
    """Compute per-class scores for a ticker.

    Task #130: `window_days_per_class` lets callers override the lookback
    window for a specific class (e.g. {"insider": 30}). Useful when an
    insider_cluster rule fires on a 30d cluster but the default 14d
    cross-class window would mark its own insider trades as out-of-range.

    Returns:
        {
          'ticker': 'AAPL',
          'window_days': 14,
          'window_days_per_class': {'insider': 30},  # only if overridden
          'asof': '2026-05-17T...',
          'insider': 0.0-1.0,
          'congress': 0.0-1.0,
          'news': 0.0-1.0,
          'technical': 0.0-1.0,
          'filings': 0.0-1.0,            # Task #167
          'composite': 0.0-5.0,           # 5 classes now (was 4.0 max)
          'classes_confirming': int,      # how many >= 0.4
          'detail': {...per-class breakdown...},
        }
    """
    wpc = window_days_per_class or {}
    start, end = _as_window(asof_ts, window_days)
    ticker = (ticker or "").upper().strip()
    if not ticker:
        return {
            "ticker": ticker, "window_days": window_days,
            "asof": end.isoformat(),
            "insider": 0.0, "congress": 0.0, "news": 0.0, "technical": 0.0,
            "filings": 0.0,
            "composite": 0.0, "classes_confirming": 0, "detail": {},
        }
    # per-class windows (default to the global window if no override)
    def _w(cls: str) -> tuple[datetime, datetime]:
        if cls in wpc:
            return _as_window(asof_ts, int(wpc[cls]))
        return start, end
    i_start, i_end = _w("insider")
    c_start, c_end = _w("congress")
    n_start, n_end = _w("news")
    t_start, t_end = _w("technical")
    f_start, f_end = _w("filings")
    insider_s, insider_d = _score_insider(cur, ticker, i_start, i_end)
    congress_s, congress_d = _score_congress(cur, ticker, c_start, c_end)
    news_s, news_d = _score_news(cur, ticker, n_start, n_end)
    tech_s, tech_d = _score_technical(cur, ticker, t_start, t_end)
    filings_s, filings_d = _score_filings(cur, ticker, f_start, f_end)
    classes = [insider_s, congress_s, news_s, tech_s, filings_s]
    confirming = sum(1 for s in classes if s >= CONFIRMING_THRESHOLD)
    composite = sum(classes)
    result = {
        "ticker": ticker,
        "window_days": window_days,
        "asof": end.isoformat(),
        "insider": round(insider_s, 4),
        "congress": round(congress_s, 4),
        "news": round(news_s, 4),
        "technical": round(tech_s, 4),
        "filings": round(filings_s, 4),
        "composite": round(composite, 4),
        "classes_confirming": confirming,
        "detail": {
            "insider": insider_d, "congress": congress_d,
            "news": news_d, "technical": tech_d,
            "filings": filings_d,
        },
    }
    if wpc:
        result["window_days_per_class"] = dict(wpc)
    return result


def _per_class_window_for_source(source: str) -> dict:
    """Task #130: rules whose own detection window is longer than the default
    14d cross-class window need their own class lookback bumped, or their
    confirming class would be empty.

    Currently:
      * insider_cluster -> 30d insider lookback (matches the rule's 30d
        cluster-detection window). Other classes stay at the global default.

    The env override GEM_CC_WINDOW_DAYS_INSIDER lets ops bump it further
    without a code change.
    """
    wpc: dict = {}
    if source == "insider_cluster":
        try:
            ins = int(os.environ.get("GEM_CC_WINDOW_DAYS_INSIDER", "30"))
        except ValueError:
            ins = 30
        wpc["insider"] = ins
    return wpc


def validate_cross_class(cur, g: dict, *, window_days: int = 14,
                         min_classes: int = 2,
                         min_composite: float = 1.5) -> tuple[bool, dict]:
    """Validator-shaped wrapper.

    Returns (pass, info) matching the validate_kill_predictor / validate_llm_judge
    convention used in gem_hunter.insert_gems.

    Pass = at least one extracted ticker passes the cross-class gate. If NO
    tickers can be extracted from the gem, we PASS (bypass) so non-equity
    gems (actor_pair country alignments, signal_pair correlations) still
    flow through the rest of the validator pipeline.
    """
    tickers = extract_tickers_from_gem(cur, g)
    if not tickers:
        return True, {"bypassed": "no_ticker_extractable"}

    asof = None
    ev = g.get("evidence") or {}
    if isinstance(ev, dict):
        for k in ("date", "asof", "detected_at", "computed_at", "as_of"):
            v = ev.get(k)
            if v:
                asof = v
                break

    # Task #130 Fix B: rule-source-specific window override
    wpc = _per_class_window_for_source(g.get("source") or "")

    per_ticker = []
    best_pass = False
    best_score = None
    for t in tickers[:10]:  # cap to avoid running 50 lookups
        s = cross_class_score(cur, t, asof_ts=asof, window_days=window_days,
                              window_days_per_class=wpc or None)
        per_ticker.append(s)
        if s["classes_confirming"] >= min_classes and s["composite"] >= min_composite:
            best_pass = True
            if best_score is None or s["composite"] > best_score["composite"]:
                best_score = s
        else:
            if best_score is None or s["composite"] > best_score["composite"]:
                best_score = s

    info = {
        "tickers_checked": tickers[:10],
        "min_classes": min_classes,
        "min_composite": min_composite,
        "window_days": window_days,
        "window_days_per_class": wpc or None,
        "source": g.get("source"),
        "best": best_score,
        "per_ticker": per_ticker,
    }

    # Task #132: insider-solo-OK shortcut.
    # High-conviction single-class insider clusters (n>=5 insiders, value>=$750K,
    # insider class score>=0.95) bypass the 2-class requirement. Tagged with
    # confidence_level="single_class_high_conviction" by the caller via info.
    if (not best_pass) and (g.get("source") == "insider_cluster"):
        import os as _os
        solo_ok_enabled = _os.environ.get("GEM_INSIDER_SOLO_OK", "1") == "1"
        solo_min_n = int(_os.environ.get("GEM_INSIDER_SOLO_OK_N", "5"))
        solo_min_value = float(_os.environ.get("GEM_INSIDER_SOLO_OK_VALUE_USD", "750000"))
        ev = g.get("evidence") or {}
        try:
            n_ins = int(ev.get("n_insiders") or 0)
            total_v = float(ev.get("total_value_usd") or 0.0)
        except Exception:
            n_ins, total_v = 0, 0.0
        insider_score = 0.0
        if best_score and isinstance(best_score, dict):
            insider_score = float(best_score.get("insider") or 0.0)
        if solo_ok_enabled and n_ins >= solo_min_n and total_v >= solo_min_value and insider_score >= 0.95:
            best_pass = True
            info["solo_ok"] = {
                "reason": "insider_solo_high_conviction",
                "n_insiders": n_ins,
                "total_value_usd": total_v,
                "insider_score": insider_score,
                "min_n": solo_min_n,
                "min_value": solo_min_value,
            }
            info["confidence_level"] = "single_class_high_conviction"
    return best_pass, info


# ---------------------------------------------------------------------------
# Manual smoke-test entry (python cross_class.py AAPL [window_days])
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import os
    import sys
    import psycopg2

    if len(sys.argv) < 2:
        print("usage: python cross_class.py TICKER [window_days] [asof YYYY-MM-DD]")
        sys.exit(2)
    ticker = sys.argv[1]
    window = int(sys.argv[2]) if len(sys.argv) > 2 else 14
    asof = sys.argv[3] if len(sys.argv) > 3 else None

    conn = psycopg2.connect(
        host=os.environ.get("PG_HOST", "100.75.185.36"),
        port=int(os.environ.get("PG_PORT", "5432")),
        user=os.environ.get("PG_USER", "grid"),
        password=os.environ.get("PG_PASSWORD", "gridmaster2026"),
        dbname=os.environ.get("PG_DB", "griddb"),
    )
    cur = conn.cursor()
    s = cross_class_score(cur, ticker, asof_ts=asof, window_days=window)
    print(json.dumps(s, indent=2, default=str))
    cur.close()
    conn.close()
