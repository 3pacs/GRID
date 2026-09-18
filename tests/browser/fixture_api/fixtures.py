"""Sanitized fixture payloads for the GRID browser-acceptance harness.

Every value here is synthetic: ticker ``TEST1``, round numbers, dates in
2026-09. Nothing here is read from, or copied from, any real account,
database, or production/staging system.

Shapes follow what was read directly from this worktree's ``api/routers/*``
(base) and from the read-only composed-g checkout (composed tree
44019a43, "development composition, not production" — see
docs/reference/AVAILABILITY_CONTRACT.md there) as of 2026-09-18:

  - api/routers/regime.py            (confidence: None = unscored, float 0.0 survives)
  - api/routers/watchlist_core.py    (get_portfolio: total_value null + basis, no $ P&L)
  - api/routers/watchlist_overview.py(get_ticker_edge: trust_score/shares/value null-safe)
  - api/routers/dad.py               (_gold_from_summary, Finviz value_kind text/numeric)
  - api/routers/valuation.py         (catalyst_timeline: milestones/predictions)
  - api/routers/intelligence_risk.py (_build_dashboard_snapshot: trust.top_sources/
                                       convergence_events, sub-system available/reason)
  - intelligence/trust_scorer.py     (detect_convergence: combined_confidence None+
                                       confidence_basis "unscored" when nothing is scored)
  - api/routers/system.py            (health/pipeline-health schemas)

Three scenarios, selected by SCENARIO in server.py:
  healthy  - every widget has real-shaped data, explicit source + as_of.
  partial  - one widget/dataset is unavailable or stale while others are fine;
             a measured 0.0 sits next to a null (None) so the two are visibly
             distinct kinds, not the same "empty-ish" value.
  empty    - no holdings, no watchlist, no workbook evidence, no convergence.
"""

from __future__ import annotations

import json
import os

_FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")


def _load_json_fixture(name: str) -> dict:
    """Load a static JSON fixture file shipped next to this module."""
    with open(os.path.join(_FIXTURE_DIR, name), encoding="utf-8") as fh:
        return json.load(fh)


TICKER = "TEST1"
AS_OF_DATE = "2026-09-15"
AS_OF_DATETIME = "2026-09-15T14:30:00+00:00"
GENERATED_AT = "2026-09-18T09:00:00+00:00"


# ── Auth ──────────────────────────────────────────────────────────────

def _fixture_role() -> str:
    """FIXTURE_ROLE env var: 'admin' (default, full operator cockpit) or
    'contributor' (pwa/src/authSession.js:57 SIMPLE_ROLES -> dad-mode simple
    shell, just the two contributor pages). Read at request time (not at
    import time) so a running server picks up a changed env var on restart.
    """
    return os.environ.get("FIXTURE_ROLE", "admin")


def login_response() -> dict:
    """A synthetic dev token. Not a real JWT — the fixture server does not
    verify it; see tests/browser/README.md for why that is legitimate here.
    """
    role = _fixture_role()
    return {
        "token": f"dev-fixture-token.not-a-real-jwt.{role}",
        "expires_in": 3600,
        "role": role,
        "username": "fixture-operator" if role == "admin" else "fixture-dad",
    }


def verify_response() -> dict:
    role = _fixture_role()
    return {
        "valid": True,
        "expires_at": "2026-09-16T14:30:00+00:00",
        "role": role,
        "username": "fixture-operator" if role == "admin" else "fixture-dad",
    }


# ── Journey (a): home / market overview ─────────────────────────────

def regime_current(scenario: str) -> dict:
    if scenario == "empty":
        return {
            "state": "UNCALIBRATED",
            "confidence": 0.0,
            "transition_probability": 0.0,
            "top_drivers": [],
            "contradiction_flags": [],
            "model_version": "none",
            "as_of": GENERATED_AT,
            "baseline_comparison": "No data — run auto_regime or wait for scheduled detection",
            "as_of_date": "",
            "staleness_days": None,
            "data_as_of": "",
            "data_staleness_days": None,
        }
    # partial and healthy both have a measured regime; partial marks the
    # *data* behind it as older than the reading itself (data_staleness_days
    # > staleness_days) rather than inventing a fresher number.
    data_staleness = 6 if scenario == "partial" else 0
    return {
        "state": "RISK_ON",
        "confidence": 0.62,
        "transition_probability": 0.18,
        "top_drivers": ["credit_spreads_tightening", "breadth_expansion"],
        "contradiction_flags": [],
        "model_version": "regime-hmm v3",
        "as_of": AS_OF_DATETIME,
        "baseline_comparison": "Matches baseline HMM state",
        "as_of_date": AS_OF_DATE,
        "staleness_days": 0,
        "data_as_of": "2026-09-09" if scenario == "partial" else AS_OF_DATE,
        "data_staleness_days": data_staleness,
    }


def watchlist_list(scenario: str) -> dict:
    if scenario == "empty":
        return {"items": [], "total": 0, "limit": 50, "offset": 0, "has_more": False}
    item = {
        "id": 1,
        "ticker": TICKER,
        "display_name": "Test One Corp",
        "asset_type": "stock",
        "added_at": "2026-09-01T00:00:00+00:00",
        "weight": None,
    }
    return {"items": [item], "total": 1, "limit": 50, "offset": 0, "has_more": False}


def news_momentum(scenario: str) -> dict:
    """GET /api/v1/physics/momentum -> mirrors `MomentumResult.to_dict()`
    (physics/momentum.py:37-59) exactly: available/sentiment_trend/
    momentum_direction/energy_state/direction/summary/details/warnings.

    An earlier version of this fixture invented a shape (`status`,
    `lookback_days`, `series`, `source`) that shares no keys with the real
    one. `widgets.jsx` NewsCard reads `data?.direction` and `data?.summary`
    (~:248-249); with the wrong shape both were always `undefined`, so the
    card showed "It's quiet — no big news right now." even on the
    `healthy` scenario, where real momentum data existed. That was a pure
    fixture bug, not a real consumer mismatch — the router's `direction`/
    `summary` fields do exist and NewsCard reads them correctly.
    """
    if scenario in ("empty", "partial"):
        # partial: this is the widget that is unavailable while the rest of
        # home (regime, watchlist) is fine. empty: nothing has been computed.
        return {
            "available": False,
            "sentiment_trend": "unavailable",
            "momentum_direction": "unavailable",
            "energy_state": "unavailable",
            "direction": "unavailable",
            "summary": "",
            "details": {},
            "warnings": ["No price features available"] if scenario == "empty" else ["GDELT lag: latest available data is 2 days old"],
        }
    return {
        "available": True,
        "sentiment_trend": "rising",
        "momentum_direction": "accelerating",
        "energy_state": "medium",
        "direction": "bullish",
        "summary": f"News sentiment for {TICKER} is rising and accelerating — tone is net bullish this week.",
        "details": {"lookback_days": 63, "as_of": AS_OF_DATE},
        "warnings": [],
    }


def options_recommendations(scenario: str, ticker: str | None = None) -> dict:
    """GET /api/v1/options/recommendations -> mirrors
    api/routers/options.py::get_recommendations (~line 200-238) and its
    fallback `_load_saved_recommendations` (~line 138-194), read from THIS
    worktree (not composed-h — see README "Provenance note" for why that
    read was declined).

    Two corrections to how this fixture gap was described, both confirmed
    by reading source directly rather than assumed:

    1. This endpoint is not called by Portfolio.jsx's OPTIONS P&L card or
       WatchlistAnalysis.jsx's options panel — grepped the whole `pwa/src`
       tree and `getOptionsRecommendations`/`/api/v1/options/recommendations`
       only appears in `pwa/src/views/Options.jsx` and once in
       `pwa/src/app.jsx:137`, inside `OPERATOR_PRELOAD_API_PATHS` — a
       generic background preload fired for any admin session, not a
       per-view fetch. Portfolio.jsx's OPTIONS P&L card reads
       `get_portfolio`'s own `options_pnl` (already covered by
       `watchlist_portfolio` above); WatchlistAnalysis.jsx's `OptionsIntel`
       reads `get_ticker_analysis`'s own `options` field (already covered
       by `watchlist_ticker_analysis`). The 404 itself was real (this route
       had no fixture at all) and worth fixing regardless of which code
       path triggers it.
    2. `_load_saved_recommendations`'s query is
       `WHERE (outcome IS NULL OR outcome = 'OPEN')` — this endpoint
       structurally can never return a closed/WIN recommendation (those
       only ever appear via `GET /api/v1/options/recommendations/history`,
       a separate, unfixtured route). A "healthy = one WIN closed, one
       open" pair was asked for; this fixture instead serves two OPEN
       recommendations, which is what the real endpoint can honestly
       return, and says so below rather than serving a shape the API
       cannot actually produce.
    """
    now = GENERATED_AT
    if scenario == "empty":
        return {
            "recommendations": [],
            "generated_at": now,
            "scan_summary": {
                "total_scanned": 0, "passed_sanity": 0, "rejected": 0,
                "source": "persisted", "fresh_scan": False,
                "reason": "Options recommender module is not installed",
            },
        }

    def _rec(rec_ticker, strike, confidence, expected_return, kelly, note):
        return {
            "ticker": rec_ticker, "direction": "CALL", "strike": strike,
            "expiry": "2026-10-16", "entry_price": 3.2, "target_price": 5.0,
            "stop_loss": 1.8, "expected_return": expected_return,
            "kelly_fraction": kelly, "confidence": confidence,
            "thesis": note,
            "sanity_status": {"liquidity": {"status": "pass"}, "spread": {"status": "pass"}},
            "dealer_context": "long_gamma", "generated_at": now,
            # This endpoint's own query excludes anything else — see
            # docstring above. Always "OPEN" here, never a WIN/LOSS row.
            "outcome": "OPEN",
        }

    recs = [_rec(TICKER, 105.0, 0.58, 0.31, 0.04, "Synthetic fixture recommendation — TEST1 call.")]
    if scenario == "healthy":
        recs.append(_rec(TICKER, 110.0, 0.42, 0.19, 0.02, "Synthetic fixture recommendation — TEST1 further OTM call."))
    if scenario == "partial":
        # One recommendation with an explicit reason instead of a fabricated
        # confidence/expected_return: the scanner ran but this particular
        # opportunity failed a sanity check, so it was never scored.
        recs = [{
            "ticker": TICKER, "direction": "CALL", "strike": 105.0,
            "expiry": "2026-10-16", "entry_price": 3.2, "target_price": None,
            "stop_loss": None, "expected_return": None,
            "kelly_fraction": None, "confidence": None,
            "thesis": "Unscored — dealer gamma context unavailable for this expiry.",
            "sanity_status": {
                "liquidity": {"status": "pass"},
                "spread": {"status": "fail", "reason": "bid-ask spread exceeds 15% of mid"},
            },
            "dealer_context": None, "generated_at": now, "outcome": "OPEN",
        }]
    if ticker:
        recs = [r for r in recs if r.get("ticker", "").upper() == ticker.upper()]
    return {
        "recommendations": recs,
        "generated_at": now,
        "scan_summary": {
            "total_scanned": len(recs), "passed_sanity": len(recs), "rejected": 0,
            "source": "persisted", "fresh_scan": False,
            "reason": "Options recommender module is not installed",
        },
    }


def alerts_list(scenario: str) -> dict:
    """GET /api/v1/alerts -> mirrors api/routers/price_alerts.py::list_alerts
    (~line 186-203): `{"alerts": [{id,ticker,direction,threshold,note,active,
    created_at,triggered_at,last_price,price_at_create}]}`.

    This route had no fixture at all — the fixture server 404'd on it, so
    Home's alerts panel (`loadAlerts`, Home.jsx:42-46) silently caught the
    error and left the panel empty on every scenario, including `healthy`.
    """
    if scenario == "empty":
        return {"alerts": []}
    alert = {
        "id": 1, "ticker": TICKER, "direction": "above", "threshold": 110.0,
        "note": "Tell me when TEST1 hits $110", "active": True,
        "created_at": "2026-09-10T00:00:00+00:00", "triggered_at": None,
        "last_price": 100.0, "price_at_create": 95.0,
    }
    if scenario == "partial":
        # A second alert exists but has already triggered — recently
        # triggered alerts stay visible for 7 days per the router's own
        # WHERE clause, distinct from the still-active one above.
        return {"alerts": [alert, {
            "id": 2, "ticker": "TEST2", "direction": "below", "threshold": 40.0,
            "note": None, "active": False,
            "created_at": "2026-09-08T00:00:00+00:00", "triggered_at": "2026-09-14T00:00:00+00:00",
            "last_price": 38.0, "price_at_create": 45.0,
        }]}
    return {"alerts": [alert]}


def sector_flows(scenario: str) -> dict:
    """GET /api/v1/flows/sectors -> mirrors api/routers/flows.py::get_sectors
    (~line 267-284): `sectors` is a DICT keyed by sector name (not a list),
    each value carrying `sector_stress` (not `flow_1d_pct`/`flow_5d_pct`,
    which don't exist on this endpoint). The cold/empty payload is exactly
    `{"sectors": {}, "stale": True, "unavailable": True}` (flows.py:478).

    `widgets.jsx` MoneyFlowCard filters entries with
    `typeof s?.sector_stress === 'number'` (~:270-274) — an earlier version
    of this fixture used the wrong top-level type (a list) and the wrong
    field name (`flow_1d_pct`), so every entry failed that filter and the
    card always rendered "Nothing notable moving right now." even on
    `healthy`.
    """
    if scenario == "empty":
        return {"sectors": {}, "stale": True, "unavailable": True}
    sectors = {
        "Technology": {
            "etf": "XLK", "etf_price": 210.0, "etf_change_30d": 0.03, "etf_z": 0.4,
            "etf_options": None, "actors": [], "sector_stress": 0.8, "subsectors": ["semiconductors"],
        },
        "Energy": {
            "etf": "XLE", "etf_price": 90.0, "etf_change_30d": -0.02, "etf_z": -0.3,
            "etf_options": None, "actors": [], "sector_stress": -0.5, "subsectors": ["oil_gas"],
        },
    }
    payload = {"sectors": sectors, "computed_at": GENERATED_AT}
    if scenario == "partial":
        # Snapshot is stale (seeded from a persisted snapshot on cold start,
        # flows.py:347-386) rather than freshly computed — MoneyFlowCard
        # shows "Data as of {ageLabel}" instead of treating it as live.
        payload["snapshot_age_s"] = 21600.0  # 6h old
    return payload


# ── Journey (b): ticker investigation ────────────────────────────────
#
# These mirror the ACTUAL return dicts of api/routers/dad.py on the
# composed-g tree (read directly, not guessed), because
# pwa/src/views/TickerLookup.jsx reads a specific, non-obvious key set from
# each response (data.workbook.{evidence,files,sheets}, data.summary,
# data.gold, data.decision_stack, data.finviz, data.grid_data, data.signals,
# data.tradingview, data.dad_stats, data.source_lanes, data.fit_signals,
# data.risks, data.next_actions, data.status — see TickerLookup.jsx:277-292).
# An earlier version of these fixtures invented a smaller, wrong shape; it
# 200'd but rendered "NaN hits" (LanePill, TickerLookup.jsx:55, needs
# lane.file_hits + lane.sheet_hits + lane.evidence_rows, all three present)
# and "Price loading" forever (price_history items need a `value` key, not
# `close`). Router functions mirrored, with line numbers on composed-g as of
# 2026-09-18:
#   _build_compact_dad_response   dad.py:1916  (-> GET .../gold)
#   _assemble_dad_response        dad.py:1856  (assembles gold's full dict)
#   _load_workbook_context        dad.py:1682  (workbook/summary/source_lanes/
#                                                dad_stats/fit_signals)
#   _gold_from_summary            dad.py:864   (gold.{verdict,heuristic_score,...})
#   _grid_decision_stack          dad.py:1390  (decision_stack.{stance,cards,...})
#   _lane_counts                  dad.py:309   (source_lanes[i].{id,label,detail,
#                                                file_hits,sheet_hits,evidence_rows})
#   _dad_stat_cards               dad.py:341   (dad_stats[i].{id,label,state,why,
#                                                hits,prompt})
#   _tradingview_payload          dad.py:366   (tradingview.{chart_url,...})
#   _compact_finviz               dad.py:237   (finviz.{status,stats,field_count,...})
#   _compact_grid                 dad.py:256   (grid_data.{price_history[{date,value}],
#                                                metrics,source_freshness,...})
#   _compact_signals              dad.py:278   (signals.{signal_count,regime,...})
#   _build_evidence_payload       dad.py:1958  (-> GET .../evidence)
#   _build_chart_payload          dad.py:1989  (-> GET .../chart)
#   _build_finviz_payload         dad.py:2049  (-> GET .../finviz)
#   _build_options_payload        dad.py:2073  (-> GET .../options)
#   _options_history_context      dad.py:1190  (options.{latest,history,...})
#
# The view also opens an SSE stream, GET .../gold/stream (EventSource,
# api.js:370-399, streamDadTickerGold), before falling back to the four
# parallel GETs above on stream error (TickerLookup.jsx:399-406,
# startDetailStream's onError -> hydrateDetails). This fixture server has no
# route for .../gold/stream, so it 404s — that is the intended fallback
# path, not a bug, and is exercised on every lookup.

def _dad_workbook_summary(scenario: str) -> dict | None:
    if scenario == "empty":
        return None
    if scenario == "partial":
        return {"ticker": TICKER, "mentions": 3, "file_count": 1, "sheet_count": 1,
                "evidence_score": 2.0, "source_types": "cell"}
    return {"ticker": TICKER, "mentions": 8, "file_count": 2, "sheet_count": 3,
            "evidence_score": 9.0, "source_types": "sheet_name,filename"}


def _dad_workbook_evidence(scenario: str) -> list[dict]:
    if scenario == "empty":
        return []
    rows = [
        {"source_type": "sheet_name", "evidence_text": f"{TICKER} target multiple",
         "context_score": 4.0, "file": "test_workbook_2026.xlsx", "sheet": "Watchlist",
         "cell": "B12", "row_context": "position sizing notes", "column_header": "Ticker"},
    ]
    if scenario == "healthy":
        rows.append({"source_type": "filename", "evidence_text": f"{TICKER} historical CAGR vs QQQ",
                      "context_score": 5.0, "file": f"test_workbook_2026_{TICKER}.xlsx", "sheet": "Summary",
                      "cell": "A1", "row_context": "long-term chart + benchmark notes", "column_header": None})
    if scenario == "partial":
        # This row's own extraction is unresolved — no fabricated text.
        rows.append({"source_type": "cell", "evidence_text": None, "context_score": 0.0,
                      "file": "test_workbook_2026.xlsx", "sheet": "Notes", "cell": None,
                      "row_context": None, "column_header": None})
    return rows


def _dad_workbook_files(scenario: str) -> list[dict]:
    if scenario == "empty":
        return []
    return [{"file": "test_workbook_2026.xlsx", "mentions": 6, "score": 9.0}]


def _dad_workbook_sheets(scenario: str) -> list[dict]:
    if scenario == "empty":
        return []
    sheets = [{"file": "test_workbook_2026.xlsx", "sheet": "Watchlist", "mentions": 4, "score": 6.0}]
    if scenario == "healthy":
        sheets.append({"file": f"test_workbook_2026_{TICKER}.xlsx", "sheet": "Summary", "mentions": 2, "score": 5.0})
    return sheets


def _dad_source_lanes(scenario: str, evidence: list[dict], files: list[dict], sheets: list[dict]) -> list[dict]:
    """Mirrors dad.py:309 _lane_counts: classify by filename/sheet keywords,
    count evidence/file/sheet hits per lane, drop lanes with zero hits.
    """
    if not (evidence or files or sheets):
        return []
    workbook_lane = {
        "id": "workbook", "label": "Other Workbook",
        "detail": "Workbook evidence that needs manual classification.",
        "evidence_rows": len(evidence), "file_hits": sum(int(r.get("mentions") or 0) for r in files),
        "sheet_hits": 0,
    }
    lanes = [workbook_lane]
    if scenario == "healthy":
        lanes.append({
            "id": "dad_method", "label": "Dad Method",
            "detail": "Framework files: CAGR, historical charts, portfolio role, prudent entry, and benchmark context.",
            "evidence_rows": 0, "file_hits": 0,
            "sheet_hits": sum(int(r.get("mentions") or 0) for r in sheets),
        })
    return [lane for lane in lanes if lane["evidence_rows"] or lane["file_hits"] or lane["sheet_hits"]]


def _dad_stat_cards(scenario: str) -> list[dict]:
    """Mirrors dad.py:341 _dad_stat_cards / DAD_STAT_CATALOG (dad.py:83-121):
    six fixed cards, each 'present' or 'needed' depending on whether the
    workbook corpus text hits that card's keywords. Empty/partial corpora
    hit nothing, so every card reads 'needed' — that is the real behaviour,
    not a fixture bug.
    """
    catalog = [
        ("chart_quality", "Chart Quality",
         "Dad checks whether the long-term chart is up-and-right and whether it recovered from prior highs.",
         "Show 1Y, 5Y, 10Y trend, drawdown, recovery from high, and QQQ/SPY relative strength."),
        ("benchmark_fit", "Benchmark Fit",
         "He thinks in S&P/QQQ context: what sector or theme am I overweight, missing, or duplicating?",
         "Compare against QQQ/SPY and show whether it adds a new role or just repeats existing exposure."),
        ("prudent_entry", "Prudent Entry",
         "His buy decision wants zones: speculative, prudent, and back-up-the-truck levels.",
         "Show valuation context, current price versus target zones, and reason not to buy yet."),
        ("options_yield", "Options / Yield",
         "He uses covered calls and spreads to define payoff, premium, stop, and take-profit.",
         "Show premium, days to expiry, return if exercised, max loss, take-profit, and stop-loss."),
        ("portfolio_role", "Portfolio Role",
         "He wants to know what account, sleeve, and allocation role a name belongs in.",
         "Show sleeve, target size, current exposure, cost basis evidence, and rebalance impact."),
        ("risk_control", "Risk Control",
         "The spreadsheet language repeatedly checks downside, distance from high, stop-loss, and drawdown.",
         "Show what can break the thesis, downside level, debt/liquidity risk, and exit trigger."),
    ]
    present_ids = {"chart_quality", "portfolio_role"} if scenario == "healthy" else set()
    hits_by_id = {
        "chart_quality": ["historical", "cagr"],
        "portfolio_role": ["portfolio"],
    }
    cards = []
    for stat_id, label, why, prompt in catalog:
        present = stat_id in present_ids
        cards.append({
            "id": stat_id, "label": label,
            "state": "present" if present else "needed",
            "why": why,
            "hits": hits_by_id.get(stat_id, []) if present else [],
            "prompt": prompt,
        })
    return cards


def _dad_fit_signals(scenario: str, summary: dict | None) -> list[dict]:
    if not summary:
        return []
    signals = [{
        "label": "Workbook depth",
        "state": "strong" if int(summary.get("file_count") or 0) >= 3 else "partial",
        "detail": f"{summary.get('mentions', 0)} mentions across {summary.get('file_count', 0)} files "
                   f"and {summary.get('sheet_count', 0)} sheets",
    }]
    if scenario == "healthy":
        signals.append({"label": "Named at file level", "state": "strong",
                         "detail": "The ticker appears in workbook or file names, not only individual cells."})
    else:
        signals.append({"label": "Context still thin", "state": "neutral",
                         "detail": "Mentions were found, but the extractor has not classified clear decision language yet."})
    return signals


def _dad_gold(summary: dict | None) -> dict:
    """Mirrors dad.py:864 _gold_from_summary exactly (composed-g)."""
    if not summary:
        return {
            "verdict": "No workbook history yet",
            "heuristic_score": None,
            "weights": None,
            "score_basis": "no_workbook_history",
            "tone": "neutral",
            "one_liner": "This ticker is not showing up in Dad's copied workbook corpus yet.",
        }
    mentions = int(summary.get("mentions") or 0)
    file_count = int(summary.get("file_count") or 0)
    sheet_count = int(summary.get("sheet_count") or 0)
    evidence_score = float(summary.get("evidence_score") or 0)
    weights = {
        "evidence_score": {"weight": 2.5, "cap": None, "input": evidence_score, "points": round(evidence_score * 2.5, 2)},
        "file_count": {"weight": 8, "cap": None, "input": file_count, "points": round(file_count * 8, 2)},
        "sheet_count": {"weight": 2, "cap": None, "input": sheet_count, "points": round(sheet_count * 2, 2)},
        "mentions": {"weight": 1, "cap": 30, "input": mentions, "points": round(min(mentions, 30) * 1, 2)},
        "_clamp": {"min": 0, "max": 100},
        "_verdict_thresholds": {
            "high_workbook_conviction": {"heuristic_score": 80, "file_count": 3},
            "known_name": {"heuristic_score": 45},
            "light_workbook_footprint": {"heuristic_score": 15},
        },
    }
    raw_total = sum(v["points"] for k, v in weights.items() if not k.startswith("_"))
    score = min(100, round(raw_total))
    if score >= 80 and file_count >= 3:
        verdict, tone, one_liner = "High workbook conviction", "strong", "Dad's workbooks mention this ticker repeatedly across files and sheets."
    elif score >= 45:
        verdict, tone, one_liner = "Known name in Dad's research", "watch", "This has enough workbook footprint to deserve a serious look."
    elif score >= 15:
        verdict, tone, one_liner = "Light workbook footprint", "light", "There is workbook evidence, but not enough to treat it as a core Dad name."
    else:
        verdict, tone, one_liner = "Trace evidence only", "neutral", "Only a small amount of workbook evidence showed up for this ticker."
    return {
        "verdict": verdict, "heuristic_score": score, "weights": weights,
        "score_basis": "workbook_footprint_weighted_count", "tone": tone, "one_liner": one_liner,
    }


def _dad_finviz(scenario: str, *, include_fields: bool = False) -> dict:
    """Mirrors dad.py:237 _compact_finviz (composed-g)."""
    if scenario == "empty":
        stats: list[dict] = []
        status, freshness = "unavailable", {"state": "missing", "age_hours": None, "label": "missing"}
    else:
        stats = [
            {"field": "sector", "label": "Sector", "group": "profile", "raw_value": "Technology",
             "parsed": None, "numeric_value": None, "value_kind": "text"},
            {"field": "pe_ratio", "label": "P/E", "group": "valuation", "raw_value": "18.00",
             "parsed": 18.0, "numeric_value": 18.0, "value_kind": "numeric"},
        ]
        if scenario == "partial":
            # A field whose scrape came back non-numeric: text, not a fabricated 0.0.
            stats.append({"field": "dividend_pct", "label": "Dividend %", "group": "valuation",
                           "raw_value": "N/A", "parsed": None, "numeric_value": None, "value_kind": "text"})
            status, freshness = "stale", {"state": "stale", "age_hours": 168.0, "label": "stale"}
        else:
            status, freshness = "ready", {"state": "fresh", "age_hours": 4.0, "label": "fresh"}
    return {
        "status": status,
        "source": "postgres",
        "freshness": freshness,
        "latest_pull": "2026-09-15T06:00:00+00:00" if scenario != "empty" else None,
        "latest_obs_date": AS_OF_DATE if scenario != "empty" else None,
        "field_count": len(stats),
        "rows_inserted": 0,
        "live_refresh_requested": False,
        "refresh_available": True,
        "stats": stats,
        "error": None,
        **({"fields": {s["field"]: s for s in stats}} if include_fields else {}),
    }


def _dad_price_history(scenario: str) -> list[dict]:
    if scenario == "empty":
        return []
    # PriceChart / TickerLookup.jsx read `.value`, never `.close`.
    return [
        {"date": "2026-09-10", "value": 98.0},
        {"date": "2026-09-11", "value": 99.0},
        {"date": "2026-09-12", "value": 100.0},
        {"date": "2026-09-15", "value": 100.0},
    ]


def _dad_metrics(scenario: str, price_history: list[dict]) -> dict:
    if not price_history:
        return {}
    latest, first = price_history[-1]["value"], price_history[0]["value"]
    high = max(r["value"] for r in price_history)
    low = min(r["value"] for r in price_history)
    return {
        "latest_price": latest, "first_price": first, "window_days": 365, "window_label": "1Y",
        "window_start": price_history[0]["date"],
        "return_window_pct": ((latest - first) / first * 100) if first else None,
        "high_window": high, "low_window": low,
        "pct_from_window_high": ((latest - high) / high * 100) if high else None,
        "pct_above_window_low": ((latest - low) / low * 100) if low else None,
        "obs_count": len(price_history), "as_of": price_history[-1]["date"],
    }


def _dad_source_freshness(scenario: str) -> list[dict]:
    """Mirrors dad.py:979 _source_freshness for the six sources the chart
    endpoint always asks about; a source with no row still gets an entry
    ('missing'), it never disappears from the list.
    """
    sources = ["yfinance", "Finviz", "TradingView", "Social_Smart_Money", "SEC_INSIDER", "Unusual_Whales"]
    out = []
    for name in sources:
        if scenario != "empty" and name == "yfinance":
            state = "stale" if scenario == "partial" else "fresh"
            age = 168.0 if scenario == "partial" else 6.0
            out.append({"source": name, "last_pull": "2026-09-08T06:00:00+00:00" if scenario == "partial" else "2026-09-15T06:00:00+00:00",
                        "latest_raw_pull": None, "state": state, "age_hours": age, "label": state})
        else:
            out.append({"source": name, "last_pull": None, "latest_raw_pull": None,
                        "state": "missing", "age_hours": None, "label": "missing"})
    return out


def _dad_grid_data(scenario: str, *, include_price_history: bool) -> dict:
    """Mirrors dad.py:256 _compact_grid (composed-g)."""
    price_history = _dad_price_history(scenario) if include_price_history else []
    metrics = _dad_metrics(scenario, _dad_price_history(scenario))
    return {
        "status": "ready" if metrics else "missing",
        "feature_names": [f"{TICKER}_close"] if metrics else [],
        "metrics": metrics,
        "freshness": (
            {"state": "fresh", "age_hours": 6.0, "label": "fresh"} if metrics
            else {"state": "missing", "age_hours": None, "label": "missing"}
        ),
        "source_freshness": _dad_source_freshness(scenario),
        "features": [],
        "price_history": price_history,
        "price_points_total": len(_dad_price_history(scenario)),
        "price_points_returned": len(price_history),
    }


def _dad_signals(scenario: str, *, include_rows: bool) -> dict:
    """Mirrors dad.py:278 _compact_signals (composed-g)."""
    signal_rows = [] if scenario == "empty" else [
        {"source_type": "social", "source_id": "test-forum", "date": AS_OF_DATE, "signal_type": "BUY",
         "signal_value": {}, "trust_score": 0.4 if scenario == "healthy" else 0.0,
         "created_at": AS_OF_DATETIME},
    ]
    tv_rows: list[dict] = []
    payload = {
        "signal_count": len(signal_rows),
        "tradingview_count": len(tv_rows),
        "regime": None,
    }
    if include_rows:
        payload["signal_sources"] = signal_rows
        payload["tradingview_signals"] = tv_rows
    return payload


def _dad_options_context(scenario: str, *, days: int = 90, limit: int = 90) -> dict:
    """Mirrors dad.py:1190 _options_history_context (composed-g)."""
    if scenario == "empty":
        return {"status": "missing", "latest": None, "history": [], "days": days, "limit": limit,
                "freshness": {"state": "missing", "age_hours": None, "label": "missing"}}
    latest = {
        "date": AS_OF_DATE, "put_call_ratio": 0.9, "max_pain": 100.0, "iv_skew": 0.02,
        "total_oi": 500, "total_volume": 120, "spot_price": 100.0, "iv_atm": 0.35,
        "term_slope": 0.01, "oi_concentration": 0.3,
    }
    return {
        "status": "ready", "latest": latest, "history": [latest], "days": days, "limit": limit,
        "freshness": {"state": "fresh" if scenario == "healthy" else "aging",
                       "age_hours": 6.0 if scenario == "healthy" else 96.0,
                       "label": "fresh" if scenario == "healthy" else "aging"},
    }


def _dad_decision_stack(scenario: str, summary: dict | None, gold: dict) -> dict:
    """Simplified but structurally accurate mirror of dad.py:1390
    _grid_decision_stack: same key set (stance/tone/heuristic_score/weights/
    cards/reasons/blockers/method), synthetic point values.
    """
    gold_score = gold.get("heuristic_score") or 0
    cards = [
        {"source": "Dad workbooks",
         "state": "strong" if summary and int(summary.get("file_count") or 0) >= 3 else ("watch" if summary else "missing"),
         "points": round(gold_score * 0.6, 1), "detail": gold.get("one_liner") or "No workbook prior."},
        {"source": "GRID price history", "state": "watch" if scenario != "empty" else "missing",
         "points": 6.0 if scenario != "empty" else 0.0,
         "detail": "1Y +2.0%, 0.0% from 1Y high" if scenario != "empty" else "No GRID chart history."},
        {"source": "Finviz fundamentals",
         "state": "caution" if scenario == "partial" else ("watch" if scenario == "healthy" else "missing"),
         "points": 4.0 if scenario == "healthy" else 0.0,
         # Must agree with FinvizPanel's own "unavailable, 0 fields, missing"
         # for this same scenario (dad_ticker_finviz / _dad_finviz above) —
         # an earlier version of this card said "2 fields, fresh" even when
         # `empty` had zero Finviz rows, contradicting the Finviz panel in
         # the same response.
         "detail": (
             "Finviz fundamentals are stale; refresh before making the call." if scenario == "partial"
             else "0 fields, missing" if scenario == "empty"
             else "2 fields, fresh"
         ),
         "inputs": {"forward_pe": 18.0 if scenario != "empty" else None, "roe": None, "debt_equity": None,
                     "profit_margin": None, "eps_next_5y": None},
         "skipped_fields": ["roe", "debt_equity", "profit_margin", "eps_next_5y"]},
        {"source": "GRID options", "state": "watch" if scenario != "empty" else "missing",
         "points": 2.0 if scenario != "empty" else 0.0,
         "detail": f"Latest options date {AS_OF_DATE}" if scenario != "empty" else "No options_daily_signals row."},
        {"source": "GRID signals", "state": "missing", "points": 0.0,
         "detail": "0 trusted signal rows, 0 TradingView alerts"},
    ]
    total = round(sum(c["points"] for c in cards), 1)
    if total >= 70:
        stance, tone = "Deep review first", "strong"
    elif total >= 45:
        stance, tone = "Watchlist with checks", "watch"
    elif total >= 25:
        stance, tone = "Needs more evidence", "caution"
    else:
        stance, tone = "Do not surface hard yet", "light"
    blockers = ["Finviz fundamentals are stale; refresh before making the call."] if scenario == "partial" else []
    reasons = ["Workbook prior exists, but GRID needs more current confirmation."] if summary else \
        ["GRID does not have enough current evidence for this ticker yet."]
    return {
        "stance": stance, "stance_basis": "heuristic_score_thresholds", "tone": tone,
        "heuristic_score": total, "weights": {"workbook_prior_multiplier": 0.6},
        "score_basis": "hand_picked_point_awards",
        "skipped_terms": {"finviz": ["roe", "debt_equity", "profit_margin", "eps_next_5y"]},
        "cards": cards, "reasons": reasons[:5], "blockers": blockers[:6],
        "method": ("Hand-picked point awards over the workbook prior plus GRID price, "
                   "fundamentals, options, signal, regime and freshness checks. Not a "
                   "backtested or calibrated conviction score - every weight is in `weights`."),
    }


def _dad_tradingview(ticker: str) -> dict:
    """Mirrors dad.py:366 _tradingview_payload exactly."""
    symbol = ticker.replace(".", "-")
    tv_symbol = f"NASDAQ:{symbol}"
    encoded = tv_symbol.replace(":", "%3A")
    return {
        "symbol": tv_symbol,
        "chart_url": f"https://www.tradingview.com/chart/?symbol={encoded}",
        "symbol_search_url": f"https://www.tradingview.com/symbols/{symbol}/",
        "webhook_note": "GRID can show TradingView webhook alerts already sent into /api/v1/tradingview/webhook.",
    }


def dad_ticker_gold(ticker: str, scenario: str) -> dict:
    """GET /api/v1/dad/ticker/{ticker}/gold -> mirrors
    _build_compact_dad_response / _assemble_dad_response (compact=True)."""
    summary = _dad_workbook_summary(scenario)
    evidence = _dad_workbook_evidence(scenario)
    files = _dad_workbook_files(scenario)
    sheets = _dad_workbook_sheets(scenario)
    gold = _dad_gold(summary)
    finviz = _dad_finviz(scenario, include_fields=False)
    grid_data = _dad_grid_data(scenario, include_price_history=False)
    signals = _dad_signals(scenario, include_rows=False)
    decision_stack = _dad_decision_stack(scenario, summary, gold)
    risks = ["Workbook footprint is historical evidence, not a live buy/sell recommendation.",
             "Current price, fundamentals, news, and liquidity still need a fresh market check."]
    next_actions = ["Compare the workbook evidence against a current chart and fundamentals pass.",
                     "Check whether Dad's workbook language is buy, watch, hold, or sell before acting."]
    if scenario == "empty":
        risks.append("Regex extraction found no confident workbook footprint for this ticker.")
        next_actions.insert(0, "Try the company name or related ticker if this was renamed, delisted, or crypto-like.")
    return {
        "ticker": ticker,
        "status": "ready" if summary else "not_found",
        "payload_mode": "compact",
        "gold": gold,
        "decision_stack": decision_stack,
        "source": {"attached": scenario != "empty", "db_path": None},
        "message": None,
        "summary": summary,
        "workbook": {"files": files, "sheets": sheets, "evidence": evidence},
        "source_lanes": _dad_source_lanes(scenario, evidence, files, sheets),
        "dad_stats": _dad_stat_cards(scenario),
        "finviz": finviz,
        "grid_data": grid_data,
        "options": _dad_options_context(scenario)["latest"],
        "signals": signals,
        "tradingview": _dad_tradingview(ticker),
        "fit_signals": _dad_fit_signals(scenario, summary),
        "risks": risks,
        "next_actions": next_actions,
        "detail_urls": {
            "evidence": f"/api/v1/dad/ticker/{ticker}/evidence",
            "chart": f"/api/v1/dad/ticker/{ticker}/chart",
            "finviz": f"/api/v1/dad/ticker/{ticker}/finviz",
            "options": f"/api/v1/dad/ticker/{ticker}/options",
        },
        "hydration": {"compact": True, "evidence": "inline_sample", "chart": "detail_endpoint",
                       "finviz": "summary", "options": "latest"},
        "performance": {"timings_ms": {}, "total_ms": 0.0},
        "cache": {"hit": False, "ttl_seconds": 300},
    }


def dad_ticker_evidence(ticker: str, scenario: str) -> dict:
    """GET .../evidence -> mirrors _build_evidence_payload (dad.py:1958)."""
    summary = _dad_workbook_summary(scenario)
    evidence = _dad_workbook_evidence(scenario)
    files = _dad_workbook_files(scenario)
    sheets = _dad_workbook_sheets(scenario)
    return {
        "ticker": ticker,
        "status": "ready" if summary else "not_found",
        "summary": summary,
        "workbook": {"files": files, "sheets": sheets, "evidence": evidence},
        "source_lanes": _dad_source_lanes(scenario, evidence, files, sheets),
        "dad_stats": _dad_stat_cards(scenario),
        "fit_signals": _dad_fit_signals(scenario, summary),
        "page": {"limit": 50, "offset": 0, "returned": len(evidence)},
        "performance": {"timings_ms": {}, "total_ms": 0.0},
    }


def dad_ticker_chart(ticker: str, scenario: str) -> dict:
    """GET .../chart -> mirrors _build_chart_payload (dad.py:1989)."""
    grid_data = _dad_grid_data(scenario, include_price_history=True)
    signals = _dad_signals(scenario, include_rows=True)
    return {
        "ticker": ticker,
        "status": grid_data["status"],
        "payload_mode": "chart",
        "range": "1Y",
        "points": 260,
        "grid_data": grid_data,
        "price_history": grid_data["price_history"],
        "metrics": grid_data["metrics"],
        "features": grid_data["features"],
        "source_freshness": grid_data["source_freshness"],
        "tradingview_signals": signals.get("tradingview_signals", []),
        "regime": signals.get("regime"),
        "signals": signals,
        "performance": {"timings_ms": {}, "total_ms": 0.0},
    }


def dad_ticker_finviz(ticker: str, scenario: str) -> dict:
    """GET .../finviz -> mirrors _build_finviz_payload (dad.py:2049).
    Response is wrapped in a top-level `finviz` key; TickerLookup.jsx merges
    `finvizResult.finviz` into `data.finviz` (mergeTickerData, line 111-113).
    """
    finviz = _dad_finviz(scenario, include_fields=True)
    return {
        "ticker": ticker,
        "status": finviz["status"],
        "payload_mode": "finviz",
        "finviz": finviz,
        "performance": {"timings_ms": {}, "total_ms": 0.0},
    }


def dad_ticker_options(ticker: str, scenario: str) -> dict:
    """GET .../options -> mirrors _build_options_payload (dad.py:2073).
    Response is wrapped in a top-level `options` key; TickerLookup.jsx merges
    `optionsResult.options` into `data.options` (mergeTickerData, line 114-116).
    """
    ctx = _dad_options_context(scenario)
    return {
        "ticker": ticker,
        "status": ctx["status"],
        "payload_mode": "options",
        "options": ctx,
        "latest": ctx["latest"],
        "history": ctx["history"],
        "freshness": ctx["freshness"],
        "performance": {"timings_ms": {}, "total_ms": 0.0},
    }


def catalyst_timeline(ticker: str, scenario: str) -> dict:
    """GET /api/v1/valuation/catalyst-timeline/{ticker}.

    Exercises the three-kind contract in one payload: a milestone whose
    probability was never scored (``None`` -> "unscored"), a milestone whose
    probability was measured at exactly 0 (a real "we think this will not
    happen" reading, distinct from unscored), and a measured 0.0 value-impact
    that must render as a real number, not a blank/missing field.
    """
    if ticker.upper() == "ACME":
        # Supplied by the session that owns CatalystTimeline.jsx /
        # valuation.py::catalyst_timeline (see README "Provenance of the
        # ACME catalyst-timeline fixture"). Served in every scenario so the
        # view's null-vs-zero contract can be checked regardless of scenario.
        return _load_json_fixture("catalyst-timeline-ACME.json")

    if scenario == "empty":
        return {"ticker": ticker, "status": "ok", "events": [], "as_of": AS_OF_DATE}

    events = [
        {
            "id": "ms-1",
            "type": "milestone",
            "subtype": "product_launch",
            "date": "2026-09-20",
            "label": "TEST1 launch checkpoint",
            "status": "PENDING",
            "target_value": None,
            "target_unit": None,
            "actual_value": None,
            "achievement_pct": None,
            # Unscored: nobody has assigned a probability to this node yet.
            "probability": None,
            "confidence_source": None,
            "value_impact_ps": None,
            "value_impact_pct": None,
            "notes": "unscored node — no probability has been assigned",
            "invalidation": "No target value set for product_launch",
        },
        {
            "id": "ms-2",
            "type": "milestone",
            "subtype": "regulatory_decision",
            "date": "2026-09-25",
            "label": "TEST1 filing decision",
            "status": "PENDING",
            "target_value": 1,
            "target_unit": "approval",
            "actual_value": None,
            "achievement_pct": None,
            # Measured at exactly 0 — a real "expected to fail" reading, not
            # the absence of a reading. Must render as "0%", never blank.
            "probability": 0,
            "confidence_source": "analyst_model",
            # Measured 0.0 impact (a genuine "no expected effect" reading),
            # distinct from an unmeasured null. Must render as "0.0", not a gap.
            "value_impact_ps": 0.0,
            "value_impact_pct": 0.0,
            "notes": "measured 0.0 — a real zero, not an unavailable reading",
            "invalidation": "Filing rejected or withdrawn before 2026-09-25",
        },
    ]
    if scenario == "healthy":
        events.append({
            "id": "pred-1",
            "type": "oracle_prediction",
            "subtype": "price_target",
            "date": "2026-09-30",
            "label": "TEST1 30d price target",
            "status": "OPEN",
            "direction": "bullish",
            "confidence": 0.55,
            "value_impact_ps": 3.0,
            "value_impact_pct": 3.0,
            "notes": None,
            "invalidation": "Price closes below entry for 3 consecutive sessions",
        })
    return {"ticker": ticker, "status": "ok", "events": events, "as_of": AS_OF_DATE}


# ── Journey (c): watchlist / portfolio, /edge, trust+convergence ────

def watchlist_portfolio(scenario: str) -> dict:
    """GET /api/v1/watchlist/portfolio -> mirrors api/routers/watchlist_core.py
    ::get_portfolio (composed-g, ~line 133-379) exactly.

    Two things an earlier version of this fixture got wrong, both because it
    guessed the shape instead of reading the router:
      1. Position keys are ticker/display_name/price/change_1d/change_1w/
         weight/sector/asset_type — NOT custom_weight/return_1d_pct/has_price.
         Portfolio.jsx:277 reads `p.weight` (a 0..1 fraction) and renders
         `(p.weight * 100).toFixed(1)}%`; the wrong key name rendered "NaN%".
      2. A holding with no price is NOT included in `positions` with a null
         price — it is dropped from `positions` entirely and counted in
         `positions_missing_price` / listed in `missing_price_tickers`
         instead (get_portfolio's docstring: "a holding with no price is
         not a holding worth zero: it is a holding we could not measure").
    There are deliberately no dollar P&L keys at all (no total_value,
    no per-position pnl_1d) — see the README's "defects observed" list for
    what that means for Portfolio.jsx, which still reads some of them.
    """
    if scenario == "empty":
        return {
            "total_value": None,
            "total_value_basis": "GRID stores no position sizes or cost basis for watchlist items",
            "weighted_return_1d_pct": None,
            "return_1d_weight_coverage": None,
            "positions": [],
            "positions_missing_price": 0,
            "missing_price_tickers": [],
            "weight_priced_total": 0.0,
            "allocation": {"by_sector": {}, "by_asset_type": {}},
            "risk_metrics": {
                "concentration_top3": None,
                "beta_proxy_by_asset_class": None,
                "beta_proxy_basis": "no per-position beta measured; proxy by asset class only",
                "sector_diversification_score": None,
            },
            "options_pnl": {"total_recommendations": 0, "wins": 0, "losses": 0, "open": 0, "total_return": 0},
        }
    position = {
        "ticker": TICKER,
        "display_name": "Test One Corp",
        "price": 100.0,
        "change_1d": 0.0,
        # Fraction, not percent — same convention as change_1d
        # (watchlist_helpers.py `pct_1w = round((last-first)/first, 5)`).
        # An earlier version used 1.0 here (= "+100.00%" once Portfolio.jsx's
        # fmtPct multiplies by 100), a straight units-convention mistake.
        "change_1w": 0.012,
        "weight": 1.0,
        "sector": "Technology",
        "asset_type": "stock",
    }
    positions = [position]
    missing_price_tickers: list[str] = []
    if scenario == "partial":
        # TEST2 is on the watchlist but has no price at all: it never enters
        # `positions` (a missing price is not a 0% holding), it is excluded
        # from weight_priced_total / the weighted return, and is reported
        # only in missing_price_tickers.
        missing_price_tickers = ["TEST2"]
    return {
        "total_value": None,
        "total_value_basis": "GRID stores no position sizes or cost basis for watchlist items",
        "weighted_return_1d_pct": 0.0,
        "return_1d_weight_coverage": 1.0,
        "positions": positions,
        "positions_missing_price": len(missing_price_tickers),
        "missing_price_tickers": missing_price_tickers,
        "weight_priced_total": sum(p["weight"] for p in positions),
        "allocation": {"by_sector": {"Technology": 1.0}, "by_asset_type": {"stock": 1.0}},
        "risk_metrics": {
            "concentration_top3": 1.0,
            "beta_proxy_by_asset_class": 1.0,
            "beta_proxy_basis": "asset-class proxy, not a measured per-position beta",
            "sector_diversification_score": 0.0,
        },
        "options_pnl": {"total_recommendations": 0, "wins": 0, "losses": 0, "open": 0, "total_return": 0},
    }


def ticker_edge(ticker: str, scenario: str) -> dict:
    if scenario == "empty":
        return {
            "ticker": ticker,
            "congressional": [],
            "insider": [],
            "dark_pool": None,
            "whale_flow": [],
            "prediction_markets": [],
            "smart_money": [],
            "convergence": [],
        }
    congressional = [
        {
            "member": "Test Member",
            "action": "BUY",
            "amount": "$1,001 - $15,000",
            "date": "2026-09-05",
            "committee": "N/A",
            # Measured trust, not a default midpoint.
            "trust_score": 0.6,
        },
    ]
    smart_money = [
        {
            "source": "test-forum",
            "user": "test_user",
            "direction": "BUY",
            # Unscored: this source has never been graded. Must render as
            # "unknown"/"--", never as a 0.5 midpoint.
            "trust_score": None,
        },
    ]
    payload = {
        "ticker": ticker,
        "congressional": congressional,
        "insider": [
            {
                "name": "Test Insider",
                "title": "CFO",
                "action": "BUY",
                "shares": 500,
                "value": 50000,
                "date": "2026-09-04",
                "cluster": False,
            },
        ],
        # Mirrors watchlist_overview.py:572-578 (composed-g) exactly:
        # {volume_vs_avg, signal, date} — not {available, relative_volume,
        # as_of}. An earlier version used the wrong shape; WatchlistAnalysis
        # .jsx:391 reads `dark_pool.volume_vs_avg?.toFixed(1)`, and since
        # `undefined` coerces to the literal string "undefined" in a
        # template literal, the panel rendered "undefinedx avg volume".
        "dark_pool": {"volume_vs_avg": 1.0, "signal": "accumulation", "date": AS_OF_DATE},
        "whale_flow": [
            {"strike": 100.0, "expiry": "2026-10-16", "direction": "BUY", "premium": 25000, "date": "2026-09-12"},
        ],
        "prediction_markets": [],
        "smart_money": smart_money,
        "convergence": [],
    }
    if scenario == "partial":
        # volume_vs_avg is unmeasured (null, never a fabricated 1.0) while
        # the rest of /edge is fine — this dark-pool print's metadata simply
        # didn't carry a volume_vs_avg (composed-g's own comment, same file).
        payload["dark_pool"] = {"volume_vs_avg": None, "signal": "accumulation", "date": AS_OF_DATE}
    return payload


def intelligence_dashboard(scenario: str) -> dict:
    if scenario == "empty":
        return {
            "trust": {"top_sources": [], "convergence_events": [], "total_tracked": 0},
            "levers": {"active_events": [], "top_pullers": []},
            "cross_ref": {"red_flags": [], "total_checks": 0},
            "source_audit": {"discrepancies": [], "single_points_of_failure": 0},
            "postmortems": {"recent_failures": [], "lessons": ""},
            "overall_confidence": None,
            "narrative": "",
            "generated_at": GENERATED_AT,
            "errors": [],
        }
    top_sources = [
        {
            "source_type": "congressional",
            "source_id": "test-member",
            "trust_score": 0.61,
            "hit_count": 4,
            "miss_count": 2,
            "total_signals": 6,
            "win_rate": 0.6667,
            "avg_lead_time_hours": 84.0,
            "avg_return_on_hits": 0.031,
            "best_ticker": TICKER,
            "last_signal_date": "2026-09-05",
            "rank": 1,
        },
    ]
    convergence_events = [
        {
            "ticker": TICKER,
            "signal_type": "BUY",
            "direction": "bullish",
            "direction_basis": "majority_of_sources_agree",
            "source_count": 3,
            "scored_source_count": 2,
            "sources": [
                {"source_type": "congressional", "source_id": "test-member", "trust_score": 0.61, "signal_date": "2026-09-05"},
                {"source_type": "insider", "source_id": "test-insider", "trust_score": 0.58, "signal_date": "2026-09-04"},
                {"source_type": "social", "source_id": "test-forum", "trust_score": None, "signal_date": "2026-09-03"},
            ],
            "combined_confidence": 0.595,
            "confidence_basis": "mean_trust_of_scored_sources",
            "detected_at": GENERATED_AT,
        },
    ]
    if scenario == "partial":
        # A second, real 3-source convergence exists but none of its sources
        # is scored yet: combined_confidence is None (never a 0.5 midpoint),
        # and it must sort AFTER the scored event above, never treated as 0.
        convergence_events.append({
            "ticker": "TEST2",
            "signal_type": "SELL",
            "direction": None,
            "direction_basis": "sources_disagree_or_unresolved",
            "source_count": 3,
            "scored_source_count": 0,
            "sources": [
                {"source_type": "congressional", "source_id": "test-member-2", "trust_score": None, "signal_date": "2026-09-02"},
                {"source_type": "insider", "source_id": "test-insider-2", "trust_score": None, "signal_date": "2026-09-02"},
                {"source_type": "social", "source_id": "test-forum-2", "trust_score": None, "signal_date": "2026-09-01"},
            ],
            "combined_confidence": None,
            "confidence_basis": "unscored",
            "detected_at": GENERATED_AT,
        })
    return {
        "trust": {"top_sources": top_sources, "convergence_events": convergence_events, "total_tracked": len(top_sources)},
        "levers": {"active_events": [], "top_pullers": []},
        "cross_ref": {"red_flags": [], "total_checks": 3},
        "source_audit": {"discrepancies": [], "single_points_of_failure": 0},
        "postmortems": {"recent_failures": [], "lessons": ""},
        "overall_confidence": 0.6,
        "narrative": "Fixture snapshot for browser-acceptance testing.",
        "generated_at": GENERATED_AT,
        "errors": [] if scenario == "healthy" else ["volatility_risk: no VIX close series in resolved_series"],
    }


# ── Journey (d): research status (no dedicated page — see README) ──
# Closest analog wired up in the PWA is Pipeline Health (api.getPipelineHealth
# -> GET /api/v1/system/pipeline-health), which is what Hermes/research
# scheduling actually surfaces today.

def _field_record(
    *, availability, provenance=None, value=None, unit=None,
    obs_date=None, obs_start=None, obs_end=None,
    published_at=None, available_at=None, ingested_at=None,
    revision=None, source_catalog=None, series_id=None,
    calculation_version=None, coverage_fraction=None,
    coverage_count=None, coverage_expected=None, stale_reason=None,
):
    """Mirrors store/availability_fields.py::FieldRecord.to_dict() exactly
    (read at commit b5babef4, lines ~166-190: availability/provenance/value/
    unit/obs_date/obs_start/obs_end/published_at/available_at/ingested_at/
    revision/source_catalog/series_id/calculation_version/
    coverage_fraction/coverage_count/coverage_expected/stale_reason).
    """
    return {
        "availability": availability, "provenance": provenance, "value": value, "unit": unit,
        "obs_date": obs_date, "obs_start": obs_start, "obs_end": obs_end,
        "published_at": published_at, "available_at": available_at, "ingested_at": ingested_at,
        "revision": revision, "source_catalog": source_catalog, "series_id": series_id,
        "calculation_version": calculation_version, "coverage_fraction": coverage_fraction,
        "coverage_count": coverage_count, "coverage_expected": coverage_expected,
        "stale_reason": stale_reason,
    }


# ── God View pillars (W6) ─────────────────────────────────────────────
# GET /api/v1/godview/pillars/{cftc|<other>} -> mirrors
# api/routers/godview_pillars.py (read at commit 7560fb02) exactly, plus
# godview/cftc_pillar.py (same commit, PILLAR_NAME/CFTC_PILLAR_CONTRACTS/
# STALE_AFTER_DAYS/read_cftc_pillar/PillarReadResult) and
# store/availability_fields.py's measured_field/derived_field/
# unavailable_field -> FieldRecord.to_dict() (same 17-key shape as
# _field_record above; ingested_at is NEVER set by this router's
# _row_to_field_records — its `common` dict has no ingested_at key, so it
# stays None even on a fully healthy read).
#
# The route id is `godview` (not `godview-pillars`), confirmed directly:
# `git show d197c9a3 -- pwa/src/routes.js` adds `{id: 'godview', ...,
# component: './views/GodViewPillars.jsx'}`. There are SIX (not five)
# known-unbuilt pillar names in `_KNOWN_UNBUILT_PILLARS`
# (api/routers/godview_pillars.py): finra_short_volume, sec_regsho_ftd,
# commodity_warehouses, fed_net_liquidity, buyback_blackouts, dealer_gex.

GODVIEW_PILLAR_NAME = "cftc_positioning"
GODVIEW_UNBUILT_PILLARS = (
    "finra_short_volume", "sec_regsho_ftd", "commodity_warehouses",
    "fed_net_liquidity", "buyback_blackouts", "dealer_gex",
)
# key -> contract_code, matching godview/cftc_pillar.py::CFTC_PILLAR_CONTRACTS.
GODVIEW_CFTC_CONTRACTS = {"SP500": "ES", "NOTE10Y": "ZN", "GOLD": "GC", "CRUDE_OIL": "CL"}

_GODVIEW_RAW_FIELD_UNIT = {
    "total_open_interest": "contracts", "commercial_long": "contracts",
    "commercial_short": "contracts", "commercial_net": "contracts",
    "noncommercial_long": "contracts", "noncommercial_short": "contracts",
    "noncommercial_net": "contracts", "spec_net_pct_oi": "pct",
}
_GODVIEW_DERIVED_FIELD_UNIT = {
    "z_score_1y": "zscore", "z_score_3y": "zscore",
    "percentile_3y": "pct", "crowding_regime": None,
}


def _godview_unavailable(reason: str, pillar: str | None = None) -> dict:
    """Mirrors store/availability.py::unavailable() exactly: every name
    passed as a `**fields` kwarg is emitted as None, INCLUDING `pillar`
    itself — api/routers/godview_pillars.py always calls
    `unavailable(..., source=PILLAR_NAME_OR_pillar_name, pillar=..., coverage=None)`,
    so the response's own `pillar` key is always null, never the pillar name
    (that's `source`'s job).
    """
    return {
        "available": False, "status": "unavailable", "reason": reason,
        "as_of": None, "source": pillar, "pillar": None, "coverage": None,
    }


def _godview_field(
    *, availability, provenance=None, value=None, unit=None,
    obs_date=None, published_at=None, available_at=None,
    revision=None, source_catalog=None, series_id=None,
    calculation_version=None, coverage_fraction=None, stale_reason=None,
):
    """Same 17-key FieldRecord.to_dict() shape as `_field_record` above —
    kept as its own helper because this pillar never sets ingested_at/
    obs_start/obs_end/coverage_count/coverage_expected, unlike some future
    pillar might.
    """
    return {
        "availability": availability, "provenance": provenance, "value": value, "unit": unit,
        "obs_date": obs_date, "obs_start": None, "obs_end": None,
        "published_at": published_at, "available_at": available_at, "ingested_at": None,
        "revision": revision, "source_catalog": source_catalog, "series_id": series_id,
        "calculation_version": calculation_version, "coverage_fraction": coverage_fraction,
        "coverage_count": None, "coverage_expected": None, "stale_reason": stale_reason,
    }


def _godview_contract_fields(contract_code: str, *, report_date, release_date, available_at,
                              generation_id, values: dict, coverage_fraction) -> dict:
    common = dict(
        obs_date=report_date, published_at=release_date, available_at=available_at,
        revision=generation_id, source_catalog="raw_series:cftc", series_id=contract_code,
    )
    out = {}
    for name, unit in _GODVIEW_RAW_FIELD_UNIT.items():
        out[name] = _godview_field(availability="available", provenance="measured",
                                     value=values[name], unit=unit, **common)
    for name, unit in _GODVIEW_DERIVED_FIELD_UNIT.items():
        value = values.get(name)
        if value is None:
            out[name] = _godview_field(
                availability="unavailable", unit=unit, calculation_version="cftc_pillar_v1",
                coverage_fraction=coverage_fraction, stale_reason="partial_history", **common,
            )
        else:
            out[name] = _godview_field(
                availability="available", provenance="derived", value=value, unit=unit,
                calculation_version="cftc_pillar_v1", coverage_fraction=coverage_fraction, **common,
            )
    return out


def godview_pillar_cftc(scenario: str) -> dict:
    """GET /api/v1/godview/pillars/cftc?as_of=... -> mirrors
    api/routers/godview_pillars.py::get_cftc_pillar exactly.

    - empty: never configured -> `_godview_unavailable("never_configured", GODVIEW_PILLAR_NAME)`
      (mirrors `unavailable(STALE_NEVER_CONFIGURED, source=PILLAR_NAME, pillar=PILLAR_NAME,
      coverage=None)` — no `generation_id`/`fields` at all, matching the
      router's early return before any generation is even looked up).
    - healthy: one complete generation, all 4 tracked contracts present,
      `report_date` a Tuesday (2026-09-15), `release_date` the following
      Friday (2026-09-18, `report_date + 3 days` per the contract doc
      section 2), coverage 1.0, stale_reason null, status "ok".
    - partial: CRUDE_OIL (CL) has `release_date IS NULL` in the underlying
      table, so the strict-PIT query's `WHERE release_date IS NOT NULL`
      (cftc_pillar.py's `read_cftc_pillar`) excludes it entirely —
      `contracts_with_data=3`, `coverage=0.75`, status "partial". The
      remaining 3 contracts' newest qualifying release is old enough that
      `age_days > STALE_AFTER_DAYS` (10), so top-level `stale_reason` is
      "stale". GOLD (GC) additionally has a short derived-history window:
      `coverage_fraction=0.6` on its row, and its `z_score_3y`/
      `percentile_3y`/`crowding_regime` fields are `unavailable_field
      (STALE_PARTIAL_HISTORY, ...)` (only `z_score_1y` still has enough
      history to compute) — this is the per-FIELD `stale_reason`, whose
      real value is `"partial_history"`, not `"stale"` (`"stale"` is only
      ever a top-level `stale_reason`, never a per-field one — the router
      has no code path that sets a field's `stale_reason` to `"stale"`).
    """
    if scenario == "empty":
        return _godview_unavailable("never_configured", GODVIEW_PILLAR_NAME)

    generation_id = "gen-20260918-01"
    generation_published_at = "2026-09-18T15:50:00+00:00"
    report_date = "2026-09-15"       # Tuesday
    release_date = "2026-09-18"      # report_date + 3 days (Friday)
    available_at = "2026-09-18T15:45:00+00:00"

    base_values = {
        "SP500": dict(total_open_interest=500000, commercial_long=200000, commercial_short=180000,
                       commercial_net=20000, noncommercial_long=150000, noncommercial_short=140000,
                       noncommercial_net=10000, spec_net_pct_oi=2.0,
                       z_score_1y=0.5, z_score_3y=0.6, percentile_3y=70.0, crowding_regime="neutral"),
        "NOTE10Y": dict(total_open_interest=300000, commercial_long=140000, commercial_short=120000,
                          commercial_net=20000, noncommercial_long=90000, noncommercial_short=95000,
                          noncommercial_net=-5000, spec_net_pct_oi=-1.7,
                          z_score_1y=-0.3, z_score_3y=-0.4, percentile_3y=35.0, crowding_regime="neutral"),
        "GOLD": dict(total_open_interest=450000, commercial_long=210000, commercial_short=230000,
                      commercial_net=-20000, noncommercial_long=160000, noncommercial_short=120000,
                      noncommercial_net=40000, spec_net_pct_oi=8.9,
                      z_score_1y=1.8, z_score_3y=None, percentile_3y=None, crowding_regime=None),
        "CRUDE_OIL": dict(total_open_interest=1800000, commercial_long=900000, commercial_short=950000,
                            commercial_net=-50000, noncommercial_long=600000, noncommercial_short=550000,
                            noncommercial_net=50000, spec_net_pct_oi=2.8,
                            z_score_1y=0.2, z_score_3y=0.1, percentile_3y=55.0, crowding_regime="neutral"),
    }

    if scenario == "healthy":
        included = ["SP500", "NOTE10Y", "GOLD", "CRUDE_OIL"]
        # Give GOLD its 3y fields back for the fully-healthy scenario.
        base_values["GOLD"] = dict(base_values["GOLD"], z_score_3y=2.1, percentile_3y=96.0, crowding_regime="extreme")
        coverage_fractions = {"SP500": 1.0, "NOTE10Y": 1.0, "GOLD": 1.0, "CRUDE_OIL": 1.0}
        stale_reason = None
        newest_release = release_date
    else:  # partial
        included = ["SP500", "NOTE10Y", "GOLD"]  # CRUDE_OIL excluded: release_date IS NULL
        coverage_fractions = {"SP500": 1.0, "NOTE10Y": 1.0, "GOLD": 0.6}
        stale_reason = "stale"
        newest_release = "2026-09-04"  # > STALE_AFTER_DAYS (10) before as_of 2026-09-18

    contracts_with_data = len(included)
    contracts_expected = len(GODVIEW_CFTC_CONTRACTS)
    coverage = contracts_with_data / contracts_expected

    fields_by_contract = {}
    for key in included:
        code = GODVIEW_CFTC_CONTRACTS[key]
        fields_by_contract[code] = _godview_contract_fields(
            code, report_date=report_date if scenario == "healthy" else newest_release,
            release_date=release_date if scenario == "healthy" else newest_release,
            available_at=available_at, generation_id=generation_id,
            values=base_values[key], coverage_fraction=coverage_fractions[key],
        )

    return {
        "available": True,
        "status": "ok" if (coverage == 1.0 and stale_reason is None) else "partial",
        "pillar": GODVIEW_PILLAR_NAME,
        "as_of": "2026-09-18",
        "coverage": coverage,
        "contracts_with_data": contracts_with_data,
        "contracts_expected": contracts_expected,
        "stale_reason": stale_reason,
        "generation_id": generation_id,
        "generation_published_at": generation_published_at,
        "contracts": dict(GODVIEW_CFTC_CONTRACTS),
        "fields": fields_by_contract,
    }


def godview_pillar_unbuilt(pillar_name: str) -> dict:
    """GET /api/v1/godview/pillars/{pillar_name} for any pillar other than
    `cftc` -> mirrors api/routers/godview_pillars.py::get_pillar_not_built
    exactly: a known-but-unbuilt name gets "not built yet — no data"; any
    other name gets "unknown godview pillar: {name}" — both via the same
    `unavailable()` shape, never a 404.
    """
    if pillar_name not in GODVIEW_UNBUILT_PILLARS:
        return _godview_unavailable(f"unknown godview pillar: {pillar_name}", pillar_name)
    return _godview_unavailable("not built yet — no data", pillar_name)


def research_status(scenario: str) -> dict:
    """GET /api/v1/snapshots/research/latest -> mirrors
    api/routers/snapshots.py::get_latest_research_run (read at commit
    2218e88d, lines 36-70) and scripts/research_status.py's
    latest_research_run_result / latest_hypothesis_outcome (same commit,
    whole file): a research_run row flattened to the top level (run_id,
    status — the RECORD's own lifecycle status, not an envelope sentinel —
    phase, error, error_category, iteration, iterations, skip_reasons,
    failure_reasons, duration_s, generation, code_sha, inputs), plus
    latest_hypothesis (id/statement/layer/state/kill_reason/updated_at)
    when a hypothesis_registry row exists — a SEPARATE, independent read,
    not extracted from the run record (research_status.py's
    latest_hypothesis_outcome docstring).

    - empty (table reachable, zero rows): {"status": "no_runs"}
    - healthy: an "ok" run, iterations 3, no skip/failure reasons.
    - partial: a "failed" run at phase "feature_list" with one skip reason
      and one failure reason (a timeout-status example was explicitly not
      requested — not built).
    """
    if scenario == "empty":
        return {"status": "no_runs"}
    if scenario == "partial":
        return {
            "id": 501, "created_at": AS_OF_DATETIME,
            "run_id": "run-test1-002", "status": "failed", "phase": "feature_list",
            "error": "feature_list query timed out", "error_category": "operational_timeout",
            "iteration": 2, "iterations": 2,
            "skip_reasons": ["TEST2: insufficient price history"],
            "failure_reasons": ["feature_list: statement timeout after 30s"],
            "duration_s": 41.2, "generation": 7, "code_sha": "dbef7ced",
            "inputs": {"feature_ids_count": 0, "market_snapshot_keys": [], "evaluation_version": "v3"},
            "latest_hypothesis": {
                "id": 2, "statement": "Untestable placeholder hypothesis", "layer": "REGIME",
                "state": "FAILED", "kill_reason": "feature series has < 30 observations in window",
                "updated_at": AS_OF_DATETIME,
            },
        }
    return {
        "id": 500, "created_at": AS_OF_DATETIME,
        "run_id": "run-test1-001", "status": "ok", "phase": "complete",
        "error": None, "error_category": None,
        "iteration": 3, "iterations": 3,
        "skip_reasons": [], "failure_reasons": [],
        "duration_s": 118.4, "generation": 7, "code_sha": "dbef7ced",
        "inputs": {
            "feature_ids_count": 40,
            "market_snapshot_keys": ["vix_close", "sp500_full"],
            "evaluation_version": "v3",
        },
        "latest_hypothesis": {
            "id": 1, "statement": f"{TICKER} momentum leads sector flow", "layer": "REGIME",
            "state": "TESTING", "kill_reason": None, "updated_at": AS_OF_DATETIME,
        },
    }


def pipeline_health(scenario: str) -> dict:
    """GET /api/v1/system/pipeline-health -> mirrors PipelineHealthResponse
    (api/schemas/system.py, read at commit 28b536df, lines 176-190):
    top-level `availability`/`stale_reason` (draft #567 contract addition),
    and each source's `field_record` (:143-148,
    store/availability_fields.py::FieldRecord.to_dict()).
    """
    if scenario == "empty":
        return {
            "summary": {"total_sources": 0, "healthy": 0, "stale": 0, "broken": 0},
            "sources": [],
            "coverage": {},
            "recent_errors": [],
            "resolver_status": {"pending": 0, "last_run": None, "last_resolved": 0},
            "availability": "unavailable",
            "stale_reason": "unknown",
        }
    sources = [
        {
            "name": "test_puller_fred",
            "type": "macro",
            "status": "healthy",
            "last_pull": "2026-09-15T06:00:00+00:00",
            "rows_last_pull": 12,
            "next_scheduled": "2026-09-16T06:00:00+00:00",
            "freshness": "green",
            "series_count": 4,
            "error": None,
            "field_record": _field_record(
                availability="available", provenance="measured",
                ingested_at="2026-09-15T06:00:00+00:00", source_catalog="test_puller_fred",
                # healthy: published_at/available_at/revision/coverage stay null.
            ),
        },
    ]
    summary = {"total_sources": 1, "healthy": 1, "stale": 0, "broken": 0}
    recent_errors = []
    availability = "available"
    stale_reason = None
    if scenario == "partial":
        sources[0]["field_record"]["stale_reason"] = None  # still healthy, unaffected
        sources.append({
            "name": "test_puller_finviz",
            "type": "altdata",
            "status": "stale",
            "last_pull": "2026-09-08T06:00:00+00:00",
            "rows_last_pull": 0,
            "next_scheduled": "2026-09-16T06:00:00+00:00",
            "freshness": "yellow",
            "series_count": 0,
            "error": "last successful pull was 7 days ago",
            "field_record": _field_record(
                availability="available", provenance="measured",
                ingested_at="2026-09-08T06:00:00+00:00", source_catalog="test_puller_finviz",
                stale_reason="stale",
            ),
        })
        sources.append({
            "name": "test_puller_never_configured",
            "type": "altdata",
            "status": "broken",
            "last_pull": None,
            "rows_last_pull": None,
            "next_scheduled": None,
            "freshness": "red",
            "series_count": None,
            "error": "no API key / puller never set up for this environment",
            "field_record": _field_record(
                availability="unavailable", source_catalog="test_puller_never_configured",
                stale_reason="never_configured",
            ),
        })
        summary = {"total_sources": 3, "healthy": 1, "stale": 1, "broken": 1}
        recent_errors = [{"timestamp": "2026-09-15T06:05:00+00:00", "source": "test_puller_finviz", "message": "stale (>24h since last SUCCESS row)"}]
    return {
        "summary": summary,
        "sources": sources,
        "coverage": {"macro": {"total": 1, "with_data": 1, "pct": 100.0}},
        "recent_errors": recent_errors,
        "resolver_status": {"pending": 0, "last_run": "2026-09-15T06:10:00+00:00", "last_resolved": 4},
        "availability": availability,
        "stale_reason": stale_reason,
    }


# ── Journey (e): data health / source drill-down ────────────────────

def system_health(scenario: str) -> dict:
    """GET /api/v1/system/health -> mirrors api/routers/system.py::health.

    Operator.jsx reads several `checks` fields this fixture originally
    omitted: `disk_percent`/`disk_free_gb` (system.py:157-158, ":189-192"
    template `Disk: {pct}% ({free}GB free)"`), `api_keys_configured`/
    `api_keys_total` (system.py:197-198, ":193-196" template
    `"API Keys: {configured}/{total}"`), `ws_clients` (system.py:148),
    `llm_available` (system.py:168), and `thread_ingestion`
    (system.py:141, `checks[f"thread_{name}"]`). Omitting them didn't error
    (Operator.jsx has no null-guard on any of these), it rendered
    "Disk: % (GB free)" and "API Keys: /" with the numbers missing.
    """
    if scenario == "empty":
        return {
            "status": "degraded",
            "checks": {
                "database": True,
                "features_registered": False,
                "recent_data": False,
                "pool_healthy": True,
                "pool_size": 5,
                "pool_checked_out": 0,
                "pool_overflow": 0,
                "disk_percent": 24.0,
                "disk_free_gb": 380.0,
                "api_keys_configured": 0,
                "api_keys_total": 5,
                "ws_clients": 0,
                "llm_available": False,
                "thread_ingestion": False,
            },
            "degraded_reasons": ["no features registered", "no data pulled in 7 days", "thread 'ingestion' not running"],
        }
    checks = {
        "database": True,
        "features_registered": True,
        "recent_data": True,
        "pool_healthy": True,
        "pool_size": 5,
        "pool_checked_out": 1,
        "pool_overflow": 0,
        "disk_percent": 24.0,
        "disk_free_gb": 380.0,
        "api_keys_configured": 4,
        "api_keys_total": 5,
        "ws_clients": 0,
        "llm_available": True,
        "thread_ingestion": True,
    }
    degraded_reasons: list[str] = []
    status = "ok"
    if scenario == "partial":
        checks["recent_data"] = False
        checks["llm_available"] = False
        degraded_reasons.append("no data pulled in 7 days")
        status = "degraded"
    return {"status": status, "checks": checks, "degraded_reasons": degraded_reasons}


def hermes_status(scenario: str) -> dict:
    """GET /api/v1/system/hermes-status -> mirrors api/schemas/system.py's
    HermesStatusResponse exactly (running/cycle_count/task_status/
    operator_state/...), consumed by pwa/src/views/Operator.jsx (hermes.running,
    hermes.task_status, hermes.operator_state.last_pipeline_run) and
    pwa/src/views/Settings.jsx.
    """
    if scenario == "empty":
        return {
            "running": False, "cycle_count": 0, "task_status": {}, "operator_state": {},
            "uptime_seconds": 0.0, "schedule": {}, "tasks": [], "snapshots": [], "task_count": 0,
        }
    task_status = {
        "test_puller_fred": {"last_run": "2026-09-15T06:00:00+00:00", "success": True, "duration_s": 4.2, "error": None},
    }
    if scenario == "partial":
        task_status["test_puller_finviz"] = {
            "last_run": "2026-09-15T06:05:00+00:00", "success": False, "duration_s": 0.5,
            "error": "operational timeout (statement_timeout)",
        }
    return {
        "running": True,
        "cycle_count": 42,
        "task_status": task_status,
        "operator_state": {
            "last_pipeline_run": "2026-09-15T06:10:00+00:00",
            "pulls_retried": 0, "fixes_applied": 0, "hypotheses_tested": 0, "errors_diagnosed": 0,
        },
        "uptime_seconds": 86400.0,
        "schedule": {},
        "tasks": [],
        "snapshots": [],
        "task_count": len(task_status),
    }


def sector_health(sector: str, scenario: str) -> dict:
    if scenario == "empty":
        return {"sector": sector, "available": False, "status": "unavailable", "reason": "no sector series resolved", "as_of": None}
    payload = {"sector": sector, "available": True, "status": "healthy", "as_of": AS_OF_DATE, "series_count": 6}
    if scenario == "partial":
        payload = {"sector": sector, "available": False, "status": "unavailable", "reason": "partial coverage (2/6 series stale)", "as_of": None}
    return payload


# ── Journey (a) extra: home compose + verdict stream ─────────────────
# Home.jsx (pwa/src/views/Home.jsx) never renders WidgetGrid directly — it
# posts the question to POST /api/v1/chat/compose (api.js:905, api/routers/
# chat.py:2128 compose_layout) and only builds the layout from that
# response's `widgets`/`spoken_reply`. Each widget then fetches its OWN data
# independently (widgets.jsx useFetch calls) — ticker_pulse calls
# api.getTickerQuote, watchlist calls api.getWatchlist, macro_regime calls
# api.getCurrent, news calls api.getNewsMomentum, money_flow calls
# api.getSectorFlows — all already served above. The verdict widget additionally
# opens POST /api/v1/chat/ask/stream (SSE) on mount whenever compose gave it a
# non-empty props.question (widgets.jsx VerdictCard, ~line 91).

def chat_compose(scenario: str) -> dict:
    """POST /api/v1/chat/compose -> mirrors ChatComposeResponse
    (api/routers/chat.py:306-318): spoken_reply, widgets[{type,title,props}],
    allocation[{ticker,weight}], generated_at, model_used, cannot_fulfill,
    request_id, alert_created.
    """
    widgets = [
        {"type": "verdict", "title": "Your read", "props": {"question": f"How is {TICKER} doing right now?"}},
        {"type": "ticker_pulse", "title": TICKER, "props": {"ticker": TICKER}},
        {"type": "watchlist", "title": "My stocks", "props": {}},
        {"type": "macro_regime", "title": "The market right now", "props": {}},
        {"type": "news", "title": "What's in the news", "props": {}},
        {"type": "money_flow", "title": "Where attention is going", "props": {}},
    ]
    spoken = {
        "healthy": f"Here's how things look for {TICKER} and the market right now.",
        "partial": f"Here's what I've got for {TICKER} — a couple of things are still catching up.",
        "empty": "I don't have anything saved or measured yet, but here's the market overview.",
    }[scenario]
    allocation = [] if scenario == "empty" else [{"ticker": TICKER, "weight": 1.0}]
    return {
        "spoken_reply": spoken,
        "widgets": widgets,
        "allocation": allocation,
        "generated_at": GENERATED_AT,
        "model_used": "fixture-rule-based",
        "cannot_fulfill": False,
        "request_id": None,
        "alert_created": False,
    }


def chat_ask_stream_deltas(scenario: str) -> list[str]:
    """Text chunks for the SSE stream at POST /api/v1/chat/ask/stream
    (api/routers/chat.py:2740 ask_grid_stream, media_type text/event-stream).
    api.js's askStream (api.js:945-976) only reads `data: {"delta": ...}`
    lines and concatenates them — no explicit terminator is required, the
    stream just ends when the connection closes.
    """
    sentence = {
        "healthy": f"{TICKER} looks steady today, in line with a risk-on market read.",
        "partial": f"{TICKER}'s workbook history is thin, and one data source is stale right now.",
        "empty": "There isn't enough saved or measured data yet to give a read.",
    }[scenario]
    words = sentence.split(" ")
    return [w + " " for w in words]


# ── Journey (b) extra: watchlist ticker analysis/overview + derivatives ──
# The #/watchlist/{ticker} view (WatchlistAnalysis.jsx) calls these before
# /edge: GET .../analysis (watchlist_analysis.py::get_ticker_analysis),
# GET .../overview (watchlist_overview.py::get_ticker_overview), and three
# derivatives endpoints that return their own `{"error": ...}` shape on
# failure rather than a fixture-side HTTP error.

def watchlist_ticker_analysis(ticker: str, scenario: str) -> dict:
    """Mirrors api/routers/watchlist_analysis.py::get_ticker_analysis
    (composed-g, ~line 58-105 assembling `analysis`)."""
    if scenario == "empty":
        return {"ticker": ticker, "watchlist_item": None, "watchlist_saved": False, "period": "3M",
                 "price_history": [], "price_source": None, "related_features": [], "options": [],
                 "regime": None, "tradingview_signals": []}
    price_history = [{"date": "2026-09-10", "value": 98.0}, {"date": "2026-09-15", "value": 100.0}]
    payload = {
        "ticker": ticker, "watchlist_item": {"ticker": ticker, "display_name": "Test One Corp"},
        "watchlist_saved": True, "period": "3M",
        "price_history": price_history, "price_source": "grid",
        "related_features": [{"name": f"{ticker}_close", "z_score": 0.4}],
        "options": [],
        # Mirrors watchlist_analysis.py:264-267 exactly: state/confidence/
        # posture/as_of. `posture` (from decision_journal.grid_recommendation)
        # was missing from an earlier version of this fixture;
        # WatchlistAnalysis.jsx:1411 reads `regime.posture || '--'` — an
        # honest fallback, but it fired for every scenario because the
        # field was simply absent, not because it was legitimately unmeasured.
        "regime": {"state": "RISK_ON", "confidence": 0.62, "posture": "Risk-On / Overweight Growth", "as_of": AS_OF_DATE},
        "tradingview_signals": [],
    }
    return payload


def watchlist_ticker_overview(ticker: str, scenario: str) -> dict:
    """Mirrors api/routers/watchlist_overview.py::get_ticker_overview
    (composed-g, ~line 31-44 docstring: overview/key_levels/sentiment/
    generated_at/sector_path)."""
    if scenario == "empty":
        return {"overview": "No price, options, or regime context is available for this ticker yet.",
                 "key_levels": [], "sentiment": "unknown", "generated_at": GENERATED_AT, "sector_path": []}
    return {
        "overview": f"{ticker} is trading near its 5-day range with a risk-on macro backdrop.",
        # key_levels items are {"label": ..., "value": ...} — mirrors
        # watchlist_overview.py:250-256 exactly. An earlier version used a
        # "level" key instead of "value"; WatchlistAnalysis.jsx:217 reads
        # `level.value`, so that typo rendered "5D low: $" with nothing
        # after the "$" (AIOverviewCard), and separately fed `undefined`
        # into PriceChart.jsx:121's `yScale(level.value)` as `keyLevels`,
        # producing the browser console's
        # `<text> attribute y: Expected length, "NaN"` warning.
        "key_levels": [{"label": "5D low", "value": 98.0}, {"label": "5D high", "value": 100.0}],
        "sentiment": "neutral" if scenario == "partial" else "constructive",
        "generated_at": GENERATED_AT,
        "sector_path": ["Technology"],
    }


def derivatives_gex(ticker: str, scenario: str) -> dict:
    """Mirrors api/routers/derivatives.py::get_gex (~line 81-95): returns the
    full GEX profile, or {"error": ..., "ticker": ...} on failure — a shape
    the fixture reproduces directly rather than a fixture-side HTTP error.
    """
    if scenario in ("partial", "empty"):
        return {"error": "no options chain rows for this ticker", "ticker": ticker}
    return {
        "ticker": ticker, "gex_aggregate": 1_250_000.0, "gamma_flip": 99.5, "gamma_wall": 105.0,
        "put_wall": 95.0, "call_wall": 105.0, "dealer_delta": 0.0, "vanna_exposure": 200.0,
        "charm_exposure": -50.0, "regime": "long_gamma", "spot": 100.0, "per_strike": [],
    }


def derivatives_vanna_charm(ticker: str, scenario: str) -> dict:
    """Mirrors api/routers/derivatives.py::get_vanna_charm (~line 172-192)."""
    if scenario in ("partial", "empty"):
        return {"error": "no options chain rows for this ticker", "ticker": ticker}
    return {
        "ticker": ticker, "vanna_exposure": 200.0, "charm_exposure": -50.0, "spot": 100.0,
        "per_strike": [], "days_to_opex": 12, "interpretation": "Dealers are modestly long gamma into OpEx.",
    }


def derivatives_flow_timeline(ticker: str, scenario: str) -> dict:
    """Mirrors api/routers/derivatives.py::get_flow_timeline (~line 854-872)."""
    if scenario == "empty":
        return {"ticker": ticker, "days": 90, "history": [], "gamma_flip_crossings": []}
    history = [{"date": AS_OF_DATE, "net_gex": 1_250_000.0, "spot": 100.0, "regime": "long_gamma"}]
    return {"ticker": ticker, "days": 90, "history": history, "gamma_flip_crossings": []}


# ── Journey (d)/(e) real surfaces: Operator.jsx + Discovery.jsx ──────
# The lead's browser run of the real PWA found these are the actual
# operator-only ("research status" / "data health") surfaces wired up —
# not a dedicated /research-status route. Operator.jsx (pwa/src/views/
# Operator.jsx:51-64) loads all six in parallel on mount; Discovery.jsx
# loads jobs/results/hypotheses.

def system_status(scenario: str) -> dict:
    """GET /api/v1/system/status -> mirrors SystemStatusResponse
    (api/schemas/system.py:49-55)."""
    return {
        "database": {"connected": scenario != "empty", "size_mb": 512.0 if scenario != "empty" else 0.0},
        "hyperspace": {"node_online": False, "api_available": False, "peer_id": None, "points": None,
                        "connected_peers": None, "model_loaded": None},
        "grid": {"features_total": 40 if scenario != "empty" else 0, "features_model_eligible": 12,
                  "hypotheses_total": 6, "hypotheses_in_production": 1,
                  "journal_entries_total": 30, "journal_entries_with_outcomes": 18},
        "server": {"disk_total_gb": 500.0, "disk_used_gb": 120.0, "disk_free_gb": 380.0, "disk_percent": 24.0,
                    "cpu_percent": 8.0, "memory_total_gb": 32.0, "memory_used_gb": 10.0, "memory_percent": 31.0,
                    "cpu_temp_c": None, "gpu_temp_c": None},
        "uptime_seconds": 86400.0,
        "server_time": GENERATED_AT,
    }


def system_freshness(scenario: str) -> dict:
    """GET /api/v1/system/freshness -> mirrors FreshnessResponse
    (api/schemas/system.py, read at commit 28b536df, lines 82-102):
    top-level `availability`/`stale_reason` (draft #567), and each
    StaleSource's `field_record` (:82-91, same FieldRecord.to_dict() shape
    as pipeline_health above).
    """
    if scenario == "empty":
        return {
            "families": [], "overall_status": "RED", "stale_sources": [],
            "availability": "unavailable", "stale_reason": "unknown",
        }
    families = [{"family": "macro", "total": 1, "fresh_today": 1, "status": "GREEN"}]
    stale_sources: list[dict] = []
    overall = "GREEN"
    availability = "available"
    stale_reason = None
    if scenario == "partial":
        families = [{"family": "macro", "total": 2, "fresh_today": 1, "stale": 1, "status": "YELLOW"}]
        stale_sources = [
            {
                "source": "test_puller_finviz", "last_pull": "2026-09-08T06:00:00+00:00", "stale": True,
                "field_record": _field_record(
                    availability="available", provenance="measured",
                    ingested_at="2026-09-08T06:00:00+00:00", source_catalog="test_puller_finviz",
                    stale_reason="stale",
                ),
            },
            {
                "source": "test_puller_never_configured", "last_pull": None, "stale": True,
                "field_record": _field_record(
                    availability="unavailable", source_catalog="test_puller_never_configured",
                    stale_reason="never_configured",
                ),
            },
        ]
        overall = "YELLOW"
    else:
        families[0]["stale"] = 0
    return {
        "families": families, "overall_status": overall, "stale_sources": stale_sources,
        "availability": availability, "stale_reason": stale_reason,
    }


def snapshots_issues(scenario: str) -> list[dict]:
    """GET /api/v1/snapshots/issues -> returns a PLAIN LIST (not
    {"issues": [...]}) — mirrors api/routers/snapshots.py:123-181 exactly.
    See README "defects observed": Operator.jsx:61 does
    `issuesRes?.issues || issuesRes || []`, which works for this shape,
    but crashes (`issues.map is not a function`, Operator.jsx:365, no
    Array.isArray guard) if a real fetch instead returns an `{error:true,...}`
    object — this fixture always 200s so that path isn't reproduced here,
    only documented.
    """
    if scenario == "empty":
        return []
    issues = [
        {"id": 1, "created_at": "2026-09-15T06:05:00+00:00", "category": "ingestion", "severity": "WARNING",
         "source": "test_puller_finviz", "title": "Stale pull", "detail": "No SUCCESS row in 24h.",
         "stack_trace": None, "hermes_diagnosis": "Rate limited upstream.", "fix_applied": False,
         "fix_result": None, "resolved_at": None, "cycle_number": 41},
    ]
    if scenario == "partial":
        issues.append({"id": 2, "created_at": "2026-09-15T06:06:00+00:00", "category": "scoring",
                        "severity": "ERROR", "source": "trust_scorer", "title": "Convergence detection skipped",
                        "detail": "signal_sources query timed out.", "stack_trace": None,
                        "hermes_diagnosis": None, "fix_applied": False, "fix_result": None,
                        "resolved_at": None, "cycle_number": 42})
    return issues


def snapshots_latest(category: str, scenario: str) -> list[dict]:
    """GET /api/v1/snapshots/latest/{category} -> a PLAIN LIST, mirrors
    api/routers/snapshots.py:29-46 (store.get_latest)."""
    if scenario == "empty":
        return []
    row = {"category": category, "snapshot_date": AS_OF_DATE, "metrics": {"sources_ok": 1, "sources_total": 1}}
    if scenario == "partial":
        row["metrics"] = {"sources_ok": 1, "sources_total": 2}
    return [row]


def discovery_jobs(scenario: str) -> dict:
    """GET /api/v1/discovery/jobs -> mirrors api/routers/discovery.py:117-126."""
    if scenario == "empty":
        return {"jobs": []}
    jobs = [{"id": "job-1", "type": "orthogonality", "status": "complete", "started": AS_OF_DATETIME}]
    if scenario == "partial":
        jobs.append({"id": "job-2", "type": "clustering", "status": "failed", "started": AS_OF_DATETIME,
                      "error": "insufficient feature history"})
    return {"jobs": jobs}


def discovery_results(result_type: str, scenario: str) -> dict:
    """GET /api/v1/discovery/results/{orthogonality|clustering} -> mirrors
    api/routers/discovery.py:129-149, which returns `job["result"]` as-is —
    i.e. the raw summary dict from discovery/orthogonality.py or
    discovery/clustering.py, not a generic wrapper.

    Discovery.jsx reads orthoResult.{n_features_analyzed,true_dimensionality}
    (discovery/orthogonality.py:368-370) and clusterResult.{best_k,
    pca_components_used,variance_explained} (discovery/clustering.py:
    247-252) directly off `result`. An earlier version of this fixture
    returned `{"type", "generated_at", "summary": "..."}` instead — none of
    those keys exist on the real result, so `orthoResult.n_features_analyzed`
    etc. were `undefined` (rendered blank) and
    `clusterResult.variance_explained * 100` was `undefined * 100` = `NaN`
    (Discovery.jsx:339 has no null-guard on that multiply — see README
    "Defects observed").
    """
    if scenario == "empty" or (scenario == "partial" and result_type == "clustering"):
        return {"result": None, "message": f"No completed {result_type} run found"}
    if result_type == "orthogonality":
        result = {
            "n_features_analyzed": 40, "n_features_dropped": 2,
            "true_dimensionality": 6, "variance_explained_by_true_dim": 0.82,
            "by_family": {"macro": 4, "options": 2}, "total_features": 40,
        }
    else:
        result = {
            "best_k": 4, "pca_components_used": 6, "variance_explained": 0.82,
            "silhouette": 0.41, "gmm_persistence_days": 30,
        }
    return {"result": result}


def discovery_hypotheses(scenario: str) -> dict:
    """GET /api/v1/discovery/hypotheses -> mirrors
    api/routers/discovery.py:261-291 (plain hypothesis_registry rows)."""
    if scenario == "empty":
        return {"hypotheses": []}
    hyps = [{"id": 1, "statement": f"{TICKER} momentum leads sector flow", "state": "TESTING",
             "layer": "REGIME", "created_at": AS_OF_DATETIME, "updated_at": AS_OF_DATETIME}]
    if scenario == "partial":
        hyps.append({"id": 2, "statement": "Untestable placeholder hypothesis", "state": "TESTING",
                      "layer": "REGIME", "created_at": AS_OF_DATETIME, "updated_at": AS_OF_DATETIME,
                      "skip_reason": "feature series has < 30 observations in window"})
    return {"hypotheses": hyps}


def discovery_hypotheses_results(scenario: str) -> dict:
    """GET /api/v1/discovery/hypotheses/results -> mirrors
    api/routers/discovery.py:155-251 (results[]/count)."""
    if scenario == "empty":
        return {"results": [], "count": 0}
    results = [{"id": 1, "statement": f"{TICKER} momentum leads sector flow", "state": "TESTING",
                "layer": "REGIME", "correlation": 0.42, "optimal_lag": 3, "r_squared": 0.18,
                "feature_ids": [1], "lag_structure": None, "created_at": AS_OF_DATETIME,
                "updated_at": AS_OF_DATETIME, "tested_at": AS_OF_DATETIME}]
    if scenario == "partial":
        # Explicit failure/skip reason on the second hypothesis — never a
        # fabricated correlation/r_squared for an untested one.
        results.append({"id": 2, "statement": "Untestable placeholder hypothesis", "state": "FAILED",
                         "layer": "REGIME", "correlation": None, "optimal_lag": None, "r_squared": None,
                         "feature_ids": [], "lag_structure": None, "created_at": AS_OF_DATETIME,
                         "updated_at": AS_OF_DATETIME, "tested_at": None,
                         "skip_reason": "feature series has < 30 observations in window"})
    return {"results": results, "count": len(results)}


def ticker_quote(ticker: str, scenario: str) -> dict:
    """GET /api/v1/watchlist/{ticker}/quote -> mirrors
    api/routers/watchlist_overview.py::get_ticker_quote (~line 378-437):
    ticker/price/change_pct/put_call_ratio/max_pain/iv_atm/sentiment/source/
    as_of/stale. Powers the Home page's ticker_pulse widget.
    """
    if scenario == "empty":
        return {"ticker": ticker, "price": None, "change_pct": None, "put_call_ratio": None,
                 "max_pain": None, "iv_atm": None, "sentiment": None, "source": None,
                 "as_of": None, "stale": None}
    # Deliberately non-zero on `healthy` (0.6%, not 0.0%): a measured flat
    # day (change_pct == 0.0) is legitimate and would render "+0.0%"
    # correctly, but a fixture-chosen exact 0.0 is easy to mistake for a
    # missing-value default when reading the rendered page, so this fixture
    # avoids the ambiguity rather than actually being broken (unlike the
    # other fields fixed in this pass, this one worked; see README).
    return {
        "ticker": ticker, "price": 100.0, "change_pct": 0.6 if scenario == "healthy" else -0.4,
        "put_call_ratio": 0.9, "max_pain": 100.0, "iv_atm": 0.35,
        "sentiment": "neutral", "source": "grid", "as_of": AS_OF_DATE,
        "stale": scenario == "partial",
    }


def ten_year_portfolio_weekly(scenario: str) -> dict:
    """GET /api/v1/ten-year-portfolio/weekly -> mirrors
    api/routers/ten_year_portfolio.py::weekly_ten_year_portfolio (~line
    262-298) + build_weekly_recommendation / build_profile_portfolio.

    TenYearPortfolio.jsx:69-76 `money(value)` renders `null`/`NaN` as the
    literal string "$0" (not "--" or "n/a" like its sibling `pct()`/
    `number()` helpers) — see README "defects observed". That collapse
    happens client-side, so this fixture still serves honest nulls in the
    empty scenario; it does not manufacture a real-looking $0 either.
    """
    if scenario == "empty":
        return {
            "status": "empty",
            "message": "No eligible Yahoo adjusted-close price history found.",
            "universe": {"mode": "stocks_only", "requested_years": 10, "series_available": 0,
                          "requested_candidates": 40, "stock_candidates": 0, "ranked_candidates": 0,
                          "source": "raw_series:yfinance_adjusted_close",
                          "frontier_source": "resolved_series:ticker_full",
                          "input_universe_size": 40, "input_universe": "dad_chart_core_universe",
                          "frontier_input_universe_size": 12},
            "as_of": None, "capital": 1_000_000.0,
            "benchmark": {"ticker": "QQQ", "cagr": None, "total_return": None, "sparkline": []},
            "ranked": [], "profiles": [], "candidate_boards": [],
        }
    allocation = {
        "ticker": TICKER, "score": 0.6, "cagr": 0.08, "annual_volatility": 0.22, "latest_price": 100.0,
        "years": 10.0, "target_weight": 1.0, "target_dollars": 1_000_000.0, "whole_shares": 10000,
        "estimated_position_value": 1_000_000.0, "action": "BUY", "hold_until_rank_below": 15,
        "themes": ["synthetic"],
    }
    monte_carlo = {
        "years": 10, "simulations": 2000, "p10": 900_000.0, "p50": 1_400_000.0, "p90": 2_100_000.0,
        "probability_above_start": 0.78, "expected_annual_return": 0.08, "annual_volatility": 0.22,
    }
    profile = {
        "id": "dad_chartist", "label": "Dad Chartist", "description": "Synthetic fixture profile.",
        "capital": 1_000_000.0, "cash_target": 0.0, "estimated_invested": 1_000_000.0,
        "estimated_residual_cash": 0.0, "top_n": 15, "max_position": 0.1, "configured_max_position": 0.1,
        "hold_buffer": 5,
        "weekly_policy": {"review": "weekly", "rebalance_threshold": "hold-rank/trend/weight drift only",
                           "entry_rule": "New buys must rank inside top 15.",
                           "exit_rule": "Existing names can be held until rank 20 unless the chart breaks."},
        "monte_carlo": monte_carlo, "allocations": [allocation],
    }
    if scenario == "partial":
        # Benchmark data is stale/incomplete while the ranked universe itself
        # is fine — a stale flag on the benchmark, not a fabricated CAGR.
        benchmark = {"ticker": "QQQ", "cagr": None, "total_return": None, "sparkline": [],
                      "stale": True, "reason": "benchmark price history did not cover the full window"}
    else:
        benchmark = {"ticker": "QQQ", "cagr": 0.12, "total_return": 2.1, "sparkline": [100.0, 105.0, 112.0]}
    return {
        "status": "ok",
        "as_of": AS_OF_DATE,
        "capital": 1_000_000.0,
        "benchmark": benchmark,
        "universe": {"mode": "stocks_only", "requested_years": 10, "series_available": 1,
                      "requested_candidates": 40, "stock_candidates": 1, "ranked_candidates": 1,
                      "source": "raw_series:yfinance_adjusted_close",
                      "frontier_source": "resolved_series:ticker_full",
                      "input_universe_size": 40, "input_universe": "dad_chart_core_universe",
                      "frontier_input_universe_size": 12},
        "ranked": [allocation],
        "profiles": [profile],
        "candidate_boards": [],
    }
