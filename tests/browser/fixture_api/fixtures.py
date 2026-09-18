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

TICKER = "TEST1"
AS_OF_DATE = "2026-09-15"
AS_OF_DATETIME = "2026-09-15T14:30:00+00:00"
GENERATED_AT = "2026-09-18T09:00:00+00:00"


# ── Auth ──────────────────────────────────────────────────────────────

def login_response() -> dict:
    """A synthetic dev token. Not a real JWT — the fixture server does not
    verify it; see tests/browser/README.md for why that is legitimate here.
    """
    return {
        "token": "dev-fixture-token.not-a-real-jwt.TEST1",
        "expires_in": 3600,
        "role": "admin",
        "username": "fixture-operator",
    }


def verify_response() -> dict:
    return {
        "valid": True,
        "expires_at": "2026-09-16T14:30:00+00:00",
        "role": "admin",
        "username": "fixture-operator",
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
    if scenario in ("empty", "partial"):
        # partial: this is the widget that is unavailable while the rest of
        # home (regime, watchlist) is fine. empty: nothing has been computed.
        return {
            "available": False,
            "status": "unavailable",
            "reason": "no news_momentum rows in the lookback window",
            "lookback_days": 63,
            "series": [],
            "as_of": None,
        }
    return {
        "available": True,
        "status": "ok",
        "lookback_days": 63,
        "series": [
            {"ticker": TICKER, "momentum_score": 0.31, "article_count": 12, "as_of": AS_OF_DATE},
        ],
        "as_of": AS_OF_DATE,
        "source": "physics.news_momentum",
    }


def sector_flows(scenario: str) -> dict:
    if scenario == "empty":
        return {"sectors": [], "as_of": None, "available": False, "reason": "no flow data"}
    return {
        "sectors": [
            {"sector": "Technology", "flow_1d_pct": 0.0, "flow_5d_pct": 1.2, "as_of": AS_OF_DATE},
            {"sector": "Energy", "flow_1d_pct": -0.4, "flow_5d_pct": -2.1, "as_of": AS_OF_DATE},
        ],
        "as_of": AS_OF_DATE,
        "available": True,
        "source": "flows.sector_flows",
    }


# ── Journey (b): ticker investigation ────────────────────────────────

def dad_ticker_gold(ticker: str, scenario: str) -> dict:
    if scenario == "empty":
        gold = {
            "verdict": "No workbook history yet",
            "heuristic_score": None,
            "weights": None,
            "score_basis": "no_workbook_history",
            "tone": "neutral",
            "one_liner": "This ticker is not showing up in Dad's copied workbook corpus yet.",
        }
    else:
        gold = {
            "verdict": "Known name in Dad's research",
            "heuristic_score": 52,
            "weights": {
                "evidence_score": {"weight": 2.5, "cap": None, "input": 9.0, "points": 22.5},
                "file_count": {"weight": 8, "cap": None, "input": 2, "points": 16.0},
                "sheet_count": {"weight": 2, "cap": None, "input": 3, "points": 6.0},
                "mentions": {"weight": 1, "cap": 30, "input": 8, "points": 8.0},
                "_clamp": {"min": 0, "max": 100},
            },
            "score_basis": "workbook_footprint_weighted_count",
            "tone": "watch",
            "one_liner": "This has enough workbook footprint to deserve a serious look.",
        }
    return {
        "ticker": ticker,
        "status": "ok",
        "gold": gold,
        "price": ticker_price_block(scenario),
        "finviz_summary": dad_ticker_finviz(ticker, scenario)["stats"][:3],
        "generated_at": GENERATED_AT,
    }


def ticker_price_block(scenario: str) -> dict:
    if scenario == "empty":
        return {"available": False, "status": "unavailable", "reason": "no price series", "price": None, "as_of": None}
    return {
        "available": True,
        "price": 100.0,
        "change_1d_pct": 0.0,
        "as_of": AS_OF_DATE,
        "price_source": "fixture",
    }


def dad_ticker_evidence(ticker: str, scenario: str) -> dict:
    if scenario == "empty":
        return {
            "ticker": ticker,
            "status": "ok",
            "rows": [],
            "total": 0,
            "limit": 50,
            "offset": 0,
            "source_lanes": [],
        }
    rows = [
        {
            "id": 1,
            "file_name": "test_workbook_2026.xlsx",
            "sheet_name": "Watchlist",
            "source_type": "sheet_name",
            "evidence_text": f"{TICKER} — target multiple",
            "row_context": "position sizing notes",
            "obs_date": "2026-09-02",
        },
    ]
    if scenario == "partial":
        # one evidence row's own extraction is unresolved: no fabricated text.
        rows.append({
            "id": 2,
            "file_name": "test_workbook_2026.xlsx",
            "sheet_name": "Notes",
            "source_type": "cell",
            "evidence_text": None,
            "row_context": None,
            "obs_date": None,
        })
    return {
        "ticker": ticker,
        "status": "ok",
        "rows": rows,
        "total": len(rows),
        "limit": 50,
        "offset": 0,
        "source_lanes": ["filename", "sheet_name"],
    }


def dad_ticker_chart(ticker: str, scenario: str) -> dict:
    if scenario == "empty":
        return {
            "ticker": ticker,
            "status": "ok",
            "range": "1Y",
            "prices": [],
            "signals": [],
            "available": False,
            "reason": "no price history in raw_series for this range",
        }
    prices = [
        {"date": "2026-09-10", "close": 98.0},
        {"date": "2026-09-11", "close": 99.0},
        {"date": "2026-09-12", "close": 100.0},
        {"date": "2026-09-15", "close": 100.0},
    ]
    payload = {
        "ticker": ticker,
        "status": "ok",
        "range": "1Y",
        "prices": prices,
        "signals": [],
        "available": True,
        "price_source": "fixture",
    }
    if scenario == "partial":
        # Price history is fine but the options-overlay signal is stale/unscored.
        payload["signals"] = [
            {"date": "2026-09-15", "signal_type": "options_flow", "value": None, "reason": "stale (last pull 2026-09-08)"},
        ]
    return payload


def dad_ticker_finviz(ticker: str, scenario: str) -> dict:
    if scenario == "empty":
        return {
            "ticker": ticker,
            "status": "ok",
            "freshness": {"state": "missing", "last_pull": None},
            "latest_obs_date": None,
            "field_count": 0,
            "rows_inserted": 0,
            "skipped_text_fields": 0,
            "live_refresh_requested": False,
            "refresh_available": True,
            "stats": [],
            "scraped": False,
        }
    stats = [
        {"field": "sector", "label": "Sector", "group": "profile", "raw_value": "Technology",
         "parsed": None, "numeric_value": None, "value_kind": "text"},
        {"field": "pe_ratio", "label": "P/E", "group": "valuation", "raw_value": "18.00",
         "parsed": 18.0, "numeric_value": 18.0, "value_kind": "numeric"},
    ]
    if scenario == "partial":
        # A field whose scrape came back non-numeric: text, not a fabricated 0.0.
        stats.append({
            "field": "dividend_pct", "label": "Dividend %", "group": "valuation",
            "raw_value": "N/A", "parsed": None, "numeric_value": None, "value_kind": "text",
        })
        freshness_state = "stale"
    else:
        freshness_state = "fresh"
    return {
        "ticker": ticker,
        "status": "ok",
        "freshness": {"state": freshness_state, "last_pull": "2026-09-15T06:00:00+00:00"},
        "latest_obs_date": AS_OF_DATE,
        "field_count": len(stats),
        "rows_inserted": 0,
        "skipped_text_fields": 1 if scenario == "partial" else 0,
        "live_refresh_requested": False,
        "refresh_available": True,
        "stats": stats,
        "scraped": False,
    }


def dad_ticker_options(ticker: str, scenario: str) -> dict:
    if scenario == "empty":
        return {"ticker": ticker, "status": "ok", "days": [], "available": False, "reason": "no options signal history"}
    days = [
        {"date": "2026-09-15", "put_call_ratio": 0.9, "unusual_volume": False, "notional_flow": 0.0},
    ]
    return {"ticker": ticker, "status": "ok", "days": days, "available": True}


def catalyst_timeline(ticker: str, scenario: str) -> dict:
    """GET /api/v1/valuation/catalyst-timeline/{ticker}.

    Exercises the three-kind contract in one payload: a milestone whose
    probability was never scored (``None`` -> "unscored"), a milestone whose
    probability was measured at exactly 0 (a real "we think this will not
    happen" reading, distinct from unscored), and a measured 0.0 value-impact
    that must render as a real number, not a blank/missing field.
    """
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
        "asset_type": "stock",
        "custom_weight": None,
        "price": 100.0,
        "return_1d_pct": 0.0,
        "has_price": True,
    }
    payload = {
        "total_value": None,
        "total_value_basis": "GRID stores no position sizes or cost basis for watchlist items",
        "weighted_return_1d_pct": 0.0,
        "return_1d_weight_coverage": 1.0,
        "positions": [position],
        "positions_missing_price": 0,
        "missing_price_tickers": [],
        "weight_priced_total": 1.0,
        "allocation": {"by_sector": {"Technology": 1.0}, "by_asset_type": {"stock": 1.0}},
        "risk_metrics": {
            "concentration_top3": 1.0,
            "beta_proxy_by_asset_class": {"stock": 1.0},
            "beta_proxy_basis": "asset-class proxy, not a measured per-position beta",
            "sector_diversification_score": 0.0,
        },
        "options_pnl": {"total_recommendations": 0, "wins": 0, "losses": 0, "open": 0, "total_return": 0},
    }
    if scenario == "partial":
        # A second holding exists but has no price at all: excluded from the
        # weighted return (not counted as a 0% move) and reported explicitly.
        payload["positions"].append({
            "ticker": "TEST2",
            "display_name": "Test Two Inc",
            "asset_type": "stock",
            "custom_weight": None,
            "price": None,
            "return_1d_pct": None,
            "has_price": False,
        })
        payload["positions_missing_price"] = 1
        payload["missing_price_tickers"] = ["TEST2"]
        payload["return_1d_weight_coverage"] = 0.5
    return payload


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
        "dark_pool": {"available": True, "relative_volume": 1.0, "as_of": AS_OF_DATE},
        "whale_flow": [
            {"strike": 100.0, "expiry": "2026-10-16", "direction": "BUY", "premium": 25000, "date": "2026-09-12"},
        ],
        "prediction_markets": [],
        "smart_money": smart_money,
        "convergence": [],
    }
    if scenario == "partial":
        # dark pool relative volume is unavailable while the rest of /edge is fine.
        payload["dark_pool"] = {"available": False, "reason": "no dark-pool print in lookback window"}
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

def pipeline_health(scenario: str) -> dict:
    if scenario == "empty":
        return {
            "summary": {"total_sources": 0, "healthy": 0, "stale": 0, "broken": 0},
            "sources": [],
            "coverage": {},
            "recent_errors": [],
            "resolver_status": {"pending": 0, "last_run": None, "last_resolved": 0},
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
        },
    ]
    summary = {"total_sources": 1, "healthy": 1, "stale": 0, "broken": 0}
    recent_errors = []
    if scenario == "partial":
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
        })
        summary = {"total_sources": 2, "healthy": 1, "stale": 1, "broken": 0}
        recent_errors = [{"timestamp": "2026-09-15T06:05:00+00:00", "source": "test_puller_finviz", "message": "stale (>24h since last SUCCESS row)"}]
    return {
        "summary": summary,
        "sources": sources,
        "coverage": {"macro": {"total": 1, "with_data": 1, "pct": 100.0}},
        "recent_errors": recent_errors,
        "resolver_status": {"pending": 0, "last_run": "2026-09-15T06:10:00+00:00", "last_resolved": 4},
    }


# ── Journey (e): data health / source drill-down ────────────────────

def system_health(scenario: str) -> dict:
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
            },
            "degraded_reasons": ["no features registered", "no data pulled in 7 days"],
        }
    checks = {
        "database": True,
        "features_registered": True,
        "recent_data": True,
        "pool_healthy": True,
        "pool_size": 5,
        "pool_checked_out": 1,
        "pool_overflow": 0,
    }
    degraded_reasons: list[str] = []
    status = "ok"
    if scenario == "partial":
        checks["recent_data"] = False
        degraded_reasons.append("no data pulled in 7 days")
        status = "degraded"
    return {"status": status, "checks": checks, "degraded_reasons": degraded_reasons}


def hermes_status(scenario: str) -> dict:
    if scenario == "empty":
        return {"tasks": [], "as_of": GENERATED_AT}
    tasks = [
        {"name": "test_puller_fred", "last_run": "2026-09-15T06:00:00+00:00", "success": True, "duration_s": 4.2, "error": None},
    ]
    if scenario == "partial":
        tasks.append({
            "name": "test_puller_finviz", "last_run": "2026-09-15T06:05:00+00:00",
            "success": False, "duration_s": 0.5, "error": "operational timeout (statement_timeout)",
        })
    return {"tasks": tasks, "as_of": GENERATED_AT}


def sector_health(sector: str, scenario: str) -> dict:
    if scenario == "empty":
        return {"sector": sector, "available": False, "status": "unavailable", "reason": "no sector series resolved", "as_of": None}
    payload = {"sector": sector, "available": True, "status": "healthy", "as_of": AS_OF_DATE, "series_count": 6}
    if scenario == "partial":
        payload = {"sector": sector, "available": False, "status": "unavailable", "reason": "partial coverage (2/6 series stale)", "as_of": None}
    return payload
