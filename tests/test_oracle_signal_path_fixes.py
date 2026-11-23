"""Unit tests for the 8 verified defects on Oracle's per-ticker signal path.

Covers:
1. Explicit feature polarity (oracle/feature_polarity.py + _gather_signals) and
   repo-wide feature_registry coverage so new features cannot silently vote.
2. pcr and max_pain_gap excluded from directional votes and coherence when no
   >=63-obs PIT history helper exists.
3. bull_score and bear_score both use abs(z * weight).
4. Dead credit-cycle routing block and misleading log line removed.
5. _oracle_one_ticker passes ticker=ticker to _find_anti_signals.
6. Anti-signal veto removes second subtraction and uses proportional opposing
   mass (OPPOSING_VETO_FRACTION = 0.40) plus hard supply-shock/regulatory vetoes.
7. Count-robust strength = |sum(w*z_signed)| / sqrt(sum(w^2)) and non-saturating
   raw_confidence = 0.5 + 0.45 * tanh(strength / 3) clamped to [0.5, 0.95].
8. Volatility- and horizon-scaled expected_move_pct with fail-closed "no_vol"
   rejection when 20-session PIT realized vol is unavailable.
"""

from __future__ import annotations

import ast
import math
import pathlib
import re
import sys
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest
from loguru import logger as log

from oracle import engine as oe
from store.observations import Observation, SplitEvent

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
NOW = datetime(2026, 11, 20, 16, 0, tzinfo=timezone.utc)


def _bare_oracle(
    *,
    models: list[oe.OracleModel] | None = None,
    signals: list[oe.Signal] | None = None,
    daily_vol: float | None = 0.01,
    spot: float = 100.0,
) -> oe.OracleEngine:
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = MagicMock()
    oracle.models = models or [oe.OracleModel("test_model", "1.0", "test", ["equity", "vol"])]
    oracle._get_flow_context = lambda ticker: {}
    oracle._get_entry_price_reference = lambda ticker: {"status": "measured", "price": spot}
    oracle._get_journal_feedback = lambda ticker: {}
    oracle._get_convergence_for_ticker = lambda ticker: []
    oracle._read_options_signal = lambda ticker: oe.OptionsSignalRead(None, False)
    if signals is not None:
        oracle._gather_signals = lambda ticker, families, options_read=None: list(signals)
    if daily_vol is not None:
        oracle._get_realized_daily_vol = lambda ticker, as_of, window=20: daily_vol
    else:
        oracle._get_realized_daily_vol = lambda ticker, as_of, window=20: None
    return oracle


def _mock_gather_engine(feat_rows: list[tuple[int, str, str]], opt_row=None) -> MagicMock:
    conn = MagicMock()

    def execute(stmt, params=None):
        res = MagicMock()
        sql = str(stmt)
        if "feature_registry" in sql and "resolved_series" not in sql:
            res.fetchall.return_value = feat_rows
        elif "options_daily_signals" in sql:
            # The Oracle selects the row's lineage beside its eight values
            # (False: a public row).
            res.fetchone.return_value = None if opt_row is None else (*opt_row, False)
        else:
            raise AssertionError(f"Unexpected SQL: {sql}")
        return res

    conn.execute.side_effect = execute
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine


def _collect_repo_feature_registry_entries() -> list[tuple[str, str | None, str]]:
    """Collect (name, family_or_None, source) for features seeded/created across the repo."""
    entries: list[tuple[str, str | None, str]] = []

    # 1. SQL VALUES rows in schema.sql, ingestion/seed_v2.py, migrations/*.sql, migrations/versions/*.py
    sql_row_re = re.compile(
        r"\(\s*'([^']+)'\s*,\s*'([^']+)'\s*,\s*'(?:[^']|'')*'\s*,"
        r"\s*'(?:[^']|'')*'\s*,\s*\d+\s*,\s*-?\d+\s*,"
        r"\s*'(?:ZSCORE|MINMAX|RAW|RANK)'",
        re.MULTILINE,
    )
    sql_sources = [
        REPO_ROOT / "schema.sql",
        REPO_ROOT / "ingestion" / "seed_v2.py",
        *sorted((REPO_ROOT / "migrations").glob("*.sql")),
        *sorted((REPO_ROOT / "migrations" / "versions").glob("*.py")),
    ]
    for path in sql_sources:
        txt = path.read_text(encoding="utf-8", errors="ignore")
        for m in sql_row_re.finditer(txt):
            entries.append((m.group(1), m.group(2), str(path.relative_to(REPO_ROOT))))

    # 2. normalization/entity_map.py SEED_MAPPINGS and NEW_MAPPINGS_V2 targets
    em_path = REPO_ROOT / "normalization" / "entity_map.py"
    em_ast = ast.parse(em_path.read_text(encoding="utf-8"))
    ns: dict = {}
    for node in em_ast.body:
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id in ("SEED_MAPPINGS", "NEW_MAPPINGS_V2")
        ):
            exec(compile(ast.Module(body=[node], type_ignores=[]), str(em_path), "exec"), ns)
    for mapping_name in ("SEED_MAPPINGS", "NEW_MAPPINGS_V2"):
        for feat_name in ns.get(mapping_name, {}).values():
            entries.append((feat_name, None, f"entity_map:{mapping_name}"))

    # 3. ingestion/altdata/gdelt_bulk.py FEATURES + GKG_FEATURES
    from ingestion.altdata import gdelt_bulk
    for name, (fam, _) in gdelt_bulk.FEATURES.items():
        entries.append((name, fam, "ingestion/altdata/gdelt_bulk.py"))
    for name in gdelt_bulk.GKG_FEATURES:
        entries.append((name, "sentiment", "ingestion/altdata/gdelt_bulk.py"))

    # 4. ingestion/altdata/world_news.py NEWS_TAXONOMY features
    from ingestion.altdata import world_news
    for cat, cfg in world_news.NEWS_TAXONOMY.items():
        entries.append((f"wn_{cat}_article_count", cfg["family"], "ingestion/altdata/world_news.py"))
        entries.append((f"wn_{cat}_sentiment_avg", "sentiment", "ingestion/altdata/world_news.py"))
        entries.append((f"wn_{cat}_global_spread", "sentiment", "ingestion/altdata/world_news.py"))
    for comp in ("wn_total_volume", "wn_fear_index", "wn_breadth"):
        entries.append((comp, "sentiment", "ingestion/altdata/world_news.py"))

    # 5. ingestion/options.py, scripts/pull_intraday.py, features/lab.py, and
    #    news_scraper/trending_news/parse_edgar/parse_eia/parse_gdelt templates
    for suffix, fam in (
        ("pcr", "sentiment"), ("max_pain", "sentiment"), ("iv_skew", "vol"),
        ("total_oi", "sentiment"), ("opt_vol", "sentiment"), ("iv_atm", "vol"),
        ("iv_m90_put_v1", "vol"), ("iv_m110_call_v1", "vol"),
        ("term_slope", "vol"), ("oi_conc", "sentiment"),
    ):
        entries.append((f"spy_{suffix}", fam, "ingestion/options.py"))
    for suffix, fam in (
        ("intraday_range", "vol"), ("intraday_vol", "vol"), ("vwap_dev", "vol"),
        ("volume_skew", "sentiment"), ("close_location", "vol"),
    ):
        entries.append((f"spy_{suffix}", fam, "scripts/pull_intraday.py"))
    entries.append(("TSFRESH:vix_spot:mean", "tsfresh", "features/lab.py"))
    for name, fam in (
        ("news_volume_daily", "sentiment"), ("news_bullish_ratio", "sentiment"),
        ("news_bearish_ratio", "sentiment"), ("news_avg_confidence", "sentiment"),
        ("trending_volume_daily", "sentiment"), ("trending_reddit_count", "sentiment"),
        ("trending_hackernews_count", "sentiment"), ("trending_polymarket_count", "sentiment"),
        ("trending_avg_engagement", "sentiment"), ("trending_avg_relevance", "sentiment"),
        ("edgar_agg_revenue", "macro"), ("edgar_agg_net_income", "macro"),
        ("edgar_agg_assets", "macro"), ("edgar_agg_debt", "macro"),
        ("edgar_agg_cash", "macro"), ("edgar_agg_equity", "macro"),
    ):
        entries.append((name, fam, "ingestion/scripts"))

    return entries


# ── Defect 1: Feature Polarity ───────────────────────────────────────────────


def test_defect1_feature_polarity_signs_and_unknown_handling() -> None:
    from oracle.feature_polarity import (
        FAMILY_DEFAULT_POLARITY,
        FEATURE_POLARITY,
        get_feature_polarity,
        resolve_feature_polarity,
    )

    assert all(v in (-1, 0, 1) for v in FEATURE_POLARITY.values())
    assert all(v in (-1, 0, 1) for v in FAMILY_DEFAULT_POLARITY.values())

    # Unambiguously bearish when rising
    for name, fam in [
        ("vix_spot", "vol"),
        ("vix_1m_chg", "vol"),
        ("vix_3m_ratio", "vol"),
        ("vvix", "vol"),
        ("skew_index", "vol"),
        ("hy_spread_proxy", "credit"),
        ("hy_oas_spread", "credit"),
        ("ig_oas_spread", "credit"),
        ("hy_spread_3m_chg", "credit"),
        ("euro_btp_bund_spread", "credit"),
        ("unemployment_rate", "macro"),
        ("initial_claims", "macro"),
        ("continued_claims", "macro"),
        ("systemic_stress_composite", "systemic"),
        ("sofr_spread_to_ffr", "rates"),
        ("ted_spread", "rates"),
        ("tga_drawdown", "rates"),
        ("gdelt_tension_us_china", "sentiment"),
        ("news_bearish_ratio", "sentiment"),
        ("aaii_bearish_pct", "sentiment"),
        ("spy_pcr", "sentiment"),
        ("spy_iv_atm", "vol"),
    ]:
        assert get_feature_polarity(name, fam) == -1, f"{name} ({fam}) should be -1"

    # Unambiguously bullish when rising (stationary / change / growth-rate series)
    for name, fam in [
        ("sp500_adline", "breadth"),
        ("sp500_adline_slope", "breadth"),
        ("sp500_mom_3m", "breadth"),
        ("sp500_mom_12_1", "breadth"),
        ("spy_full", "equity"),
        ("smh_close", "equity"),
        ("hyg_full", "credit"),
        ("jnk_full", "credit"),
        ("lqd_full", "credit"),
        ("ism_pmi_mfg", "macro"),
        ("conf_board_lei", "macro"),
        ("fed_net_liquidity_change_1w", "macro"),
        ("fed_net_liquidity_change_1m", "macro"),
        ("bls_nonfarm_productivity", "macro"),
        ("china_m2_yoy", "macro"),
        ("ci_loans_growth", "macro"),
        ("imf_us_gdp_growth", "macro"),
        ("korea_exports_yoy", "trade"),
        ("trade_volume_yoy", "trade"),
        ("bis_global_cbflow_usd", "flows"),
        ("news_bullish_ratio", "sentiment"),
        ("spy_term_slope", "vol"),
        ("spy_macd", "vol"),
    ]:
        assert get_feature_polarity(name, fam) == 1, f"{name} ({fam}) should be +1"

    # Non-directional (rates, fx, commodity, unsigned flows, celestial)
    for name, fam in [
        ("fed_funds_rate", "rates"),
        ("yld_curve_2s10s", "rates"),
        ("dxy_index", "fx"),
        ("eurusd", "fx"),
        ("gold_full", "commodity"),
        ("shy_full", "credit"),
        ("ief_full", "credit"),
        ("etf_flow_spy_5d", "flows"),
        ("lunar_phase", "alternative"),
        ("wn_financial_article_count", "credit"),
    ]:
        assert get_feature_polarity(name, fam) == 0, f"{name} ({fam}) should be 0"

    # Unknown feature in family with no default fails closed as non-directional
    pol, src = resolve_feature_polarity("totally_unknown_synthetic_metric", "macro")
    assert pol == 0
    assert src == "unknown"


def test_followup3_trending_levels_contrarian_sentiment_and_weak_features_are_zero() -> None:
    from oracle.feature_polarity import get_feature_polarity

    # Non-stationary trending LEVEL series must be 0 (not +1)
    trending_levels = [
        ("nonfarm_payrolls", "macro"),
        ("real_gdp", "macro"),
        ("m2_money_supply", "macro"),
        ("fed_balance_sheet", "macro"),
        ("fed_net_liquidity", "macro"),
        ("fed_treasury_holdings", "macro"),
        ("nyfed_soma_total", "macro"),
        ("nyfed_soma_treasury", "macro"),
        ("nyfed_soma_mbs", "macro"),
        ("ny_fed_soma_holdings", "rates"),
        ("edgar_agg_revenue", "macro"),
        ("edgar_agg_net_income", "macro"),
        ("edgar_agg_cash", "macro"),
        ("edgar_agg_equity", "macro"),
        ("bank_reserves_at_fed", "macro"),
        ("total_reserves", "rates"),
        ("retail_sales_sa", "macro"),
        ("retail_sales_nsa", "macro"),
        ("real_disp_income", "macro"),
        ("industrial_production", "macro"),
        ("eu_industrial_output", "macro"),
        ("manufacturing_employment", "macro"),
        ("bls_jolts_openings", "macro"),
        ("euklems_labor_prod_us", "macro"),
        ("bis_property_prices_g20", "macro"),
        ("bis_global_cbflow", "flows"),
        ("loan_growth", "credit"),
        ("korea_exports_total", "trade"),
        ("korea_semi_exports", "trade"),
        ("korea_auto_exports", "trade"),
        ("australia_iron_exports", "trade"),
        ("us_exports_total", "trade"),
        ("global_semi_trade", "trade"),
        ("global_iron_trade", "trade"),
        ("global_wheat_trade", "trade"),
        ("oi_consumer_spend", "alternative"),
        ("oi_employment_overall", "alternative"),
        ("oi_spend_high_income", "alternative"),
        ("oi_spend_low_income", "alternative"),
        ("viirs_us_lights", "alternative"),
        ("viirs_china_lights", "alternative"),
        ("viirs_em_lights", "alternative"),
        ("patent_velocity_tech", "alternative"),
        ("patent_velocity_software", "alternative"),
        ("innovation_composite", "alternative"),
    ]
    for name, fam in trending_levels:
        assert get_feature_polarity(name, fam) == 0, f"trending level {name} ({fam}) should be 0"

    # Contrarian-ambiguous sentiment levels -> 0
    for name in (
        "aaii_bullish_pct",
        "aaii_bull_bear_spread",
        "umich_sentiment",
        "consumer_sentiment",
    ):
        assert get_feature_polarity(name, "sentiment") == 0, f"{name} should be 0"

    # Weak / noisy series -> 0
    assert get_feature_polarity("global_crude_trade", "trade") == 0
    assert get_feature_polarity("finra_short_interest_total", "alternative") == 0

    # Change / growth-rate series keep their directional polarity
    for name, fam, expected in [
        ("fed_net_liquidity_change_1w", "macro", 1),
        ("fed_net_liquidity_change_1m", "macro", 1),
        ("bls_nonfarm_productivity", "macro", 1),
        ("ecb_m3_yoy", "macro", 1),
        ("ecb_bank_lending_yoy", "macro", 1),
        ("china_m2_yoy", "macro", 1),
        ("china_tss_yoy", "macro", 1),
        ("china_credit_impulse", "macro", 1),
        ("brazil_credit_growth", "macro", 1),
        ("ci_loans_growth", "macro", 1),
        ("china_indpro_yoy", "macro", 1),
        ("india_iip_yoy", "macro", 1),
        ("korea_iip_yoy", "macro", 1),
        ("nyfed_nowcast_gdp_q1", "macro", 1),
        ("imf_us_gdp_growth", "macro", 1),
        ("australia_gdp_qoq", "macro", 1),
        ("korea_exports_yoy", "trade", 1),
        ("trade_volume_yoy", "trade", 1),
        ("us_china_bilateral_yoy", "trade", 1),
        ("bis_global_cbflow_usd", "flows", 1),
        ("patent_velocity_software_yoy", "alternative", 1),
        ("vix_1m_chg", "vol", -1),
        ("hy_spread_3m_chg", "credit", -1),
        ("tga_drawdown", "rates", -1),
        ("aaii_bearish_pct", "sentiment", -1),
    ]:
        assert get_feature_polarity(name, fam) == expected, f"{name} ({fam}) should be {expected}"


def test_defect1_every_repo_seeded_feature_has_polarity_or_family_default() -> None:
    from oracle.feature_polarity import resolve_feature_polarity

    entries = _collect_repo_feature_registry_entries()
    assert len(entries) >= 150, "Expected to parse repo feature_registry seeds/mappings"

    missing = []
    for name, fam, origin in entries:
        pol, src = resolve_feature_polarity(name, fam)
        assert pol in (-1, 0, 1)
        if src == "unknown":
            missing.append((name, fam, origin))

    assert not missing, (
        f"{len(missing)} feature_registry feature(s) lack explicit polarity or family default: "
        f"{missing[:20]}"
    )


def test_defect1_gather_signals_inverts_bearish_features_and_neutralizes_unknown(monkeypatch) -> None:
    if hasattr(oe, "_UNKNOWN_POLARITY_LOGGED"):
        monkeypatch.setattr(oe, "_UNKNOWN_POLARITY_LOGGED", {})
    today = date.today()
    idx = pd.DatetimeIndex([today - timedelta(days=i) for i in range(9, -1, -1)])
    # Feature 1: vix_spot spikes UP (+z) -> must vote bearish, raw z_score stays > 0.5
    # Feature 2: vix_1m_chg drops DOWN (-z) -> must vote bullish, raw z_score stays < -0.5
    # Feature 3: hy_oas_spread spikes UP (+z) -> must vote bearish
    # Feature 4: unemployment_rate spikes UP (+z) -> must vote bearish
    # Feature 5: sp500_adline spikes UP (+z) -> must vote bullish
    # Feature 6: dxy_index spikes UP (+z) -> polarity 0 -> neutral
    # Feature 7: unknown_brand_new_feature (macro) spikes UP (+z) -> unknown -> neutral + INFO log
    matrix = pd.DataFrame(
        {
            1: [15.0] * 9 + [35.0],
            2: [5.0] * 9 + [-10.0],
            3: [3.5] * 9 + [7.0],
            4: [4.0] * 9 + [6.5],
            5: [100.0] * 9 + [150.0],
            6: [100.0] * 9 + [110.0],
            7: [10.0] * 9 + [50.0],
        },
        index=idx,
    )
    feat_rows = [
        (1, "vix_spot", "vol"),
        (2, "vix_1m_chg", "vol"),
        (3, "hy_oas_spread", "credit"),
        (4, "unemployment_rate", "macro"),
        (5, "sp500_adline", "breadth"),
        (6, "dxy_index", "fx"),
        (7, "unknown_brand_new_feature", "macro"),
    ]
    pit = MagicMock()
    pit.get_feature_matrix.return_value = matrix
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = _mock_gather_engine(feat_rows)
    oracle._pit_store_instance = pit

    info_records: list[tuple[str, str]] = []
    sink_id = log.add(
        lambda msg: info_records.append((msg.record["level"].name, msg.record["message"])),
        level="INFO",
    )
    try:
        sigs = oracle._gather_signals("SPY", ["vol", "credit", "macro", "breadth", "fx"])
    finally:
        log.remove(sink_id)

    by_name = {s.name: s for s in sigs}
    assert by_name["vix_spot"].direction == "bearish"
    assert by_name["vix_spot"].z_score > 0.5
    assert by_name["vix_1m_chg"].direction == "bullish"
    assert by_name["vix_1m_chg"].z_score < -0.5
    assert by_name["hy_oas_spread"].direction == "bearish"
    assert by_name["unemployment_rate"].direction == "bearish"
    assert by_name["sp500_adline"].direction == "bullish"
    assert by_name["dxy_index"].direction == "neutral"
    assert by_name["unknown_brand_new_feature"].direction == "neutral"

    # Unknown feature name logged once at INFO level
    unknown_logs = [
        m for lvl, m in info_records
        if lvl == "INFO" and "unknown_brand_new_feature" in m
    ]
    assert len(unknown_logs) == 1


def test_followup4_unknown_feature_names_logged_at_info_once_per_process_per_day(monkeypatch) -> None:
    assert hasattr(oe, "_UNKNOWN_POLARITY_LOGGED")
    monkeypatch.setattr(oe, "_UNKNOWN_POLARITY_LOGGED", {})
    today = date.today()
    idx = pd.DatetimeIndex([today - timedelta(days=i) for i in range(9, -1, -1)])
    matrix = pd.DataFrame(
        {
            1: [10.0] * 9 + [50.0],
            2: [20.0] * 9 + [60.0],
        },
        index=idx,
    )
    feat_rows = [
        (1, "mystery_feat_one", "macro"),
        (2, "mystery_feat_two", "sentiment"),
    ]
    pit = MagicMock()
    pit.get_feature_matrix.return_value = matrix
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = _mock_gather_engine(feat_rows)
    oracle._pit_store_instance = pit

    info_records: list[tuple[str, str]] = []
    sink_id = log.add(
        lambda msg: info_records.append((msg.record["level"].name, msg.record["message"])),
        level="INFO",
    )
    try:
        # Multiple tickers and multiple models on the same day -> only 1 INFO log
        for tk in ("SPY", "QQQ", "NVDA"):
            for _ in range(3):
                oracle._gather_signals(tk, ["macro", "sentiment"])
        same_day_logs = [
            m for lvl, m in info_records
            if lvl == "INFO" and "mystery_feat_one" in m and "mystery_feat_two" in m
        ]
        assert len(same_day_logs) == 1

        # Next day -> logs once again, then suppressed for repeats on that day
        tomorrow = today + timedelta(days=1)
        oe._log_unknown_polarity_features({"mystery_feat_one", "mystery_feat_two"}, tomorrow)
        oe._log_unknown_polarity_features({"mystery_feat_one", "mystery_feat_two"}, tomorrow)
        two_day_logs = [
            m for lvl, m in info_records
            if lvl == "INFO" and "mystery_feat_one" in m and "mystery_feat_two" in m
        ]
        assert len(two_day_logs) == 2
    finally:
        log.remove(sink_id)


# ── Defect 2: pcr and max_pain_gap zero-z coherence distortion ───────────────


def test_defect2_pcr_and_max_pain_gap_non_directional_and_excluded_from_coherence() -> None:
    pit = MagicMock()
    pit.get_feature_matrix.return_value = pd.DataFrame()
    # pcr=1.8 (>1.2 previously bearish), spot=100, max_pain=90 (mp_pct=10% >3% previously bearish)
    opt_row = (1.8, 0.25, 0.05, 90.0, 100.0, 5000, 0.02, 0.15)
    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle.engine = _mock_gather_engine([], opt_row=opt_row)
    oracle._pit_store_instance = pit

    opt_sigs = oracle._gather_signals("SPY", ["sentiment"])
    by_name = {s.name: s for s in opt_sigs}
    assert by_name["pcr"].direction == "neutral"
    assert by_name["pcr"].z_score == 0.0
    assert by_name["max_pain_gap"].direction == "neutral"
    assert by_name["max_pain_gap"].z_score == 0.0

    # Coherence with one bullish directional signal + pcr + max_pain_gap must be 1.0 (not 1/3)
    combined = [oe.Signal("sp500_adline", "breadth", 10.0, 2.0, "bullish", 1.0, 0)] + opt_sigs
    assert oracle._compute_coherence(combined, "CALL") == 1.0

    # Even a manually supplied zero-z signal labelled bearish must not dilute coherence
    zero_z_bear = oe.Signal("stale_zero", "sentiment", 1.8, 0.0, "bearish", 1.0, 0)
    assert oracle._compute_coherence([combined[0], zero_z_bear], "CALL") == 1.0


# ── Defect 3: bull_score uses abs(z * weight) ────────────────────────────────


def test_defect3_bull_score_uses_abs_for_negative_z_bullish_signals(monkeypatch) -> None:
    # A bullish signal with negative raw z_score (e.g. falling VIX with polarity=-1,
    # or a registry row with direction="bullish" and negative z_score) must add
    # positive weight to bull_score, NOT subtract from it.
    signals = [
        oe.Signal("vix_spot", "vol", 11.0, -2.5, "bullish", 1.0, 0),
        oe.Signal("reg_bull_neg_z", "equity", -1.5, -1.5, "bullish", 1.0, 0),
        oe.Signal("minor_bear", "breadth", -0.6, -0.6, "bearish", 1.0, 0),
    ]
    oracle = _bare_oracle(signals=signals, daily_vol=0.01)
    oracle._find_anti_signals = lambda sigs, direction, ticker=None, **kw: []
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    monkeypatch.setitem(
        sys.modules,
        "intelligence.confidence_calibration",
        SimpleNamespace(calibrate_confidence=lambda v, *a: v),
    )
    monkeypatch.setenv("GRID_SIGNAL_OVERRIDES_ENABLED", "false")
    import intelligence.signal_weight_overrides as swo
    monkeypatch.setattr(swo, "SIGNAL_OVERRIDES_ENABLED", False)

    preds = oracle._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)
    assert len(preds) == 1
    assert preds[0].direction == "CALL"
    # bull_score = 2.5 + 1.5 = 4.0, bear_score = 0.6 -> net = 3.4, sqrt(3)
    assert preds[0].signal_strength == pytest.approx(3.4 / math.sqrt(3.0), abs=0.002)


# ── Defect 4: Dead credit-cycle routing removed ──────────────────────────────


def test_defect4_dead_credit_routing_removed(monkeypatch) -> None:
    signals = [
        oe.Signal("sp500_adline", "breadth", 10.0, 2.0, "bullish", 1.0, 0),
        oe.Signal("sp500_mom_3m", "breadth", 0.1, 1.5, "bullish", 1.0, 0),
        oe.Signal("spy_full", "equity", 500.0, 1.2, "bullish", 1.0, 0),
    ]
    oracle = _bare_oracle(signals=signals, daily_vol=0.01)
    oracle._find_anti_signals = lambda sigs, direction, ticker=None, **kw: []
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    monkeypatch.setitem(
        sys.modules,
        "intelligence.confidence_calibration",
        SimpleNamespace(calibrate_confidence=lambda v, *a: v),
    )

    messages: list[str] = []
    sink_id = log.add(lambda msg: messages.append(str(msg)), level="DEBUG")
    try:
        preds = oracle._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)
    finally:
        log.remove(sink_id)

    assert len(preds) == 1
    assert not any("credit routing keys" in m for m in messages)
    assert not hasattr(oe.OracleEngine, "_get_credit_cycle_routing")
    engine_src = (REPO_ROOT / "oracle" / "engine.py").read_text(encoding="utf-8")
    assert "credit_family_boost" not in engine_src


# ── Defect 5: _find_anti_signals receives ticker & caches external queries ───


def test_defect5_oracle_one_ticker_passes_ticker_to_find_anti_signals(monkeypatch) -> None:
    signals = [
        oe.Signal("sp500_adline", "breadth", 10.0, 2.0, "bullish", 1.0, 0),
        oe.Signal("sp500_mom_3m", "breadth", 0.1, 1.5, "bullish", 1.0, 0),
        oe.Signal("spy_full", "equity", 500.0, 1.2, "bullish", 1.0, 0),
    ]
    oracle = _bare_oracle(signals=signals, daily_vol=0.01)
    seen: list[tuple[str, str | None]] = []

    def spy_anti(sigs, direction, ticker=None, **kw):
        seen.append((direction, ticker))
        return []

    oracle._find_anti_signals = spy_anti
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    monkeypatch.setitem(
        sys.modules,
        "intelligence.confidence_calibration",
        SimpleNamespace(calibrate_confidence=lambda v, *a: v),
    )

    preds = oracle._oracle_one_ticker(1, "XOM", 1, NOW, {}, horizon_days=1)
    assert len(preds) == 1
    assert seen == [("CALL", "XOM")]


def test_followup1_external_anti_signals_cached_once_per_ticker_across_n_models(monkeypatch) -> None:
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    import intelligence.signal_weight_overrides as swo
    monkeypatch.setattr(swo, "SIGNAL_OVERRIDES_ENABLED", False)
    monkeypatch.setitem(
        sys.modules,
        "intelligence.confidence_calibration",
        SimpleNamespace(calibrate_confidence=lambda v, *a: v),
    )

    signals = [
        oe.Signal("sp500_adline", "breadth", 10.0, 2.0, "bullish", 1.0, 0),
        oe.Signal("sp500_mom_3m", "breadth", 0.1, 1.5, "bullish", 1.0, 0),
        oe.Signal("spy_full", "equity", 500.0, 1.2, "bullish", 1.0, 0),
    ]
    models = [
        oe.OracleModel(f"model_{i}", "1.0", "test", ["equity", "breadth"])
        for i in range(5)
    ]
    oracle = _bare_oracle(models=models, signals=signals, daily_vol=0.01)

    query_counts = {"supply": 0, "regulatory": 0}
    conn = MagicMock()

    def fake_execute(stmt, params=None):
        sql = getattr(stmt, "text", str(stmt))
        res = MagicMock()
        if "supply_shock_attributions" in sql:
            query_counts["supply"] += 1
            # Low severity (0.3) + high reg (0.7) = 1.0 < 1.2 so predictions pass and record both
            res.fetchall.return_value = [(
                "brent_crude", date(2026, 11, 10), 0.08, -0.03, -0.3,
                "confirmed", "upstream shock",
            )]
        elif "regulatory_events" in sql:
            query_counts["regulatory"] += 1
            res.fetchall.return_value = [(
                "fda", "warning_letter", date(2026, 11, 12), "high",
                "FDA warning", "https://fda.gov/warn",
            )]
        else:
            res.fetchall.return_value = []
        return res

    conn.execute.side_effect = fake_execute
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    oracle.engine = MagicMock()
    oracle.engine.connect.return_value = conn

    preds_xom = oracle._oracle_one_ticker(1, "XOM", 2, NOW, {}, horizon_days=1)
    assert len(preds_xom) == 5
    assert query_counts == {"supply": 1, "regulatory": 1}
    for p in preds_xom:
        names = {a.name for a in p.anti_signals}
        assert "cross_lens_supply_shock" in names
        assert "regulatory_threat" in names

    # Second ticker runs the two queries once for that ticker too (total 2 each across 10 model runs)
    preds_cvx = oracle._oracle_one_ticker(2, "CVX", 2, NOW, {}, horizon_days=1)
    assert len(preds_cvx) == 5
    assert query_counts == {"supply": 2, "regulatory": 2}


def test_followup2_regulatory_anti_signals_apply_only_to_bullish_directions(monkeypatch) -> None:
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    import intelligence.signal_weight_overrides as swo
    monkeypatch.setattr(swo, "SIGNAL_OVERRIDES_ENABLED", False)
    monkeypatch.setitem(
        sys.modules,
        "intelligence.confidence_calibration",
        SimpleNamespace(calibrate_confidence=lambda v, *a: v),
    )

    reg_calls = 0
    conn = MagicMock()

    def fake_execute(stmt, params=None):
        nonlocal reg_calls
        sql = getattr(stmt, "text", str(stmt))
        res = MagicMock()
        if "regulatory_events" in sql:
            reg_calls += 1
            res.fetchall.return_value = [
                ("fda", "recall", date(2026, 11, 10), "critical", "Class I recall", "https://fda.gov/1"),
                ("doj", "subpoena", date(2026, 11, 11), "high", "DOJ probe", "https://doj.gov/2"),
            ]
        else:
            res.fetchall.return_value = []
        return res

    conn.execute.side_effect = fake_execute
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    # 1. Direct _regulatory_anti_signals tests across both directions
    oracle_direct = oe.OracleEngine.__new__(oe.OracleEngine)
    oracle_direct.engine = MagicMock()
    oracle_direct.engine.connect.return_value = conn

    assert len(oracle_direct._regulatory_anti_signals("PFE", "CALL")) == 2
    assert len(oracle_direct._regulatory_anti_signals("PFE", "LONG")) == 2
    assert reg_calls == 2

    # Bearish directions must return [] without querying DB
    assert oracle_direct._regulatory_anti_signals("PFE", "PUT") == []
    assert oracle_direct._regulatory_anti_signals("PFE", "SHORT") == []
    assert reg_calls == 2

    # _find_anti_signals forwards direction so PUT gets no regulatory anti-signals
    assert any(a.name == "regulatory_threat" for a in oracle_direct._find_anti_signals([], "CALL", ticker="PFE"))
    assert oracle_direct._find_anti_signals([], "PUT", ticker="PFE") == []

    # 2. End-to-end _oracle_one_ticker: severe regulatory threat vetoes CALL, does NOT veto PUT
    bull_signals = [oe.Signal(f"bull_{i}", "equity", 1.0, 1.5, "bullish", 1.0, 0) for i in range(4)]
    bear_signals = [oe.Signal(f"bear_{i}", "vol", 30.0, 1.5, "bearish", 1.0, 0) for i in range(4)]

    oracle_call = _bare_oracle(signals=bull_signals, daily_vol=0.01)
    oracle_call.engine = MagicMock()
    oracle_call.engine.connect.return_value = conn
    assert oracle_call._oracle_one_ticker(1, "PFE", 1, NOW, {}, horizon_days=1) == []

    oracle_put = _bare_oracle(signals=bear_signals, daily_vol=0.01)
    oracle_put.engine = MagicMock()
    oracle_put.engine.connect.return_value = conn
    put_preds = oracle_put._oracle_one_ticker(1, "PFE", 1, NOW, {}, horizon_days=1)
    assert len(put_preds) == 1
    assert put_preds[0].direction == "PUT"
    assert put_preds[0].anti_signals == []

    # Mixed CALL + PUT models on the same ticker: CALL vetoed, PUT emitted, reg query runs once
    reg_calls_before = reg_calls
    mixed_models = [
        oe.OracleModel("bull_m", "1.0", "test", ["equity"]),
        oe.OracleModel("bear_m", "1.0", "test", ["vol"]),
        oe.OracleModel("bull_m2", "1.0", "test", ["equity"]),
    ]
    oracle_mixed = _bare_oracle(models=mixed_models, daily_vol=0.01)
    oracle_mixed.engine = MagicMock()
    oracle_mixed.engine.connect.return_value = conn
    oracle_mixed._gather_signals = lambda ticker, fams, options_read=None: (
        list(bull_signals) if "equity" in fams else list(bear_signals)
    )
    mixed_preds = oracle_mixed._oracle_one_ticker(1, "PFE", 1, NOW, {}, horizon_days=1)
    assert [p.model_name for p in mixed_preds] == ["bear_m"]
    assert mixed_preds[0].direction == "PUT"
    assert reg_calls - reg_calls_before == 1


# ── Defect 6: Anti-signal proportional veto & no double subtraction ──────────


def test_defect6_proportional_veto_no_double_count_and_hard_vetoes(monkeypatch) -> None:
    assert hasattr(oe, "OPPOSING_VETO_FRACTION")
    assert oe.OPPOSING_VETO_FRACTION == pytest.approx(0.40)

    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    monkeypatch.setitem(
        sys.modules,
        "intelligence.confidence_calibration",
        SimpleNamespace(calibrate_confidence=lambda v, *a: v),
    )
    import intelligence.signal_weight_overrides as swo
    monkeypatch.setattr(swo, "SIGNAL_OVERRIDES_ENABLED", False)

    # Case A: 10 bulls (z=1.0, w=1.0) vs 2 bears (z=-1.6, w=1.0)
    # Opposing mass = 3.2, total directional mass = 13.2 -> fraction = 0.2424 <= 0.40 -> NOT vetoed.
    # No second anti_deduction subtraction: strength = (10.0 - 3.2) / sqrt(12).
    signals_pass = [
        oe.Signal(f"bull_{i}", "equity", 1.0, 1.0, "bullish", 1.0, 0) for i in range(10)
    ] + [
        oe.Signal(f"bear_{i}", "breadth", -1.6, -1.6, "bearish", 1.0, 0) for i in range(2)
    ]
    oracle = _bare_oracle(signals=signals_pass, daily_vol=0.01)
    oracle._cross_lens_anti_signals = lambda ticker, direction: []
    oracle._regulatory_anti_signals = lambda ticker, direction="CALL": []

    preds = oracle._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)
    assert len(preds) == 1
    expected_strength = (10.0 - 3.2) / math.sqrt(12.0)
    assert preds[0].direction == "CALL"
    assert preds[0].signal_strength == pytest.approx(expected_strength, abs=0.002)
    assert len(preds[0].anti_signals) == 2  # still recorded for audit

    # Case B: 4 bulls (z=1.0, w=1.0) vs 3 bears (z=-1.0, w=1.0)
    # Opposing fraction = 3 / 7 = 0.4286 > 0.40 -> VETOED.
    signals_veto = [
        oe.Signal(f"bull_{i}", "equity", 1.0, 1.0, "bullish", 1.0, 0) for i in range(4)
    ] + [
        oe.Signal(f"bear_{i}", "breadth", -1.0, -1.0, "bearish", 1.0, 0) for i in range(3)
    ]
    oracle_b = _bare_oracle(signals=signals_veto, daily_vol=0.01)
    oracle_b._cross_lens_anti_signals = lambda ticker, direction: []
    oracle_b._regulatory_anti_signals = lambda ticker, direction="CALL": []
    assert oracle_b._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1) == []

    # Case C: Unanimous bulls, but hard vetoes from cross-lens supply shock + regulatory threat
    signals_unanimous = [
        oe.Signal(f"bull_{i}", "equity", 1.0, 1.0, "bullish", 1.0, 0) for i in range(6)
    ]
    oracle_c = _bare_oracle(signals=signals_unanimous, daily_vol=0.01)
    oracle_c._cross_lens_anti_signals = lambda ticker, direction: [
        oe.AntiSignal("cross_lens_supply_shock", "supply", 0.2, -0.8, "upstream shock", 0.8),
    ]
    oracle_c._regulatory_anti_signals = lambda ticker, direction="CALL": [
        oe.AntiSignal("regulatory_threat", "regulatory", 0.7, 0.0, "FDA warning", 0.7),
    ]
    assert oracle_c._oracle_one_ticker(1, "XOM", 1, NOW, {}, horizon_days=1) == []


# ── Defect 7: Count-robust strength & non-saturating confidence ──────────────


def test_defect7_count_robust_confidence_does_not_saturate(monkeypatch) -> None:
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    import intelligence.signal_weight_overrides as swo
    monkeypatch.setattr(swo, "SIGNAL_OVERRIDES_ENABLED", False)

    seen_raw: list[float] = []
    monkeypatch.setitem(
        sys.modules,
        "intelligence.confidence_calibration",
        SimpleNamespace(
            calibrate_confidence=lambda value, model_name, engine: (seen_raw.append(value) or value)
        ),
    )

    # 6 agreeing signals at z=1.0, w=1.0 previously hit raw_confidence=6.0 -> raw/5=1.0 -> 0.95 cap.
    sigs_6 = [oe.Signal(f"s{i}", "equity", 1.0, 1.0, "bullish", 1.0, 0) for i in range(6)]
    oracle = _bare_oracle(signals=sigs_6, daily_vol=0.01)
    oracle._find_anti_signals = lambda sigs, direction, ticker=None, **kw: []

    preds = oracle._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)
    assert len(preds) == 1
    expected_strength = 6.0 / math.sqrt(6.0)
    expected_conf = 0.5 + 0.45 * math.tanh(expected_strength / 3.0)
    assert preds[0].signal_strength == pytest.approx(expected_strength, abs=0.002)
    assert preds[0].confidence == pytest.approx(expected_conf, abs=0.001)
    assert preds[0].confidence < 0.85  # no longer saturates to 0.95
    assert len(seen_raw) == 1 and seen_raw[0] == pytest.approx(expected_conf, abs=1e-4)

    # Low model weight clamps raw_capped to floor 0.5 before calibration
    weak_model = oe.OracleModel("weak", "1.0", "test", ["equity"], weight=0.2)
    oracle_low = _bare_oracle(models=[weak_model], signals=sigs_6, daily_vol=0.01)
    oracle_low._find_anti_signals = lambda sigs, direction, ticker=None, **kw: []
    preds_low = oracle_low._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)
    assert preds_low[0].confidence == pytest.approx(0.5, abs=1e-4)


# ── Defect 8: Volatility- and horizon-scaled expected move + no_vol reject ───


def test_defect8_expected_move_vol_and_horizon_scaled(monkeypatch) -> None:
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    import intelligence.signal_weight_overrides as swo
    monkeypatch.setattr(swo, "SIGNAL_OVERRIDES_ENABLED", False)
    monkeypatch.setitem(
        sys.modules,
        "intelligence.confidence_calibration",
        SimpleNamespace(calibrate_confidence=lambda v, *a: v),
    )

    # 3 agreeing signals at z=1.5, w=1.0 -> strength = 4.5 / sqrt(3) = 2.5981 -> strength_norm = 0.8660
    sigs = [oe.Signal(f"s{i}", "equity", 1.0, 1.5, "bullish", 1.0, 0) for i in range(3)]
    strength = 4.5 / math.sqrt(3.0)
    strength_norm = min(1.0, strength / 3.0)

    oracle_1d = _bare_oracle(signals=sigs, daily_vol=0.01, spot=500.0)
    oracle_1d._find_anti_signals = lambda s, d, ticker=None, **kw: []
    pred_1d = oracle_1d._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=1)[0]

    expected_1d = strength_norm * 0.01 * math.sqrt(1 * 252.0 / 365.0) * 100.0
    assert pred_1d.expected_move_pct == pytest.approx(round(expected_1d, 2), abs=0.01)
    assert pred_1d.expected_move_pct < 1.5  # realistic 1-day SPY target, not 5%

    oracle_16d = _bare_oracle(signals=sigs, daily_vol=0.01, spot=500.0)
    oracle_16d._find_anti_signals = lambda s, d, ticker=None, **kw: []
    pred_16d = oracle_16d._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=16)[0]
    expected_16d = strength_norm * 0.01 * math.sqrt(16 * 252.0 / 365.0) * 100.0
    assert pred_16d.expected_move_pct == pytest.approx(round(expected_16d, 2), abs=0.01)
    assert pred_16d.expected_move_pct == pytest.approx(pred_1d.expected_move_pct * 4.0, abs=0.05)

    # 365 calendar days converts to 252 trading days under sqrt (not sqrt(365))
    oracle_1y = _bare_oracle(signals=sigs, daily_vol=0.005, spot=500.0)
    oracle_1y._find_anti_signals = lambda s, d, ticker=None, **kw: []
    pred_1y = oracle_1y._oracle_one_ticker(1, "SPY", 1, NOW, {}, horizon_days=365)[0]
    expected_1y = strength_norm * 0.005 * math.sqrt(252.0) * 100.0
    unconverted_1y = strength_norm * 0.005 * math.sqrt(365.0) * 100.0
    assert pred_1y.expected_move_pct == pytest.approx(round(expected_1y, 2), abs=0.01)
    assert pred_1y.expected_move_pct < round(unconverted_1y, 2)

    # Extreme vol * horizon caps at 30%
    oracle_cap = _bare_oracle(signals=sigs, daily_vol=0.10, spot=500.0)
    oracle_cap._find_anti_signals = lambda s, d, ticker=None, **kw: []
    pred_cap = oracle_cap._oracle_one_ticker(1, "MEME", 1, NOW, {}, horizon_days=90)[0]
    assert pred_cap.expected_move_pct == 30.0


def test_defect8_rejects_with_no_vol_when_realized_vol_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(oe, "_USE_SIGNAL_REGISTRY", False)
    import intelligence.signal_weight_overrides as swo
    monkeypatch.setattr(swo, "SIGNAL_OVERRIDES_ENABLED", False)
    monkeypatch.setitem(
        sys.modules,
        "intelligence.confidence_calibration",
        SimpleNamespace(calibrate_confidence=lambda v, *a: v),
    )

    sigs = [oe.Signal(f"s{i}", "equity", 1.0, 1.5, "bullish", 1.0, 0) for i in range(3)]
    models = [
        oe.OracleModel(f"m{i}", "1.0", "test", ["equity", "vol"])
        for i in range(4)
    ]
    oracle = _bare_oracle(models=models, signals=sigs, daily_vol=None)
    vol_lookups = 0

    def counting_no_vol(ticker, as_of, window=20):
        nonlocal vol_lookups
        vol_lookups += 1
        return None

    oracle._get_realized_daily_vol = counting_no_vol
    oracle._find_anti_signals = lambda s, d, ticker=None, **kw: []

    warnings: list[str] = []
    sink_id = log.add(
        lambda msg: warnings.append(msg.record["message"]) if msg.record["level"].name == "WARNING" else None,
        level="WARNING",
    )
    try:
        preds = oracle._oracle_one_ticker(1, "ILLIQ", 2, NOW, {}, horizon_days=1)
        assert preds == []
        assert vol_lookups == 1
        assert len([m for m in warnings if "no_vol" in m]) == 1

        # Second ticker emits its own single no_vol warning (once per ticker)
        preds2 = oracle._oracle_one_ticker(2, "ILLIQ2", 2, NOW, {}, horizon_days=1)
        assert preds2 == []
        assert vol_lookups == 2
        assert len([m for m in warnings if "no_vol" in m]) == 2
    finally:
        log.remove(sink_id)


def test_defect8_get_realized_daily_vol_uses_pit_closes_and_handles_splits(monkeypatch) -> None:
    import scripts.score_oracle_trades as sot

    oracle = oe.OracleEngine.__new__(oe.OracleEngine)
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    oracle.engine = MagicMock()
    oracle.engine.connect.return_value = conn

    # 21 settled trading days alternating +1% / -1% around a 10:1 split on day 10
    base_day = date(2026, 10, 20)
    split_day = base_day + timedelta(days=10)
    raw_closes: dict[date, Observation] = {}
    price = 1000.0
    true_prices = []
    for i in range(21):
        d = base_day + timedelta(days=i)
        if i > 0:
            ret = 0.01 if i % 2 == 1 else -0.01
            price *= math.exp(ret)
        true_prices.append(price)
        # Simulate raw as-traded TIINGO close dividing by 10 on/after split_day
        stored_val = price / 10.0 if d >= split_day else price
        raw_closes[d] = Observation(
            "YF:NVDA:close", d, stored_val,
            datetime(d.year, d.month, d.day, 22, 0, tzinfo=timezone.utc),
            "TIINGO",
        )

    expected_vol = float(np.std(np.diff(np.log(np.asarray(true_prices))), ddof=1))

    captured_kwargs: dict = {}

    def fake_load_closes(c, tickers, start, end, *, as_of_ts=None):
        captured_kwargs.update(tickers=tickers, start=start, end=end, as_of_ts=as_of_ts)
        return {"NVDA": raw_closes}

    monkeypatch.setattr(sot, "load_expiry_closes", fake_load_closes)
    monkeypatch.setattr(
        sot,
        "load_split_book",
        lambda c, tickers, as_of_ts=None: {"NVDA": [SplitEvent("NVDA", split_day, 10.0, "TIINGO")]},
    )

    vol = oracle._get_realized_daily_vol("NVDA", as_of=NOW, window=20)
    assert vol is not None
    assert vol == pytest.approx(expected_vol, rel=1e-4)
    assert captured_kwargs["as_of_ts"] == NOW
    assert captured_kwargs["end"] == NOW.date()

    # Fewer than 21 sessions -> fails closed (None)
    short_closes = dict(list(raw_closes.items())[:15])
    monkeypatch.setattr(sot, "load_expiry_closes", lambda *a, **kw: {"NVDA": short_closes})
    assert oracle._get_realized_daily_vol("NVDA", as_of=NOW, window=20) is None
