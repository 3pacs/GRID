"""Unit tests for verified defects in analysis/thesis_scorer.py model scorers.

All inputs are small synthetic fixtures with mocked DB connections/helpers
(no database, no network). Covers defects 1-7:
  1. _score_fed_liquidity: $50B/$100B bands in millions USD, unit consistency,
     L191 negative-band sign fix, fail-closed on missing components.
  2. _score_dealer_gamma / _score_vanna_charm: 2-trading-day max age using
     ingestion.market_calendar, status="stale", confidence=0, excluded from score.
  3. _score_social_sentiment: contrarian sign (negated) + consistent text.
  4. _score_cftc_positioning: equity-index direction; Gold safe-haven risk-off;
     WTI excluded from equity score.
  5. _score_valuation_compression: require EPS growth > 0 to claim earnings
     growth; price-driven compression described accurately and not scored bullish.
  6. _score_news_sentiment: include neutrals in denominator and require >= 5
     directional articles for a nonzero score.
  7. snapshot_thesis risk_factors: populated when direction == NEUTRAL too.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from analysis import thesis_scorer as ts
from ingestion.altdata.cftc_markets import series_id_for_root


# ── Mock helpers ───────────────────────────────────────────────────────────


def _mock_engine(execute_fn):
    """Build a minimal Engine stub whose connect()/begin() yield a mock conn."""
    conn = MagicMock()
    conn.execute.side_effect = execute_fn
    engine = MagicMock()

    @contextmanager
    def _ctx():
        yield conn

    engine.connect.side_effect = _ctx
    engine.begin.side_effect = _ctx
    return engine, conn


def _patch_clock(monkeypatch, fixed_day: date):
    """Freeze thesis_scorer.datetime.now() and thesis_scorer.date.today()."""
    fixed_dt = datetime(fixed_day.year, fixed_day.month, fixed_day.day, 15, 0, tzinfo=timezone.utc)

    class _FakeDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_dt if tz is not None else fixed_dt.replace(tzinfo=None)

    class _FakeDate(date):
        @classmethod
        def today(cls):
            return fixed_day

    monkeypatch.setattr(ts, "datetime", _FakeDatetime)
    monkeypatch.setattr(ts, "date", _FakeDate)


# ── Defect 1: _score_fed_liquidity ─────────────────────────────────────────


def _fed_liq_engine(current: dict[str, float | None], prior: dict[str, float | None], pull_ts=None):
    pull_ts = pull_ts or datetime.now(timezone.utc)

    def execute(stmt, *args, **kwargs):
        sql = str(stmt)
        src = prior if "CURRENT_DATE - 30" in sql else current
        res = MagicMock()
        if "WALCL" in sql:
            val = src.get("WALCL")
            if val is None:
                res.fetchone.return_value = None
            elif "pull_timestamp" in sql:
                res.fetchone.return_value = (val, pull_ts)
            else:
                res.fetchone.return_value = (val,)
        elif "RRPONTSYD" in sql:
            val = src.get("RRPONTSYD")
            res.fetchone.return_value = None if val is None else (val,)
        elif "WTREGEN" in sql:
            val = src.get("WTREGEN")
            res.fetchone.return_value = None if val is None else (val,)
        else:
            res.fetchone.return_value = None
        return res

    engine, _ = _mock_engine(execute)
    return engine


def test_fed_liquidity_23069m_worked_example_does_not_peg_at_80():
    # +$23,069M 30-day change is well inside the ±$50B (±50,000M) band.
    current = {"WALCL": 6_743_031.0, "WTREGEN": 948_674.0, "RRPONTSYD": 1.501}
    prior = {"WALCL": 6_719_962.0, "WTREGEN": 948_674.0, "RRPONTSYD": 1.501}

    verdict = ts._score_fed_liquidity(_fed_liq_engine(current, prior), accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] == pytest.approx(23_069.0 / 50_000.0 * 25.0, abs=0.1)
    assert verdict["score"] < 20.0
    assert "$50B" in verdict["threshold"]
    assert "$100B" in verdict["threshold"]
    assert "Expanding liquidity supports risk assets." not in verdict["reasoning"]


@pytest.mark.parametrize(
    "change_m,expected_score",
    [
        (150_000.0, 80.0),
        (100_000.0, 60.0),
        (75_000.0, 45.0),
        (50_000.0, 30.0),
        (0.0, 0.0),
        (-50_000.0, -30.0),
        (-75_000.0, -45.0),
        (-100_000.0, -60.0),
        (-150_000.0, -80.0),
    ],
)
def test_fed_liquidity_50b_and_100b_bands_are_calibrated_and_symmetric(change_m, expected_score):
    base_walcl = 7_000_000.0
    current = {"WALCL": base_walcl + change_m, "WTREGEN": 750_000.0, "RRPONTSYD": 100.0}
    prior = {"WALCL": base_walcl, "WTREGEN": 750_000.0, "RRPONTSYD": 100.0}

    verdict = ts._score_fed_liquidity(_fed_liq_engine(current, prior), accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] == pytest.approx(expected_score, abs=0.1)


def test_fed_liquidity_rrp_billions_drain_converts_consistently():
    # RRP rising +$50B (100B -> 150B) drains $50,000M of net liquidity -> -30.0.
    current = {"WALCL": 7_000_000.0, "WTREGEN": 750_000.0, "RRPONTSYD": 150.0}
    prior = {"WALCL": 7_000_000.0, "WTREGEN": 750_000.0, "RRPONTSYD": 100.0}

    verdict = ts._score_fed_liquidity(_fed_liq_engine(current, prior), accuracy=0.5)

    assert verdict["score"] == pytest.approx(-30.0, abs=0.1)
    assert verdict["direction"] == "bearish"


@pytest.mark.parametrize(
    "missing_in,missing_key",
    [
        ("current", "RRPONTSYD"),
        ("current", "WTREGEN"),
        ("prior", "RRPONTSYD"),
        ("prior", "WTREGEN"),
        ("prior", "WALCL"),
    ],
)
def test_fed_liquidity_missing_component_fails_closed(missing_in, missing_key):
    current = {"WALCL": 7_000_000.0, "WTREGEN": 750_000.0, "RRPONTSYD": 100.0}
    prior = {"WALCL": 7_000_000.0, "WTREGEN": 750_000.0, "RRPONTSYD": 100.0}
    (current if missing_in == "current" else prior)[missing_key] = None

    verdict = ts._score_fed_liquidity(_fed_liq_engine(current, prior), accuracy=0.5)

    assert verdict["status"] in ("no_data", "stale")
    assert verdict["confidence"] == 0
    assert verdict["score"] == 0


# ── Defect 2: _score_dealer_gamma and _score_vanna_charm ───────────────────


def _options_engine(row):
    def execute(stmt, *args, **kwargs):
        res = MagicMock()
        res.fetchone.return_value = row
        return res

    engine, _ = _mock_engine(execute)
    return engine


def test_dealer_gamma_marks_stale_after_2_trading_days(monkeypatch):
    # Signal from Fri 2026-11-20 evaluated on Wed 2026-11-25 = 3 trading days old.
    _patch_clock(monkeypatch, date(2026, 11, 25))
    engine = _options_engine((1.45, 580.0, date(2026, 11, 20)))

    verdict = ts._score_dealer_gamma(engine, accuracy=0.5)

    assert verdict["status"] == "stale"
    assert verdict["confidence"] == 0
    assert verdict["score"] == 0


def test_vanna_charm_marks_stale_after_2_trading_days(monkeypatch):
    # Signal from Fri 2026-11-20 evaluated on Wed 2026-11-25 = 3 trading days old.
    _patch_clock(monkeypatch, date(2026, 11, 25))
    engine = _options_engine((560.0, 580.0, date(2026, 11, 20)))

    verdict = ts._score_vanna_charm(engine, accuracy=0.5)

    assert verdict["status"] == "stale"
    assert verdict["confidence"] == 0
    assert verdict["score"] == 0


def test_options_models_use_trading_calendar_for_weekends_and_holidays(monkeypatch):
    # Fri 2026-11-20 -> Sun 2026-11-22 (0 trading days) and Tue 2026-11-24 (2 trading days): active.
    for as_of in (date(2026, 11, 22), date(2026, 11, 24)):
        _patch_clock(monkeypatch, as_of)
        dg = ts._score_dealer_gamma(_options_engine((1.45, 580.0, date(2026, 11, 20))), 0.5)
        vc = ts._score_vanna_charm(_options_engine((560.0, 580.0, date(2026, 11, 20))), 0.5)
        assert dg["status"] == "active" and dg["confidence"] > 0
        assert vc["status"] == "active" and vc["confidence"] > 0

    # Thanksgiving Thu 2026-11-26 is a US market holiday:
    # Tue 2026-11-24 -> Fri 2026-11-27 has only 2 elapsed trading days (Wed 25, Fri 27) -> active.
    _patch_clock(monkeypatch, date(2026, 11, 27))
    dg_ok = ts._score_dealer_gamma(_options_engine((1.45, 580.0, date(2026, 11, 24))), 0.5)
    vc_ok = ts._score_vanna_charm(_options_engine((560.0, 580.0, date(2026, 11, 24))), 0.5)
    assert dg_ok["status"] == "active" and dg_ok["confidence"] > 0
    assert vc_ok["status"] == "active" and vc_ok["confidence"] > 0

    # Tue 2026-11-24 -> Mon 2026-11-30 has 3 elapsed trading days (Wed 25, Fri 27, Mon 30) -> stale.
    _patch_clock(monkeypatch, date(2026, 11, 30))
    dg_stale = ts._score_dealer_gamma(_options_engine((1.45, 580.0, date(2026, 11, 24))), 0.5)
    vc_stale = ts._score_vanna_charm(_options_engine((560.0, 580.0, date(2026, 11, 24))), 0.5)
    assert dg_stale["status"] == "stale" and dg_stale["confidence"] == 0 and dg_stale["score"] == 0
    assert vc_stale["status"] == "stale" and vc_stale["confidence"] == 0 and vc_stale["score"] == 0


def test_stale_models_are_excluded_from_score_thesis(monkeypatch):
    def _score_stale_options(engine, acc):
        return ts._verdict("dealer_gamma", "Options Sentiment (PCR)", 60, 0, "stale", "", "stale", status="stale")

    def _score_stale_nonzero_conf(engine, acc):
        # Even if a stale verdict carried nonzero confidence, score_thesis must exclude it.
        return ts._verdict("legacy_stale", "Legacy Stale", 0, 20, "stale", "", "stale", status="stale")

    def _score_active_bear(engine, acc):
        return ts._verdict("active_bear", "Active Bear", -50, 60, "bear", "", "bearish signal", status="active")

    monkeypatch.setattr(ts, "_MODEL_SCORERS", [_score_stale_options, _score_stale_nonzero_conf, _score_active_bear])
    monkeypatch.setattr(ts, "_load_model_accuracies", lambda engine: {})
    monkeypatch.setattr(ts, "_get_regime_context", lambda engine: {"regime": "CRISIS", "confidence": 0.9, "adjustments": {"dealer_gamma": 1.4}})
    ts._thesis_result_cache["data"] = None

    result = ts.score_thesis(MagicMock(), force_refresh=True)

    assert result["score"] == pytest.approx(-50.0)
    assert result["direction"] == "BEARISH"
    assert result["active_models"] == 1
    by_key = {m["key"]: m for m in result["models"]}
    assert by_key["dealer_gamma"]["weight_in_final"] == 0.0
    assert by_key["legacy_stale"]["weight_in_final"] == 0.0
    assert by_key["active_bear"]["weight_in_final"] == 1.0


# ── Defect 3: _score_social_sentiment ──────────────────────────────────────


def _social_engine(total: int, bull: int, bear: int):
    def execute(stmt, *args, **kwargs):
        res = MagicMock()
        res.fetchone.return_value = (total, bull, bear)
        return res

    engine, _ = _mock_engine(execute)
    return engine


def test_social_sentiment_euphoria_scores_contrarian_bearish():
    verdict = ts._score_social_sentiment(_social_engine(20, 16, 2), accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] == pytest.approx(-(16 - 2) / 20 * 80.0, abs=0.1)
    assert verdict["direction"] == "bearish"
    assert "euphoria" in verdict["reasoning"].lower()
    assert "bearish" in verdict["reasoning"].lower()


def test_social_sentiment_panic_scores_contrarian_bullish():
    verdict = ts._score_social_sentiment(_social_engine(20, 2, 16), accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] == pytest.approx((16 - 2) / 20 * 80.0, abs=0.1)
    assert verdict["direction"] == "bullish"
    assert "panic" in verdict["reasoning"].lower()
    assert "bullish" in verdict["reasoning"].lower()


def test_social_sentiment_balanced_is_neutral():
    verdict = ts._score_social_sentiment(_social_engine(10, 5, 5), accuracy=0.5)

    assert verdict["score"] == 0.0
    assert verdict["direction"] == "neutral"


# ── Defect 4: _score_cftc_positioning ──────────────────────────────────────


def _cftc_engine(current_by_root: dict[str, tuple[float, date]], prior_by_root: dict[str, float] | None = None):
    prior_by_root = prior_by_root or {}
    sid_to_root = {
        series_id_for_root(r, "net_speculative"): r
        for r in ("ES", "GC", "CL", "VX")
    }

    def execute(stmt, params=None):
        sql = str(stmt)
        sid = (params or {}).get("sid")
        root = sid_to_root.get(sid)
        res = MagicMock()
        if "CURRENT_DATE - 28" in sql:
            val = prior_by_root.get(root)
            res.fetchone.return_value = None if val is None else (val,)
        else:
            row = current_by_root.get(root)
            res.fetchone.return_value = row
        return res

    engine, _ = _mock_engine(execute)
    return engine


def test_cftc_short_sp500_and_long_gold_does_not_flip_positive():
    obs = date(2026, 11, 17)
    engine = _cftc_engine(
        current_by_root={"ES": (-120_000.0, obs), "GC": (220_000.0, obs)},
        prior_by_root={"ES": (-80_000.0), "GC": (180_000.0)},
    )

    verdict = ts._score_cftc_positioning(engine, accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] < -20.0
    assert verdict["direction"] == "bearish"


def test_cftc_long_gold_alone_is_risk_off_not_bullish():
    obs = date(2026, 11, 17)
    engine = _cftc_engine(
        current_by_root={"GC": (200_000.0, obs)},
        prior_by_root={"GC": (150_000.0)},
    )

    verdict = ts._score_cftc_positioning(engine, accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] < 0.0
    assert verdict["direction"] == "bearish"
    assert "risk-off" in verdict["reasoning"].lower() or "safe-haven" in verdict["threshold"].lower()


def test_cftc_short_sp500_not_flipped_by_long_wti():
    obs = date(2026, 11, 17)
    engine = _cftc_engine(
        current_by_root={"ES": (-80_000.0, obs), "CL": (250_000.0, obs)},
        prior_by_root={"ES": (-40_000.0), "CL": (200_000.0)},
    )

    verdict = ts._score_cftc_positioning(engine, accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] < -20.0
    assert verdict["direction"] == "bearish"


# ── Defect 5: _score_valuation_compression ─────────────────────────────────


def _dummy_connect_engine():
    engine = MagicMock()

    @contextmanager
    def _ctx():
        yield object()

    engine.connect.side_effect = _ctx
    return engine


def test_valuation_price_driven_compression_not_bullish_and_no_earnings_claim(monkeypatch):
    # Prices dropped 12% over 30d while trailing EPS was unchanged (0% growth):
    # P/E fell from 25.0x to 22.0x (-12%). Must NOT claim earnings growth or score bullish.
    today = date(2026, 11, 21)
    _patch_clock(monkeypatch, today)

    def fake_pe(conn, ticker, as_of):
        if ticker not in ("AAPL", "MSFT", "AMZN", "JPM"):
            return None
        if as_of == today:
            return SimpleNamespace(pe=22.0, eps_ttm=10.0, price=220.0)
        return SimpleNamespace(pe=25.0, eps_ttm=10.0, price=250.0)

    monkeypatch.setattr(ts, "trailing_pe", fake_pe)

    verdict = ts._score_valuation_compression(_dummy_connect_engine(), accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] <= 0
    assert verdict["direction"] != "bullish"
    assert "Earnings growing faster than prices" not in verdict["reasoning"]
    assert "price-driven" in verdict["reasoning"].lower()


def test_valuation_falling_eps_price_crash_not_bullish(monkeypatch):
    # Prices fell 20%, EPS fell 5% -> P/E fell ~15.8% from 20x to 16.84x.
    today = date(2026, 11, 21)
    _patch_clock(monkeypatch, today)

    def fake_pe(conn, ticker, as_of):
        if ticker not in ("AAPL", "MSFT", "AMZN", "JPM"):
            return None
        if as_of == today:
            return SimpleNamespace(pe=160.0 / 9.5, eps_ttm=9.5, price=160.0)
        return SimpleNamespace(pe=200.0 / 10.0, eps_ttm=10.0, price=200.0)

    monkeypatch.setattr(ts, "trailing_pe", fake_pe)

    verdict = ts._score_valuation_compression(_dummy_connect_engine(), accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] <= 0
    assert verdict["direction"] != "bullish"
    assert "price-driven" in verdict["reasoning"].lower()
    assert "Earnings growing faster than prices" not in verdict["reasoning"]


def test_valuation_earnings_driven_compression_claims_earnings_growth_and_bullish(monkeypatch):
    # Prices flat ($250), EPS grew +15% (10.0 -> 11.5) -> P/E compressed from 25.0x to 21.74x (-13%).
    today = date(2026, 11, 21)
    _patch_clock(monkeypatch, today)

    def fake_pe(conn, ticker, as_of):
        if ticker not in ("AAPL", "MSFT", "AMZN", "JPM"):
            return None
        if as_of == today:
            return SimpleNamespace(pe=250.0 / 11.5, eps_ttm=11.5, price=250.0)
        return SimpleNamespace(pe=250.0 / 10.0, eps_ttm=10.0, price=250.0)

    monkeypatch.setattr(ts, "trailing_pe", fake_pe)

    verdict = ts._score_valuation_compression(_dummy_connect_engine(), accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] > 0
    assert verdict["direction"] == "bullish"
    assert "Earnings growing faster than prices" in verdict["reasoning"]


# ── Defect 6: _score_news_sentiment ────────────────────────────────────────


def _news_engine(rows):
    def execute(stmt, *args, **kwargs):
        res = MagicMock()
        res.fetchall.return_value = rows
        return res

    engine, _ = _mock_engine(execute)
    return engine


def _article(sentiment: str, conf: float = 0.5, title: str = "Headline"):
    now = datetime.now(timezone.utc)
    return (title, "wire", sentiment, now, conf, None)


def test_news_sentiment_1_bearish_among_27_scores_zero_not_minus_100():
    rows = [_article("BEARISH", 0.8, "One bad story")] + [
        _article("NEUTRAL", 0.5, f"Neutral {i}") for i in range(26)
    ]

    verdict = ts._score_news_sentiment(_news_engine(rows), accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] == 0.0
    assert verdict["direction"] == "neutral"
    assert "strongly negative" not in verdict["reasoning"].lower()


def test_news_sentiment_fewer_than_5_directional_scores_zero():
    rows = [_article("BEARISH", 0.9, f"Bear {i}") for i in range(4)] + [
        _article("NEUTRAL", 0.5, f"Neutral {i}") for i in range(23)
    ]

    verdict = ts._score_news_sentiment(_news_engine(rows), accuracy=0.5)

    assert verdict["score"] == 0.0
    assert verdict["direction"] == "neutral"


def test_news_sentiment_includes_neutrals_in_denominator_when_5_plus_directional():
    # 5 bearish * 0.8 = 4.0; 22 neutral * 0.5 = 11.0; total weight = 15.0 -> -26.7 (not -100).
    rows = [_article("BEARISH", 0.8, f"Bear {i}") for i in range(5)] + [
        _article("NEUTRAL", 0.5, f"Neutral {i}") for i in range(22)
    ]

    verdict = ts._score_news_sentiment(_news_engine(rows), accuracy=0.5)

    assert verdict["status"] == "active"
    assert verdict["score"] == pytest.approx(-4.0 / 15.0 * 100.0, abs=0.1)
    assert verdict["direction"] == "bearish"


def test_news_sentiment_no_rows_fails_closed():
    verdict = ts._score_news_sentiment(_news_engine([]), accuracy=0.5)

    assert verdict["status"] == "stale"
    assert verdict["confidence"] == 0
    assert verdict["score"] == 0


# ── Defect 7: snapshot_thesis risk_factors ─────────────────────────────────


def test_snapshot_thesis_populates_risk_factors_when_neutral(monkeypatch):
    captured = {}

    def execute(stmt, params=None):
        sql = str(stmt)
        res = MagicMock()
        if "INSERT INTO thesis_snapshots" in sql:
            captured["params"] = dict(params)
            res.fetchone.return_value = (42,)
        return res

    engine, _ = _mock_engine(execute)
    monkeypatch.setattr("intelligence.thesis_tracker._ensure_tables", lambda eng: "public")
    monkeypatch.setattr("intelligence.thesis_schema.require_thesis_connection", lambda conn, expected_schema=None: "public")

    thesis = {
        "direction": "NEUTRAL",
        "conviction": 4,
        "score": -4.0,
        "bull_pct": 42.0,
        "bear_pct": 48.0,
        "active_models": 4,
        "total_models": 5,
        "evaluation_window": "5d",
        "models": [
            {"key": "geo", "name": "Geopolitical Risk", "direction": "bearish", "score": -60.0, "confidence": 70.0, "data_point": "war", "reasoning": "Elevated war risk.", "status": "active"},
            {"key": "cftc", "name": "Futures Positioning (COT)", "direction": "bearish", "score": -45.0, "confidence": 65.0, "data_point": "short", "reasoning": "Short ES.", "status": "active"},
            {"key": "fg", "name": "Fear & Greed", "direction": "bullish", "score": 50.0, "confidence": 60.0, "data_point": "20", "reasoning": "Contrarian fear.", "status": "active"},
            {"key": "flat", "name": "Flat Model", "direction": "neutral", "score": 1.0, "confidence": 50.0, "data_point": "0", "reasoning": "Flat.", "status": "active"},
            {"key": "stale_m", "name": "Stale Model", "direction": "bearish", "score": -90.0, "confidence": 0.0, "data_point": "old", "reasoning": "Stale.", "status": "stale"},
        ],
    }

    snap_id = ts.snapshot_thesis(engine, thesis)

    assert snap_id == 42
    risk_factors = json.loads(captured["params"]["rf"])
    assert len(risk_factors) == 3
    assert [r["key"] for r in risk_factors] == ["geo", "fg", "cftc"]
    assert all(r["key"] != "stale_m" and r["key"] != "flat" for r in risk_factors)


def test_snapshot_thesis_risk_factors_for_bullish_and_bearish_ranked_top_3(monkeypatch):
    monkeypatch.setattr("intelligence.thesis_tracker._ensure_tables", lambda eng: "public")
    monkeypatch.setattr("intelligence.thesis_schema.require_thesis_connection", lambda conn, expected_schema=None: "public")

    for direction, opposing_dir in (("BULLISH", "bearish"), ("BEARISH", "bullish")):
        captured = {}

        def execute(stmt, params=None):
            res = MagicMock()
            if "INSERT INTO thesis_snapshots" in str(stmt):
                captured["params"] = dict(params)
                res.fetchone.return_value = (7,)
            return res

        engine, _ = _mock_engine(execute)
        sign = -1 if opposing_dir == "bearish" else 1
        models = [
            {"key": f"m{i}", "name": f"M{i}", "direction": opposing_dir, "score": sign * s, "confidence": 60.0, "data_point": "x", "reasoning": f"R{i}", "status": "active"}
            for i, s in enumerate([20.0, 70.0, 50.0, 35.0])
        ]
        thesis = {
            "direction": direction,
            "conviction": 45,
            "score": -sign * 45.0,
            "bull_pct": 70.0,
            "bear_pct": 30.0,
            "active_models": 4,
            "total_models": 4,
            "evaluation_window": "5d",
            "models": models,
        }
        assert ts.snapshot_thesis(engine, thesis) == 7
        risk_factors = json.loads(captured["params"]["rf"])
        assert [r["key"] for r in risk_factors] == ["m1", "m2", "m3"]
