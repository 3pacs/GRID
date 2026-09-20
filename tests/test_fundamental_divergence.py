"""Tests for ``intelligence.fundamental_divergence``.

Uses a MagicMock engine with a dispatch-by-sql side_effect, matching
the pattern in ``tests/test_sector_health.py``. Every test tailors
exactly the rows the module will see so none of them touch a live
database.

Covers:
  * percentile rank helper edge cases
  * classification thresholds
  * end-to-end compute_divergence with mocked capital_flows + raw_series
  * empty-universe short-circuit
  * narrative string shape
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from intelligence import fundamental_divergence as fd


# ─────────────────────────────────────────────────────────────────
# Mock engine helpers (mirrors test_sector_health.py)
# ─────────────────────────────────────────────────────────────────


def _res(rows=None, one=None):
    m = MagicMock()
    m.fetchall.return_value = rows if rows is not None else []
    if one is not None:
        m.fetchone.return_value = one
    elif rows:
        m.fetchone.return_value = rows[0]
    else:
        m.fetchone.return_value = None
    return m


def _make_engine(side_effect):
    engine = MagicMock()
    conn = MagicMock()

    def execute(stmt, *args, **kwargs):
        sql = str(getattr(stmt, "text", stmt))
        return side_effect(sql)

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _regclass(value):
    return (value,)


# ─────────────────────────────────────────────────────────────────
# 1. Percentile helper — edge cases
# ─────────────────────────────────────────────────────────────────


def test_percentile_rank_edges_and_tiny_population():
    # Tiny population → neutral 50
    assert fd._percentile_rank(0.10, [0.05, 0.15]) == 50.0
    # None value → neutral 50
    assert fd._percentile_rank(None, [0.1, 0.2, 0.3, 0.4]) == 50.0
    # Midrank in 4-element population: value at index 2 of 4
    pop = [0.10, 0.20, 0.30, 0.40]
    pct = fd._percentile_rank(0.30, pop)
    assert 0.0 <= pct <= 100.0
    # Highest in a 4-el population should land near 100 (but not >100)
    top = fd._percentile_rank(0.40, pop)
    assert top > pct
    assert top <= 100.0


# ─────────────────────────────────────────────────────────────────
# 2. Classification thresholds
# ─────────────────────────────────────────────────────────────────


def test_classify_thresholds():
    assert fd._classify(45.0) == "long_candidate"
    assert fd._classify(30.01) == "long_candidate"
    assert fd._classify(30.0) == "aligned"  # strictly greater
    assert fd._classify(0.0) == "aligned"
    assert fd._classify(-30.0) == "aligned"  # strictly less
    assert fd._classify(-30.01) == "short_candidate"
    assert fd._classify(-55.0) == "short_candidate"


# ─────────────────────────────────────────────────────────────────
# 3. Fundamental scoring composition
# ─────────────────────────────────────────────────────────────────


def test_build_fundamental_score_weights_sum_to_100_max():
    """Best-case ticker in a sector should land at the ceiling."""
    fund = {
        "revenue_cagr": 0.50,   # top of sector distribution
        "margin_trend": "expanding",
        "shareholder_yield": 0.10,  # top of sector distribution
    }
    cagrs = [0.05, 0.10, 0.20, 0.50]
    yields = [0.01, 0.03, 0.05, 0.10]
    score = fd._build_fundamental_score(fund, cagrs, yields)
    assert 70.0 <= score <= 100.0

    # Worst-case ticker in a sector should land at the floor.
    fund_bad = {
        "revenue_cagr": -0.05,
        "margin_trend": "contracting",
        "shareholder_yield": 0.0,
    }
    score_bad = fd._build_fundamental_score(
        fund_bad,
        [-0.05, 0.02, 0.10, 0.20],
        [0.0, 0.01, 0.03, 0.05],
    )
    assert 0.0 <= score_bad <= 40.0
    assert score > score_bad


# ─────────────────────────────────────────────────────────────────
# 3b. Batched loaders (fable-daily-intel-sql-tasks, 2026-09-20)
# ─────────────────────────────────────────────────────────────────


def test_load_batch_fundamentals_groups_rows_by_ticker():
    """One combined capital_flows query must split correctly by
    ticker_key and match _fundamentals_from_period_map's per-ticker
    output — the batched loader must not cross-contaminate tickers."""
    rows = [
        ("AAA", date(2025, 12, 31), "revenue", 2744.0),
        ("AAA", date(2025, 12, 31), "cogs", 1000.0),
        ("AAA", date(2024, 12, 31), "revenue", 1960.0),
        ("AAA", date(2024, 12, 31), "cogs", 900.0),
        ("AAA", date(2023, 12, 31), "revenue", 1400.0),
        ("AAA", date(2023, 12, 31), "cogs", 700.0),
        ("AAA", date(2022, 12, 31), "revenue", 1000.0),
        ("AAA", date(2022, 12, 31), "cogs", 600.0),
        # BBB only has 2 periods -> below MIN_PERIODS, must map to None.
        ("BBB", date(2025, 12, 31), "revenue", 500.0),
        ("BBB", date(2024, 12, 31), "revenue", 480.0),
    ]

    def side_effect(sql: str):
        s = sql.lower()
        if "to_regclass" in s:
            return _res(one=_regclass("public.x"))
        if "from capital_flows" in s:
            return _res(rows=rows)
        return _res()

    engine = _make_engine(side_effect)
    with engine.connect() as conn:
        out = fd._load_batch_fundamentals(conn, ["AAA", "BBB", "CCC"])

    assert out["BBB"] is None, "under MIN_PERIODS must map to None"
    assert out["CCC"] is None, "ticker absent from the batch rows must map to None"
    assert out["AAA"] is not None
    assert out["AAA"]["periods"] == 4
    assert out["AAA"]["revenue_cagr"] == pytest.approx((2744.0 / 1000.0) ** (1 / 3) - 1)


def test_load_batch_fundamentals_missing_table_returns_all_none():
    engine = _make_engine(lambda sql: _res(one=(None,)) if "to_regclass" in sql.lower() else _res())
    with engine.connect() as conn:
        out = fd._load_batch_fundamentals(conn, ["AAA", "BBB"])
    assert out == {"AAA": None, "BBB": None}


def test_load_batch_price_cagrs_computes_per_ticker_and_respects_min_obs():
    as_of = date(2026, 4, 11)

    def side_effect(sql: str):
        s = sql.lower()
        if "to_regclass" in s:
            return _res(one=_regclass("public.x"))
        if "count(*)" in s and "from raw_series" in s:
            # BBB has too few observations to qualify.
            return _res(rows=[("YF:AAA:close", 800), ("YF:BBB:close", 10)])
        if "distinct on" in s and "from raw_series" in s:
            return _res(rows=[("YF:AAA:close", 120.0, as_of)])
        return _res()

    engine = _make_engine(side_effect)
    with engine.connect() as conn:
        out = fd._load_batch_price_cagrs(conn, ["AAA", "BBB"], as_of)

    assert out["BBB"] is None, "under MIN_PRICE_OBS must map to None"
    # Same (val, date) row returned for both the "latest" and "prior"
    # DISTINCT ON calls above -> flat price -> cagr == 0.
    assert out["AAA"] == pytest.approx(0.0)


def test_load_batch_price_cagrs_missing_table_returns_all_none():
    engine = _make_engine(lambda sql: _res(one=(None,)) if "to_regclass" in sql.lower() else _res())
    with engine.connect() as conn:
        out = fd._load_batch_price_cagrs(conn, ["AAA"], date(2026, 4, 11))
    assert out == {"AAA": None}


# ─────────────────────────────────────────────────────────────────
# 4. End-to-end compute_divergence with mocked SQL
# ─────────────────────────────────────────────────────────────────


def test_compute_divergence_writes_long_candidate_for_fundamentals_beating_price():
    """Two-ticker universe:
        AAA — strong fundamentals (rev CAGR 40%, expanding, sy 6%) + weak
              price (-10% CAGR)
        BBB — weak fundamentals (rev CAGR 2%, contracting, sy 0.5%) + strong
              price (+30% CAGR)
    AAA should land in long_candidate, BBB in short_candidate.
    """
    # Three tickers satisfy MIN_SECTOR_POPULATION=3 so percentile ranks
    # produce real spread (not neutral 50).
    fake_universe = [
        fd.SectorTicker(ticker="AAA", sector="Technology"),
        fd.SectorTicker(ticker="BBB", sector="Technology"),
        fd.SectorTicker(ticker="CCC", sector="Technology"),
    ]

    # Pre-compute the annual rows each ticker should produce.
    aaa_rows = [
        (date(2025, 12, 31), "revenue", 2744.0),   # 40% 3y cagr from 1000
        (date(2025, 12, 31), "cogs", 1000.0),       # margin ~63%
        (date(2025, 12, 31), "dividends", 80.0),
        (date(2025, 12, 31), "buybacks", 80.0),    # sy = 160/2744 ~ 5.8%
        (date(2024, 12, 31), "revenue", 1960.0),
        (date(2024, 12, 31), "cogs", 900.0),
        (date(2023, 12, 31), "revenue", 1400.0),
        (date(2023, 12, 31), "cogs", 700.0),
        (date(2022, 12, 31), "revenue", 1000.0),
        (date(2022, 12, 31), "cogs", 600.0),        # margin 40% → expanding
    ]
    bbb_rows = [
        (date(2025, 12, 31), "revenue", 1060.0),    # ~2% 3y cagr
        (date(2025, 12, 31), "cogs", 900.0),        # margin ~15%
        (date(2025, 12, 31), "dividends", 3.0),
        (date(2025, 12, 31), "buybacks", 2.0),      # sy tiny
        (date(2024, 12, 31), "revenue", 1040.0),
        (date(2024, 12, 31), "cogs", 820.0),
        (date(2023, 12, 31), "revenue", 1020.0),
        (date(2023, 12, 31), "cogs", 700.0),
        (date(2022, 12, 31), "revenue", 1000.0),
        (date(2022, 12, 31), "cogs", 600.0),        # margin 40% → contracting
    ]
    # Middle-of-pack ticker so percentile ranks have spread.
    ccc_rows = [
        (date(2025, 12, 31), "revenue", 1500.0),    # ~14% cagr
        (date(2025, 12, 31), "cogs", 750.0),        # margin 50%
        (date(2025, 12, 31), "dividends", 20.0),
        (date(2025, 12, 31), "buybacks", 20.0),     # sy ~2.6%
        (date(2024, 12, 31), "revenue", 1300.0),
        (date(2024, 12, 31), "cogs", 650.0),
        (date(2023, 12, 31), "revenue", 1150.0),
        (date(2023, 12, 31), "cogs", 570.0),
        (date(2022, 12, 31), "revenue", 1000.0),
        (date(2022, 12, 31), "cogs", 500.0),        # 50% → flat
    ]

    # AAA: latest 95 over 3y old 131 → CAGR ~ -10%
    # BBB: latest 220 over 3y old 100 → CAGR ~ +30%
    price_data = {
        "YF:AAA:close": {
            "count": 800,
            "latest": (95.0, date(2026, 4, 10)),
            "prior": (131.0, date(2023, 4, 11)),
        },
        "YF:BBB:close": {
            "count": 800,
            "latest": (220.0, date(2026, 4, 10)),
            "prior": (100.0, date(2023, 4, 11)),
        },
        "YF:CCC:close": {
            "count": 800,
            "latest": (120.0, date(2026, 4, 10)),
            "prior": (100.0, date(2023, 4, 11)),   # ~6% cagr middle
        },
    }

    # fable-daily-intel-sql-tasks (2026-09-20): compute_divergence now
    # loads fundamentals and price CAGRs for the WHOLE universe in a
    # handful of BATCHED queries instead of one (fundamentals) to three
    # (price: count/latest/prior) queries PER TICKER — see
    # _load_batch_fundamentals/_load_batch_price_cagrs. Call sequence is
    # now: one combined capital_flows query (all tickers' annual rows,
    # tagged by ticker_key), one raw_series COUNT(*) query (all series),
    # then two DISTINCT-ON raw_series queries in a fixed order (latest
    # as_of, then prior as_of — see _load_batch_price_cagrs).
    combined_fund_rows = [
        (tk, fp, ft, amt)
        for tk, rows in (("AAA", aaa_rows), ("BBB", bbb_rows), ("CCC", ccc_rows))
        for fp, ft, amt in rows
    ]
    count_rows = [
        (sid, data["count"]) for sid, data in price_data.items()
    ]
    latest_rows = [
        (sid, data["latest"][0], data["latest"][1])
        for sid, data in price_data.items()
    ]
    prior_rows = [
        (sid, data["prior"][0], data["prior"][1])
        for sid, data in price_data.items()
    ]
    state: dict = {"price_asof_calls": 0}

    def side_effect(sql: str):
        s = sql.lower()
        if "to_regclass" in s:
            return _res(one=_regclass("public.x"))

        if "from capital_flows" in s:
            return _res(rows=combined_fund_rows)

        if "count(*)" in s and "from raw_series" in s:
            return _res(rows=count_rows)

        if "distinct on" in s and "from raw_series" in s:
            state["price_asof_calls"] += 1
            rows = latest_rows if state["price_asof_calls"] == 1 else prior_rows
            return _res(rows=rows)

        return _res()

    engine = _make_engine(side_effect)
    with patch.object(fd, "_load_universe", return_value=fake_universe):
        rows = fd.compute_divergence(engine, as_of=date(2026, 4, 11))

    by_ticker = {r["ticker"]: r for r in rows}
    assert "AAA" in by_ticker
    assert "BBB" in by_ticker
    assert by_ticker["AAA"]["fundamental_score"] > by_ticker["BBB"]["fundamental_score"]
    assert by_ticker["AAA"]["price_score"] < by_ticker["BBB"]["price_score"]
    # The fundamental/price spread should be wide enough to trigger
    # opposite classifications when there are only two tickers in the
    # sector.
    assert by_ticker["AAA"]["classification"] == "long_candidate"
    assert by_ticker["BBB"]["classification"] == "short_candidate"
    # Divergences are opposite signs and above threshold.
    assert by_ticker["AAA"]["divergence"] > fd.LONG_THRESHOLD
    assert by_ticker["BBB"]["divergence"] < fd.SHORT_THRESHOLD
    # Narrative includes the ticker, classification verdict and sector.
    assert "AAA" in by_ticker["AAA"]["narrative"]
    assert "LONG candidate" in by_ticker["AAA"]["narrative"]
    assert "SHORT candidate" in by_ticker["BBB"]["narrative"]


# ─────────────────────────────────────────────────────────────────
# 5. Empty universe short-circuit
# ─────────────────────────────────────────────────────────────────


def test_compute_divergence_empty_universe_returns_empty_list():
    engine = _make_engine(lambda sql: _res())
    with patch.object(fd, "_load_universe", return_value=[]):
        rows = fd.compute_divergence(engine, as_of=date(2026, 4, 11))
    assert rows == []
