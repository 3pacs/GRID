"""Time-integrity guards: look-ahead, stale-as-fresh, series corruption.

Batch 5 of the fake-data remediation. Every test here pins one rule:

  * a value dated *D* may only rest on data that existed at *D*
    (no later options chain standing in for an earlier one),
  * a computation that failed is null, not zero/"neutral",
  * a price written into ``raw_series`` carries the trading day it actually
    belongs to — never ``_today()`` on a weekend read,
  * a window-derived metric is named after the window it measured.

Audit references: C-H9, C-M5, C-M6, C-M13, C-M14, C-M17, D-H13, D-M3,
D-M30, B-H10.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

os.environ.setdefault("DB_PASSWORD", "test-password")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _pwd_ctx.hash("testpassword123"))

from fastapi.testclient import TestClient

from api.auth import create_token
from api.main import app
from physics.dealer_gamma import DealerGammaEngine

client = TestClient(app)


def _auth_header() -> dict[str, str]:
    return {"Authorization": f"Bearer {create_token(expires_hours=1)}"}


def _today() -> date:
    """Today's date, used for every relative fixture in this module.

    Nothing here compares this against a date produced inside production
    code (DB reads are mocked), so the UTC calendar day is a safe stand-in
    and keeps the module free of DTZ011.
    """
    return datetime.now(timezone.utc).date()


def _chain_frame() -> pd.DataFrame:
    """Minimal non-empty chain — the numbers don't matter here, the date does."""
    return pd.DataFrame(
        [
            {"strike": 100.0, "opt_type": "call", "open_interest": 1000.0,
             "implied_volatility": 0.25, "dte": 30.0},
            {"strike": 100.0, "opt_type": "put", "open_interest": 1000.0,
             "implied_volatility": 0.25, "dte": 30.0},
        ]
    )


# ── 1. compute_gex_profile: the chain's own date, never a later one ────────


class TestGexPointInTime:
    def test_requested_date_with_no_chain_is_unavailable_not_substituted(self):
        """C-H9: an explicit snap_date is strict — a later chain is never used."""
        engine = DealerGammaEngine(MagicMock())
        chain_dates = {date(2026, 3, 20): _chain_frame()}  # only a LATER chain

        engine._load_chain = lambda _t, d: chain_dates.get(d, pd.DataFrame())
        engine._latest_snap_date_on_or_before = lambda _t, _c: None
        engine._get_spot = lambda _t, _d: 100.0

        result = engine.compute_gex_profile("SPY", date(2026, 3, 10))

        assert result["available"] is False
        assert result["snap_date"] is None
        assert result["requested_snap_date"] == "2026-03-10"
        assert "gex_aggregate" not in result

    def test_as_of_uses_the_latest_chain_on_or_before_and_dates_it_honestly(self):
        """The 'as of' read may reach backwards. Never forwards."""
        engine = DealerGammaEngine(MagicMock())
        earlier, asked, later = date(2026, 3, 6), date(2026, 3, 10), date(2026, 3, 20)
        chains = {earlier: _chain_frame(), later: _chain_frame()}

        engine._load_chain = lambda _t, d: chains.get(d, pd.DataFrame())
        engine._latest_snap_date_on_or_before = lambda _t, cutoff: max(
            (d for d in chains if d <= cutoff), default=None
        )
        engine._get_spot = lambda _t, _d: 100.0

        result = engine.compute_gex_profile("SPY", asked, as_of=True)

        assert result["available"] is True
        assert result["snap_date"] == str(earlier)
        assert result["requested_snap_date"] == str(asked)
        assert date.fromisoformat(result["snap_date"]) <= asked

    def test_spot_is_read_at_the_chain_date_not_the_requested_date(self):
        """A chain from D-4 must be priced against D-4's spot, not D's."""
        engine = DealerGammaEngine(MagicMock())
        earlier, asked = date(2026, 3, 6), date(2026, 3, 10)
        spot_calls: list[date] = []

        engine._load_chain = lambda _t, d: _chain_frame() if d == earlier else pd.DataFrame()
        engine._latest_snap_date_on_or_before = lambda _t, _c: earlier

        def _spot(_ticker, d):
            spot_calls.append(d)
            return 100.0

        engine._get_spot = _spot
        engine.compute_gex_profile("SPY", asked, as_of=True)

        assert spot_calls == [earlier]

    def test_load_chain_has_no_latest_snapshot_fallback(self):
        """The look-ahead used to live here: no rows -> recurse on MAX(snap_date)."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        frame = DealerGammaEngine(mock_engine)._load_chain("SPY", date(2026, 3, 10))

        assert frame.empty
        assert mock_conn.execute.call_count == 1
        assert "MAX(snap_date)" not in str(mock_conn.execute.call_args[0][0])

    def test_latest_snapshot_lookup_is_bounded_by_the_cutoff(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (date(2026, 3, 6),)
        mock_engine = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        engine = DealerGammaEngine(mock_engine)
        found = engine._latest_snap_date_on_or_before("SPY", date(2026, 3, 10))

        sql = str(mock_conn.execute.call_args[0][0])
        assert "snap_date <= :cutoff" in sql
        assert mock_conn.execute.call_args[0][1]["cutoff"] == date(2026, 3, 10)
        assert found == date(2026, 3, 6)


# ── 2. /derivatives/overview: the dates actually used (C-M6) ───────────────


class TestMarketGexSummaryDates:
    def test_summary_reports_snap_date_range_not_today(self):
        engine = DealerGammaEngine(MagicMock())
        engine.compute_all_tickers = lambda _snap=None, as_of=False: [
            {"ticker": "SPY", "snap_date": "2026-03-09", "spot": 500.0,
             "gex_aggregate": 1.0, "vanna_exposure": 0.0, "charm_exposure": 0.0,
             "regime": "LONG_GAMMA", "gamma_flip": None, "put_wall": None,
             "call_wall": None},
            {"ticker": "QQQ", "snap_date": "2026-03-06", "spot": 400.0,
             "gex_aggregate": -1.0, "vanna_exposure": 0.0, "charm_exposure": 0.0,
             "regime": "SHORT_GAMMA", "gamma_flip": None, "put_wall": None,
             "call_wall": None},
        ]

        summary = engine.get_market_gex_summary()

        assert summary["snap_date_min"] == "2026-03-06"
        assert summary["snap_date_max"] == "2026-03-09"
        assert "snap_date" not in summary
        assert {t["ticker"]: t["snap_date"] for t in summary["tickers"]} == {
            "SPY": "2026-03-09", "QQQ": "2026-03-06",
        }

    def test_no_today_substitution_left_in_the_source(self):
        """Criterion 3's grep, as an assertion."""
        src = Path(__file__).resolve().parents[1] / "physics" / "dealer_gamma.py"
        assert "snap_date or _today()" not in src.read_text(encoding="utf-8")


# ── 3. /flow-timeline: no look-ahead, no fabricated zero bars ──────────────


def _timeline_rows(sig_dates: list[date]) -> list[tuple]:
    return [(d, 500.0, 0.2, 0.9, 1000) for d in sig_dates]


def _wire_timeline_db(mock_engine, rows):
    mock_conn = MagicMock()
    mock_conn.execute.return_value.fetchall.return_value = rows
    mock_engine.return_value.connect.return_value.__enter__ = MagicMock(
        return_value=mock_conn
    )
    mock_engine.return_value.connect.return_value.__exit__ = MagicMock(
        return_value=False
    )


class TestFlowTimeline:
    @patch("api.routers.derivatives._get_gex_engine")
    @patch("api.routers.derivatives.get_db_engine")
    def test_dealer_gamma_no_lookahead(self, mock_db, mock_gex):
        """Every bar's chain_snap_date is at or before the bar's own date."""
        today = _today()
        sig_dates = [today - timedelta(days=n) for n in (30, 20, 10, 3)]
        _wire_timeline_db(mock_db, _timeline_rows(sig_dates))

        # A single chain exists, and it is NEWER than the first three bars.
        chain_date = today - timedelta(days=5)

        def _profile(ticker, snap_date=None, as_of=False, **_kw):
            assert as_of is True, "historical bars must use as-of semantics"
            used = min(snap_date, chain_date)
            if used < today - timedelta(days=40):
                return {"available": False, "error": "no chain",
                        "ticker": ticker, "snap_date": None}
            return {
                "available": True, "ticker": ticker, "snap_date": str(used),
                "requested_snap_date": str(snap_date), "spot": 500.0,
                "gex_aggregate": 1.5e9, "regime": "LONG_GAMMA",
            }

        mock_gex.return_value.compute_gex_profile = _profile

        response = client.get(
            "/api/v1/derivatives/flow-timeline/SPY?days=90", headers=_auth_header()
        )

        assert response.status_code == 200
        history = response.json()["history"]
        assert len(history) == len(sig_dates)
        for bar in history:
            assert bar["chain_snap_date"] is not None
            assert date.fromisoformat(bar["chain_snap_date"]) <= date.fromisoformat(
                bar["date"]
            ), f"look-ahead: {bar['chain_snap_date']} used for bar {bar['date']}"

    @patch("api.routers.derivatives._get_gex_engine")
    @patch("api.routers.derivatives.get_db_engine")
    def test_failed_bar_is_null_not_zero_neutral(self, mock_db, mock_gex):
        """C-M5: an exception must not append net_gex 0 / regime 'neutral'."""
        today = _today()
        good_date, bad_date = today - timedelta(days=10), today - timedelta(days=5)
        _wire_timeline_db(mock_db, _timeline_rows([good_date, bad_date]))

        def _profile(ticker, snap_date=None, as_of=False, **_kw):
            if snap_date == bad_date:
                raise RuntimeError("chain read blew up")
            return {
                "available": True, "ticker": ticker, "snap_date": str(snap_date),
                "spot": 500.0, "gex_aggregate": 2.0e9, "regime": "LONG_GAMMA",
            }

        mock_gex.return_value.compute_gex_profile = _profile

        response = client.get(
            "/api/v1/derivatives/flow-timeline/SPY?days=30", headers=_auth_header()
        )

        bars = {b["date"]: b for b in response.json()["history"]}
        failed = bars[str(bad_date)]
        assert failed["net_gex"] is None
        assert failed["regime"] is None
        assert failed["chain_snap_date"] is None
        assert bars[str(good_date)]["net_gex"] == round(2.0e9)

    @patch("api.routers.derivatives._get_gex_engine")
    @patch("api.routers.derivatives.get_db_engine")
    def test_unavailable_bar_does_not_fake_a_gamma_flip_crossing(self, mock_db, mock_gex):
        """A null bar can neither confirm nor break a sign change."""
        today = _today()
        dates = [today - timedelta(days=n) for n in (20, 15, 10)]
        _wire_timeline_db(mock_db, _timeline_rows(dates))

        def _profile(ticker, snap_date=None, as_of=False, **_kw):
            if snap_date == dates[1]:
                return {"available": False, "error": "no chain",
                        "ticker": ticker, "snap_date": None}
            return {
                "available": True, "ticker": ticker, "snap_date": str(snap_date),
                "spot": 500.0, "gex_aggregate": 1.0e9, "regime": "LONG_GAMMA",
            }

        mock_gex.return_value.compute_gex_profile = _profile

        payload = client.get(
            "/api/v1/derivatives/flow-timeline/SPY?days=30", headers=_auth_header()
        ).json()

        assert payload["gamma_flip_crossings"] == []
        assert payload["history"][1]["net_gex"] is None

    @patch("api.routers.derivatives._get_gex_engine")
    def test_gex_endpoint_refuses_a_later_chain_for_a_requested_date(self, mock_gex):
        """GET /gex/{t}?snap_date=<no chain> -> available:false, not a profile."""
        captured: dict = {}

        def _profile(ticker, snap_date=None, **kwargs):
            captured["snap_date"] = snap_date
            captured["as_of"] = kwargs.get("as_of", False)
            return {"available": False, "error": f"No options data for {ticker}",
                    "ticker": ticker, "snap_date": None,
                    "requested_snap_date": str(snap_date)}

        mock_gex.return_value.compute_gex_profile = _profile

        response = client.get(
            "/api/v1/derivatives/gex/SPY?snap_date=2026-03-10", headers=_auth_header()
        )

        assert response.status_code == 200
        body = response.json()
        assert body["available"] is False
        assert "gex_aggregate" not in body
        assert captured["snap_date"] == date(2026, 3, 10)
        assert captured["as_of"] is False, "an explicit date must not fall back"


# ── 4. Prices carry their own bar date (C-M14, D-M30) ─────────────────────


def _ohlc(dates, close):
    return pd.DataFrame(
        {
            "Open": list(close),
            "High": [c * 1.01 for c in close],
            "Low": [c * 0.99 for c in close],
            "Close": list(close),
            "Volume": [1_000_000] * len(close),
        },
        index=pd.to_datetime(dates),
    )


class TestPriceBarDates:
    def test_friday_close_served_on_sunday_reports_friday(self):
        """C-M14: updated_at is the bar's timestamp, not now()."""
        from api.routers.watchlist_helpers import _batch_fetch_prices

        frame = _ohlc(["2026-03-12", "2026-03-13"], [180.0, 182.0])  # Thu, Fri

        with patch("yfinance.download", return_value=frame):
            result = _batch_fetch_prices(["SPY"])

        quote = result["SPY"]
        assert quote["bar_date"] == "2026-03-13"
        assert quote["updated_at"].startswith("2026-03-13")
        assert not quote["updated_at"].startswith(_today().isoformat())
        assert quote["fetched_at"] != quote["updated_at"]

    def test_live_history_fallback_reports_its_bar_date(self):
        from api.routers.watchlist_helpers import _fetch_live_price

        frame = _ohlc(["2026-03-12", "2026-03-13"], [180.0, 182.0])
        fake_info = MagicMock()
        fake_info.last_price = None
        fake_info.previous_close = None
        fake_ticker = MagicMock()
        fake_ticker.fast_info = fake_info
        fake_ticker.history.return_value = frame

        with patch("yfinance.Ticker", return_value=fake_ticker):
            live = _fetch_live_price("SPY")

        assert live["bar_date"] == "2026-03-13"

    def test_undated_fast_info_quote_reports_no_bar_date(self):
        """fast_info has no date of its own, so we must not invent one."""
        from api.routers.watchlist_helpers import _fetch_live_price

        fake_info = MagicMock()
        fake_info.last_price = 182.0
        fake_info.previous_close = 180.0
        fake_ticker = MagicMock()
        fake_ticker.fast_info = fake_info

        with patch("yfinance.Ticker", return_value=fake_ticker):
            live = _fetch_live_price("SPY")

        assert live["price"] == 182.0
        assert live["bar_date"] is None

    def test_cache_skips_the_write_when_the_bar_date_is_unknown(self):
        from api.routers.watchlist_helpers import _cache_price_to_db

        engine = MagicMock()
        _cache_price_to_db(engine, "SPY", 182.0, None)

        assert engine.begin.call_count == 0

    def test_cached_row_lands_on_the_bar_date_not_today(self):
        from api.routers.watchlist_helpers import _cache_price_to_db

        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (7,)
        engine = MagicMock()
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        _cache_price_to_db(engine, "SPY", 182.0, "2026-03-13")

        insert = [c for c in conn.execute.call_args_list
                  if "INSERT INTO raw_series" in str(c[0][0])]
        assert len(insert) == 1
        assert insert[0][0][1]["obs_date"] == "2026-03-13"


class TestWeekendReadDoesNotCorruptRawSeries:
    """Criterion 5, end to end through a GET handler.

    /watchlist/{t}/quote falls through to the live quote when nothing is
    stored, and used to cache it under ``_today()``. Read on a Sunday
    that wrote Friday's close as a Sunday observation.
    """

    @patch("api.routers.watchlist_overview._init_table")
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_no_raw_series_row_for_a_non_trading_day(self, mock_engine, _init):
        read_conn = MagicMock()
        read_conn.execute.return_value.fetchall.return_value = []
        read_conn.execute.return_value.fetchone.return_value = None
        write_conn = MagicMock()
        write_conn.execute.return_value.fetchone.return_value = (7,)

        mock_engine.return_value.connect.return_value.__enter__ = MagicMock(
            return_value=read_conn
        )
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(
            return_value=False
        )
        mock_engine.return_value.begin.return_value.__enter__ = MagicMock(
            return_value=write_conn
        )
        mock_engine.return_value.begin.return_value.__exit__ = MagicMock(
            return_value=False
        )

        friday = "2026-03-13"
        with patch(
            "api.routers.watchlist_overview._fetch_live_price",
            return_value={"price": 182.0, "prev_close": 180.0, "pct_1d": 0.011,
                          "bar_date": friday, "source": "live"},
        ):
            response = client.get("/api/v1/watchlist/SPY/quote", headers=_auth_header())

        assert response.status_code == 200
        assert response.json()["as_of"] == friday

        inserts = [c for c in write_conn.execute.call_args_list
                   if "INSERT INTO raw_series" in str(c[0][0])]
        assert len(inserts) == 1
        obs_date = inserts[0][0][1]["obs_date"]
        assert obs_date == friday
        assert obs_date != _today().isoformat()

    @patch("api.routers.watchlist_overview._init_table")
    @patch("api.routers.watchlist_overview.get_db_engine")
    def test_undated_quote_writes_nothing(self, mock_engine, _init):
        read_conn = MagicMock()
        read_conn.execute.return_value.fetchall.return_value = []
        read_conn.execute.return_value.fetchone.return_value = None
        write_conn = MagicMock()
        write_conn.execute.return_value.fetchone.return_value = (7,)
        mock_engine.return_value.connect.return_value.__enter__ = MagicMock(
            return_value=read_conn
        )
        mock_engine.return_value.connect.return_value.__exit__ = MagicMock(
            return_value=False
        )
        mock_engine.return_value.begin.return_value.__enter__ = MagicMock(
            return_value=write_conn
        )
        mock_engine.return_value.begin.return_value.__exit__ = MagicMock(
            return_value=False
        )

        with patch(
            "api.routers.watchlist_overview._fetch_live_price",
            return_value={"price": 182.0, "prev_close": 180.0, "pct_1d": 0.011,
                          "bar_date": None, "source": "live"},
        ):
            response = client.get("/api/v1/watchlist/SPY/quote", headers=_auth_header())

        assert response.status_code == 200
        assert response.json()["as_of"] is None
        assert not [c for c in write_conn.execute.call_args_list
                    if "INSERT INTO raw_series" in str(c[0][0])]


# ── 5. Valuation freshness survives the valid_fields filter (C-M17) ───────


def _valuation_engine(statement_date: date, price_date: date):
    """IntrinsicValueEngine over a DB where statements and price differ in age."""
    from valuation.intrinsic import IntrinsicValueEngine

    def _execute(_sql, params=None):
        sid = (params or {}).get("sid", "")
        result = MagicMock()
        if sid.endswith(("price", "market_cap")):
            result.fetchone.return_value = (100.0, price_date)
        elif sid.startswith("fmp:"):
            result.fetchone.return_value = (1000.0, statement_date)
        else:
            result.fetchone.return_value = None
        return result

    conn = MagicMock()
    conn.execute.side_effect = _execute
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return IntrinsicValueEngine(engine)


class TestValuationFreshness:
    def test_stale_statements_survive_the_valid_fields_filter(self):
        as_of = date(2026, 3, 10)
        statement_date = date(2025, 6, 30)
        inputs = _valuation_engine(statement_date, as_of).gather_inputs("RXT", as_of)

        assert inputs is not None
        assert inputs.data_freshness == "STALE"
        assert inputs.statements_as_of == statement_date

    def test_result_carries_freshness_and_statement_date(self):
        as_of = date(2026, 3, 10)
        statement_date = date(2025, 6, 30)
        engine = _valuation_engine(statement_date, as_of)
        result = engine.valuate("RXT", as_of)

        assert result is not None
        assert result.data_freshness == "STALE"
        assert result.statements_as_of == statement_date
        payload = result.to_dict()
        assert payload["data_freshness"] == "STALE"
        assert payload["statements_as_of"] == statement_date

    def test_valuation_date_is_the_newest_input_not_today(self):
        """Nothing recent on file -> the valuation is dated when the data is."""
        as_of = _today()
        old = as_of - timedelta(days=400)
        result = _valuation_engine(old, old).valuate("RXT", as_of)

        assert result is not None
        assert result.valuation_date == old
        assert result.valuation_date != as_of
        assert result.data_freshness == "STALE"

    def test_fresh_price_dates_the_valuation_but_statements_stay_honest(self):
        as_of = _today()
        statement_date = as_of - timedelta(days=400)
        result = _valuation_engine(statement_date, as_of).valuate("RXT", as_of)

        assert result is not None
        assert result.valuation_date == as_of
        assert result.statements_as_of == statement_date
        assert result.data_freshness == "STALE"


# ── 6. Unmapped prediction targets are not scored against SPY (D-H13) ────


class TestAstroGridScoring:
    def test_unmapped_targets_are_not_priced_against_spy(self):
        from store.astrogrid import AstroGridStore

        store = AstroGridStore(MagicMock())
        store._get_symbol_price_at_date = MagicMock(
            side_effect=AssertionError("no price lookup may happen for unmapped targets")
        )

        score = store._build_prediction_score(
            conn=MagicMock(),
            prediction_id="pred-1",
            call="Rare earth miners rip higher",
            setup="mystical",
            invalidation="",
            market_overlay={},
            mystical_payload={},
            grid_payload={},
            target_symbols=["MP", "LYNAS"],  # outside the scoreable universe
            start_date=date(2026, 2, 1),
            evaluation_date=date(2026, 3, 10),
        )

        assert score is None

    def test_source_has_no_spy_substitution(self):
        src = Path(__file__).resolve().parents[1] / "store" / "astrogrid.py"
        assert 'or ["SPY"]' not in src.read_text(encoding="utf-8")


# ── 7. change_20d_pct is absent, not a 3-day change (D-M3) ───────────────


class TestScorecardHistoryBaseline:
    def test_baseline_is_none_when_nothing_predates_the_cutoff(self):
        from api.routers.astrogrid_helpers import _find_history_baseline

        today = _today()
        # Oldest first, exactly three sessions on record.
        history = [
            (today - timedelta(days=3), 100.0),
            (today - timedelta(days=2), 101.0),
            (today - timedelta(days=1), 102.0),
        ]

        assert _find_history_baseline(history, today, 1) == pytest.approx(102.0)
        assert _find_history_baseline(history, today, 20) is None

    def test_three_days_of_history_yields_null_change_20d(self):
        from api.routers.astrogrid_helpers import _build_scorecard_item

        today = _today()
        history = [
            (today - timedelta(days=3), 100.0),
            (today - timedelta(days=2), 101.0),
            (today - timedelta(days=1), 102.0),
        ]

        item = _build_scorecard_item(
            {"symbol": "SPY", "label": "S&P 500", "group": "macro",
             "asset_class": "equity", "lookup_ticker": "SPY"},
            "sp500_close",
            ["sp500_close"],
            history,
            None,
        )

        assert item["change_1d_pct"] is not None
        assert item["change_20d_pct"] is None

    def test_mirror_matches(self):
        """astrogrid_api/ is a near-duplicate; the fix must land in both."""
        root = Path(__file__).resolve().parents[1]
        for rel in ("api/routers/astrogrid_helpers.py",
                    "astrogrid_api/astrogrid_helpers.py"):
            src = (root / rel).read_text(encoding="utf-8")
            assert "return history[0][1] if history else None" not in src, rel


# ── 8. Dad chart metrics name their own window (B-H10) ───────────────────


def _dad_engine(price_rows):
    conn = MagicMock()
    price_result = MagicMock()
    price_result.fetchall.return_value = price_rows
    feature_result = MagicMock()
    feature_result.fetchall.return_value = []
    conn.execute.side_effect = [price_result, feature_result]
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return engine


class TestDadChartWindowMetrics:
    def test_one_month_window_is_not_labelled_52w_or_1y(self):
        from api.routers.dad import _grid_market_context

        today = _today()
        rows = [(today - timedelta(days=n), 100.0 + n) for n in range(30, -1, -1)]
        grid = _grid_market_context(_dad_engine(rows), "SPY", days=31)

        metrics = grid["metrics"]
        assert metrics["window_days"] == 31
        assert metrics["window_label"] == "1M"
        for key in ("high_window", "low_window", "return_window_pct",
                    "pct_from_window_high"):
            assert key in metrics
        assert not [k for k in metrics if "52w" in k or "1y" in k]

    def test_decision_stack_copy_names_the_measured_window(self):
        from api.routers.dad import _gold_from_summary, _grid_decision_stack

        grid = {
            "metrics": {"return_window_pct": -45.0, "pct_from_window_high": -40.0,
                        "window_days": 31, "window_label": "1M"},
            "source_freshness": [],
        }
        decision = _grid_decision_stack(
            None, _gold_from_summary(None), grid,
            {"status": "unavailable", "fields": {}}, None,
            {"signal_sources": [], "tradingview_signals": [], "regime": None},
        )

        text = " ".join(decision.get("blockers", []))
        assert "1M" in text
        assert "52-week" not in text
        assert "1Y" not in text
