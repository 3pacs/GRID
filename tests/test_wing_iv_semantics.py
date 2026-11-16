"""Synthetic method/lineage regressions; no provider or research data."""
import json
import socket
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from store.options_iv import WING_METHOD, fixed_moneyness_wings, wing_comparison_key
from tests.test_options_capture_batch import _Yahoo, SESSION_NOW, puller as _capture_fixture

capture_puller = _capture_fixture


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError("network forbidden in synthetic IV tests")
    monkeypatch.setattr(socket.socket, "connect", denied)
    monkeypatch.setattr(socket.socket, "connect_ex", denied)


def wings(calls=(), puts=(), spot=100, **kwargs):
    return fixed_moneyness_wings(
        calls, puts, spot, expiry="2026-12-16", snap_date="2026-11-16",
        source="cboe", capture_batch_id="batch-1", **kwargs,
    )


def test_fixed_band_mean_is_not_delta_or_nearest_otm():
    result = wings(
        calls=[{"strike": 110, "iv": .2}, {"strike": 113, "iv": .4},
               {"strike": 105, "iv": .9}],
        puts=[{"strike": 87, "iv": .4}, {"strike": 90, "iv": .6},
              {"strike": 95, "iv": .1}],
    )
    assert result["iv_m90_put_v1"] == pytest.approx(.5)
    assert result["iv_m110_call_v1"] == pytest.approx(.3)
    meta = result["wing_iv_metadata"]
    assert meta["method"] == WING_METHOD
    assert meta["dte"] == 30
    assert meta["expiry"] == "2026-12-16"
    assert meta["put"]["strikes"] == [87.0, 90.0]
    assert meta["call"]["count"] == 2


@pytest.mark.parametrize("bad", [None, 0, -1, float("nan"), float("inf"), True, "bad"])
def test_invalid_iv_is_missing(bad):
    result = wings(calls=[{"strike": 110, "iv": bad}])
    assert result["iv_m110_call_v1"] is None
    assert result["iv_m90_put_v1"] is None


@pytest.mark.parametrize("spot", [None, 0, -1, float("nan"), float("inf"), True])
def test_invalid_spot_cannot_select_wings(spot):
    assert wings(calls=[{"strike": 110, "iv": .2}], spot=spot)["iv_m110_call_v1"] is None


def test_sparse_chain_does_not_substitute_atm_or_far_strike():
    result = wings(calls=[{"strike": 100, "iv": .2}, {"strike": 150, "iv": .8}])
    assert result["iv_m110_call_v1"] is None
    assert result["wing_iv_metadata"]["call"]["count"] == 0


def test_only_known_same_method_source_and_tenor_are_comparable():
    meta = wings()["wing_iv_metadata"]
    assert wing_comparison_key(meta) == (WING_METHOD, "cboe", 30)
    assert wing_comparison_key(None) is None
    for change in [{"method": "unknown"}, {"source": None}, {"dte": None},
                   {"dte": True}, {"expiry": "bad"}, {"spot": None},
                   {"units": "percent"}, {"spot_basis": "oi_strike"}]:
        assert wing_comparison_key({**meta, **change}) is None
    assert wing_comparison_key({**meta, "source": "yahoo"}) != wing_comparison_key(meta)
    assert wing_comparison_key({**meta, "expiry": "2026-12-17", "dte": 31}) != wing_comparison_key(meta)


def test_dates_accept_native_date_and_reject_nonpositive_tenor():
    result = fixed_moneyness_wings([], [], 100, expiry=date(2026, 11, 16),
                                  snap_date=date(2026, 11, 16), source="cboe")
    assert wing_comparison_key(result["wing_iv_metadata"]) is None


def test_actual_producer_scopes_tenor_preserves_legacy_and_versions_publication(capture_puller, monkeypatch):
    expirations = [int((SESSION_NOW + timedelta(days=d)).timestamp()) for d in (10, 30)]
    class Provider(_Yahoo):
        def get_options(self, ticker, expiry=None):
            data = super().get_options(ticker, expiry)
            data["calls"] = [dict(row) for row in data["calls"]]
            data["puts"] = [dict(row) for row in data["puts"]]
            far = expiry == expirations[1]
            for row in data["calls"]:
                row["impliedVolatility"] = .8 if far else .2
            for row in data["puts"]:
                row["impliedVolatility"] = .9 if far else .4
            return data
    capture_puller._yahoo = Provider(expirations, [90, 100, 110])
    published = []
    monkeypatch.setattr(capture_puller, "_push_to_resolved", lambda *a, **kw: published.append((a, kw)) or 0)
    result = capture_puller._pull_ticker("SPY", SESSION_NOW.date().isoformat())
    assert result["status"] == "SUCCESS"
    sql, values = next((s, p) for s, p in capture_puller.engine.calls if "INSERT INTO options_daily_signals" in s)
    assert values["iv_m90_put_v1"] == .4
    assert values["iv_m110_call_v1"] == .2
    assert "iv_25d_put" not in sql and "iv_25d_call" not in sql
    meta = json.loads(values["wing_meta"])
    assert meta["dte"] == 10 and meta["source"] == "yahoo"
    assert meta["capture_batch_id"] == result["capture_batch_id"]
    signals = published[0][0][3]
    assert "iv_25d_put" not in signals and "iv_m90_put_v1" in signals
    assert published[0][1]["wing_metadata"] == meta


def test_actual_producer_never_uses_far_tenor_to_fill_empty_front(capture_puller):
    expirations = [int((SESSION_NOW+timedelta(days=d)).timestamp()) for d in (10, 30)]
    class Provider(_Yahoo):
        def get_options(self, ticker, expiry=None):
            data = super().get_options(ticker, expiry)
            if expiry is None:
                data["calls"] = []
                data["puts"] = []
            return data
    capture_puller._yahoo = Provider(expirations, [90, 100, 110])
    assert capture_puller._pull_ticker("SPY", SESSION_NOW.date().isoformat())["status"] == "SUCCESS"
    values = next(p for s, p in capture_puller.engine.calls if "INSERT INTO options_daily_signals" in s)
    assert values["iv_m90_put_v1"] is None and values["iv_m110_call_v1"] is None
    assert json.loads(values["wing_meta"])["dte"] == 10


def test_resolved_value_keeps_its_own_lineage_and_is_not_model_admitted():
    from ingestion.options import OptionsPuller
    puller = OptionsPuller.__new__(OptionsPuller)
    puller.source_id = 7
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (42,)
    conn.execute.return_value.rowcount = 1
    meta = wings()["wing_iv_metadata"]
    assert puller._push_to_resolved(conn, "SPY", "2026-11-16", {
        "iv_m90_put_v1": ("vol", "Put IV proxy 87-93% spot v1", .4),
    }, wing_metadata=meta) == 1
    registry = conn.execute.call_args_list[0].args[1]
    assert registry["missing_policy"] == "NAN"
    assert registry["model_eligible"] is False
    resolved = conn.execute.call_args_list[-1].args[1]
    assert json.loads(resolved["wing_meta"]) == meta


def history_row(day, put=.4, call=.2, source="cboe", dte=30, method=WING_METHOD):
    meta = fixed_moneyness_wings([], [], 100, expiry=day+timedelta(days=dte),
                                 snap_date=day, source=source)["wing_iv_metadata"]
    meta["method"] = method
    return (day, .2, put, call, .01, meta)


def percentile(rows):
    from analysis.vol_surface import VolSurfaceEngine
    db = MagicMock()
    db.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = rows
    return VolSurfaceEngine(db).historical_percentile("SPY", as_of_date=date(2026, 11, 16))


def test_percentile_excludes_mixed_unknown_methods_sources_and_tenors():
    day = date(2026, 11, 16)
    rows = [history_row(day-timedelta(days=6), put=.3),
            history_row(day-timedelta(days=5), put=.5),
            history_row(day-timedelta(days=4), put=4, method="legacy"),
            history_row(day-timedelta(days=3), put=4, source="yahoo"),
            history_row(day-timedelta(days=2), put=4, dte=10),
            (day-timedelta(days=1), .2, 4, .2, .01, None),
            history_row(day)]
    result = percentile(rows)
    assert result["skew_percentile"] == 50
    assert result["skew_history_count"] == 2
    assert result["skew_history_excluded"] == 4
    assert result["skew_method"] == WING_METHOD


@pytest.mark.parametrize("rows", [[], [history_row(date(2026, 11, 16))],
    [history_row(date(2026, 11, 15)), history_row(date(2026, 11, 16))],
    [history_row(date(2026, 11, 15)), (date(2026, 11, 16), .2, .4, .2, .01, None)]])
def test_percentile_never_uses_current_as_history_or_infers_legacy(rows):
    assert percentile(rows)["skew_percentile"] is None


def test_percentile_excludes_missing_nonfinite_and_mismatched_metadata():
    day = date(2026, 11, 16)
    rows = [history_row(day-timedelta(days=4)),
            history_row(day-timedelta(days=3), put=float("inf")),
            history_row(day-timedelta(days=2), call=None),
            history_row(day-timedelta(days=1)), history_row(day)]
    rows[3][-1]["snap_date"] = "2026-10-01"
    result = percentile(rows)
    assert result["skew_history_count"] == 1
    assert result["skew_percentile"] is None


def api_engine(batch=None):
    day = date(2026, 11, 16)
    db = MagicMock()
    calls = []
    def execute(statement, params):
        sql = str(statement)
        calls.append((sql, params))
        result = MagicMock()
        if "MAX(snap_date)" in sql:
            result.fetchone.return_value = (day,)
        elif "FROM options_capture_batches" in sql:
            result.fetchone.return_value = batch
        else:
            assert "capture_batch_id = :batch" in sql and params["batch"] == "batch-1"
            result.fetchall.return_value = [
                (110, day+timedelta(days=30), 30, .2, "call"),
                (90, day+timedelta(days=30), 30, .4, "put"),
                (100, day+timedelta(days=30), 30, .3, "call"),
                (110, day+timedelta(days=60), 60, .6, "call"),
                (90, day+timedelta(days=60), 60, .8, "put"),
            ]
        return result
    db.connect.return_value.__enter__.return_value.execute.side_effect = execute
    return db, calls


def test_api_uses_capture_spot_not_oi_strike_and_preserves_each_tenor(monkeypatch):
    from api.routers import derivatives
    from fastapi.encoders import jsonable_encoder
    from starlette.responses import JSONResponse
    batch = ("batch-1", 100, "cboe", datetime(2026, 11, 16, 15, tzinfo=timezone.utc), False)
    db, calls = api_engine(batch)
    monkeypatch.setattr(derivatives, "get_db_engine", lambda: db)
    response = JSONResponse(jsonable_encoder(derivatives.get_term_structure("spy")))
    assert response.status_code == 200
    data = json.loads(response.body)
    assert data["spot"] == 100
    near, far = data["term_structure"]
    assert near["iv_m90_put_v1"] == .4 and far["iv_m90_put_v1"] == .8
    assert near["iv_25d_put"] is None and near["iv_25d_call"] is None
    assert far["wing_iv_metadata"]["dte"] == 60
    assert near["wing_iv_metadata"]["capture_batch_id"] == "batch-1"
    assert all(sql.lstrip().startswith("SELECT") for sql, _ in calls)


@pytest.mark.parametrize("batch", [None, ("b", 0, "cboe", None, False),
                                   ("b", 100, "cboe", None, True)])
def test_api_without_observed_capture_spot_never_guesses(monkeypatch, batch):
    from api.routers import derivatives
    db, calls = api_engine(batch)
    monkeypatch.setattr(derivatives, "get_db_engine", lambda: db)
    assert derivatives.get_term_structure("spy")["term_structure"] == []
    assert len(calls) == 2


def test_legacy_api_values_are_explicitly_unknown():
    from store.options_iv import public_wing_fields
    result = public_wing_fields(.4, .2, None, .9, .8)
    assert result["iv_m90_put_v1"] is None
    assert result["iv_25d_put"] is None
    assert result["legacy_wing_iv"] == {"method": "unknown_legacy", "iv_25d_put": .9, "iv_25d_call": .8}


def test_bulk_path_uses_same_proxy_without_atm_fill_or_legacy_series(monkeypatch):
    from scripts import bulk_historical_pull as bulk
    import yfinance
    monkeypatch.setattr(bulk, "OPTIONS_TICKERS", ["SPY"])
    monkeypatch.setattr(bulk, "_ensure_source", lambda *a: 7)
    monkeypatch.setattr(bulk.time, "sleep", lambda *a: None)
    day = date.today()
    chain = MagicMock()
    chain.calls = pd.DataFrame({"strike": [100, 110], "impliedVolatility": [.3, .2], "openInterest": [10, 10], "volume": [1, 1]})
    chain.puts = pd.DataFrame({"strike": [100], "impliedVolatility": [.3], "openInterest": [10], "volume": [1]})
    ticker = MagicMock(options=[str(day+timedelta(days=30))], info={"regularMarketPrice": 100})
    ticker.option_chain.return_value = chain
    monkeypatch.setattr(yfinance, "Ticker", lambda *a: ticker)
    db = MagicMock()
    conn = db.begin.return_value.__enter__.return_value
    conn.execute.return_value.fetchone.return_value = None
    assert bulk.pull_options_history(db)[0]["status"] == "OK"
    writes = [c.args[1] for c in conn.execute.call_args_list if "INSERT INTO raw_series" in str(c.args[0])]
    by_id = {p["sid"]: p for p in writes}
    assert "OPT:SPY:iv_m90_put_v1" not in by_id  # no put band, never ATM
    assert by_id["OPT:SPY:iv_m110_call_v1"]["val"] == .2
    assert json.loads(by_id["OPT:SPY:iv_m110_call_v1"]["payload"])["source"] == "yahoo"
    assert not any("iv_25d" in p["sid"] or p["sid"] == "OPT:SPY:iv_skew" for p in writes)


def test_delta_strike_proxy_moves_with_tenor_and_iv_while_fixed_bands_do_not():
    from analysis.vol_surface import VolSurfaceEngine
    engine = VolSurfaceEngine(MagicMock())
    strikes = [engine._delta_to_strike(.25, 100, iv, dte/365) for iv, dte in [(.2, 7), (.2, 90), (.8, 90)]]
    assert strikes[0] < strikes[1] < strikes[2]
    assert all(abs(k-110) > 1 for k in strikes)
    assert engine._interpolate_iv(pd.DataFrame({"strike": [100, 110], "iv": [.2, .3]}), 120) is None


def test_surface_proxy_discloses_model_and_never_falls_back_to_fixed_wings():
    from analysis.vol_surface import VolSurfaceEngine
    engine = VolSurfaceEngine(MagicMock())
    day = date(2026, 11, 16)
    def chain(strikes, iv):
        return pd.DataFrame([{"strike": s, "iv": iv, "opt_type": side,
                              "expiry": day+timedelta(days=30), "dte": 30}
                             for s in strikes for side in ("call", "put")])
    engine._get_options_data = lambda *a: chain([80, 90, 100, 110, 120], .2)
    engine._get_spot_price = lambda *a: 100
    row = engine.compute_skew("SPY", day)[0]
    assert row["method"] == "bs_atm_iv_delta_strike_proxy_v1"
    assert row["put_25d_iv"] is None and row["call_25d_iv"] is None
    # Targets fall outside [90,110] at high IV; both fixed wings exist.
    engine._get_options_data = lambda *a: chain([90, 100, 110], 2.0)
    assert engine.compute_skew("SPY", day) == []


def test_both_watchlist_readers_expose_proxy_and_quarantine_legacy_labels(monkeypatch):
    from api.routers import watchlist_analysis as route, watchlist_helpers as helpers
    meta = wings()["wing_iv_metadata"]
    row = (date(2026, 11, 16), .5, 100, 1.1, 1000, 50, 100, .3,
           .9, .8, .01, .1, .4, .2, meta)
    db = MagicMock()
    calls = []
    def execute(statement, params=None):
        sql = str(statement)
        calls.append(sql)
        result = MagicMock()
        result.fetchone.return_value = None
        result.fetchall.return_value = [row] if "FROM options_daily_signals" in sql else []
        if "SELECT rs.obs_date, rs.value " in sql:
            result.fetchall.return_value = [(date(2026, 11, 16), 100)]
        return result
    db.connect.return_value.__enter__.return_value.execute.side_effect = execute
    captured = []
    for module in (route, helpers):
        monkeypatch.setattr(module, "get_db_engine", lambda: db)
        monkeypatch.setattr(module, "_resolve_feature_names", lambda *a: ["spy_close"])
        monkeypatch.setattr(module, "_set_analysis_cache", lambda *a: captured.append(a[-1]))
    monkeypatch.setattr(route, "_get_analysis_cached", lambda *a: None)
    direct = route.get_ticker_analysis("SPY", "3M", stored_request=None)
    assert helpers._preload_one("SPY") == "SPY"
    for data in (direct, captured[-1]):
        option = data["options"][0]
        assert option["iv_25d_put"] is None
        assert option["iv_m90_put_v1"] == .4
        assert option["legacy_wing_iv"]["iv_25d_put"] == .9
    assert all(sql.lstrip().startswith("SELECT") for sql in calls)
    assert "Legacy" in helpers._get_display_name("spy_iv_25d_put")


def test_loaded_legacy_wings_do_not_change_scanner_scores():
    from tests.test_options_scanner import _scanner_with_row, _signal_row
    first = _scanner_with_row(_signal_row(iv_25d_put=.9, iv_25d_call=.1))
    second = _scanner_with_row(_signal_row(iv_25d_put=None, iv_25d_call=None))
    # The persisted-wing columns are loaded for compatibility but the scanner
    # scores iv_skew (OTM-put/ATM-put ratio), not put-minus-call wing history.
    day = date(2026, 10, 15)
    left = first._scan_ticker("SPY", day)
    right = second._scan_ticker("SPY", day)
    assert len(left) == len(right) == 1
    assert left[0].score == right[0].score
    assert left[0].signals == right[0].signals
