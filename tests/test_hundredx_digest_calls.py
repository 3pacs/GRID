"""Call specificity for the 100x digest (``alerts/hundredx_digest.py``).

Pins what one emailed call must state and how its numbers are derived:

* ``_pick_contract`` names one contract: strike nearest 2% OTM inside the
  band, two-sided quote, spread no wider than ``MAX_SPREAD_PCT`` of mid.
* ``_verify_opportunity`` only looks at expiries >= ``MIN_DTE_DAYS`` and walks
  to the next expiry when the first has no executable contract.
* ``_call_economics`` derives breakeven, max loss, required move, IV-implied
  odds and the invalidation level — and leaves them None when an input is
  missing.
* the card shows values (not labels) with their source and time.
* ``_persist_calls`` writes parameterized rows to the insert-only ledger.

No network, database, SMTP or yfinance call happens: every collaborator is
an in-test fake.
"""

from __future__ import annotations

import math
import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from alerts import hundredx_digest as hx

TODAY = date(2026, 11, 3)


def _side(rows):
    return pd.DataFrame(rows, columns=["strike", "bid", "ask", "impliedVolatility",
                                       "openInterest", "volume", "lastTradeDate"])


# ── _pick_contract ──────────────────────────────────────────────────────────

def test_pick_put_nearest_two_percent_otm():
    side = _side([(90, 1.0, 1.1, .5, 10, 1, None), (98, 2.0, 2.2, .5, 10, 1, None),
                  (100, 3.0, 3.2, .5, 10, 1, None)])
    k = hx._pick_contract(side, 100.0, "PUT")
    assert k["strike"] == 98 and k["bid"] == 2.0 and k["ask"] == 2.2


def test_pick_call_nearest_two_percent_otm_inside_band():
    side = _side([(97, 5.0, 5.2, .5, 10, 1, None),     # below band (0.98)
                  (103, 1.5, 1.6, .5, 10, 1, None), (111, .2, .25, .5, 10, 1, None)])
    assert hx._pick_contract(side, 100.0, "CALL")["strike"] == 103


def test_pick_skips_one_sided_and_wide_quotes():
    side = _side([(98, 0.0, 2.2, .5, 900, 1, None),     # no bid
                  (97, 0.30, 2.40, .5, 900, 1, None),   # spread 156% of mid
                  (95, 1.00, 1.30, .5, 5, 1, None)])    # spread 26% -> the pick
    assert hx._pick_contract(side, 100.0, "PUT")["strike"] == 95


def test_pick_returns_none_when_nothing_is_executable():
    side = _side([(98, 0.0, 0.0, .5, 10, 1, None), (99, .25, 2.4, .5, 10, 1, None)])
    assert hx._pick_contract(side, 100.0, "PUT") is None
    assert hx._pick_contract(_side([]), 100.0, "PUT") is None


def test_pick_breaks_ties_by_open_interest():
    side = _side([(97, 2.0, 2.2, .5, 10, 1, None), (99, 2.0, 2.2, .5, 500, 1, None)])
    assert hx._pick_contract(side, 100.0, "PUT")["strike"] == 99


# ── _verify_opportunity against a fake yfinance ─────────────────────────────

class _Chain:
    def __init__(self, calls, puts):
        self.calls, self.puts = calls, puts


def _fake_yf(spot, chains: dict[str, pd.DataFrame], info_field="regularMarketPrice"):
    requested: list[str] = []

    class Ticker:
        def __init__(self, t):
            self.info = {info_field: spot}
            self.options = tuple(chains)

        def option_chain(self, e):
            requested.append(e)
            return _Chain(chains[e], chains[e])

    return types.SimpleNamespace(Ticker=Ticker), requested


@pytest.fixture
def _quiet(monkeypatch):
    monkeypatch.setattr(hx, "_update_spot_in_db", lambda *a: None)
    monkeypatch.setattr(hx, "_store_bonus_data", lambda *a: None)
    monkeypatch.setattr(hx.time, "sleep", lambda *_: None)


def _exp(days: int) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def test_verify_never_names_a_contract_under_min_dte(monkeypatch, _quiet):
    near = _side([(98, 1.0, 1.1, .5, 9000, 1, None)])
    yf, requested = _fake_yf(100.0, {_exp(7): near, _exp(14): near})
    monkeypatch.setitem(sys.modules, "yfinance", yf)

    v = hx._verify_opportunity("X", {"direction": "PUT", "spot_price": 100.0, "signals": {}}, None)

    assert v["contract"] is None
    assert any(f">= {hx.MIN_DTE_DAYS} days" in i for i in v["issues"])
    assert requested == []


def test_verify_walks_to_next_expiry_when_first_is_unquoted(monkeypatch, _quiet):
    wide = _side([(98, .25, 2.4, .5, 300, 1, None)])
    good = _side([(98, 2.0, 2.2, .45, 50, 3, "2026-11-03 20:58:00+00:00")])
    yf, requested = _fake_yf(100.0, {_exp(10): good, _exp(30): wide, _exp(58): good})
    monkeypatch.setitem(sys.modules, "yfinance", yf)

    v = hx._verify_opportunity("X", {"direction": "PUT", "spot_price": 100.0, "signals": {}}, None)

    assert requested == [_exp(30), _exp(58)]
    k = v["contract"]
    assert k["expiry"] == _exp(58) and k["strike"] == 98
    assert isinstance(k["quote_at"], datetime) and k["quote_source"] == "yfinance option_chain"
    assert v["spot_source"] == "yfinance info.regularMarketPrice"


def test_verify_labels_previous_close_spot(monkeypatch, _quiet):
    good = _side([(98, 2.0, 2.2, .45, 50, 3, None)])
    yf, _ = _fake_yf(100.0, {_exp(30): good}, info_field="previousClose")
    monkeypatch.setitem(sys.modules, "yfinance", yf)

    v = hx._verify_opportunity("X", {"direction": "PUT", "spot_price": 100.0, "signals": {}}, None)

    assert v["spot_source"] == "yfinance info.previousClose"


def test_verify_does_not_compare_aggregate_oi_with_single_expiry_volume(monkeypatch, _quiet):
    """A different measure and an unknown expiry set cannot prove divergence."""
    calls = _side([(103, 1.0, 1.1, .4, 50, 100, None)])
    puts = _side([(98, 2.0, 2.2, .4, 180, 100, None)])

    class Ticker:
        def __init__(self, t):
            self.info = {"regularMarketPrice": 100.0}
            self.options = (_exp(30),)

        def option_chain(self, e):
            return _Chain(calls, puts)

    monkeypatch.setitem(sys.modules, "yfinance", types.SimpleNamespace(Ticker=Ticker))
    v = hx._verify_opportunity("X", {"direction": "PUT", "spot_price": 100.0,
                                     "signals": {"pcr": {"value": 3.6}}}, None)

    assert not any("PCR diverged" in i for i in v["issues"])
    assert v["verified"]
    assert v["pcr_comparison"]["status"] == "unavailable"
    assert "exact captured-expiry coverage" in v["pcr_comparison"]["reason"]
    assert v["live_chain"]["scope"] == "single_expiry"
    assert v["live_chain"]["expiry"] == _exp(30)
    assert v["live_chain"]["volume_put_call_ratio"] == 1.0
    assert v["live_chain"]["open_interest_put_call_ratio"] == 3.6
    assert "pcr" not in v["live_chain"]  # no ambiguous measure


@pytest.mark.parametrize("calls,puts,expected", [
    ([10], [0], 0.0),
    ([0], [10], None),
    ([0], [0], None),
    ([10], [None], None),
    ([10, None], [20], None),
    ([10], [float("nan")], None),
    ([float("inf")], [20], None),
    ([-1], [20], None),
    ([1.5], [20], None),
    ([10], [], 0.0),  # an explicitly empty side with a count column is zero
])
def test_chain_ratios_preserve_zero_denominators_and_missing_counts(calls, puts, expected):
    def frame(values):
        return pd.DataFrame({"volume": values, "openInterest": values})

    data = hx._chain_measures(_Chain(frame(calls), frame(puts)), "2026-12-18",
                              datetime(2026, 11, 5, tzinfo=timezone.utc))
    assert data["volume_put_call_ratio"] == expected
    assert data["open_interest_put_call_ratio"] == expected


def test_chain_missing_columns_are_unavailable_not_zero():
    chain = _Chain(pd.DataFrame({"volume": [10]}), pd.DataFrame({"openInterest": [20]}))
    data = hx._chain_measures(chain, "2026-12-18", datetime.now(timezone.utc))
    assert data["total_call_volume"] == 10
    assert data["total_put_volume"] is None
    assert data["total_call_open_interest"] is None
    assert data["total_put_open_interest"] == 20
    assert data["volume_put_call_ratio"] is None
    assert data["open_interest_put_call_ratio"] is None


def test_thin_contract_still_qualifies_without_claiming_a_fill():
    side = _side([(98, 1.90, 3.10, .5, 2, 0, "2026-10-30 20:00:00+00:00")])
    contract = hx._pick_contract(side, 100, "PUT")
    assert contract["oi"] == 2 and contract["volume"] == 0
    assert (contract["ask"] - contract["bid"]) / 2.5 == pytest.approx(.48)
    summary = hx._plain(hx._format_summary_section([{}], [{}])["body"])
    assert "retrieved two-sided quote" in summary
    assert "60%-of-mid spread limit" in summary
    assert "do not establish available size or a fill" in summary
    assert "executable" not in summary


def test_missing_contract_counts_are_displayed_as_missing():
    contract = hx._pick_contract(_side([(98, 2, 2.2, .5, None, None, None)]), 100, "PUT")
    assert contract["oi"] is None and contract["volume"] is None


@pytest.mark.parametrize("missing", [("volume",), ("openInterest",), ("volume", "openInterest")])
def test_verification_with_missing_count_columns_keeps_quote_and_marks_ratios_unavailable(
    monkeypatch, _quiet, missing,
):
    side = _side([(98, 2, 2.2, .5, 2, 0, None)]).drop(columns=list(missing))
    yf, _ = _fake_yf(100, {_exp(30): side})
    monkeypatch.setitem(sys.modules, "yfinance", yf)
    v = hx._verify_opportunity("X", {"direction": "PUT", "spot_price": 100, "signals": {}}, None)
    assert v["contract"]["strike"] == 98
    assert v["verified"]
    assert v["pcr_comparison"]["status"] == "unavailable"
    if "volume" in missing:
        assert v["contract"]["volume"] is None
        assert v["live_chain"]["volume_put_call_ratio"] is None
    if "openInterest" in missing:
        assert v["contract"]["oi"] is None
        assert v["live_chain"]["open_interest_put_call_ratio"] is None


def test_retrieved_chain_scope_is_separate_from_selected_contract(monkeypatch, _quiet):
    wide = _side([(98, .25, 2.4, .5, 300, 100, None)])
    thin = _side([(98, 1.9, 3.1, .5, 2, 0, "2026-10-30 20:00:00+00:00")])
    yf, requested = _fake_yf(100, {_exp(30): wide, _exp(58): thin})
    monkeypatch.setitem(sys.modules, "yfinance", yf)
    v = hx._verify_opportunity("X", {"direction": "PUT", "spot_price": 100,
                                     "signals": {"pcr": {"value": 0}}}, None)
    assert requested == [_exp(30), _exp(58)]
    assert v["live_chain"]["expiry"] == _exp(30)
    assert v["contract"]["expiry"] == _exp(58)
    assert v["contract"]["quote_at_kind"] == "local_retrieval_time"
    assert v["contract"]["quote_at"].date() == datetime.now(timezone.utc).date()
    assert "2026-10-30" in v["contract"]["last_trade"]
    card = hx._plain(hx._format_opportunity_card(
        {"ticker": "X", "direction": "PUT", "signals": {"pcr": {"value": 0}}}, v, 1)["body"])
    assert f"single expiry {_exp(30)}" in card
    assert f"BUY X {_exp(58)}" in card
    assert "Stored put/call  open-interest ratio 0.00 across captured expiries" in card
    assert "Put/call comparison  unavailable" in card
    assert "provider bid/ask timestamp unavailable" in card
    assert "local retrieval time" in card
    assert "2 reported OI, 0 reported volume" in card
    assert "traded today" not in card
    assert "executable" not in card


def test_card_labels_ratios_and_missing_comparison_data():
    opp, v = _card_inputs()
    opp["signals"] = {"pcr": {"value": 3.6, "score": 10, "direction": "PUT"}}
    v["live_chain"] = hx._chain_measures(
        _Chain(_side([(103, 1, 1.1, .4, 50, 100, None)]),
               _side([(98, 2, 2.2, .4, 180, 0, None)])),
        "2026-12-18", v["spot_at"])
    card = hx._plain(hx._format_opportunity_card(opp, v, 1)["body"])
    assert "put/call open-interest ratio 3.60 across captured expiries" in card
    assert "volume ratio 0.00; open-interest ratio 3.60; single expiry 2026-12-18" in card
    assert "Put/call comparison  unavailable" in card
    opp["signals"] = {}
    v["live_chain"] = None
    card = hx._plain(hx._format_opportunity_card(opp, v, 1)["body"])
    assert "Stored put/call  open-interest ratio N/A" in card
    assert "Retrieved put/call  N/A (chain data unavailable)" in card
    assert "Put/call comparison  unavailable" in card


# ── _call_economics ─────────────────────────────────────────────────────────

def test_put_economics():
    e = hx._call_economics("PUT", 17.0, 1.35, 2.40, 18.27, 0.78, TODAY + timedelta(days=73), TODAY)
    assert e["breakeven"] == pytest.approx(14.60)
    assert e["max_loss"] == pytest.approx(240.0)
    assert e["required_move"] == pytest.approx(14.60 / 18.27 - 1)
    t = 73 / 365
    d2 = (math.log(18.27 / 14.60) - 0.5 * 0.78 ** 2 * t) / (0.78 * math.sqrt(t))
    assert e["prob_beyond_breakeven"] == pytest.approx(1 - 0.5 * (1 + math.erf(d2 / math.sqrt(2))))
    assert 0.31 < e["prob_beyond_breakeven"] < 0.33
    assert e["invalidation"] == pytest.approx(18.27 * math.exp(0.78 * math.sqrt(5 / 252)))
    assert e["invalidation"] > 18.27


def test_call_economics_invalidation_is_below_spot():
    e = hx._call_economics("CALL", 102.0, 2.0, 2.2, 100.0, 0.30, TODAY + timedelta(days=30), TODAY)
    assert e["breakeven"] == pytest.approx(104.2)
    assert e["invalidation"] < 100.0
    assert 0 < e["prob_beyond_breakeven"] < e["prob_itm"] < 1


@pytest.mark.parametrize("spot,iv", [(None, 0.5), (100.0, None), (100.0, 0.0)])
def test_economics_leave_missing_inputs_missing(spot, iv):
    e = hx._call_economics("PUT", 98.0, 2.0, 2.2, spot, iv, TODAY + timedelta(days=30), TODAY)
    assert e["breakeven"] == pytest.approx(95.8) and e["max_loss"] == pytest.approx(220.0)
    assert e["prob_beyond_breakeven"] is None and e["invalidation"] is None
    if spot is None:
        assert e["required_move"] is None


# ── Signals as values ───────────────────────────────────────────────────────

def test_signal_lines_show_values_and_opposition():
    signals = {
        "pcr": {"score": 10, "direction": "CALL", "value": 3.6062},
        "iv_skew": {"score": 10, "direction": "PUT", "value": 0.5749},
        "max_pain_div": {"score": 8, "direction": "PUT", "value": 25.0},
        "term_structure": {"score": 9, "direction": "PUT", "value": -0.6975},
        "oi_concentration": {"score": 0, "value": 0.11},
    }
    support, against = hx._signal_lines(signals, "PUT", 21.96)
    assert len(support) == 3
    assert support[0].startswith("OTM-put/ATM IV skew 0.57")
    assert "IV term slope -0.698" in support[1]
    assert "max pain $25.00 vs spot $21.96 (+13.8%" in support[2]
    assert against == ["pcr"]


def test_vol_surface_is_labelled_low_information():
    text_ = hx._signal_text("vol_surface", {"value": {"butterfly_violations": 67,
                                                       "calendar_violations": 17,
                                                       "front_skew": 0.0052}}, 18.0)
    assert "67 butterfly / 17 calendar" in text_ and "low information" in text_


# ── Card ────────────────────────────────────────────────────────────────────

def _card_inputs():
    contract = {"strike": 17.0, "bid": 1.35, "ask": 2.40, "iv": 0.78, "oi": 53, "volume": 17,
                "last_trade": "2026-11-03 20:58:11+00:00", "expiry": "2027-01-15",
                "quote_at": datetime(2026, 11, 3, 15, 2, tzinfo=timezone.utc),
                "quote_source": "yfinance option_chain"}
    opp = {"ticker": "OMER", "direction": "PUT", "score": 6.07, "as_of": TODAY,
           "signals": {"iv_skew": {"score": 10, "direction": "PUT", "value": 0.726}},
           "signal_date": date(2026, 11, 3),
           "signal_captured_at": datetime(2026, 11, 3, 15, 0, tzinfo=timezone.utc),
           "catalyst": {"status": "found", "type": "earnings", "date": "2026-11-12",
                        "source": "earnings_calendar", "notes": "FQ 3"},
           "llm_verdict": "SUSPECT", "llm_reason": "IV <b>high</b>"}
    v = {"live_spot": 18.27, "spot_source": "yfinance info.regularMarketPrice",
         "spot_at": datetime(2026, 11, 3, 15, 2, tzinfo=timezone.utc), "contract": contract,
         "issues": []}
    return opp, v


@pytest.mark.parametrize("rendered_day, stale_note", [
    (3, ""),
    (8, "; STALE at rendering (5d old)"),
])
def test_card_states_the_exact_call(monkeypatch, rendered_day, stale_note):
    # Pin main's clock seam while retaining both fresh and stale regressions.
    monkeypatch.setattr(hx, "_utc_today", lambda: date(2026, 11, rendered_day))
    opp, v = _card_inputs()
    card = hx._format_opportunity_card(opp, v, 1)
    plain = hx._plain(card["body"])

    assert card["title"] == "#1 — OMER PUTS (UNPROVEN)"
    assert "BUY OMER 2027-01-15 $17.00 PUT @ $2.40 (ask)" in plain
    assert "bid $1.35 / ask $2.40 / mid $1.88" in plain
    assert "2026-11-03 15:02Z via yfinance option_chain" in plain
    assert "Breakeven at expiry  $14.60 — needs -20.1% from spot in 73 days" in plain
    assert "Max loss  $240 per contract" in plain
    assert "32% of finishing past breakeven" in plain
    assert "exit if OMER trades above $20.39" in plain
    assert "earnings 2026-11-12 (earnings_calendar) FQ 3" in plain
    assert "OTM-put/ATM IV skew 0.73" in plain
    assert ("options_daily_signals row 2026-11-03 (0d old at idea date)"
            f"{stale_note}, captured 2026-11-03 15:00Z") in plain
    assert ("STALE at rendering" in plain) == bool(stale_note)
    assert "SUSPECT: IV <b>high</b>" in plain          # escaped in HTML ...
    assert "<b>high</b>" not in card["body"]            # ... not injected
    # The fictional payoff multiple is gone.
    assert "Payoff" not in card["body"] and "x</div>" not in card["body"]


def test_card_labels_missing_catalyst_and_signal_date():
    opp, v = _card_inputs()
    opp.update(catalyst={"status": "unavailable"}, signal_date=None)
    plain = hx._plain(hx._format_opportunity_card(opp, v, 1)["body"])
    assert "Catalyst before expiry  N/A (catalyst lookup unavailable)" in plain
    assert "Signal data as of  N/A" in plain


# ── Track record section ────────────────────────────────────────────────────

def test_track_record_section_rows_and_headline():
    record = {
        "calls": [
            {"ticker": "OMER", "direction": "PUT", "expiry": date(2026, 10, 16), "strike": 17.0,
             "entry": 1.0, "status": "SETTLED", "return": 1.5,
             "emailed_at": datetime(2026, 9, 16, 15, tzinfo=timezone.utc)},
            {"ticker": "BEAM", "direction": "PUT", "expiry": date(2026, 12, 18), "strike": 27.0,
             "entry": 5.1, "status": "OPEN", "return": -0.42,
             "emailed_at": datetime(2026, 11, 2, 15, tzinfo=timezone.utc)},
        ],
        "settled": 1, "settled_wins": 1, "settled_hit_rate": 1.0, "settled_median_return": 1.5,
        "open": 1, "open_median_return": -0.42, "unpriced": 0,
    }
    plain = hx._plain(hx._format_track_record(record)["body"])
    assert "Last 2 calls: 1 settled, 1 above entry (hit rate 100%, median +150%); 1 open" in plain
    assert "2026-09-16  OMER 2026-10-16 $17.00 PUT  $1.00  SETTLED  +150%" in plain
    assert "BEAM 2026-12-18 $27.00 PUT  $5.10  OPEN  -42%" in plain


def test_track_record_empty_and_error_states():
    assert "starts with this digest" in hx._format_track_record({"calls": []})["body"]
    assert "unavailable: boom" in hx._format_track_record(None, "boom")["body"]


# ── Ledger write ────────────────────────────────────────────────────────────

class _Conn:
    def __init__(self, log):
        self.log = log

    def execute(self, stmt, params=None):
        self.log.append((str(stmt), params))

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Engine:
    def __init__(self):
        self.log = []

    def begin(self):
        return _Conn(self.log)

    def connect(self):
        return _Conn(self.log)


def test_persist_calls_is_one_parameterized_insert_per_call():
    opp, v = _card_inputs()
    econ = hx._call_economics("PUT", 17.0, 1.35, 2.40, 18.27, 0.78, date(2027, 1, 15), TODAY)
    row = {"ticker": "OMER", "direction": "PUT", "contract": v["contract"], "economics": econ,
           "spot": 18.27, "spot_source": "yfinance", "score": 6.07,
           "signal_date": date(2026, 11, 3), "signals": opp["signals"],
           "catalyst": opp["catalyst"], "llm_verdict": "PASS", "provenance": {"a": 1}}
    eng = _Engine()

    assert hx._persist_calls(eng, "run1", [row, dict(row, ticker="BEAM")]) == 2

    assert len(eng.log) == 2
    sql, params = eng.log[0]
    assert sql.startswith("INSERT INTO options_digest_calls")
    assert "OMER" not in sql and ":ticker" in sql
    assert params["ask"] == 2.40 and params["strike"] == 17.0 and params["run"] == "run1"
    assert params["be"] == pytest.approx(14.60)
    assert '"iv_skew"' in params["signals"]
    assert "UPDATE" not in sql and "DELETE" not in sql
    assert hx._persist_calls(eng, "run2", []) == 0


def test_load_recent_calls_binds_the_window():
    since = datetime(2026, 10, 4, tzinfo=timezone.utc)
    emailed = datetime(2026, 11, 3, 15, tzinfo=timezone.utc)
    rows = [("OMER", "PUT", date(2027, 1, 15), 17, emailed)]
    eng = type("E", (), {"connect": lambda s: _RowsConn(rows)})()

    assert hx._load_recent_calls(eng, since) == [
        {"ticker": "OMER", "direction": "PUT", "expiry": date(2027, 1, 15), "strike": 17.0,
         "emailed_at": emailed}]


class _RowsConn(_Conn):
    def __init__(self, rows):
        super().__init__([])
        self.rows = rows

    def execute(self, stmt, params=None):
        assert ":since" in str(stmt) and isinstance(params["since"], datetime)
        return types.SimpleNamespace(fetchall=lambda: self.rows)


# ── Migration ───────────────────────────────────────────────────────────────

def test_ledger_migration_is_append_only_and_granted():
    path = (Path(__file__).resolve().parent.parent / "migrations" / "versions"
            / "options_digest_calls_20261104.py")
    src = path.read_text()
    assert 'down_revision = "merge_yf_finra_20261023"' in src
    assert "CREATE TABLE IF NOT EXISTS options_digest_calls" in src
    assert "BEFORE UPDATE OR DELETE ON options_digest_calls" in src
    assert "BEFORE TRUNCATE ON options_digest_calls" in src
    # Append-only is also a privilege: grid may read and insert, nothing else.
    assert "GRANT SELECT, INSERT ON options_digest_calls TO grid;" in src
    assert "GRANT USAGE, SELECT ON SEQUENCE options_digest_calls_id_seq TO grid;" in src
    assert "UPDATE ON options_digest_calls TO" not in src
    # Every column the digest binds exists in the table.
    import re

    cols = set(re.findall(r"^\s{8}([a-z_]+)\s+[A-Z]", src, re.M))
    insert_cols = re.search(r"INSERT INTO options_digest_calls \((.*?)\) VALUES",
                            Path(hx.__file__).read_text().replace('"\n                "', ""),
                            re.S)
    assert insert_cols is not None
    wanted = {c.strip() for c in insert_cols.group(1).replace('"', "").split(",")}
    assert wanted <= cols, wanted - cols


def test_plain_text_keeps_table_cells_apart():
    record = {"calls": [{"ticker": "A", "direction": "PUT", "expiry": date(2026, 12, 18),
                         "strike": 1.0, "entry": 1.0, "status": "OPEN", "return": 0.1,
                         "emailed_at": datetime(2026, 11, 2, tzinfo=timezone.utc)}],
              "settled": 0, "settled_wins": 0, "settled_hit_rate": None, "open": 1,
              "open_median_return": 0.1, "unpriced": 0}
    plain = hx._plain(hx._format_track_record(record)["body"])
    assert "Emailed  Contract  Entry  Status  Return" in plain



def test_summary_does_not_invent_quote_coverage_or_risk():
    data = [{"direction": "CALL", "economics": {"max_loss": float("inf")}}, {}]
    card = hx._format_summary_section(data, [{"contract": {"bid": 1, "ask": float("nan")}}])
    plain = hx._plain(card["body"])
    assert "0/2 shown ideas have a retrieved two-sided quote" in plain
    assert "1 direction unavailable" in plain
    assert "coverage unavailable" in plain
    assert "Premium at risk" not in plain
    assert "UNPROVEN" in plain


def test_summary_scanner_failure_is_not_zero_candidate_all_clear():
    card = hx._format_summary_section([], [], scan_stats={
        "scanner_failed": True, "sanity_rejected": 0, "llm_rejected": 0, "not_checked": 0})
    assert "candidate coverage is unavailable" in card["body"]
    assert "0 scanner candidates returned" not in card["body"]


def test_card_keeps_full_thesis_and_flags_expired_stale_inputs():
    opp, verification = _card_inputs()
    today = datetime.now(timezone.utc).date()
    old = today - timedelta(days=40)
    opp.update(as_of=old, signal_date=old, thesis="Detailed <thesis> " * 50 + "THE END",
               catalyst={"status": "found", "date": old.isoformat(), "type": "earnings", "source": "test"})
    verification["contract"]["expiry"] = (today - timedelta(days=1)).isoformat()
    html = hx._format_opportunity_card(opp, verification, 1)["body"]
    assert "EXPIRED" in html and "Historical idea: BUY" in html
    assert "PAST DATE, outcome unconfirmed" in html
    assert "STALE at rendering" in html
    assert "Detailed &lt;thesis&gt;" in html and "THE END" in html
    assert "<thesis>" not in html


def test_track_record_keeps_full_bounded_sample_and_price_dates():
    row = {"ticker": "X", "direction": "PUT", "expiry": date(2026, 10, 1), "strike": 10,
           "entry": 1, "status": "SETTLED", "return": .5, "priced_on": date(2026, 10, 1),
           "emailed_at": datetime(2026, 9, 1, tzinfo=timezone.utc)}
    calls = [dict(row, ticker=f"R{i:02}") for i in range(50)]
    record = {"as_of": date(2026, 11, 1), "calls": calls, "settled": 50, "settled_wins": 50,
              "settled_hit_rate": 1, "settled_median_return": .5, "open": 0, "open_median_return": None}
    plain = hx._plain(hx._format_track_record(record)["body"])
    assert "all 50 supplied rows shown" in plain
    assert "R49" in plain and "Price date" in plain
    assert "not a lifetime record" in plain and "not realized account P/L" in plain


def test_card_escapes_provenance_dates_and_catalyst_text():
    opp, verification = _card_inputs()
    verification["contract"]["quote_at"] = "<quote>"
    verification["spot_at"] = "<spot>"
    opp["catalyst"]["date"] = "<event>"
    html = hx._format_opportunity_card(opp, verification, 1)["body"]
    for field in ("quote", "spot", "event"):
        assert f"<{field}>" not in html
        assert f"&lt;{field}&gt;" in html



def test_card_expiring_today_and_future_signal_are_explicit():
    opp, verification = _card_inputs()
    now = datetime.now(timezone.utc)
    opp.update(as_of=now, signal_date=now.date() + timedelta(days=1))
    verification["contract"]["expiry"] = now.date().isoformat()
    html = hx._format_opportunity_card(opp, verification, 1)["body"]
    assert "Expires today; intraday expiry cutoff unverified" in html
    assert "FUTURE date, freshness unverified" in html
