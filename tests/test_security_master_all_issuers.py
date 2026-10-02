"""Tests for the all-issuers ``security_master`` seed (builder + loader).

Fixtures only: no network, no database, no real SEC files. The Postgres proof lives in
``tests/test_apply_security_master_seed_pg.py``.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from intelligence.people_events_pipeline import security as S
from scripts import apply_security_master_seed as loader
from scripts import build_security_master_all_issuers as builder

TICKERS_AS_OF = "2026-09-27"


def _subs(rows: list[tuple[str, str, str, str]]) -> pd.DataFrame:
    """(accession, filing_date, issuer_cik, issuer_ticker) -> a submissions frame."""
    return pd.DataFrame(rows, columns=["accession_number", "filing_date", "issuer_cik", "issuer_ticker"])


def _build(rows, company_tickers=None, **kw):
    return builder.build_rows(_subs(rows), company_tickers or {}, tickers_as_of=TICKERS_AS_OF, **kw)


def _plus(iso, days):
    return (date.fromisoformat(iso) + timedelta(days=days)).isoformat()


def _rows_of(built, cik, ticker):
    """Ticker rows of one entity for one ticker, in window order."""
    out = [r for r in built["security_identifiers"] if r["id_scheme"] == "ticker" and r["id_value"] == ticker
           and r["entity_id"] == builder.entity_id_for_cik(cik)]
    return sorted(out, key=lambda r: r["valid_from"])


def _tickers(built, cik=None):
    out = [r for r in built["security_identifiers"] if r["id_scheme"] == "ticker"]
    if cik is not None:
        out = [r for r in out if r["entity_id"] == builder.entity_id_for_cik(cik)]
    return {(r["id_value"], r["valid_from"], r["valid_to"]): r for r in out}


# --- placeholder and multi-ticker parsing ---------------------------------------------------


@pytest.mark.parametrize("raw", [None, float("nan"), "", "  ", "NONE", "None", "N/A", "n/a", "NA", "-", "--", "0",
                                 "NO SYMBOL", "No Ticker", "NOT APPLICABLE", "UNKNOWN", "TBD", "none."])
def test_placeholders_are_rejected(raw):
    tickers, why = builder.parse_ticker_field(raw)
    assert tickers == ()
    assert why in {"blank", "placeholder"}


@pytest.mark.parametrize("raw, expected", [
    ("AAPL", ("AAPL",)),
    (" aapl ", ("AAPL",)),
    ("BRK.B", ("BRKB",)),
    ("BRK-B", ("BRKB",)),
    ("brk/a", ("BRKA",)),
    ("ISCA, ISCB", ("ISCA", "ISCB")),
    ("GEF,GEF.B", ("GEF", "GEFB")),
    ("BRK.A, BRK.B", ("BRKA", "BRKB")),
    ("WSO; WSOB", ("WSO", "WSOB")),
    ("Z AND ZG", ("Z", "ZG")),
    ("LTR;CG", ("LTR", "CG")),
    ("CRDA CRDB", ("CRDA", "CRDB")),
    ("BIO BIOB", ("BIO", "BIOB")),
    ("BWINA / B", ("BWINA", "BWINB")),
    ("N O G", ("NOG",)),
    ("NYSE: KRC", ("KRC",)),
    ("NASDAQ:AAPL", ("AAPL",)),
    ("(SIRI)", ("SIRI",)),
    ("SIRI (NASDAQ)", ("SIRI",)),
    ("SWWI.PK", ("SWWI",)),
    ("NONE, MSFT", ("MSFT",)),
    ("AAPL, AAPL", ("AAPL",)),
    ("NWIN(OB)", ("NWIN",)),
    ("DYSL:OB", ("DYSL",)),
    ("QADA_QADB", ("QADA", "QADB")),
    ("BDG/BDGA", ("BDG", "BDGA")),
    ("STZ/STZ.B", ("STZ", "STZB")),
    ("BF/B", ("BFB",)),
    ("UA/UAA", ("UA", "UAA")),
    ("AT&T", ("T",)),
    ("AT & T", ("T",)),
    ("Z & ZG", ("Z", "ZG")),
    ("AAPL.O", ("AAPL",)),
    ("ABC.N", ("ABC",)),
    ("BRK.A", ("BRKA",)),
    ("ENTX-PK", ("ENTX",)),
    ("NM", ("NM",)),   # a real ticker that is also a market tag
    ("PK", ("PK",)),
])
def test_ticker_cleaning(raw, expected):
    tickers, why = builder.parse_ticker_field(raw)
    assert tickers == expected
    assert why is None


@pytest.mark.parametrize("raw, why", [
    ("000", "numeric"),
    ("85453P206", "too_long_or_id_like"),   # a CUSIP in the ticker field
    ("UNASSIGNED", "too_long_or_id_like"),
    ("US000B", "too_long_or_id_like"),
    ("ASX:CRN", "foreign_exchange"),
    ("OV6:GR", "foreign_exchange"),
    ("NYSE", "unparseable"),
    ("[NONE]", "placeholder"),
    ("NONE***", "placeholder"),
    ("M??????", "unparseable"),
    ("HCA INC.", "ambiguous_whitespace"),
])
def test_noise_that_is_not_a_us_ticker_is_rejected_with_a_reason(raw, why):
    assert builder.parse_ticker_field(raw) == ((), why)


def test_ambiguous_whitespace_is_rejected_not_guessed():
    # "LEE ENT" is Lee Enterprises' name fragment; reading it as two tickers would invent ENT.
    assert builder.parse_ticker_field("LEE ENT") == ((), "ambiguous_whitespace")


def test_cleaned_values_match_the_consumers_normalizer():
    from intelligence.people_events_pipeline.rules import normalize_ticker

    for raw in ("BRK.B", "brk-b", "GOOG", "BF/B"):
        assert builder.parse_ticker_field(raw)[0] == (normalize_ticker(raw),)


def test_company_tickers_parse_and_merge():
    one = builder.parse_company_tickers({
        "0": {"cik_str": 1652044, "ticker": "GOOGL", "title": "Alphabet Inc."},
        "1": {"cik_str": 1652044, "ticker": "GOOG", "title": "Alphabet Inc. (class C)"},
        "2": {"cik_str": "bad", "ticker": "ZZZ", "title": "No cik"},
        "3": {"cik_str": 99, "ticker": "BRK-B", "title": "Berkshire"},
    })
    assert one[1652044] == {"name": "Alphabet Inc.", "tickers": ["GOOGL", "GOOG"]}
    assert one[99]["tickers"] == ["BRKB"]
    assert 0 not in one and len(one) == 2
    assert builder.company_tickers_without_cik({"2": {"cik_str": "bad", "ticker": "ZZZ"}, "3": {"cik_str": 5, "ticker": "OK"}}) == {"ZZZ"}
    other = builder.parse_company_tickers({"0": {"cik_str": 1652044, "ticker": "GOOGX", "title": "Other name"}})
    merged = builder.merge_company_tickers([one, other])
    assert merged[1652044]["name"] == "Alphabet Inc."  # first file wins the name
    assert merged[1652044]["tickers"] == ["GOOGL", "GOOG", "GOOGX"]


# --- dated ticker history -------------------------------------------------------------------


def test_ticker_change_gives_closed_and_open_windows():
    built = _build([
        ("a1", "2015-03-02", "100", "OLD"),
        ("a2", "2017-12-01", "100", "OLD"),
        ("a3", "2018-01-15", "100", "NEW"),
        ("a4", "2019-07-01", "100", "NEW"),
    ])
    assert set(_tickers(built, 100)) == {
        ("OLD", "2015-03-02", "2018-01-14"),  # the day before the first later filing naming another ticker
        ("NEW", "2018-01-15", None),          # still the latest
    }


def test_a_closed_window_is_capped_at_the_grace_period_after_the_issuers_last_filing():
    """OLDT is named in 2008 and the issuer is silent until 2020 (when it names NEWT): OLDT ends in 2009, not 2019."""
    built = _build([("a1", "2008-03-01", "400", "OLDT"), ("a2", "2020-01-01", "400", "NEWT")])
    rows = _tickers(built, 400)
    assert set(rows) == {("OLDT", "2008-03-01", _plus("2008-03-01", 400)), ("NEWT", "2020-01-01", None)}
    detail = rows[("OLDT", "2008-03-01", _plus("2008-03-01", 400))]["conflict_detail"]
    assert detail["reason"] == "grace_period_before_the_issuers_next_ticker"
    assert built["report"]["silent_windows_closed"] == {"grace_period_before_the_issuers_next_ticker": 1}
    assert _resolve_ticker(built, [("OLDT", "2008-06-01"), ("OLDT", "2015-01-01"), ("OLDT", "2019-06-01")]) == [
        ("sm_0000000400", "ticker", False), (None, "ticker_outside_validity", False), (None, "ticker_outside_validity", False)]


def test_placeholder_filings_do_not_close_a_window():
    built = _build([
        ("a1", "2015-03-02", "100", "ABC"),
        ("a2", "2016-01-01", "100", "NONE"),
        ("a3", "2017-01-01", "100", "N/A"),
        ("a4", "2018-01-01", "100", "ABC"),
    ])
    assert set(_tickers(built, 100)) == {("ABC", "2015-03-02", None)}


def test_dual_class_issuer_keeps_both_tickers_open():
    built = _build([
        ("a1", "2012-01-03", "200", "ISCA, ISCB"),
        ("a2", "2013-02-04", "200", "ISCA"),
        ("a3", "2014-03-05", "200", "ISCB"),
    ])
    assert set(_tickers(built, 200)) == {("ISCA", "2012-01-03", None), ("ISCB", "2012-01-03", None)}


def test_share_class_siblings_stay_open_when_a_filing_names_only_one_class():
    built = _build([
        ("a1", "2010-01-04", "400", "GOOG"),
        ("a2", "2012-02-02", "400", "GOOGL"),
        ("a3", "2014-03-03", "400", "GOOG"),
    ])
    assert set(_tickers(built, 400)) == {("GOOG", "2010-01-04", None), ("GOOGL", "2012-02-02", None)}
    # ... until a filing names a ticker outside the family, which closes the whole family together.
    built = _build([
        ("a1", "2010-01-04", "400", "GOOG"),
        ("a2", "2012-02-02", "400", "GOOGL"),
        ("a3", "2014-03-03", "400", "GOOG"),
        ("a4", "2016-04-04", "400", "XYZ"),
    ])
    close = _plus("2014-03-03", 400)  # capped at the family's last filing + grace, not held to the next ticker's day
    assert set(_tickers(built, 400)) == {("GOOG", "2010-01-04", close), ("GOOGL", "2012-02-02", close), ("XYZ", "2016-04-04", None)}


def test_an_old_ticker_ends_even_if_the_next_filing_is_in_a_multi_ticker_field():
    built = _build([
        ("a1", "2010-01-04", "300", "AAA"),
        ("a2", "2010-11-01", "300", "AAA"),
        ("a3", "2011-06-01", "300", "BBB, BBC"),
    ])
    assert set(_tickers(built, 300)) == {
        ("AAA", "2010-01-04", "2011-05-31"), ("BBB", "2011-06-01", None), ("BBC", "2011-06-01", None)}


def test_cik_identifier_is_unpadded_and_dated_at_the_first_filing():
    built = _build([("a1", "2011-04-04", "0000000123", "XYZ"), ("a2", "2009-02-02", "123", "NONE")])
    ciks = [r for r in built["security_identifiers"] if r["id_scheme"] == "cik"]
    assert [(r["entity_id"], r["id_value"], r["valid_from"], r["valid_to"]) for r in ciks] == [
        ("sm_0000000123", "123", "2009-02-02", None)]
    sm = built["security_master"][0]
    assert sm["entity_id"] == "sm_0000000123" and sm["cik"] == 123 and sm["source"] == "all_issuers_v1"


def test_rows_with_a_bad_cik_or_date_are_dropped_and_counted():
    built = _build([
        ("a1", "2011-04-04", "123", "XYZ"),
        ("a2", "2011-04-05", "", "LOST"),
        ("a3", "not-a-date", "123", "XYZ"),
        ("a4", "2011-04-06", "0", "ZERO"),
    ])
    stats = built["noise"]["stats"]
    assert stats["rows_bad_cik"] == 2 and stats["rows_bad_filing_date"] == 1
    assert built["report"]["tickers_without_cik"]["form345_distinct_tickers"] == 2
    assert built["report"]["tickers_without_cik"]["form345_rows_with_a_ticker_but_no_valid_cik"] == 2
    assert {r["entity_id"] for r in built["security_master"]} == {"sm_0000000123"}


def test_company_tickers_names_ciks_and_adds_only_unnamed_current_tickers():
    ct = {
        123: {"name": "Acme Corp", "tickers": ["XYZ", "XYZ2"]},   # XYZ is named by a filing and still open
        777: {"name": "Quiet Co", "tickers": ["QUIET"]},           # never filed Form 3/4/5
    }
    built = _build([("a1", "2011-04-04", "123", "XYZ"), ("a2", "2012-04-04", "456", "OTHR")], ct,
                   nonderiv_names={456: "Other Inc"})
    assert set(_tickers(built, 123)) == {("XYZ", "2011-04-04", None), ("XYZ2", TICKERS_AS_OF, None)}
    assert set(_tickers(built, 777)) == {("QUIET", TICKERS_AS_OF, None)}
    names = {r["cik"]: (r["name"], r["provenance"]["name_source"]) for r in built["security_master"]}
    assert names == {123: ("Acme Corp", "sec_company_tickers"), 456: ("Other Inc", "form4_latest_issuer_name"),
                     777: ("Quiet Co", "sec_company_tickers")}
    quiet = next(r for r in built["security_master"] if r["cik"] == 777)
    assert quiet["provenance"]["form345_filings"] == 0
    # SEC absence alone is a candidate signal, never a flip (GD0 decision #2).
    other = next(r for r in built["security_master"] if r["cik"] == 456)
    assert other["is_active"] is True and other["delisted_basis"] == "candidate_sec_absence_only"


def test_sic_is_optional_and_zero_means_none():
    rows = [("a1", "2011-04-04", "123", "XYZ"), ("a2", "2011-04-04", "124", "ABC")]
    plain = _build(rows)
    assert all(r["sic"] is None for r in plain["security_master"])
    with_sic = _build(rows, sic_map={123: {"sic": 3571, "name": "Acme", "fetched_at": "x"}, 124: {"sic": None, "name": None, "fetched_at": "x"}})
    got = {r["cik"]: r["sic"] for r in with_sic["security_master"]}
    assert got == {123: 3571, 124: None}
    assert next(r for r in with_sic["security_master"] if r["cik"] == 123)["provenance"]["sic_source"] == \
        "sec_submissions_current_not_point_in_time"


# --- silent issuers: windows that must not stay open forever --------------------------------


def _quarterly(prefix, cik, ticker, first_year, last_year):
    """Four filings a year naming ``ticker``: an issuer whose insiders file all the time."""
    return [(f"{prefix}{y}{m}", f"{y}-{m:02d}-15", str(cik), ticker) for y in range(first_year, last_year + 1) for m in (2, 5, 8, 11)]


def _resolve_ticker(built, events):
    ids = _identifiers_frame(built)
    out = S.resolve_securities(_events([(None, t, d) for t, d in events]), ids)
    return list(zip(out["security_id"], out["security_match_basis"], out["security_conflict"]))


def test_a_silent_non_holder_closes_400_days_after_its_last_filing():
    built = _build([("a1", "2008-03-01", "5", "ARMX"), ("a2", "2010-03-01", "5", "ARMX"),
                    ("z1", "2026-01-05", "6", "LIVE")])  # data runs to 2026-01-05
    row = _tickers(built, 5)
    assert set(row) == {("ARMX", "2008-03-01", "2011-04-05")}  # 2010-03-01 + 400 days
    detail = row[("ARMX", "2008-03-01", "2011-04-05")]["conflict_detail"]
    assert detail["kind"] == "silent_issuer_closed" and detail["grace_days"] == 400
    assert detail["reason"] == "grace_period_after_last_filing" and detail["last_filing_naming_ticker"] == "2010-03-01"
    assert built["report"]["silent_windows_closed"] == {"grace_period_after_last_filing": 1}


def test_an_issuer_inside_the_grace_period_of_the_data_end_stays_open():
    built = _build([("a1", "2024-03-01", "5", "NEWCO"), ("z1", "2025-03-01", "6", "LIVE")])  # 2024-03-01 + 400d is after 2025-03-01
    assert set(_tickers(built, 5)) == {("NEWCO", "2024-03-01", None)}


def test_the_company_tickers_holder_stays_open_however_long_it_has_been_quiet():
    ct = {5: {"name": "Quiet Co", "tickers": ["QUIET"]}}
    built = _build([("a1", "2008-03-01", "5", "QUIET"), ("z1", "2026-01-05", "6", "LIVE")], ct)
    assert set(_tickers(built, 5)) == {("QUIET", "2008-03-01", None)}


def test_a_silent_issuer_is_closed_the_day_before_another_cik_takes_its_ticker():
    built = _build([("a1", "2008-01-02", "1", "GONE"), ("b1", "2008-06-01", "2", "GONE"),
                    *_quarterly("b", 2, "GONE", 2009, 2012)])
    gone = _tickers(built, 1)
    assert set(gone) == {("GONE", "2008-01-02", "2008-05-31")}
    assert gone[("GONE", "2008-01-02", "2008-05-31")]["conflict_detail"]["reason"] == "other_cik_took_the_ticker"
    assert not built["conflicts"]


def test_silent_closure_can_be_turned_off_and_the_grace_period_changed():
    rows = [("a1", "2008-03-01", "5", "ARMX"), ("z1", "2026-01-05", "6", "LIVE")]
    assert set(_tickers(_build(rows, close_silent=False), 5)) == {("ARMX", "2008-03-01", None)}
    assert set(_tickers(_build(rows, grace_days=30), 5)) == {("ARMX", "2008-03-01", "2008-03-31")}


def test_share_class_siblings_are_closed_together_by_the_silence_rule():
    built = _build([("a1", "2010-01-04", "400", "GOOG"), ("a2", "2012-02-02", "400", "GOOGL"), ("a3", "2014-03-03", "400", "GOOG"),
                    ("z1", "2026-01-05", "6", "LIVE")])
    assert {k[2] for k in _tickers(built, 400)} == {"2015-04-07"}  # the family's last filing (2014-03-03) + 400 days


def test_probe_acquired_subsidiary_does_not_hold_the_parents_ticker():
    """A subsidiary naming BAC in 2009-2012 must not capture BAC for the rest of time."""
    rows = ([(f"p{y}", f"{y}-03-01", "70858", "BAC") for y in range(2006, 2027)]
            + [(f"s{y}", f"{y}-02-01", "65100", "MER") for y in (2006, 2007, 2008)]
            + [(f"s{y}", f"{y}-05-01", "65100", "BAC") for y in (2009, 2010, 2011, 2012)])
    built = _build(rows, {70858: {"name": "Bank of America", "tickers": ["BAC"]}})
    sub_close = _plus("2012-05-01", 400)
    (sub,) = _rows_of(built, 65100, "BAC")
    assert (sub["valid_from"], sub["valid_to"]) == ("2009-05-01", sub_close)
    assert sub["conflict_flag"] is True and sub["is_primary"] is False
    before, during, after = _rows_of(built, 70858, "BAC")
    assert (before["valid_to"], during["valid_from"], during["valid_to"], after["valid_from"]) == (
        "2009-04-30", "2009-05-01", sub_close, _plus(sub_close, 1))
    assert [(r["is_primary"], r["conflict_flag"]) for r in (before, during, after)] == [(True, False), (True, True), (True, False)]
    got = _resolve_ticker(built, [("BAC", "2008-06-01"), ("BAC", "2010-06-01"), ("BAC", "2018-06-01"), ("BAC", "2026-06-01")])
    assert [g[0] for g in got] == ["sm_0000070858"] * 4
    assert [g[2] for g in got] == [False, True, False, False]  # only the contested period carries the conflict


def test_probe_one_stray_filing_cannot_hijack_a_ticker():
    rows = [(f"a{y}", f"{y}-03-01", "320193", "AAPL") for y in range(2006, 2027)] + [("x1", "2015-07-01", "999", "AAPL")]
    built = _build(rows, {320193: {"name": "Apple", "tickers": ["AAPL"]}})
    got = _resolve_ticker(built, [("AAPL", "2015-06-01"), ("AAPL", "2015-12-01"), ("AAPL", "2016-09-01"), ("AAPL", "2026-06-01")])
    assert [g[0] for g in got] == ["sm_0000320193"] * 4
    (stray,) = _rows_of(built, 999, "AAPL")
    assert (stray["valid_from"], stray["valid_to"]) == ("2015-07-01", "2016-08-04")
    assert stray["is_primary"] is False and stray["conflict_flag"] is True


def test_probe_dead_issuer_is_not_resolved_after_a_non_filer_takes_the_ticker():
    rows = [("d1", "2008-03-01", "5", "ARMX"), ("d2", "2010-03-01", "5", "ARMX"), ("z1", "2026-01-05", "6", "LIVE")]
    built = _build(rows, {9: {"name": "Foreign ADR plc", "tickers": ["ARMX"]}})
    got = _resolve_ticker(built, [("ARMX", "2009-01-01"), ("ARMX", "2020-01-01"), ("ARMX", "2026-09-30")])
    assert got[0][:2] == ("sm_0000000005", "ticker")
    assert got[1][:2] == (None, "ticker_outside_validity")  # not silently the dead issuer
    assert got[2][:2] == ("sm_0000000009", "ticker")


def test_probe_a_stray_latest_filing_closes_the_real_ticker_and_is_documented_behaviour():
    rows = [(f"a{y}", f"{y}-03-01", "111", "ABCD") for y in range(2010, 2026)] + [("z", "2026-01-05", "111", "WXYZ")]
    built = _build(rows)
    assert ("ABCD", "2010-03-01", "2026-01-04") in _tickers(built, 111)  # a last filing naming another ticker ends it
    assert _resolve_ticker(built, [("ABCD", "2026-03-01")])[0][:2] == (None, "ticker_outside_validity")


def test_r1_probe_a_holder_that_loses_one_contest_is_still_primary_everywhere_else():
    """Holder A (company_tickers, files yearly 2006-2026) loses a 2009 contest on filing days to subsidiary B; a stray C
    files once in 2018. A must be primary in 2018/2019, not the stray."""
    rows = ([(f"a{y}", f"{y}-03-01", "100", "TKR") for y in range(2006, 2027)]
            + [(f"b{m}{d}", f"2009-0{m}-1{d}", "200", "TKR") for m in (4, 5, 6) for d in range(0, 4)]
            + [("c1", "2018-07-01", "300", "TKR")])
    built = _build(rows, {100: {"name": "Holder", "tickers": ["TKR"]}})
    got = _resolve_ticker(built, [("TKR", "2009-05-15"), ("TKR", "2018-08-01"), ("TKR", "2019-06-01"), ("TKR", "2022-01-01")])
    assert [g[0] for g in got] == ["sm_0000000200", "sm_0000000100", "sm_0000000100", "sm_0000000100"]
    assert [g[2] for g in got] == [True, True, True, False]  # 2009 and the stray's window (to 2019-08-05) are contested
    holder = _rows_of(built, 100, "TKR")
    lost = [r for r in holder if not r["is_primary"]]
    assert len(lost) == 1 and lost[0]["valid_from"].startswith("2009") and lost[0]["conflict_flag"]  # only the 2009 contest
    assert [r["conflict_flag"] for r in holder].count(True) == 2
    (stray,) = _rows_of(built, 300, "TKR")
    assert stray["is_primary"] is False and stray["conflict_flag"] is True
    assert built["report"]["overlap_pairs_without_primary"] == 0 and built["report"]["overlap_segments_with_two_primaries"] == 0
    assert builder.primary_violations(built["security_identifiers"]) == []


def test_primary_violations_reports_segments_without_exactly_one_primary():
    def row(entity, vf, vt, primary):
        return {"entity_id": entity, "id_scheme": "ticker", "id_value": "T", "valid_from": vf, "valid_to": vt, "is_primary": primary}

    ok = [row("sm_1", "2010-01-01", None, True), row("sm_2", "2015-01-01", "2016-12-31", False)]
    assert builder.primary_violations(ok) == []
    none_primary = [row("sm_1", "2010-01-01", None, False), row("sm_2", "2015-01-01", "2016-12-31", False)]
    (v,) = builder.primary_violations(none_primary)
    assert (v["kind"], v["from"], v["to"], v["pairs"]) == ("no_primary", "2015-01-01", "2016-12-31", 1)
    two = [row("sm_1", "2010-01-01", None, True), row("sm_2", "2015-01-01", "2016-12-31", True)]
    (v,) = builder.primary_violations(two)
    assert v["kind"] == "two_primaries"
    # The old per-row scheme: A primary only until a contest it lost, so a later overlap has no primary at all.
    per_row = [row("sm_1", "2006-01-01", None, False), row("sm_2", "2009-01-01", "2009-12-31", True), row("sm_3", "2018-07-01", "2019-08-01", False)]
    assert [x["kind"] for x in builder.primary_violations(per_row)] == ["no_primary"]


def test_every_contested_segment_has_exactly_one_primary_in_a_busy_fixture():
    rows = (_quarterly("a", 1, "BUSY", 2008, 2024) + _quarterly("b", 2, "BUSY", 2012, 2016) + _quarterly("c", 3, "BUSY", 2014, 2020)
            + [("d1", "2015-03-03", "4", "BUSY")])
    built = _build(rows, {3: {"name": "Holder", "tickers": ["BUSY"]}})
    assert built["report"]["contested_overlap_segments"] > 3
    assert built["report"]["overlap_pairs_without_primary"] == 0 and built["report"]["overlap_segments_with_two_primaries"] == 0
    assert builder.primary_violations(built["security_identifiers"]) == []
    keys = [(r["entity_id"], r["id_scheme"], r["id_value"], r["valid_from"]) for r in built["security_identifiers"]]
    assert len(keys) == len(set(keys))  # split pieces never collide on the table's unique key


# --- conflicts: a ticker reused by a different CIK ------------------------------------------


def _reuse_rows():
    return [
        # CIK 1 files under REUSE every quarter 2008-2020; CIK 2 files twice in 2015: contested, CIK 1 has the evidence.
        *_quarterly("a", 1, "REUSE", 2008, 2020),
        ("b1", "2015-06-01", "2", "REUSE"),
        ("b2", "2015-09-01", "2", "REUSE"),
        # a clean ticker change on CIK 3 (not a conflict)
        ("c1", "2010-01-04", "3", "CHG"),
        ("c2", "2012-01-04", "3", "CHG2"),
    ]


def test_ticker_reuse_by_a_different_cik_is_flagged_and_one_primary_is_chosen_by_evidence():
    built = _build(_reuse_rows())
    one, two = _rows_of(built, 1, "REUSE"), _rows_of(built, 2, "REUSE")
    (stray,) = two
    assert (stray["valid_from"], stray["conflict_flag"], stray["is_primary"]) == ("2015-06-01", True, False)
    before, during, after = one
    assert [(r["is_primary"], r["conflict_flag"]) for r in one] == [(True, False), (True, True), (True, False)]
    assert (during["valid_from"], during["valid_to"]) == (stray["valid_from"], stray["valid_to"])
    detail = during["conflict_detail"]
    assert detail["kind"] == "overlapping_ticker_claim" and detail["other_entities"] == ["sm_0000000002"]
    assert detail["primary_basis"] == ["most_filing_days_in_overlap"]
    assert [c["ticker"] for c in built["conflicts"]] == ["REUSE"]
    assert built["conflicts"][0]["segments"][0]["from"] == "2015-06-01" and built["conflicts"][0]["primary_entities"] == ["sm_0000000001"]
    assert built["report"]["conflict_tickers"] == 1 and built["report"]["conflict_identifier_rows"] == 2
    unrelated = [r for r in built["security_identifiers"] if r["id_value"] in {"CHG", "CHG2"}]
    assert unrelated and not any(r["conflict_flag"] for r in unrelated)


def test_the_company_tickers_holder_wins_an_overlap_that_is_open_at_the_snapshot():
    rows = [*_quarterly("a", 1, "BOTH", 2010, 2026), *_quarterly("b", 2, "BOTH", 2012, 2026)]
    built = _build(rows, {2: {"name": "Holder", "tickers": ["BOTH"]}})
    prim = {r["entity_id"]: r["is_primary"] for r in built["security_identifiers"] if r["id_value"] == "BOTH" and r["conflict_flag"]}
    assert prim == {"sm_0000000001": False, "sm_0000000002": True}
    basis = next(r for r in built["security_identifiers"] if r["id_value"] == "BOTH" and r["conflict_flag"])["conflict_detail"]["primary_basis"]
    assert basis == ["company_tickers_holder_open_at_snapshot"]


def test_an_exact_tie_falls_to_the_smaller_entity_id_deterministically():
    rows = [("a1", "2012-01-02", "1", "TIE"), ("b1", "2012-01-02", "2", "TIE"), ("z1", "2012-02-01", "6", "LIVE")]
    built = _build(rows)
    prim = {r["entity_id"]: r["is_primary"] for r in built["security_identifiers"] if r["id_value"] == "TIE" and r["conflict_flag"]}
    assert prim == {"sm_0000000001": True, "sm_0000000002": False}


def test_sequential_reuse_with_closed_windows_is_not_a_conflict():
    built = _build([
        ("a1", "2008-01-02", "1", "SEQ"),
        ("a2", "2009-01-02", "1", "SEQ2"),   # CIK 1 moved on, so SEQ closes 2009-01-01
        ("b1", "2015-06-01", "2", "SEQ"),
    ])
    assert not built["conflicts"]
    assert _tickers(built, 1)[("SEQ", "2008-01-02", "2009-01-01")]["conflict_flag"] is False


# --- idempotency ----------------------------------------------------------------------------


def _artifact(tmp_path: Path, name: str, built) -> dict:
    return builder.write_artifact(built, tmp_path / name, inputs=[], params={"tickers_as_of": TICKERS_AS_OF}, code_sha="abc123")


def test_a_second_build_is_byte_identical_and_inserts_nothing_new(tmp_path):
    rows = _reuse_rows()
    first = _artifact(tmp_path, "one", _build(rows, {1: {"name": "One Inc", "tickers": ["REUSE"]}}))
    second = _artifact(tmp_path, "two", _build(rows, {1: {"name": "One Inc", "tickers": ["REUSE"]}}))
    assert first["output"]["sha256"] == second["output"]["sha256"]
    assert (tmp_path / "one" / builder.SEED_FILE).read_bytes() == (tmp_path / "two" / builder.SEED_FILE).read_bytes()

    seed = loader.load_seed(tmp_path / "one", require_receipt=True)
    fresh = loader.plan_inserts(seed, loader.Existing())
    assert len(fresh.sm_inserts) == 3 and len(fresh.si_inserts) == len(seed.security_identifiers)

    # Database state after the first apply == every row of the first plan.
    existing = loader.build_existing(
        [(r["entity_id"], r["cik"]) for r in fresh.sm_inserts],
        [(r["entity_id"], r["id_scheme"], r["id_value"], r["valid_from"], r["valid_to"]) for r in fresh.si_inserts],
    )
    again = loader.plan_inserts(loader.load_seed(tmp_path / "two", require_receipt=True), existing)
    assert again.sm_inserts == [] and again.si_inserts == []
    assert again.skipped["security_master:entity_exists"] == 3


def test_receipt_records_inputs_hash_counts_and_code(tmp_path):
    src = tmp_path / "company_tickers.json"
    src.write_text("{}", encoding="utf-8")
    built = _build([("a1", "2011-04-04", "123", "XYZ")])
    inputs = [{"label": "company_tickers", "path": str(src), "sha256": builder.sha256_file(src), "bytes": 2}]
    receipt = builder.write_artifact(built, tmp_path / "out", inputs=inputs, params={"tickers_as_of": TICKERS_AS_OF}, code_sha="deadbeef")
    on_disk = json.loads((tmp_path / "out" / "receipt.json").read_text(encoding="utf-8"))
    assert on_disk["code"]["git_head"] == "deadbeef" and len(on_disk["code"]["builder_file_sha256_lf"]) == 64
    assert on_disk["inputs"][0]["sha256"] == builder.sha256_file(src)
    assert on_disk["counts"]["entities"] == 1 and on_disk["writes_to_database"] is False
    assert on_disk["output"]["sha256"] == receipt["output"]["sha256"]
    with pytest.raises(FileExistsError):
        builder.write_artifact(built, tmp_path / "out", inputs=inputs, params={}, code_sha="deadbeef")


def test_the_builder_refuses_a_dirty_git_tree_unless_allowed(monkeypatch):
    class Done:
        def __init__(self, out):
            self.returncode, self.stdout = 0, out

    def fake_run(argv, **_kw):
        return Done("abc123\n" if argv[1] == "rev-parse" else " M scripts/x.py\n")

    monkeypatch.setattr(builder.subprocess, "run", fake_run)
    with pytest.raises(ValueError, match="uncommitted changes"):
        builder._code_sha(None)
    assert builder._code_sha(None, allow_dirty=True)["dirty"] is True
    assert builder._code_sha("pinned")["dirty"] is None  # an explicit --code-sha is taken as given
    monkeypatch.setattr(builder.subprocess, "run", lambda argv, **_kw: Done("abc123\n" if argv[1] == "rev-parse" else ""))
    assert builder._code_sha(None) == {**builder._code_sha(None), "git_head": "abc123", "dirty": False}


def test_builder_and_loader_never_open_a_connection_or_update_or_delete():
    for name in ("build_security_master_all_issuers.py", "apply_security_master_seed.py"):
        src = (Path(__file__).resolve().parent.parent / "scripts" / name).read_text(encoding="utf-8")
        code = re.sub(r'""".*?"""', "", src, flags=re.S)  # docstrings legitimately describe the rollback SQL
        kept = [ln for ln in code.splitlines() if "all_issuers_v1" not in ln]  # the printed ROLLBACK_SQL constant
        code = " ".join(kept)
        assert not re.search(r"\b(UPDATE\s+\w+\s+SET|DELETE\s+FROM|TRUNCATE|DROP\s+TABLE|ALTER\s+TABLE)\b", code, re.I), name
    builder_src = (Path(__file__).resolve().parent.parent / "scripts" / "build_security_master_all_issuers.py").read_text(encoding="utf-8")
    assert not re.search(r"create_engine|psycopg2|import requests|urllib|sqlalchemy", builder_src)


# --- the consumer: resolve_securities on the produced identifiers ---------------------------


def _identifiers_frame(built) -> pd.DataFrame:
    return pd.DataFrame(built["security_identifiers"])[S.IDENTIFIER_COLUMNS]


def _events(rows: list[tuple]) -> pd.DataFrame:
    """(entity_cik, entity_ticker, event_date) -> issuer events."""
    return pd.DataFrame({
        "channel": "form4",
        "entity_kind": "issuer",
        "entity_cik": pd.array([r[0] for r in rows], dtype="Int64"),
        "entity_ticker": [r[1] for r in rows],
        "event_date": [pd.Timestamp(r[2]).date() for r in rows],
    })


def test_resolve_securities_matches_by_cik_and_by_in_window_ticker_and_rejects_out_of_window():
    built = _build([
        ("a1", "2015-03-02", "100", "OLD"),
        ("a1b", "2016-05-10", "100", "OLD"),
        ("a1c", "2017-12-01", "100", "OLD"),
        ("a2", "2018-01-15", "100", "NEW"),
        ("a3", "2019-07-01", "100", "NEW"),
        ("z1", "2016-01-04", "200", "OTHER"),
    ])
    ids = _identifiers_frame(built)
    ev = _events([
        (100, None, "2005-01-01"),     # CIK match, before any identifier's valid_from
        (None, "OLD", "2016-06-01"),   # inside OLD's window
        (None, "OLD", "2018-02-01"),   # after OLD closed (2018-01-14): out of window
        (None, "NEW", "2018-01-15"),   # first day of NEW
        (None, "NEW", "2017-12-31"),   # before NEW started
        (None, "NEVER", "2018-01-15"), # unknown ticker
        (200, "OLD", "2016-06-01"),    # CIK wins over a ticker that belongs to another entity
    ])
    out = S.resolve_securities(ev, ids)
    assert list(out["security_id"]) == ["sm_0000000100", "sm_0000000100", None, "sm_0000000100", None, None, "sm_0000000200"]
    assert list(out["security_match_basis"]) == [
        "cik", "ticker", "ticker_outside_validity", "ticker", "ticker_outside_validity", "unmatched", "cik"]


def test_resolve_securities_counts_conflicts_and_lands_on_the_primary_claimant():
    built = _build(_reuse_rows())
    out = S.resolve_securities(_events([(None, "REUSE", "2015-07-01"), (None, "REUSE", "2010-01-01")]), _identifiers_frame(built))
    # 2015-07: both CIKs claim it (flagged); the evidence-backed primary (CIK 1) wins, not the newer claimant (CIK 2).
    assert list(out["security_id"]) == ["sm_0000000001", "sm_0000000001"]
    assert list(out["security_conflict"]) == [True, False]  # 2010 is outside the contested period


def test_multi_ticker_filings_resolve_through_each_ticker():
    built = _build([("a1", "2012-01-03", "200", "ISCA, ISCB")])
    out = S.resolve_securities(_events([(None, "ISCA", "2013-01-01"), (None, "ISCB", "2013-01-01")]), _identifiers_frame(built))
    assert list(out["security_id"]) == ["sm_0000000200", "sm_0000000200"]


# --- loader: plan against an existing (Technology) seed -------------------------------------


def _tech_existing():
    """What the GD1 Technology seed left behind for AAPL (CIK 320193), dated at its seed day."""
    return loader.build_existing(
        [("sm_0000320193", 320193), ("sm_tkr_CFLT", None)],
        [("sm_0000320193", "cik", "320193", "2026-09-28", None),
         ("sm_0000320193", "ticker", "AAPL", "2026-09-28", None),
         ("sm_tkr_CFLT", "ticker", "CFLT", "2026-09-28", None)],
    )


def _aapl_seed(tmp_path):
    built = _build([
        ("a1", "2006-02-02", "320193", "AAPL"),
        ("a2", "2020-02-02", "320193", "AAPL"),
        ("c1", "2019-02-02", "1699838", "CFLT"),
    ], {320193: {"name": "Apple Inc.", "tickers": ["AAPL"]}})
    _artifact(tmp_path, "tech", built)
    return loader.load_seed(tmp_path / "tech", require_receipt=True)


def test_existing_technology_rows_are_kept_and_never_rewritten(tmp_path):
    plan = loader.plan_inserts(_aapl_seed(tmp_path), _tech_existing())
    assert [r["entity_id"] for r in plan.sm_inserts] == ["sm_0001699838"]  # AAPL's entity already exists
    assert plan.skipped["security_master:entity_exists"] == 1
    # The seed's own CIK row (valid_from 2026-09-28) means the entity is already identified by that CIK.
    assert plan.skipped["security_identifiers:cik_already_identified"] == 1
    inserted = {(r["entity_id"], r["id_scheme"], r["id_value"], r["valid_from"]) for r in plan.si_inserts}
    # AAPL gets the earlier-dated ticker row (a different valid_from, so not a duplicate key) ...
    assert ("sm_0000320193", "ticker", "AAPL", "2006-02-02") in inserted
    # ... but nothing for the key the seed already holds.
    assert not any(k[2] == "AAPL" and k[3] == "2026-09-28" for k in inserted)


def test_exact_duplicate_keys_are_skipped():
    seed = loader.Seed(
        [{"entity_id": "sm_0000000001", "cik": 1, "name": "A", "security_type": "equity", "is_active": True,
          "delisted_at": None, "delisted_reason": None, "delisted_basis": None, "sic": None, "source": "all_issuers_v1",
          "provenance": {}}],
        [{"entity_id": "sm_0000000001", "id_scheme": "ticker", "id_value": "AAA", "valid_from": "2010-01-01",
          "valid_to": None, "is_primary": True, "source": "all_issuers_v1:sec_form345", "conflict_flag": False,
          "conflict_detail": None}],
        "x")
    ex = loader.build_existing([("sm_0000000001", 1)], [("sm_0000000001", "ticker", "AAA", "2010-01-01", None)])
    plan = loader.plan_inserts(seed, ex)
    assert plan.sm_inserts == [] and plan.si_inserts == []
    assert plan.skipped["security_identifiers:exists"] == 1


def test_new_ticker_overlapping_another_entitys_existing_row_is_demoted_only_inside_the_overlap(tmp_path):
    plan = loader.plan_inserts(_aapl_seed(tmp_path), _tech_existing())
    cflt = sorted((r for r in plan.si_inserts if r["id_value"] == "CFLT"), key=lambda r: r["valid_from"])
    assert [(r["valid_from"], r["valid_to"], r["is_primary"], r["conflict_flag"]) for r in cflt] == [
        ("2019-02-02", "2026-09-27", True, False),   # nobody else holds CFLT here: the builder's flags stand
        ("2026-09-28", None, False, True),           # the existing sm_tkr_CFLT row wins only from its own start
    ]
    assert cflt[1]["conflict_detail"]["existing_db_entities"] == ["sm_tkr_CFLT"]
    summary = plan.summary()
    assert summary["new_ticker_rows_overlapping_existing_other_entity"] == 1
    (review,) = plan.db_ticker_overlaps
    assert review["entity_id"] == "sm_0001699838" and review["new_window"] == ["2019-02-02", None]
    assert review["demoted_segments"] == [["2026-09-28", None, ["sm_tkr_CFLT"]]]


def _ticker_row(entity, vf, vt, **kw):
    return {"entity_id": entity, "id_scheme": "ticker", "id_value": "MID", "valid_from": vf, "valid_to": vt, "is_primary": True,
            "source": "all_issuers_v1:sec_form345", "conflict_flag": False, "conflict_detail": None, **kw}


def test_a_new_row_is_split_into_before_overlap_after_when_an_existing_window_sits_inside_it():
    ex = loader.build_existing([("sm_old", None)], [("sm_old", "ticker", "MID", "2012-01-01", "2013-12-31"),
                                                    ("sm_old", "ticker", "MID", "2015-01-01", "2015-06-30")])
    plan = loader.Plan()
    pieces = loader._split_against_existing(_ticker_row("sm_new", "2010-01-01", "2020-12-31"), ex, plan)
    assert [(p["valid_from"], p["valid_to"], p["is_primary"], p["conflict_flag"]) for p in pieces] == [
        ("2010-01-01", "2011-12-31", True, False), ("2012-01-01", "2013-12-31", False, True),
        ("2014-01-01", "2014-12-31", True, False), ("2015-01-01", "2015-06-30", False, True),
        ("2015-07-01", "2020-12-31", True, False)]
    keys = {(p["entity_id"], p["id_value"], p["valid_from"]) for p in pieces}
    assert len(keys) == len(pieces)  # the unique key (entity, scheme, value, valid_from) never collides


def test_a_new_row_with_no_overlap_or_overlapping_only_its_own_entity_is_untouched():
    ex = loader.build_existing([("sm_old", None)], [("sm_old", "ticker", "MID", "2005-01-01", "2009-12-31"),
                                                    ("sm_new", "ticker", "MID", "2010-01-01", None)])
    row = _ticker_row("sm_new", "2010-06-01", None)
    plan = loader.Plan()
    assert loader._split_against_existing(row, ex, plan) == [row] and plan.db_ticker_overlaps == []


def test_the_overlap_split_is_idempotent_against_the_database_after_an_apply(tmp_path):
    seed = _aapl_seed(tmp_path)
    first = loader.plan_inserts(seed, _tech_existing())
    existing = loader.build_existing(
        [("sm_0000320193", 320193), ("sm_tkr_CFLT", None)] + [(r["entity_id"], r["cik"]) for r in first.sm_inserts],
        [("sm_0000320193", "cik", "320193", "2026-09-28", None), ("sm_0000320193", "ticker", "AAPL", "2026-09-28", None),
         ("sm_tkr_CFLT", "ticker", "CFLT", "2026-09-28", None)]
        + [(r["entity_id"], r["id_scheme"], r["id_value"], r["valid_from"], r["valid_to"]) for r in first.si_inserts])
    again = loader.plan_inserts(seed, existing)
    assert again.sm_inserts == [] and again.si_inserts == [] and again.db_ticker_overlaps == []


def test_the_dry_run_receipt_lists_every_overlap_for_review(tmp_path, monkeypatch):
    art, built = _written_artifact(tmp_path)
    existing = loader.build_existing([("sm_tkr_REUSE", None)], [("sm_tkr_REUSE", "ticker", "REUSE", "2010-01-01", None)])
    monkeypatch.setattr(loader, "read_existing", lambda *_a, **_k: (existing, {"security_master": 1, "security_identifiers": 1}))
    rec = loader.run(_args(art, db_url="postgresql://u@h/db"), now=lambda: _utc(12), engine_factory=lambda _u: _FakeEngine())
    review = rec["ticker_overlaps_for_review"]
    assert review and all(r["ticker"] == "REUSE" and r["demoted_segments"] for r in review)
    assert rec["plan"]["new_ticker_rows_overlapping_existing_other_entity"] == len(review)


def test_cik_held_by_a_different_entity_blocks_the_new_entity_and_its_identifiers():
    seed = loader.Seed(
        [{"entity_id": "sm_0000000009", "cik": 9, "name": "N", "security_type": "equity", "is_active": True,
          "delisted_at": None, "delisted_reason": None, "delisted_basis": None, "sic": None, "source": "all_issuers_v1",
          "provenance": {}}],
        [{"entity_id": "sm_0000000009", "id_scheme": "cik", "id_value": "9", "valid_from": "2010-01-01", "valid_to": None,
          "is_primary": True, "source": "all_issuers_v1:sec_form345", "conflict_flag": False, "conflict_detail": None}],
        "x")
    plan = loader.plan_inserts(seed, loader.build_existing([("sm_tkr_NINE", 9)], []))
    assert plan.sm_inserts == [] and plan.si_inserts == []  # no FK failure, no orphan
    assert plan.cik_collisions == [{"entity_id": "sm_0000000009", "cik": 9, "existing_entity_id": "sm_tkr_NINE"}]


# --- loader: window, dry run, apply ---------------------------------------------------------


def _utc(h, m=0):
    return datetime(2026, 10, 2, h, m, tzinfo=timezone.utc)


@pytest.mark.parametrize("h, m, blocked", [(3, 29, False), (3, 30, True), (7, 0, True), (10, 29, True), (10, 30, False), (23, 59, False)])
def test_backup_window_is_03_30_to_10_30_utc(h, m, blocked):
    assert loader.in_blocked_window(_utc(h, m)) is blocked
    if blocked:
        with pytest.raises(loader.WindowRefused):
            loader.check_window(_utc(h, m))
    else:
        loader.check_window(_utc(h, m))


class _FakeResult:
    def __init__(self, rowcount=0, rows=None):
        self.rowcount = rowcount
        self._rows = rows or []

    def __iter__(self):
        return iter(self._rows)

    def scalar(self):
        return 0


class _FakeConn:
    def __init__(self, log):
        self.log = log

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.log.append((sql, params))
        if "jsonb_to_recordset" in sql:
            return _FakeResult(rowcount=len(json.loads(params["payload"])))
        return _FakeResult()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def begin(self):
        return self


class _FakeEngine:
    def __init__(self):
        self.log = []
        self.disposed = False

    def begin(self):
        return _FakeConn(self.log)

    connect = begin

    def dispose(self):
        self.disposed = True


_REAL_CODE_IDENTITY = loader.code_identity
CLEAN_CODE = {"git_head": "c0ffee", "dirty": False, "loader_file_sha256_lf": "0" * 64}


@pytest.fixture(autouse=True)
def _clean_checkout(monkeypatch):
    monkeypatch.setattr(loader, "code_identity", lambda: dict(CLEAN_CODE))


def _args(seed_dir, **kw):
    base = dict(seed_dir=seed_dir, apply=False, db_url=None, expect_output_sha256=None, batch_size=2,
                statement_timeout_ms=60000, lock_timeout_ms=5000, receipt=None, allow_dirty=False)
    base.update(kw)
    if base["apply"] and base["expect_output_sha256"] is None and (Path(seed_dir) / "receipt.json").exists():
        base["expect_output_sha256"] = json.loads((Path(seed_dir) / "receipt.json").read_text(encoding="utf-8"))["output"]["sha256"]
    return SimpleNamespace(**base)


def _written_artifact(tmp_path):
    built = _build(_reuse_rows())
    _artifact(tmp_path, "art", built)
    return tmp_path / "art", built


def test_dry_run_without_db_url_counts_the_artifact_and_opens_no_connection(tmp_path):
    art, built = _written_artifact(tmp_path)

    def boom(_url):
        raise AssertionError("a dry run without --db-url must not connect")

    rec = loader.run(_args(art), now=lambda: _utc(12), engine_factory=boom)
    assert rec["status"] == "counted_artifact_only" and rec["writes_to_database"] is False
    assert rec["plan"]["security_master_to_insert"] == len(built["security_master"])
    assert "DELETE FROM security_master WHERE source = 'all_issuers_v1'" in rec["rollback_sql"]
    assert Path(rec["receipt_path"]).exists()


def test_apply_refuses_inside_the_backup_window_before_connecting(tmp_path):
    art, _ = _written_artifact(tmp_path)
    with pytest.raises(loader.WindowRefused):
        loader.run(_args(art, apply=True, db_url="postgresql://u:p@h/db"), now=lambda: _utc(5),
                   engine_factory=lambda _u: pytest.fail("connected inside the window"))
    with pytest.raises(loader.WindowRefused):  # a read-only dry run with --db-url is a database read too
        loader.run(_args(art, db_url="postgresql://u:p@h/db"), now=lambda: _utc(4),
                   engine_factory=lambda _u: pytest.fail("connected inside the window"))


def test_db_url_env_is_read_from_the_environment_not_argv(tmp_path, monkeypatch):
    art, _ = _written_artifact(tmp_path)
    monkeypatch.setenv("SM_TEST_URL", "postgresql://u:secret@dbhost:5432/griddb")
    monkeypatch.setattr(loader, "read_existing", lambda *_a, **_k: (loader.Existing(), {"security_master": 7, "security_identifiers": 9}))
    seen = []
    rec = loader.run(_args(art, db_url_env="SM_TEST_URL"), now=lambda: _utc(12), engine_factory=lambda url: seen.append(url) or _FakeEngine())
    assert seen == ["postgresql://u:secret@dbhost:5432/griddb"]
    assert rec["status"] == "dry_run_complete" and rec["before_counts"] == {"security_master": 7, "security_identifiers": 9}
    assert "secret" not in json.dumps(rec)


def test_apply_requires_the_receipt_and_a_matching_hash(tmp_path):
    art, _ = _written_artifact(tmp_path)
    with pytest.raises(ValueError, match="--expect-output-sha256"):
        loader.load_seed(art, expect_sha256="0" * 64)
    (art / builder.SEED_FILE).write_text((art / builder.SEED_FILE).read_text(encoding="utf-8") + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="does not match receipt"):
        loader.load_seed(art)
    (art / "receipt.json").unlink()
    with pytest.raises(ValueError, match="receipt.json is missing"):
        loader.load_seed(art, require_receipt=True)


def test_apply_batches_insert_on_conflict_do_nothing_with_timeouts_and_never_updates(tmp_path, monkeypatch):
    art, built = _written_artifact(tmp_path)
    engine = _FakeEngine()
    monkeypatch.setattr(loader, "read_existing", lambda *_a, **_k: (loader.Existing(), {"security_master": 0, "security_identifiers": 0}))
    rec = loader.run(_args(art, apply=True, db_url="postgresql://u:secret@dbhost:5432/griddb"), now=lambda: _utc(12),
                     engine_factory=lambda _u: engine)
    assert rec["status"] == "applied" and rec["writes_to_database"] is True and engine.disposed
    assert rec["inserted"] == {"security_master": len(built["security_master"]), "security_identifiers": len(built["security_identifiers"])}
    assert rec["target"] == {"host": "dbhost", "port": 5432, "database": "griddb"}
    assert "secret" not in json.dumps(rec)
    statements = [sql for sql, _ in engine.log]
    inserts = [s for s in statements if "INSERT INTO" in s]
    assert inserts and all("ON CONFLICT" in s and "DO NOTHING" in s for s in inserts)
    assert not [s for s in statements if re.search(r"\b(UPDATE|DELETE|TRUNCATE|DROP|ALTER)\b", s, re.I)]
    keys = [p["k"] for s, p in engine.log if "set_config" in s]
    assert keys.count("statement_timeout") == keys.count("lock_timeout") == len(inserts)
    sizes = [len(json.loads(p["payload"])) for s, p in engine.log if "jsonb_to_recordset" in s]
    assert max(sizes) <= 2  # --batch-size honoured


def test_apply_stops_cleanly_when_the_backup_window_starts_mid_run(tmp_path, monkeypatch):
    art, _ = _written_artifact(tmp_path)
    engine = _FakeEngine()
    monkeypatch.setattr(loader, "read_existing", lambda *_a, **_k: (loader.Existing(), {"security_master": 0, "security_identifiers": 0}))
    clock = iter([_utc(3, 0), _utc(3, 5), _utc(3, 10), _utc(3, 31), _utc(3, 32), _utc(3, 33), _utc(3, 34), _utc(3, 35)] + [_utc(3, 36)] * 50)
    rec = loader.run(_args(art, apply=True, db_url="postgresql://u@h/db"), now=lambda: next(clock), engine_factory=lambda _u: engine)
    assert rec["status"] == "stopped_backup_window"
    assert json.loads(Path(rec["receipt_path"]).read_text(encoding="utf-8"))["status"] == "stopped_backup_window"


# --- match-rate harness ---------------------------------------------------------------------


def _sec_row(**over):
    row = dict(
        accession_number="0001214156-26-000001", document_type="4", amended=False, filing_date="2026-04-03",
        issuer_cik="0000000100", issuer_ticker="NEW", owner_cik="0001214156", owner_name="DOE JANE",
        is_director=False, is_officer=True, is_ten_pct_owner=False, nonderiv_trans_sk="1",
        transaction_date="2026-04-01", transaction_date_raw="01-APR-2026", transaction_code="P",
        shares=1000.0, price_per_share=20.0, acquired_disposed_code="A",
    )
    row.update(over)
    return row


def test_match_rate_harness_reports_full_ticker_only_and_agreement():
    from scripts import security_master_match_rate as mr

    built = _build([
        ("a1", "2015-03-02", "100", "OLD"),
        ("a2", "2018-01-15", "100", "NEW"),
        ("a3", "2026-04-03", "100", "NEW"),
        ("z1", "2016-01-04", "200", "OTHER"),
    ])
    ids = _identifiers_frame(built)
    form345 = pd.DataFrame([
        _sec_row(accession_number="0001-26-1"),                                              # CIK 100, ticker NEW, in window
        _sec_row(accession_number="0001-26-2", issuer_cik="0000000300", issuer_ticker="NEW", shares=2000.0, owner_cik="0000000777"),  # unknown CIK, ticker NEW: ticker only
        _sec_row(accession_number="0001-26-3", issuer_cik="0000000400", issuer_ticker="ZZZ", shares=3000.0, owner_cik="0000000888"),  # unknown to the artifact
    ])
    report = mr.run(form345, ids, baseline=ids.iloc[0:0])
    after, before = report["after"], report["before"]
    assert after["full"]["issuer_events"] == 3 and after["full"]["matched"] == 2
    assert after["full"]["by_basis"]["cik"] == 1 and after["full"]["by_basis"]["ticker"] == 1
    assert after["ticker_only"]["matched"] == 2          # NEW resolves by ticker alone, inside its open window
    assert after["agreement"] == {"events_resolved_by_cik_and_by_ticker": 1, "same_entity": 1, "agreement_rate": 1.0}
    assert before["full"]["matched"] == 0 and before["full"]["match_rate"] == 0.0


# --- loader: apply guards, code identity, window labelling ---------------------------------


def test_apply_requires_an_expected_artifact_hash(tmp_path):
    art, _ = _written_artifact(tmp_path)
    args = _args(art, apply=True, db_url="postgresql://u@h/db")
    args.expect_output_sha256 = None
    with pytest.raises(ValueError, match="--apply requires --expect-output-sha256"):
        loader.run(args, now=lambda: _utc(12), engine_factory=lambda _u: pytest.fail("connected without a pinned hash"))
    with pytest.raises(SystemExit):  # the CLI refuses earlier, at argument parsing
        loader.main(["--seed-dir", str(art), "--apply", "--db-url", "postgresql://u@h/db"])


def test_apply_refuses_a_dirty_tree_unless_allowed_and_records_the_loader_code(tmp_path, monkeypatch):
    art, _ = _written_artifact(tmp_path)
    monkeypatch.setattr(loader, "code_identity", lambda: {**CLEAN_CODE, "dirty": True})
    monkeypatch.setattr(loader, "read_existing", lambda *_a, **_k: (loader.Existing(), {"security_master": 0, "security_identifiers": 0}))
    with pytest.raises(ValueError, match="uncommitted changes"):
        loader.run(_args(art, apply=True, db_url="postgresql://u@h/db"), now=lambda: _utc(12), engine_factory=lambda _u: _FakeEngine())
    rec = loader.run(_args(art, apply=True, db_url="postgresql://u@h/db", allow_dirty=True), now=lambda: _utc(12),
                     engine_factory=lambda _u: _FakeEngine())
    assert rec["loader_code"]["dirty"] is True and rec["status"] == "applied"
    dry = loader.run(_args(art), now=lambda: _utc(12))  # a dry run never needs a clean tree
    assert dry["loader_code"]["git_head"] == "c0ffee"


def _lf_sha(path):
    import hashlib

    data = Path(path).read_bytes()
    return hashlib.sha256(data.replace(bytes([13, 10]), bytes([10]))).hexdigest()


def test_code_identity_hashes_the_loader_with_lf_line_endings():
    ident = _REAL_CODE_IDENTITY()
    assert ident["loader_file_sha256_lf"] == _lf_sha(loader.__file__)
    assert ident["dirty"] in (True, False, None) and (ident["git_head"] is None or len(ident["git_head"]) == 40)
    assert builder.sha256_text_file(Path(builder.__file__)) == _lf_sha(builder.__file__)



def test_a_batch_that_would_cross_into_the_window_is_not_started(tmp_path):
    art, _ = _written_artifact(tmp_path)
    seed = loader.load_seed(art, require_receipt=True)
    plan = loader.plan_inserts(seed, loader.Existing())
    engine = _FakeEngine()
    # 03:29:45 plus the 30 s first-batch estimate is 03:30:15: refused before any statement is sent.
    with pytest.raises(loader.WindowRefused):
        loader.apply_plan(engine, plan, now=lambda: datetime(2026, 10, 2, 3, 29, 45, tzinfo=timezone.utc))
    assert engine.log == []
    # A smaller estimate lets the same batch start.
    loader.apply_plan(engine, plan, now=lambda: datetime(2026, 10, 2, 3, 29, 45, tzinfo=timezone.utc), batch_estimate_s=5)
    assert engine.log


def test_a_run_that_finished_before_the_window_is_labelled_applied_not_stopped(tmp_path, monkeypatch):
    art, built = _written_artifact(tmp_path)
    engine = _FakeEngine()
    calls = {"n": 0}

    def reads(*_a, **kw):
        calls["n"] += 1
        if calls["n"] == 2:  # the post-apply count runs after 03:30
            raise loader.WindowRefused("window started")
        return loader.Existing(), {"security_master": 0, "security_identifiers": 0}

    monkeypatch.setattr(loader, "read_existing", reads)
    clock = iter([_utc(3, 0)] * 100)
    rec = loader.run(_args(art, apply=True, db_url="postgresql://u@h/db"), now=lambda: next(clock), engine_factory=lambda _u: engine)
    assert rec["status"] == "applied" and rec["after_counts"] is None and "re-run the dry run after 10:30Z".lower() in rec["note"].lower()
    assert rec["inserted"]["security_master"] == len(built["security_master"])
