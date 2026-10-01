"""Tests for intelligence/people_events_pipeline (GD2 materializer library + GD3 planner).

Pure fixtures, no database, no network. The E1-style gates at the bottom
(append-future determinism, planted look-ahead canaries, reproducibility)
are the people-events counterparts of ``evals/e1``'s gates.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from intelligence.people_events_pipeline import adapters as A
from intelligence.people_events_pipeline import dryrun as D
from intelligence.people_events_pipeline import merge as M
from intelligence.people_events_pipeline import plan as P
from intelligence.people_events_pipeline import readonly as RO
from intelligence.people_events_pipeline import rules as R
from intelligence.people_events_pipeline import security as S

UTC = timezone.utc
OBSERVED = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


# --- fixtures ------------------------------------------------------------------------------


def sec_row(**over):
    row = dict(
        accession_number="0001214156-26-000001", document_type="4", amended=False, filing_date="2026-04-03",
        issuer_cik="0000320193", issuer_ticker="AAPL", owner_cik="0001214156", owner_name="COOK TIMOTHY D",
        is_director=False, is_officer=True, is_ten_pct_owner=False, nonderiv_trans_sk="1",
        transaction_date="2026-04-01", transaction_date_raw="01-APR-2026", transaction_code="P",
        shares=1000.0, price_per_share=200.0, acquired_disposed_code="A",
    )
    row.update(over)
    return row


def sig(id_, source_type, ticker, signal_date, payload, created_at="2026-04-03T00:00:00Z", source_id="x",
        signal_type="BUY"):
    return dict(id=id_, source_type=source_type, source_id=source_id, ticker=ticker, signal_date=signal_date,
                signal_type=signal_type, signal_value=payload, created_at=created_at)


def qq_insider(id_=10, **payload):
    p = {"Name": "Timothy D. Cook", "TransactionCode": "P", "Shares": 1000, "PricePerShare": 200,
         "fileDate": "2026-04-03"}
    p.update(payload)
    return sig(id_, "quiverquant:insider", "AAPL", "2026-04-01", p, source_id="qq_insider_trading",
               signal_type="insider_buy")


def frame(rows):
    return pd.DataFrame(rows)


def run(form345=None, signal_sources=None, holdings=None):
    parts = []
    if form345 is not None:
        parts.append(A.form4_from_form345(frame(form345))[0])
    if signal_sources is not None:
        parts.append(A.from_signal_sources(frame(signal_sources), OBSERVED)[0])
    if holdings is not None:
        parts.append(A.thirteen_f_changes(frame(holdings))[0])
    parts = [p for p in parts if not p.empty]
    cands = pd.concat(parts, ignore_index=True) if parts else A.empty_candidates()
    return M.merge_candidates(cands)


# --- rules ---------------------------------------------------------------------------------


class TestKnownAtRules:
    def test_section16_cutoff_is_22h_new_york_in_both_dst_regimes(self):
        assert R.section16_filing_known_at(date(2026, 4, 3)) == datetime(2026, 4, 4, 2, 0, tzinfo=UTC)  # EDT
        assert R.section16_filing_known_at(date(2026, 1, 9)) == datetime(2026, 1, 10, 3, 0, tzinfo=UTC)  # EST

    def test_section16_series_matches_scalar_and_vs1(self):
        dates = pd.Series(pd.to_datetime(["2026-04-03", "2026-01-09", "2026-03-08", "2026-11-01"]))
        got = R.section16_filing_known_at_series(dates)
        for d, g in zip(dates, got):
            assert g.to_pydatetime() == R.section16_filing_known_at(d.date())
        from analysis.panel_insider_density import filing_known_at

        assert list(filing_known_at(dates)) == list(got)

    def test_next_session_open_skips_weekend_and_holiday_and_is_dst_aware(self):
        assert R.next_session_open_after(date(2026, 4, 3)) == datetime(2026, 4, 6, 13, 30, tzinfo=UTC)  # Fri->Mon
        assert R.next_session_open_after(date(2026, 1, 9)) == datetime(2026, 1, 12, 14, 30, tzinfo=UTC)  # EST
        # 2026-07-03 is the observed Independence Day holiday.
        assert R.next_session_open_after(date(2026, 7, 2)).date() == date(2026, 7, 6)

    def test_next_session_series_keeps_nat(self):
        s = R.next_session_open_after_series(pd.Series([pd.Timestamp("2026-04-03"), pd.NaT]))
        assert s.iloc[0] == pd.Timestamp("2026-04-06 13:30", tz="UTC") and pd.isna(s.iloc[1])


class TestNormalization:
    @pytest.mark.parametrize("a,b", [
        ("COOK TIMOTHY D", "Timothy D. Cook"), ("Cook, Timothy", "TIMOTHY D COOK"),
        ("Hon. Nancy Pelosi", "Pelosi Nancy"), ("Smith John Jr.", "John Smith"),
    ])
    def test_name_tokens_fold_order_initials_and_honorifics(self, a, b):
        assert R.name_tokens(a) == R.name_tokens(b) != ""

    @pytest.mark.parametrize("raw,want", [("brk.b", "BRKB"), ("BRK-B", "BRKB"), ("NONE", None), ("", None),
                                          (None, None), ("N/A", None), ("TOOLONGTICKER1", None)])
    def test_normalize_ticker(self, raw, want):
        assert R.normalize_ticker(raw) == want

    def test_amount_band_and_congress_direction(self):
        assert R.amount_band_low("$1,001 - $15,000") == "1001"
        assert R.amount_band_low(None) == "NA"
        assert R.congress_direction("Sale (Partial)") == "sell"
        assert R.congress_direction("Purchase") == "buy"
        assert R.congress_direction("Exchange") is None

    def test_form4_direction_never_turns_exercise_into_buy(self):
        assert R.form4_direction("M", "A") is None
        assert R.form4_direction("P", "D") == "buy"
        assert R.form4_direction(None, "D") == "sell"

    @pytest.mark.parametrize("basis,actor,matched,channel,want", [
        ("filing", "owner_cik", True, "form4", "high"),
        ("filing", "normalized_name", True, "form4", "medium"),
        ("filing", "owner_cik", False, "form4", "medium"),
        ("first_seen", "owner_cik", True, "form4", "low"),
        ("filing", "agency_code", True, "gov_contract_qq_aggregate", "low"),
    ])
    def test_confidence_rubric(self, basis, actor, matched, channel, want):
        assert R.confidence(basis, actor, matched, channel) == want


# --- adapters ------------------------------------------------------------------------------


class TestForm345Adapter:
    def test_joint_filers_fold_to_one_act_with_lowest_cik_as_actor(self):
        rows = [sec_row(owner_cik="0000000200", owner_name="FUND LP"),
                sec_row(owner_cik="0000000100", owner_name="FUND GP LLC")]
        cands, skips = A.form4_from_form345(frame(rows))
        assert len(cands) == 1
        assert cands.loc[0, "actor_id"] == "0000000100"
        assert cands.loc[0, "co_actor_ids"] == "0000000200"
        assert skips["sec_form345:joint_filer_rows_folded"] == 1

    def test_vectorized_key_equals_scalar_key(self):
        cands, _ = A.form4_from_form345(frame([sec_row()]))
        want = R.form4_dedup_key("t:AAPL", R.name_tokens("COOK TIMOTHY D"), date(2026, 4, 1), "P", 1000.0)
        assert cands.loc[0, "dedup_key"] == want
        assert cands.loc[0, "known_at"] == pd.Timestamp("2026-04-04 02:00", tz="UTC")
        assert cands.loc[0, "known_at_basis"] == "filing"

    def test_transaction_after_filing_is_dropped_not_kept(self):
        cands, skips = A.form4_from_form345(frame([sec_row(transaction_date="2026-04-05")]))
        assert cands.empty and skips["sec_form345:transaction_after_filing"] == 1

    def test_two_same_size_lines_in_one_filing_stay_two_acts(self):
        rows = [sec_row(nonderiv_trans_sk="1", price_per_share=200.0),
                sec_row(nonderiv_trans_sk="2", price_per_share=201.0)]
        cands, skips = A.form4_from_form345(frame(rows))
        assert cands["dedup_key"].nunique() == 2
        assert skips["sec_form345:same_accession_same_key_numbered"] == 1

    def test_amendment_repeating_a_line_merges_and_keeps_original_known_at(self):
        rows = [sec_row(), sec_row(accession_number="0001214156-26-000009", document_type="4/A", amended=True,
                                   filing_date="2026-04-20", nonderiv_trans_sk="9")]
        ev = run(form345=rows).events
        assert len(ev) == 1
        assert ev.loc[0, "known_at"] == pd.Timestamp("2026-04-04 02:00", tz="UTC")
        assert ev.loc[0, "n_sources"] == 1 and ev.loc[0, "n_source_rows"] == 2

    def test_two_owner_ciks_with_alike_names_never_merge(self):
        rows = [sec_row(owner_cik="0000000101", owner_name="SMITH JOHN A"),
                sec_row(accession_number="a2", owner_cik="0000000102", owner_name="SMITH JOHN B")]
        cands, skips = A.form4_from_form345(frame(rows))
        assert cands["dedup_key"].nunique() == 2
        assert skips["sec_form345:owner_name_collision_split"] == 2

    def test_ticker_missing_falls_back_to_issuer_cik(self):
        cands, _ = A.form4_from_form345(frame([sec_row(issuer_ticker="NONE")]))
        assert "|c:320193|" in cands.loc[0, "dedup_key"]

    def test_non_transaction_forms_are_counted(self):
        cands, skips = A.form4_from_form345(frame([sec_row(document_type="3")]))
        assert cands.empty and skips["sec_form345:not_a_transaction_form"] == 1


class TestSignalSourceAdapters:
    def test_qq_insider_merges_with_sec_and_known_at_is_min_bound(self):
        ev = run(form345=[sec_row()], signal_sources=[qq_insider(uploaded="2026-04-03T23:00:00Z")]).events
        assert len(ev) == 1 and ev.loc[0, "n_sources"] == 2
        assert ev.loc[0, "known_at"] == pd.Timestamp("2026-04-03 23:00", tz="UTC")
        assert ev.loc[0, "known_at_basis"] == "first_seen"
        assert ev.loc[0, "actor_id"] == "0001214156"  # descriptive fields from the SEC row

    def test_overwritable_source_never_uses_created_at(self):
        # A planted early created_at (the first payload's) must not become known_at.
        rec = qq_insider(fileDate=None)
        rec["created_at"] = "2026-04-01T00:00:00Z"
        cands, _ = A.from_signal_sources(frame([rec]), OBSERVED)
        assert cands.loc[0, "known_at"] == pd.Timestamp(OBSERVED)
        cands, skips = A.from_signal_sources(frame([rec]), None)
        assert cands.empty and skips["quiverquant:insider:no_known_at"] == 1

    def test_qq_filing_before_trade_is_dropped(self):
        cands, skips = A.from_signal_sources(frame([qq_insider(fileDate="2026-03-01")]), OBSERVED)
        assert cands.empty and skips["quiverquant:insider:filing_before_transaction"] == 1

    def test_edgar_native_cluster_and_derivative_rows_are_not_acts(self):
        rows = [
            sig(1, "insider", "AAPL", "2026-04-01", {"insider_count": 3}, signal_type="CLUSTER_BUY",
                source_id="cluster_aapl"),
            sig(2, "insider", "AAPL", "2026-04-01", {"is_derivative": True, "transaction_code": "M"},
                source_id="COOK TIMOTHY D"),
            sig(3, "insider", "AAPL", "2026-04-01", {"transaction_code": "P", "shares": 1000, "price": 200,
                                                     "filing_date": "2026-04-03", "accession": "acc"},
                source_id="COOK TIMOTHY D", signal_type="UNUSUAL_BUY"),
        ]
        cands, skips = A.from_signal_sources(frame(rows), OBSERVED)
        assert len(cands) == 1 and cands.loc[0, "direction"] == "buy"
        assert skips["insider:derived_cluster_row"] == 1 and skips["insider:derivative_line"] == 1
        assert cands.loc[0, "known_at"] == pd.Timestamp("2026-04-03 00:00", tz="UTC")  # created_at is earlier

    def test_congress_native_statutory_bound_is_not_a_known_at(self):
        payload = {"disclosure_date": "2026-05-16", "disclosure_basis": "statutory_bound",
                   "amount_range": "$1,001 - $15,000", "chamber": "house"}
        rec = sig(1, "congressional", "MSFT", "2026-04-01", payload, created_at="2026-06-20T10:00:00Z",
                  source_id="Nancy Pelosi")
        cands, skips = A.from_signal_sources(frame([rec]), OBSERVED)
        assert cands.loc[0, "known_at"] == pd.Timestamp("2026-06-20 10:00", tz="UTC")
        assert cands.loc[0, "known_at_basis"] == "first_seen"
        assert skips["congressional:statutory_bound_not_used"] == 1

    def test_congress_qq_and_native_merge(self):
        native = sig(1, "congressional", "MSFT", "2026-04-01",
                     {"disclosure_date": "2026-04-20", "disclosure_basis": "reported", "amount_range": "$1,001 - $15,000"},
                     created_at="2026-04-25T00:00:00Z", source_id="Nancy Pelosi")
        qq = sig(2, "quiverquant:house", "MSFT", "2026-04-20",
                 {"Representative": "Nancy Pelosi", "BioGuideID": "P000197", "TransactionDate": "2026-04-01",
                  "Transaction": "Purchase", "Range": "$1,001 - $15,000", "ReportDate": "2026-04-20",
                  "last_modified": "2026-04-22T00:00:00Z"}, signal_type="house_trading")
        ev = run(signal_sources=[native, qq]).events
        assert len(ev) == 1 and ev.loc[0, "n_sources"] == 2
        assert ev.loc[0, "known_at"] == pd.Timestamp(R.next_session_open_after(date(2026, 4, 20)))

    def test_usaspending_without_action_date_is_dropped(self):
        rec = sig(1, "gov_contract", "LMT", "2026-04-01", {"award_id": "A1", "amount": 5e6,
                                                           "award_date_basis": "first_seen"})
        cands, skips = A.from_signal_sources(frame([rec]), OBSERVED)
        assert cands.empty and skips["gov_contract:no_action_date"] == 1

    def test_qq_gov_aggregate_uses_snapshot_day_only_for_pre_fix_rows(self):
        pre = sig(1, "quiverquant:gov_contracts", "LMT", "2026-05-02", {"Year": 2026, "Qtr": 2, "Amount": 1e6},
                  created_at="2026-05-02T09:00:00Z", signal_type="gov_contracts")
        post = sig(2, "quiverquant:gov_contracts", "LMT", "2026-06-30", {"Year": 2026, "Qtr": 2, "Amount": 1e6},
                   created_at="2026-04-01T09:00:00Z", signal_type="gov_contracts")
        cands, _ = A.from_signal_sources(frame([pre, post]), OBSERVED)
        by_id = dict(zip(cands["source_record_id"], cands["known_at"]))
        assert by_id["quiverquant:gov_contracts:1"] == pd.Timestamp(R.next_session_open_after(date(2026, 5, 2)))
        assert by_id["quiverquant:gov_contracts:2"] == pd.Timestamp(OBSERVED)  # post-fix rows are rewritten
        ev = M.merge_candidates(cands).events
        assert len(ev) == 1 and ev.loc[0, "event_date"] == date(2026, 4, 1)

    def test_fara_is_not_an_issuer_event(self):
        rec = sig(1, "foreign_lobbying", "XLE", "2026-04-01", {"principal_name": "X", "activity_type": "a"},
                  source_id="Some Registrant LLC")
        ev = run(signal_sources=[rec]).events
        res = S.resolve_securities(ev, pd.DataFrame())
        assert res.loc[0, "security_match_basis"] == "not_an_issuer"

    def test_unknown_source_type_is_counted(self):
        _, skips = A.from_signal_sources(frame([sig(1, "quiverquant:wsb", "GME", "2026-04-01", {})]), OBSERVED)
        assert skips["quiverquant:wsb:not_a_people_channel"] == 1


def holding(id_, cik, cusip, shares, report, filed):
    return dict(id=id_, cik=cik, holder_name="Big Fund LLC", ticker="AAPL", cusip=cusip, shares_held=shares,
                value_usd=shares * 200.0, report_date=report, filed_date=filed, source="sec_13f_live",
                created_at="2026-05-01")


class TestThirteenF:
    def test_changes_need_a_previous_filed_quarter(self):
        rows = [
            holding(1, "1001", "037833100", 100, "2025-12-31", "2026-02-10"),
            holding(2, "1001", "037833100", 150, "2026-03-31", "2026-05-12"),
            holding(3, "1001", "594918104", 10, "2025-12-31", "2026-02-10"),  # exits in Q1
            holding(4, "1001", "88160R101", 5, "2026-03-31", "2026-05-12"),  # new in Q1
        ]
        cands, skips = A.thirteen_f_changes(frame(rows))
        codes = dict(zip(cands["attrs"].map(lambda a: a["security"]), cands["transaction_code"]))
        assert codes == {"cusip:037833100": "INC", "cusip:594918104": "EXIT", "cusip:88160R101": "NEW"}
        assert skips["sec_13f:first_quarter_coverage_start"] == 2
        assert set(cands["known_at"]) == {pd.Timestamp(R.next_session_open_after(date(2026, 5, 12)))}

    def test_gap_and_missing_filed_date(self):
        rows = [holding(1, "1001", "X", 100, "2025-03-31", "2025-05-01"),
                holding(2, "1001", "X", 150, "2026-03-31", "2026-05-12"),
                holding(3, "1001", "Y", 1, "2026-03-31", None)]
        cands, skips = A.thirteen_f_changes(frame(rows))
        assert cands.empty
        assert skips["sec_13f:non_consecutive_quarter"] == 1 and skips["sec_13f:no_filed_date"] == 1


# --- merge, security -----------------------------------------------------------------------


class TestMerge:
    def test_order_independence(self):
        rows = [sec_row(), sec_row(nonderiv_trans_sk="2", transaction_code="S", shares=50.0),
                sec_row(accession_number="a2", owner_cik="0000000007", owner_name="DOE JANE")]
        sigs = [qq_insider(), qq_insider(id_=11, Name="Jane Doe", Shares=1000)]
        a = run(form345=rows, signal_sources=sigs).events
        b = run(form345=list(reversed(rows)), signal_sources=list(reversed(sigs))).events
        pd.testing.assert_frame_equal(a, b)

    def test_near_duplicate_needs_two_origins(self):
        lots = [sec_row(nonderiv_trans_sk="1", shares=100.0), sec_row(nonderiv_trans_sk="2", shares=300.0)]
        ev = run(form345=lots).events
        assert not ev["near_duplicate"].any()  # lots of one filing
        ev = run(form345=[sec_row()], signal_sources=[qq_insider(Shares=999)]).events
        assert ev["near_duplicate"].all() and len(ev) == 2  # flagged, never merged

    def test_link_echoes_only_points_backwards(self):
        ev = pd.DataFrame([
            {"channel": "form4", "dedup_key": "f", "entity_ticker": "AAPL", "known_at": pd.Timestamp("2026-04-04", tz="UTC")},
            {"channel": "news", "dedup_key": "n1", "entity_ticker": "AAPL", "known_at": pd.Timestamp("2026-04-05", tz="UTC")},
            {"channel": "news", "dedup_key": "n0", "entity_ticker": "AAPL", "known_at": pd.Timestamp("2026-04-03", tz="UTC")},
        ])
        echo = M.link_echoes(ev)
        assert echo.tolist() == [None, "f", None]


class TestSecurity:
    IDS = pd.DataFrame([
        {"entity_id": "sm_0000320193", "id_scheme": "cik", "id_value": "320193", "valid_from": "2026-09-27",
         "valid_to": None, "is_primary": True, "conflict_flag": False},
        {"entity_id": "sm_0000320193", "id_scheme": "ticker", "id_value": "AAPL", "valid_from": "2026-09-27",
         "valid_to": None, "is_primary": True, "conflict_flag": False},
    ])

    def test_cik_matches_history_but_ticker_respects_validity(self):
        ev = run(form345=[sec_row()], signal_sources=[qq_insider(id_=3, Name="Someone Else")]).events
        res = S.resolve_securities(ev, self.IDS)
        basis = dict(zip(res["source"], res["security_match_basis"]))
        assert basis == {"sec_form345": "cik", "quiverquant": "ticker_outside_validity"}
        assert res.loc[res["source"] == "quiverquant", "security_id"].isna().all()
        summary = S.match_summary(res)["form4"]
        assert summary["matched"] == 1 and summary["match_rate"] == 0.5

    def test_ticker_inside_validity_matches(self):
        ids = self.IDS.assign(valid_from="2020-01-01")
        ev = run(signal_sources=[qq_insider()]).events
        assert S.resolve_securities(ev, ids).loc[0, "security_id"] == "sm_0000320193"


# --- write plan ----------------------------------------------------------------------------


def _resolved(**kw):
    return S.resolve_securities(run(**kw).events, pd.DataFrame())


class TestWritePlan:
    def test_idempotent(self):
        ev = _resolved(form345=[sec_row()])
        plan1 = P.build_write_plan(ev, pd.DataFrame(), OBSERVED)
        stored = P.apply_in_memory(pd.DataFrame(), plan1, OBSERVED)
        plan2 = P.build_write_plan(ev, stored, OBSERVED)
        assert set(plan2["op"]) == {"unchanged"}
        assert len(P.apply_in_memory(stored, plan2, OBSERVED)) == len(stored)

    def test_new_source_adds_refs_and_tightens_known_at(self):
        stored = P.apply_in_memory(pd.DataFrame(), P.build_write_plan(_resolved(form345=[sec_row()]),
                                                                       pd.DataFrame(), OBSERVED), OBSERVED)
        ev = _resolved(form345=[sec_row()], signal_sources=[qq_insider(uploaded="2026-04-03T23:00:00Z")])
        plan = P.build_write_plan(ev, stored, OBSERVED)
        assert sorted(plan["op"]) == ["add_sources", "tighten_known_at"]
        after = P.apply_in_memory(stored, plan, OBSERVED)
        assert len(after) == 1 and len(after.loc[0, "source_refs"]) == 2
        assert after.loc[0, "known_at"] == pd.Timestamp("2026-04-03 23:00", tz="UTC")

    def test_content_change_supersedes_with_one_visible_version_at_every_instant(self):
        stored = P.apply_in_memory(pd.DataFrame(), P.build_write_plan(_resolved(form345=[sec_row()]),
                                                                       pd.DataFrame(), OBSERVED), OBSERVED)
        later = OBSERVED + timedelta(days=5)
        ev = _resolved(form345=[sec_row(price_per_share=250.0)])  # same key, new size_usd
        plan = P.build_write_plan(ev, stored, later)
        assert list(plan["op"]) == ["supersede"]
        after = P.apply_in_memory(stored, plan, later)
        assert len(after) == 2
        for t in pd.date_range("2026-04-04 03:00", "2026-10-10", freq="12h", tz="UTC"):
            assert len(P.visible_at(after, t)) == 1
        assert P.visible_at(after, OBSERVED).iloc[0]["content_hash"] == stored.loc[0, "content_hash"]
        assert P.visible_at(after, later).iloc[0]["content_hash"] == ev.loc[0, "content_hash"]

    def test_retract_only_for_complete_scope(self):
        stored = P.apply_in_memory(pd.DataFrame(), P.build_write_plan(_resolved(form345=[sec_row()]),
                                                                       pd.DataFrame(), OBSERVED), OBSERVED)
        other = _resolved(form345=[sec_row(owner_cik="0000000009", owner_name="NEW PERSON")])
        assert "retract" not in set(P.build_write_plan(other, stored, OBSERVED)["op"])
        plan = P.build_write_plan(other, stored, OBSERVED, complete_channels=["form4"])
        assert "retract" in set(plan["op"])
        after = P.apply_in_memory(stored, plan, OBSERVED)
        assert len(after) == 2  # the retracted row stays
        old_key = stored.loc[0, "dedup_key"]
        assert old_key in set(P.visible_at(after, OBSERVED - timedelta(days=1))["dedup_key"])  # PIT: still there
        assert old_key not in set(P.visible_at(after, OBSERVED)["dedup_key"])


# --- E1-style gates ------------------------------------------------------------------------


class TestLookAheadGates:
    def test_append_future_determinism(self):
        as_of = pd.Timestamp("2026-04-10", tz="UTC")
        past = [sec_row(), sec_row(accession_number="a2", owner_cik="7", owner_name="DOE JANE", filing_date="2026-04-06",
                                   transaction_date="2026-04-02")]
        future = [sec_row(accession_number="a3", owner_cik="8", owner_name="LATE FILER", filing_date="2026-05-20",
                          transaction_date="2026-04-02"),
                  sec_row(accession_number="a4", owner_cik="9", owner_name="NEXT ACT", filing_date="2026-04-14",
                          transaction_date="2026-04-13")]

        def visible(rows):
            ev = _resolved(form345=rows)
            stored = P.apply_in_memory(pd.DataFrame(), P.build_write_plan(ev, pd.DataFrame(), OBSERVED), OBSERVED)
            return P.visible_at(stored, as_of).sort_values("dedup_key").reset_index(drop=True)

        pd.testing.assert_frame_equal(visible(past), visible(past + future))

    def test_planted_leak_trips_the_pit_invariant(self):
        good = run(form345=[sec_row()]).events
        assert M.pit_violations(good, pd.Timestamp(OBSERVED)) == {
            "known_before_event": 0, "known_after_observation": 0, "missing_known_at": 0}
        leaky = good.copy()
        leaky["known_at"] = pd.to_datetime(leaky["event_date"]).dt.tz_localize("UTC") - pd.Timedelta(seconds=1)
        assert M.pit_violations(leaky)["known_before_event"] == 1
        future = good.copy()
        future["known_at"] = pd.Timestamp(OBSERVED) + pd.Timedelta(days=1)
        assert M.pit_violations(future, pd.Timestamp(OBSERVED))["known_after_observation"] == 1

    def test_planted_future_known_events_never_reach_a_density_read(self):
        # Events traded before t but filed after t: a reader keyed on event_date
        # would see them (the leak); the PIT read keyed on known_at must not.
        t = pd.Timestamp("2026-04-10 20:00", tz="UTC")
        names = ["ALPHA ANN", "BETA BOB", "GAMMA GUS", "DELTA DEE", "EPSILON EVE"]
        rows = [sec_row(accession_number=f"x{i}", owner_cik=str(100 + i), owner_name=names[i],
                        transaction_date="2026-04-01", filing_date="2026-04-20") for i in range(5)]
        ev = _resolved(form345=rows)
        stored = P.apply_in_memory(pd.DataFrame(), P.build_write_plan(ev, pd.DataFrame(), OBSERVED), OBSERVED)
        leaky_count = (pd.to_datetime(ev["event_date"]).dt.tz_localize("UTC") <= t).sum()
        assert leaky_count == 5  # the canary would trip a leaky reader...
        assert len(P.visible_at(stored, t)) == 0  # ...and the PIT read is clean

    def test_reproducible_report(self, tmp_path):
        kwargs = dict(form345=frame([sec_row(), sec_row(nonderiv_trans_sk="3", shares=7.0)]),
                      signal_sources=frame([qq_insider()]), holdings=None, identifiers=TestSecurity.IDS,
                      stored=None, observed_at=OBSERVED)
        a, b = D.run_dry_run(**kwargs), D.run_dry_run(**kwargs)
        a.pop("timings"), b.pop("timings")
        assert json.dumps(D.to_jsonable(a), sort_keys=True) == json.dumps(D.to_jsonable(b), sort_keys=True)


# --- read-only guards ----------------------------------------------------------------------


class TestReadOnlyGuards:
    @pytest.mark.parametrize("hhmm,ok", [("03:29", True), ("03:30", False), ("07:00", False), ("10:29", False),
                                         ("10:30", True), ("23:00", True)])
    def test_backup_window(self, hhmm, ok):
        h, m = map(int, hhmm.split(":"))
        now = datetime(2026, 10, 1, h, m, tzinfo=UTC)
        if ok:
            RO.assert_db_window_open(now)
        else:
            with pytest.raises(RO.WindowClosed):
                RO.assert_db_window_open(now)

    @pytest.mark.parametrize("sql", [
        "SELECT * FROM raw_series", "DELETE FROM people_events", "SELECT 1; SELECT 2",
        "SELECT * FROM insider_trades", "WITH x AS (UPDATE people_events SET n_sources = 1 RETURNING 1) SELECT 1",
        "UPDATE signal_sources SET x = 1",
    ])
    def test_guard_rejects(self, sql):
        with pytest.raises(ValueError):
            RO.guard_sql(sql)

    def test_module_queries_pass_the_guard(self):
        for name in dir(RO):
            if name.endswith("_SQL") and name.startswith("_"):
                RO.guard_sql(getattr(RO, name))


# --- CLI -----------------------------------------------------------------------------------


def test_cli_writes_report_and_refuses_overwrite(tmp_path):
    pytest.importorskip("pyarrow")
    from scripts import people_events_dry_run as cli

    pq_path = tmp_path / "nonderiv.parquet"
    frame([dict(sec_row(), quarter="2026q2"), dict(sec_row(nonderiv_trans_sk="2", shares=5.0), quarter="2026q2")]) \
        .to_parquet(pq_path)
    out = tmp_path / "report.json"
    assert cli.main(["--form345", str(pq_path), "--out", str(out), "--observed-at", "2026-10-01T12:00:00+00:00"]) == 0
    report = json.loads(out.read_text())
    assert report["mode"] == "dry_run_no_writes"
    assert report["channels"]["form4"]["events"] == 2
    assert report["would_write"]["form4"]["insert"] == 2
    assert cli.main(["--form345", str(pq_path), "--out", str(out)]) == 2
