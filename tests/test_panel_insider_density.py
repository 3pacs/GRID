"""Tests for the VS1 panel harness (``analysis/panel_insider_density.py``).

Synthetic data only: no production DB, no price or outcome of any real issuer.
Prices for the end-to-end test live in an in-memory SQLite ``raw_series``
shaped like production and are read through ``store.observations.read_window``.
"""

from __future__ import annotations

import functools
import json
import re
import sqlite3
import zipfile
from dataclasses import asdict, replace
from datetime import date, datetime, time, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

from analysis import panel_insider_density as vs1
from analysis.offline_research_proof import bh_adjusted, digest, holm_adjusted

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

UTC = timezone.utc
NOW = datetime(2026, 9, 27, 8, 0, tzinfo=UTC)
AS_OF_TS = "2026-09-26T00:00:00+00:00"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "vs1"


# --- pre-registration pin ---------------------------------------------------------------


def test_repository_preregistration_hashes_to_the_pinned_body_sha():
    assert vs1.check_prereg() == vs1.PREREG_BODY_SHA256
    assert len(vs1.PREREG_BODY_SHA256) == 64


def test_body_hash_ignores_line_endings_and_text_outside_the_markers(tmp_path):
    body = "\n## spec\nW = 90\n"
    a = tmp_path / "a.md"
    b = tmp_path / "b.md"
    a.write_bytes(f"intro\n{vs1.BODY_START}{body}{vs1.BODY_END}\ntrailer 1\n".encode())
    b.write_bytes(
        f"other intro\r\n{vs1.BODY_START}{body}{vs1.BODY_END}\r\ntrailer 2\r\n".replace("\n", "\r\n").encode()
    )
    assert vs1.prereg_body_sha256(a) == vs1.prereg_body_sha256(b)
    c = tmp_path / "c.md"
    c.write_text(f"{vs1.BODY_START}{body.replace('90', '91')}{vs1.BODY_END}", encoding="utf-8")
    assert vs1.prereg_body_sha256(c) != vs1.prereg_body_sha256(a)
    d = tmp_path / "d.md"
    d.write_text(f"{vs1.BODY_START}{body}", encoding="utf-8")
    with pytest.raises(ValueError):
        vs1.prereg_body_sha256(d)


def test_prereg_mismatch_is_refused(tmp_path):
    (tmp_path / vs1.PREREG_PATH).parent.mkdir(parents=True)
    (tmp_path / vs1.PREREG_PATH).write_text(f"{vs1.BODY_START}\nedited\n{vs1.BODY_END}", encoding="utf-8")
    with pytest.raises(ValueError, match="spec changed"):
        vs1.check_prereg(tmp_path)


def test_run_alpha_is_s11_spending():
    assert vs1.run_alpha(1) == pytest.approx(0.05)
    assert vs1.run_alpha(2) == pytest.approx(0.10 / 6)
    assert sum(vs1.run_alpha(k) for k in range(1, 500)) < vs1.LEDGER_Q
    assert vs1.trial_names() == ("A90|fwd5", "A90|fwd20", "A30|fwd5", "A30|fwd20")


# --- sector membership ---------------------------------------------------------------


def _map(entries):
    """entries: [(sector, subsector, ticker, weight, type)]"""
    out = {}
    for sector, sub, ticker, weight, kind in entries:
        subs = out.setdefault(sector, {"etf": "X", "subsectors": {}})["subsectors"]
        subs.setdefault(sub, {"weight": 0.1, "actors": []})["actors"].append(
            {"ticker": ticker, "weight": weight, "type": kind}
        )
    return out


def test_primary_sector_is_the_unique_max_weight_and_ties_are_excluded():
    sector_map = _map([
        ("Technology", "a", "AAA", 0.2, "company"),
        ("Communication Services", "b", "AAA", 0.22, "company"),
        ("Technology", "a", "BBB", 0.05, "company"),
        ("Materials", "c", "BBB", 0.05, "company"),
        ("Technology", "a", "CCC", 0.01, "company"),
        ("Technology", "d", "CCC", 0.3, "company"),
        ("Technology", "a", "PPP", 0.5, "person"),
    ])
    primary = vs1.primary_sectors(sector_map)
    assert primary == {"AAA": "Communication Services", "BBB": None, "CCC": "Technology"}


def test_sector_universe_resolves_ciks_and_collapses_share_classes():
    sector_map = _map([
        ("Technology", "a", "GOOGA", 0.2, "company"),
        ("Technology", "a", "GOOGC", 0.1, "company"),
        ("Technology", "a", "NOCIK", 0.1, "company"),
        ("Technology", "a", "TIE", 0.1, "company"),
        ("Energy", "e", "TIE", 0.1, "company"),
    ])
    issuers = pd.DataFrame({"ticker": ["GOOGA", "GOOGC", "TIE"], "cik": [1652044, 1652044, 7]})
    universe, info = vs1.sector_universe("Technology", sector_map, issuers)
    assert universe.to_dict("records") == [{"ticker": "GOOGA", "cik": 1652044}]
    assert info["unmapped_no_cik"] == ["NOCIK"]
    assert info["ambiguous_tie_excluded"] == ["TIE"]
    assert info["share_class_duplicates_dropped"] == 1
    with pytest.raises(ValueError):
        vs1.sector_universe("Crypto", sector_map, issuers)


SECTOR_MAP_SNAPSHOT = FIXTURES / "sector_map_1bb2f61b_companies.json"


def _companies_only(sector_map):
    """What primary_sectors reads: company actors (ticker, type, weight) per subsector."""
    out = {}
    for sector, body in sector_map.items():
        subs = {}
        for name, sub in (body.get("subsectors") or {}).items():
            actors = [
                {"ticker": str(a["ticker"]), "type": "company", "weight": float(a.get("weight") or 0.0)}
                for a in (sub or {}).get("actors") or ()
                if a.get("ticker") and a.get("type") == "company"
            ]
            if actors:
                subs[name] = {"actors": actors}
        out[sector] = {"subsectors": subs}
    return out


def _snapshot():
    raw = json.loads(SECTOR_MAP_SNAPSHOT.read_text(encoding="utf-8"))
    assert raw["source_lf_sha256"] == vs1.SECTOR_MAP_SHA256
    return raw["SECTOR_MAP"]


def _prereg_body() -> str:
    return vs1.prereg_body((vs1.REPO / vs1.PREREG_PATH).read_text(encoding="utf-8"))


def test_pinned_sector_map_snapshot_gives_exactly_the_preregistered_technology_list():
    """The snapshot of the pinned map (origin/main 1bb2f61b) reproduces §2.2 and §13 exactly.

    Uses a fixture, not the live yaml: an edit to analysis/sector_map_data.yaml
    makes the harness refuse to run (hash pin) but must not break CI.
    """
    snapshot = _snapshot()
    primary = vs1.primary_sectors(snapshot)
    tech = sorted(t for t, s in primary.items() if s == "Technology")
    listed = re.search(r"Resulting 88 Technology tickers:(.*?)\n2\. \*\*CIK", _prereg_body(), re.S)
    prereg = sorted(token.strip(".") for token in listed.group(1).split())
    assert len(prereg) == 88
    assert tech == prereg
    ties = sorted(t for t, s in primary.items() if s is None and vs1._ticker_in_sector(snapshot, t, "Technology"))
    assert ties == ["ALB", "LAC", "MP", "SQM", "UUUU"]
    elsewhere = sorted(
        t for t, s in primary.items()
        if s not in (None, "Technology") and vs1._ticker_in_sector(snapshot, t, "Technology")
    )
    assert elsewhere == ["AMZN", "APD", "BABA", "BYDDF", "GOOGL", "LIN", "META"]
    counts = {sector: sum(1 for s in primary.values() if s == sector) for sector in vs1.OTHER_SECTORS}
    assert counts == {
        "Energy": 97, "Financials": 118, "Healthcare": 133, "Industrials": 99,
        "Consumer Discretionary": 154, "Consumer Staples": 124, "Real Estate": 97,
        "Utilities": 67, "Communication Services": 56, "Materials": 75,
    }


def test_sector_map_snapshot_matches_the_repository_file_while_it_is_unchanged():
    if vs1.file_sha256(vs1.REPO / vs1.SECTOR_MAP_PATH) != vs1.SECTOR_MAP_SHA256:
        pytest.skip("sector_map_data.yaml changed since registration: the harness refuses it; "
                    "the snapshot stays the pre-registered reference")
    assert _companies_only(vs1.load_sector_map()) == _snapshot()


def test_issuer_map_reads_sec_company_tickers_json(tmp_path):
    path = tmp_path / "company_tickers.json"
    path.write_text(json.dumps({"0": {"cik_str": 320193, "ticker": "aapl", "title": "Apple"}}))
    assert vs1.load_issuer_map(path).to_dict("records") == [{"ticker": "AAPL", "cik": 320193}]


# --- Form 4 events -------------------------------------------------------------------


def _row(**overrides):
    base = {
        "accession_number": "0000000001-12-000001",
        "filing_date": "2012-03-02",
        "issuer_cik": "100",
        "document_type": "4",
        "amended": "False",
        "owner_cik": "5000",
        "nonderiv_trans_sk": "1",
        "transaction_date": "2012-03-01",
        "transaction_code": "P",
        "shares": "1000",
        "price_per_share": "20.0",
        "acquired_disposed_code": "A",
    }
    return {**base, **overrides}


def _submission(**overrides):
    """A SUBMISSION-table row (accession x owner), as in derived/submissions.parquet."""
    base = {
        "accession_number": "0000000001-12-000001",
        "filing_date": "2012-03-02",
        "issuer_cik": "100",
        "document_type": "4",
        "owner_cik": "5000",
    }
    return {**base, **overrides}


def _submissions_of(rows):
    """The SUBMISSION table for transaction rows: one row per accession they name."""
    return [
        _submission(accession_number=r["accession_number"], filing_date=r["filing_date"],
                    issuer_cik=r["issuer_cik"], document_type=r["document_type"], owner_cik=r["owner_cik"])
        for r in rows
    ]


def _frame(rows):
    frame = pd.DataFrame(rows).astype("string")
    frame.columns = [c.upper() for c in frame.columns]
    return frame


def _events(rows, submissions=None):
    """Events from transaction rows; the SUBMISSION table defaults to the rows' own accessions."""
    subs = _submissions_of(rows) if submissions is None else submissions
    return vs1.build_events(_frame(rows), submissions=_frame(subs))


def test_event_rules_exclude_amendments_other_codes_tiny_and_bad_dates():
    rows = [
        _row(),  # kept
        _row(accession_number="a2", document_type="4/A", nonderiv_trans_sk="1"),
        _row(accession_number="a3", amended="True"),
        _row(accession_number="a4", document_type="5"),
        _row(accession_number="a5", transaction_code="S", acquired_disposed_code="D"),
        _row(accession_number="a6", acquired_disposed_code="D"),
        _row(accession_number="a7", shares="99"),
        _row(accession_number="a8", price_per_share="5", shares="1000"),  # $5,000
        _row(accession_number="a9", price_per_share=""),
        _row(accession_number="a10", transaction_date="2012-03-05"),  # after filing
        _row(accession_number="a11", transaction_date="2010-01-01"),  # > 365 d lag
        _row(accession_number="a12", issuer_cik=""),
    ]
    events = _events(rows)
    counts = events.receipt["counts"]
    assert len(events.purchases) == 1
    assert counts["excluded_not_form_4"] == 2
    assert counts["excluded_amended"] == 1
    assert counts["excluded_not_acquired"] == 1
    assert counts["excluded_small_or_unpriced"] == 3
    assert counts["excluded_transaction_date"] == 2
    assert counts["excluded_missing_accession_issuer_or_filing_date"] == 1
    # every valid accession (any code, form, amendment) is Section 16 activity
    assert counts["activity_accessions"] == 11
    assert counts["transaction_accessions_missing_from_submissions"] == 0


def test_section16_activity_is_every_submission_incl_holdings_only_and_derivative_only_filings():
    """§2.1/§2.2: activity is every accession of the issuer, not only those with a
    non-derivative transaction line (the review's blocking finding 4)."""
    # issuer 300 has no non-derivative line at all: a holdings-only Form 3 and a
    # derivative-only Form 4; issuer 100 has one purchase line.
    submissions = [
        _submission(accession_number="f3-holdings", issuer_cik="300", document_type="3", filing_date="2013-01-02"),
        _submission(accession_number="f4-deriv", issuer_cik="300", document_type="4", filing_date="2014-06-02"),
        _submission(accession_number="f4-deriv", issuer_cik="300", document_type="4", filing_date="2014-06-02",
                    owner_cik="5001"),  # owner fan-out: still one accession
        _submission(),  # the purchase's own accession
    ]
    events = _events([_row()], submissions)
    counts = events.receipt["counts"]
    assert counts["submission_accessions"] == 3
    assert counts["activity_accessions"] == 3
    assert counts["transaction_accessions_missing_from_submissions"] == 0
    t = vs1.decision_instants([date(2013, 1, 2), date(2013, 1, 3), date(2016, 5, 31), date(2016, 6, 3)])
    mask = vs1.active_mask(events.activity, [300], t)
    # Form 3 known at 22:00 ET 2013-01-02 -> active from the next close; the
    # derivative-only Form 4 keeps it active 730 days after 2014-06-02
    assert mask[300].tolist() == [False, True, True, False]
    # built from the transaction table alone, issuer 300 would never be a filer
    transactions_only = _events([_row()], [_submission()])
    assert not vs1.active_mask(transactions_only.activity, [300], t)[300].any()


def test_a_transaction_accession_missing_from_the_submission_table_is_counted_and_kept():
    events = _events([_row(), _row(accession_number="late", nonderiv_trans_sk="9")], [_submission()])
    counts = events.receipt["counts"]
    assert counts["transaction_accessions_missing_from_submissions"] == 1
    assert counts["activity_accessions"] == 2


def test_build_events_requires_the_submission_table():
    with pytest.raises(TypeError):
        vs1.build_events(_frame([_row()]))
    with pytest.raises(ValueError, match="SUBMISSION"):
        vs1.build_events(_frame([_row()]), submissions=None)


def test_joint_filings_are_one_purchase_by_one_actor():
    rows = [
        # one accession, fanned out over three owners (the derived file's layout)
        _row(owner_cik="7003"), _row(owner_cik="7001"), _row(owner_cik="7002"),
        # the same purchase filed separately a day later by another owner
        _row(accession_number="b1", filing_date="2012-03-03", owner_cik="6999"),
        # a second line in the first accession: a different purchase
        _row(nonderiv_trans_sk="2", shares="2000", owner_cik="7001"),
    ]
    events = _events(rows)
    purchases = events.purchases.sort_values("shares")
    assert len(purchases) == 2
    first = purchases.iloc[0]
    assert first["actor"] == 6999 and first["n_reports"] == 2
    assert first["filing_date"] == pd.Timestamp("2012-03-02")  # earliest filing
    assert purchases.iloc[1]["actor"] == 7001


def test_known_at_is_filing_date_22h_new_york_in_utc_across_dst():
    known = vs1.filing_known_at(pd.Series(pd.to_datetime(["2019-01-15", "2019-07-15"])))
    assert list(known) == [
        pd.Timestamp("2019-01-16T03:00", tz="UTC"),
        pd.Timestamp("2019-07-16T02:00", tz="UTC"),
    ]


def test_sec_dataset_date_format_is_parsed():
    parsed = vs1.parse_dates(pd.Series(["31-MAR-2023", "2023-03-31", "junk"]))
    assert parsed.iloc[0] == parsed.iloc[1] == pd.Timestamp("2023-03-31")
    assert pd.isna(parsed.iloc[2])


def test_missing_owner_cik_is_refused_without_an_owner_table():
    frame = pd.DataFrame([_row()]).drop(columns=["owner_cik"]).astype("string")
    subs = _frame([_submission()]).drop(columns=["OWNER_CIK"])
    with pytest.raises(ValueError, match="owner"):
        vs1.build_events(frame, submissions=subs)
    owners = pd.DataFrame({"ACCESSION_NUMBER": ["0000000001-12-000001"], "RPTOWNERCIK": ["42"]})
    events = vs1.build_events(frame, owners, submissions=subs)
    assert list(events.purchases["actor"]) == [42]


def test_parquet_reader_filters_issuers_and_keeps_declared_columns(tmp_path):
    frame = pd.DataFrame([_row(), _row(issuer_cik="200", accession_number="z")]).assign(extra="x")
    path = tmp_path / "nonderiv.parquet"
    frame.to_parquet(path)
    subs_path = tmp_path / "submissions.parquet"
    pd.DataFrame([_submission(), _submission(accession_number="z", issuer_cik="200"),
                  _submission(accession_number="h3", document_type="3")]).to_parquet(subs_path)
    events = vs1.load_events(path, issuers=[100], submissions_path=subs_path)
    assert list(events.purchases["issuer_cik"]) == [100]
    assert events.receipt["inputs"]["transactions"]["sha256"] == vs1.data_sha256(path)
    assert events.receipt["inputs"]["submissions"]["sha256"] == vs1.data_sha256(subs_path)
    assert events.receipt["counts"]["submission_accessions"] == 2  # issuer 100 only
    assert events.receipt["counts"]["activity_accessions"] == 2


def _form345_zip(path: Path, submissions: list[dict], owners: list[dict]) -> None:
    def tsv(rows, columns):
        lines = ["\t".join(columns)] + ["\t".join(str(r.get(c, "")) for c in columns) for r in rows]
        return "\n".join(lines) + "\n"

    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("SUBMISSION.tsv", tsv(submissions, ["ACCESSION_NUMBER", "FILING_DATE", "PERIOD_OF_REPORT",
                                                          "DATE_OF_ORIG_SUB", "DOCUMENT_TYPE", "ISSUERCIK",
                                                          "ISSUERNAME", "ISSUERTRADINGSYMBOL"]))
        zf.writestr("REPORTINGOWNER.tsv", tsv(owners, ["ACCESSION_NUMBER", "RPTOWNERCIK", "RPTOWNERNAME"]))
        zf.writestr("NONDERIV_TRANS.tsv", "ACCESSION_NUMBER\tTRANS_CODE\n")


def test_submissions_builder_keeps_every_accession_and_feeds_section16_activity(tmp_path):
    from scripts import build_form345_submissions as builder

    raw = tmp_path / "raw"
    raw.mkdir()
    _form345_zip(
        raw / "2013q1_form345.zip",
        [
            {"ACCESSION_NUMBER": "f3", "FILING_DATE": "02-JAN-2013", "DOCUMENT_TYPE": "3", "ISSUERCIK": "300",
             "ISSUERTRADINGSYMBOL": "XYZ", "PERIOD_OF_REPORT": "31-DEC-2012"},
            {"ACCESSION_NUMBER": "f4a", "FILING_DATE": "15-MAR-2013", "DOCUMENT_TYPE": "4/A", "ISSUERCIK": "300",
             "DATE_OF_ORIG_SUB": "01-MAR-2013"},
            {"ACCESSION_NUMBER": "noowner", "FILING_DATE": "20-MAR-2013", "DOCUMENT_TYPE": "5", "ISSUERCIK": "301"},
        ],
        [{"ACCESSION_NUMBER": "f3", "RPTOWNERCIK": "11"}, {"ACCESSION_NUMBER": "f4a", "RPTOWNERCIK": "12"},
         {"ACCESSION_NUMBER": "f4a", "RPTOWNERCIK": "13"}],
    )
    out = tmp_path / "derived" / "submissions.parquet"
    receipt = builder.build(raw, out)
    assert receipt["rows"] == 4 and receipt["accessions"] == 3
    assert receipt["output_sha256"] == vs1.data_sha256(out)
    table = pd.read_parquet(out)
    assert list(table.columns) == builder.OUT_COLUMNS
    assert table.set_index("accession_number")["amended"].groupby(level=0).first().to_dict() == {
        "f3": False, "f4a": True, "noowner": False,
    }
    assert set(table["filing_date"]) == {"2013-01-02", "2013-03-15", "2013-03-20"}
    with pytest.raises(FileExistsError):
        builder.build(raw, out)  # write-once
    accessions, counts = vs1.section16_accessions(vs1.read_table(out), issuers=[300])
    assert sorted(accessions["accession"]) == ["f3", "f4a"] and counts["submission_accessions"] == 2


# --- features ------------------------------------------------------------------------


def _purchases(rows):
    frame = pd.DataFrame(rows, columns=["issuer_cik", "actor", "filing_date"])
    frame["filing_date"] = pd.to_datetime(frame["filing_date"])
    frame["known_at"] = vs1.filing_known_at(frame["filing_date"])
    return frame


def test_density_never_counts_an_event_before_it_is_known():
    purchases = _purchases([(1, 10, "2015-06-10")])
    decisions = vs1.decision_instants([date(2015, 6, 10), date(2015, 6, 11)])
    a = vs1.density(purchases, [1], decisions, 90, 45.0)
    # 16:00 ET on the filing date precedes the 22:00 ET known_at: not counted yet
    assert a.iloc[0, 0] == 0.0
    # next session close: counted, age 18 hours
    assert a.iloc[1, 0] == pytest.approx(np.exp(-(18 / 24) / 45.0))


def test_density_counts_distinct_actors_once_with_their_latest_event_inside_the_window():
    purchases = _purchases([
        (1, 10, "2015-01-02"),  # actor 10, old
        (1, 10, "2015-03-02"),  # actor 10, latest -> this one counts
        (1, 11, "2015-03-20"),
        (1, 12, "2014-10-01"),  # outside the 90-day window
        (2, 10, "2015-03-20"),  # another issuer
    ])
    t = vs1.decision_instants([date(2015, 3, 31)])
    a = vs1.density(purchases, [1, 2, 3], t, 90, 45.0)
    known = purchases["known_at"]
    age = [(t[0] - known.iloc[i]).total_seconds() / 86400 for i in (1, 2)]
    assert a.loc[t[0], 1] == pytest.approx(sum(np.exp(-x / 45.0) for x in age))
    assert a.loc[t[0], 3] == 0.0
    unweighted = vs1.density(purchases, [1], t, 90, 1e12)
    assert unweighted.iloc[0, 0] == pytest.approx(2.0)


def test_section16_activity_mask_uses_a_trailing_730_day_window():
    activity = pd.DataFrame({"issuer_cik": [1], "known_at": vs1.filing_known_at(pd.Series(pd.to_datetime(["2013-01-02"])))})
    t = vs1.decision_instants([date(2013, 1, 2), date(2013, 1, 3), date(2014, 12, 31), date(2015, 1, 5)])
    mask = vs1.active_mask(activity, [1, 2], t)
    assert mask[1].tolist() == [False, True, True, False]
    assert not mask[2].any()


def test_entry_positions_are_one_per_issuer_and_first_close_after_known_at():
    purchases = _purchases([
        (1, 10, "2015-06-10"),  # Wednesday filing -> entry close Thursday 06-11
        (1, 11, "2015-06-10"),  # same issuer, same entry session: one position
        (1, 10, "2015-06-11"),  # next day -> entry Friday 06-12
        (2, 12, "2015-06-12"),  # Friday filing -> Monday 06-15
        (3, 13, "2015-06-30"),  # after the last session: reported, not dropped
    ]).assign(value=[1e5, 2e5, 3e5, 4e5, 5e5])
    sessions = [date(2015, 6, d) for d in (10, 11, 12, 15, 16)]
    positions = vs1.entry_positions(purchases, sessions).sort_values(["issuer_cik", "entry_close"])
    first = positions.iloc[0]
    assert first["entry_close"] == pd.Timestamp("2015-06-11T20:00", tz="UTC")
    assert first["actors"] == [10, 11] and first["n_actors"] == 2 and first["purchases"] == 2
    assert first["total_value"] == pytest.approx(3e5) and first["largest_value"] == pytest.approx(2e5)
    assert positions.iloc[1]["entry_close"] == pd.Timestamp("2015-06-12T20:00", tz="UTC")
    assert positions.iloc[2]["entry_close"] == pd.Timestamp("2015-06-15T20:00", tz="UTC")
    last = positions[positions["issuer_cik"] == 3].iloc[0]
    assert last["status"] == "no_entry_session" and pd.isna(last["entry_close"])
    # no entry close ever precedes the filing's known_at
    opened = positions[positions["status"] == "opened"]
    assert (opened["entry_close"] > opened["last_known_at"]).all()


def test_missing_labels_are_counted_not_silently_dropped():
    feature = np.array([[1.0, 0.0, 0.0, np.nan], [0.0, 2.0, 0.0, 0.0]])
    label = np.array([[np.nan, 0.1, np.nan, 0.2], [0.1, np.nan, 0.3, 0.0]])
    panel = vs1.TrialPanel("A90|fwd20", "discovery", 20, ["a", "b"], ["c", "d"], list("wxyz"), feature, label)
    counts = vs1.missing_labels(panel)
    assert counts == {"issuer_dates_with_feature": 7, "missing_label": 3, "buyer_issuer_dates": 2,
                      "buyer_missing_label": 2, "buyer_missing_share": 1.0}
    verdict = vs1.verdict({"calibration": {"state": "WEAK_POSITIVE"},
                           "ledger": [{"trial": "A90|fwd20", "labels": counts}]}, [], {"gate_passed": False})
    assert any("SURVIVORSHIP_WARNING" in note for note in verdict["notes"])


def test_decision_instant_is_the_new_york_close():
    assert list(vs1.decision_instants([date(2019, 1, 15), date(2019, 7, 15)])) == [
        pd.Timestamp("2019-01-15T21:00", tz="UTC"),
        pd.Timestamp("2019-07-15T20:00", tz="UTC"),
    ]


# --- statistics ----------------------------------------------------------------------


@functools.lru_cache(maxsize=4)
def _window_days(window: str) -> pd.DatetimeIndex:
    lo, hi = vs1.window_bounds(window)
    return pd.bdate_range(lo, hi - pd.Timedelta(days=1), tz="UTC")


def _synthetic_trials(rng, ic=(0.0, 0.0, 0.0, 0.0), T=120, E=40, window="discovery",
                      factor_phi=0.3, prevalence=0.2):
    """Four correlated trial panels; labels load on a persistent common factor
    whose exposure is correlated with the feature (the hard null case)."""
    days = _window_days(window)
    beta = rng.standard_normal(E)
    panels = {}
    base = np.where(rng.random((T, E)) < prevalence, rng.exponential(1.0, (T, E)), 0.0)
    base = base + 0.5 * np.clip(beta, 0, None)[None, :] * (rng.random((T, E)) < prevalence)
    for k, trial in enumerate(vs1.trial_names()):
        h = int(trial.split("fwd")[1])
        decided = days[::h][: T + 1]
        n = len(decided) - 1
        feature = base[:n] if trial.startswith("A90") else base[:n] * (rng.random((n, E)) < 0.6)
        f = np.zeros(n)
        for t in range(1, n):
            f[t] = factor_phi * f[t - 1] + rng.standard_normal()
        z = feature - feature.mean(1, keepdims=True)
        z = z / (z.std(1, keepdims=True) + 1e-12)
        label = ic[k] * 1.5 * z + 0.8 * beta[None, :] * f[:, None] + rng.standard_normal((n, E))
        panels[trial] = vs1.TrialPanel(
            trial=trial, window=window, horizon=h,
            decision_at=[d.isoformat() for d in decided[:n]],
            label_end=[d.isoformat() for d in decided[1 : n + 1]],
            entities=[f"T{i}" for i in range(E)],
            feature=feature.astype(float), label=label,
        )
    return panels


def _pvalues(panels, perms=499):
    return [
        vs1.measure_trial(panels[t], perms=perms, sensitivity=False)["p"] for t in vs1.trial_names()
    ]


def test_signflip_null_controls_familywise_and_false_discovery_rates_under_the_global_null():
    rng = np.random.default_rng(7)
    reps = 150
    holm_any = bh_any = 0
    for _ in range(reps):
        p = _pvalues(_synthetic_trials(rng))
        holm_any += any(x <= vs1.run_alpha(1) for x in holm_adjusted(p))
        bh_any += any(x <= vs1.BH_Q for x in bh_adjusted(p))
    # nominal 0.05 (Holm) and 0.10 (BH: FDR = FWER under the global null); 3 SE slack
    assert holm_any / reps <= 0.05 + 3 * np.sqrt(0.05 * 0.95 / reps)
    assert bh_any / reps <= 0.10 + 3 * np.sqrt(0.10 * 0.90 / reps)


def test_bh_false_discovery_proportion_is_controlled_with_two_true_effects():
    rng = np.random.default_rng(11)
    reps, fdp = 100, []
    for _ in range(reps):
        p = _pvalues(_synthetic_trials(rng, ic=(0.25, 0.25, 0.0, 0.0)))
        rejected = [i for i, x in enumerate(bh_adjusted(p)) if x <= vs1.BH_Q]
        false = [i for i in rejected if i >= 2]
        fdp.append(len(false) / max(1, len(rejected)))
    assert np.mean(fdp) <= vs1.BH_Q + 0.05


# --- registry-backed one-shot helpers (synthetic input hashes; no real file) ---------------


def _frozen_inputs(manifest=None, **overrides):
    manifest = manifest or _manifest([])
    base = {
        "sector": "Technology",
        "price_manifest_sha256": manifest.digest(),
        "probe_report_sha256": manifest.probe_report_sha256,
        "form4_sha256": "1" * 64,
        "submissions_sha256": "2" * 64,
        "issuer_map_sha256": "3" * 64,
        "power_sha256": "4" * 64,
        "accept_underpowered": False,
        "as_of_ts": AS_OF_TS,
    }
    return {**base, **overrides}


def _observed(inputs):
    return {k: v for k, v in inputs.items() if k not in ("as_of_ts", "accept_underpowered")}


def _register(log_dir):
    """A copy of the pinned VS1 v1 registration (the only registration the harness accepts)."""
    return vs1.register(log_dir, vs1.REGISTERED_AT, vs1.REGISTERED_CODE_SHA)


def _offhost(log_dir) -> Path:
    """The registry's off-host anchor log (stands in for the committed vault file)."""
    return Path(log_dir).parent / f"{Path(log_dir).name}.offhost-anchors.jsonl"


def _witness(log_dir) -> Path:
    """Export the registry's anchor lines to its off-host log (the operator's commit + push)."""
    vs1.export_anchors(log_dir, _offhost(log_dir))
    return _offhost(log_dir)


def _discovery_key(log_dir, manifest=None):
    """register -> inputs_frozen -> discovery_opened -> off-host witness -> key; returns key, inputs."""
    _register(log_dir)
    inputs = _frozen_inputs(manifest)
    vs1.freeze_inputs(log_dir, NOW, inputs)
    vs1.open_discovery(log_dir, NOW, _observed(inputs))
    return vs1.resume_discovery(log_dir, _observed(inputs), _witness(log_dir)), inputs


def _spec(run_id="r"):
    return vs1.RunSpec(run_id=run_id, sector="Technology", trials=vs1.trial_names())


def _discover(log_dir, panels, run_id="r", manifest=None, sensitivity=False):
    """A sealed discovery (chain: ... discovery_opened, discovery_frozen)."""
    key, inputs = _discovery_key(log_dir, manifest)
    frozen = vs1.discover_panel(_spec(run_id), panels, inputs={"inputs_frozen_sha256": key.inputs_frozen_sha256},
                                sensitivity=sensitivity)
    vs1.seal_discovery(log_dir, NOW, key, frozen)
    return frozen, _observed(inputs)


def _open_holdout(log_dir, frozen, observed, **overrides):
    kwargs = {"allow_holdout": True, "prereg_sha256": vs1.PREREG_BODY_SHA256, "log_dir": log_dir, "now": NOW,
              "observed": observed, **overrides}
    return vs1.open_holdout(frozen, **kwargs)


def _resume_holdout(log_dir, frozen, observed, external_anchors):
    return vs1.resume_holdout(frozen, allow_holdout=True, prereg_sha256=vs1.PREREG_BODY_SHA256, log_dir=log_dir,
                              observed=observed, external_anchors=external_anchors)


def _holdout_key(log_dir, frozen, observed):
    """holdout_opened -> off-host witness -> key."""
    _open_holdout(log_dir, frozen, observed)
    return _resume_holdout(log_dir, frozen, observed, _witness(log_dir))


def _kinds(log_dir):
    return [r["kind"] for r in vs1.registry(log_dir).read_all()]


def test_planted_effect_is_found_in_discovery_and_survives_the_holdout(tmp_path):
    rng = np.random.default_rng(3)
    planted = (0.0, 0.25, 0.0, 0.0)  # the primary trial only
    frozen, observed = _discover(tmp_path, _synthetic_trials(rng, planted), run_id="test")
    ledger = {t["trial"]: t for t in frozen["payload"]["ledger"]}
    assert ledger["A90|fwd20"]["selected"] and ledger["A90|fwd20"]["mean_ic"] > 0
    assert frozen["payload"]["calibration"]["state"] == "CONSISTENT"
    key = _holdout_key(tmp_path, frozen, observed)
    result = vs1.evaluate_panel_holdout(
        frozen, _synthetic_trials(rng, planted, window="holdout"), key, power={"gate_passed": True}
    )
    survivor = next(c for c in result["holdout_checks"] if c["trial"] == "A90|fwd20")
    assert survivor["retrospective_survivor"]
    assert result["verdict"]["state"] == "HOLDOUT_SURVIVOR_FORWARD_PENDING"
    assert result["promotion_allowed"] is False
    vs1.seal_holdout(tmp_path, NOW, key, result)
    assert _kinds(tmp_path) == ["header", "preregistration", "inputs_frozen", "discovery_opened",
                                "discovery_frozen", "holdout_opened", "holdout_result"]
    assert vs1.registry(tmp_path).verify_chain()["ok"]


def test_null_discovery_yields_no_survivor_and_flags_underpowered(tmp_path):
    rng = np.random.default_rng(5)
    frozen, observed = _discover(tmp_path, _synthetic_trials(rng, factor_phi=0.0), run_id="null")
    key = _holdout_key(tmp_path, frozen, observed)
    result = vs1.evaluate_panel_holdout(frozen, _synthetic_trials(rng, window="holdout"), key, power=None)
    assert result["verdict"]["state"] in ("NO_SURVIVOR", "MACHINERY_SUSPECT")
    if result["verdict"]["state"] == "NO_SURVIVOR":
        assert any("UNDERPOWERED" in n for n in result["verdict"]["notes"])


def test_contrary_discovery_is_machinery_suspect():
    ledger = [
        {"trial": t, "status": "tested", "mean_ic": -0.05 if t == "A30|fwd5" else 0.01,
         "p": 0.01 if t == "A30|fwd5" else 0.5, "p_one_sided_positive": 0.3, "selected": False}
        for t in vs1.trial_names()
    ]
    calib = vs1.calibration(ledger)
    assert calib["state"] == "CONTRARY" and calib["contrary_trials"] == ["A30|fwd5"]
    verdict = vs1.verdict({"calibration": calib}, [], {"gate_passed": False})
    assert verdict["state"] == "MACHINERY_SUSPECT"


def test_absent_effect_when_powered_is_machinery_suspect_but_not_when_underpowered():
    calib = {"state": "ABSENT"}
    assert vs1.verdict({"calibration": calib}, [], {"gate_passed": True})["state"] == "MACHINERY_SUSPECT"
    assert vs1.verdict({"calibration": calib}, [], {"gate_passed": False})["state"] == "NO_SURVIVOR"


def test_rank_ic_abstains_on_constant_feature_or_too_few_issuers():
    feature = np.zeros((3, 25))
    feature[1, :3] = 1.0
    label = np.random.default_rng(0).standard_normal((3, 25))
    label[2, :] = np.nan
    label[2, :5] = 1.0
    ic, counts = vs1.rank_ic_series(feature, label)
    assert np.isnan(ic[0]) and np.isfinite(ic[1]) and np.isnan(ic[2])
    assert counts.tolist() == [25, 25, 5]


def test_signflip_pvalue_resolution_and_direction():
    ic = np.full(40, 0.05)
    mean, two, one = vs1.signflip_pvalues(ic, 1, 999, 1, 1)
    assert mean == pytest.approx(0.05)
    assert two == pytest.approx(1 / 1000) and one == pytest.approx(1 / 1000)
    _, _, other_way = vs1.signflip_pvalues(ic, 1, 999, 1, -1)
    assert other_way == 1.0


def test_planted_power_grows_with_the_effect_and_is_zero_without_buyers():
    rng = np.random.default_rng(1)
    feature = np.where(rng.random((150, 40)) < 0.2, 1.0, 0.0)
    weak = vs1.planted_power(feature, 0.02, sims=30, perms=199)
    strong = vs1.planted_power(feature, 0.2, sims=30, perms=199)
    assert strong["power"] > weak["power"] and strong["power"] >= 0.9
    assert vs1.planted_power(np.zeros((150, 40)), 0.2, sims=5, perms=99)["power"] == 0.0


# --- holdout and discovery refusals ------------------------------------------------------


@pytest.fixture()
def sealed_null(tmp_path):
    """(frozen discovery, observed inputs, registry dir) with discovery_frozen in the chain."""
    rng = np.random.default_rng(9)
    frozen, observed = _discover(tmp_path, _synthetic_trials(rng, T=40))
    return frozen, observed, tmp_path


def test_holdout_is_refused_without_the_flag_or_the_matching_hash(sealed_null):
    frozen, observed, log_dir = sealed_null
    with pytest.raises(PermissionError):
        _open_holdout(log_dir, frozen, observed, allow_holdout=False)
    with pytest.raises(PermissionError):
        _open_holdout(log_dir, frozen, observed, allow_holdout="yes")
    with pytest.raises(PermissionError):
        _open_holdout(log_dir, frozen, observed, prereg_sha256="0" * 64)
    tampered = json.loads(json.dumps(frozen))
    tampered["payload"]["ledger"][0]["selected"] = True
    with pytest.raises(PermissionError):
        _open_holdout(log_dir, tampered, observed)
    # none of the refusals consumed the holdout
    assert "holdout_opened" not in _kinds(log_dir)


def test_holdout_evaluation_needs_a_key_for_this_manifest(sealed_null):
    frozen, observed, log_dir = sealed_null
    with pytest.raises(TypeError):
        vs1.HoldoutKey(object(), frozen["sha256"], {"as_of_ts": AS_OF_TS})
    other = _holdout_key(log_dir, frozen, observed)
    other.frozen_sha256 = "f" * 64
    with pytest.raises(PermissionError):
        vs1.evaluate_panel_holdout(frozen, {}, other)


def test_discovery_refuses_holdout_panels_and_undeclared_trials():
    rng = np.random.default_rng(2)
    spec = _spec()
    with pytest.raises(ValueError):
        vs1.discover_panel(spec, _synthetic_trials(rng, T=40, window="holdout"), inputs={})
    panels = _synthetic_trials(rng, T=40)
    panels.pop("A30|fwd5")
    with pytest.raises(ValueError):
        vs1.discover_panel(spec, panels, inputs={})
    with pytest.raises(ValueError):
        vs1.RunSpec(run_id="r", sector="Energy", run_k=2, trials=vs1.trial_names()).validate()
    with pytest.raises(ValueError):
        vs1.RunSpec(run_id="r", sector="Technology", run_k=2, trials=vs1.trial_names()).validate()


@pytest.mark.parametrize("override", [{"perms": 999}, {"perms": 20001}, {"seed": 1}, {"min_n": 31}, {"min_n": 29}])
def test_run_spec_refuses_any_statistical_setting_other_than_the_registered_one(override):
    vs1.RunSpec(run_id="r", sector="Technology", trials=vs1.trial_names()).validate()
    spec = vs1.RunSpec(run_id="r", sector="Technology", trials=vs1.trial_names(), **override)
    with pytest.raises(ValueError, match="pre-registered"):
        spec.validate()
    rng = np.random.default_rng(2)
    with pytest.raises(ValueError, match="pre-registered"):
        vs1.discover_panel(spec, _synthetic_trials(rng, T=40), inputs={}, sensitivity=False)


# --- one-shot enforcement through the registry chain ---------------------------------------


def test_freeze_inputs_needs_a_registration_and_every_input(tmp_path):
    with pytest.raises(PermissionError, match="not registered"):
        vs1.freeze_inputs(tmp_path, NOW, _frozen_inputs())
    _register(tmp_path)
    for key in vs1.FROZEN_INPUT_KEYS:
        partial = {k: v for k, v in _frozen_inputs().items() if k != key}
        with pytest.raises(ValueError, match=key):
            vs1.freeze_inputs(tmp_path, NOW, partial)
    with pytest.raises(ValueError, match="sha256"):
        vs1.freeze_inputs(tmp_path, NOW, _frozen_inputs(form4_sha256="nothex"))
    with pytest.raises(ValueError, match="later than the freeze"):
        vs1.freeze_inputs(tmp_path, NOW, _frozen_inputs(as_of_ts="2026-09-28T00:00:00+00:00"))
    with pytest.raises(ValueError):
        vs1.freeze_inputs(tmp_path, NOW, _frozen_inputs(as_of_ts="2026-09-26T00:00:00"))  # naive
    with pytest.raises(ValueError, match="VS1"):
        vs1.freeze_inputs(tmp_path, NOW, _frozen_inputs(sector="Energy"))
    record = vs1.freeze_inputs(tmp_path, NOW, _frozen_inputs())
    assert record["kind"] == "inputs_frozen" and record["supersedes"] is None
    assert record["inputs"]["as_of_ts"] == AS_OF_TS
    # a re-freeze before any discovery supersedes (no price read under the first)
    again = vs1.freeze_inputs(tmp_path, NOW, _frozen_inputs(form4_sha256="5" * 64))
    assert again["supersedes"] is not None


def test_discovery_is_refused_without_an_inputs_frozen_record(tmp_path):
    _register(tmp_path)
    with pytest.raises(PermissionError, match="inputs_frozen"):
        vs1.open_discovery(tmp_path, NOW, _observed(_frozen_inputs()))
    assert "discovery_opened" not in _kinds(tmp_path)


@pytest.mark.parametrize("field", ["price_manifest_sha256", "probe_report_sha256", "form4_sha256",
                                   "submissions_sha256", "issuer_map_sha256", "power_sha256", "sector"])
def test_discovery_is_refused_when_any_input_differs_from_inputs_frozen(tmp_path, field):
    _register(tmp_path)
    inputs = _frozen_inputs()
    vs1.freeze_inputs(tmp_path, NOW, inputs)
    observed = {**_observed(inputs), field: "e" * 64}
    with pytest.raises(PermissionError, match=field):
        vs1.open_discovery(tmp_path, NOW, observed)
    assert "discovery_opened" not in _kinds(tmp_path)  # the refusal did not consume discovery


def test_discovery_runs_once_even_after_a_crash_and_its_inputs_cannot_be_refrozen(tmp_path):
    key, inputs = _discovery_key(tmp_path)
    assert _kinds(tmp_path)[-1] == "discovery_opened"
    assert key.as_of_ts == datetime(2026, 9, 26, tzinfo=UTC)
    # the first discovery "crashed" after opening (prices may have been read): no second one
    with pytest.raises(PermissionError, match="one shot"):
        vs1.open_discovery(tmp_path, NOW, _observed(inputs))
    with pytest.raises(PermissionError, match="already opened"):
        vs1.freeze_inputs(tmp_path, NOW, _frozen_inputs(form4_sha256="5" * 64))
    with pytest.raises(TypeError):
        vs1.DiscoveryKey(object(), key.inputs_frozen_sha256, inputs)


def test_a_sealed_discovery_cannot_be_rerun_or_resealed(tmp_path):
    rng = np.random.default_rng(9)
    key, inputs = _discovery_key(tmp_path)
    frozen = vs1.discover_panel(_spec(), _synthetic_trials(rng, T=40),
                                inputs={"inputs_frozen_sha256": key.inputs_frozen_sha256}, sensitivity=False)
    other = json.loads(json.dumps(frozen))
    other["payload"]["inputs"]["inputs_frozen_sha256"] = "0" * 64
    other["sha256"] = digest(other["payload"])
    with pytest.raises(PermissionError, match="inputs_frozen"):
        vs1.seal_discovery(tmp_path, NOW, key, other)
    record = vs1.seal_discovery(tmp_path, NOW, key, frozen)
    assert record["kind"] == "discovery_frozen" and record["discovery_sha256"] == frozen["sha256"]
    with pytest.raises(PermissionError, match="already frozen"):
        vs1.seal_discovery(tmp_path, NOW, key, frozen)
    with pytest.raises(PermissionError, match="one shot"):
        vs1.open_discovery(tmp_path, NOW, _observed(inputs))


def test_holdout_is_refused_unless_the_chain_froze_this_discovery_file(sealed_null, tmp_path_factory):
    frozen, observed, log_dir = sealed_null
    # a different discovery (self-consistent file, not the one in the chain)
    rng = np.random.default_rng(10)
    other_dir = tmp_path_factory.mktemp("other")
    other, _ = _discover(other_dir, _synthetic_trials(rng, T=40), run_id="other")
    with pytest.raises(PermissionError, match="not the one the registry chain froze"):
        _open_holdout(log_dir, other, observed)
    # a registry with no frozen discovery at all
    empty = tmp_path_factory.mktemp("empty")
    _register(empty)
    with pytest.raises(PermissionError, match="no single frozen discovery"):
        _open_holdout(empty, frozen, observed)
    # inputs that differ from the discovery's inputs_frozen
    with pytest.raises(PermissionError, match="form4_sha256"):
        _open_holdout(log_dir, frozen, {**observed, "form4_sha256": "e" * 64})
    assert "holdout_opened" not in _kinds(log_dir)


def test_holdout_opens_once_and_records_holdout_opened_before_any_holdout_price(sealed_null):
    frozen, observed, log_dir = sealed_null
    opened = _open_holdout(log_dir, frozen, observed)
    # the record exists before any key (and so before any holdout price)
    assert _kinds(log_dir)[-1] == "holdout_opened" and opened["kind"] == "holdout_opened"
    assert vs1.registry(log_dir).read_all()[-1]["discovery_sha256"] == frozen["sha256"]
    assert opened["records"] == len(_kinds(log_dir))
    with pytest.raises(PermissionError, match="already opened"):
        _open_holdout(log_dir, frozen, observed)
    key = _resume_holdout(log_dir, frozen, observed, _witness(log_dir))
    assert key.as_of_ts == datetime(2026, 9, 26, tzinfo=UTC)
    result = {"discovery_manifest": frozen["sha256"], "verdict": {"state": "NO_SURVIVOR"}}
    vs1.seal_holdout(log_dir, NOW, key, result)
    with pytest.raises(PermissionError, match="already recorded"):
        vs1.seal_holdout(log_dir, NOW, key, result)
    assert vs1.registry(log_dir).verify_chain()["ok"]


def test_a_broken_chain_refuses_every_stage(sealed_null):
    frozen, observed, log_dir = sealed_null
    path = log_dir / vs1.REGISTRY_LOG
    lines = path.read_bytes().split(b"\n")
    lines[2] = lines[2].replace(b'"form4_sha256":"1111', b'"form4_sha256":"9111')
    path.write_bytes(b"\n".join(lines))
    with pytest.raises(RuntimeError, match="broken"):
        _open_holdout(log_dir, frozen, observed)


# --- Stage-0 power settings ------------------------------------------------------------------


def _power_file(gate_power=0.4, **overrides):
    rows = [{"target_ic": ic, "power": gate_power if ic == vs1.POWER_GATE_IC else 0.9,
             "sims": vs1.POWER_SIMS, "usable_dates": 90} for ic in vs1.POWER_TARGET_ICS]
    power = {
        "table": {t: [dict(r) for r in rows] for t in vs1.trial_names()},
        "settings": vs1.power_settings(),
        "gate_passed": gate_power >= vs1.POWER_GATE,
    }
    return json.loads(json.dumps({**power, **overrides}))  # as read back from power.json


def test_power_file_must_carry_the_registered_settings():
    assert vs1.power_settings()["sims"] == 200 and vs1.power_settings()["perms"] == 999
    vs1.verify_power(_power_file())
    vs1.verify_power(_power_file(gate_power=0.6))
    for name, value in (("sims", 30), ("perms", 199), ("seed", 1)):
        with pytest.raises(ValueError, match="pre-registered settings"):
            vs1.verify_power(_power_file(settings={**vs1.power_settings(), name: value}))
    with pytest.raises(ValueError, match="pre-registered settings"):
        vs1.verify_power({k: v for k, v in _power_file().items() if k != "settings"})
    bad = _power_file()
    bad["table"]["A90|fwd20"][0]["sims"] = 30
    with pytest.raises(ValueError, match="200 simulations"):
        vs1.verify_power(bad)
    with pytest.raises(ValueError, match="gate_passed"):
        vs1.verify_power(_power_file(gate_power=0.4, gate_passed=True))


def test_stage0_power_has_no_settings_override():
    with pytest.raises(TypeError):
        vs1.stage0_power({}, sims=30)
    with pytest.raises(ValueError, match="declared trials"):
        vs1.stage0_power({"A90|fwd20": np.zeros((10, 10))})


def test_power_cli_has_no_sims_or_perms_override(capsys):
    from scripts import run_vs1_insider_density as cli

    with pytest.raises(SystemExit):
        cli.main(["power", "--form4", "f", "--submissions", "s", "--issuer-map", "m", "--out", "o",
                  "--sims", "30"])
    assert "unrecognized arguments: --sims" in capsys.readouterr().err


def test_overlapping_or_out_of_window_decisions_are_refused():
    rng = np.random.default_rng(4)
    panel = _synthetic_trials(rng, T=40)["A90|fwd20"]
    panel.label_end[0] = panel.decision_at[3]
    with pytest.raises(ValueError, match="overlapping"):
        vs1.validate_panel(panel)
    panel = _synthetic_trials(rng, T=40)["A90|fwd20"]
    panel.label_end[-1] = "2020-01-02T00:00:00+00:00"
    with pytest.raises(ValueError, match="outside"):
        vs1.validate_panel(panel)


# --- prices through store.observations (SQLite) --------------------------------------------

TIINGO, YFINANCE = 1, 2


def _price_db(series: dict[str, pd.Series], extra_yf: dict[str, pd.Series] | None = None):
    engine = create_engine("sqlite://")
    md = MetaData()
    catalog = Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String))
    raw = Table(
        "raw_series", md,
        Column("series_id", String), Column("source_id", Integer), Column("obs_date", Date),
        Column("pull_timestamp", DateTime), Column("value", Float), Column("raw_payload", Text),
        Column("pull_status", String),
    )
    md.create_all(engine)
    pulled = datetime(2026, 9, 20, 6, 0)
    rows = []
    for source, table in ((TIINGO, series), (YFINANCE, extra_yf or {})):
        for ticker, values in table.items():
            rows.extend(
                {"series_id": f"YF:{ticker}:close", "source_id": source, "obs_date": d.date(),
                 "pull_timestamp": pulled, "value": float(v), "raw_payload": "{}", "pull_status": "SUCCESS"}
                for d, v in values.items()
            )
    with engine.begin() as c:
        c.execute(catalog.insert(), [{"id": TIINGO, "name": "tiingo"}, {"id": YFINANCE, "name": "yfinance"}])
        c.execute(raw.insert(), rows)
    return engine


def _manifest(tickers, source="tiingo"):
    return vs1.PriceManifest(
        source=source, series_template="YF:{ticker}:close", basis="split+dividend adjusted",
        benchmark="XLK", admitted=tuple(sorted(set(tickers) | {"XLK"})), probe_report_sha256="a" * 64,
    )


def test_price_reader_refuses_unadmitted_sources_tickers_and_reads_past_the_split(tmp_path):
    dates = pd.bdate_range("2019-12-20", "2020-01-10")
    engine = _price_db({"XLK": pd.Series(100.0, index=dates), "AAA": pd.Series(10.0, index=dates)})
    key, _ = _discovery_key(tmp_path / "a", _manifest(["AAA"]))
    empty_key, _ = _discovery_key(tmp_path / "b", _manifest([]))
    read = functools.partial(vs1.load_price_panel, start=date(2019, 12, 1), as_of=date(2019, 12, 31),
                             window="discovery")
    with engine.connect() as conn:
        with pytest.raises(ValueError):
            read(conn, _manifest(["AAA"], "yfinance"), ["AAA"], key=key)
        with pytest.raises(PermissionError, match="not in the admitted"):
            read(conn, _manifest([]), ["AAA"], key=empty_key)
        with pytest.raises(PermissionError):
            vs1.load_price_panel(conn, _manifest(["AAA"]), ["AAA"], start=date(2019, 12, 1),
                                 as_of=date(2020, 1, 1), window="discovery", key=key)
        with pytest.raises(PermissionError):
            vs1.load_price_panel(conn, _manifest(["AAA"]), ["AAA"], start=date(2019, 12, 1),
                                 as_of=date(2020, 1, 10), window="holdout", key=key)
        panel = read(conn, _manifest(["AAA"]), ["AAA"], key=key)
    assert panel.receipt["series"]["AAA"]["last"] == "2019-12-31"
    assert panel.closes().index.max() == pd.Timestamp("2019-12-31")
    assert panel.receipt["as_of_ts"] == AS_OF_TS  # the frozen read instant


def test_price_reader_refuses_without_a_registry_key_or_with_another_manifest(tmp_path):
    dates = pd.bdate_range("2019-12-02", "2019-12-31")
    engine = _price_db({"XLK": pd.Series(100.0, index=dates), "AAA": pd.Series(10.0, index=dates)})
    key, _ = _discovery_key(tmp_path, _manifest(["AAA"]))
    read = functools.partial(vs1.load_price_panel, start=date(2019, 12, 1), as_of=date(2019, 12, 31),
                             window="discovery")
    other_source = vs1.PriceManifest(source="twelvedata", series_template="YF:{ticker}:close",
                                     basis="split+dividend adjusted", benchmark="XLK",
                                     admitted=("AAA", "XLK"), probe_report_sha256="a" * 64)
    other_probe = replace(_manifest(["AAA"]), probe_report_sha256="b" * 64)
    with engine.connect() as conn:
        with pytest.raises(PermissionError, match="DiscoveryKey"):
            read(conn, _manifest(["AAA"]), ["AAA"], key=None)
        with pytest.raises(PermissionError, match="inputs_frozen"):
            read(conn, other_source, ["AAA"], key=key)
        with pytest.raises(PermissionError, match="inputs_frozen"):
            read(conn, other_probe, ["AAA"], key=key)


def test_price_reader_takes_only_the_manifest_source_on_a_shared_series_id(tmp_path):
    dates = pd.bdate_range("2019-12-02", "2019-12-31")
    engine = _price_db(
        {"XLK": pd.Series(100.0, index=dates), "AAA": pd.Series(10.0, index=dates)},
        extra_yf={"AAA": pd.Series(99.0, index=dates)},
    )
    key, _ = _discovery_key(tmp_path, _manifest(["AAA"]))
    with engine.connect() as conn:
        panel = vs1.load_price_panel(conn, _manifest(["AAA"]), ["AAA"], start=date(2019, 12, 1),
                                     as_of=date(2019, 12, 31), window="discovery", key=key)
    assert set(panel.closes()["AAA"]) == {10.0}


def test_labels_are_purged_at_the_split_and_horizon_spaced():
    dates = pd.bdate_range("2019-11-01", "2020-02-28")
    closes = pd.DataFrame({"AAA": np.linspace(10, 20, len(dates)), "XLK": 100.0}, index=dates)
    positions, labels, momentum = vs1.relative_labels(closes, "XLK", ["AAA"], 5, "discovery")
    ends = [dates[i + 5] for i in positions]
    assert max(ends) < pd.Timestamp("2020-01-01")
    assert all(b - a == 5 for a, b in zip(positions, positions[1:]))
    assert labels.shape == (len(positions), 1) and np.isfinite(labels).all()
    positions, _, _ = vs1.relative_labels(closes, "XLK", ["AAA"], 5, "holdout")
    assert dates[positions[0]] >= pd.Timestamp("2020-01-01")


def _holdings_only_submissions(ciks, dates):
    """Quarterly holdings-only filings (no transaction line) keep every issuer a Section 16 filer."""
    return [
        _submission(accession_number=f"act-{cik}-{d.date()}", issuer_cik=str(cik), filing_date=str(d.date()),
                    document_type="3" if k == 0 else "4", owner_cik="9")
        for cik in ciks
        for k, d in enumerate(dates[::60])
    ]


def test_end_to_end_planted_insider_effect_through_the_price_reader(tmp_path):
    """Buyers' stocks drift up after the filing; the harness must find it, and only
    through admitted, source-filtered, split-bounded reads behind the registry chain."""
    rng = np.random.default_rng(12)
    tickers = [f"T{i:02d}" for i in range(24)]
    ciks = list(range(1000, 1024))
    dates = pd.bdate_range("2010-01-04", "2026-06-30")
    n = len(dates)
    rets = rng.normal(0.0, 0.01, (n, len(tickers)))
    rows = []
    for j, cik in enumerate(ciks):
        for i in np.flatnonzero(rng.random(n) < 0.004):
            filed = dates[min(i + 1, n - 1)]
            rows.append(_row(accession_number=f"p-{cik}-{i}", issuer_cik=str(cik),
                             owner_cik=str(int(rng.integers(1, 6))), filing_date=str(filed.date()),
                             transaction_date=str(dates[i].date())))
            k = min(i + 2, n - 1)
            rets[k:k + 20, j] += 0.004  # +8% over the 20 sessions after the filing is known
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rets, axis=0)), index=dates, columns=tickers)
    prices["XLK"] = 100 * np.exp(np.cumsum(rng.normal(0, 0.008, n)))
    engine = _price_db({c: prices[c] for c in prices.columns})
    # Section 16 activity from the SUBMISSION table only: holdings-only filings
    # with no transaction line, plus the purchases' own accessions.
    events = _events(rows, _submissions_of(rows) + _holdings_only_submissions(ciks, dates))
    assert events.receipt["counts"]["transaction_accessions_missing_from_submissions"] == 0
    universe = pd.DataFrame({"ticker": tickers, "cik": ciks})
    manifest = _manifest(tickers)
    key, inputs = _discovery_key(tmp_path, manifest)
    with engine.connect() as conn:
        discovery = vs1.load_price_panel(conn, manifest, tickers, start=date(2011, 11, 1),
                                         as_of=date(2019, 12, 31), window="discovery", key=key)
    assert max(o["last"] for o in discovery.receipt["series"].values()) <= "2019-12-31"
    panels = vs1.build_trial_panels(events, universe, discovery, "discovery")
    frozen = vs1.discover_panel(_spec("e2e"), panels, sensitivity=True,
                                inputs={"price": discovery.receipt_sha, "inputs_frozen_sha256": key.inputs_frozen_sha256})
    vs1.seal_discovery(tmp_path, NOW, key, frozen)
    ledger = {t["trial"]: t for t in frozen["payload"]["ledger"]}
    primary = ledger["A90|fwd20"]
    assert primary["selected"] and primary["mean_ic"] > 0
    assert primary["magnitude"]["buyer_minus_nonbuyer"] > 0
    assert "momentum_mean_ic" in primary["baseline"]
    assert primary["magnitude"]["small_line_buyer_issuer_dates"] > 0  # $20k lines
    assert primary["labels"]["buyer_missing_share"] == 0.0
    hkey = _holdout_key(tmp_path, frozen, _observed(inputs))
    with engine.connect() as conn:
        holdout = vs1.load_price_panel(conn, manifest, tickers, start=date(2019, 11, 1),
                                       as_of=date(2026, 6, 30), window="holdout", key=hkey)
    result = vs1.evaluate_panel_holdout(frozen, vs1.build_trial_panels(events, universe, holdout, "holdout"),
                                        hkey, power={"gate_passed": True})
    assert result["verdict"]["state"] == "HOLDOUT_SURVIVOR_FORWARD_PENDING"


def _pre_entry_jump_discovery(log_dir):
    """Discovery where every purchase is followed by a +10% jump realised strictly before
    the first close after the filing became public, and by nothing afterwards.

    Filings land on decision sessions ``p`` (both horizon grids). known_at is 22:00 ET
    on ``p``; the first close after it is ``p + 1``; the jump is the move from close
    ``p`` to close ``p + 1`` (e.g. the next morning's reaction to the filing), so it is
    already in the entry close. Filings are independent draws per issuer and grid
    session (memoryless), so the feature built from earlier filings says nothing
    about the next jump: a correct harness sees no effect in either direction.
    """
    rng = np.random.default_rng(21)
    tickers = [f"T{i:02d}" for i in range(24)]
    ciks = list(range(1000, 1024))
    dates = pd.bdate_range("2011-10-03", "2019-12-31")
    n = len(dates)
    first = int(np.flatnonzero(dates >= pd.Timestamp("2012-01-01"))[0])
    rets = rng.normal(0.0, 0.01, (n, len(tickers)))
    rows = []
    grid = range(first, n - 2, 20)
    for j, cik in enumerate(ciks):
        for p in grid:
            if rng.random() >= 0.15:
                continue
            rows.append(_row(accession_number=f"p-{cik}-{p}", issuer_cik=str(cik),
                             owner_cik=str(int(rng.integers(1, 6))), filing_date=str(dates[p].date()),
                             transaction_date=str(dates[p - 1].date()), shares="5000"))
            rets[p + 1, j] += np.log(1.10)
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rets, axis=0)), index=dates, columns=tickers)
    prices["XLK"] = 100 * np.exp(np.cumsum(rng.normal(0, 0.008, n)))
    engine = _price_db({c: prices[c] for c in prices.columns})
    events = _events(rows, _submissions_of(rows) + _holdings_only_submissions(ciks, dates))
    universe = pd.DataFrame({"ticker": tickers, "cik": ciks})
    manifest = _manifest(tickers)
    key, _ = _discovery_key(log_dir, manifest)
    with engine.connect() as conn:
        prices_panel = vs1.load_price_panel(conn, manifest, tickers, start=date(2011, 10, 1),
                                            as_of=date(2019, 12, 31), window="discovery", key=key)
    panels = vs1.build_trial_panels(events, universe, prices_panel, "discovery")
    frozen = vs1.discover_panel(_spec("jump"), panels, sensitivity=False,
                                inputs={"inputs_frozen_sha256": key.inputs_frozen_sha256})
    return {t["trial"]: t for t in frozen["payload"]["ledger"]}


def test_a_jump_before_the_first_close_after_the_filing_is_public_is_not_detected(tmp_path, monkeypatch):
    """Negative control (review item 9): the harness must not credit a price move that
    is already in the entry close. As a check that this test can fail, the same data
    with the filing treated as public at 15:00 ET on its filing date (before that
    day's close) does detect the jump."""
    ledger = _pre_entry_jump_discovery(tmp_path / "correct")
    assert not any(t["selected"] for t in ledger.values())
    primary = ledger["A90|fwd20"]
    assert primary["status"] == "tested" and abs(primary["mean_ic"]) < 0.03
    assert all(t["p"] > 0.05 for t in ledger.values())  # not even an unadjusted hit, either sign
    monkeypatch.setattr(vs1, "KNOWN_AT_LOCAL", time(15, 0))
    leaky = _pre_entry_jump_discovery(tmp_path / "leaky")
    assert leaky["A90|fwd20"]["selected"] and leaky["A90|fwd20"]["mean_ic"] > 0.05


# --- CLI: freeze-inputs -> discover -> holdout, prices behind the chain -----------------------


def _cli_inputs(tmp_path, monkeypatch):
    """Synthetic input files for the CLI and a counting SQLite stand-in for the read-only engine."""
    from scripts import run_real_panel_scan

    rng = np.random.default_rng(31)
    tickers = [f"T{i:02d}" for i in range(20)]
    ciks = list(range(1000, 1020))
    dates = pd.bdate_range("2011-10-03", "2019-12-31")
    rows = []
    for cik in ciks:
        for i in np.flatnonzero(rng.random(len(dates)) < 0.004):
            filed = dates[min(i + 1, len(dates) - 1)]
            rows.append(_row(accession_number=f"p-{cik}-{i}", issuer_cik=str(cik), owner_cik="7",
                             filing_date=str(filed.date()), transaction_date=str(dates[i].date())))
    files = {name: tmp_path / name for name in ("form4.parquet", "submissions.parquet", "company_tickers.json",
                                                "manifest.json", "probe.json", "power.json")}
    pd.DataFrame(rows).to_parquet(files["form4.parquet"])
    pd.DataFrame(_submissions_of(rows) + _holdings_only_submissions(ciks, dates)).to_parquet(
        files["submissions.parquet"])
    files["company_tickers.json"].write_text(json.dumps(
        {str(k): {"cik_str": c, "ticker": t, "title": t} for k, (t, c) in enumerate(zip(tickers, ciks))}))
    files["probe.json"].write_text(json.dumps({"probe": "synthetic"}))
    manifest = replace(_manifest(tickers), probe_report_sha256=vs1.data_sha256(files["probe.json"]))
    files["manifest.json"].write_text(json.dumps({**asdict(manifest), "admitted": list(manifest.admitted)}))
    sector_map = _map([("Technology", "a", t, 0.1, "company") for t in tickers])
    monkeypatch.setattr(vs1, "load_sector_map", lambda repo_root=None: sector_map)
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.01, (len(dates), 21)), axis=0)),
                          index=dates, columns=tickers + ["XLK"])
    engine = _price_db({c: prices[c] for c in prices.columns})
    reads = []

    def fake_engine(timeout_s, name):
        reads.append(name)
        return engine

    monkeypatch.setattr(run_real_panel_scan, "read_only_engine", fake_engine)
    return files, reads


def _cli_args(files, log_dir, *extra):
    return [
        "--form4", str(files["form4.parquet"]), "--submissions", str(files["submissions.parquet"]),
        "--issuer-map", str(files["company_tickers.json"]), "--log-dir", str(log_dir),
        "--price-manifest", str(files["manifest.json"]), "--probe-report", str(files["probe.json"]),
        "--power", str(files["power.json"]), *extra,
    ]


def _git(cwd, *argv):
    import subprocess

    result = subprocess.run(
        ["git", "-c", "user.name=vs1-test", "-c", "user.email=vs1-test@example.invalid",
         "-c", "commit.gpgsign=false", *argv],
        cwd=cwd, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


def _vault(tmp_path):
    """A stand-in vault: a git work tree with a bare 'origin' to push to."""
    remote, vault = tmp_path / "remote.git", tmp_path / "vault"
    _git(tmp_path, "init", "-q", "--bare", str(remote))
    _git(tmp_path, "init", "-q", str(vault))
    _git(vault, "remote", "add", "origin", str(remote))
    (vault / "README.md").write_text("vault\n")
    _git(vault, "add", "README.md")
    _git(vault, "commit", "-q", "-m", "init")
    _git(vault, "push", "-q", "origin", "HEAD:refs/heads/main")
    return vault, vault / "05-GRID" / "vs1" / vs1.REGISTRY_ANCHORS


def test_cli_discovery_and_holdout_are_one_shot_and_need_a_pushed_offhost_witness(tmp_path, monkeypatch):
    from argparse import Namespace

    from scripts import run_vs1_insider_density as cli

    files, reads = _cli_inputs(tmp_path, monkeypatch)
    log_dir = tmp_path / "registry"
    vault, anchors = _vault(tmp_path)
    witness = ["--external-anchors", str(anchors)]
    # the power file: pre-registered settings, computed on these inputs (a synthetic table)
    ns = Namespace(sector="Technology", issuer_map=str(files["company_tickers.json"]),
                   form4=str(files["form4.parquet"]), submissions=str(files["submissions.parquet"]), owners=None)
    universe, info = cli._universe(ns)
    events = cli._events(ns, universe)
    files["power.json"].write_text(json.dumps({**_power_file(gate_power=0.6),
                                               "inputs": cli._power_inputs(events, info)}))

    cli.main(["register", "--log-dir", str(log_dir)])
    assert vs1.registry(log_dir).verify_chain()["head_sha256"] == vs1.REGISTERED_RECORD_SHA256[1]
    run_dir = tmp_path / "run"
    discover = ["discover", *_cli_args(files, log_dir, *witness, "--out", str(run_dir))]
    # nothing is read before inputs are frozen, a discovery is opened and witnessed off-host
    with pytest.raises(SystemExit, match="does not exist"):
        cli.main(discover)
    with pytest.raises(PermissionError, match="inputs_frozen"):
        cli.main(["open-discovery", *_cli_args(files, log_dir)])
    cli.main(["freeze-inputs", *_cli_args(files, log_dir, "--as-of-ts", AS_OF_TS, "--code-sha", "c" * 40)])
    frozen_inputs = vs1.latest_frozen_inputs(log_dir)
    assert frozen_inputs["as_of_ts"] == AS_OF_TS and frozen_inputs["accept_underpowered"] is False

    # the vault log witnesses the chain up to inputs_frozen (committed and pushed) ...
    vs1.export_anchors(log_dir, anchors)
    _git(vault, "add", "-A")
    _git(vault, "commit", "-q", "-m", "vs1 anchors: inputs_frozen")
    _git(vault, "push", "-q", "origin", "HEAD:refs/heads/main")
    # ... but not discovery_opened: discover refuses (the missing-anchor refusal)
    cli.main(["open-discovery", *_cli_args(files, log_dir)])
    assert _kinds(log_dir)[-1] == "discovery_opened"
    with pytest.raises(PermissionError, match="covers 3 records"):
        cli.main(discover)
    # exported but not committed, then committed but not pushed: still refused
    vs1.export_anchors(log_dir, anchors)
    with pytest.raises(SystemExit, match="not committed"):
        cli.main(discover)
    _git(vault, "commit", "-q", "-am", "vs1 anchors: discovery_opened")
    with pytest.raises(SystemExit, match="push it first"):
        cli.main(discover)
    assert reads == [] and not run_dir.exists()
    # pushed: the discovery reads prices once and seals
    _git(vault, "push", "-q", "origin", "HEAD:refs/heads/main")
    cli.main(discover)
    assert reads == ["vs1_insider_density"]
    assert _kinds(log_dir)[-3:] == ["inputs_frozen", "discovery_opened", "discovery_frozen"]
    frozen = json.loads((run_dir / "discovery-frozen.json").read_text())
    assert vs1.registry(log_dir).read_all()[-1]["discovery_sha256"] == frozen["sha256"]
    assert frozen["payload"]["inputs"]["offhost_witness"]["path"] == "05-GRID/vs1/" + vs1.REGISTRY_ANCHORS
    with pytest.raises(PermissionError, match="one shot"):
        cli.main(["discover", *_cli_args(files, log_dir, *witness, "--out", str(tmp_path / "run2"))])
    with pytest.raises(PermissionError, match="one shot"):
        cli.main(["open-discovery", *_cli_args(files, log_dir)])

    request = ["--run-dir", str(run_dir), "--allow-holdout", "--prereg-sha256", vs1.PREREG_BODY_SHA256]
    # a different price manifest: refused before any price read and before holdout_opened
    good_manifest = files["manifest.json"].read_text()
    changed = json.loads(good_manifest)
    changed["basis"] = "split adjusted"
    files["manifest.json"].write_text(json.dumps(changed))
    with pytest.raises(SystemExit, match="price manifest differs"):
        cli.main(["open-holdout", *request, *_cli_args(files, log_dir)])
    with pytest.raises(SystemExit, match="price manifest differs"):
        cli.main(["holdout", *request, *_cli_args(files, log_dir, *witness)])
    files["manifest.json"].write_text(good_manifest)
    # a different power file: same
    good_power = files["power.json"].read_text()
    files["power.json"].write_text(json.dumps({**json.loads(good_power), "note": "edited"}))
    with pytest.raises(SystemExit, match="power file differs"):
        cli.main(["holdout", *request, *_cli_args(files, log_dir, *witness)])
    files["power.json"].write_text(good_power)
    # no flag
    with pytest.raises(PermissionError, match="allow_holdout"):
        cli.main(["open-holdout", "--run-dir", str(run_dir), "--prereg-sha256", vs1.PREREG_BODY_SHA256,
                  *_cli_args(files, log_dir)])
    assert "holdout_opened" not in _kinds(log_dir)
    # holdout before holdout_opened: refused
    with pytest.raises(PermissionError, match="0 holdout_opened"):
        cli.main(["holdout", *request, *_cli_args(files, log_dir, *witness)])
    # opened and exported to the vault log, but not committed: refused, no price read
    cli.main(["open-holdout", *request, *_cli_args(files, log_dir, *witness)])
    assert _kinds(log_dir)[-1] == "holdout_opened"
    with pytest.raises(SystemExit, match="not committed"):
        cli.main(["holdout", *request, *_cli_args(files, log_dir, *witness)])
    with pytest.raises(PermissionError, match="already opened"):
        cli.main(["open-holdout", *request, *_cli_args(files, log_dir)])
    assert reads == ["vs1_insider_density"]


# --- registry (research_forward_log chain) ---------------------------------------------------


def test_registry_appends_a_pinned_header_and_one_registration(tmp_path):
    records = _register(tmp_path)
    assert [r["kind"] for r in records] == ["header", "preregistration"]
    assert records[0]["prereg_sha256"] == vs1.PREREG_BODY_SHA256
    assert records[1]["runs"]["vs1"]["alpha"] == pytest.approx(0.05)
    log = vs1.registry(tmp_path)
    check = log.verify_chain()
    assert check["ok"] and check["records"] == 2 and check["anchored_records"] == 2
    assert check["head_sha256"] == vs1.REGISTERED_RECORD_SHA256[1]
    with pytest.raises(ValueError, match="already registered"):
        _register(tmp_path)
    # the S10 forward log's pin does not accept this registry's header
    from analysis.research_forward_log import ForwardLog

    other = ForwardLog(tmp_path, log_filename=vs1.REGISTRY_LOG, anchor_filename=vs1.REGISTRY_ANCHORS,
                       lock_filename=vs1.REGISTRY_LOCK)
    assert not other.verify_chain()["ok"]


def test_registry_detects_an_edited_line(tmp_path):
    _register(tmp_path)
    path = tmp_path / vs1.REGISTRY_LOG
    lines = path.read_bytes().split(b"\n")
    lines[1] = lines[1].replace(b'"ledger_id":"grid-granular-panel"', b'"ledger_id":"grid-granular-panel2"')
    path.write_bytes(b"\n".join(lines))
    assert not vs1.registry(tmp_path).verify_chain()["ok"]


# --- the pinned registration and the off-host witness (re-review of #697 at 7c28f281) --------


def test_the_pin_is_the_real_vs1_registration():
    """Header + preregistration as registered 2026-09-27T09:27:45Z against d1e13f6e:
    chain head 5b10ff57... at 2 records, reproduced from code."""
    assert vs1.REGISTERED_RECORD_SHA256[1] == "5b10ff57c48c68164fbef9100c174f62d26be3c87e45928cd122549c9bcf7508"
    records = vs1.registration_records(vs1.REGISTERED_AT, vs1.REGISTERED_CODE_SHA)
    assert tuple(vs1.chained_sha256(records)) == vs1.REGISTERED_RECORD_SHA256
    assert vs1.REGISTERED_PREREG_SHA256 == vs1.PREREG_BODY_SHA256


def test_a_copy_of_the_registration_is_byte_identical_to_the_original(tmp_path):
    _register(tmp_path)
    anchors = (tmp_path / vs1.REGISTRY_ANCHORS).read_bytes()
    assert anchors == (
        b'{"head_sha256":"5b10ff57c48c68164fbef9100c174f62d26be3c87e45928cd122549c9bcf7508",'
        b'"prev_anchor_sha256":null,"records":2,"run_at":"2026-09-27T09:27:45.013793+00:00"}\n'
    )


def test_a_fresh_or_re_registered_registry_is_refused_as_a_fork(tmp_path):
    # register refuses to start any other registration
    with pytest.raises(PermissionError, match="fork"):
        vs1.register(tmp_path / "fresh", NOW, "c" * 40)
    with pytest.raises(PermissionError, match="fork"):
        vs1.register(tmp_path / "fresh", vs1.REGISTERED_AT, "c" * 40)
    assert not (tmp_path / "fresh" / vs1.REGISTRY_LOG).exists()
    # a registration written around register() is refused by every stage
    forged = tmp_path / "forged"
    vs1.registry(forged).append(vs1.registration_records(NOW, vs1.REGISTERED_CODE_SHA))
    assert vs1.registry(forged).verify_chain()["ok"]  # a valid chain, just not the pinned one
    with pytest.raises(PermissionError, match="fork"):
        vs1.freeze_inputs(forged, NOW, _frozen_inputs())
    with pytest.raises(PermissionError, match="fork"):
        vs1.open_discovery(forged, NOW, _observed(_frozen_inputs()))
    with pytest.raises(PermissionError, match="fork"):
        vs1.latest_frozen_inputs(forged)
    # an empty directory is not a registry at all
    with pytest.raises(PermissionError, match="not registered"):
        vs1.freeze_inputs(tmp_path / "empty", NOW, _frozen_inputs())


def test_a_prefix_copy_fork_can_open_locally_but_never_gets_a_price_key(tmp_path):
    real, fork = tmp_path / "real", tmp_path / "fork"
    key, inputs = _discovery_key(real)  # the real run, witnessed off-host
    offhost = _offhost(real)
    # fork: a byte copy of the registered 2-record prefix, other inputs
    fork.mkdir()
    for name in (vs1.REGISTRY_LOG, vs1.REGISTRY_ANCHORS):
        lines = (real / name).read_bytes().split(b"\n")
        (fork / name).write_bytes(b"\n".join(lines[: 2 if name == vs1.REGISTRY_LOG else 1]) + b"\n")
    assert vs1.registry(fork).verify_chain()["head_sha256"] == vs1.REGISTERED_RECORD_SHA256[1]
    other = _frozen_inputs(form4_sha256="7" * 64)
    vs1.freeze_inputs(fork, NOW, other)
    vs1.open_discovery(fork, NOW, _observed(other))  # locally indistinguishable
    with pytest.raises(PermissionError, match="does not witness"):
        vs1.resume_discovery(fork, _observed(other), offhost)
    # and the off-host log cannot be extended with the fork's chain
    with pytest.raises(PermissionError, match="another registry chain"):
        vs1.export_anchors(fork, offhost)
    assert key.inputs_frozen_sha256 != vs1.latest_frozen_inputs(fork)


def test_a_deleted_and_recreated_registry_is_refused_by_the_offhost_log(tmp_path):
    import shutil

    log_dir = tmp_path / "registry"
    _, inputs = _discovery_key(log_dir)
    offhost = _offhost(log_dir)
    shutil.rmtree(log_dir)
    _register(log_dir)
    retry = _frozen_inputs(as_of_ts="2026-09-26T12:00:00+00:00")
    vs1.freeze_inputs(log_dir, NOW, retry)
    vs1.open_discovery(log_dir, NOW, _observed(retry))
    with pytest.raises(PermissionError, match="does not witness"):
        vs1.resume_discovery(log_dir, _observed(retry), offhost)


def test_discovery_is_refused_until_the_offhost_log_contains_discovery_opened(tmp_path):
    _register(tmp_path)
    inputs = _frozen_inputs()
    vs1.freeze_inputs(tmp_path, NOW, inputs)
    early = _witness(tmp_path)  # witnessed up to inputs_frozen only
    opened = vs1.open_discovery(tmp_path, NOW, _observed(inputs))
    assert opened["records"] == 4 and opened["head_sha256"] == vs1.registry(tmp_path).verify_chain()["head_sha256"]
    with pytest.raises(PermissionError, match="off-host anchor log"):
        vs1.resume_discovery(tmp_path, _observed(inputs), None)
    with pytest.raises(PermissionError, match="does not exist"):
        vs1.resume_discovery(tmp_path, _observed(inputs), tmp_path.parent / "missing.jsonl")
    with pytest.raises(PermissionError, match="covers 3 records"):
        vs1.resume_discovery(tmp_path, _observed(inputs), early)
    # a hand-edited off-host log (head not this chain's) is refused too
    lines = early.read_bytes().split(b"\n")
    edited = tmp_path.parent / "edited.jsonl"
    edited.write_bytes(lines[0].replace(b"5b10ff57", b"5b10ff58") + b"\n")
    with pytest.raises(PermissionError, match="does not witness"):
        vs1.resume_discovery(tmp_path, _observed(inputs), edited)
    # once exported (and, for the CLI, committed and pushed): the key
    key = vs1.resume_discovery(tmp_path, _observed(inputs), _witness(tmp_path))
    assert key.as_of_ts == datetime(2026, 9, 26, tzinfo=UTC)
    # the witness does not reopen anything: inputs still have to match
    with pytest.raises(PermissionError, match="form4_sha256"):
        vs1.resume_discovery(tmp_path, {**_observed(inputs), "form4_sha256": "e" * 64}, _offhost(tmp_path))


def test_an_offhost_log_with_windows_line_endings_is_accepted(tmp_path):
    _register(tmp_path)
    inputs = _frozen_inputs()
    vs1.freeze_inputs(tmp_path, NOW, inputs)
    vs1.open_discovery(tmp_path, NOW, _observed(inputs))
    witness = _witness(tmp_path)
    witness.write_bytes(witness.read_bytes().replace(b"\n", b"\r\n"))
    vs1.resume_discovery(tmp_path, _observed(inputs), witness)
    assert vs1.export_anchors(tmp_path, witness) == []  # nothing new, prefix recognised


def test_holdout_is_refused_until_the_offhost_log_contains_holdout_opened(sealed_null):
    frozen, observed, log_dir = sealed_null
    before = _witness(log_dir)  # covers discovery_frozen, not holdout_opened
    with pytest.raises(PermissionError, match="0 holdout_opened"):
        _resume_holdout(log_dir, frozen, observed, before)
    _open_holdout(log_dir, frozen, observed)
    with pytest.raises(PermissionError, match="covers"):
        _resume_holdout(log_dir, frozen, observed, before)
    with pytest.raises(PermissionError, match="off-host anchor log"):
        _resume_holdout(log_dir, frozen, observed, None)
    key = _resume_holdout(log_dir, frozen, observed, _witness(log_dir))
    assert key.frozen_sha256 == frozen["sha256"]


def test_forward_log_defaults_are_unchanged():
    from analysis import research_forward_log as fl

    log = fl.ForwardLog(Path("x"))
    assert log.path.name == fl.LOG_FILENAME and log.prereg_sha256 == fl.PREREG_SHA256
    with pytest.raises(ValueError):
        fl.ForwardLog(Path("x"), prereg_sha256="nothex")
