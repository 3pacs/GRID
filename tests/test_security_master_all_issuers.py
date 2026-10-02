"""Tests for the all-issuers ``security_master`` seed (builder + loader).

Fixtures only: no network, no database, no real SEC files. The Postgres proof lives in
``tests/test_apply_security_master_seed_pg.py``.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
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
        ("a2", "2016-05-10", "100", "OLD"),
        ("a3", "2018-01-15", "100", "NEW"),
        ("a4", "2019-07-01", "100", "NEW"),
    ])
    assert set(_tickers(built, 100)) == {
        ("OLD", "2015-03-02", "2018-01-14"),  # the day before the first later filing naming another ticker
        ("NEW", "2018-01-15", None),          # still the latest
    }


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
    assert set(_tickers(built, 400)) == {
        ("GOOG", "2010-01-04", "2016-04-03"), ("GOOGL", "2012-02-02", "2016-04-03"), ("XYZ", "2016-04-04", None)}


def test_an_old_ticker_ends_even_if_the_next_filing_is_in_a_multi_ticker_field():
    built = _build([
        ("a1", "2010-01-04", "300", "AAA"),
        ("a2", "2012-06-01", "300", "BBB, BBC"),
    ])
    assert set(_tickers(built, 300)) == {
        ("AAA", "2010-01-04", "2012-05-31"), ("BBB", "2012-06-01", None), ("BBC", "2012-06-01", None)}


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


# --- conflicts: a ticker reused by a different CIK ------------------------------------------


def _reuse_rows():
    return [
        # CIK 1 used REUSE from 2008 and is still filing under it, CIK 2 starts using it in 2015: contested.
        ("a1", "2008-01-02", "1", "REUSE"),
        ("a2", "2020-01-02", "1", "REUSE"),
        ("b1", "2015-06-01", "2", "REUSE"),
        # a clean ticker change on CIK 3 (not a conflict)
        ("c1", "2010-01-04", "3", "CHG"),
        ("c2", "2012-01-04", "3", "CHG2"),
    ]


def test_ticker_reuse_by_a_different_cik_is_flagged_never_silently_resolved():
    built = _build(_reuse_rows())
    rows = [r for r in built["security_identifiers"] if r["id_value"] == "REUSE"]
    assert len(rows) == 2 and all(r["conflict_flag"] for r in rows)
    assert all(r["is_primary"] is False for r in rows)
    assert {r["entity_id"] for r in rows} == {"sm_0000000001", "sm_0000000002"}
    detail = {r["entity_id"]: r["conflict_detail"] for r in rows}
    assert detail["sm_0000000001"]["other_entities"] == ["sm_0000000002"]
    assert detail["sm_0000000001"]["kind"] == "overlapping_ticker_claim"
    assert [c["ticker"] for c in built["conflicts"]] == ["REUSE"]
    assert built["conflicts"][0]["pairs"][0]["overlap_from"] == "2015-06-01"
    assert built["report"]["conflict_tickers"] == 1 and built["report"]["conflict_identifier_rows"] == 2
    unrelated = [r for r in built["security_identifiers"] if r["id_value"] in {"CHG", "CHG2"}]
    assert unrelated and not any(r["conflict_flag"] for r in unrelated)


def test_sequential_reuse_with_closed_windows_is_not_a_conflict():
    built = _build([
        ("a1", "2008-01-02", "1", "SEQ"),
        ("a2", "2009-01-02", "1", "SEQ2"),   # CIK 1 moved on, so SEQ closes 2009-01-01
        ("b1", "2015-06-01", "2", "SEQ"),
    ])
    assert not built["conflicts"]
    assert _tickers(built, 1)[("SEQ", "2008-01-02", "2009-01-01")]["conflict_flag"] is False


def test_handoff_clamp_only_closes_silent_issuers_and_can_be_turned_off():
    rows = [
        ("a1", "2008-01-02", "1", "GONE"),     # CIK 1 stopped filing in 2008 and never changed ticker
        ("b1", "2015-06-01", "2", "GONE"),
        ("c1", "2008-01-02", "5", "LIVE"),     # CIK 5 is still filing under LIVE after CIK 6 starts: contested
        ("c2", "2020-01-02", "5", "LIVE"),
        ("d1", "2015-06-01", "6", "LIVE"),
    ]
    literal = _build(rows, handoff_clamp=False)
    assert {c["ticker"] for c in literal["conflicts"]} == {"GONE", "LIVE"}
    clamped = _build(rows)
    assert {c["ticker"] for c in clamped["conflicts"]} == {"LIVE"}
    gone = _tickers(clamped, 1)[("GONE", "2008-01-02", "2015-05-31")]
    assert gone["conflict_flag"] is False and gone["conflict_detail"]["kind"] == "ticker_reuse_handoff"
    assert clamped["report"]["handoff_windows_closed"] == 1


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
    assert on_disk["code"]["git_head"] == "deadbeef" and len(on_disk["code"]["builder_file_sha256"]) == 64
    assert on_disk["inputs"][0]["sha256"] == builder.sha256_file(src)
    assert on_disk["counts"]["entities"] == 1 and on_disk["writes_to_database"] is False
    assert on_disk["output"]["sha256"] == receipt["output"]["sha256"]
    with pytest.raises(FileExistsError):
        builder.write_artifact(built, tmp_path / "out", inputs=inputs, params={})


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


def test_resolve_securities_counts_conflicts_and_prefers_the_non_conflicted_row():
    built = _build(_reuse_rows())
    out = S.resolve_securities(_events([(None, "REUSE", "2016-01-01"), (None, "REUSE", "2010-01-01")]), _identifiers_frame(built))
    # 2016: both CIKs claim it (conflict, resolved deterministically by latest valid_from); 2010: only CIK 1 does.
    assert list(out["security_id"]) == ["sm_0000000002", "sm_0000000001"]
    assert list(out["security_conflict"]) == [True, True]


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


def test_new_ticker_overlapping_another_entitys_existing_row_is_inserted_flagged(tmp_path):
    plan = loader.plan_inserts(_aapl_seed(tmp_path), _tech_existing())
    cflt = [r for r in plan.si_inserts if r["id_value"] == "CFLT"]
    assert len(cflt) == 1
    assert cflt[0]["conflict_flag"] is True and cflt[0]["is_primary"] is False
    assert cflt[0]["conflict_detail"]["existing_db_entities"] == ["sm_tkr_CFLT"]
    assert plan.summary()["new_ticker_rows_overlapping_existing_other_entity"] == 1


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


def _args(seed_dir, **kw):
    base = dict(seed_dir=seed_dir, apply=False, db_url=None, expect_output_sha256=None, batch_size=2,
                statement_timeout_ms=60000, lock_timeout_ms=5000, receipt=None)
    base.update(kw)
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
