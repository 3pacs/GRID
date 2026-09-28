"""Direct tests for ``ingestion/altdata/verified_13f_filers.py`` -- the
single verified 13F filer-CIK map built 2026-09-28 from the union of the
three old contradictory lists, each CIK checked against SEC's own
registrant data. See the module docstring and
``verified_13f_filers_evidence.json`` for methodology.
"""

from __future__ import annotations

import json
from pathlib import Path

from ingestion.altdata.verified_13f_filers import (
    DROPPED_FILERS,
    VERIFIED_FILERS,
    Filer,
    filer_by_cik,
    filer_by_key,
)

_EVIDENCE_PATH = Path(__file__).resolve().parent.parent / "ingestion" / "altdata" / "verified_13f_filers_evidence.json"


def test_filer_by_key_known():
    f = filer_by_key("berkshire_hathaway")
    assert f is not None
    assert f.cik == "1067983"


def test_filer_by_key_unknown_returns_none():
    assert filer_by_key("not_a_real_filer") is None


def test_filer_by_cik_known():
    f = filer_by_cik("1067983")
    assert f is not None
    assert f.key == "berkshire_hathaway"


def test_filer_by_cik_handles_zero_padding_and_int():
    plain = filer_by_cik("1067983")
    assert filer_by_cik("0001067983") == plain
    assert filer_by_cik(1067983) == plain


def test_all_entries_are_filer_instances():
    for f in VERIFIED_FILERS:
        assert isinstance(f, Filer)
        assert f.key and f.cik and f.display_name
        assert f.cik.isdigit()


def test_dropped_filers_documented_with_searched_terms():
    for d in DROPPED_FILERS:
        assert "manager_label" in d
        assert "old_cik" in d
        assert "searched_terms" in d
        assert len(d["searched_terms"]) >= 2


def test_evidence_file_exists_and_is_valid_json():
    assert _EVIDENCE_PATH.exists(), f"evidence file missing at {_EVIDENCE_PATH}"
    data = json.loads(_EVIDENCE_PATH.read_text(encoding="utf-8"))
    assert data["final_verified_filer_count"] == len(VERIFIED_FILERS)
    assert len(data["union_pull_99_ciks"]) == 99
    assert data["dropped_unconfirmed"]
    for entry in data["union_pull_99_ciks"]:
        assert "cik" in entry
        assert "fetch_date" in entry


def test_evidence_file_ciks_are_traceable_for_every_verified_filer():
    # Every VERIFIED_FILERS CIK must appear either in the raw 99-CIK union
    # pull or in the separately-searched replacement list -- no filer's CIK
    # should be unexplainable from the evidence file.
    data = json.loads(_EVIDENCE_PATH.read_text(encoding="utf-8"))
    union_ciks = {e["cik"] for e in data["union_pull_99_ciks"]}
    replacement_ciks = {e["cik"] for e in data["replacement_ciks_found_via_sec_search"]}
    documented = union_ciks | replacement_ciks
    for f in VERIFIED_FILERS:
        assert f.cik in documented, f"{f.key} ({f.cik}) has no evidence trail"
