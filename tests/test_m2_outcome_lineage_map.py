"""Hold the M2 outcome-lineage draft (#960) to the code it cites.

The protocol document and its JSON companion describe existing machinery. These
tests fail when that machinery moves or changes shape (a verdict or claim type is
added, a cited file is renamed, the packet stops flagging missing decision
vintages), so the draft is revised instead of silently going stale. They read
files only: no database, network or model.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MAP_PATH = ROOT / "docs" / "research" / "m2-outcome-lineage-v0.json"
DOC_PATH = ROOT / "docs" / "research" / "m2-outcome-lineage-protocol-v0.md"


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def lineage() -> dict:
    return json.loads(MAP_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def ingest():
    return _load("ingest_publication_review_for_m2_map", "scripts/ingest_publication_review.py")


@pytest.fixture(scope="module")
def doc() -> str:
    return DOC_PATH.read_text(encoding="utf-8")


def test_status_is_draft_and_authorises_nothing(lineage: dict, doc: str) -> None:
    assert lineage["status"] == "DRAFT_UNREVIEWED"
    assert lineage["authorises"] == []
    assert lineage["revision"] == 6
    assert "DRAFT v0, revision 6" in doc and "authorises nothing" in doc
    assert lineage["document"] == str(DOC_PATH.relative_to(ROOT))


def _cited_paths(lineage: dict) -> set[str]:
    paths: set[str] = set()
    for link in lineage["chain"]:
        paths.update(link["code"])
    for stage in lineage["stages"]:
        paths.update(stage["code"])
    paths.add(lineage["cofiring"]["code"])
    paths.update(s["file"] for s in lineage["symbols"])
    return paths


def test_every_cited_path_exists(lineage: dict) -> None:
    missing = sorted(p for p in _cited_paths(lineage) if not (ROOT / p).exists())
    assert missing == []


def test_cited_symbols_exist(lineage: dict) -> None:
    for sym in lineage["symbols"] + [{"file": lineage["cofiring"]["code"],
                                      "contains": f"def {lineage['cofiring']['function']}("}]:
        assert sym["contains"] in (ROOT / sym["file"]).read_text(encoding="utf-8"), sym


def test_stages_are_s0_to_s8_in_order(lineage: dict, doc: str) -> None:
    assert [s["stage"] for s in lineage["stages"]] == [f"S{i}" for i in range(9)]
    for stage in lineage["stages"]:
        assert f"| {stage['stage']} " in doc, stage["stage"]


def test_claim_types_match_ingest_and_grader(lineage: dict, ingest) -> None:
    assert set(lineage["claim_types"]) == set(ingest.CLAIM_TYPES)
    grader_src = (ROOT / "scripts" / "grade_publication_review.py").read_text(encoding="utf-8")
    match = re.search(r"^CLAIM_TYPES = frozenset\(\{(.*?)\}\)", grader_src, re.M)
    assert match, "grader no longer defines CLAIM_TYPES as a frozenset literal"
    assert set(re.findall(r'"([a-z_]+)"', match.group(1))) == set(lineage["claim_types"])


RESOLVED_STATE = {"confirmed": "CONFIRMED", "refuted": "REFUTED", "partially_confirmed": "PARTIAL"}
UNAVAILABLE_DATA = {"MISSING_BASELINE", "MISSING_ENDPOINT", "UNUSABLE_ENDPOINT", "NO_SOURCE"}


QUALIFYING_WITNESSES = {"study_read", "timed_grader_confirmation"}


def run_visibility(run: dict, read_at: datetime) -> str:
    """VISIBLE on a positive witness by R; NOT_VISIBLE only on negative evidence; else VISIBILITY_UNKNOWN.

    ingested_at is never read. A positive witness after R proves nothing about R.
    """
    witness = run.get("witness")
    if (witness and witness.get("kind") in QUALIFYING_WITNESSES and witness.get("observed_at") is not None
            and witness.get("run_id") == run["run_id"] and witness.get("review_sha256") == run["review_sha256"]
            and datetime.fromisoformat(witness["observed_at"]) <= read_at):
        return "VISIBLE"
    for read in run.get("negative_reads", []):
        if negative_read_proves_absence(read, run, read_at):
            return "NOT_VISIBLE"
    commit = run.get("authoritative_commit")
    if (commit and commit.get("source") == "trusted" and commit.get("run_id") == run["run_id"]
            and commit.get("review_sha256") == run["review_sha256"] and commit.get("committed_at")
            and datetime.fromisoformat(commit["committed_at"]) > read_at):
        return "NOT_VISIBLE"
    return "VISIBILITY_UNKNOWN"


def negative_read_proves_absence(read: dict, run: dict, read_at: datetime) -> bool:
    """Every condition must hold; a missing field or an error never proves absence."""
    acquired = read.get("snapshot_acquired_at")
    if (read.get("source") != "primary" or read.get("error") is not None or not acquired
            or datetime.fromisoformat(acquired) < read_at
            or read.get("table") != "publication_review_runs"):
        return False
    if read.get("lookup") == "exact_identity":
        return read.get("found") is False
    if read.get("lookup") == "scoped_list":
        ids = read.get("returned_run_ids")
        return (read.get("complete") is True and isinstance(ids, list)
                and read.get("publication_day") == run.get("publication_day") and run["run_id"] not in ids)
    return False


def derive(row: dict, rules: dict, read_at: datetime) -> dict:
    """Doc section 2.2 as code: maturity, data and state for one verdict row at read instant R."""
    horizon = row["horizon_end"]
    if horizon is None:
        maturity = "NO_HORIZON"
    else:
        maturity = "IMMATURE" if datetime.fromisoformat(horizon) > read_at else "MATURE"
    verdict = row["verdict"]
    if verdict in RESOLVED_STATE:
        data = "PRESENT"
    elif verdict == "unresolved":
        if row["claim_type"] in {"event", "causal"} or maturity == "NO_HORIZON":
            data = "NOT_GRADABLE"
        else:
            data = "MISSING_ENDPOINT" if maturity == "MATURE" else "AWAITING_ENDPOINT"
    else:  # unscorable
        cause = row.get("root_cause") or ""
        data = next((r["data"] for r in rules["unscorable_root_cause_prefixes"] if cause.startswith(r["prefix"])),
                    "UNKNOWN_CAUSE")
    graded_at = datetime.fromisoformat(row["graded_at"])
    visibility = run_visibility(row["run"], read_at)
    if visibility != "VISIBLE":
        state = visibility
    elif data == "UNKNOWN_CAUSE":
        state = "UNKNOWN"
    elif maturity == "IMMATURE":
        state = "PENDING"
    elif maturity == "NO_HORIZON" or data == "NOT_GRADABLE":
        state = "UNMEASURABLE"
    elif data == "PRESENT":
        state = RESOLVED_STATE[verdict]
    elif data in UNAVAILABLE_DATA:
        # Only a grading at or after the horizon can say the input was missing at maturity.
        state = "UNAVAILABLE" if graded_at >= datetime.fromisoformat(horizon) else "NOT_ASSESSED"
    else:
        raise AssertionError(f"no state for {maturity}/{data}")
    return {"maturity": maturity, "data": data, "state": state}


def test_derivation_fixtures(lineage: dict) -> None:
    rules = lineage["derivation"]
    read_at = datetime.fromisoformat(rules["read_at"])
    for fixture in rules["fixtures"]:
        assert derive(fixture, rules, read_at) == fixture["expect"], fixture["name"]


def test_fixtures_cover_every_verdict_and_the_reviewed_cases(lineage: dict, ingest) -> None:
    fixtures = lineage["derivation"]["fixtures"]
    assert {f["verdict"] for f in fixtures} == set(ingest.VERDICTS)
    expects = {(f["expect"]["maturity"], f["expect"]["data"]) for f in fixtures}
    # Review 6033027329 (1): due-but-missing endpoint and pre-maturity missing baseline are distinct cases.
    assert ("MATURE", "MISSING_ENDPOINT") in expects
    assert ("IMMATURE", "AWAITING_ENDPOINT") in expects
    assert ("IMMATURE", "MISSING_BASELINE") in expects
    # Review 6033670709: paired same-cause pre/post-horizon gradings, and a row recorded after R.
    states = {f["name"]: f["expect"]["state"] for f in fixtures}
    assert {states[n] for n in states if n.startswith("pair A")} == {"NOT_ASSESSED", "UNAVAILABLE"}
    assert {states[n] for n in states if n.startswith("pair B")} == {"NOT_ASSESSED", "UNAVAILABLE"}
    assert "NOT_VISIBLE" in states.values()
    # Review 6033699395: a transaction straddling R, and runs with no or a mismatched witness.
    straddle = next(f for f in fixtures if f["name"].startswith("transaction straddling R"))
    assert datetime.fromisoformat(straddle["run"]["ingested_at"]) < datetime.fromisoformat(lineage["derivation"]["read_at"])
    assert straddle["expect"]["state"] == "VISIBILITY_UNKNOWN"
    assert list(states.values()).count("VISIBILITY_UNKNOWN") >= 3
    # Review 6033821128: NOT_VISIBLE only on genuine negative evidence, in pairs with the unproven case.
    assert states["pair C1: positive witness only after R, no negative evidence"] == "VISIBILITY_UNKNOWN"
    assert states["pair C2: same, plus a scoped read at R that did not return the run"] == "NOT_VISIBLE"
    assert states["pair C3: authoritative commit after R"] == "NOT_VISIBLE"
    for f in fixtures:
        if f["expect"]["state"] == "NOT_VISIBLE":
            assert f["run"]["negative_reads"] or f["run"]["authoritative_commit"], f["name"]
    # Review 6033941569: stale snapshots, replicas, partial/missing lists, errors and mis-bound commits stay UNKNOWN.
    for name in ("neg: stale snapshot acquired before R, completed after", "neg: replica read",
                 "neg: partial (filtered or paginated) list", "neg: list missing returned_run_ids",
                 "neg: errored read", "neg: exact identity lookup without a found field",
                 "commit proof bound to another run", "commit proof from an untrusted source"):
        assert states[name] == "VISIBILITY_UNKNOWN", name
    assert states["neg: exact identity lookup found no row"] == "NOT_VISIBLE"


def test_ingested_at_never_decides_visibility(lineage: dict) -> None:
    rules = lineage["derivation"]
    read_at = datetime.fromisoformat(rules["read_at"])
    for fixture in rules["fixtures"]:
        moved = json.loads(json.dumps(fixture))
        moved["run"]["ingested_at"] = "1970-01-01T00:00:00+00:00"
        assert derive(moved, rules, read_at)["state"] == derive(fixture, rules, read_at)["state"], fixture["name"]


def test_supersession_reads_the_newest_run_witnessed_by_r(lineage: dict) -> None:
    sup = lineage["derivation"]["supersession_fixture"]
    for case in sup["cases"]:
        read_at = datetime.fromisoformat(case["read_at"])
        # The run set is frozen at R: only runs positively witnessed by R exist for the study.
        frozen = [r for r in sup["runs"] if run_visibility(r, read_at) == "VISIBLE"]
        chosen = max(frozen, key=lambda r: r["run_id"])["run_id"] if frozen else None
        assert chosen == case["expect_run_id"], case
        # A superseding run first seen after R is never substituted in.
        for r in sup["runs"]:
            if r not in frozen:
                assert run_visibility(r, read_at) != "VISIBLE"


def test_pending_and_unknown_never_enter_the_matured_denominator(lineage: dict) -> None:
    rules = lineage["derivation"]
    read_at = datetime.fromisoformat(rules["read_at"])
    denominator = set(rules["matured_denominator"])
    separate = set(rules["separate_counts"])
    assert denominator == {"CONFIRMED", "REFUTED", "PARTIAL", "UNAVAILABLE"}
    assert separate.isdisjoint(denominator)
    for fixture in rules["fixtures"]:
        got = derive(fixture, rules, read_at)
        # Every derived state is either in the denominator or has its own count; nothing falls between.
        assert got["state"] in denominator | separate, fixture["name"]
        visible = run_visibility(fixture["run"], read_at) == "VISIBLE"
        assessed = (got["data"] == "PRESENT" or fixture["horizon_end"] is None
                    or datetime.fromisoformat(fixture["graded_at"]) >= datetime.fromisoformat(fixture["horizon_end"]))
        expected_in = (visible and assessed and got["maturity"] == "MATURE"
                       and got["data"] not in {"NOT_GRADABLE", "UNKNOWN_CAUSE"})
        assert (got["state"] in denominator) == expected_in, fixture["name"]


def test_root_cause_prefixes_are_the_graders_own(lineage: dict) -> None:
    grader_src = (ROOT / "scripts" / "grade_publication_review.py").read_text(encoding="utf-8")
    for rule in lineage["derivation"]["unscorable_root_cause_prefixes"]:
        assert re.search(r'f?"' + re.escape(rule["prefix"]), grader_src), rule["prefix"]


def test_every_stage_states_its_binding(lineage: dict, doc: str) -> None:
    allowed = set(lineage["binding_values"])
    for stage in lineage["stages"]:
        assert stage["binding"] and set(stage["binding"]) <= allowed, stage["stage"]
    assert "Binding today" in doc


def test_publication_channels_match_ingest_id_pattern(lineage: dict, ingest) -> None:
    channels = set(re.findall(r"[a-z_]+", ingest.PUBLICATION_ID.pattern.split(")")[0]))
    assert channels == set(lineage["publication_channels"])


def test_missing_input_link_is_still_flagged_by_the_packet(lineage: dict) -> None:
    """Link I is MISSING because the packet says so; when it stops saying so, revise the draft."""
    link = next(item for item in lineage["chain"] if item["link"] == "I")
    assert link["state"] == "MISSING"
    flag = link["evidence_of_gap"]
    assert flag["contains"] in (ROOT / flag["file"]).read_text(encoding="utf-8")


def test_every_gap_is_named_in_the_doc_and_referenced(lineage: dict, doc: str) -> None:
    ids = [g["id"] for g in lineage["gaps"]]
    assert ids == [f"L{i}" for i in range(1, len(ids) + 1)]
    for gap_id in ids:
        assert f"| {gap_id} |" in doc, gap_id
    referenced = {g for item in lineage["chain"] for g in item.get("gaps", [])}
    assert referenced <= set(ids)


def test_cited_eval_releases_exist(lineage: dict) -> None:
    released = json.loads((ROOT / "evals" / "RELEASED.json").read_text(encoding="utf-8"))
    versions = {e.get("version") for e in released["entries"] if e.get("kind") == "suite"}
    assert set(lineage["released_suites"]) <= versions
