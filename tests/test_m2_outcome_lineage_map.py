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
    assert "DRAFT v0" in doc and "authorises nothing" in doc
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


def test_outcome_states_cover_every_verdict_and_only_verdicts(lineage: dict, ingest) -> None:
    encodings = {s["verdict"] for s in lineage["outcome_states"]}
    assert encodings == set(ingest.VERDICTS)
    names = [s["state"] for s in lineage["outcome_states"]]
    assert len(names) == len(set(names))
    # A non-success state is never encoded as a resolved verdict.
    for state in lineage["outcome_states"]:
        if state["state"] in {"PENDING", "UNRESOLVABLE_BY_RULE", "UNAVAILABLE", "UNMEASURABLE"}:
            assert state["verdict"] not in ingest.RESOLVED, state


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
    referenced = {g for item in lineage["chain"] + lineage["outcome_states"] for g in item.get("gaps", [])}
    assert referenced <= set(ids)


def test_cited_eval_releases_exist(lineage: dict) -> None:
    released = json.loads((ROOT / "evals" / "RELEASED.json").read_text(encoding="utf-8"))
    versions = {e.get("version") for e in released["entries"] if e.get("kind") == "suite"}
    assert set(lineage["released_suites"]) <= versions
