"""GEX P3 intraday family v1: pre-registration body pin, family rules, registry design.

Offline: no database or network; no outcome or price is read. One test may run
`git show` to prove the engine pin against the reference commit's LF content.
"""

import ast
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import socket
import sqlite3
import subprocess

import pytest

from analysis import gex_intraday_prereg as g

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
CODE = "cdf1b7f7f5a3cf2f37030a9c8164203405a23ecb"
IDX = {h: i for i, h in enumerate(["PO1", "PO2", "DW1", "DW2", "MG1", "MG2", "MG3", "SC1", "SC2", "RB1"])}


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("pre-registration code attempted external IO")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)


@pytest.fixture(scope="module")
def checked():
    return g.check_prereg()


def test_body_hash_is_pinned_and_lf_stable(checked):
    assert checked["body_sha256"] == g.PREREG_BODY_SHA256
    text = (g.REPO / g.PREREG_PATH).read_text(encoding="utf-8")
    assert g.body_sha256(g.prereg_body(text.replace("\n", "\r\n"))) == g.PREREG_BODY_SHA256


def test_any_body_edit_is_refused(tmp_path):
    src = (g.REPO / g.PREREG_PATH).read_text(encoding="utf-8")
    target = tmp_path / g.PREREG_PATH
    target.parent.mkdir(parents=True)
    target.write_text(src.replace("Seed 20261001", "Seed 20261002"), encoding="utf-8")
    with pytest.raises(g.PreregError, match="pinned"):
        g.check_prereg(tmp_path)


def test_sub_families_are_separate_with_fixed_k(checked):
    family = checked["family"]
    assert tuple(family["sub_families"]) == g.SUB_FAMILIES
    assert family["holdout"]["correction"] == "fixed_k_bonferroni_within_sub_family"
    assert family["holdout"]["k"] == g.FIXED_K == {k: len(v) for k, v in checked["sub_families"].items()}
    for h in family["hypotheses"]:
        assert h["uses_gamma"] is (h["sub_family"] in ("DW", "MG", "SC"))


def test_gamma_and_breadth_trade_claims_are_paired(checked):
    by_id = {h["id"]: h for h in checked["family"]["hypotheses"]}
    assert by_id["MG2"]["kind"] == "paired_trade" and "net(PO1)" in by_id["MG2"]["statistic"]
    assert by_id["SC1"]["kind"] == "paired_trade" and "net(MG2)" in by_id["SC1"]["statistic"]
    for hid in ("DW2", "MG1", "MG3", "SC2"):
        assert "PO2's x" in by_id[hid]["statistic"]
        assert "with intercept" in by_id[hid]["statistic"]


def test_no_side_or_distance_uses_a_post_decision_price(checked):
    for h in checked["family"]["hypotheses"]:
        if h["decision"] == "D0":
            # O_S (the opening auction) is unknown at D0; sides use PM and P0.
            assert "O_S" not in h["rule"] and "O_S" not in h["input"], h["id"]
    body = g.read_body()
    # The S-1 close receipt can predate D0 only in winter and is never
    # guaranteed, so it is not admitted as P0 in any season.
    assert "09:30 EDT (after D0) in summer and 08:30 EST in winter" in body
    assert "not admitted as P0 in any season" in body
    assert "registered_at <= D0" in body and "backfilled = false" in body


def test_every_hypothesis_has_an_executable_price_contract(checked):
    body = g.read_body()
    for h in checked["family"]["hypotheses"]:
        assert h["price_contract"] in ("PC-OC", "PC-CC")
        assert h["e2_rule"] == g.E2_RULE[(h["price_contract"], h["kind"])]
    # Receipt rule from the SPY outcome-selection design: created_at, fixed
    # per-session deadline, no fallback.
    assert "`created_at`" in body and "(E + 6 calendar days) 00:00Z" in body
    assert "no fallback" in body
    assert "ln(max(abs(ln(C_S/O_S)), 0.00005))" in body


def test_no_outcome_dependent_exclusion_and_v1_separation():
    body = g.read_body()
    assert "halted" not in body  # a halt is only a missing auction print
    assert "the only outcome-side code" in body
    assert "MG1, MG2, SC1, SC2 and DW1 stay `BLOCKED` until the v1 closure artifact" in body
    assert "proven on vault `origin/main` (section 10)" in body
    for gate in ("`rules.json` stream registration", "E3 trial ledger"):
        assert gate in body


def test_every_hypothesis_selects_with_high_probability_by_design(checked):
    # Windows sized for P(select) ~ 0.8 at the planted effect: a fixed
    # discovery window never caps joint power below the 0.5 gate.
    nd = {h["id"]: h["n_discovery"] for h in checked["family"]["hypotheses"]}
    assert nd == {
        "PO1": 300, "PO2": 250, "DW1": 300, "DW2": 250, "MG1": 250,
        "MG2": 300, "MG3": 450, "SC1": 300, "SC2": 650, "RB1": 64,
    }


def test_nothing_is_ready_without_admitted_inputs(checked):
    assert {h["input_status"] for h in checked["family"]["hypotheses"]} <= {
        "BLOCKED_INPUT",
        "BLOCKED_PRICE_CONTRACT",
    }


def test_engine_pin_is_lf_content_of_the_reference_commit(checked, monkeypatch, tmp_path):
    pin = checked["family"]["engine_pin"]
    assert pin["hash_basis"] == "sha256 of LF git content"
    assert set(g.ENGINE_FILES) >= {"store/astrogrid.py", "ingestion/market_calendar.py"}
    # CRLF copies of the pinned content still match (grid-svr is LF, Windows is CRLF).
    matches = g.engine_matches_pin(checked["family"])
    monkeypatch.undo()  # this test alone may run git
    verified_by_git = False
    for f in g.ENGINE_FILES:
        shown = subprocess.run(
            ["git", "-C", str(g.REPO), "show", f"{pin['reference_commit']}:{f}"],
            capture_output=True,
        )
        if shown.returncode == 0:
            verified_by_git = True
            assert hashlib.sha256(shown.stdout).hexdigest() == pin[f]
            crlf = tmp_path / f
            crlf.parent.mkdir(parents=True, exist_ok=True)
            crlf.write_bytes(shown.stdout.replace(b"\n", b"\r\n"))
    if verified_by_git:
        assert all(g.engine_matches_pin(checked["family"], tmp_path).values())
        tree = subprocess.run(
            ["git", "-C", str(g.REPO), "rev-parse", f"{pin['reference_commit']}^{{tree}}"],
            capture_output=True,
            text=True,
        )
        assert tree.stdout.strip() == pin["reference_tree"]
    elif not all(matches.values()):
        pytest.skip("reference commit not fetched and engine changed since; pin unverifiable here")
    assert verified_by_git or all(matches.values())


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda f: f["hypotheses"][IDX["PO1"]].update(uses_gamma=True), "must not mix"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(uses_gamma=False), "must not mix"),
        (lambda f: f["hypotheses"][IDX["RB1"]].update(uses_gamma=True), "must not mix"),
        (lambda f: f["hypotheses"][IDX["PO1"]].update(sub_family="MG"), "sub-family"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(input_status="READY"), "opening-auction"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(rule="x since 2026-10-15"), "dates"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(rule="x after the 2027 rebalance"), "dates"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(rule="x in October only"), "dates"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(rule="x after May"), "dates"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(rule="x since Oct. close"), "dates"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(rule="x after 10/15"), "dates"),
        (lambda f: f["engine_pin"].pop("store/astrogrid.py"), "engine pin"),
        (lambda f: f["engine_pin"].update(reference_tree="abc"), "engine pin"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(direction="two-sided"), "one-sided"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(n_holdout_ladder=[500, 250]), "ladder"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(n_holdout=250.0), "integer"),
        (lambda f: f["hypotheses"][IDX["MG1"]].update(e2_rule="e2.direction.v1"), "e2 rule"),
        (lambda f: f["hypotheses"][IDX["MG2"]].update(statistic="mean net return"), "paired"),
        (lambda f: f["hypotheses"][IDX["MG3"]]["planted_effect"].pop("base_rate"), "Stage-0"),
        (lambda f: f["hypotheses"][IDX["MG3"]]["planted_effect"].update(base_rate=1.5), "base rate"),
        (lambda f: f["hypotheses"].append(copy.deepcopy(f["hypotheses"][0])), "ten"),
        (lambda f: f["holdout"].update(correction="pooled_bonferroni"), "never pooled"),
        (lambda f: f["holdout"]["k"].update(MG=2), "never pooled"),
        (lambda f: f.update(sub_families=["PO", "MG"]), "sub-families"),
        (lambda f: f["engine_pin"].update({"physics/dealer_gamma.py": "x"}), "engine pin"),
        (lambda f: f["engine_pin"].update(reference_commit="cdf1b7f7"), "engine pin"),
        (lambda f: f["stage0"].update(min_joint_power=0.3), "stage0"),
        (lambda f: f.update(discovery={"select_one_sided_p": 0.2}), "discovery"),
        (lambda f: f["cost"].update(bps_per_side=1.0), "cost"),
        (lambda f: f["hypotheses"][IDX["MG1"]]["planted_effect"].update(slope_per_sd=0.15), "sign"),
        (lambda f: f["hypotheses"][IDX["PO1"]].update(direction="negative"), "sign"),
        (lambda f: f["hypotheses"][IDX["MG3"]]["planted_effect"].update(diff=0.0), "sign"),
    ],
)
def test_family_rules_are_enforced(checked, mutate, match):
    family = copy.deepcopy(checked["family"])
    mutate(family)
    with pytest.raises(g.PreregError, match=match):
        g.validate_family(family)


def test_registration_is_deterministic_and_write_once(tmp_path, checked):
    records = g.register(tmp_path, NOW, CODE, dry_run=True)
    assert [r["kind"] for r in records] == ["header", "preregistration"]
    assert records[0]["prereg_sha256"] == g.PREREG_BODY_SHA256
    assert all(r["promotion_allowed"] is False for r in records)
    assert records[1]["trials"] == [h["id"] for h in checked["family"]["hypotheses"]]
    assert records[1]["fixed_k"] == g.FIXED_K
    assert not list(tmp_path.iterdir())  # dry run writes nothing
    written = g.register(tmp_path, NOW, CODE)
    assert written[1]["prev_sha256"] is not None
    assert g.verify(tmp_path)["ok"] and g.verify(tmp_path)["records"] == 2
    with pytest.raises(PermissionError, match="fork"):
        g.register(tmp_path, NOW, CODE)


def test_pinned_registration_refuses_a_fork(tmp_path):
    written = g.register(tmp_path / "real", NOW, CODE)
    pins = tuple(hashlib.sha256(g.canonical(r)).hexdigest() for r in written)
    assert g.verify(tmp_path / "real", registered=pins)["ok"]
    g.register(tmp_path / "fork", datetime(2026, 10, 2, tzinfo=timezone.utc), CODE)
    result = g.verify(tmp_path / "fork", registered=pins)
    assert not result["ok"] and "fork" in result["detail"]


@pytest.mark.parametrize("now,code", [(datetime(2026, 10, 1, 12), CODE), (NOW, "abc")])
def test_registration_inputs_validated(tmp_path, now, code):
    with pytest.raises(g.PreregError):
        g.register(tmp_path, now, code, dry_run=True)


def test_now_accepts_z_suffix_on_python_310():
    assert g.parse_now("2026-10-01T12:00:00Z") == NOW
    with pytest.raises(g.PreregError):
        g.parse_now("2026-10-01T12:00:00")


def test_tampered_registry_and_foreign_witness_refused(tmp_path):
    log_dir, vault = tmp_path / "log", tmp_path / "vault"
    g.register(log_dir, NOW, CODE)
    assert len(g.export_anchors(log_dir, vault)) == 1
    assert g.export_anchors(log_dir, vault) == []  # idempotent
    witness = vault / g.WITNESS_PATH
    assert g.verify(log_dir, witness)["ok"]
    log = log_dir / g.REGISTRY_LOG
    lines = log.read_bytes().splitlines()
    record = json.loads(lines[1])
    record["promotion_allowed"] = True
    lines[1] = g.canonical(record)
    log.write_bytes(b"\n".join(lines) + b"\n")
    assert not g.verify(log_dir, witness)["ok"]
    other = tmp_path / "other"
    g.register(other, datetime(2026, 10, 2, tzinfo=timezone.utc), CODE)
    with pytest.raises(PermissionError, match="another registry"):
        g.export_anchors(other, vault)


def test_witness_paths_are_distinct_vault_files():
    assert g.WITNESS_PATH != g.DECISION_WITNESS_PATH
    body = g.read_body()
    assert g.WITNESS_PATH.as_posix() in body and g.DECISION_WITNESS_PATH.as_posix() in body
    assert "self-reported" in body and "push event time" in body


def test_registry_module_reads_no_outcomes_or_frozen_v1():
    tree = ast.parse(Path(g.__file__).read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    assert not any(
        m.startswith(("paper_log", "store", "db", "ingestion", "sqlalchemy", "physics", "evals"))
        for m in imported
    )


def test_v1_closure_artifact_is_concrete_and_otherwise_blocks_forever():
    body = g.read_body()
    assert "05-GRID/Paper-Log/gex_levels_v1/CLOSURE.json" in body
    assert "log_head_sha256" in body and "without `--interim`" in body
    assert "Absent this artifact, the five stay `BLOCKED`" in body
    assert "terminal evaluation record or terminal stop record" not in body


def test_rb1_calendar_is_defined_and_consistent(checked):
    body = g.read_body()
    assert "M = the last NYSE session of the calendar month" in body
    assert "M-2 = the\n  third-to-last" in body
    rb1 = next(h for h in checked["family"]["hypotheses"] if h["id"] == "RB1")
    assert "third-to-last" in rb1["decision"] and "second-to-last" not in rb1["decision"]
    assert "triggered month-end trades" in body


def test_partners_are_counterfactuals_and_dw2_sign_is_registered(checked):
    body = g.read_body()
    assert "Partners are counterfactuals" in body and "`partner_unavailable`" in body
    assert "never filled with zero" in body
    dw2 = next(h for h in checked["family"]["hypotheses"] if h["id"] == "DW2")
    assert dw2["direction"] == "positive" and dw2["planted_effect"]["slope_per_sd"] > 0
    assert "no_wall" in dw2["rule"]


def test_close_contract_requires_auction_check():
    body = g.read_body()
    assert "not certified to be\n  the closing auction print" in body
    assert "`spy_close_v1`\n  close equals the official closing auction print" in body


@pytest.mark.parametrize("word", ["Market", "Decision", "Separate", "Maybe", "Octane", "Junction"])
def test_date_pattern_ignores_ordinary_capitalized_words(word):
    assert not g.DATE_LIKE.search(f"{word} rule on the session")


def test_final_cleanup_wording_is_binding():
    body = g.read_body()
    assert "stays `BLOCKED_PRICE_CONTRACT`; any replacement close source is a new version" in body
    assert "It is never P0, and it is an input (PO2, RB1) only through receipts created" in body
    assert "used as an outcome only" not in body
    assert "line number `log_records` of the v1 log hashes to `log_head_sha256`" in body
    assert "requires stdout byte-identical to the quoted\n    stdout" in body
    assert "session strictly after the proof date" in body
    assert "batch completing between 13:30Z and D0 can see the S-1 close" in body
    assert "ex-dates in the month on or before M-3" in body


def test_v1_access_statement_matches_the_closure_verification():
    body = g.read_body()
    assert "not read, imported or written" not in body
    assert "reads only the v1 closure artifact" not in body
    assert "reads the v1 log read-only, and runs v1's pinned code only on a scratch" in body
    assert "Nothing in this family ever writes to v1." in body
    assert "statsmodels versions, and the SHA-256 of the grid-svr environment's lock" in body
    assert "after stripping leading whitespace (status prints it indented)" in body
    assert "requires that\n    stripped line to be byte-identical to the quoted advisory line" in body
    # No statement anywhere contradicts the section 7 read-only verification.
    assert "Nothing here reads, writes" not in body
    assert "Section 7's closure verification alone reads the log read-only" in body
    assert "the later of the two section 10 observations" in body
