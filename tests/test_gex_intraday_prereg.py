"""GEX P3 intraday family v1: pre-registration body pin, family rules, registry design.

Offline: no database, network or subprocess; no outcome or price is read.
"""

import ast
import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import socket
import sqlite3
import subprocess

import pytest

from analysis import gex_intraday_prereg as g

NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
CODE = "cdf1b7f7f5a3cf2f37030a9c8164203405a23ecb"


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


def test_sub_families_are_separate_and_never_pooled(checked):
    family = checked["family"]
    assert tuple(family["sub_families"]) == g.SUB_FAMILIES
    assert family["holdout"]["correction"] == "frozen_selection_bonferroni_within_sub_family"
    assert sorted(sum(checked["sub_families"].values(), [])) == sorted(
        h["id"] for h in family["hypotheses"]
    )
    for h in family["hypotheses"]:
        if h["sub_family"] == "PO":
            assert h["uses_gamma"] is False
        elif h["sub_family"] in ("DW", "MG", "SC"):
            assert h["uses_gamma"] is True


def test_every_hypothesis_has_an_executable_price_contract(checked):
    body = g.read_body()
    for h in checked["family"]["hypotheses"]:
        assert h["price_contract"] in ("PC-OC", "PC-CC")
        assert h["price_contract"] in body
    # The receipt rule copies spy_close_v2: created_at, fixed deadline, no fallback.
    assert "`created_at`" in body and "(S + 6 calendar days) 00:00Z" in body
    assert "no fallback" in body


def test_nothing_is_ready_without_admitted_inputs(checked):
    statuses = {h["id"]: h["input_status"] for h in checked["family"]["hypotheses"]}
    assert "READY" not in statuses.values()
    assert all(
        statuses[h["id"]] == "BLOCKED_PRICE_CONTRACT" or statuses[h["id"]] == "BLOCKED_INPUT"
        for h in checked["family"]["hypotheses"]
    )


def test_engine_pin_names_the_engine_by_content(checked):
    pin = checked["family"]["engine_pin"]
    assert set(pin) == {"physics/dealer_gamma.py", "physics/greeks/black_scholes.py", "reference_commit"}
    # Report-only: a later engine change must run the pinned archive, never relabel.
    assert set(g.engine_matches_pin(checked["family"])) == set(g.ENGINE_FILES)


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda f: f["hypotheses"][0].update(uses_gamma=True), "must not mix"),
        (lambda f: f["hypotheses"][4].update(uses_gamma=False), "must not mix"),
        (lambda f: f["hypotheses"][0].update(sub_family="MG"), "sub-family"),
        (lambda f: f["hypotheses"][4].update(input_status="READY"), "opening-auction"),
        (lambda f: f["hypotheses"][4].update(rule="x since 2026-10-15"), "dates"),
        (lambda f: f["hypotheses"][4].update(direction="two-sided"), "one-sided"),
        (lambda f: f["hypotheses"][4].update(n_holdout_ladder=[500, 120]), "ladder"),
        (lambda f: f["hypotheses"].append(copy.deepcopy(f["hypotheses"][0])), "ten"),
        (lambda f: f["holdout"].update(correction="pooled_bonferroni"), "never pooled"),
        (lambda f: f.update(sub_families=["PO", "MG"]), "sub-families"),
        (lambda f: f["engine_pin"].update({"physics/dealer_gamma.py": "x"}), "engine pin"),
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
    assert records[1]["promotion_allowed"] is False
    assert records[1]["trials"] == [h["id"] for h in checked["family"]["hypotheses"]]
    assert not list(tmp_path.iterdir())  # dry run writes nothing
    written = g.register(tmp_path, NOW, CODE)
    assert written[1]["prev_sha256"] is not None
    assert g.verify(tmp_path)["ok"] and g.verify(tmp_path)["records"] == 2
    with pytest.raises(PermissionError, match="fork"):
        g.register(tmp_path, NOW, CODE)


@pytest.mark.parametrize("now,code", [(datetime(2026, 10, 1, 12), CODE), (NOW, "abc")])
def test_registration_inputs_validated(tmp_path, now, code):
    with pytest.raises(g.PreregError):
        g.register(tmp_path, now, code, dry_run=True)


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
