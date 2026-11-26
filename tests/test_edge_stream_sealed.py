"""Sealed trees: an open registration whose code the live package no longer reproduces is judged
by the verbatim tree that does, never abandoned for a change it was not sealed against, and never
judged by code it was not sealed with."""

from __future__ import annotations

import ast
import hashlib
import os
import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy
import pytest

from alpha_research.edge_stream import families as F
from alpha_research.edge_stream import ledger as L
from alpha_research.edge_stream import policy as P
from alpha_research.edge_stream import runner as R
from alpha_research.edge_stream import sanity as S
from alpha_research.edge_stream import sealed as SE
from alpha_research.edge_stream import data as D
from alpha_research.edge_stream.data import RequestCapReached, SourceError, StaticSource
from alpha_research.edge_stream.sealed.tree_a1d9bd4c import families as SF
from alpha_research.edge_stream.sealed.tree_a1d9bd4c import policy as SP
from alpha_research.edge_stream.sealed.tree_a1d9bd4c import runner as SR

TREE = "tree_a1d9bd4c"
TODAY = date(2026, 11, 13)  # a Friday
NOW = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)
#: grid-hermes' interpreter (/data/grid_v4/grid_venv-314) has numpy 2.5.4; the hash carries major.minor
DEPLOYED_NUMPY = "2.5.4"
#: ... and Python 3.14. The hash is over ``ast.dump`` output, which differs between Python minor
#: versions, so the sealed hashes reproduce only there (CI runs 3.11: the byte pins still hold).
deployed_python = pytest.mark.skipif(sys.version_info[:2] != (3, 14),
                                     reason="registration hashes carry ast.dump of Python 3.14 (grid-hermes)")


def test_the_tree_is_the_pinned_byte_copy():
    here = Path(SE.__file__).resolve().parent / TREE
    pinned = SE.TREES[TREE]["files"]
    assert sorted(p.name for p in here.glob("*.py")) == sorted(pinned)
    for name, sha in pinned.items():
        assert hashlib.sha256((here / name).read_bytes()).hexdigest() == sha, name


@deployed_python
def test_the_tree_reproduces_the_open_registrations_it_seals(monkeypatch):
    monkeypatch.setattr(numpy, "__version__", DEPLOYED_NUMPY)
    for proposal_id, code in SE.TREES[TREE]["seals"].items():
        reg = {"family": proposal_id.split("-")[0], "code_sha256": code}
        assert SE.evaluator(reg) is SR, proposal_id
        assert R._evaluator(reg) is SR
        # the live package does not reproduce it: without the tree it would be abandoned
        assert F.code_sha256(F.FAMILIES[reg["family"]]) != code


def test_a_hash_no_tree_reproduces_is_abandoned():
    reg = {"family": "EC1", "code_sha256": "e" * 64}
    assert SE.evaluator(reg) is None
    with pytest.raises(R.CodeChanged):
        R._evaluator(reg)
    assert SE.evaluator({"family": "ZZ9", "code_sha256": "e" * 64}) is None


def test_the_live_code_still_judges_what_it_reproduces():
    reg = {"family": "IM1", "code_sha256": F.code_sha256(F.FAMILIES["IM1"])}
    assert R._evaluator(reg) is None


def _sessions(tickers, start, end):
    out = {}
    for t in tickers:
        days, d = {}, start
        while d < end:
            if d.weekday() < 5:
                days[d.isoformat()] = [("09:30", 100.0, 100.0, 100.0, 100.0, 1.0),
                                       ("09:59", 100.5, 100.5, 100.5, 100.5, 1.0),
                                       ("15:29", 100.4, 100.4, 100.4, 100.4, 1.0),
                                       ("15:59", 100.6, 100.6, 100.6, 100.6, 1.0)]
            d += timedelta(days=1)
        out[t] = days
    return out


def _sealed_forward(monkeypatch):
    """A ledger under the tree's policy (3) holding the tree's own forward registration."""
    item = dict(next(q for q in SF.QUEUE if q["lane"] == "forward"))
    with monkeypatch.context() as m:
        m.setattr(P, "policy_payload", SP.policy_payload)
        led = L.memory("2026-11-13T06:27:00+00:00")
        prop = SF.build_proposal(item, TODAY)
        report = S.check(prop, L.load_state(led), TODAY)
        assert S.passed(report), report
        sha = L.append_registration(led, {**prop, "sanity": report, "recorded_at": NOW.isoformat()})
    assert SE.evaluator(prop) is SR and F.code_sha256(F.FAMILIES["IM1"]) != prop["code_sha256"]
    return led, sha, prop


def test_a_cycle_judges_a_sealed_forward_registration_by_its_tree(tmp_path, monkeypatch):
    led, sha, prop = _sealed_forward(monkeypatch)
    calls = []
    real = SR.forward_progress
    monkeypatch.setattr(SR, "forward_progress", lambda *a, **k: calls.append(1) or real(*a, **k))
    src = StaticSource(_sessions(prop["instruments"], date(2026, 11, 16), date(2026, 11, 21)),
                       today=date(2026, 11, 20))
    s = R.run_cycle(led, src, tmp_path, now=datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc), register=False)
    state = L.load_state(led)
    assert calls == [1] and s["abandoned"] == [] and not state.closed(sha)
    assert s["forward"][0]["registration"] == sha[:16] and s["forward"][0]["sessions"] == 4
    assert state.reads[sha] == {"progress"}
    # the live policy (4) is now in force; the registration keeps the terms it was sealed with
    assert state.policy_sha256 == P.policy_sha256() != SP.policy_sha256()
    assert state.registrations[sha]["policy_sha256"] == SP.policy_sha256()


def test_a_sealed_trees_exceptions_come_back_as_the_runners(monkeypatch):
    led, sha, prop = _sealed_forward(monkeypatch)
    src = StaticSource(_sessions(prop["instruments"], date(2026, 11, 16), date(2026, 11, 18)),
                       today=date(2026, 11, 18))
    with pytest.raises(R.NotReady):  # 2 of 100 sessions: the tree's NotReady, seen as the runner's
        R._judged_by(SR, "compute_look", sha, prop, src, date(2026, 11, 18), None)

    class _Capped(StaticSource):
        def sessions(self, *a, **k):
            raise SR.RequestCapReached("cap")

    with pytest.raises(RequestCapReached):
        R._judged_by(SR, "forward_progress", prop, _Capped({}, today=TODAY), TODAY, None)


@deployed_python
def test_an_edited_tree_reproduces_nothing(monkeypatch):
    monkeypatch.setattr(numpy, "__version__", DEPLOYED_NUMPY)
    code = SE.TREES[TREE]["seals"]["IM1-v1:us_equity_index:forward:2026-11-23"]
    real = SF._ast_digest
    monkeypatch.setattr(SF, "_ast_digest", lambda src: real(src + "\nX_EDIT = 1\n"))
    assert SE.evaluator({"family": "IM1", "code_sha256": code}) is None


# --- fail closed before import (Codex review 6069166270) ------------------------------


def _tree_copy(tmp_path) -> Path:
    src = SE.tree_dir(TREE)
    dst = tmp_path / TREE
    dst.mkdir()
    for f in SE.TREES[TREE]["files"]:
        (dst / f).write_bytes((src / f).read_bytes())
    assert SE.verified(TREE, dst)
    return dst


def _no_import(monkeypatch) -> list:
    """Record any import of the tree (the evaluator catches what an import raises, so a stub that
    raised would pass silently)."""
    imported: list = []
    real = SE._modules
    monkeypatch.setattr(SE, "_modules", lambda name: imported.append(name) or real(name))
    return imported


@pytest.mark.parametrize("damage", ["ledger_edited", "sanity_edited", "missing", "extra", "unreadable"])
def test_a_damaged_tree_is_refused_before_import(tmp_path, monkeypatch, damage):
    """ledger.py, sanity.py and universe.py are imported by the sealed runner but not covered by
    the registration hash: only the byte manifest guards them."""
    root = _tree_copy(tmp_path)
    if damage == "ledger_edited":
        (root / "ledger.py").write_bytes((root / "ledger.py").read_bytes() + b"\nCLOSED = lambda *a: False\n")
    elif damage == "sanity_edited":
        (root / "sanity.py").write_bytes((root / "sanity.py").read_bytes().replace(b"0.60", b"0.99", 1))
    elif damage == "missing":
        (root / "universe.py").unlink()
    elif damage == "extra":
        (root / "shadow.py").write_text("X = 1\n")
    else:
        (root / "ledger.py").chmod(0)
        if os.access(root / "ledger.py", os.R_OK):  # running as root: unreadable cannot be staged
            pytest.skip("file stays readable for this user")
    assert not SE.verified(TREE, root)
    monkeypatch.setattr(SE, "tree_dir", lambda name: root)
    imported = _no_import(monkeypatch)
    code = SE.TREES[TREE]["seals"]["IM1-v1:us_equity_index:forward:2026-11-23"]
    assert SE.evaluator({"family": "IM1", "code_sha256": code}) is None
    assert imported == []


def test_a_tree_that_fails_to_import_judges_nothing(monkeypatch):
    def broken(name):
        raise ImportError("synthetic broken import")
    monkeypatch.setattr(SE, "_modules", broken)
    code = SE.TREES[TREE]["seals"]["EC1-v1:sp500_events:forward:2026-11-26:confirms-603ba3e2332b"]
    assert SE.evaluator({"family": "EC1", "code_sha256": code}) is None
    with pytest.raises(R.CodeChanged):
        R._evaluator({"family": "EC1", "code_sha256": code})


def test_a_tree_module_loaded_from_elsewhere_judges_nothing(monkeypatch, tmp_path):
    SE.evaluator({"family": "IM1", "code_sha256": "0" * 64})  # import the tree for real
    assert SE._imported_from_tree(TREE)
    fake = types.ModuleType("ledger")
    fake.__file__ = str(tmp_path / "ledger.py")
    monkeypatch.setitem(sys.modules, f"{SE.__name__}.{TREE}.ledger", fake)
    assert not SE._imported_from_tree(TREE)
    code = SE.TREES[TREE]["seals"]["IM1-v1:us_equity_index:forward:2026-11-23"]
    assert SE.evaluator({"family": "IM1", "code_sha256": code}) is None


# --- what the tree does not freeze: live sources, extras and retry rules ----------------


def _function(module, name: str) -> str:
    tree = ast.parse(Path(module.__file__).read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    body = node.body[1:] if isinstance(node.body[0], ast.Expr) else node.body  # docstring
    return ast.dump(ast.Module(body=body, type_ignores=[]))


def test_live_sources_are_the_trees_sources():
    """A sealed look reads bars through the live sources and extras: they must be the tree's code.
    Changing data.py while a sealed registration is open needs a frozen adapter (or its loss)."""
    assert F._ast_digest(Path(D.__file__).read_text()) == F._ast_digest((SE.tree_dir(TREE) / "data.py").read_text())


def test_retry_rules_are_the_trees():
    """Failed attempts of a sealed registration are counted by the live runner under live policy."""
    assert _function(R, "_attempt_failed") == _function(SR, "_attempt_failed")
    for k in ("FETCH_ATTEMPTS_BEFORE_ABANDON", "FETCH_DAYS_BEFORE_ABANDON", "FORWARD_FAILURE_DAYS_BEFORE_ABANDON"):
        assert getattr(P, k) == getattr(SP, k), k


class _Failing(StaticSource):
    def __init__(self, exc, **kw):
        super().__init__({}, **kw)
        self.exc = exc

    def sessions(self, *a, **k):
        raise self.exc


def test_a_cap_is_not_counted_against_a_sealed_registration(tmp_path, monkeypatch):
    led, sha, prop = _sealed_forward(monkeypatch)
    for d in range(SP.FORWARD_FAILURE_DAYS_BEFORE_ABANDON + 2):
        now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc) + timedelta(days=d)
        s = R.run_cycle(led, _Failing(RequestCapReached("cap"), today=now.date()), tmp_path, now=now, register=False)
        assert s["looks"] == [{"registration": sha[:16], "status": "deferred: request cap reached this cycle"}]
    assert not (tmp_path / "attempts.jsonl").exists() and not L.load_state(led).closed(sha)


def test_source_errors_abandon_a_sealed_registration_on_the_trees_day(tmp_path, monkeypatch):
    led, sha, prop = _sealed_forward(monkeypatch)
    limit = SP.FORWARD_FAILURE_DAYS_BEFORE_ABANDON
    for d in range(limit):
        now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc) + timedelta(days=d)
        s = R.run_cycle(led, _Failing(SourceError("down"), today=now.date()), tmp_path, now=now, register=False)
        status = s["looks"][0]["status"]
        assert status == ("abandoned" if d == limit - 1 else f"retry later (failed on {d + 1} day(s))"), d
    rec = L.load_state(led).abandoned[sha]
    assert "cause" not in rec and rec["reason"].startswith(f"look failed on {limit} day(s)")


@pytest.mark.parametrize("today, end", [(date(2026, 11, 13), date(2026, 11, 13)),  # before the window
                                        (date(2026, 11, 24), date(2026, 11, 21)),  # a week, then a gap
                                        (date(2026, 12, 2), date(2026, 12, 2))])   # Thanksgiving inside
def test_readiness_and_date_coverage_match_the_tree(monkeypatch, today, end):
    led, sha, prop = _sealed_forward(monkeypatch)
    sessions = _sessions(prop["instruments"], date(2026, 11, 16), end)
    for t in sessions:
        sessions[t].pop("2026-11-26", None)
    src = StaticSource(sessions, today=today)
    assert R._judged_by(SR, "forward_progress", prop, src, today, None) == R.forward_progress(prop, src, today)
