"""Sealed trees: an open registration whose code the live package no longer reproduces is judged
by the verbatim tree that does, never abandoned for a change it was not sealed against, and never
judged by code it was not sealed with."""

from __future__ import annotations

import hashlib
import sys
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
from alpha_research.edge_stream.data import RequestCapReached, StaticSource
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
