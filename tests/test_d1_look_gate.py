"""Tests for the admission-before-read gate (contract §8, G14). Synthetic only.

Every reader here is a recording sentinel: the claim under test is that a look which is not
admitted makes zero reads, and that an admitted look makes exactly one, after the check.
"""

from __future__ import annotations

import ast
import copy
import inspect
import json
from pathlib import Path
import shutil

import pytest

from analysis import d0_admission as A
from analysis import d0_manifest as M
from analysis import d1_look_gate as G
from tests.test_d0_admission import HYP, TODAY, _build, _resign

pytestmark = pytest.mark.skipif(shutil.which("ssh-keygen") is None, reason="ssh-keygen not installed")


class Sentinel:
    """A reader that records every call. Zero calls is the proof of 'no read'."""

    def __init__(self, result: object = None, error: Exception | None = None) -> None:
        self.requests: list[dict] = []
        self.result, self.error = result, error

    def __call__(self, request: dict) -> object:
        self.requests.append(copy.deepcopy(request))
        if self.error is not None:
            raise self.error
        return self.result


@pytest.fixture()
def world(tmp_path):
    return _build(tmp_path, HYP, 120)


@pytest.fixture()
def accepted(monkeypatch):
    """The verifier flag on for this test only; the committed flag is never changed."""
    monkeypatch.setattr(A, "VERIFIER_ACCEPTED", True)


_UNSET = object()


def _look(world, manifest=_UNSET, hypothesis=HYP, reader=None):
    reader = reader if reader is not None else Sentinel()
    res = G.run_look(world["m"] if manifest is _UNSET else manifest, hypothesis, reader,
                     repo_root=world["root"], today=TODAY)
    return res, reader


def _denied(res, sentinel, needle=None):
    assert res.status == G.DENIED, res
    assert sentinel.requests == [], "a denied look read something"
    assert [e for e, _ in res.events] == ["denied"]
    if needle is not None:
        assert any(needle in e for e in res.errors), res.errors


# ---- the committed state: nothing is admitted, the flag is off ----

def test_the_committed_manifest_denies_every_supported_hypothesis():
    m = json.loads((M.REPO / M.MANIFEST_PATH).read_text(encoding="utf-8"))
    for hid in A.FROZEN_SPEC["hypotheses"]:
        sentinel = Sentinel()
        res = G.run_look(m, hid, sentinel)
        _denied(res, sentinel)


def test_a_complete_signed_admission_is_still_denied_while_the_verifier_flag_is_off(world):
    assert A.VERIFIER_ACCEPTED is False
    res, sentinel = _look(world)
    _denied(res, sentinel, "not yet accepted")


# ---- the admitted path: one read, after the check, with the frozen request ----

def test_an_admitted_look_makes_exactly_one_read_after_the_check(world, accepted):
    sentinel = Sentinel(result={"rows": 3})
    res, _ = _look(world, reader=sentinel)
    assert res.status == G.READ and res.errors == () and res.data == {"rows": 3}
    assert [e for e, _ in res.events] == ["admission_verified", "read_started"]
    assert len(sentinel.requests) == 1
    req = sentinel.requests[0]
    assert req["hypothesis"] == HYP
    assert req["window"] == A.frozen_hypothesis(HYP)["window"]  # the frozen window, not a caller's
    assert req["manifest_sha256"] == A.manifest_digest(world["m"])
    assert req["gate"] == G.GATE_VERSION


def test_the_reader_runs_only_after_the_verifier_has_answered(world, accepted, monkeypatch):
    order: list[str] = []
    real = A.admission_errors

    def spy(*a, **k):
        order.append("verify")
        return real(*a, **k)

    monkeypatch.setattr(A, "admission_errors", spy)
    res, _ = _look(world, reader=lambda req: order.append("read"))
    assert res.status == G.READ and order == ["verify", "read"]


# ---- denied inputs: each makes zero reads and returns a structured denial ----

def _unsigned(w):
    m = copy.deepcopy(w["m"])
    m["reviewer"]["signed"] = False
    return m


def _near_spelled(w):
    m = copy.deepcopy(w["m"])
    next(h for h in m["hypotheses"] if h["id"] == HYP)["status"] = "admitted"
    return m


def _duplicate_hypothesis(w):
    m = copy.deepcopy(w["m"])
    m["hypotheses"].append(copy.deepcopy(next(h for h in m["hypotheses"] if h["id"] == HYP)))
    return _resign(w, m)


def _relabelled_after_signing(w):
    m = copy.deepcopy(w["m"])
    m["signed_at"] = "2026-11-19"
    return m


def _tampered_input(w):
    m = copy.deepcopy(w["m"])
    next(i for i in m["inputs"] if i.get("admission_status") == "ADMITTED")["missingness"] = "edited after signing"
    return m


@pytest.mark.parametrize("build", [_unsigned, _near_spelled, _duplicate_hypothesis, _relabelled_after_signing,
                                   _tampered_input])
def test_an_inadmissible_manifest_makes_zero_reads(world, accepted, build):
    res, sentinel = _look(world, manifest=build(world))
    _denied(res, sentinel)


def test_a_missing_signature_makes_zero_reads(world, accepted):
    (world["root"] / A.SIGNATURE_DIR / f"{HYP}.operator.sig").unlink()
    res, sentinel = _look(world)
    _denied(res, sentinel)


@pytest.mark.parametrize("bad", [None, [], "manifest", 7, True])
def test_a_manifest_that_is_not_a_mapping_makes_zero_reads(world, accepted, bad):
    res, sentinel = _look(world, manifest=bad)
    _denied(res, sentinel, "not a mapping")


@pytest.mark.parametrize("key", ["inputs", "gaps", "hypotheses"])
@pytest.mark.parametrize("bad", ["x", {"id": HYP}, 5])
def test_a_collection_that_is_not_a_list_makes_zero_reads(world, accepted, key, bad):
    m = copy.deepcopy(world["m"])
    m[key] = bad
    res, sentinel = _look(world, manifest=m)
    _denied(res, sentinel, "not a list")


@pytest.mark.parametrize("key", ["inputs", "gaps", "hypotheses"])
@pytest.mark.parametrize("bad", [None, "x", 3, ["a"]])
def test_a_malformed_member_makes_zero_reads(world, accepted, key, bad):
    m = copy.deepcopy(world["m"])
    m[key] = [bad] + m[key]
    res, sentinel = _look(world, manifest=m)
    _denied(res, sentinel, "not a mapping")


# ---- the named hypothesis must itself be admitted ----

def test_an_empty_manifest_verifies_nothing_and_is_not_permission(accepted):
    assert A.admission_errors({}, today=TODAY) == []  # the trap: the verifier alone says 'no errors'
    sentinel = Sentinel()
    res = G.run_look({}, HYP, sentinel)
    _denied(res, sentinel, "not an ADMITTED row")


def test_admitting_one_hypothesis_does_not_admit_another(world, accepted):
    res, sentinel = _look(world, hypothesis="D2-H3")
    _denied(res, sentinel, "not an ADMITTED row")


@pytest.mark.parametrize("hid", [None, "", 7, ["D2-H2"], "D9-H9", "d2-h2"])
def test_an_unsupported_or_malformed_hypothesis_id_makes_zero_reads(world, accepted, hid):
    res, sentinel = _look(world, hypothesis=hid)
    _denied(res, sentinel)


# ---- fail closed on the gate's own collaborators ----

@pytest.mark.parametrize("reader", [None, "reader", 3, [lambda r: r]])
def test_a_reader_that_is_not_callable_is_denied_not_raised(world, accepted, reader):
    res = G.run_look(world["m"], HYP, reader, repo_root=world["root"], today=TODAY)
    assert res.status == G.DENIED and any("not callable" in e for e in res.errors)


def test_a_verifier_that_raises_denies_the_look(world, accepted, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("verifier broke")

    monkeypatch.setattr(A, "admission_errors", boom)
    res, sentinel = _look(world)
    _denied(res, sentinel, "RuntimeError")


def test_a_reader_that_raises_is_reported_and_not_retried(world, accepted):
    sentinel = Sentinel(error=OSError("disk gone"))
    res, _ = _look(world, reader=sentinel)
    assert res.status == G.READ_FAILED and "OSError" in res.errors[0]
    assert len(sentinel.requests) == 1
    assert [e for e, _ in res.events] == ["admission_verified", "read_started"]


# ---- no way around the gate ----

def test_the_gate_has_no_override_argument():
    params = set(inspect.signature(G.run_look).parameters)
    assert params == {"manifest", "hypothesis", "reader", "repo_root", "today"}


def _calls(fn_node):
    for node in ast.walk(fn_node):
        if isinstance(node, ast.Call):
            yield node


def test_the_verifier_call_precedes_the_reader_call_in_the_source():
    tree = ast.parse(Path(G.__file__).read_text(encoding="utf-8"))
    run = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_look")
    deny = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_denials")
    verify_lines = [c.lineno for c in _calls(deny)
                    if isinstance(c.func, ast.Attribute) and c.func.attr == "admission_errors"]
    reader_lines = [c.lineno for c in _calls(run) if isinstance(c.func, ast.Name) and c.func.id == "reader"]
    denial_lines = [c.lineno for c in _calls(run) if isinstance(c.func, ast.Name) and c.func.id == "_denials"]
    assert len(verify_lines) == 1 and len(reader_lines) == 1 and len(denial_lines) == 1
    assert denial_lines[0] < reader_lines[0]  # run_look asks _denials before it ever calls the reader
    ret = [n for n in ast.walk(run) if isinstance(n, ast.If)]
    assert ret and ret[0].lineno < reader_lines[0] and any(isinstance(b, ast.Return) for b in ret[0].body)


def _imports(path: Path) -> set[str]:
    out: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            out.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            out.add(node.module or "")
            out.update(f"{node.module}.{a.name}" for a in node.names)
    return out


def test_the_gate_imports_no_data_access_of_its_own():
    imported = _imports(Path(G.__file__))
    stdlib = {"__future__", "dataclasses", "datetime", "pathlib", "typing"}
    assert {m.split(".")[0] for m in imported} <= stdlib | {"analysis"}
    assert {m for m in imported if m.startswith("analysis.")} <= {"analysis.d0_admission", "analysis.d0_manifest"}


HOLDOUT_READERS = ("holdout_basis_probe", "evals.e3.holdout", "evals.e3")


def test_no_other_d1_or_d2_module_imports_a_holdout_reader():
    """A look module must go through the gate: none of analysis/d1_*.py or d2_*.py may import a reader itself."""
    offenders = []
    for path in sorted((M.REPO / "analysis").glob("d[12]_*.py")):
        if path.name == "d1_look_gate.py":
            continue
        for mod in _imports(path):
            if any(h in mod for h in HOLDOUT_READERS):
                offenders.append((path.name, mod))
    assert offenders == []
