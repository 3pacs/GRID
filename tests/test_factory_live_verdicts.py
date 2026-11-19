"""Live verdicts on the factory ledger (3pacs/GRID#1162).

A factory candidate is tested only forward, in a gex-signals ``panel-factory``
slot. The slot's one look comes back as an export line and is appended to the
factory ledger as ``factory_forward_outcome``. These tests cover the seam:
export file -> import -> reload -> summarize, and every refusal.
"""
from __future__ import annotations

import hashlib
import json

import pytest

from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf

T1, T2, T3 = "a" * 64, "b" * 64, "c" * 64
RECORDED = "2026-11-20T02:00:00+00:00"
LOOKED = "2027-03-01T21:10:00+00:00"

# One export line exactly as gex-signals writes it. The same text and hash are pinned in gex
# (signals-20261023/tests/test_factory_export.py): if either side changes the format, its pin fails.
EXAMPLE_LINE = (
    '{"alpha_per_test":0.005,"chain":{"head":"' + "d" * 64 + '","rows":21},"exported_at":"2027-03-01T21:15:00+00:00",'
    '"family":"panel-factory","looked_at":"2027-03-01T21:10:00+00:00","lower_bound":0.0123,"mean_net":0.0456,"n":100,'
    '"registered_at":"2026-12-01T22:00:00+00:00","resolved_fires":104,"row_hash":"' + "e" * 64 + '",'
    '"rule":{"decision":"15:00","feature":"F1","horizon":"close","k":1.5,"op":">=","side":"long"},'
    '"schema":"panel-factory-verdict/1","sessions":212,"slot_trial_id":"T-PF-01",'
    '"source":{"result_sha256":"' + "f" * 64 + '","run_id":"r1","trial_id":"' + "a" * 64 + '"},'
    '"through_day":"2027-03-01","verdict":"PASS"}'
)
EXAMPLE_SHA256 = "68e0cb277e39c305b66a674fb2baf15dcd061533460f3bbc801a5ca4bfe91a3e"


def paths(tmp_path):
    (tmp_path / "ledger").mkdir(exist_ok=True)
    (tmp_path / "anchor").mkdir(exist_ok=True)
    return tmp_path / "ledger" / "factory.jsonl", tmp_path / "anchor" / "factory.anchor.jsonl"


CLASS = {T1: "price", T2: "tape"}   # T3 carries no class, like a run recorded before classes existed


def trial(tid, selected=False):
    out = {"trial_id": tid, "status": "tested", "selected": selected}
    return dict(out, **{"class": CLASS[tid]}) if tid in CLASS else out


def run(ledger, run_id="r1", trials=((T1, True), (T2, False), (T3, False))):
    """Declare and record one run; returns the result record's line sha256."""
    lse.record_factory_declared(ledger, run_id=run_id, run_file={"run_id": run_id}, data_manifest_sha256="0" * 64,
                                trial_count=len(trials), recorded_at="2026-11-20T01:00:00+00:00")
    out = lse.record_factory_result(ledger, trials=[trial(t, s) for t, s in trials], max_t={"critical_t": 3.1},
                                    report_sha256="1" * 64, frozen_sha256="2" * 64, recorded_at=RECORDED)
    return out["sha256"]


def line(result_sha, trial_id=T1, verdict="PASS", n=100, lower=0.0123, mean=0.0456, slot="T-PF-01", run_id="r1", **over):
    entry = {
        "schema": "panel-factory-verdict/1", "slot_trial_id": slot, "family": "panel-factory",
        "registered_at": "2026-12-01T22:00:00+00:00", "row_hash": "e" * 64,
        "source": {"run_id": run_id, "trial_id": trial_id, "result_sha256": result_sha},
        "rule": {"feature": "F1", "op": ">=", "k": 1.5, "side": "long", "decision": "15:00", "horizon": "close"},
        "verdict": verdict, "n": n, "mean_net": mean, "lower_bound": lower, "alpha_per_test": 0.005,
        "resolved_fires": max(n, 0), "sessions": 212, "looked_at": LOOKED, "through_day": "2027-03-01",
        "chain": {"head": "d" * 64, "rows": 21}, "exported_at": "2027-03-01T21:15:00+00:00",
    }
    entry.update(over)
    return lse.canonical(entry).decode()


def export(tmp_path, *lines, name="verdicts.jsonl"):
    path = tmp_path / name
    path.write_text("".join(x + "\n" for x in lines))
    return path


def test_the_export_line_is_the_pinned_contract():
    entry = json.loads(EXAMPLE_LINE)
    assert tuple(sorted(entry)) == tuple(sorted(lse.FACTORY_VERDICT_FIELDS))
    assert entry["schema"] == lse.FACTORY_VERDICT_SCHEMA
    assert lse.canonical(entry).decode() == EXAMPLE_LINE
    assert hashlib.sha256(EXAMPLE_LINE.encode()).hexdigest() == EXAMPLE_SHA256


def test_export_import_reload_summarize(tmp_path):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    sha = run(ledger)
    path = export(
        tmp_path,
        line(sha, T1, "PASS"),
        line(sha, T2, "FAIL", lower=-0.01, mean=0.002, slot="T-PF-02"),
        line(sha, T3, "UNDERPOWERED", n=41, lower=None, mean=None, slot="T-PF-03"),
    )
    assert lse.ingest_factory_verdicts(ledger, path, recorded_at="2027-03-02T00:00:00+00:00") == 3

    reloaded = lse.Ledger(lp, anchor=ap)   # the whole chain re-verifies from the file and its anchor
    records = reloaded.of_kind(lse.FACTORY_FORWARD)
    assert [r["verdict"] for r in records] == ["pass", "fail", "underpowered"]
    assert records[0]["result_sha256"] == sha and records[0]["slot"]["trial_id"] == "T-PF-01"
    assert records[0]["source_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert records[0]["source_chain"] == {"head": "d" * 64, "rows": 21}

    live = lse.summarize(reloaded)["live"]
    assert (live["looked"], live["passed"], live["failed"], live["underpowered"]) == (3, 1, 1, 1)
    assert live["looked_not_selected_in_run"] == 2   # T2 and T3 were not selected by their run: shown, not hidden
    assert live["lapsed"] is None                    # no retirement checker yet: unknown, not zero
    assert live["by_class"] == {
        "price": {"looked": 1, "passed": 1, "failed": 0, "underpowered": 0},
        "tape": {"looked": 1, "passed": 0, "failed": 1, "underpowered": 0},
        "unclassed": {"looked": 1, "passed": 0, "failed": 0, "underpowered": 1},
    }
    assert [v["slot"] for v in live["verdicts"]] == ["T-PF-01", "T-PF-02", "T-PF-03"]
    summary = lse.summarize(reloaded)
    assert summary["candidates_declared"] == 3 and summary["runs_recorded"] == 1   # a verdict is not a new candidate


def test_importing_the_same_file_again_adds_nothing(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    sha = run(ledger)
    path = export(tmp_path, line(sha))
    assert lse.ingest_factory_verdicts(ledger, path) == 1
    assert lse.ingest_factory_verdicts(ledger, path) == 0
    assert len(ledger.of_kind(lse.FACTORY_FORWARD)) == 1


def test_a_trial_takes_one_verdict(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    sha = run(ledger)
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha)))
    with pytest.raises(ValueError, match="different live verdict"):
        lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha, verdict="FAIL", lower=-0.2), name="again.jsonl"))
    with pytest.raises(ValueError, match="one look"):   # two lines for one trial in one file
        lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha, T2), line(sha, T2, slot="T-PF-09"), name="twice.jsonl"))
    assert len(ledger.of_kind(lse.FACTORY_FORWARD)) == 1   # and the good line of that file was not appended either


def test_a_verdict_does_not_close_or_block_an_open_run(tmp_path):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    sha = run(ledger)
    lse.record_factory_declared(ledger, run_id="r2", run_file={"run_id": "r2"}, data_manifest_sha256="0" * 64, trial_count=1)
    assert lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha))) == 1
    reloaded = lse.Ledger(lp, anchor=ap)
    assert lse.summarize(reloaded)["run_open"] == "r2"
    lse.record_factory_result(reloaded, trials=[trial(T2)], max_t={}, report_sha256="3" * 64, frozen_sha256="4" * 64)
    final = lse.summarize(lse.Ledger(lp, anchor=ap))
    assert final["run_open"] is None and final["runs_recorded"] == 2 and final["live"]["looked"] == 1


@pytest.mark.parametrize("change, message", [
    ({"result_sha": "9" * 64}, "must name a factory_run_result"),
    ({"trial_id": "7" * 64}, "is not a trial of run"),
    ({"run_id": "other"}, "not the run of the result"),
    ({"lower": -0.001}, "lower bound is above zero"),
    ({"verdict": "FAIL"}, "lower bound is above zero"),
    ({"n": 99}, "need 100 resolved fires"),
    ({"lower": None}, "need 100 resolved fires and a lower bound"),
    ({"verdict": "UNDERPOWERED", "n": 100}, "fewer than 100"),
    ({"looked_at": "2026-11-20T01:59:59+00:00"}, "cannot predate"),
    ({"looked_at": "2027-03-01T21:10:00"}, "timezone"),
    ({"looked_at": None}, "time of its look"),
    ({"verdict": "NOT_YET"}, "only final verdicts"),
    ({"schema": "panel-factory-verdict/2"}, "schema"),
    ({"row_hash": ""}, "registration hash"),
    ({"chain": {"head": "d" * 64}}, "head and row count"),
])
def test_refusals(tmp_path, change, message):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    sha = run(ledger)
    change = dict(change)
    path = export(tmp_path, line(change.pop("result_sha", sha), **change))
    with pytest.raises(ValueError, match=message):
        lse.ingest_factory_verdicts(ledger, path)
    assert ledger.of_kind(lse.FACTORY_FORWARD) == []


def test_the_file_format_is_strict(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    sha = run(ledger)
    good = line(sha)
    extra = lse.canonical(dict(json.loads(good), note="x")).decode()
    spaced = json.dumps(json.loads(good), sort_keys=True)   # same content, not canonical bytes
    for bad, message in ((extra, "fields must be exactly"), (spaced, "not canonical")):
        with pytest.raises(ValueError, match=message):
            lse.ingest_factory_verdicts(ledger, export(tmp_path, bad))
    with pytest.raises(ValueError, match="line 2"):   # one bad line refuses the whole file
        lse.ingest_factory_verdicts(ledger, export(tmp_path, good, line(sha, T2, lower=-1.0)))
    assert ledger.of_kind(lse.FACTORY_FORWARD) == []


def test_only_a_factory_ledger_takes_live_verdicts(tmp_path):
    s11 = lse.Ledger.create(None, catalog=lse.synthetic_catalog(), ledger_id="s11-test")
    with pytest.raises(ValueError, match="factory ledger"):
        lse.ingest_factory_verdicts(s11, export(tmp_path, line("9" * 64)))
    with pytest.raises(ValueError, match="refuses"):
        s11._append({"kind": lse.FACTORY_FORWARD, "recorded_at": RECORDED})


def test_an_edited_verdict_is_caught_on_reload(tmp_path):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger), verdict="FAIL", lower=-0.01)))
    lp.write_bytes(lp.read_bytes().replace(b'"verdict":"fail"', b'"verdict":"pass"'))
    with pytest.raises(ValueError):
        lse.Ledger(lp, anchor=ap)


def test_reverification_applies_the_verdict_rules_not_only_the_hash_chain(tmp_path, monkeypatch):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    sha = run(ledger)
    path = export(tmp_path, line(sha, lower=-0.5))   # "PASS" with a negative lower bound
    with monkeypatch.context() as patch:             # a writer that skipped the rules: the chain and anchor are valid
        patch.setattr(lse, "validate_factory_forward", lambda *a, **k: None)
        assert lse.ingest_factory_verdicts(ledger, path) == 1
    with pytest.raises(ValueError, match="lower bound is above zero"):
        lse.Ledger(lp, anchor=ap)


def test_the_command_appends_once_and_reports_a_refusal(tmp_path, capsys):
    lp, ap = paths(tmp_path)
    sha = run(lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap))
    args = ["verdicts", "--ledger", str(lp), "--anchor", str(ap), "--file"]
    assert mf.main(args + [str(export(tmp_path, line(sha)))]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] and out["appended"] == 1 and out["live"]["passed"] == 1
    assert out["head_sha256"] == lse.Ledger(lp, anchor=ap).head
    assert mf.main(args + [str(export(tmp_path, line(sha)))]) == 0   # a retry adds nothing
    assert json.loads(capsys.readouterr().out)["appended"] == 0
    assert mf.main(args + [str(export(tmp_path, line(sha, T2, lower=-1.0), name="bad.jsonl"))]) == 2
    assert "lower bound" in json.loads(capsys.readouterr().out)["refused"]
    assert mf.main(args + [str(tmp_path / "missing.jsonl")]) == 2
    assert len(lse.Ledger(lp, anchor=ap).of_kind(lse.FACTORY_FORWARD)) == 1
