"""Live verdicts on the factory ledger (3pacs/GRID#1162).

A factory candidate is tested only forward, in a gex-signals ``panel-factory``
slot. The slot's one look comes back as an export line and is appended to the
factory ledger as ``factory_forward_outcome``; a nightly Desk receipt that read
the same stored look is then recorded as ``factory_forward_witness``. These
tests cover the seam: export file -> import -> witness -> reload -> summarize,
and every refusal.
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
RULE = {"feature": "F1", "op": ">=", "k": 1.5, "side": "long", "decision": "15:00", "horizon": "close"}
FAMILY, FEATURE = "SPY|change|15:00+close", "F1|z>=1.5|long"
LOOK = {T1: "1" * 64, T2: "2" * 64, T3: "3" * 64}     # digest of each slot's stored look
ROW = {T1: "e" * 64, T2: "d" * 64, T3: "f" * 64}      # each slot's registration hash
SLOT = {T1: "T-PF-01", T2: "T-PF-02", T3: "T-PF-03"}

# One export line exactly as gex-signals writes it. The same text and hash are pinned in gex
# (signals-20261023/tests/test_factory_export.py): if either side changes the format, its pin fails.
EXAMPLE_LINE = (
    '{"alpha_per_test":0.005,"chain":{"head":"' + "d" * 64 + '","rows":21},"exported_at":"2027-03-01T21:15:00+00:00",'
    '"family":"panel-factory","look_sha256":"' + "c" * 64 + '","looked_at":"2027-03-01T21:10:00+00:00","lower_bound":0.0123,"mean_net":0.0456,"n":100,'
    '"registered_at":"2026-12-01T22:00:00+00:00","resolved_fires":104,"row_hash":"' + "e" * 64 + '",'
    '"rule":{"decision":"15:00","feature":"F1","horizon":"close","k":1.5,"op":">=","side":"long"},'
    '"schema":"panel-factory-verdict/2","sessions":212,"slot_trial_id":"T-PF-01",'
    '"source":{"result_sha256":"' + "f" * 64 + '","run_id":"r1","trial_id":"' + "a" * 64 + '"},'
    '"through_day":"2027-03-01","verdict":"PASS"}'
)
EXAMPLE_SHA256 = "d71228b35211ba333a2a628df7e9305c0f48b30280111d97df6edac07dca9e8a"


def paths(tmp_path):
    (tmp_path / "ledger").mkdir(exist_ok=True)
    (tmp_path / "anchor").mkdir(exist_ok=True)
    return tmp_path / "ledger" / "factory.jsonl", tmp_path / "anchor" / "factory.anchor.jsonl"


CLASS = {T1: "price", T2: "tape"}   # T3 carries no class, like a run recorded before classes existed


def trial(tid, selected=False):
    out = {"trial_id": tid, "status": "tested", "selected": selected, "family": FAMILY, "feature": FEATURE}
    return dict(out, **{"class": CLASS[tid]}) if tid in CLASS else out


def run(ledger, run_id="r1", trials=((T1, True), (T2, False), (T3, False))):
    """Declare and record one run; returns the result record's line sha256."""
    lse.record_factory_declared(ledger, run_id=run_id, run_file={"run_id": run_id}, data_manifest_sha256="0" * 64,
                                trial_count=len(trials), recorded_at="2026-11-20T01:00:00+00:00")
    out = lse.record_factory_result(ledger, trials=[trial(t, s) for t, s in trials], max_t={"critical_t": 3.1},
                                    report_sha256="1" * 64, frozen_sha256="2" * 64, recorded_at=RECORDED)
    return out["sha256"]


def line(result_sha, trial_id=T1, verdict="PASS", n=100, lower=0.0123, mean=0.0456, run_id="r1", **over):
    entry = {
        "schema": "panel-factory-verdict/2", "slot_trial_id": SLOT.get(trial_id, "T-PF-09"), "family": "panel-factory",
        "registered_at": "2026-12-01T22:00:00+00:00", "row_hash": ROW.get(trial_id, "9" * 64),
        "source": {"run_id": run_id, "trial_id": trial_id, "result_sha256": result_sha},
        "rule": dict(RULE), "verdict": verdict, "n": n, "mean_net": mean, "lower_bound": lower, "alpha_per_test": 0.005,
        "resolved_fires": 104 if n == 100 else n, "sessions": 212, "looked_at": LOOKED, "through_day": "2027-03-01",
        "look_sha256": LOOK.get(trial_id, "9" * 64),
        "chain": {"head": "d" * 64, "rows": 21}, "exported_at": "2027-03-01T21:15:00+00:00",
    }
    entry.update(over)
    return lse.canonical(entry).decode()


def underpowered(result_sha, trial_id=T3, **over):
    return line(result_sha, trial_id, "UNDERPOWERED", n=41, lower=None, mean=None, sessions=500, **over)


def export(tmp_path, *lines, name="verdicts.jsonl"):
    path = tmp_path / name
    path.write_text("".join(x + "\n" for x in lines))
    return path


def receipt_payload(*trials, **change):
    """The panel_trials payload of a nightly Desk receipt that read these slots and their stored looks."""
    body = {"store": "panel_trials", "rows": 21, "head": "d" * 64,
            "factory_slots": [{"trial_id": SLOT[t], "registered_at": "2026-12-01T22:00:00+00:00", "row_hash": ROW[t]} for t in trials],
            "factory_looks": [{"trial_id": SLOT[t], "looked_at": LOOKED, "through_day": "2027-03-01", "result_sha256": LOOK[t]}
                              for t in trials]}
    for key, value in change.items():
        for item in body["factory_looks"]:
            item[key] = value
    return lse.canonical(body)


def witness(ledger, payload, session="2027-03-01", at="2027-03-02T00:20:05+00:00"):
    return lse.witness_factory_verdicts(ledger, payload, receipt_session=session, receipt_head_sha256="7" * 64, witnessed_at=at)


def test_the_export_line_is_the_pinned_contract():
    entry = json.loads(EXAMPLE_LINE)
    assert tuple(sorted(entry)) == tuple(sorted(lse.FACTORY_VERDICT_FIELDS))
    assert entry["schema"] == lse.FACTORY_VERDICT_SCHEMA
    assert lse.canonical(entry).decode() == EXAMPLE_LINE
    assert hashlib.sha256(EXAMPLE_LINE.encode()).hexdigest() == EXAMPLE_SHA256
    assert lse.factory_rule_names(entry["rule"]) == (FAMILY, FEATURE)   # the slot's rule, as the factory names it


def test_export_import_reload_summarize(tmp_path):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    sha = run(ledger)
    path = export(tmp_path, line(sha, T1, "PASS"), line(sha, T2, "FAIL", lower=-0.01, mean=0.002), underpowered(sha))
    assert lse.ingest_factory_verdicts(ledger, path, recorded_at="2027-03-02T00:00:00+00:00") == 3

    reloaded = lse.Ledger(lp, anchor=ap)   # the whole chain re-verifies from the file and its anchor
    records = reloaded.of_kind(lse.FACTORY_FORWARD)
    assert [r["verdict"] for r in records] == ["pass", "fail", "underpowered"]
    assert records[0]["result_sha256"] == sha and records[0]["slot"]["trial_id"] == "T-PF-01"
    assert records[0]["look_sha256"] == LOOK[T1] and records[0]["slot"]["row_hash"] == ROW[T1]
    assert records[0]["source_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()

    live = lse.summarize(reloaded)["live"]
    assert (live["looked"], live["passed"], live["failed"], live["underpowered"]) == (3, 1, 1, 1)
    assert (live["witnessed"], live["unwitnessed"]) == (0, 3)   # imported, not yet matched to a receipt
    assert all(v["witness"] == {"status": "unwitnessed"} for v in live["verdicts"])
    assert live["looked_not_selected_in_run"] == 2   # T2 and T3 were not selected by their run: shown, not hidden
    assert live["lapsed"] is None                    # no retirement checker yet: unknown, not zero
    assert live["by_class"] == {
        "price": {"looked": 1, "passed": 1, "failed": 0, "underpowered": 0},
        "tape": {"looked": 1, "passed": 0, "failed": 1, "underpowered": 0},
        "unclassed": {"looked": 1, "passed": 0, "failed": 0, "underpowered": 1},
    }
    summary = lse.summarize(reloaded)
    assert summary["candidates_declared"] == 3 and summary["runs_recorded"] == 1   # a verdict is not a new candidate


# --- retries ----------------------------------------------------------------------------------


def test_a_repackaged_export_of_the_same_result_adds_nothing(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    sha = run(ledger)
    assert lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha))) == 1
    assert lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha))) == 0   # the same file again
    later = line(sha, exported_at="2027-04-01T00:00:00+00:00", chain={"head": "8" * 64, "rows": 30})   # a new envelope
    assert lse.ingest_factory_verdicts(ledger, export(tmp_path, later, underpowered(sha), name="repackaged.jsonl")) == 1
    assert [r["trial_id"] for r in ledger.of_kind(lse.FACTORY_FORWARD)] == [T1, T3]   # only the new slot was appended
    assert lse.ingest_factory_verdicts(ledger, export(tmp_path, later, later, name="twice.jsonl")) == 0


@pytest.mark.parametrize("change, field", [
    ({"verdict": "FAIL", "lower": -0.2}, "verdict"),
    ({"mean": 0.0457}, "mean_net"),                       # the same verdict, another mean
    ({"lower": 0.0124}, "lower_bound"),
    ({"look_sha256": "4" * 64}, "look_sha256"),           # the same numbers from another stored look
    ({"through_day": "2027-03-02"}, "through_day"),
    ({"sessions": 213}, "sessions"),
    ({"resolved_fires": 105}, "resolved_fires"),
    ({"row_hash": "5" * 64}, "slot"),                     # another registration of the slot
    ({"looked_at": "2027-03-01T21:11:00+00:00"}, "looked_at"),
])
def test_a_changed_result_for_the_same_trial_is_a_conflict_not_a_retry(tmp_path, change, field):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    sha = run(ledger)
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha)))
    with pytest.raises(ValueError, match="a conflict, not a retry") as err:
        lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha, **change), name="again.jsonl"))
    assert field in str(err.value)
    with pytest.raises(ValueError, match="a conflict, not a retry"):   # and inside one file
        lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha, T2, "FAIL", lower=-0.01), line(sha, T2, "FAIL", lower=-0.02), name="twice.jsonl"))
    assert len(ledger.of_kind(lse.FACTORY_FORWARD)) == 1   # and the good line of that file was not appended either


def test_another_writer_between_load_and_append_stops_the_batch_and_a_rerun_completes_it(tmp_path):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    sha = run(ledger)
    path = export(tmp_path, line(sha), line(sha, T2, "FAIL", lower=-0.01))
    other = lse.Ledger(lp, anchor=ap)                      # a second writer on the same files
    lse.record_factory_declared(other, run_id="r2", run_file={"run_id": "r2"}, data_manifest_sha256="0" * 64, trial_count=1)
    with pytest.raises(ValueError, match="0 of 2 verdicts appended, then: ledger file changed outside this ledger"):
        lse.ingest_factory_verdicts(ledger, path)
    assert lse.ingest_factory_verdicts(lse.Ledger(lp, anchor=ap), path) == 2   # the same import on the ledger as it now is
    assert lse.summarize(lse.Ledger(lp, anchor=ap))["run_open"] == "r2"       # and the other writer's run is still open


# --- the witness ------------------------------------------------------------------------------


def test_a_receipt_that_read_the_same_stored_look_witnesses_the_verdict(tmp_path):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    sha = run(ledger)
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha), line(sha, T2, "FAIL", lower=-0.01)),
                                recorded_at="2027-03-01T22:00:00+00:00")
    payload = receipt_payload(T1)          # that night's receipt read slot 1's look; slot 2's is not in it
    assert witness(ledger, payload) == 1 and witness(ledger, payload) == 0   # once

    live = lse.summarize(lse.Ledger(lp, anchor=ap))["live"]
    assert (live["witnessed"], live["unwitnessed"]) == (1, 1)
    first, second = live["verdicts"]
    assert first["witness"] == {"status": "witnessed", "receipt_session": "2027-03-01", "receipt_head_sha256": "7" * 64,
                                "witnessed_at": "2027-03-02T00:20:05+00:00"}
    assert first["arrived_at"] == "2027-03-01T22:00:00+00:00"   # when the verdict arrived and when its witness did stay separate
    assert second["witness"] == {"status": "unwitnessed"}
    record = lse.Ledger(lp, anchor=ap).of_kind(lse.FACTORY_FORWARD_WITNESS)[0]
    assert record["receipt"]["payload_sha256"] == hashlib.sha256(payload).hexdigest() and record["look_sha256"] == LOOK[T1]


@pytest.mark.parametrize("change", [
    {"result_sha256": "4" * 64},                 # the receipt read another stored look
    {"looked_at": "2027-03-01T21:11:00+00:00"},  # another look time
    {"through_day": "2027-02-28"},               # another last day
])
def test_a_receipt_with_another_look_witnesses_nothing(tmp_path, change):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger))))
    assert witness(ledger, receipt_payload(T1, **change)) == 0
    body = json.loads(receipt_payload(T1))
    body["factory_slots"][0]["row_hash"] = "5" * 64          # the same look under another registration row
    assert witness(ledger, lse.canonical(body)) == 0
    assert witness(ledger, lse.canonical({"store": "panel_trials", "factory_slots": [], "factory_looks": None})) == 0   # no look table
    assert lse.summarize(ledger)["live"]["unwitnessed"] == 1 and ledger.of_kind(lse.FACTORY_FORWARD_WITNESS) == []


def test_a_witness_can_arrive_long_after_the_verdict_and_never_before_it(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    sha = run(ledger)
    assert witness(ledger, receipt_payload(T1)) == 0          # nothing imported yet: nothing to witness
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha)), recorded_at="2027-03-01T22:00:00+00:00")
    assert lse.summarize(ledger)["live"]["verdicts"][0]["witness"] == {"status": "unwitnessed"}
    assert witness(ledger, receipt_payload(T1), session="2027-03-09", at="2027-03-10T00:20:00+00:00") == 1   # a late receipt
    entry = lse.summarize(ledger)["live"]["verdicts"][0]
    assert entry["witness"]["receipt_session"] == "2027-03-09" and entry["arrived_at"] < entry["witness"]["witnessed_at"]


@pytest.mark.parametrize("change, message", [
    ({"forward_sha256": "9" * 64}, "must name a live verdict"),
    ({"look_sha256": "4" * 64}, "another stored look"),
    ({"trial_id": T2}, "another verdict"),
    ({"receipt": {"session": "2027-03-01", "head_sha256": "x", "payload_sha256": "7" * 64}}, "names its receipt"),
    ({"witnessed_at": "2027-03-02T00:20:05"}, "timezone"),
])
def test_the_ledger_refuses_a_witness_that_is_not_for_this_verdict(tmp_path, change, message):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger))))
    forward_sha = next(iter(lse.factory_forward_index(ledger)))
    good = {"kind": lse.FACTORY_FORWARD_WITNESS, "run_id": "r1", "trial_id": T1, "forward_sha256": forward_sha,
            "look_sha256": LOOK[T1], "receipt": {"session": "2027-03-01", "head_sha256": "7" * 64, "payload_sha256": "8" * 64},
            "witnessed_at": "2027-03-02T00:20:05+00:00", "recorded_at": "2027-03-02T00:20:06+00:00"}
    with pytest.raises(ValueError, match=message):
        ledger._append({**good, **change})
    ledger._append(good)
    with pytest.raises(ValueError, match="already has its witness"):
        ledger._append(good)


# --- other rules ------------------------------------------------------------------------------


def test_a_verdict_does_not_close_or_block_an_open_run(tmp_path):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    sha = run(ledger)
    lse.record_factory_declared(ledger, run_id="r2", run_file={"run_id": "r2"}, data_manifest_sha256="0" * 64, trial_count=1)
    assert lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha))) == 1
    assert witness(ledger, receipt_payload(T1)) == 1
    reloaded = lse.Ledger(lp, anchor=ap)
    assert lse.summarize(reloaded)["run_open"] == "r2"
    lse.record_factory_result(reloaded, trials=[trial(T2)], max_t={}, report_sha256="3" * 64, frozen_sha256="4" * 64)
    final = lse.summarize(lse.Ledger(lp, anchor=ap))
    assert final["run_open"] is None and final["runs_recorded"] == 2 and final["live"]["looked"] == 1


@pytest.mark.parametrize("change, message", [
    ({"result_sha": "9" * 64}, "must name a factory_run_result"),
    ({"trial_id": "7" * 64}, "is not a trial of run"),
    ({"run_id": "other"}, "not the run of the result"),
    ({"rule": {**RULE, "k": 2.0}}, "not the rule of the trial"),             # another threshold
    ({"rule": {**RULE, "horizon": "60m"}}, "not the rule of the trial"),     # another horizon
    ({"rule": {**RULE, "side": "short"}}, "not the rule of the trial"),
    ({"rule": {"feature": "F1"}}, "not the rule of the trial"),
    ({"lower": -0.001}, "lower bound as recorded is above zero"),
    ({"lower": 0.0}, "lower bound as recorded is above zero"),               # a pass needs a bound above zero as stored
    ({"lower": 0}, "lower bound as recorded is above zero"),
    ({"verdict": "FAIL"}, "lower bound as recorded is above zero"),
    ({"n": 99}, "exactly 100 resolved fires"),
    ({"n": 101, "resolved_fires": 104}, "exactly 100 resolved fires"),       # the look is on the first hundred, no more
    ({"lower": None}, "finite mean and lower bound"),
    ({"mean": None}, "finite mean and lower bound"),
    ({"resolved_fires": 99}, "n <= resolved_fires <= sessions"),
    ({"sessions": 50}, "n <= resolved_fires <= sessions"),
    ({"verdict": "UNDERPOWERED", "n": 100}, "fewer than 100"),
    ({"verdict": "UNDERPOWERED", "n": 41, "resolved_fires": 41}, "no mean or bound"),   # underpowered with a bound attached
    ({"verdict": "UNDERPOWERED", "n": 41, "resolved_fires": 60, "lower": None, "mean": None}, "all counted"),
    ({"alpha_per_test": 0.05}, "per-slot alpha is fixed"),
    ({"look_sha256": "abc"}, "exact stored look"),
    ({"row_hash": ""}, "registration hash"),
    ({"family": "panel-rules"}, "panel-factory family"),
    ({"looked_at": "2026-11-20T01:59:59+00:00"}, "cannot predate"),
    ({"looked_at": "2026-12-01T21:59:59+00:00"}, "cannot predate"),         # before its own slot was registered
    ({"looked_at": "2027-03-01T21:10:00"}, "timezone"),
    ({"looked_at": None}, "time of its look"),
    ({"through_day": None}, "last day it covers"),
    ({"verdict": "NOT_YET"}, "only final verdicts"),
    ({"schema": "panel-factory-verdict/1"}, "schema"),
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
    missing = lse.canonical({k: v for k, v in json.loads(good).items() if k != "look_sha256"}).decode()
    for bad, message in ((extra, "fields must be exactly"), (spaced, "not canonical"), (missing, "fields must be exactly")):
        with pytest.raises(ValueError, match=message):
            lse.ingest_factory_verdicts(ledger, export(tmp_path, bad))
    with pytest.raises(ValueError, match="line 2"):   # one bad line refuses the whole file
        lse.ingest_factory_verdicts(ledger, export(tmp_path, good, line(sha, T2, lower=-1.0)))
    assert ledger.of_kind(lse.FACTORY_FORWARD) == []


def test_only_a_factory_ledger_takes_live_verdicts(tmp_path):
    s11 = lse.Ledger.create(None, catalog=lse.synthetic_catalog(), ledger_id="s11-test")
    with pytest.raises(ValueError, match="factory ledger"):
        lse.ingest_factory_verdicts(s11, export(tmp_path, line("9" * 64)))
    with pytest.raises(ValueError, match="factory ledger"):
        lse.witness_factory_verdicts(s11, receipt_payload(T1), receipt_session="2027-03-01", receipt_head_sha256="7" * 64)
    with pytest.raises(ValueError, match="refuses"):
        s11._append({"kind": lse.FACTORY_FORWARD, "recorded_at": RECORDED})


def test_an_edited_verdict_is_caught_on_reload(tmp_path):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger), verdict="FAIL", lower=-0.01)))
    lp.write_bytes(lp.read_bytes().replace(b'"verdict":"fail"', b'"verdict":"pass"'))
    with pytest.raises(ValueError):
        lse.Ledger(lp, anchor=ap)


def test_reverification_applies_the_rules_not_only_the_hash_chain(tmp_path, monkeypatch):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    sha = run(ledger)
    with monkeypatch.context() as patch:             # a writer that skipped the rules: the chain and anchor are valid
        patch.setattr(lse, "validate_factory_forward", lambda *a, **k: None)
        assert lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha, lower=-0.5))) == 1   # "PASS", negative bound
    with pytest.raises(ValueError, match="lower bound as recorded is above zero"):
        lse.Ledger(lp, anchor=ap)


def test_the_commands_import_once_witness_once_and_report_a_refusal(tmp_path, capsys):
    lp, ap = paths(tmp_path)
    sha = run(lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap))
    args = ["verdicts", "--ledger", str(lp), "--anchor", str(ap), "--file"]
    assert mf.main(args + [str(export(tmp_path, line(sha)))]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] and out["appended"] == 1 and out["live"]["passed"] == 1 and out["live"]["unwitnessed"] == 1
    assert out["head_sha256"] == lse.Ledger(lp, anchor=ap).head
    assert mf.main(args + [str(export(tmp_path, line(sha)))]) == 0   # a retry adds nothing
    assert json.loads(capsys.readouterr().out)["appended"] == 0
    assert mf.main(args + [str(export(tmp_path, line(sha, T2, lower=-1.0), name="bad.jsonl"))]) == 2
    assert "lower bound" in json.loads(capsys.readouterr().out)["refused"]
    assert mf.main(args + [str(tmp_path / "missing.jsonl")]) == 2
    capsys.readouterr()
    payload = tmp_path / "payload.json"
    payload.write_bytes(receipt_payload(T1))
    seen = ["verdicts-witness", "--ledger", str(lp), "--anchor", str(ap), "--payload", str(payload),
            "--receipt-session", "2027-03-01", "--receipt-head", "7" * 64]
    assert mf.main(seen) == 0
    assert json.loads(capsys.readouterr().out) | {"head_sha256": None} == {
        "ok": True, "witnessed_now": 1, "witnessed": 1, "unwitnessed": 0, "head_sha256": None}
    assert mf.main(seen) == 0 and json.loads(capsys.readouterr().out)["witnessed_now"] == 0
    assert len(lse.Ledger(lp, anchor=ap).of_kind(lse.FACTORY_FORWARD)) == 1
