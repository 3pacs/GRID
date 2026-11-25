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
from datetime import date, timedelta

import pytest

from analysis import desk_receipt_witness as drw
from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf
from intelligence import desk_source_receipts as dsr

T1, T2, T3 = "a" * 64, "b" * 64, "c" * 64
RECORDED = "2026-11-20T02:00:00+00:00"
LOOKED = "2027-03-01T21:10:00+00:00"
RULE = {"feature": "F1", "op": ">=", "k": 1.5, "side": "long", "decision": "15:00", "horizon": "close"}
FAMILY, FEATURE = "SPY|change|15:00+close", "F1|z>=1.5|long"
ROW = {T1: "e" * 64, T2: "d" * 64, T3: "f" * 64}      # each slot's registration hash
SLOT = {T1: "T-PF-01", T2: "T-PF-02", T3: "T-PF-03"}


def look_text(trial, verdict="PASS", n=100, mean=0.0456, lower=0.0123, resolved=104):
    """A slot's stored look exactly as gex-signals writes it (factory_slots.look: sorted-key JSON)."""
    if verdict == "UNDERPOWERED":
        res = {"trial_id": SLOT[trial], "verdict": verdict, "resolved_fires": n, "min_n": 100, "sessions": 500,
               "note": "the deadline passed before min_n resolved fires: closed, counted, not passed"}
    else:
        res = {"trial_id": SLOT[trial], "verdict": verdict, "n": n, "mean_net": mean, "lower_bound": lower, "alpha_per_test": 0.005,
               "b": 10000, "seed": 20261119, "hit_rate": 0.58, "resolved_fires": resolved}
    res.update(version="panel-factory/2", looked_at=LOOKED, through_day="2027-03-01", row_hash=ROW[trial])
    return json.dumps(res, sort_keys=True)


# The three looks the tests import: slot 1 passed, slot 2 failed, slot 3 closed underpowered.
LOOK_TEXT = {T1: look_text(T1), T2: look_text(T2, "FAIL", lower=-0.01), T3: look_text(T3, "UNDERPOWERED", n=41)}
LOOK = {t: hashlib.sha256(text.encode()).hexdigest() for t, text in LOOK_TEXT.items()}     # digest of each stored look

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


def receipt_body(*trials, continuity="extends_last_receipt", texts=None, **change):
    """The panel_trials payload of a nightly Desk receipt that read these slots and retains their stored looks."""
    texts = texts or LOOK_TEXT
    content = {t: hashlib.sha256(f"registration of {SLOT[t]}".encode()).hexdigest() for t in trials}
    body = {"store": "panel_trials", "rows": 21, "head": "d" * 64, "continuity": continuity, "factory_looks_table": "present",
            "trials": [{"seq": i, "trial_id": SLOT[t], "family": "panel-factory", "registered_at": "2026-12-01T22:00:00+00:00",
                        "row_hash": ROW[t], "content_sha256": content[t]} for i, t in enumerate(trials, start=1)],
            "factory_slots": [{"trial_id": SLOT[t], "registered_at": "2026-12-01T22:00:00+00:00", "row_hash": ROW[t],
                               "content_sha256": content[t], "params": "{}"} for t in trials],
            "factory_looks": [{"trial_id": SLOT[t], "looked_at": LOOKED, "through_day": "2027-03-01", "result": texts[t],
                               "result_sha256": hashlib.sha256(texts[t].encode()).hexdigest()} for t in trials]}
    for key, value in change.items():
        for item in body["factory_looks"]:
            item[key] = value
    return body


def receipts(tmp_path, body, session="2027-03-01", name="desk", export_anchors=True):
    """A real receipt log whose ``session`` receipt read ``body`` as its panel_trials source, and the anchor copy
    exported to a vault folder. Returns (log folder, the anchor copy's path, the log)."""
    log = dsr.ReceiptLog(tmp_path / name / "log")
    read_at = dsr.session_cutoff(date.fromisoformat(session)) + timedelta(minutes=20)
    out = log.record({"panel_trials": lambda _cutoff: dsr.SourceRead(payload=body, rows=body.get("rows", 0), scope={"database": "signals.db"})},
                     now=lambda: read_at)
    assert out["written"] and out["record"]["sources"]["panel_trials"]["status"] == "read"
    vault = tmp_path / name / "vault"
    if export_anchors:
        log.export_anchors(vault)
    return log.root, vault / dsr.WITNESS_DIR / dsr.ANCHOR_NAME, log


def witness(ledger, tmp_path, body, session="2027-03-01", at="2027-03-02T00:20:05+00:00", name="desk"):
    """Build that night's receipt log and present it. The witness's own clock reads ``at``."""
    root, anchors, _log = receipts(tmp_path, body, session, name)
    return witness_from(ledger, root, anchors, session, at)


def witness_from(ledger, root, anchors, session="2027-03-01", at="2027-03-02T00:20:05+00:00"):
    clock, lse.now_iso = lse.now_iso, lambda: at
    try:
        return lse.witness_factory_verdicts(ledger, root, anchors, session)
    finally:
        lse.now_iso = clock


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
    assert live["lapsed"] is None                    # one passed, and no retirement reading covers it: unknown, not zero
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


def test_a_receipt_that_retains_the_same_stored_look_witnesses_the_verdict(tmp_path):
    lp, ap = paths(tmp_path)
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    sha = run(ledger)
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha), line(sha, T2, "FAIL", lower=-0.01)),
                                recorded_at="2027-03-01T22:00:00+00:00")
    root, anchors, log = receipts(tmp_path, receipt_body(T1))      # that night's receipt read slot 1's look; slot 2's is not in it
    assert witness_from(ledger, root, anchors) == 1 and witness_from(ledger, root, anchors) == 0   # once

    live = lse.summarize(lse.Ledger(lp, anchor=ap))["live"]
    assert (live["witnessed"], live["unwitnessed"]) == (1, 1)
    first, second = live["verdicts"]
    receipt_line = list(dsr.raw_lines(log.path))[1]
    assert first["witness"] == {"status": "witnessed", "receipt_session": "2027-03-01",
                                "receipt_head_sha256": hashlib.sha256(receipt_line).hexdigest(),
                                "witnessed_at": "2027-03-02T00:20:05+00:00"}
    assert first["arrived_at"] == "2027-03-01T22:00:00+00:00"   # when the verdict arrived and when its witness did stay separate
    assert second["witness"] == {"status": "unwitnessed"}
    record = lse.Ledger(lp, anchor=ap).of_kind(lse.FACTORY_FORWARD_WITNESS)[0]
    stored = json.loads(receipt_line)["sources"]["panel_trials"]
    assert record["receipt"]["payload_sha256"] == stored["payload_sha256"] and record["look_sha256"] == LOOK[T1]
    assert record["receipt"]["read_finished_at"] == stored["read_finished_at"] and record["receipt"]["log_records"] == 2
    assert record["receipt"]["external_anchor"] == {"file": dsr.ANCHOR_NAME, "sha256": hashlib.sha256(anchors.read_bytes()).hexdigest(),
                                                    "witnessed_records": 2}
    assert record["look_recomputed"] is True
    assert record["registration_content_sha256"] == hashlib.sha256(b"registration of T-PF-01").hexdigest()
    assert record["checked"] == {"by_code": list(drw.BY_CODE), "by_a_person": drw.BY_A_PERSON}   # says what it did not check
    assert "held off-host" in record["checked"]["by_a_person"]


def test_a_receipt_that_retains_another_look_witnesses_nothing(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger))))
    other = {T1: look_text(T1, mean=0.0457)}                                  # the receipt retains another stored look
    cases = {
        "another look": receipt_body(T1, texts=other),
        "another look time": receipt_body(T1, looked_at="2027-03-01T21:11:00+00:00"),
        "another last day": receipt_body(T1, through_day="2027-02-28"),
        "no looks table": {**receipt_body(T1), "factory_looks_table": "absent", "factory_looks": None},
    }
    under_another_row = receipt_body(T1)
    under_another_row["factory_slots"][0]["row_hash"] = "5" * 64              # the same look under another registration row
    cases["another registration row"] = under_another_row
    for name, body in cases.items():
        assert witness(ledger, tmp_path, body, name=name.replace(" ", "-")) == 0, name
    assert lse.summarize(ledger)["live"]["unwitnessed"] == 1 and ledger.of_kind(lse.FACTORY_FORWARD_WITNESS) == []


def test_a_receipt_identity_that_is_only_asserted_is_refused(tmp_path):
    """Codex on 30778727: a fabricated receipt id and a non-date session reached the append boundary."""
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger))))
    root, anchors, log = receipts(tmp_path, receipt_body(T1))
    fake = tmp_path / "fake-anchors.jsonl"
    fake.write_bytes(lse.canonical({"records": 2, "head_sha256": "7" * 64, "prev_anchor_sha256": None}) + b"\n")
    refusals = [
        ((tmp_path / "no-such-log", anchors, "2027-03-01"), "no receipt log"),
        ((root, anchors, "the night of the look"), "an ISO date"),
        ((root, anchors, "2027-03-02"), "no receipt for session"),
        ((root, log.anchor_path, "2027-03-01"), "other than the receipt log's own anchors"),   # local-head-only evidence
        ((root, tmp_path / "missing-anchors.jsonl", "2027-03-01"), "other than the receipt log's own anchors"),
        ((root, fake, "2027-03-01"), "does not verify"),                                       # a head nobody recorded
    ]
    for args, message in refusals:
        with pytest.raises(drw.WitnessError, match=message):
            witness_from(ledger, *args)
    assert ledger.of_kind(lse.FACTORY_FORWARD_WITNESS) == []


def test_a_receipt_the_outside_copy_does_not_cover_is_refused(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger))))
    root, anchors, log = receipts(tmp_path, {"store": "panel_trials", "continuity": "first_read", "factory_slots": [],
                                             "factory_looks_table": "present", "factory_looks": []}, session="2027-02-26")
    read_at = dsr.session_cutoff(date(2027, 3, 1)) + timedelta(minutes=20)
    body = receipt_body(T1)
    log.record({"panel_trials": lambda _c: dsr.SourceRead(payload=body, rows=21, scope={})}, now=lambda: read_at)
    with pytest.raises(drw.WitnessError, match="not under the anchor copy presented"):
        witness_from(ledger, root, anchors)                 # the copy was exported before this receipt existed
    log.export_anchors(anchors.parents[3])
    assert witness_from(ledger, root, anchors) == 1


def test_a_payload_that_is_not_the_bytes_the_receipt_names_is_refused(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger))))
    root, anchors, log = receipts(tmp_path, receipt_body(T1))
    stored = next(log.payload_dir.iterdir())
    stored.chmod(0o600)
    stored.write_bytes(stored.read_bytes().replace(b"0.0456", b"0.0999"))
    with pytest.raises(drw.WitnessError, match="not the bytes its hash names"):
        witness_from(ledger, root, anchors)
    assert ledger.of_kind(lse.FACTORY_FORWARD_WITNESS) == []


def _replace_receipt_after_verification(monkeypatch, log, source, field="read_finished_at", value="2027-03-01T21:22:00+00:00"):
    """Make the real verifier succeed, then rewrite the live receipt line before the caller can go back for it."""
    real = dsr.ReceiptLog.verify
    genuine = list(dsr.raw_lines(log.path))

    def verify_then_replace(self, *args, **kwargs):
        out = real(self, *args, **kwargs)
        record = json.loads(genuine[1])
        record["sources"][source][field] = value
        log.path.write_bytes(genuine[0] + b"\n" + dsr.canonical(record) + b"\n")
        return out

    monkeypatch.setattr(dsr.ReceiptLog, "verify", verify_then_replace)
    return genuine, real


def test_a_receipt_replaced_after_it_was_verified_cannot_supply_the_witness(tmp_path, monkeypatch):
    """Codex on 3a958111: the log was verified, then reopened, and a replacement line gave an unanchored head."""
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger))))
    root, anchors, log = receipts(tmp_path, receipt_body(T1))
    genuine, real = _replace_receipt_after_verification(monkeypatch, log, "panel_trials")

    assert witness_from(ledger, root, anchors) == 1

    replaced = list(dsr.raw_lines(log.path))
    assert replaced[1] != genuine[1]                                    # the live log was replaced under the witness
    record = ledger.of_kind(lse.FACTORY_FORWARD_WITNESS)[0]["receipt"]
    assert record["head_sha256"] == hashlib.sha256(genuine[1]).hexdigest() != hashlib.sha256(replaced[1]).hexdigest()
    assert record["read_finished_at"] == json.loads(genuine[1])["sources"]["panel_trials"]["read_finished_at"]
    assert record["external_anchor"]["sha256"] == hashlib.sha256(anchors.read_bytes()).hexdigest()
    monkeypatch.setattr(dsr.ReceiptLog, "verify", real)
    assert not dsr.ReceiptLog(root).verify(anchors)["ok"]              # the replacement itself never verifies:
    other = lse.Ledger.create_factory(None, ledger_id="factory-test-2")
    lse.ingest_factory_verdicts(other, export(tmp_path, line(run(other)), name="again.jsonl"))
    with pytest.raises(drw.WitnessError, match="does not verify"):      # presented from the start, it is refused
        witness_from(other, root, anchors)
    assert other.of_kind(lse.FACTORY_FORWARD_WITNESS) == []


def test_the_three_live_files_are_each_read_exactly_once(tmp_path, monkeypatch):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger))))
    root, anchors, log = receipts(tmp_path, receipt_body(T1))
    live = {str(log.path): "log", str(log.anchor_path): "local anchors", str(anchors): "anchor copy"}
    reads = []
    real_read, real_open = type(log.path).read_bytes, open

    def counting_read(self):
        reads.append(live.get(str(self)))
        return real_read(self)

    def counting_open(file, *args, **kwargs):
        reads.append(live.get(str(file)))
        return real_open(file, *args, **kwargs)

    monkeypatch.setattr(type(log.path), "read_bytes", counting_read)
    monkeypatch.setattr("builtins.open", counting_open)
    receipt = drw.verified_receipt(root, anchors, "2027-03-01", "panel_trials")
    monkeypatch.undo()
    assert sorted(x for x in reads if x) == ["anchor copy", "local anchors", "log"]      # verification ran on private copies
    assert receipt["log_records"] == 2


@pytest.mark.parametrize("field, over", [
    ("mean_net", {"mean": 0.0999}),
    ("lower_bound", {"lower": 0.02}),
    ("resolved_fires", {"resolved_fires": 150}),
    ("verdict", {"verdict": "FAIL", "lower": -0.01}),
])
def test_a_verdict_that_names_the_retained_look_and_differs_from_it_is_refused(tmp_path, field, over):
    """One altered field in the import, with the true look digest: the import is not that look."""
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger), **over)))     # the ledger cannot know yet
    with pytest.raises(ValueError, match="differs from the look the receipt retains") as refused:
        witness(ledger, tmp_path, receipt_body(T1))
    assert field in str(refused.value)
    assert ledger.of_kind(lse.FACTORY_FORWARD_WITNESS) == [] and lse.summarize(ledger)["live"]["unwitnessed"] == 1


def test_an_underpowered_look_and_a_failed_one_are_compared_field_by_field_too(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    sha = run(ledger, trials=((T1, True), (T2, True), (T3, True)))
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha, T2, "FAIL", lower=-0.01), underpowered(sha)))
    assert witness(ledger, tmp_path, receipt_body(T2, T3)) == 2
    assert lse.factory_look_differs(json.loads(LOOK_TEXT[T3]), {**next(iter(lse.factory_forward_index(ledger).values())), "n": 1}) != []


def test_a_look_text_that_is_not_its_digest_or_a_payload_read_without_history_is_refused(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger))))
    with pytest.raises(ValueError, match="not the text its digest names"):
        witness(ledger, tmp_path, receipt_body(T1, result=look_text(T1, mean=0.0999)), name="edited-text")
    with pytest.raises(ValueError, match="not read against the receipt log's own history"):
        witness(ledger, tmp_path, receipt_body(T1, continuity="not_checked"), name="no-history")
    for name, strip in (("no-trials", lambda b: b.pop("trials")), ("empty-trials", lambda b: b.update(trials=[]))):
        unbound = receipt_body(T1)
        strip(unbound)                                       # the payload of 0bdeba34: registrations by row hash only
        with pytest.raises(ValueError, match="does not bind its registrations by content"):
            witness(ledger, tmp_path, unbound, name=name)
    for name, change in (("another-time", {"registered_at": "2020-01-01T00:00:00+00:00"}), ("no-digest", {"content_sha256": None})):
        moved = receipt_body(T1)
        moved["factory_slots"][0].update(change)             # the same row hash, another registration
        with pytest.raises(ValueError, match="the import is not that registration"):
            witness(ledger, tmp_path, moved, name=name)
    old_shape = receipt_body(T1)
    del old_shape["continuity"]
    for look in old_shape["factory_looks"]:
        del look["result"]                                   # the payload of 778d8bb1: a digest and no text
    with pytest.raises(ValueError, match="not read against the receipt log's own history"):
        witness(ledger, tmp_path, old_shape, name="old-shape")


def test_a_witness_can_arrive_long_after_the_verdict_and_never_before_it(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    sha = run(ledger)
    assert witness(ledger, tmp_path, receipt_body(T1), name="early") == 0          # nothing imported yet: nothing to witness
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha)), recorded_at="2027-03-01T22:00:00+00:00")
    assert lse.summarize(ledger)["live"]["verdicts"][0]["witness"] == {"status": "unwitnessed"}
    assert witness(ledger, tmp_path, receipt_body(T1), session="2027-03-09", at="2027-03-10T00:20:00+00:00", name="late") == 1
    entry = lse.summarize(ledger)["live"]["verdicts"][0]
    assert entry["witness"]["receipt_session"] == "2027-03-09" and entry["arrived_at"] < entry["witness"]["witnessed_at"]
    root, anchors, _log = receipts(tmp_path, receipt_body(T1), name="clock")
    other = lse.Ledger.create_factory(None, ledger_id="factory-test-2")
    lse.ingest_factory_verdicts(other, export(tmp_path, line(run(other)), name="v2.jsonl"))
    with pytest.raises(ValueError, match="before its receipt was read"):
        witness_from(other, root, anchors, at="2027-03-01T21:00:00+00:00")       # the witness's clock is behind the receipt's


@pytest.mark.parametrize("change, message", [
    ({"forward_sha256": "9" * 64}, "must name a live verdict"),
    ({"look_sha256": "4" * 64}, "another stored look"),
    ({"trial_id": T2}, "another verdict"),
    ({"receipt": {"session": "2027-03-01", "head_sha256": "x", "payload_sha256": "7" * 64}}, "names its receipt"),
    ({"receipt": {"session": "2027-03-01", "head_sha256": "7" * 64, "payload_sha256": "8" * 64}}, "anchor copy that covered"),   # the old shape
    ({"receipt": {"session": "the night of the look"}}, "names its receipt"),
    ({"receipt": {"session": "the night of the look", "head_sha256": "7" * 64, "payload_sha256": "8" * 64}}, "ISO date"),
    ({"checked": {"by_code": ["everything"], "by_a_person": ""}}, "what a person must check"),
    ({"witnessed_at": "2027-03-02T00:20:05"}, "timezone"),
    ({"witnessed_at": "2027-03-01T21:00:00+00:00"}, "before its receipt was read"),
])
def test_the_ledger_refuses_a_witness_that_is_not_for_this_verdict(tmp_path, change, message):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger))))
    forward_sha = next(iter(lse.factory_forward_index(ledger)))
    good = {"kind": lse.FACTORY_FORWARD_WITNESS, "run_id": "r1", "trial_id": T1, "forward_sha256": forward_sha,
            "look_sha256": LOOK[T1],
            "receipt": {"session": "2027-03-01", "head_sha256": "7" * 64, "payload_sha256": "8" * 64, "log_records": 2,
                        "read_finished_at": "2027-03-01T21:20:00+00:00",
                        "external_anchor": {"file": "a.jsonl", "sha256": "6" * 64, "witnessed_records": 2}},
            "checked": {"by_code": list(drw.BY_CODE), "by_a_person": drw.BY_A_PERSON},
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
    assert witness(ledger, tmp_path, receipt_body(T1)) == 1
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
        lse.witness_factory_verdicts(s11, tmp_path / "log", tmp_path / "anchors.jsonl", "2027-03-01")
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


def test_the_commands_import_once_witness_once_and_report_a_refusal(tmp_path, capsys, monkeypatch):
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
    root, anchors, log = receipts(tmp_path, receipt_body(T1))
    seen = ["verdicts-witness", "--ledger", str(lp), "--anchor", str(ap), "--receipts", str(root),
            "--external-anchors", str(anchors), "--session", "2027-03-01"]
    assert mf.main(seen[:-1] + ["2027-03-02"]) == 2                               # no receipt for that session
    assert "no receipt for session" in json.loads(capsys.readouterr().out)["refused"]
    assert mf.main(seen[:-3] + [str(log.anchor_path), "--session", "2027-03-01"]) == 2    # the log's own anchors are not a witness
    capsys.readouterr()
    with pytest.raises(SystemExit):
        mf.main(seen + ["--receipt-head", "7" * 64])                              # a head is not something the caller supplies
    capsys.readouterr()
    monkeypatch.setattr(lse, "now_iso", lambda: "2027-03-02T00:20:05+00:00")
    assert mf.main(seen) == 0
    out = json.loads(capsys.readouterr().out)
    assert out | {"head_sha256": None, "retirement_reading": None} == {
        "ok": True, "witnessed_now": 1, "witnessed": 1, "unwitnessed": 0, "head_sha256": None,
        "retirement_reading": None, "lapsed": None}
    # That receipt read no retirement table, so the passed slot's retirement is unknown, and the command says why.
    assert out["retirement_reading"]["appended"] is False and "unknown, not no" in out["retirement_reading"]["reason"]
    assert mf.main(seen) == 0 and json.loads(capsys.readouterr().out)["witnessed_now"] == 0
    assert len(lse.Ledger(lp, anchor=ap).of_kind(lse.FACTORY_FORWARD)) == 1


# --- retirement: a passed slot that later lapses (3pacs/GRID#1163) ------------------------------


def check_text(trial, at_fires=125, lower=0.011, floor=0.0, checked_at="2027-04-09T21:10:00+00:00", through="2027-04-09", **over):
    """One retirement check exactly as gex-signals stores it (factory_slots.retire: sorted-key JSON)."""
    res = {"at_fires": at_fires, "n": 100, "mean_net": 0.031, "lower_bound": lower, "level": 0.9, "floor_usd": floor,
           "lapsed": lower < floor, "b": 10000, "seed": 20261119, "trial_id": SLOT[trial], "version": "panel-factory/2",
           "checked_at": checked_at, "through_day": through,
           "cohort": {"first_day": "2026-12-02", "last_day": through, "days": [], "sha256": "5" * 64}}
    res.update(over)
    return json.dumps(res, sort_keys=True)


LAPSE = {"at_fires": 150, "lower": -0.02, "checked_at": "2027-05-07T21:10:00+00:00", "through": "2027-05-07"}


def retirement_body(*trials, checks=(), **change):
    """A nightly receipt's panel_trials payload that also read the retirement checks: ``checks`` is (trial, text) pairs."""
    body = receipt_body(*trials, **change)
    body["factory_retirement_checks_table"] = "present"
    body["factory_retirement_checks"] = []
    for trial, text in checks:
        stored = json.loads(text)
        body["factory_retirement_checks"].append(
            {"trial_id": SLOT[trial], "at_fires": stored["at_fires"], "checked_at": stored["checked_at"],
             "through_day": stored["through_day"], "lapsed": stored["lapsed"], "result": text,
             "result_sha256": hashlib.sha256(text.encode()).hexdigest()})
    return body


def reading(ledger, tmp_path, body, session, name=None):
    """Build that session's receipt log and read it for the passed slots' retirement checks, an hour after the read."""
    root, anchors, _log = receipts(tmp_path, body, session, name or f"desk-{session}")
    at = (dsr.session_cutoff(date.fromisoformat(session)) + timedelta(hours=1)).isoformat()
    clock, lse.now_iso = lse.now_iso, lambda: at
    try:
        return lse.read_factory_retirement(ledger, root, anchors, session)
    finally:
        lse.now_iso = clock


def three_verdicts(tmp_path, lp=None, ap=None):
    ledger = lse.Ledger.create_factory(lp, ledger_id="factory-test", anchor=ap)
    sha = run(ledger)
    path = export(tmp_path, line(sha, T1, "PASS"), line(sha, T2, "FAIL", lower=-0.01, mean=0.002), underpowered(sha))
    assert lse.ingest_factory_verdicts(ledger, path, recorded_at="2027-03-02T00:00:00+00:00") == 3
    return ledger, sha


def one_pass(tmp_path):
    """A ledger whose only verdict so far is slot 1's pass; slot 2 has not been looked at yet."""
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    sha = run(ledger)
    assert lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha, T1, "PASS"), name="one.jsonl")) == 1
    return ledger, sha


S1, S2, S3, S4 = "2027-03-01", "2027-04-12", "2027-05-10", "2027-05-11"
PASSING, LAPSING = check_text(T1), check_text(T1, **LAPSE)


def test_readings_follow_a_passed_slot_until_it_lapses_and_summarize_counts_it(tmp_path):
    lp, ap = paths(tmp_path)
    ledger, _ = three_verdicts(tmp_path, lp, ap)
    before = lse.summarize(ledger)["live"]
    assert before["lapsed"] is None and before["retirement"]["passed_not_read"] == 1 and before["retirement"]["readings"] == 0

    first = reading(ledger, tmp_path, retirement_body(T1, T2, T3), S1)
    assert (first["appended"], first["covered"], first["lapsed"]) == (True, 1, 0)   # only the passed slot is read
    live = lse.summarize(ledger)["live"]
    assert live["lapsed"] == 0 and live["passed"] == 1      # a count now: the one passed slot was read, and had not lapsed
    assert live["retirement"] == {**live["retirement"], "as_of_session": S1, "passed_read": 1, "passed_not_read": 0, "readings": 1}
    by_trial = {v["trial_id"]: v["retirement"] for v in live["verdicts"]}
    assert by_trial[T1] == {"status": "not_lapsed_at_last_reading", "checks": 0}
    assert by_trial[T2] is None and by_trial[T3] is None    # a slot that did not pass has nothing to retire

    assert reading(ledger, tmp_path, retirement_body(T1, checks=[(T1, PASSING)]), S2)["lapsed"] == 0
    assert lse.summarize(ledger)["live"]["verdicts"][0]["retirement"] == {"status": "not_lapsed_at_last_reading", "checks": 1}

    third = reading(ledger, tmp_path, retirement_body(T1, checks=[(T1, PASSING), (T1, LAPSING)]), S3)
    assert (third["appended"], third["lapsed"]) == (True, 1)
    reloaded = lse.Ledger(lp, anchor=ap)                    # every reading re-verifies by its rules from the file
    live = lse.summarize(reloaded)["live"]
    assert live["lapsed"] == 1 and live["passed"] == 1      # passed is passed; lapsed says what became of it
    assert live["retirement"]["as_of_session"] == S3 and live["retirement"]["readings"] == 3
    assert live["verdicts"][0]["retirement"] == {
        "status": "lapsed", "at_fires": 150, "checked_at": LAPSE["checked_at"], "through_day": LAPSE["through"],
        "result_sha256": hashlib.sha256(LAPSING.encode()).hexdigest(), "n": 100, "lower_bound": -0.02, "floor_usd": 0.0}

    again = reading(reloaded, tmp_path, retirement_body(T1, checks=[(T1, PASSING), (T1, LAPSING)]), S4)
    assert again["appended"] is False and "already recorded as lapsed" in again["reason"]
    stale = reading(reloaded, tmp_path, retirement_body(T1, checks=[(T1, PASSING), (T1, LAPSING)]), S2, name="desk-again")
    assert stale["appended"] is False and "this session or a later one" in stale["reason"]
    assert len(reloaded.of_kind(lse.FACTORY_RETIREMENT_READING)) == 3


def test_unknown_is_not_zero(tmp_path):
    ledger, sha = one_pass(tmp_path)

    older = reading(ledger, tmp_path, receipt_body(T1), S1)                  # a receipt from before the checks were read
    assert older["appended"] is False and "unknown, not no" in older["reason"]
    assert lse.summarize(ledger)["live"]["lapsed"] is None

    elsewhere = reading(ledger, tmp_path, retirement_body(T2), S2)           # reads the table, retains no passed slot
    assert elsewhere["appended"] is False and "retains none of the passed slots" in elsewhere["reason"]
    another_look = retirement_body(T1, texts={**LOOK_TEXT, T1: look_text(T1, mean=0.05)})
    assert reading(ledger, tmp_path, another_look, S3)["appended"] is False  # the same slot, another stored look
    assert lse.summarize(ledger)["live"]["lapsed"] is None

    assert reading(ledger, tmp_path, retirement_body(T1), S4)["appended"] is True
    assert lse.summarize(ledger)["live"]["lapsed"] == 0
    # A second slot passes later. No reading covers it yet, so the count is unknown again, not still 0.
    t2 = hashlib.sha256(look_text(T2).encode()).hexdigest()
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha, T2, "PASS", look_sha256=t2), name="later.jsonl"))
    live = lse.summarize(ledger)["live"]
    assert live["passed"] == 2 and live["lapsed"] is None and live["retirement"]["passed_not_read"] == 1
    assert live["retirement"]["lapsed_among_read"] == 0


def test_with_no_passed_verdict_nothing_can_lapse(tmp_path):
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    assert lse.summarize(ledger)["live"]["lapsed"] == 0                      # no verdict at all
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(run(ledger), T2, "FAIL", lower=-0.01)))

    out = reading(ledger, tmp_path, retirement_body(T2), S1)

    assert out["appended"] is False and "nothing can lapse" in out["reason"]
    assert lse.summarize(ledger)["live"]["lapsed"] == 0 and ledger.of_kind(lse.FACTORY_RETIREMENT_READING) == []


def test_a_second_passed_slot_is_read_beside_the_first(tmp_path):
    ledger, sha = one_pass(tmp_path)
    assert reading(ledger, tmp_path, retirement_body(T1, T2, checks=[(T1, PASSING), (T1, LAPSING)]), S1)["lapsed"] == 1
    t2_text = {**LOOK_TEXT, T2: look_text(T2)}
    lse.ingest_factory_verdicts(ledger, export(tmp_path, line(sha, T2, "PASS", look_sha256=hashlib.sha256(t2_text[T2].encode()).hexdigest()),
                                               name="t2.jsonl"))
    both = retirement_body(T1, T2, texts=t2_text, checks=[(T1, PASSING), (T1, LAPSING), (T2, check_text(T2))])

    out = reading(ledger, tmp_path, both, S2)

    assert (out["appended"], out["covered"], out["lapsed"]) == (True, 2, 1)   # T1's lapse is carried, T2 is read
    live = lse.summarize(ledger)["live"]
    assert (live["passed"], live["lapsed"]) == (2, 1)
    # A later receipt that no longer retains the first slot would take it back: refused, nothing appended.
    with pytest.raises(ValueError, match="a slot the last reading covered is missing"):
        reading(ledger, tmp_path, retirement_body(T2, texts=t2_text, checks=[(T2, check_text(T2))]), S3)
    assert len(ledger.of_kind(lse.FACTORY_RETIREMENT_READING)) == 2


@pytest.mark.parametrize("checks, message", [
    ([(T1, PASSING)], "takes nothing back"),                                                   # the lapse is gone
    ([(T1, PASSING), (T1, check_text(T1, **{**LAPSE, "lower": -0.03}))], "takes nothing back"),   # another lapse than the one recorded
    ([(T1, LAPSING)], "takes nothing back"),                                                   # fewer checks than were read
])
def test_a_later_receipt_cannot_take_a_lapse_back(tmp_path, checks, message):
    ledger, _ = three_verdicts(tmp_path)
    assert reading(ledger, tmp_path, retirement_body(T1, checks=[(T1, PASSING), (T1, LAPSING)]), S1)["lapsed"] == 1
    with pytest.raises(ValueError, match=message):
        reading(ledger, tmp_path, retirement_body(T1, T2, checks=checks), S2)
    assert lse.summarize(ledger)["live"]["lapsed"] == 1


def _swap(body, **change):
    body["factory_retirement_checks"][-1].update(change)
    return body


@pytest.mark.parametrize("body, message", [
    (lambda: _swap(retirement_body(T1, checks=[(T1, PASSING)]), result=PASSING.replace("0.011", "0.012")), "not the text its digest names"),
    (lambda: _swap(retirement_body(T1, checks=[(T1, PASSING)]), lapsed=True), "lapse flag is not what its own bound and floor give"),
    (lambda: retirement_body(T1, checks=[(T1, check_text(T1, lapsed=True))]), "lapse flag is not what its own bound and floor give"),
    (lambda: retirement_body(T1, checks=[(T1, check_text(T2))]), "is not that slot's check"),
    (lambda: _swap(retirement_body(T1, checks=[(T1, PASSING)]), at_fires=126), "is not that slot's check"),
    (lambda: retirement_body(T1, checks=[(T1, check_text(T1, lower=-0.02)), (T1, check_text(T1, at_fires=150))]), "a check after the one that lapsed it"),
    (lambda: retirement_body(T1, checks=[(T1, check_text(T1, at_fires=100, lower=-0.02))]), "comes after the look"),
    (lambda: retirement_body(T1, checks=[(T1, check_text(T1, lower=-0.02, checked_at="2027-02-01T21:10:00+00:00"))]), "comes after the look"),
    (lambda: retirement_body(T1, continuity="not_checked"), "not read against the receipt log's own history"),
])
def test_a_receipt_that_contradicts_itself_or_the_look_is_refused(tmp_path, body, message):
    ledger, _ = three_verdicts(tmp_path)
    with pytest.raises(ValueError, match=message):
        reading(ledger, tmp_path, body(), S1)
    assert ledger.of_kind(lse.FACTORY_RETIREMENT_READING) == [] and lse.summarize(ledger)["live"]["lapsed"] is None


def test_what_a_reading_must_be(tmp_path):
    ledger, _ = three_verdicts(tmp_path)
    reading(ledger, tmp_path, retirement_body(T1, checks=[(T1, PASSING), (T1, LAPSING)]), S1)
    good = ledger.of_kind(lse.FACTORY_RETIREMENT_READING)[0]
    forward = lse.factory_forward_index(ledger)
    failed = next(sha for sha, v in forward.items() if v["verdict"] == "fail")
    slot = good["slots"][0]

    def refused(message, earlier=(), **change):
        with pytest.raises(ValueError, match=message):
            lse.validate_factory_retirement_reading({**good, **change}, forward, list(earlier))

    lse.validate_factory_retirement_reading(good, forward, [])
    refused("names at least one passed slot", slots=[])
    refused("must name a passed live verdict", slots=[{**slot, "forward_sha256": failed}])
    refused("must name a passed live verdict", slots=[{**slot, "forward_sha256": "0" * 64}])
    refused("names each slot once", slots=[slot, slot])
    refused("another verdict, another stored look or another slot", slots=[{**slot, "look_sha256": "0" * 64}])
    refused("another verdict, another stored look or another slot", slots=[{**slot, "slot_trial_id": "T-PF-09"}])
    refused("how many checks", slots=[{**slot, "lapsed": 1}])
    refused("carries its lapsing check", slots=[{**slot, "lapse": None}])
    refused("carries its lapsing check", slots=[{**slot, "lapse": {**slot["lapse"], "extra": 1}}])
    refused("below the registered floor", slots=[{**slot, "lapse": {**slot["lapse"], "lower_bound": 0.0}}])
    refused("comes after the look", slots=[{**slot, "lapse": {**slot["lapse"], "at_fires": 100}}])
    refused("carries no lapsing check", slots=[{**slot, "lapsed": False}])
    refused("cannot arrive before its receipt was read", read_at="2027-03-01T00:00:00+00:00")
    refused("a witness names its receipt", receipt={**good["receipt"], "head_sha256": "short"})
    refused("for a session after the last reading's", earlier=[good])
    assert lse.FACTORY_RETIREMENT_READING in lse.FACTORY_KINDS


def test_a_reading_written_past_the_rules_is_caught_on_reload(tmp_path, monkeypatch):
    lp, ap = paths(tmp_path)
    ledger, _ = three_verdicts(tmp_path, lp, ap)
    with monkeypatch.context() as patch:             # a writer that skipped the rules: the chain and anchor are valid
        patch.setattr(lse, "validate_factory_retirement_reading", lambda *a, **k: None)
        text = check_text(T1, at_fires=150, lower=0.02, lapsed=True)     # "lapsed" with a bound above the floor
        body = retirement_body(T1, checks=[(T1, text)])
        record = {"kind": lse.FACTORY_RETIREMENT_READING, "read_at": "2027-03-02T00:20:05+00:00", "recorded_at": "2027-03-02T00:20:05+00:00",
                  "checks_recomputed": True, "checked": {"by_code": ["x"], "by_a_person": "that the anchor copy is the vault's own file"},
                  "receipt": {"session": S1, "head_sha256": "1" * 64, "payload_sha256": "2" * 64, "log_records": 2,
                              "read_finished_at": "2027-03-01T21:20:00+00:00", "external_anchor": {"sha256": "3" * 64, "witnessed_records": 2}},
                  "slots": [{"forward_sha256": next(iter(lse.factory_forward_index(ledger))), "run_id": "r1", "trial_id": T1,
                             "look_sha256": LOOK[T1], "slot_trial_id": SLOT[T1], "checks": 1, "lapsed": True,
                             "lapse": {"at_fires": 150, "checked_at": "2027-04-09T21:10:00+00:00", "through_day": "2027-04-09",
                                       "result_sha256": body["factory_retirement_checks"][0]["result_sha256"], "n": 100,
                                       "lower_bound": 0.02, "floor_usd": 0.0}}]}
        ledger._append(record)
    with pytest.raises(ValueError, match="below the registered floor"):
        lse.Ledger(lp, anchor=ap)


def test_only_a_factory_ledger_takes_retirement_readings(tmp_path):
    s11 = lse.Ledger.create(None, catalog=lse.synthetic_catalog(), ledger_id="s11-test")
    with pytest.raises(ValueError, match="belong on a factory ledger"):
        lse.read_factory_retirement(s11, tmp_path / "log", tmp_path / "anchors.jsonl", S1)
