"""The live trial ledger as a Desk receipt source (3pacs/GRID#1162).

gex-signals registers every forward test in ``panel_trials`` (SQLite, a
``prev_hash`` chain) and stores a factory slot's one look in
``factory_looks``. The nightly receipt reads both, so each session's hub post
puts the chain head where this host cannot rewrite it. Synthetic database
only: no live file is opened.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from intelligence import desk_source_receipts as dsr

UTC = timezone.utc
WED_CLOSE = datetime(2026, 11, 18, 21, 0, tzinfo=UTC)
THU_CLOSE = datetime(2026, 11, 19, 21, 0, tzinfo=UTC)


def _hash(n: int) -> str:
    return hashlib.sha256(str(n).encode()).hexdigest()


LOOK_1 = json.dumps({"verdict": "PASS", "n": 100}, sort_keys=True)
OTHER_FACTORY_TABLES = [t for t in dsr.FACTORY_TABLES if t != "factory_looks"]


def _db(path, trials=(("T-1", "panel-rules-b"), ("T-PF-01", "panel-factory"), ("T-PF-02", "panel-factory")),
        looks: bool | None = True, tables=tuple(OTHER_FACTORY_TABLES)):
    """A signals.db as gex-signals makes it: the trial chain and the factory tables. ``looks=None`` is a database from
    before the factory (none of its tables). ``tables`` names the other factory tables to create beside the looks."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE panel_trials (seq INTEGER PRIMARY KEY AUTOINCREMENT, trial_id TEXT, family TEXT, "
                 "registered_at TEXT, params TEXT, prev_hash TEXT, row_hash TEXT NOT NULL)")
    previous = None
    for i, (trial, family) in enumerate(trials, start=1):
        conn.execute("INSERT INTO panel_trials (trial_id, family, registered_at, params, prev_hash, row_hash) "
                     "VALUES (?, ?, ?, '{}', ?, ?)", (trial, family, f"2026-12-0{i}T22:00:00+00:00", previous, _hash(i)))
        previous = _hash(i)
    if looks is not None:
        for name in tables:
            conn.execute(f"CREATE TABLE {name} (trial_id TEXT, day TEXT)")
        conn.execute("CREATE TABLE factory_looks (trial_id TEXT PRIMARY KEY, looked_at TEXT NOT NULL, "
                     "through_day TEXT, result TEXT NOT NULL)")
        if looks:
            conn.execute("INSERT INTO factory_looks VALUES ('T-PF-01', '2027-03-01T21:10:00+00:00', '2027-03-01', ?)", (LOOK_1,))
    conn.commit()
    conn.close()
    return path


def _read(db):
    return dsr.panel_trials_source(db)(WED_CLOSE)


def test_the_head_the_families_the_slots_and_the_looks_are_read(tmp_path):
    read = _read(_db(tmp_path / "signals.db"))

    p = read.payload
    assert (read.rows, p["rows"], p["head"]) == (3, 3, _hash(3))
    assert p["families"] == {"panel-factory": 2, "panel-rules-b": 1}
    assert [s["trial_id"] for s in p["factory_slots"]] == ["T-PF-01", "T-PF-02"]
    assert p["factory_slots"][0]["row_hash"] == _hash(2)
    look = p["factory_looks"][0]
    assert (look["trial_id"], look["looked_at"], look["through_day"]) == ("T-PF-01", "2027-03-01T21:10:00+00:00", "2027-03-01")
    assert look["result_sha256"] == hashlib.sha256(b'{"n": 100, "verdict": "PASS"}').hexdigest()
    assert look["result"] == LOOK_1 and hashlib.sha256(look["result"].encode()).hexdigest() == look["result_sha256"]
    assert p["continuity"] == "not_checked" and read.scope["history"] == "not_checked"   # asked directly, nothing to compare with
    assert p["links_checked"] is True and p["row_hashes_recomputed"] is False   # says what was not checked
    assert p["proof"] == "row_links_only" and "row links only" in read.scope["proof"]
    assert p["row_hashes"] == [_hash(1), _hash(2), _hash(3)] and p["factory_looks_table"] == "present"
    assert read.scope["database"] == "signals.db"


def test_an_empty_ledger_is_a_read_of_zero_rows(tmp_path):
    read = _read(_db(tmp_path / "signals.db", trials=(), looks=False))

    assert read.rows == 0 and read.payload["head"] is None and read.payload["factory_looks"] == []


def test_a_missing_looks_table_is_not_an_empty_one(tmp_path):
    before = _read(_db(tmp_path / "before.db", looks=None)).payload      # a database from before the factory tables
    empty = _read(_db(tmp_path / "empty.db", looks=False)).payload       # the table is there and holds nothing
    assert (before["factory_looks_table"], before["factory_looks"]) == ("absent", None)
    assert (empty["factory_looks_table"], empty["factory_looks"]) == ("present", [])
    half = _db(tmp_path / "half.db", looks=None)                         # a generation with factory tables but not this one
    conn = sqlite3.connect(half)
    conn.execute("CREATE TABLE factory_fires (trial_id TEXT, day TEXT)")
    conn.commit()
    conn.close()
    with pytest.raises(dsr.SourceUnavailable, match="incomplete generation.*factory_looks"):
        _read(half)


@pytest.mark.parametrize("tables", [(), ("factory_fires",), ("factory_feature_values", "factory_outcomes")])
def test_the_factory_tables_come_as_a_set_and_the_looks_table_alone_is_not_one(tmp_path, tables):
    """Codex on 778d8bb1: a looks-only layout passed."""
    with pytest.raises(dsr.SourceUnavailable, match="incomplete generation"):
        _read(_db(tmp_path / "partial.db", tables=tables))


def test_a_look_must_belong_to_a_registered_factory_slot(tmp_path):
    db = _db(tmp_path / "signals.db")
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO factory_looks VALUES ('T-1', '2027-03-02T21:10:00+00:00', '2027-03-02', '{}')")   # not a factory slot
    conn.commit()
    conn.close()
    with pytest.raises(dsr.SourceUnavailable, match="no registered factory slot"):
        _read(db)


@pytest.mark.parametrize("damage", [
    "UPDATE panel_trials SET prev_hash = 'x' WHERE seq = 2",
    "DELETE FROM panel_trials WHERE seq = 2",
    "UPDATE panel_trials SET row_hash = 'short' WHERE seq = 3",
])
def test_broken_links_are_unknown_not_a_head(tmp_path, damage):
    db = _db(tmp_path / "signals.db")
    conn = sqlite3.connect(db)
    conn.execute(damage)
    conn.commit()
    conn.close()

    with pytest.raises(dsr.SourceUnavailable, match="hash links are broken"):
        _read(db)


def test_a_missing_file_a_missing_table_and_no_path_are_unknown(tmp_path):
    with pytest.raises(dsr.SourceUnavailable, match="could not be"):
        _read(tmp_path / "absent.db")
    assert not (tmp_path / "absent.db").exists()   # a read-only open creates nothing
    sqlite3.connect(tmp_path / "empty.db").close()
    with pytest.raises(dsr.SourceUnavailable, match="could not be read"):
        _read(tmp_path / "empty.db")
    with pytest.raises(dsr.SourceUnavailable, match="not unchanged"):
        _read(None)


def test_the_read_never_writes(tmp_path):
    db = _db(tmp_path / "signals.db")
    before = db.read_bytes()
    _read(db)
    assert db.read_bytes() == before
    assert sorted(x.name for x in tmp_path.iterdir()) == ["signals.db"]


def test_a_new_registration_changes_the_head_the_next_receipt_holds(tmp_path):
    db = _db(tmp_path / "signals.db")
    log = dsr.ReceiptLog(tmp_path / "log")
    sources = {"panel_trials": dsr.panel_trials_source(db)}
    at = WED_CLOSE + timedelta(minutes=30)
    first = log.record(sources, now=lambda: at)["record"]["sources"]["panel_trials"]

    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO panel_trials (trial_id, family, registered_at, params, prev_hash, row_hash) "
                 "VALUES ('T-PF-03', 'panel-factory', '2026-12-04T22:00:00+00:00', '{}', ?, ?)", (_hash(3), _hash(4)))
    conn.commit()
    conn.close()
    later = THU_CLOSE + timedelta(minutes=30)
    second = log.record(sources, now=lambda: later)["record"]["sources"]["panel_trials"]

    assert first["status"] == second["status"] == "read" and (first["rows"], second["rows"]) == (3, 4)
    assert first["payload_sha256"] != second["payload_sha256"]
    assert json.loads(log.payload(second["payload_sha256"]))["head"] == _hash(4)
    assert log.verify(full=True)["ok"]


def test_a_rewritten_chain_is_unknown_and_an_extended_one_is_read(tmp_path):
    db = _db(tmp_path / "signals.db")
    log = dsr.ReceiptLog(tmp_path / "log")
    source = dsr.panel_trials_source(db, lambda: log.last_payload("panel_trials"))
    assert log.last_payload("panel_trials") is None
    first = log.record({"panel_trials": source}, now=lambda: WED_CLOSE + timedelta(minutes=30))["record"]
    assert first["sources"]["panel_trials"]["status"] == "read" and log.last_payload("panel_trials")["head"] == _hash(3)

    conn = sqlite3.connect(db)   # the last row replaced by another: same length, a different chain
    conn.execute("UPDATE panel_trials SET row_hash = ? WHERE seq = 3", (_hash(99),))
    conn.commit()
    conn.close()
    second = log.record({"panel_trials": source}, now=lambda: THU_CLOSE + timedelta(minutes=30))["record"]["sources"]["panel_trials"]
    assert second["status"] == "unknown" and "no longer starts with the rows the last receipt read" in second["reason"]
    assert log.last_payload("panel_trials")["head"] == _hash(3)   # the last head that was read, not a guess
    assert log.verify(full=True)["ok"]


def _two_receipts(tmp_path, change):
    """One receipt, then ``change`` applied to the database, then the next session's receipt. Returns (log, second)."""
    db = _db(tmp_path / "signals.db")
    log = dsr.ReceiptLog(tmp_path / "log")
    source = dsr.panel_trials_source(db, lambda: log.last_payload("panel_trials"))
    first = log.record({"panel_trials": source}, now=lambda: WED_CLOSE + timedelta(minutes=30))["record"]
    assert first["sources"]["panel_trials"]["status"] == "read"
    assert json.loads(log.payload(first["sources"]["panel_trials"]["payload_sha256"]))["continuity"] == "first_read"
    conn = sqlite3.connect(db)
    for statement in change:
        conn.execute(statement)
    conn.commit()
    conn.close()
    second = log.record({"panel_trials": source}, now=lambda: THU_CLOSE + timedelta(minutes=30))["record"]["sources"]["panel_trials"]
    return log, second


@pytest.mark.parametrize("change, reason", [
    (["DELETE FROM factory_looks WHERE trial_id = 'T-PF-01'"], "a stored look the last receipt read is gone"),
    (["UPDATE factory_looks SET result = '{\"n\": 100, \"verdict\": \"FAIL\"}'"], "a stored look the last receipt read has changed"),
    (["UPDATE factory_looks SET looked_at = '2027-03-05T21:10:00+00:00'"], "a stored look the last receipt read has changed"),
    (["UPDATE factory_looks SET through_day = '2027-03-02'"], "a stored look the last receipt read has changed"),
    (["DROP TABLE factory_looks", "DROP TABLE factory_fires", "DROP TABLE factory_outcomes", "DROP TABLE factory_feature_values"],
     "the factory tables the last receipt read are gone"),
])
def test_a_removed_or_altered_look_is_unknown_while_every_trial_hash_is_unchanged(tmp_path, change, reason):
    """Codex on 778d8bb1: unchanged trial hashes still passed after the old factory look was removed."""
    log, second = _two_receipts(tmp_path, change)

    assert second["status"] == "unknown" and reason in second["reason"]
    kept = log.last_payload("panel_trials")                 # the last state that was read is still what the log holds
    assert kept["row_hashes"] == [_hash(1), _hash(2), _hash(3)] and kept["factory_looks"][0]["result"] == LOOK_1
    assert log.verify(full=True)["ok"]


def test_an_added_look_and_an_added_trial_extend_the_last_receipt(tmp_path):
    log, second = _two_receipts(tmp_path, [
        "INSERT INTO factory_looks VALUES ('T-PF-02', '2027-04-01T21:10:00+00:00', '2027-04-01', '{\"n\": 100, \"verdict\": \"FAIL\"}')",
        ("INSERT INTO panel_trials (trial_id, family, registered_at, params, prev_hash, row_hash) "
         f"VALUES ('T-PF-03', 'panel-factory', '2026-12-04T22:00:00+00:00', '{{}}', '{_hash(3)}', '{_hash(4)}')"),
    ])

    assert second["status"] == "read" and second["scope"]["history"].startswith("trial rows and stored looks only added")
    payload = json.loads(log.payload(second["payload_sha256"]))
    assert payload["continuity"] == "extends_last_receipt"
    assert [look["trial_id"] for look in payload["factory_looks"]] == ["T-PF-01", "T-PF-02"] and payload["rows"] == 4


def test_a_last_payload_that_is_not_the_bytes_its_hash_names_is_unknown(tmp_path):
    db = _db(tmp_path / "signals.db")
    log = dsr.ReceiptLog(tmp_path / "log")
    source = dsr.panel_trials_source(db, lambda: log.last_payload("panel_trials"))
    first = log.record({"panel_trials": source}, now=lambda: WED_CLOSE + timedelta(minutes=30))["record"]
    stored = log.payload_dir / f"{first['sources']['panel_trials']['payload_sha256']}.json"
    stored.chmod(0o600)
    stored.write_bytes(stored.read_bytes().replace(b"PASS", b"FAIL"))        # the earlier look, rewritten in the receipt's own file

    second = log.record({"panel_trials": source}, now=lambda: THU_CLOSE + timedelta(minutes=30))["record"]["sources"]["panel_trials"]

    assert second["status"] == "unknown" and second["failure"] == "ReceiptError"


def test_history_is_append_only_for_trials_and_for_looks():
    look = {"trial_id": "T", "looked_at": "t", "through_day": "d", "result": "{}", "result_sha256": "s"}
    a = {"row_hashes": ["1", "2"], "factory_looks_table": "present", "factory_looks": [look]}
    assert dsr.panel_history_break(None, a) is None and dsr.panel_history_break(a, a) is None
    more = {**a, "row_hashes": ["1", "2", "3"], "factory_looks": [look, {**look, "trial_id": "U"}]}
    assert dsr.panel_history_break(a, more) is None
    assert "is gone" in dsr.panel_history_break(a, {**a, "factory_looks": []})
    assert "has changed" in dsr.panel_history_break(a, {**a, "factory_looks": [{**look, "result_sha256": "x"}]})
    assert "has changed" in dsr.panel_history_break(a, {**a, "factory_looks": [{**look, "result": "{ }"}]})
    assert "are gone" in dsr.panel_history_break(a, {**a, "factory_looks_table": "absent", "factory_looks": None})
    assert "cannot be compared" in dsr.panel_history_break({"row_hashes": ["1", "2"]}, a)        # an older payload shape
    before = {"row_hashes": ["1"], "factory_looks_table": "absent", "factory_looks": None}
    assert dsr.panel_history_break(before, a) is None                                            # the factory arrived since


def test_chain_extension_is_a_prefix_relation():
    a = {"row_hashes": ["1", "2"], "factory_looks_table": "absent", "factory_looks": None}
    assert dsr.panel_chain_extends(None, a) and dsr.panel_chain_extends(a, a)
    assert dsr.panel_chain_extends(a, {**a, "row_hashes": ["1", "2", "3"]})
    assert not dsr.panel_chain_extends(a, {**a, "row_hashes": ["1", "9", "3"]})    # a row replaced
    assert not dsr.panel_chain_extends(a, {**a, "row_hashes": ["1"]})              # a row removed
    assert not dsr.panel_chain_extends(a, {"head": "x"})                      # a payload without the hashes


def test_the_fourth_source_makes_a_version_2_receipt_and_one_log_replays_both(tmp_path):
    db = _db(tmp_path / "signals.db")
    v1 = dsr.default_sources(None, tmp_path / "l.jsonl", tmp_path / "a.jsonl")
    v2 = dsr.default_sources(None, tmp_path / "l.jsonl", tmp_path / "a.jsonl", db)
    assert list(v1) == ["factory_ledger", "sleuth_leads", "thesis_log"]                    # exactly what version 1 always had
    assert list(v2) == ["factory_ledger", "sleuth_leads", "thesis_log", "panel_trials"]
    log = dsr.ReceiptLog(tmp_path / "log")
    fri_close = datetime(2026, 11, 20, 21, 0, tzinfo=UTC)
    keep = ("thesis_log", "panel_trials")   # the two that need no database or ledger file

    def record(sources, close):
        at = close + timedelta(minutes=30)
        return log.record({k: v for k, v in sources.items() if k in keep}, now=lambda: at)["record"]

    one, two, three = record(v1, WED_CLOSE), record(v2, THU_CLOSE), record(v1, fri_close)   # the source off, on, off again
    assert "receipt_version" not in one and "receipt_version" not in three and two["receipt_version"] == 2
    assert set(one["sources"]) == set(three["sources"]) == {"thesis_log"} and set(two["sources"]) == {"thesis_log", "panel_trials"}
    check = log.verify(full=True)
    assert check["ok"] and check["receipts"] == 3
    # what a strict version 1 consumer needs: a receipt without receipt_version never carries the new source
    for record_ in log.records():
        if record_.get("kind") == "receipt" and "receipt_version" not in record_:
            assert "panel_trials" not in record_["sources"]


@pytest.mark.parametrize("forge, detail", [
    (lambda body: body.pop("receipt_version"), "a version 1 receipt carries a version 2 source"),
    (lambda body: body.update(receipt_version=3), "unknown receipt version 3"),
    (lambda body: body["sources"].pop("panel_trials"), "a version 2 receipt without its version 2 source"),
])
def test_a_receipt_whose_version_and_sources_disagree_fails_verification(tmp_path, monkeypatch, forge, detail):
    db = _db(tmp_path / "signals.db")
    log = dsr.ReceiptLog(tmp_path / "log")
    real = log._append

    def forged(body, at):
        forge(body)
        return real(body, at)
    monkeypatch.setattr(log, "_append", forged)
    at = WED_CLOSE + timedelta(minutes=30)
    log.record({"panel_trials": dsr.panel_trials_source(db), "thesis_log": dsr.thesis_log_source()}, now=lambda: at)
    check = log.verify()
    assert not check["ok"] and detail in check["detail"]


def test_an_unnamed_database_is_unknown_when_the_source_is_asked_for_directly(tmp_path):
    log = dsr.ReceiptLog(tmp_path / "log")
    at = WED_CLOSE + timedelta(minutes=30)
    entry = log.record({"panel_trials": dsr.panel_trials_source(None)}, now=lambda: at)["record"]["sources"]["panel_trials"]
    assert entry["status"] == "unknown" and entry["failure"] == "SourceUnavailable" and "--panel-trials-db" in entry["reason"]
    assert "payload_sha256" not in entry


@pytest.mark.parametrize("named", [True, False])
def test_the_nightly_script_names_the_database_only_when_it_is_set(tmp_path, named):
    seen = tmp_path / "argv"
    stub = tmp_path / "py"   # records how the recorder was called, then refuses, which ends the script
    stub.write_text("#!/usr/bin/env bash\n"
                    'if [ "$1" = "-c" ]; then exec python3 "$@"; fi\n'
                    f'printf \'%s\\n\' "$@" > "{seen}"\n'
                    "echo '{\"ok\": false, \"refused\": \"test\"}'; exit 2\n", encoding="utf-8")
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    env = {k: v for k, v in os.environ.items() if k != "DESK_PANEL_TRIALS_DB"}
    env.update(DESK_RECEIPTS_DIR=str(tmp_path / "log"), DESK_FACTORY_LEDGER="l", DESK_FACTORY_ANCHOR="a",
               DESK_RECEIPTS_PYTHON=str(stub), DESK_RECEIPTS_REPORT="")
    if named:
        env["DESK_PANEL_TRIALS_DB"] = "/some/where/signals.db"
    script = Path(__file__).resolve().parents[1] / "scripts" / "desk_receipts_run.sh"

    done = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True, timeout=60, check=False)

    argv = seen.read_text(encoding="utf-8").split("\n")
    assert done.returncode == 2 and "record" in argv
    if named:
        assert argv[argv.index("--panel-trials-db") + 1] == "/some/where/signals.db"
    else:
        assert "--panel-trials-db" not in argv
