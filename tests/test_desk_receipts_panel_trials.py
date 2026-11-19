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


def _db(path, trials=(("T-1", "panel-rules-b"), ("T-PF-01", "panel-factory"), ("T-PF-02", "panel-factory")),
        looks: bool | None = True):
    """A signals.db with the two tables as gex-signals makes them. ``looks=None`` leaves the looks table out."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE panel_trials (seq INTEGER PRIMARY KEY AUTOINCREMENT, trial_id TEXT, family TEXT, "
                 "registered_at TEXT, params TEXT, prev_hash TEXT, row_hash TEXT NOT NULL)")
    previous = None
    for i, (trial, family) in enumerate(trials, start=1):
        conn.execute("INSERT INTO panel_trials (trial_id, family, registered_at, params, prev_hash, row_hash) "
                     "VALUES (?, ?, ?, '{}', ?, ?)", (trial, family, f"2026-12-0{i}T22:00:00+00:00", previous, _hash(i)))
        previous = _hash(i)
    if looks is not None:
        conn.execute("CREATE TABLE factory_looks (trial_id TEXT PRIMARY KEY, looked_at TEXT NOT NULL, "
                     "through_day TEXT, result TEXT NOT NULL)")
        if looks:
            conn.execute("INSERT INTO factory_looks VALUES ('T-PF-01', '2027-03-01T21:10:00+00:00', '2027-03-01', ?)",
                         (json.dumps({"verdict": "PASS", "n": 100}, sort_keys=True),))
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
    assert p["links_checked"] is True and p["row_hashes_recomputed"] is False   # says what was not checked
    assert read.scope["database"] == "signals.db"


def test_an_empty_ledger_is_a_read_of_zero_rows(tmp_path):
    read = _read(_db(tmp_path / "signals.db", trials=(), looks=False))

    assert read.rows == 0 and read.payload["head"] is None and read.payload["factory_looks"] == []


def test_a_database_from_before_the_looks_table_says_the_looks_are_not_there(tmp_path):
    assert _read(_db(tmp_path / "signals.db", looks=None)).payload["factory_looks"] is None


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


def test_without_a_named_database_the_receipt_says_unknown(tmp_path):
    sources = dsr.default_sources(None, tmp_path / "l.jsonl", tmp_path / "a.jsonl")
    assert list(sources) == ["factory_ledger", "sleuth_leads", "thesis_log", "panel_trials"]
    log = dsr.ReceiptLog(tmp_path / "log")
    at = WED_CLOSE + timedelta(minutes=30)

    entry = log.record({"panel_trials": sources["panel_trials"]}, now=lambda: at)["record"]["sources"]["panel_trials"]

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
