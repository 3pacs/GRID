"""Offline controls for scripts/ingest_publication_review.py.

A fake engine stands in for griddb and mirrors the DDL's hard constraints
(bind names, int4 range, jsonb validity, NUL, span length, UNIQUE review hash)
so a real driver error cannot hide behind a permissive fake. The tests pin the
contract the daily-review grader has to meet (publication-review-verdicts-v1):
the file is bound to the producer's packet by hash, every claim span is a
verbatim substring of the publication it names, enums are closed, causal claims
cannot be confirmed, evidence is concrete and never from after grading,
resolved verdicts have a horizon between the claim and the grading; one hostile
row never loses the file; the ingest is idempotent per review file, atomic,
writes a receipt in every state without following symlinks, never overwrites
the INGESTED receipt, and the migration carries every column the script writes.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import ClassVar

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / rel)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def job():
    return _load("ingest_publication_review_under_test", "scripts/ingest_publication_review.py")


DAY = date(2026, 11, 6)
T0 = datetime(2026, 11, 6, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 7, 13, 6, tzinfo=timezone.utc)
GRADED = NOW - timedelta(hours=1)
BRIEF_TEXT = "SPY should close above 680 today as dealers are short gamma. VIX 17.2 at the open."
THESIS_TEXT = "Overall direction bullish into the close; conviction 0.6."
BRIEF_SPAN = "SPY should close above 680 today"
THESIS_SPAN = "Overall direction bullish into the close"


def sha(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def iso(dt: datetime) -> str:
    return dt.isoformat().replace("+00:00", "Z")


def _packet(tmp_path: Path, day: date = DAY, publications=None) -> tuple[Path, str]:
    folder = tmp_path / "packets" / day.isoformat()
    folder.mkdir(parents=True)
    pubs = publications if publications is not None else [
        {"id": "market_briefings:5001", "source": "market_briefings", "published_at": iso(T0 + timedelta(hours=14)),
         "text": BRIEF_TEXT, "sha256": sha(BRIEF_TEXT), "claims_complete": False, "source_record": {}},
        {"id": "thesis_snapshots:901", "source": "thesis_snapshots", "published_at": iso(T0 + timedelta(hours=15)),
         "text": THESIS_TEXT, "sha256": sha(THESIS_TEXT), "claims_complete": False,
         "source_record": {"overall_direction": "bullish"}},
    ]
    packet = {"publication_day": day.isoformat(), "version": "publication-review-v1", "publications": pubs}
    raw = json.dumps(packet, sort_keys=True).encode("utf-8")
    (folder / "packet.json").write_bytes(raw)
    packet_sha = hashlib.sha256(raw).hexdigest()
    (folder / "receipt.json").write_text(json.dumps({"schema": "daily-publication-packet-receipt-v1",
                                                     "publication_day": day.isoformat(), "state": "PRODUCED",
                                                     "packet_sha256": packet_sha}))
    return tmp_path / "packets", packet_sha


def header(packet_sha: str, **over) -> dict:
    h = {"kind": "review_header", "schema": "publication-review-verdicts-v1", "publication_day": DAY.isoformat(),
         "packet_sha256": packet_sha, "grader": {"model": "test/grader-1", "role": "grader"},
         "adjudicator": None, "graded_at": iso(GRADED),
         "claims_total": None, "claims_graded": None, "claims_unscorable": None}
    h.update(over)
    return h


def evidence(**over) -> dict:
    e = {"source": "YF:SPY:close", "observed": "678.9", "as_of": iso(T0 + timedelta(hours=21)), "url": None}
    e.update(over)
    return e


def brief_claim(span: str = BRIEF_SPAN, **over) -> dict:
    row = {"kind": "claim_verdict", "publication_id": "market_briefings:5001", "publication_sha256": sha(BRIEF_TEXT),
           "stated_at": iso(T0 + timedelta(hours=14)), "claim_span": span, "claim_sha256": sha(span),
           "claim_type": "level", "instrument": "SPY", "horizon_end": iso(T0 + timedelta(hours=21)),
           "verdict": "refuted", "evidence": [evidence()],
           "knowable_at_stated_at": True, "root_cause": "Gamma read was stale by two hours.",
           "lesson": "Quote the dealer-gamma timestamp with the level.", "grader_confidence": 0.8}
    row.update(over)
    return row


def thesis_claim(**over) -> dict:
    row = {"kind": "claim_verdict", "publication_id": "thesis_snapshots:901", "publication_sha256": sha(THESIS_TEXT),
           "stated_at": iso(T0 + timedelta(hours=15)), "claim_span": THESIS_SPAN, "claim_sha256": sha(THESIS_SPAN),
           "claim_type": "direction", "instrument": "SPY", "horizon_end": iso(T0 + timedelta(hours=21)),
           "verdict": "confirmed", "evidence": [evidence(observed="+0.4%")],
           "knowable_at_stated_at": None, "thesis_direction": "bullish", "actual_direction": "bullish",
           "root_cause": None, "lesson": "Direction held; conviction unit was a probability.",
           "grader_confidence": 0.7}
    row.update(over)
    return row


def unscorable_claim() -> dict:
    span = "VIX 17.2 at the open"
    return brief_claim(span=span, claim_type="data_quality", verdict="unscorable", evidence=[],
                       root_cause="No independent VIX open print in the packet.", lesson=None, horizon_end=None)


def _line(obj) -> str:
    """One JSONL line as a grader would write it: raw UTF-8, except a lone surrogate must be escaped."""
    try:
        return json.dumps(obj, ensure_ascii=False).encode("utf-8").decode("utf-8")
    except UnicodeEncodeError:
        return json.dumps(obj, ensure_ascii=True)


def _review(tmp_path: Path, lines: list, day: date = DAY, raw: bytes | None = None) -> Path:
    folder = tmp_path / "reviews" / day.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    if raw is None:
        raw = ("\n".join(_line(x) for x in lines) + "\n").encode("utf-8")
    (folder / "review.jsonl").write_bytes(raw)
    return tmp_path / "reviews"


# ── fake database that enforces what the DDL enforces ───────────────────────

class _Result:
    def __init__(self, scalar=None):
        self._scalar = scalar

    def scalar(self):
        return self._scalar


def _binds(sql: str) -> set[str]:
    return set(re.findall(r"(?<![:\w]):(\w+)", sql))


class FakeConn:
    INT_COLS: ClassVar[set[str]] = {"declared_total", "declared_graded", "declared_unscorable", "accepted_rows", "rejected_rows"}

    def __init__(self, state: dict, fail_on: tuple[str, int] | None = None):
        self.state, self.statements, self.fail_on, self._verdict_calls = state, [], fail_on, 0

    def _check_binds(self, sql: str, params: dict):
        missing = _binds(sql) - set(params)
        assert not missing, f"bind(s) {missing} missing from params"

    def _check_jsonb(self, value):
        assert isinstance(value, str), "jsonb param must be text"
        assert "\\u0000" not in value and "\x00" not in value, "NUL in jsonb"
        assert "NaN" not in value and "Infinity" not in value, "NaN/Infinity in jsonb"
        json.loads(value)

    def _check_text(self, value):
        if isinstance(value, str):
            assert "\x00" not in value, "NUL in text column"
            value.encode("utf-8")

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        self.statements.append((sql, params))
        if sql.startswith("SELECT id FROM publication_review_runs WHERE review_sha256"):
            return _Result(self.state["runs"].get(params["sha"]))
        if sql.startswith("SELECT id FROM publication_review_runs WHERE publication_day"):
            rows = [r for r in self.state["run_rows"] if r["publication_day"] == params["publication_day"]]
            return _Result(rows[-1]["_id"] if rows else None)
        if sql.startswith("INSERT INTO publication_review_runs"):
            self._check_binds(sql, params)
            for col in self.INT_COLS:
                v = params[col]
                assert v is None or (isinstance(v, int) and 0 <= v <= 2**31 - 1), f"{col} out of int4 range"
            self._check_jsonb(params["grader"])
            if params["adjudicator"] is not None:
                self._check_jsonb(params["adjudicator"])
            if params["review_sha256"] in self.state["runs"]:
                return _Result(None)  # ON CONFLICT DO NOTHING
            run_id = 100 + len(self.state["runs"])
            self.state["runs"][params["review_sha256"]] = run_id
            self.state["run_rows"].append(dict(params, _id=run_id))
            return _Result(run_id)
        if sql.startswith("INSERT INTO publication_claim_verdicts"):
            self._check_binds(sql, params)
            self._verdict_calls += 1
            if self.fail_on and self.fail_on[0] == "verdict" and self._verdict_calls == self.fail_on[1]:
                raise RuntimeError("simulated driver failure")
            assert 1 <= len(params["claim_span"]) <= 300
            self._check_jsonb(params["evidence"])
            for col in ("claim_span", "root_cause", "lesson", "instrument", "publication_id"):
                self._check_text(params[col])
            key = (params["run_id"], params["publication_id"], params["claim_sha256"])
            assert key not in self.state["verdict_keys"], "UNIQUE (run_id, publication_id, claim_sha256)"
            self.state["verdict_keys"].add(key)
            self.state["verdicts"].append(params)
            return _Result()
        raise AssertionError(f"unexpected SQL: {sql}")


class _Txn:
    """engine.begin() context: snapshot state, restore it if the block raises (rollback)."""

    def __init__(self, engine):
        self.engine = engine

    def __enter__(self):
        self.snapshot = json.loads(json.dumps(self.engine.state, default=str))
        self.snapshot_keys = set(self.engine.state["verdict_keys"])
        return self.engine.conn

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None:
            self.engine.state.clear()
            self.engine.state.update(self.snapshot)
            self.engine.state["verdict_keys"] = self.snapshot_keys
            self.engine.state["run_rows"] = [dict(r, publication_day=date.fromisoformat(r["publication_day"]))
                                             if isinstance(r["publication_day"], str) else r
                                             for r in self.engine.state["run_rows"]]
            self.engine.conn.state = self.engine.state
            self.engine.rolled_back += 1
        return False


class FakeEngine:
    def __init__(self, broken: bool = False, fail_on: tuple[str, int] | None = None):
        self.state = {"runs": {}, "run_rows": [], "verdicts": [], "verdict_keys": set()}
        self.conn, self.broken, self.rolled_back = FakeConn(self.state, fail_on), broken, 0

    def begin(self):
        if self.broken:
            raise ConnectionError("no database")
        return _Txn(self)


def _run(job, tmp_path, engine, lines, **kw):
    packet_root, packet_sha = _packet(tmp_path)
    lines = list(lines)
    if not lines or lines[0].get("kind") != "review_header":
        lines = [header(packet_sha)] + lines
    review_root = _review(tmp_path, lines)
    kw.setdefault("now", NOW)
    return job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, **kw), packet_sha


def _receipt_files(tmp_path):
    return sorted(p.name for p in (tmp_path / "reviews" / DAY.isoformat()).glob("ingest-*"))


# ── happy path ──────────────────────────────────────────────────────────────

def test_ingests_valid_file_writes_rows_and_receipt(job, tmp_path):
    engine = FakeEngine()
    receipt, packet_sha = _run(job, tmp_path, engine, [brief_claim(), thesis_claim(), unscorable_claim()])
    assert receipt["state"] == "INGESTED" and receipt["accepted_rows"] == 3 and receipt["rejected_rows"] == 0
    assert receipt["packet_sha256"] == packet_sha and receipt["supersedes_run_id"] is None
    assert receipt["verdicts"] == {"confirmed": 1, "refuted": 1, "unscorable": 1}
    assert receipt["channels"] == {"market_briefings": 2, "thesis_snapshots": 1}
    assert receipt["declared_mismatch"] == [] and receipt["packet_records_unusable"] == []
    assert len(engine.state["verdicts"]) == 3 and all(v["run_id"] == 100 for v in engine.state["verdicts"])
    run = engine.state["run_rows"][0]
    assert run["review_sha256"] == receipt["review_sha256"] and run["accepted_rows"] == 3
    assert json.loads(run["grader"])["model"] == "test/grader-1" and run["supersedes_run_id"] is None
    assert _receipt_files(tmp_path) == ["ingest-receipt.json"]
    written = json.loads((tmp_path / "reviews" / DAY.isoformat() / "ingest-receipt.json").read_text())
    assert written["state"] == "INGESTED" and written["review_sha256"] == receipt["review_sha256"]
    assert written["receipt_sha256"] == receipt["receipt_sha256"]
    assert job.exit_code(receipt) == 0


def test_verdict_row_params_match_the_packet(job, tmp_path):
    engine = FakeEngine()
    _run(job, tmp_path, engine, [brief_claim()])
    v = engine.state["verdicts"][0]
    assert v["channel"] == "market_briefings" and v["source_row_id"] == 5001
    assert v["publication_sha256"] == sha(BRIEF_TEXT) and v["claim_sha256"] == sha(BRIEF_SPAN)
    assert v["stated_at"] == T0 + timedelta(hours=14) and v["horizon_end"] == T0 + timedelta(hours=21)
    assert json.loads(v["evidence"])[0]["source"] == "YF:SPY:close" and v["grader_confidence"] == 0.8
    assert v["thesis_direction"] is None and v["knowable_at_stated_at"] is True


def test_no_thesis_table_is_ever_touched(job, tmp_path):
    engine = FakeEngine()
    _run(job, tmp_path, engine, [thesis_claim(), thesis_claim(verdict="refuted", actual_direction="bearish",
                                                              claim_span=THESIS_SPAN[:-1],
                                                              claim_sha256=sha(THESIS_SPAN[:-1]))])
    assert not any("thesis_postmortems" in s or "UPDATE" in s or "DELETE" in s for s, _ in engine.conn.statements)
    assert "thesis_postmortems" not in str(job.INSERT_VERDICT) and not hasattr(job, "INSERT_POSTMORTEM")


def test_rerun_of_same_file_is_a_noop_and_keeps_the_original_receipt(job, tmp_path):
    engine = FakeEngine()
    first, _ = _run(job, tmp_path, engine, [brief_claim(), thesis_claim()])
    original = (tmp_path / "reviews" / DAY.isoformat() / "ingest-receipt.json").read_bytes()
    second = job.ingest(engine, DAY, review_root=tmp_path / "reviews", packet_root=tmp_path / "packets",
                        now=NOW + timedelta(hours=1))
    assert second["state"] == "PRESENT" and second["run_id"] == first["run_id"] and job.exit_code(second) == 0
    assert len(engine.state["verdicts"]) == 2 and len(engine.state["run_rows"]) == 1
    assert (tmp_path / "reviews" / DAY.isoformat() / "ingest-receipt.json").read_bytes() == original
    assert _receipt_files(tmp_path) == ["ingest-receipt.json"] and second["receipt_file"] is None


def test_corrected_file_supersedes_the_previous_run(job, tmp_path):
    engine = FakeEngine()
    first, packet_sha = _run(job, tmp_path, engine, [brief_claim()])
    _review(tmp_path, [header(packet_sha), brief_claim(lesson="Corrected lesson.")])
    second = job.ingest(engine, DAY, review_root=tmp_path / "reviews", packet_root=tmp_path / "packets", now=NOW)
    assert second["state"] == "INGESTED" and second["run_id"] != first["run_id"]
    assert second["supersedes_run_id"] == first["run_id"]
    assert engine.state["run_rows"][1]["supersedes_run_id"] == first["run_id"]
    assert len(engine.state["verdicts"]) == 2
    names = _receipt_files(tmp_path)
    assert "ingest-receipt.json" in names and any(n.startswith("ingest-INGESTED-") for n in names)


def test_concurrent_ingest_of_same_bytes_reports_present(job, tmp_path):
    engine = FakeEngine()
    packet_root, packet_sha = _packet(tmp_path)
    review_root = _review(tmp_path, [header(packet_sha), brief_claim()])
    raw = (review_root / DAY.isoformat() / "review.jsonl").read_bytes()
    # Another worker committed the run between our existence check and our insert.
    original_execute = engine.conn.execute

    def racing_execute(stmt, params=None):
        sql = " ".join(str(stmt).split())
        if sql.startswith("INSERT INTO publication_review_runs"):
            engine.state["runs"][params["review_sha256"]] = 555
        return original_execute(stmt, params)

    engine.conn.execute = racing_execute
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "PRESENT" and receipt["run_id"] == 555 and engine.state["verdicts"] == []
    assert job.sha256(raw) == receipt["review_sha256"]


def test_dry_run_validates_without_writing(job, tmp_path):
    engine = FakeEngine()
    receipt, _ = _run(job, tmp_path, engine, [brief_claim()], dry_run=True)
    assert receipt["state"] == "DRY_RUN" and receipt["accepted_rows"] == 1
    assert engine.conn.statements == [] and _receipt_files(tmp_path) == []


def test_thesis_direction_cross_checks_against_the_snapshot(job, tmp_path):
    engine = FakeEngine()
    receipt, _ = _run(job, tmp_path, engine, [thesis_claim(), thesis_claim(thesis_direction="bearish",
                                                                           actual_direction="bearish")])
    assert receipt["accepted_rows"] == 1 and "overall_direction" in receipt["rejected"][0]["reason"]


@pytest.mark.parametrize("over, needle", [
    ({"verdict": "confirmed", "thesis_direction": "bullish", "actual_direction": "bearish"}, "confirmed direction"),
    ({"verdict": "refuted", "thesis_direction": "bullish", "actual_direction": "bullish"}, "refuted direction"),
])
def test_direction_claims_must_agree_with_their_verdict(job, tmp_path, over, needle):
    engine = FakeEngine()
    receipt, _ = _run(job, tmp_path, engine, [brief_claim(), thesis_claim(**over)])
    assert receipt["accepted_rows"] == 1 and needle in receipt["rejected"][0]["reason"]


# ── whole-file refusals (nothing written) ───────────────────────────────────

def test_header_packet_hash_mismatch_refuses_whole_file(job, tmp_path):
    engine = FakeEngine()
    packet_root, _ = _packet(tmp_path)
    review_root = _review(tmp_path, [header("0" * 64), brief_claim()])
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "REFUSED" and "packet_sha256" in receipt["reason"]
    assert engine.conn.statements == [] and job.exit_code(receipt) == 1
    assert _receipt_files(tmp_path) and _receipt_files(tmp_path)[0].startswith("ingest-REFUSED-")


def test_tampered_packet_refuses(job, tmp_path):
    engine = FakeEngine()
    packet_root, packet_sha = _packet(tmp_path)
    folder = packet_root / DAY.isoformat()
    (folder / "packet.json").write_bytes((folder / "packet.json").read_bytes() + b" ")
    review_root = _review(tmp_path, [header(packet_sha), brief_claim()])
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "REFUSED" and "packet.json sha256" in receipt["reason"]
    assert engine.conn.statements == []


@pytest.mark.parametrize("over, needle", [
    ({"schema": "publication-review-verdicts-v2"}, "schema"),
    ({"publication_day": "2026-11-05"}, "publication_day"),
    ({"grader": {"role": "grader"}}, "grader.model"),
    ({"grader": "test/grader-1"}, "grader"),
    ({"grader": {"model": "m", "note": "a\x00b"}}, "NUL"),
    ({"grader": {"model": "m", "deep": [[[[[1]]]]]}}, "nested"),
    ({"graded_at": "2026-11-07 12:00"}, "graded_at"),
    ({"graded_at": iso(NOW + timedelta(hours=2))}, "future"),
    ({"graded_at": iso(T0 - timedelta(hours=1))}, "before"),
    ({"kind": "claim_verdict"}, "review_header"),
])
def test_bad_header_refuses(job, tmp_path, over, needle):
    engine = FakeEngine()
    packet_root, packet_sha = _packet(tmp_path)
    review_root = _review(tmp_path, [header(packet_sha, **over), brief_claim()])
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "REFUSED" and needle in receipt["reason"] and engine.conn.statements == []


def test_header_nan_is_refused_not_failed(job, tmp_path):
    engine = FakeEngine()
    packet_root, packet_sha = _packet(tmp_path)
    raw = ('{"kind":"review_header","schema":"publication-review-verdicts-v1","publication_day":"2026-11-06",'
           f'"packet_sha256":"{packet_sha}","grader":{{"model":"m","temp":NaN}},"graded_at":"{iso(GRADED)}"}}\n'
           + json.dumps(brief_claim()) + "\n").encode("utf-8")
    review_root = _review(tmp_path, [], raw=raw)
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "REFUSED" and "NaN" in receipt["reason"]


def test_missing_packet_or_review_refuses(job, tmp_path):
    engine = FakeEngine()
    receipt = job.ingest(engine, DAY, review_root=tmp_path / "reviews", packet_root=tmp_path / "packets", now=NOW)
    assert receipt["state"] == "REFUSED" and "not present" in receipt["reason"]
    packet_root, _ = _packet(tmp_path)
    receipt = job.ingest(engine, DAY, review_root=tmp_path / "reviews", packet_root=packet_root, now=NOW)
    assert receipt["state"] == "REFUSED" and "not present" in receipt["reason"]
    assert engine.conn.statements == []


def test_all_rows_rejected_refuses(job, tmp_path):
    engine = FakeEngine()
    span = "not in the text at all"
    receipt, _ = _run(job, tmp_path, engine, [brief_claim(claim_span=span, claim_sha256=sha(span))])
    assert receipt["state"] == "REFUSED" and "no row accepted" in receipt["reason"] and engine.conn.statements == []


@pytest.mark.parametrize("raw, needle", [
    (b"\xff\xfe not utf8\n", "UTF-8"),
    (b'{"kind": "review_header"\n', "JSON"),
    (b"\n\n", "empty"),
])
def test_unreadable_bytes_are_refused_not_failed(job, tmp_path, raw, needle):
    engine = FakeEngine()
    packet_root, _ = _packet(tmp_path)
    review_root = _review(tmp_path, [], raw=raw)
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "REFUSED" and needle in receipt["reason"]


def test_bom_crlf_and_unicode_line_separators_inside_strings_still_parse(job, tmp_path):
    engine = FakeEngine()
    packet_root, packet_sha = _packet(tmp_path)
    row = brief_claim(lesson="line one line two\u0085three\x0cfour")
    raw = b"\xef\xbb\xbf" + (json.dumps(header(packet_sha), ensure_ascii=False) + "\r\n"
                             + json.dumps(row, ensure_ascii=False) + "\r\n").encode("utf-8")
    review_root = _review(tmp_path, [], raw=raw)
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "INGESTED" and receipt["accepted_rows"] == 1
    assert engine.state["verdicts"][0]["lesson"] == row["lesson"]


def test_oversized_symlinked_and_fifo_review_files_are_refused_before_reading(job, tmp_path, monkeypatch):
    engine = FakeEngine()
    packet_root, _ = _packet(tmp_path)
    folder = tmp_path / "reviews" / DAY.isoformat()
    folder.mkdir(parents=True)
    big = tmp_path / "big.jsonl"
    big.write_bytes(b"x" * 1000)
    monkeypatch.setattr(job, "MAX_FILE_BYTES", 800)  # packet.json is ~735 bytes; big is 1000
    (folder / "review.jsonl").symlink_to(big)
    receipt = job.ingest(engine, DAY, review_root=tmp_path / "reviews", packet_root=packet_root, now=NOW)
    assert receipt["state"] == "REFUSED" and "regular file" in receipt["reason"]
    (folder / "review.jsonl").unlink()
    (folder / "review.jsonl").write_bytes(b"x" * 1000)
    receipt = job.ingest(engine, DAY, review_root=tmp_path / "reviews", packet_root=packet_root, now=NOW)
    assert receipt["state"] == "REFUSED" and "bytes" in receipt["reason"]
    (folder / "review.jsonl").unlink()
    os.mkfifo(folder / "review.jsonl")
    receipt = job.ingest(engine, DAY, review_root=tmp_path / "reviews", packet_root=packet_root, now=NOW)
    assert receipt["state"] == "REFUSED" and "regular file" in receipt["reason"]
    assert engine.conn.statements == []


def test_db_failure_writes_failed_receipt_with_type_only(job, tmp_path, capsys):
    receipt, _ = _run(job, tmp_path, FakeEngine(broken=True), [brief_claim()])
    assert receipt["state"] == "FAILED" and receipt["reason"] == "ConnectionError" and job.exit_code(receipt) == 1
    assert "no database" not in json.dumps(receipt)
    assert _receipt_files(tmp_path)[0].startswith("ingest-FAILED-")
    assert "ConnectionError" in capsys.readouterr().err


def test_mid_transaction_failure_rolls_back_everything(job, tmp_path):
    engine = FakeEngine(fail_on=("verdict", 2))
    receipt, _ = _run(job, tmp_path, engine, [brief_claim(), thesis_claim()])
    assert receipt["state"] == "FAILED" and receipt["reason"] == "RuntimeError"
    assert engine.rolled_back == 1 and engine.state["verdicts"] == [] and engine.state["run_rows"] == []
    assert engine.state["runs"] == {}


# ── row-level rejections (file still ingests; exit 3) ───────────────────────

def _bad(**over):
    return brief_claim(**over)


HOSTILE_ROWS = [
    (_bad(claim_span="SPY should close above 690 today", claim_sha256=sha("SPY should close above 690 today")), "verbatim"),
    (_bad(claim_sha256="0" * 64), "claim_sha256"),
    (_bad(publication_sha256="0" * 64), "publication_sha256"),
    (_bad(publication_sha256=None), "publication_sha256"),
    (_bad(publication_id="market_briefings:5999"), "not in packet"),
    (_bad(publication_id="briefings-5001"), "malformed"),
    (_bad(publication_id="market_briefings:5001\n"), "malformed"),
    (_bad(stated_at=iso(T0 + timedelta(hours=13))), "stated_at"),
    (_bad(claim_type="vibe"), "claim_type"),
    (_bad(claim_type=["level"]), "claim_type"),
    (_bad(verdict="maybe"), "verdict"),
    (_bad(verdict={}), "verdict"),
    (_bad(claim_type="causal", verdict="confirmed"), "causal"),
    (_bad(claim_type="causal", verdict="partially_confirmed"), "causal"),
    (_bad(verdict="unscorable", evidence=[{"source": "x", "observed": "y"}]), "unscorable"),
    (_bad(verdict="unscorable", evidence=[], root_cause=None), "unscorable"),
    (_bad(verdict="confirmed", evidence=[]), "evidence"),
    (_bad(evidence="YF close 678.9"), "evidence"),
    (_bad(evidence=[{}]), "evidence.source"),
    (_bad(evidence=[evidence(observed=None)]), "evidence.observed"),
    (_bad(evidence=[evidence(observed={"v": 1})]), "evidence.observed"),
    (_bad(evidence=[evidence(as_of=None)]), "evidence.as_of"),
    (_bad(evidence=[evidence(as_of=iso(GRADED + timedelta(minutes=1)))]), "future"),
    (_bad(evidence=[evidence()] * 21), "more than 20"),
    (_bad(horizon_end="yesterday"), "horizon_end"),
    (_bad(horizon_end="0001-01-01T00:00:00+05:00"), "horizon_end"),
    (_bad(horizon_end=None), "requires horizon_end"),
    (_bad(horizon_end=iso(T0 + timedelta(hours=13))), "after stated_at"),
    (_bad(horizon_end=iso(GRADED + timedelta(minutes=1))), "no later than graded_at"),
    (_bad(verdict="unresolved", horizon_end=iso(T0 + timedelta(hours=14))), "after stated_at"),
    (_bad(grader_confidence=1.7), "grader_confidence"),
    (_bad(grader_confidence=True), "grader_confidence"),
    (_bad(grader_confidence=float("nan")), "grader_confidence"),
    (_bad(thesis_direction="up"), "thesis_direction"),
    (_bad(thesis_direction=["bullish"]), "thesis_direction"),
    (_bad(knowable_at_stated_at="yes"), "knowable_at_stated_at"),
    (_bad(claim_span="x" * 301, claim_sha256=sha("x" * 301)), "300"),
    (_bad(claim_span="SPY sho", claim_sha256=sha("SPY sho")), "shorter than 8"),
    (_bad(lesson="a\x00b"), "NUL"),
    (_bad(root_cause="\ud800"), "encodable"),
    (_bad(root_cause="r" * 2001), "longer than 2000"),
    (_bad(instrument="S" * 33), "instrument"),
    (_bad(lesson=["list"]), "lesson must be a string"),
    ({"kind": "note", "text": "free prose"}, "claim_verdict"),
    ("just a string", "claim_verdict"),
]


@pytest.mark.parametrize("row, needle", HOSTILE_ROWS)
def test_row_rejections_are_recorded_and_the_rest_ingests(job, tmp_path, row, needle):
    engine = FakeEngine()
    receipt, _ = _run(job, tmp_path, engine, [thesis_claim(), row])
    assert receipt["state"] == "INGESTED", receipt.get("reason")
    assert receipt["accepted_rows"] == 1 and receipt["rejected_rows"] == 1
    assert needle in receipt["rejected"][0]["reason"] and receipt["rejected"][0]["line"] == 3
    assert engine.state["run_rows"][0]["rejected_rows"] == 1 and len(engine.state["verdicts"]) == 1
    assert job.exit_code(receipt) == 3


def test_nan_in_evidence_is_rejected_at_the_row(job, tmp_path):
    engine = FakeEngine()
    packet_root, packet_sha = _packet(tmp_path)
    bad = json.dumps(brief_claim(evidence=[evidence(observed=float("nan"))]))  # json.dumps emits NaN
    assert "NaN" in bad
    raw = (json.dumps(header(packet_sha)) + "\n" + json.dumps(thesis_claim()) + "\n" + bad + "\n").encode("utf-8")
    review_root = _review(tmp_path, [], raw=raw)
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "INGESTED" and receipt["rejected_rows"] == 1
    assert "NaN" in receipt["rejected"][0]["reason"]


@pytest.mark.parametrize("row", [
    brief_claim(claim_type="causal", verdict="refuted"),
    brief_claim(claim_type="causal", verdict="unresolved", evidence=[]),
    brief_claim(claim_type="causal", verdict="unscorable", evidence=[], root_cause="no mechanism data"),
    brief_claim(verdict="unresolved", horizon_end=None, evidence=[]),
    brief_claim(grader_confidence=0),
    brief_claim(grader_confidence=1),
    brief_claim(instrument=None, lesson=None, knowable_at_stated_at=None),
    brief_claim(evidence=[evidence(observed=678.9, url="https://example.test/spy")]),
])
def test_legal_variants_are_accepted(job, tmp_path, row):
    engine = FakeEngine()
    receipt, _ = _run(job, tmp_path, engine, [row])
    assert receipt["state"] == "INGESTED" and receipt["accepted_rows"] == 1 and receipt["rejected_rows"] == 0


def test_duplicate_claim_in_file_rejected_once(job, tmp_path):
    engine = FakeEngine()
    receipt, _ = _run(job, tmp_path, engine, [brief_claim(), brief_claim()])
    assert receipt["accepted_rows"] == 1 and receipt["rejected"][0]["reason"].startswith("duplicate")


def test_huge_publication_id_is_truncated_in_the_receipt(job, tmp_path):
    engine = FakeEngine()
    receipt, _ = _run(job, tmp_path, engine, [thesis_claim(), brief_claim(publication_id="m" * 5000)])
    assert len(receipt["rejected"][0]["publication_id"]) <= 80


def test_declared_counts_are_reconciled(job, tmp_path):
    engine = FakeEngine()
    packet_root, packet_sha = _packet(tmp_path)
    review_root = _review(tmp_path, [header(packet_sha, claims_total=5, claims_graded=4, claims_unscorable=0),
                                     brief_claim(), thesis_claim(), unscorable_claim()])
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "INGESTED" and receipt["rejected_rows"] == 0
    assert len(receipt["declared_mismatch"]) == 3 and job.exit_code(receipt) == 3
    assert engine.state["run_rows"][0]["declared_total"] == 5


def test_out_of_range_header_counts_are_stored_null_with_a_note(job, tmp_path):
    engine = FakeEngine()
    packet_root, packet_sha = _packet(tmp_path)
    review_root = _review(tmp_path, [header(packet_sha, claims_total=2**40, claims_graded=-1, claims_unscorable=1),
                                     brief_claim(), unscorable_claim()])
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "INGESTED"
    assert engine.state["run_rows"][0]["declared_total"] is None and engine.state["run_rows"][0]["declared_graded"] is None
    assert len(receipt["notes"]) == 2 and receipt["declared_mismatch"] == []


def test_inconsistent_packet_record_is_unusable_not_fatal(job, tmp_path):
    engine = FakeEngine()
    pubs = [{"id": "market_briefings:5001", "source": "market_briefings", "published_at": iso(T0 + timedelta(hours=14)),
             "text": BRIEF_TEXT, "sha256": "0" * 64, "source_record": {}},
            {"id": "thesis_snapshots:901", "source": "thesis_snapshots", "published_at": iso(T0 + timedelta(hours=15)),
             "text": THESIS_TEXT, "sha256": sha(THESIS_TEXT), "source_record": {}}]
    packet_root, packet_sha = _packet(tmp_path, publications=pubs)
    review_root = _review(tmp_path, [header(packet_sha), brief_claim(), thesis_claim()])
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "INGESTED" and receipt["accepted_rows"] == 1
    assert receipt["packet_records_unusable"] == [{"id": "market_briefings:5001",
                                                   "reason": "text/sha256 inconsistent in packet"}]
    assert "not in packet" in receipt["rejected"][0]["reason"]


# ── receipt handling ────────────────────────────────────────────────────────

def test_receipt_is_never_written_through_a_symlink(job, tmp_path):
    engine = FakeEngine()
    packet_root, packet_sha = _packet(tmp_path)
    review_root = _review(tmp_path, [header(packet_sha), brief_claim()])
    victim = tmp_path / "victim.txt"
    victim.write_text("keep me")
    (review_root / DAY.isoformat() / "ingest-receipt.json").symlink_to(victim)
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "INGESTED" and victim.read_text() == "keep me"
    # the existing name was taken, so the INGESTED receipt went to a stamped file
    assert any(n.startswith("ingest-INGESTED-") for n in _receipt_files(tmp_path))
    assert not list((review_root / DAY.isoformat()).glob(".*.tmp"))


def test_receipt_write_error_does_not_fail_a_committed_ingest(job, tmp_path, monkeypatch, capsys):
    engine = FakeEngine()

    def no_open(*a, **k):
        raise PermissionError("read-only")

    monkeypatch.setattr(job.os, "open", no_open)
    packet_root, packet_sha = _packet(tmp_path)
    review_root = _review(tmp_path, [header(packet_sha), brief_claim()])
    monkeypatch.setattr(job, "read_regular_file", lambda path, limit: Path(path).read_bytes())
    receipt = job.ingest(engine, DAY, review_root=review_root, packet_root=packet_root, now=NOW)
    assert receipt["state"] == "INGESTED" and receipt["receipt_write_error"] == "PermissionError"
    assert job.exit_code(receipt) == 0 and "could not write receipt" in capsys.readouterr().err


# ── scheduling helpers and CLI ──────────────────────────────────────────────

def test_pending_days_scans_the_lookback_window(job, tmp_path):
    root = tmp_path / "reviews"
    for d, done in ((DAY - timedelta(days=2), False), (DAY - timedelta(days=1), True), (DAY, False),
                    (DAY - timedelta(days=5), False)):
        (root / d.isoformat()).mkdir(parents=True)
        (root / d.isoformat() / "review.jsonl").write_text("{}")
        if done:
            (root / d.isoformat() / "ingest-receipt.json").write_text("{}")
    (root / (DAY - timedelta(days=3)).isoformat()).mkdir()  # no review.jsonl
    assert job.pending_days(root, NOW, 3) == [DAY - timedelta(days=2), DAY]
    assert job.pending_days(root, NOW, 1) == [DAY]


def test_exit_codes(job):
    assert job.exit_code({"state": "INGESTED", "rejected_rows": 0, "declared_mismatch": []}) == 0
    assert job.exit_code({"state": "INGESTED", "rejected_rows": 2, "declared_mismatch": []}) == 3
    assert job.exit_code({"state": "INGESTED", "rejected_rows": 0, "declared_mismatch": ["x"]}) == 3
    assert job.exit_code({"state": "PRESENT", "rejected_rows": 2}) == 0
    assert job.exit_code({"state": "REFUSED"}) == 1 and job.exit_code({"state": "FAILED"}) == 1
    assert job.combined_exit([0, 3, 1]) == 1 and job.combined_exit([0, 3]) == 3 and job.combined_exit([0, 0]) == 0


def test_cli_rejects_bad_day_and_lookback(job):
    assert job.main(["--day", "2026-13-40"]) == 2
    assert job.main(["--lookback", "0"]) == 2


def test_cli_with_nothing_pending_exits_zero_without_a_database(job, tmp_path, capsys):
    assert job.main(["--review-root", str(tmp_path), "--packet-root", str(tmp_path)]) == 0
    assert "nothing pending" in capsys.readouterr().out


# ── contract with the migration, the packet producer and the units ─────────

def test_migration_ddl_carries_every_column_the_script_writes(job):
    ddl = (ROOT / "migrations" / "versions" / "pub_review_ingest_20261108.py").read_text()
    for stmt, table in ((job.INSERT_RUN, "publication_review_runs"), (job.INSERT_VERDICT, "publication_claim_verdicts")):
        sql = str(stmt)
        cols = re.search(rf"INSERT INTO {table}\s*\(([^)]*)\)", sql, re.DOTALL).group(1)
        columns = [c.strip() for c in cols.split(",")]
        body = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\n\s*\)\s*\"\"\"", ddl, re.DOTALL).group(1)
        declared = {line.split()[0] for line in body.splitlines()
                    if line.strip() and not line.strip().startswith(("CHECK", "UNIQUE"))}
        missing = [c for c in columns if c not in declared]
        assert not missing, f"{table}: script writes {missing} which the migration does not declare"
        assert set(_binds(sql)) >= set(columns) - {"run_id"} or table == "publication_claim_verdicts"
    assert "ON CONFLICT (review_sha256) DO NOTHING" in str(job.INSERT_RUN)
    assert "BEFORE UPDATE OR DELETE ON publication_claim_verdicts" in ddl
    assert "BEFORE UPDATE OR DELETE ON publication_review_runs" in ddl
    assert "thesis_postmortems" not in ddl.split('"""', 2)[2]  # only mentioned in the docstring
    assert 'down_revision = "sleuth_intersections_20261107"' in ddl


def test_every_bind_name_has_a_param(job, tmp_path):
    engine = FakeEngine()
    receipt, _ = _run(job, tmp_path, engine, [brief_claim(), thesis_claim()])
    assert receipt["state"] == "INGESTED"
    for sql, params in engine.conn.statements:
        if sql.startswith("INSERT"):
            assert _binds(sql) <= set(params), sql[:60]


def test_canonical_hashing_matches_the_packet_producer(job):
    producer = _load("daily_publication_packet_for_contract", "scripts/daily_publication_packet.py")
    sample = {"b": [1, "é"], "a": {"z": None}}
    assert job.safe_json(sample, 1000, what="x").encode("utf-8") == producer.canonical(sample)


def test_timer_template_is_outside_backup_window_and_after_producer():
    timer = (ROOT / "deploy" / "systemd" / "grid-publication-review-ingest.timer.template").read_text()
    hour = int(re.search(r"OnCalendar=\*-\*-\* (\d\d):", timer).group(1))
    assert hour >= 13 or hour < 3
    service = (ROOT / "deploy" / "systemd" / "grid-publication-review-ingest.service.template").read_text()
    assert "scripts/ingest_publication_review.py" in service and "SuccessExitStatus=3" in service
    assert "WorkingDirectory=/data/grid_v4/grid_release" in service
