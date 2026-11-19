"""Offline controls for scripts/daily_publication_packet.py.

A fake engine stands in for griddb: no database, no systemd, no provider. The
tests pin the packet shape the M0 checker reads (capture.json sha == export
sha, canonical row hashes), whole-record quarantine on screen hits, the
diary_run evidence rules, idempotent re-runs, the closed-window refusal, and
that the produced observation.json is accepted by the status reporter with the
first blocked stage handed to the daily-review owner.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

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
    return _load("daily_publication_packet_under_test", "scripts/daily_publication_packet.py")


@pytest.fixture(scope="module")
def status():
    return _load("daily_publication_packet_status_under_test", "scripts/daily_publication_packet_status.py")


DAY = date(2026, 11, 6)
T0 = datetime(2026, 11, 6, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 7, 6, 20, tzinfo=timezone.utc)


def _briefing(i: int, hour: int, content: str = "Hourly briefing text with numbers 1.5% and SPY 680.") -> dict:
    return {"id": 5000 + i, "briefing_type": "hourly", "briefing_date": DAY, "content": content,
            "sentiment_score": 0.12, "sentiment_label": "neutral", "created_at": T0 + timedelta(hours=hour),
            "snapshot_bytes": 1200, "snapshot_data": {"equities": {"^GSPC": {"value": 6801.1}}}}


def _thesis(i: int, hour: int) -> dict:
    return {"id": 900 + i, "timestamp": T0 + timedelta(hours=hour), "overall_direction": "neutral",
            "conviction": 0.4, "conviction_unit": "probability", "narrative": "Thesis narrative."}


def _diary(row_id: int, session: date, generated: datetime, content: str = "Diary for the session.") -> dict:
    return {"id": row_id, "date": session, "content": content, "market_moves": {"prices_enabled": True},
            "thesis_accuracy": {"verdict": "partial"}, "narrative_model": None, "narrative_fallback": True,
            "generated_at": generated}


class _Result:
    def __init__(self, rows=None, scalar=None):
        self._rows, self._scalar = rows or [], scalar

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)

    def scalar_one(self):
        return self._scalar


class FakeConn:
    """Dispatches on SQL text; records every statement for assertions."""

    def __init__(self, data: dict, fail: set[str] | None = None):
        self.data, self.fail, self.statements, self.rolled_back = data, fail or set(), [], False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.statements.append((sql, params))
        if "transaction_timestamp" in sql:
            return _Result(scalar=self.data["asof"])
        if "set_config" in sql or sql.startswith("SET TRANSACTION"):
            return _Result()
        for channel in ("market_briefings", "thesis_snapshots", "market_diary"):
            if f"FROM {channel}" in sql:
                if channel in self.fail:
                    raise RuntimeError("boom")
                key = "diary_context" if "date >= :since" in sql else channel
                return _Result(rows=self.data.get(key, []))
        raise AssertionError(f"unexpected SQL: {sql}")

    def exec_driver_sql(self, sql):
        return _Result(scalar={"SHOW transaction_read_only": "on", "SHOW transaction_isolation": "repeatable read",
                               "SHOW statement_timeout": "8s"}[sql])

    def rollback(self):
        self.rolled_back = True


class FakeEngine:
    def __init__(self, data: dict, fail: set[str] | None = None, broken: bool = False):
        self.conn, self.broken = FakeConn(data, fail), broken

    def connect(self):
        if self.broken:
            raise ConnectionError("no database")
        return self.conn


def _data(diary_rows=None, briefings=None):
    return {
        "asof": NOW - timedelta(minutes=5),
        "market_briefings": briefings if briefings is not None else [_briefing(1, 1), _briefing(2, 13)],
        "thesis_snapshots": [_thesis(1, 2)],
        "market_diary": diary_rows if diary_rows is not None else [
            _diary(42, DAY - timedelta(days=1), T0 + timedelta(hours=5, minutes=31))],
        "diary_context": [_diary(43, DAY, NOW - timedelta(minutes=50))],
    }


def _unit_runner(start="Fri 2026-11-06 05:31:50 UTC"):
    class P:
        returncode = 0
        stdout = (f"ExecMainStartTimestamp={start}\nExecMainExitTimestamp=Fri 2026-11-06 05:32:12 UTC\n"
                  "ExecMainStatus=0\nResult=success\n")
    return lambda *a, **k: P()


def _run(job, tmp_path, engine, **kw):
    kw.setdefault("limit", 500)
    kw.setdefault("due_hours", 8)
    kw.setdefault("dry_run", False)
    kw.setdefault("now", NOW)
    kw.setdefault("unit_runner", _unit_runner())
    kw.setdefault("audio_dir", tmp_path / "no-audio")
    kw.setdefault("grounding_dir", tmp_path / "no-grounding")
    return job.produce(engine, DAY, tmp_path / "packets", **kw)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_produces_screened_packet_chained_receipts_and_valid_observation(job, status, tmp_path):
    engine = FakeEngine(_data())
    result = _run(job, tmp_path, engine)
    day_dir = tmp_path / "packets" / "2026-11-06"
    assert result["state"] == "PRODUCED" and result["publications_projected"] == 4
    assert engine.conn.rolled_back and engine.conn.statements[0][0].startswith("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    assert all("'" not in sql.split(":")[0] or "set_config" in sql for sql, _ in engine.conn.statements)
    # Every channel query is bound, never interpolated.
    for sql, params in engine.conn.statements:
        if "FROM " in sql:
            assert ":start" in sql or ":since" in sql and params

    capture = json.loads((day_dir / "capture.json").read_bytes())
    packet = json.loads((day_dir / "packet.json").read_bytes())
    assert packet["capture_receipt"]["export_sha256"] == _sha(day_dir / "capture.json") == result["capture_sha256"]
    assert (day_dir / "capture.json").stat().st_mode & 0o777 == 0o600
    assert {p["source"] for p in packet["publications"]} == {"market_briefings", "thesis_snapshots", "market_diary"}
    diary_pub = next(p for p in packet["publications"] if p["source"] == "market_diary")
    assert diary_pub["id"] == "market_diary:42" and diary_pub["published_at"] == "2026-11-06T05:31:00Z"
    # Row hashes follow the M0 checker's canonical form (sort_keys, compact, ensure_ascii=False).
    rec = next(r["record"] for r in capture["records"] if r["channel"] == "market_briefings")
    expected = hashlib.sha256(json.dumps(rec, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                                         default=str).encode()).hexdigest()
    assert any(e["sha256"] == expected for e in packet["evidence"])
    assert [row["id"] for row in packet["existing_diary_context"]] == [43]

    # Receipts chain diary_run -> candidate -> capture by file hash.
    receipts = {name: _sha(day_dir / "receipts" / f"{name}.json") for name in ("diary_run", "candidate", "capture")}
    assert result["receipts"] == receipts
    candidate = json.loads((day_dir / "receipts" / "candidate.json").read_bytes())
    assert candidate["upstream_receipt_sha256"] == receipts["diary_run"]
    assert len(candidate["record_manifest"]) == 4 and "content" not in json.dumps(candidate["record_manifest"])

    # The observation validates and hands the day to the daily-review owner.
    obs = json.loads((day_dir / "observation.json").read_bytes())
    evaluated = status.evaluate(obs)
    assert evaluated["state"] == "PENDING"
    assert evaluated["stage_states"]["diary_run"] == evaluated["stage_states"]["capture"] == "passed"
    assert evaluated["first_blocked_stage"] == "admission_review"
    assert evaluated["action_owner"] == "codex_daily_review" and evaluated["authorizes_execution"] is False
    assert obs["stages"]["capture"]["receipt_sha256"] == receipts["capture"]
    diary_receipt = json.loads((day_dir / "receipts" / "diary_run.json").read_bytes())
    assert diary_receipt["unit_evidence"]["available"] is True
    assert diary_receipt["rows_generated_in_window"][0]["session_date"] == "2026-11-05"


def test_screen_hit_quarantines_whole_record_without_truncation(job, tmp_path):
    bad = "Market recap. DB_PASSWORD=hunter2hunter2 was pasted here by mistake."
    engine = FakeEngine(_data(briefings=[_briefing(1, 1), _briefing(2, 3, content=bad)]))
    result = _run(job, tmp_path, engine)
    packet = json.loads((tmp_path / "packets" / "2026-11-06" / "packet.json").read_bytes())
    ids = [p["id"] for p in packet["publications"]]
    assert "market_briefings:5002" not in ids and "market_briefings:5001" in ids
    assert packet["capture_receipt"]["quarantined_publication_ids"] == ["market_briefings:5002"]
    assert "hunter2" not in json.dumps(packet)
    capture_receipt = json.loads((tmp_path / "packets" / "2026-11-06" / "receipts" / "capture.json").read_bytes())
    [exclusion] = capture_receipt["exclusions"]
    assert exclusion["disposition"] == "QUARANTINED_SCREEN_HIT" and "credential_assignment" in exclusion["reasons"]
    assert exclusion["text_sha256"] == hashlib.sha256(bad.encode()).hexdigest()
    assert result["quarantined"] == 1 and result["publications_projected"] == 3


def test_screen_covers_source_record_and_diary_context_not_only_text(job, tmp_path):
    leaky = _briefing(1, 1)
    leaky["snapshot_data"] = {"debug": "api_key=ABCDEFGHIJ1234"}
    data = _data(briefings=[leaky, _briefing(2, 2)])
    data["diary_context"] = [_diary(43, DAY, NOW - timedelta(minutes=50), content="ok"),
                             _diary(44, DAY - timedelta(days=1), NOW - timedelta(days=1), content="postgresql://u:p@h/db")]
    result = _run(job, tmp_path, FakeEngine(data))
    packet = json.loads((tmp_path / "packets" / "2026-11-06" / "packet.json").read_bytes())
    assert [p["id"] for p in packet["publications"] if p["source"] == "market_briefings"] == ["market_briefings:5002"]
    assert [r["id"] for r in packet["existing_diary_context"]] == [43]
    assert packet["capture_receipt"]["quarantined_context_ids"] == ["market_diary:44:context"]
    assert "ABCDEFGHIJ1234" not in json.dumps(packet) and "postgresql://" not in json.dumps(packet)
    assert result["quarantined"] == 2


def test_screen_row_sees_raw_strings_that_json_escaping_would_hide(job):
    assert job.screen_row({"content": "fine", "snapshot_data": {"note": "tab\x01control"}}) == ["hidden_or_control_characters"]
    assert job.screen_row({"content": "split\npassword = abcdefghij12"}) == ["credential_assignment"]
    assert job.screen_row({"content": "clean", "k": ["also clean", {"n": 1}]}) == []
    assert "hidden_or_control_characters" in job.screen_row({"zero\u200bwidth key": "v"})
    # Key/value split: the credential lives in a JSON pair, never in one string on its own.
    assert "credential_assignment" in job.screen_row({"content": "fine", "snapshot_data": {"password": "hunter2hunter2"}})
    assert "credential_assignment" in job.screen_text('{"api_key":"ABCDEFGHIJ1234"}')
    assert "credential_assignment" in job.screen_text("password=ab'c\"d1234567")  # quotes inside the secret
    assert "credential_assignment" in job.screen_text("secret: 'q1w2e3r4t5'")
    assert job.screen_row({"content": "fine", "sentiment_score": 0.12, "narrative_fallback": True}) == []


@pytest.mark.parametrize("text, hit", [
    ("postgresql://grid:pw@localhost/griddb", True),
    ("Bearer abcdefghijklmnopqrstuvwxyz0123", True),
    ("patient id 12345 follow-up", True),
    ("zero​width", True),
    ("Fed held rates; SPY +0.4%, VIX 15.2. Token supply discussion in crypto.", False),
    ("The password reset policy at banks tightened.", False),
])
def test_screen_patterns(job, text, hit):
    assert bool(job.screen_text(text)) is hit


def test_missing_text_is_omitted_not_invented(job, tmp_path):
    engine = FakeEngine(_data(briefings=[_briefing(1, 1, content="   ")]))
    result = _run(job, tmp_path, engine)
    packet = json.loads((tmp_path / "packets" / "2026-11-06" / "packet.json").read_bytes())
    assert packet["capture_receipt"]["omitted_publication_ids"] == ["market_briefings:5001"]
    assert result["omitted_missing_text"] == 1


def test_rerun_is_a_noop_and_never_rewrites(job, tmp_path):
    engine = FakeEngine(_data())
    first = _run(job, tmp_path, engine)
    day_dir = tmp_path / "packets" / "2026-11-06"
    before = {p.name: _sha(p) for p in day_dir.glob("*.json")}
    second = _run(job, tmp_path, FakeEngine(_data(briefings=[])), now=NOW + timedelta(hours=1))
    assert second["already_present"] is True and second["receipt_sha256"] == first["receipt_sha256"]
    assert {p.name: _sha(p) for p in day_dir.glob("*.json")} == before


def test_open_window_is_refused_and_nothing_written(job, tmp_path):
    with pytest.raises(job.Refused):
        _run(job, tmp_path, FakeEngine(_data()), now=datetime(2026, 11, 6, 23, 59, tzinfo=timezone.utc))
    assert not (tmp_path / "packets").exists()


def test_scheduled_diary_without_row_is_a_visible_failure(job, status, tmp_path):
    engine = FakeEngine(_data(diary_rows=[]))
    result = _run(job, tmp_path, engine, unit_runner=_unit_runner(start="Thu 2026-11-05 05:31:50 UTC"))
    assert result["diary_run"]["state"] == "failed"
    obs = json.loads((tmp_path / "packets" / "2026-11-06" / "observation.json").read_bytes())
    evaluated = status.evaluate(obs)
    assert evaluated["state"] == "BLOCKED" and evaluated["first_blocked_stage"] == "diary_run"
    assert evaluated["action_owner"] == "claude_capture_producer"
    assert evaluated["stage_states"]["capture"] == "passed"  # the packet still exists for review
    receipt = json.loads((tmp_path / "packets" / "2026-11-06" / "receipts" / "diary_run.json").read_bytes())
    assert receipt["unit_evidence"]["available"] is False  # last run was for another window


def test_non_session_day_without_diary_is_verified_not_guessed(job, status, tmp_path):
    sunday = date(2026, 11, 8)
    data = _data(diary_rows=[])
    data["market_briefings"] = [dict(_briefing(1, 1), created_at=datetime(2026, 11, 8, 1, tzinfo=timezone.utc))]
    data["thesis_snapshots"] = []
    result = job.produce(FakeEngine(data), sunday, tmp_path / "packets", limit=500, due_hours=8, dry_run=False,
                         now=datetime(2026, 11, 9, 6, 20, tzinfo=timezone.utc), unit_runner=_unit_runner(),
                         audio_dir=tmp_path / "x", grounding_dir=tmp_path / "y")
    assert result["diary_run"]["state"] == "passed" and result["diary_run"]["outcome"].startswith("non_session_verified")
    obs = json.loads((tmp_path / "packets" / "2026-11-08" / "observation.json").read_bytes())
    assert status.evaluate(obs)["first_blocked_stage"] == "admission_review"


def test_database_failure_writes_failed_receipt_and_blocked_observation(job, status, tmp_path):
    result = _run(job, tmp_path, FakeEngine(_data(), broken=True))
    day_dir = tmp_path / "packets" / "2026-11-06"
    assert result["state"] == "FAILED" and result["error_type"] == "ConnectionError"
    assert not (day_dir / "packet.json").exists() and not (day_dir / "receipt.json").exists()
    assert list(day_dir.glob("FAILED-*.json"))
    obs = json.loads((day_dir / "observation.json").read_bytes())
    evaluated = status.evaluate(obs)
    assert evaluated["state"] == "BLOCKED" and evaluated["first_blocked_stage"] == "diary_run"
    assert obs["capture_attempt"] == "not_started"
    # A later successful run is not blocked by the failed attempt's files.
    again = _run(job, tmp_path, FakeEngine(_data()), now=NOW + timedelta(minutes=30))
    assert again["state"] == "PRODUCED" and (day_dir / "receipt.json").exists()


def test_channel_query_failure_is_recorded_and_diary_unknown(job, tmp_path):
    engine = FakeEngine(_data(), fail={"market_diary"})
    result = _run(job, tmp_path, engine)
    assert result["channels"]["market_diary"]["state"] == "QUERY_FAILED"
    assert result["diary_run"]["state"] == "unknown"
    packet = json.loads((tmp_path / "packets" / "2026-11-06" / "packet.json").read_bytes())
    assert any(b.startswith("market_diary: RuntimeError") for b in packet["blockers"])
    assert "boom" not in json.dumps(packet)


def test_dry_run_writes_nothing(job, tmp_path):
    result = _run(job, tmp_path, FakeEngine(_data()), dry_run=True)
    assert result["state"] == "PRODUCED" and result["dry_run"] is True
    assert not (tmp_path / "packets" / "2026-11-06").exists()


def test_cli_default_day_is_previous_utc_day_and_refuses_open_window(job, tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(job, "DEFAULT_PUBLIC_ROOT", tmp_path / "pub")  # never the real ~/research tree from a test
    monkeypatch.setattr(job, "systemd_unit_evidence", lambda *a, **k: {"available": False, "reason": "test"})
    monkeypatch.setattr(job, "AUDIO_DIR", tmp_path / "a")
    monkeypatch.setattr(job, "GROUNDING_DIR", tmp_path / "g")
    code = job.main(["--out-root", str(tmp_path / "p"), "--json"], engine=FakeEngine(_data()), now=NOW)
    out = json.loads(capsys.readouterr().out)
    assert code == 0 and out["publication_day"] == "2026-11-06"
    code = job.main(["--day", "2026-11-07", "--out-root", str(tmp_path / "p")], engine=FakeEngine(_data()), now=NOW)
    assert code == 2 and json.loads(capsys.readouterr().out)["state"] == "REFUSED"
    assert job.main(["--day", "nonsense", "--out-root", str(tmp_path / "p")], engine=FakeEngine(_data()), now=NOW) == 2


def test_audio_metadata_and_grounding_are_read_bounded(job, tmp_path):
    audio = tmp_path / "audio"
    audio.mkdir()
    (audio / "briefing_2026-11-06_x.json").write_text(json.dumps({
        "generated_at": "2026-11-06T12:00:00Z", "script_text": "Audio script.", "briefing_date": "2026-11-06",
        "account_balance": 12345}))
    (audio / "briefing_2026-11-05_y.json").write_text(json.dumps({"generated_at": "2026-11-05T12:00:00Z", "script_text": "old"}))
    grounding = tmp_path / "grounding"
    grounding.mkdir()
    briefing = _briefing(1, 1)
    stamp = briefing["created_at"].strftime("%Y%m%d_%H%M%S")
    (grounding / f"hourly_{stamp}.md").write_text(briefing["content"] + "\n\n---\n*Generated: now*")
    (grounding / f"hourly_{stamp}.grounding.json").write_text(json.dumps({"total_numbers": 2, "grounded_count": 2,
                                                                          "ungrounded_values": ["secret-ish"]}))
    result = _run(job, tmp_path, FakeEngine(_data(briefings=[briefing])), audio_dir=audio, grounding_dir=grounding)
    capture = json.loads((tmp_path / "packets" / "2026-11-06" / "capture.json").read_bytes())
    audio_rows = [r["record"] for r in capture["records"] if r["channel"] == "audio"]
    assert [r["id"] for r in audio_rows] == ["briefing_2026-11-06_x.json"] and "account_balance" not in audio_rows[0]
    briefing_row = next(r["record"] for r in capture["records"] if r["channel"] == "market_briefings")
    assert briefing_row["grounding_receipt"] == {"total_numbers": 2, "grounded_count": 2}
    assert result["channels"]["audio"]["count"] == 1


# ── public screened tree (never capture.json) ───────────────────────────────

def _public_files(public_root: Path) -> set[str]:
    return {str(p.relative_to(public_root)) for p in public_root.rglob("*") if p.is_file()}


def test_publish_copies_only_the_screened_allow_list(job, tmp_path):
    public_root = tmp_path / "public"
    result = _run(job, tmp_path, FakeEngine(_data()), public_root=public_root)
    assert result["state"] == "PRODUCED"
    day_dir = tmp_path / "packets" / DAY.isoformat()
    assert (day_dir / "capture.json").is_file()
    published = _public_files(public_root)
    assert published == {f"{DAY}/packet.json", f"{DAY}/receipt.json", f"{DAY}/observation.json",
                         f"{DAY}/receipts/diary_run.json", f"{DAY}/receipts/candidate.json",
                         f"{DAY}/receipts/capture.json"}
    assert "capture.json" not in {Path(p).name for p in published if "/receipts/" not in p}
    for rel in published:
        assert (public_root / rel).read_bytes() == (day_dir / Path(rel).relative_to(DAY.isoformat())).read_bytes()
        assert oct((public_root / rel).stat().st_mode & 0o777) == "0o644"
    assert oct((public_root / DAY.isoformat()).stat().st_mode & 0o777) == "0o755"
    assert oct(day_dir.stat().st_mode & 0o777) == "0o700"
    assert sorted(result["published"]["copied"]) == sorted(p.split("/", 1)[1] for p in published)


def test_rerun_publishes_an_already_produced_day_without_rewriting(job, tmp_path):
    first = _run(job, tmp_path, FakeEngine(_data()))  # no public_root: nothing published
    assert first["published"] is None
    public_root = tmp_path / "public"
    second = _run(job, tmp_path, FakeEngine(_data()), public_root=public_root)
    assert second["already_present"] and second["receipt_sha256"] == first["receipt_sha256"]
    assert f"{DAY}/receipt.json" in _public_files(public_root) and f"{DAY}/capture.json" not in _public_files(public_root)
    third = _run(job, tmp_path, FakeEngine(_data()), public_root=public_root)
    assert third["published"]["copied"] == [] and len(third["published"]["unchanged"]) == 6


def test_publish_refuses_the_private_capture_even_if_asked(job, tmp_path):
    day_dir = tmp_path / "packets" / DAY.isoformat()
    day_dir.mkdir(parents=True)
    (day_dir / "capture.json").write_bytes(b"raw")
    (day_dir / "receipt.json").write_bytes(b"{}")
    public = tmp_path / "public" / DAY.isoformat()
    job.publish_screened(day_dir, public)
    assert _public_files(tmp_path / "public") == {f"{DAY}/receipt.json"}
    with pytest.raises(RuntimeError):
        job.PUBLIC_FILES = job.PUBLIC_FILES + ("capture.json",)
        try:
            job.publish_screened(day_dir, public)
        finally:
            job.PUBLIC_FILES = tuple(f for f in job.PUBLIC_FILES if f != "capture.json")
    assert f"{DAY}/capture.json" not in _public_files(tmp_path / "public")


def test_failed_run_publishes_its_failure_marker(job, tmp_path):
    public_root = tmp_path / "public"
    result = _run(job, tmp_path, FakeEngine(_data(), broken=True), public_root=public_root)
    assert result["state"] == "FAILED"
    names = {Path(p).name for p in _public_files(public_root)}
    assert "observation.json" in names and any(n.startswith("FAILED-") for n in names)
    assert "capture.json" not in names


def test_publish_failure_never_blocks_production(job, tmp_path):
    blocked = tmp_path / "blocked"
    blocked.write_text("a file where a directory should be")
    result = _run(job, tmp_path, FakeEngine(_data()), public_root=blocked)
    assert result["state"] == "PRODUCED" and result["published"]["error"]
    assert (tmp_path / "packets" / DAY.isoformat() / "receipt.json").is_file()


def test_dry_run_publishes_nothing(job, tmp_path):
    public_root = tmp_path / "public"
    result = _run(job, tmp_path, FakeEngine(_data()), public_root=public_root, dry_run=True)
    assert result["published"] is None and not public_root.exists()


def test_cli_no_publish_flag(job, tmp_path, monkeypatch):
    monkeypatch.setattr(job, "systemd_unit_evidence", lambda *a, **k: {"available": False, "reason": "test"})
    monkeypatch.setattr(job, "AUDIO_DIR", tmp_path / "a")
    monkeypatch.setattr(job, "GROUNDING_DIR", tmp_path / "g")
    rc = job.main(["--day", DAY.isoformat(), "--out-root", str(tmp_path / "packets"),
                   "--public-root", str(tmp_path / "public"), "--no-publish"], engine=FakeEngine(_data()), now=NOW)
    assert rc == 0 and not (tmp_path / "public").exists()
    rc = job.main(["--day", DAY.isoformat(), "--out-root", str(tmp_path / "packets"),
                   "--public-root", str(tmp_path / "public")], engine=FakeEngine(_data()), now=NOW)
    assert rc == 0 and (tmp_path / "public" / DAY.isoformat() / "receipt.json").is_file()


def test_service_template_sets_the_public_root():
    text = (ROOT / "deploy" / "systemd" / "grid-publication-packet.service.template").read_text()
    assert "GRID_PUBLICATION_PACKET_PUBLIC_ROOT=/data/grid/outputs/publication-packets" in text
