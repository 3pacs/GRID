"""Unit tests for scripts/people_events_backfill.py (no database)."""

from __future__ import annotations

import pandas as pd

from scripts import people_events_backfill as B


def _row(**over):
    row = {"accession_number": "acc-1", "document_type": "4", "amended": False, "filing_date": "2026-04-03",
           "issuer_cik": "0000320193", "issuer_ticker": "AAPL", "owner_cik": "0001214156",
           "owner_name": "COOK TIMOTHY D", "is_director": False, "is_officer": True, "is_ten_pct_owner": False,
           "nonderiv_trans_sk": "1", "transaction_date": "2026-04-01", "transaction_date_raw": "01-APR-2026",
           "transaction_code": "P", "shares": 1000.0, "price_per_share": 200.0, "acquired_disposed_code": "A"}
    row.update(over)
    return row


def test_build_events_keeps_only_selected_codes_and_resolves_by_cik():
    rows = [_row(), _row(nonderiv_trans_sk="2", transaction_code="M", shares=5.0),
            _row(nonderiv_trans_sk="3", transaction_code="S", shares=7.0),
            _row(nonderiv_trans_sk="4", transaction_code="F", shares=9.0)]
    ids = pd.DataFrame([{"entity_id": "sm_0000320193", "id_scheme": "cik", "id_value": "320193",
                         "valid_from": "2026-09-27", "valid_to": None, "is_primary": True, "conflict_flag": False}])
    events, stats = B.build_events(pd.DataFrame(rows), ("P", "S", "A"), ids)
    assert sorted(events["transaction_code"]) == ["P", "S"]
    assert stats["events_all_codes"] == 4 and stats["events_selected"] == 2
    assert set(events["security_id"]) == {"sm_0000320193"}
    assert stats["pit"]["known_before_event"] == 0


def test_expected_counts_by_known_year_and_code():
    rows = [_row(), _row(accession_number="a2", filing_date="2025-12-31", transaction_date="2025-12-30",
                         transaction_date_raw="30-DEC-2025", nonderiv_trans_sk="9", transaction_code="S")]
    events, _ = B.build_events(pd.DataFrame(rows), ("P", "S"), pd.DataFrame())
    # 22:00 New York on 2025-12-31 is 2026-01-01 03:00Z: counted in the UTC year of known_at.
    assert B.expected_counts(events) == {"2026|P": 1, "2026|S": 1}


def test_growth_check_band_and_projection():
    ok, info = B.growth_check(0, 50_000_000, 50_000, 4_000_000, 8.0)
    assert ok and "bytes_per_row" not in info  # too few rows to judge
    ok, info = B.growth_check(0, 100_000_000, 100_000, 4_460_000, 8.0)
    assert ok and info["bytes_per_row"] == 1000.0 and info["projected_gb"] == 4.46
    ok, _ = B.growth_check(0, 300_000_000, 100_000, 4_460_000, 8.0)  # 3 KB/row: outside the band
    assert not ok
    ok, _ = B.growth_check(0, 190_000_000, 100_000, 4_460_000, 8.0)  # 1.9 KB/row -> 8.5 GB projected
    assert not ok


# --- main() with a fake writer / database ------------------------------------------------


import json  # noqa: E402
import hashlib  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

import pytest  # noqa: E402


def _fake_world(monkeypatch, n_events: int, writer):
    rows = [_row(accession_number=f"a{i}", owner_cik=str(1000 + i), owner_name=f"OWNER{i} PERSON{i}",
                 nonderiv_trans_sk=str(i)) for i in range(n_events)]
    events, stats = B.build_events(pd.DataFrame(rows), ("P", "S", "A"), pd.DataFrame())

    stored = []
    audit = []
    monkeypatch.setattr(B, "_load", lambda args, url: (events, dict(stats), pd.DataFrame(stored)))
    monkeypatch.setattr(B, "sha256_file", lambda path: "0" * 64)
    monkeypatch.setattr(B, "_rw_engine", lambda url: type("E", (), {"dispose": lambda self: None})())
    size = {"bytes": 0}

    def scalar(engine, sql):
        return size["bytes"]

    monkeypatch.setattr(B, "_scalar", scalar)
    monkeypatch.setattr(B.RO, "assert_db_window_open", lambda now=None: None)
    monkeypatch.setattr(B, "minutes_until_window", lambda now=None: 600.0)
    monkeypatch.setattr(B.G, "write_window_open", lambda now=None, **kwargs: True)
    monkeypatch.setattr(B.time, "sleep", lambda s: None)
    import intelligence.people_events_pipeline.writer as W

    def fake_apply(engine, ev, plan, **kw):
        kw["before_transaction"]()
        result = writer(ev, plan)
        kw["before_commit"]()
        size["bytes"] += 1000 * len(plan) + 500  # include run metadata, not just events
        stored.extend(ev.to_dict("records"))
        audit.append((kw["run_id"], len(plan)))
        return result

    monkeypatch.setattr(W, "apply_write_plan", fake_apply)

    def fake_audit(engine, prefix, source_hash):
        values = [(run, count) for run, count in audit if run.startswith(prefix + "-b")]
        n = sum(count for _, count in values)
        digest = hashlib.sha256(b"\n".join(B.G.batch_audit_record(run, batch,
            {"insert": count, "written": count, **dict.fromkeys(B.G.ZERO_COUNT_KEYS, 0)})
            for batch, (run, count) in enumerate(values))).hexdigest()
        return {"batches": len(values), "successful": len(values), "inserted": n, "written": n,
                "invalid": 0, "progress_digest": digest}

    monkeypatch.setattr(B, "_run_audit", fake_audit)
    return events


def _args(tmp_path, *extra):
    return ["execute", "--form345", str(tmp_path / "x.parquet"), "--out-dir", str(tmp_path),
            "--db-url-env", "PE_TEST_URL", "--batch-rows", "3", "--baseline-bytes", "0",
            "--expect-source-sha256", "0" * 64, *extra]


def test_execute_slices_batches_and_records_receipts(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    seen = []

    def writer(ev, plan):
        assert list(ev["dedup_key"]) == list(plan["dedup_key"])  # events and plan stay aligned
        seen.extend(plan["dedup_key"])
        return {"status": "SUCCESS", "counts": {"insert": len(plan)}}

    events = _fake_world(monkeypatch, 8, writer)
    assert B.main(_args(tmp_path)) == 0
    assert sorted(seen) == sorted(events["dedup_key"])
    receipt = json.loads(next(tmp_path.glob("execute_*.json")).read_text())
    assert receipt["status"] == "DONE" and receipt["rows_written"] == 8
    assert "batches" not in receipt  # growing batch list is append-only JSONL now
    progress = [json.loads(line) for line in (tmp_path / B.G.PROGRESS_NAME).read_text().splitlines()]
    assert len(progress) == 3 and progress[-1]["rows_written"] == 8


def test_execute_failure_is_recorded_as_failed(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    calls = {"n": 0}

    def writer(ev, plan):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("unique violation")
        return {"status": "SUCCESS", "counts": {"insert": len(plan)}}

    _fake_world(monkeypatch, 8, writer)
    with pytest.raises(RuntimeError):
        B.main(_args(tmp_path))
    receipt = json.loads(next(tmp_path.glob("execute_*.json")).read_text())
    assert receipt["status"] == "FAILED" and "unique violation" in receipt["error"]
    assert receipt["failed_run_id"].endswith("b00001") and receipt["rows_written"] == 3


def test_execute_stops_on_max_batches_and_refuses_partial_scope(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    _fake_world(monkeypatch, 8, lambda ev, plan: {"status": "SUCCESS", "counts": {"insert": len(plan)}})
    assert B.main(_args(tmp_path, "--max-batches", "1")) == 4
    receipt = json.loads(next(tmp_path.glob("execute_*.json")).read_text())
    assert receipt["status"] == "STOPPED_MAX_BATCHES" and receipt["rows_written"] == 3
    assert B.main(_args(tmp_path, "--quarters", "2026q1")) == 2


def test_execute_refuses_close_to_the_window(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    _fake_world(monkeypatch, 2, lambda ev, plan: {"status": "SUCCESS", "counts": {"insert": len(plan)}})
    monkeypatch.setattr(B.G, "write_window_open", lambda now=None, **kwargs: False)
    assert B.main(_args(tmp_path)) == 2


def test_minutes_until_window():
    assert B.minutes_until_window(datetime(2026, 10, 2, 2, 30, tzinfo=timezone.utc)) == 60.0
    assert B.minutes_until_window(datetime(2026, 10, 2, 5, 0, tzinfo=timezone.utc)) == 0.0
    assert B.minutes_until_window(datetime(2026, 10, 2, 11, 30, tzinfo=timezone.utc)) == 16 * 60.0


def test_execute_clamps_old_20000_flag_to_fifty(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    sizes = []
    _fake_world(monkeypatch, 101, lambda ev, plan:
                sizes.append(len(plan)) or {"status": "SUCCESS", "counts": {"insert": len(plan)}})
    assert B.main(_args(tmp_path, "--batch-rows", "20000")) == 0
    assert sizes == [50, 50, 1]


def test_execute_resume_reuses_baseline_and_does_not_repeat_rows(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    seen = []
    _fake_world(monkeypatch, 8, lambda ev, plan:
                seen.extend(plan["dedup_key"]) or {"status": "SUCCESS", "counts": {"insert": len(plan)}})
    assert B.main(_args(tmp_path, "--max-batches", "1")) == 4
    manifest = (tmp_path / B.G.MANIFEST_NAME).read_bytes()
    assert B.main(_args(tmp_path)) == 0
    assert len(seen) == len(set(seen)) == 8
    assert (tmp_path / B.G.MANIFEST_NAME).read_bytes() == manifest
    assert B.G.read_progress(tmp_path / B.G.PROGRESS_NAME,
                           json.loads(manifest)["run_prefix"])[:2] == (3, 8)


def test_execute_missing_progress_or_db_disagreement_refuses_resume(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    seen = []
    _fake_world(monkeypatch, 8, lambda ev, plan:
                seen.extend(plan["dedup_key"]) or {"status": "SUCCESS", "counts": {"insert": len(plan)}})
    assert B.main(_args(tmp_path, "--max-batches", "1")) == 4
    progress_path = tmp_path / B.G.PROGRESS_NAME
    original = progress_path.read_text()
    progress_path.unlink()
    with pytest.raises(RuntimeError, match="progress is missing"):
        B.main(_args(tmp_path))
    progress_path.write_text(original)
    monkeypatch.setattr(B, "_run_audit", lambda *args:
                        {"batches": 2, "successful": 2, "inserted": 6, "written": 6,
                         "invalid": 0, "progress_digest": "unexpected"})
    with pytest.raises(RuntimeError, match="disagree"):
        B.main(_args(tmp_path))
    assert len(seen) == 3


def test_execute_requires_prebackup_baseline_and_rejects_update_plan(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    calls = []
    _fake_world(monkeypatch, 3, lambda ev, plan:
                calls.append(1) or {"status": "SUCCESS", "counts": {"insert": len(plan)}})
    args = _args(tmp_path)
    del args[args.index("--baseline-bytes"):args.index("--baseline-bytes") + 2]
    with pytest.raises(RuntimeError, match="baseline-bytes"):
        B.main(args)
    original_plan = B.P.build_write_plan

    def updates(*args):
        plan = original_plan(*args)
        plan.loc[0, "op"] = "supersede"
        return plan

    monkeypatch.setattr(B.P, "build_write_plan", updates)
    assert B.main(_args(tmp_path)) == 3
    assert not calls


def test_execute_rejects_unapproved_codes_and_changed_input(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    _fake_world(monkeypatch, 3, lambda ev, plan: pytest.fail("must not write"))
    with pytest.raises(SystemExit):
        B.main(_args(tmp_path, "--codes", "P,S,M"))
    with pytest.raises(RuntimeError, match="reviewed plan hash"):
        B.main(_args(tmp_path, "--expect-source-sha256", "1" * 64))
    args = _args(tmp_path)
    del args[args.index("--expect-source-sha256"):args.index("--expect-source-sha256") + 2]
    assert B.main(args) == 2
    hashes = iter(["0" * 64, "1" * 64])
    monkeypatch.setattr(B, "sha256_file", lambda path: next(hashes))
    with pytest.raises(RuntimeError, match="source changed while loading"):
        B.main(_args(tmp_path))


def test_execute_growth_includes_run_metadata_and_stops_before_absolute_cap(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    calls = []
    _fake_world(monkeypatch, 3, lambda ev, plan:
                calls.append(1) or {"status": "SUCCESS", "counts": {"insert": len(plan)}})
    # 7.98GB is below8GB but violates the conservative32MiB reserve before writes.
    monkeypatch.setattr(B, "_scalar", lambda engine, sql: 7_980_000_000)
    assert B.main(_args(tmp_path)) == 4
    assert not calls
    receipt = json.loads(next(tmp_path.glob("execute_*.json")).read_text())
    assert receipt["status"] == "STOPPED_GROWTH"


def test_execute_throttle_preserves_twenty_thousand_row_cadence(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    _fake_world(monkeypatch, 101, lambda ev, plan: {"status": "SUCCESS", "counts": {"insert": len(plan)}})
    monkeypatch.setattr(B.G, "THROTTLE_ROWS", 100)
    sleeps = []
    monkeypatch.setattr(B.time, "sleep", sleeps.append)
    assert B.main(_args(tmp_path, "--batch-rows", "50", "--sleep", "2")) == 0
    assert sleeps == [2.0]


def test_execute_audits_full_run_history_once_per_start_or_resume(tmp_path, monkeypatch):
    monkeypatch.setenv("PE_TEST_URL", "postgresql://x/y_test")
    _fake_world(monkeypatch, 12, lambda ev, plan: {"status": "SUCCESS", "counts": {"insert": len(plan)}})
    original = B._run_audit
    calls = []

    def audit(*args):
        calls.append(1)
        return original(*args)

    monkeypatch.setattr(B, "_run_audit", audit)
    assert B.main(_args(tmp_path, "--max-batches", "2")) == 4
    assert calls == [1]
    assert B.main(_args(tmp_path)) == 0
    assert calls == [1, 1]
