import json
from datetime import datetime, timedelta, timezone

from scripts.audit_error_log import _normalize, audit


def _write_jsonl(path, rows):
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def test_normalize_buckets_volatile_error_details():
    msg = (
        "FRED pull failed for GDP: apiKey=SECRET123 at "
        "2026-05-07T01:02:03Z request "
        "9f0a8b7c6d5e4f001122334455667788"
    )

    normalized = _normalize(msg)

    assert "GDP" not in normalized
    assert "SECRET123" not in normalized
    assert "2026-05-07" not in normalized
    assert "9f0a8b7c6d5e4f001122334455667788" not in normalized
    assert "FRED pull failed for <SID>:" in normalized
    assert "apiKey=<REDACTED>" in normalized
    assert "<TS>" in normalized
    assert "<HEX>" in normalized


def test_normalize_buckets_future_reprs_and_solana_addresses():
    # 2026-11-05: these two shapes escaped bucketing, so a 331/day Helius
    # flood (one base58 mint per line) never reached the top-N.
    a = _normalize(
        "Helius GET /v0/addresses/WENWENvqqNya429ubCdR81ZmD69brwQaaBYY6p3LCpk/"
        "transactions failed: Client error '401 Unauthorized'"
    )
    b = _normalize(
        "Helius GET /v0/addresses/ARMbSB1MBRQrY6PNMao1HdQ431VafC6JRJCsGZ2Yv3iJ/"
        "transactions failed: Client error '401 Unauthorized'"
    )
    assert a == b and "<B58>" in a

    f1 = _normalize("Task exception: <Future at 0x7f3a2b1c4d50 state=finished raised TimeoutError>")
    f2 = _normalize("Task exception: <Future at 0x7f99e00a1b20 state=finished raised TimeoutError>")
    assert f1 == f2 and "<ADDR>" in f1

    # Ordinary identifiers and words are left alone.
    plain = "refresh_materialized_view_concurrently_for_all failed on supercalifragilisticexpialidocious"
    assert _normalize(plain) == plain


def test_audit_groups_recent_patterns(tmp_path, capsys):
    log_path = tmp_path / "errors.jsonl"
    cutoff = datetime(2026, 5, 7, tzinfo=timezone.utc)
    _write_jsonl(
        log_path,
        [
            {
                "ts": "2026-05-07T01:00:00+00:00",
                "level": "ERROR",
                "module": "ingestion.fred",
                "function": "pull",
                "message": "FRED pull failed for GDP: bad payload",
            },
            {
                "ts": "2026-05-07T02:00:00+00:00",
                "level": "ERROR",
                "module": "ingestion.fred",
                "function": "pull",
                "message": "FRED pull failed for CPI: bad payload",
            },
        ],
    )

    assert audit(log_path, cutoff=cutoff, top=5) == 0

    output = capsys.readouterr().out
    assert "[   2]" in output
    assert "FRED pull failed for <SID>: bad payload" in output


def test_audit_new_only_suppresses_baseline_patterns(tmp_path, capsys):
    log_path = tmp_path / "errors.jsonl"
    cutoff = datetime(2026, 5, 7, tzinfo=timezone.utc)
    _write_jsonl(
        log_path,
        [
            {
                "ts": "2026-05-06T23:00:00+00:00",
                "level": "ERROR",
                "module": "ingestion.fred",
                "function": "pull",
                "message": "FRED pull failed for GDP: bad payload",
            },
            {
                "ts": "2026-05-07T01:00:00+00:00",
                "level": "ERROR",
                "module": "ingestion.fred",
                "function": "pull",
                "message": "FRED pull failed for CPI: bad payload",
            },
        ],
    )

    assert audit(log_path, cutoff=cutoff, top=5, new_only=True) == 0

    output = capsys.readouterr().out
    assert "no NEW patterns" in output


def test_audit_fail_on_new_trips_when_threshold_exceeded(tmp_path, capsys):
    log_path = tmp_path / "errors.jsonl"
    cutoff = datetime(2026, 5, 7, tzinfo=timezone.utc)
    _write_jsonl(
        log_path,
        [
            {
                "ts": "2026-05-07T01:00:00+00:00",
                "level": "ERROR",
                "module": "ingestion.new_one",
                "function": "pull",
                "message": "first new pattern",
            },
            {
                "ts": "2026-05-07T02:00:00+00:00",
                "level": "ERROR",
                "module": "ingestion.new_two",
                "function": "pull",
                "message": "second new pattern",
            },
        ],
    )

    # Both rows are NEW (no baseline). Threshold 0 should trip.
    assert audit(log_path, cutoff=cutoff, top=5, fail_on_new=0) == 1
    err = capsys.readouterr().err
    assert "REGRESSION" in err

    # Threshold 10 should pass cleanly.
    assert audit(log_path, cutoff=cutoff, top=5, fail_on_new=10) == 0


def test_audit_fail_on_total_trips_when_count_exceeded(tmp_path, capsys):
    log_path = tmp_path / "errors.jsonl"
    cutoff = datetime(2026, 5, 7, tzinfo=timezone.utc)
    _write_jsonl(
        log_path,
        [
            {
                "ts": "2026-05-07T01:00:00+00:00",
                "level": "ERROR",
                "module": "ingestion.fred",
                "function": "pull",
                "message": f"failure {i}",
            }
            for i in range(5)
        ],
    )

    assert audit(log_path, cutoff=cutoff, top=5, fail_on_total=3) == 1
    err = capsys.readouterr().err
    assert "5 total error rows" in err

    assert audit(log_path, cutoff=cutoff, top=5, fail_on_total=100) == 0


def test_release_logs_reads_sibling_trees_and_counts_copies_once(tmp_path, capsys):
    """grid-scheduler writes the errors.jsonl of its pinned (older) release tree."""
    import os
    import time

    from scripts.audit_error_log import release_logs

    releases = tmp_path / "grid_release.releases"
    current = releases / "new" / ".server-logs"
    pinned = releases / "old" / ".server-logs"
    stale = releases / "older" / ".server-logs"
    for d in (current, pinned, stale):
        d.mkdir(parents=True)
    now = datetime.now(timezone.utc)
    shared = json.dumps({"ts": (now - timedelta(hours=1)).isoformat(), "level": "ERROR",
                         "module": "api", "function": "f", "message": "copied line"})
    sched = json.dumps({"ts": (now - timedelta(minutes=5)).isoformat(), "level": "ERROR",
                        "module": "ingestion.ml.finbert_scorer", "function": "score_all_sources",
                        "message": "FinBERT scoring failed"})
    (current / "errors.jsonl").write_text(shared + "\n")
    (pinned / "errors.jsonl").write_text(shared + "\n" + sched + "\n")
    (stale / "errors.jsonl").write_text(shared + "\n")
    old = time.time() - 3 * 86400
    os.utime(stale / "errors.jsonl", (old, old))

    live = tmp_path / "grid_release"
    live.symlink_to(releases / "new")

    cutoff = now - timedelta(hours=2)
    paths = release_logs(live / ".server-logs" / "errors.jsonl", cutoff)
    assert paths == [live / ".server-logs" / "errors.jsonl", pinned / "errors.jsonl"]

    assert audit(paths, cutoff=cutoff, top=5) == 0
    out = capsys.readouterr().out
    assert "(2 entries" in out
    assert "finbert_scorer" in out


def test_release_logs_outside_a_release_tree_is_just_the_default(tmp_path):
    from scripts.audit_error_log import release_logs

    default = tmp_path / "repo" / ".server-logs" / "errors.jsonl"
    assert release_logs(default, datetime.now(timezone.utc)) == [default]
