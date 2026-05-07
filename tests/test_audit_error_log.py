import json
from datetime import datetime, timezone

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
