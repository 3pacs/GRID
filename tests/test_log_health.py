"""Tests for the log-health canary in scripts/log_health.py.

The canary is a guard rail — it has to keep classifying the messages we see
in the real world, otherwise the "do better every day" measurement becomes
meaningless. The fixtures below are taken from actual entries in
``.server-logs/errors.jsonl``.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.log_health import (
    _classify,
    _filter_window,
    _parse_window,
    _summarise,
    main,
)


class TestClassifier:
    @pytest.mark.parametrize("msg, family", [
        ("_api_get failed after 3 attempts: 429 Client Error", "rate_limit_429"),
        ("CBOE failed: 403 Client Error: Forbidden for url: ...", "forbidden_403"),
        ("404 Client Error: Not Found", "not_found_404"),
        ("400 Client Error: Bad Request", "bad_request_400"),
        ("503 Service Unavailable", "server_5xx"),
        ("504 Server Error: Gateway Timeout", "server_5xx"),
        ("RetryError[<Future raised HTTPError>]", "retry_exhausted"),
        ("Step 'oracle_cycle' timed out after 300s", "timeout"),
        ("Cycle 13 TIMED OUT after 600s", "timeout"),
        ("Connection aborted, RemoteDisconnected", "connection"),
        ("[Errno -5] No address associated with hostname", "connection"),
        ("No module named 'edgar'", "missing_module"),
        ("[server_log] git commit failed: Author identity unknown", "git_sink"),
        ("'date'", "key_error"),
        ("KeyError: 'value'", "key_error"),
        ("FRED pull failed for VIXCLS: 'date'", "key_error"),
        ("Some unique unanticipated bug", "other"),
    ])
    def test_classifies_known_messages(self, msg, family):
        assert _classify(msg) == family


class TestParseWindow:
    @pytest.mark.parametrize("spec,expected", [
        ("24h", timedelta(hours=24)),
        ("7d", timedelta(days=7)),
        ("30m", timedelta(minutes=30)),
        ("90s", timedelta(seconds=90)),
    ])
    def test_valid(self, spec, expected):
        assert _parse_window(spec) == expected

    @pytest.mark.parametrize("spec", ["24", "1y", "abc", "h"])
    def test_invalid(self, spec):
        with pytest.raises(Exception):
            _parse_window(spec)


class TestFilterWindow:
    def test_keeps_recent_drops_old(self):
        now = datetime.now(timezone.utc)
        recent = {"ts": (now - timedelta(hours=1)).isoformat(), "message": "x"}
        old = {"ts": (now - timedelta(days=10)).isoformat(), "message": "x"}
        out = _filter_window([recent, old], timedelta(days=1))
        assert recent in out and old not in out

    def test_skips_invalid_ts(self):
        out = _filter_window(
            [{"ts": "not-a-date", "message": "x"}], timedelta(hours=1),
        )
        assert out == []

    def test_skips_missing_ts(self):
        out = _filter_window([{"message": "x"}], timedelta(hours=1))
        assert out == []


class TestSummarise:
    def test_separates_real_from_transient(self):
        errs = [
            {"ts": "x", "module": "a", "function": "b", "message": "RetryError[...]"},
            {"ts": "x", "module": "a", "function": "b", "message": "504 Gateway Timeout"},
            {"ts": "x", "module": "c", "function": "d", "message": "KeyError: 'date'"},
        ]
        s = _summarise(errs)
        assert s["total"] == 3
        # Two transient (RetryError, 504), one real (KeyError).
        assert s["transient_errors"] == 2
        assert s["real_errors"] == 1


class TestMainCli:
    def _write_log(self, path: Path, entries: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as fh:
            for e in entries:
                fh.write(json.dumps(e) + "\n")

    def test_fail_over_triggers_when_real_count_exceeds(self, tmp_path):
        log = tmp_path / "errors.jsonl"
        now = datetime.now(timezone.utc).isoformat()
        # 3 real defects, all within window.
        self._write_log(log, [
            {"ts": now, "module": "x", "function": "y", "message": "KeyError: 'date'"},
            {"ts": now, "module": "x", "function": "y", "message": "AttributeError: foo"},
            {"ts": now, "module": "x", "function": "y", "message": "AssertionError: lookahead"},
        ])

        rc = main(["--path", str(log), "--window", "24h", "--fail-over", "1", "--json"])
        assert rc == 1

    def test_fail_over_passes_when_below(self, tmp_path):
        log = tmp_path / "errors.jsonl"
        now = datetime.now(timezone.utc).isoformat()
        self._write_log(log, [
            {"ts": now, "module": "x", "function": "y", "message": "504 Gateway Timeout"},
            {"ts": now, "module": "x", "function": "y", "message": "RetryError"},
        ])

        rc = main(["--path", str(log), "--window", "24h", "--fail-over", "1", "--json"])
        assert rc == 0

    def test_runs_without_fail_over(self, tmp_path):
        log = tmp_path / "errors.jsonl"
        self._write_log(log, [])
        rc = main(["--path", str(log), "--window", "24h", "--json"])
        assert rc == 0

    def test_missing_log_is_ok(self, tmp_path):
        rc = main([
            "--path", str(tmp_path / "does_not_exist.jsonl"),
            "--window", "24h", "--json",
        ])
        assert rc == 0
