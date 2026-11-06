"""Tests for scripts/compute_job_cancel.py (dry-run-by-default job cancel CLI)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
import requests

from scripts import compute_job_cancel as cancel


def _job(job_id, *, state="QUEUED", job_type="LLM_INFERENCE",
         name="producer llm synthesis 20261010T231756Z-000",
         created_at="2026-10-10T23:17:56+00:00"):
    return {"id": job_id, "state": state, "job_type": job_type, "name": name, "created_at": created_at}


JOBS = [
    _job(22444886),
    _job(22469339, created_at="2026-10-11T02:54:06+00:00",
         name="producer llm synthesis 20261011T025406Z-011"),
    _job(5, job_type="BACKTEST", name="producer model 3 walk-forward 20261106T000027Z-001",
         created_at="2026-11-06T00:00:27+00:00"),
    _job(6, state="IN_PROGRESS"),
]


def test_select_jobs_applies_every_filter():
    picked = cancel.select_jobs(
        JOBS,
        job_type="LLM_INFERENCE",
        name_prefix="producer llm synthesis",
        created_before=datetime(2026, 10, 12, tzinfo=timezone.utc),
    )
    assert [j["id"] for j in picked] == [22444886, 22469339]

    newer_only = cancel.select_jobs(
        JOBS, job_type="LLM_INFERENCE",
        created_before=datetime(2026, 10, 11, tzinfo=timezone.utc),
    )
    assert [j["id"] for j in newer_only] == [22444886]

    # In-flight jobs are never selected.
    assert cancel.select_jobs(JOBS, ids={6}) == []


def test_summarize_groups_by_name_without_run_stamp():
    summary = cancel.summarize(JOBS[:2], now=datetime(2026, 11, 6, tzinfo=timezone.utc))

    assert summary["count"] == 2
    assert summary["groups"] == [{
        "job_type": "LLM_INFERENCE", "state": "QUEUED",
        "name_prefix": "producer llm synthesis", "count": 2,
    }]
    assert summary["max_age_days"] == 26.0


@pytest.mark.parametrize(
    ("selected", "expect", "truncated", "ok"),
    [
        (79, 79, False, True),
        (79, None, False, False),
        (79, 78, False, False),
        (0, 0, False, False),
        (79, 79, True, False),
    ],
)
def test_execution_requires_matching_expect_count(selected, expect, truncated, ok):
    refusal = cancel.execution_refusal(selected=selected, expect_count=expect, truncated=truncated)
    assert (refusal is None) is ok


class _Resp:
    def __init__(self, payload=None, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def test_fetch_flags_truncation_at_limit():
    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append((url, params))
        return _Resp(JOBS[:2])

    jobs, truncated = cancel.fetch_candidates(
        "http://c", ids=[], states=["QUEUED"], job_type="LLM_INFERENCE", fetch_limit=2, get=fake_get,
    )
    assert truncated is True
    assert calls == [("http://c/jobs", {"state": "QUEUED", "limit": 2, "job_type": "LLM_INFERENCE"})]
    assert len(jobs) == 2


def test_cancel_jobs_reports_partial_failures():
    def fake_post(url, params=None, timeout=None):
        return _Resp({}, 400 if url.endswith("/2/cancel") else 200)

    result = cancel.cancel_jobs("http://c", [1, 2, 3], reason="why", post=fake_post)

    assert result["cancelled_ids"] == [1, 3]
    assert [f["id"] for f in result["failed"]] == [2]


def _patch_http(monkeypatch, posted):
    monkeypatch.setattr(cancel.requests, "get", lambda url, params=None, timeout=None: _Resp(JOBS))

    def fake_post(url, params=None, timeout=None):
        posted.append((url, params))
        return _Resp({})

    monkeypatch.setattr(cancel.requests, "post", fake_post)


ARGS = ["--coordinator", "http://c", "--job-type", "LLM_INFERENCE",
        "--name-prefix", "producer llm synthesis", "--created-before", "2026-10-12"]


def test_main_is_dry_run_by_default(monkeypatch, capsys):
    posted = []
    _patch_http(monkeypatch, posted)

    assert cancel.main(ARGS) == 0

    out = json.loads(capsys.readouterr().out)
    assert out["dry_run"] is True
    assert out["selection"]["ids"] == [22444886, 22469339]
    assert posted == []


def test_main_execute_refuses_without_expect_count(monkeypatch, capsys):
    posted = []
    _patch_http(monkeypatch, posted)

    assert cancel.main(ARGS + ["--execute"]) == 2
    assert "expect-count" in json.loads(capsys.readouterr().out)["refused"]
    assert posted == []


def test_main_execute_cancels_with_reason(monkeypatch, capsys):
    posted = []
    _patch_http(monkeypatch, posted)

    rc = cancel.main(ARGS + ["--execute", "--expect-count", "2", "--reason", "stale filler"])

    assert rc == 0
    assert posted == [
        ("http://c/jobs/22444886/cancel", {"reason": "stale filler"}),
        ("http://c/jobs/22469339/cancel", {"reason": "stale filler"}),
    ]


def test_main_refuses_empty_filter(monkeypatch, capsys):
    posted = []
    _patch_http(monkeypatch, posted)

    assert cancel.main(["--coordinator", "http://c"]) == 2
    assert posted == []
