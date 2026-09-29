"""Revision-gate tests for ingestion/altdata/hf_financial_news.py.

tests/fixtures/hf_news/dataset_info_bloomberg_20260929.json is the real
Hub API response for danidanou/Bloomberg_Financial_News recorded from
grid-svr on 2026-09-29. No network is used.
"""

from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest

import ingestion.altdata.hf_financial_news as mod
from ingestion.altdata.hf_financial_news import HFFinancialNewsPuller

INFO = json.loads(
    (Path(__file__).parent / "fixtures" / "hf_news" / "dataset_info_bloomberg_20260929.json")
    .read_text(encoding="utf-8")
)


class _Result:
    def __init__(self, one=None, rows=None):
        self._one = one
        self._rows = rows or []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, last_pull: datetime | None):
        self.last_pull = last_pull
        self.queries: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.queries.append(sql)
        if "SELECT id FROM source_catalog" in sql:
            return _Result((203,))
        if "MAX(pull_timestamp)" in sql:
            return _Result((self.last_pull,))
        return _Result()


class _Engine:
    def __init__(self, conn):
        self.conn = conn

    def connect(self):
        return self.conn

    def begin(self):
        return self.conn


@pytest.fixture
def datasets_calls(monkeypatch):
    calls: list[dict] = []
    ds = types.ModuleType("datasets")

    def _load_dataset(**kwargs):
        calls.append(kwargs)
        return []

    ds.load_dataset = _load_dataset
    monkeypatch.setitem(sys.modules, "datasets", ds)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    return calls


def test_fetcher_parses_recorded_hub_response(monkeypatch):
    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return INFO

    seen = {}

    def _get(url, headers=None, timeout=0):  # noqa: ARG001
        seen["url"] = url
        return _Resp()

    monkeypatch.setattr(mod.requests, "get", _get)
    ts = mod.fetch_hf_dataset_last_modified("danidanou/Bloomberg_Financial_News")
    assert seen["url"] == "https://huggingface.co/api/datasets/danidanou/Bloomberg_Financial_News"
    assert ts == datetime(2024, 6, 18, 20, 10, 49, tzinfo=timezone.utc)


def test_unchanged_dataset_skips_heavy_queries(datasets_calls, monkeypatch):
    conn = _Conn(last_pull=datetime(2026, 4, 19, 3, 0, tzinfo=timezone.utc))
    puller = HFFinancialNewsPuller(_Engine(conn))
    monkeypatch.setattr(
        puller, "_last_modified_fetcher",
        lambda hf_id: datetime(2024, 6, 18, 20, 10, 49, tzinfo=timezone.utc),
    )
    result = puller.pull_subset("bloomberg_financial_news")

    assert result["status"] == "UNCHANGED"
    assert result["rows_inserted"] == 0
    assert datasets_calls == []  # no streaming
    # The LIKE-scan that timed out in prod must not run.
    assert not any("MAX(obs_date)" in q for q in conn.queries)
    assert not any("series_id LIKE" in q for q in conn.queries)


def test_modified_dataset_is_reingested(datasets_calls, monkeypatch):
    conn = _Conn(last_pull=datetime(2024, 1, 1, tzinfo=timezone.utc))
    puller = HFFinancialNewsPuller(_Engine(conn))
    monkeypatch.setattr(
        puller, "_last_modified_fetcher",
        lambda hf_id: datetime(2024, 6, 18, 20, 10, 49, tzinfo=timezone.utc),
    )
    result = puller.pull_subset("bloomberg_financial_news")
    assert result["status"] == "SUCCESS"
    assert len(datasets_calls) == 1


def test_first_ever_pull_does_not_need_hub_check(datasets_calls, monkeypatch):
    conn = _Conn(last_pull=None)
    puller = HFFinancialNewsPuller(_Engine(conn))

    def _boom(hf_id):
        raise AssertionError("should not be called")

    monkeypatch.setattr(puller, "_last_modified_fetcher", _boom)
    assert puller.pull_subset("twitter_financial_sentiment")["status"] == "SUCCESS"


def test_hub_failure_is_failed_and_all_failed_raises(datasets_calls, monkeypatch):
    conn = _Conn(last_pull=datetime(2026, 4, 19, tzinfo=timezone.utc))
    puller = HFFinancialNewsPuller(_Engine(conn))

    def _down(hf_id):
        raise ConnectionError("hub down")

    monkeypatch.setattr(puller, "_last_modified_fetcher", _down)
    one = puller.pull_subset("bloomberg_financial_news")
    assert one["status"] == "FAILED"
    # Callers treat a returned list as success, so all-failed must raise.
    with pytest.raises(RuntimeError, match="every subset failed"):
        puller.pull_all()
    assert datasets_calls == []


def test_all_unchanged_returns_normally(datasets_calls, monkeypatch):
    conn = _Conn(last_pull=datetime(2026, 4, 19, tzinfo=timezone.utc))
    puller = HFFinancialNewsPuller(_Engine(conn))
    monkeypatch.setattr(
        puller, "_last_modified_fetcher",
        lambda hf_id: datetime(2023, 1, 1, tzinfo=timezone.utc),
    )
    results = puller.pull_all()
    assert {r["status"] for r in results} == {"UNCHANGED"}
    assert sum(r["rows_inserted"] for r in results) == 0


def test_force_bypasses_gate(datasets_calls, monkeypatch):
    conn = _Conn(last_pull=datetime(2026, 4, 19, tzinfo=timezone.utc))
    puller = HFFinancialNewsPuller(_Engine(conn))
    monkeypatch.setattr(
        puller, "_last_modified_fetcher",
        lambda hf_id: datetime(2023, 1, 1, tzinfo=timezone.utc),
    )
    assert puller.pull_subset("twitter_financial_sentiment", force=True)["status"] == "SUCCESS"
    assert len(datasets_calls) == 1
