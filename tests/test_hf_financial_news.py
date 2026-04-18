from __future__ import annotations

import hashlib
from datetime import date
import sys
import types

from ingestion.altdata.hf_financial_news import HFFinancialNewsPuller


class _Result:
    def __init__(self, fetchone_value=None, fetchall_value=None):
        self._fetchone_value = fetchone_value
        self._fetchall_value = fetchall_value or []

    def fetchone(self):
        return self._fetchone_value

    def fetchall(self):
        return self._fetchall_value


class _FakeConnection:
    def __init__(self, seeded_rows: list[dict] | None = None):
        self.rows = list(seeded_rows or [])
        self.inserted_params: list[dict] = []
        self.queries: list[tuple[str, dict]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, statement, params=None):
        sql = str(statement)
        parameters = dict(params or {})
        self.queries.append((sql, parameters))

        if "SELECT id FROM source_catalog" in sql:
            return _Result((1,))

        if "SELECT MAX(obs_date) FROM raw_series" in sql:
            pattern = parameters["pattern"].removesuffix("%")
            matching = [
                row["obs_date"]
                for row in self.rows
                if row["series_id"].startswith(pattern)
            ]
            return _Result((max(matching) if matching else None,))

        if "SELECT DISTINCT obs_date FROM raw_series" in sql:
            sid = parameters["sid"]
            dates = sorted(
                {
                    row["obs_date"]
                    for row in self.rows
                    if row["series_id"] == sid
                }
            )
            return _Result(fetchall_value=[(d,) for d in dates])

        if "SELECT series_id, obs_date FROM raw_series" in sql:
            pattern = parameters["pattern"].removesuffix("%")
            min_date = parameters["od"]
            matches = [
                (row["series_id"], row["obs_date"])
                for row in self.rows
                if row["series_id"].startswith(pattern)
                and row["obs_date"] >= min_date
            ]
            return _Result(fetchall_value=matches)

        if sql.lstrip().upper().startswith("INSERT INTO RAW_SERIES"):
            self.inserted_params.append(parameters)
            self.rows.append(
                {
                    "series_id": parameters["sid"],
                    "obs_date": parameters["od"],
                }
            )
            return _Result()

        return _Result()


class _FakeEngine:
    def __init__(self, connection: _FakeConnection):
        self._connection = connection

    def connect(self):
        return self._connection

    def begin(self):
        return self._connection


def _install_fake_datasets(monkeypatch, rows):
    datasets_mod = types.ModuleType("datasets")

    def _load_dataset(**kwargs):
        return rows

    datasets_mod.load_dataset = _load_dataset
    monkeypatch.setitem(sys.modules, "datasets", datasets_mod)


def _expected_series_id(title: str, snippet: str, obs_date: date) -> str:
    digest = hashlib.md5(
        f"{title}:{snippet[:100]}:{obs_date}".encode()
    ).hexdigest()[:12]
    return f"hf_news.twitter_financial_sentiment.{digest}"


def test_no_date_rows_stay_distinct_and_idempotent(monkeypatch):
    rows = [
        {"text": "Profit jumps on strong demand", "label": 2},
        {"text": "Margins compress on weak guidance", "label": 0},
    ]
    _install_fake_datasets(monkeypatch, rows)

    conn = _FakeConnection()
    puller = HFFinancialNewsPuller(_FakeEngine(conn))

    first = puller.pull_subset("twitter_financial_sentiment")
    second = puller.pull_subset("twitter_financial_sentiment")

    assert first["status"] == "SUCCESS"
    assert first["rows_inserted"] == 2
    assert second["rows_inserted"] == 0
    assert len(conn.inserted_params) == 2
    assert {p["sid"] for p in conn.inserted_params} == {
        _expected_series_id(
            "Profit jumps on strong demand",
            "Profit jumps on strong demand",
            date(1990, 1, 1),
        ),
        _expected_series_id(
            "Margins compress on weak guidance",
            "Margins compress on weak guidance",
            date(1990, 1, 1),
        ),
    }
    assert {p["od"] for p in conn.inserted_params} == {date(1990, 1, 1)}


def test_per_article_checkpoint_uses_namespace_prefix(monkeypatch):
    rows = [
        {
            "Headline": "New markets rally",
            "Article": "Markets rose across Europe.",
            "Date": "2024-01-11",
        }
    ]
    _install_fake_datasets(monkeypatch, rows)

    seeded_rows = [
        {
            "series_id": "hf_news.bloomberg_financial_news.deadbeef0001",
            "obs_date": date(2024, 1, 10),
        }
    ]
    conn = _FakeConnection(seeded_rows=seeded_rows)
    puller = HFFinancialNewsPuller(_FakeEngine(conn))

    result = puller.pull_subset("bloomberg_financial_news")

    assert result["status"] == "SUCCESS"
    assert result["rows_inserted"] == 1
    latest_queries = [
        (sql, params)
        for sql, params in conn.queries
        if "SELECT MAX(obs_date) FROM raw_series" in sql
    ]
    assert latest_queries
    assert latest_queries[0][1]["pattern"] == "hf_news.bloomberg_financial_news.%"
