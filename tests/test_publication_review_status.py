"""store/publication_review_status.py and its place in /api/v1/system/health."""
from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from store import publication_review_status as prs

NOW = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)   # expected newest day: 2026-11-11; stale before 11-09


class FakeConn:
    """Answers the table probe and the latest-run query; can raise either."""

    def __init__(self, present=True, row=(None, None, None), probe_error=False, query_error=False):
        self.present, self.row, self.probe_error, self.query_error = present, row, probe_error, query_error
        self.statements: list[str] = []

    def execute(self, clause):
        sql = str(clause)
        self.statements.append(sql)
        result = MagicMock()
        if "to_regclass" in sql:
            if self.probe_error:
                raise RuntimeError("probe down")
            result.scalar.return_value = self.present
        else:
            if self.query_error:
                raise RuntimeError("query down")
            result.fetchone.return_value = self.row
        return result


def _tree(tmp_path: Path, *days: str, receipt=True) -> Path:
    root = tmp_path / "public"
    root.mkdir(parents=True, exist_ok=True)
    for day in days:
        (root / day).mkdir()
        if receipt:
            (root / day / "receipt.json").write_text("{}")
    return root


def test_latest_packet_day_ignores_junk_and_days_without_a_receipt(tmp_path):
    root = _tree(tmp_path, "2026-11-04", "2026-11-07")
    (root / "2026-11-09").mkdir()                      # produced dir without receipt: not counted
    (root / "notes").mkdir()
    (root / "2026-13-40").mkdir()                      # invalid date
    assert prs.latest_packet_day(root) == date(2026, 11, 7)
    assert prs.latest_packet_day(tmp_path / "missing") is None
    assert prs.latest_packet_day(_tree(tmp_path / "e")) is None


def test_fresh_chain_is_not_degraded(tmp_path):
    root = _tree(tmp_path, "2026-11-10", "2026-11-11")
    conn = FakeConn(row=(date(2026, 11, 11), datetime(2026, 11, 12, 11, 36, tzinfo=timezone.utc),
                         datetime(2026, 11, 12, 11, 37, tzinfo=timezone.utc)))
    status = prs.review_chain_status(conn, public_root=root, now=NOW)
    assert status.degraded_reasons == []
    check = status.as_check()
    assert check == {"packet_root": str(root), "latest_packet_day": "2026-11-11", "latest_review_day": "2026-11-11",
                     "latest_review_graded_at": "2026-11-12T11:36:00Z", "latest_review_ingested_at": "2026-11-12T11:37:00Z",
                     "review_table_present": True, "stale": False}
    assert conn.statements[0].startswith("SELECT to_regclass")   # the probe runs before the query


def test_two_day_slack_then_stale(tmp_path):
    root = _tree(tmp_path, "2026-11-09")
    status = prs.review_chain_status(FakeConn(row=(date(2026, 11, 9), None, None)), public_root=root, now=NOW)
    assert status.degraded_reasons == []                         # 11-09 is exactly at the slack boundary
    root = _tree(tmp_path / "older", "2026-11-08")
    status = prs.review_chain_status(FakeConn(row=(date(2026, 11, 8), None, None)), public_root=root, now=NOW)
    assert status.degraded_reasons == ["publication packets stale: latest 2026-11-08, expected 2026-11-11",
                                       "publication review stale: latest 2026-11-08, expected 2026-11-11"]
    assert status.as_check()["stale"] is True


def test_empty_tree_and_empty_table_are_degraded_but_missing_ones_are_not(tmp_path):
    root = _tree(tmp_path)                                        # exists, no days
    status = prs.review_chain_status(FakeConn(row=(None, None, None)), public_root=root, now=NOW)
    assert status.degraded_reasons == ["publication packets: no day published yet", "publication review: no day ingested yet"]
    status = prs.review_chain_status(FakeConn(present=False), public_root=tmp_path / "absent", now=NOW)
    assert status.degraded_reasons == [] and status.review_table_present is False
    assert status.latest_packet_day is None and status.latest_review_day is None


def test_probe_and_query_failures_never_raise(tmp_path):
    status = prs.review_chain_status(FakeConn(probe_error=True), public_root=tmp_path / "absent", now=NOW)
    assert status.review_table_present is None and status.degraded_reasons == []
    status = prs.review_chain_status(FakeConn(query_error=True), public_root=tmp_path / "absent", now=NOW)
    assert status.degraded_reasons == ["publication review: latest-run query failed"]


def test_datetime_publication_day_and_naive_timestamps_are_normalised(tmp_path):
    conn = FakeConn(row=(datetime(2026, 11, 11, 0, 0), datetime(2026, 11, 12, 11, 36), None))
    status = prs.review_chain_status(conn, public_root=tmp_path / "absent", now=NOW)
    assert status.latest_review_day == "2026-11-11" and status.latest_review_graded_at == "2026-11-12T11:36:00Z"
    assert status.degraded_reasons == []


# ── through the API ─────────────────────────────────────────────────────────────────────

def _engine(row):
    engine = MagicMock()
    conn = MagicMock()

    def execute(clause):
        result = MagicMock()
        sql = str(clause)
        if "to_regclass" in sql:
            result.scalar.return_value = True
        elif "publication_review_runs" in sql:
            result.fetchone.return_value = row
        else:
            result.scalar.return_value = 1
            result.fetchone.return_value = (5,)
        return result

    conn.execute.side_effect = execute
    engine.connect.return_value.__enter__.return_value = conn
    engine.connect.return_value.__exit__.return_value = False
    engine.pool = MagicMock()
    engine.pool.size.return_value = 20
    engine.pool.checkedout.return_value = 1
    engine.pool.checkedin.return_value = 19
    engine.pool.overflow.return_value = -19
    engine.pool._max_overflow = 30
    return engine


@pytest.fixture
def client():
    from fastapi.testclient import TestClient

    from api.main import app
    return TestClient(app)


def test_health_reports_the_review_chain(client, tmp_path, monkeypatch):
    monkeypatch.setattr(prs, "DEFAULT_PUBLIC_ROOT", tmp_path / "absent")
    with patch("api.routers.system.get_db_engine", return_value=_engine((None, None, None))):
        data = client.get("/api/v1/system/health").json()
    check = data["checks"]["publication_review"]
    assert check["review_table_present"] is True and check["latest_review_day"] is None and check["stale"] is True
    assert "publication review: no day ingested yet" in data["degraded_reasons"]
    assert data["status"] == "degraded"
    fresh = (datetime.now(timezone.utc).date(), datetime.now(timezone.utc), datetime.now(timezone.utc))
    with patch("api.routers.system.get_db_engine", return_value=_engine(fresh)):
        data = client.get("/api/v1/system/health").json()
    assert data["checks"]["publication_review"]["stale"] is False
    assert not any("publication review" in r for r in data["degraded_reasons"])
    json.dumps(data)   # the check is JSON-serialisable end to end
