"""FRED write boundaries, rollback fallback and acknowledged-count contracts."""
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import exc as sa_exc

from ingestion import fred


def db_error(state, cls=sa_exc.OperationalError, invalidated=False):
    return cls("fixture statement", {}, SimpleNamespace(pgcode=state),
               connection_invalidated=invalidated)


class FakeEngine:
    """Models visibility and transaction outcomes rather than a MagicMock count."""
    def __init__(self):
        self.rows = []
        self.transactions = []
        self.active = False
        self.acquisitions = 0
        self.bad_date = None
        self.statement_error = None
        self.statement_error_once = False
        self.commit_error_at = None
        self.commit_error = None
        self.commit_before_error = False
        self.acquire_error_at = None
        self.rollback_error = False
        self.close_error_at = None
        self.commit_attempts = 0

    def connect(self):
        self.acquisitions += 1
        if self.acquisitions == self.acquire_error_at:
            raise sa_exc.TimeoutError("fixture acquisition")
        return FakeConnection(self)


class FakeConnection:
    def __init__(self, engine):
        self.engine = engine
        self.pending = []
        self.attempts = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def begin(self):
        assert not self.engine.active
        self.engine.active = True
        self.engine.transactions.append(self)
        return self

    def execute(self, sql, params):
        sql = str(sql)
        if "SELECT DISTINCT" in sql:
            assert "pull_status = 'SUCCESS'" in sql
            rows = self.engine.rows + self.pending
            return SimpleNamespace(fetchall=lambda: [(r["od"],) for r in rows
                if r["sid"] == params["sid"] and r.get("status", "SUCCESS") == "SUCCESS"
                and params.get("start_date", date.min) <= r["od"] <= params.get("end_date", date.max)])
        if "SELECT MAX" in sql:
            days = [r["od"] for r in self.engine.rows if r["sid"] == params["sid"]
                    and r.get("status", "SUCCESS") == "SUCCESS"]
            return SimpleNamespace(fetchone=lambda: (max(days) if days else None,))
        assert "INSERT INTO raw_series" in sql and self.engine.active
        assert "SAVEPOINT" not in sql and "pull_timestamp" not in sql
        self.attempts += 1
        if params["od"] == self.engine.bad_date:
            raise db_error("23514", sa_exc.IntegrityError)
        if self.engine.statement_error:
            error = self.engine.statement_error
            if self.engine.statement_error_once:
                self.engine.statement_error = None
            raise error
        point = dict(params)
        if "'FAILED'" in sql:
            point["status"] = "FAILED"
        self.pending.append(point)
        return SimpleNamespace(rowcount=1)

    def commit(self):
        self.engine.commit_attempts += 1
        failed = self.engine.commit_attempts == self.engine.commit_error_at
        if not failed or self.engine.commit_before_error:
            self.engine.rows.extend(self.pending)
        self.engine.active = False
        self.outcome = "COMMIT_UNKNOWN" if failed else "COMMIT"
        if failed:
            raise self.engine.commit_error

    def rollback(self):
        self.engine.active = False
        self.outcome = "ROLLBACK"
        if self.engine.rollback_error:
            raise RuntimeError("fixture rollback acknowledgement lost")
        self.pending.clear()

    def close(self):
        if self.engine.acquisitions == self.engine.close_error_at:
            raise RuntimeError("fixture close failure")


def make_puller(monkeypatch, count=121, engine=None):
    engine = engine or FakeEngine()
    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller.engine, puller.source_id = engine, 1
    frame = pd.DataFrame({"value": list(range(count))}, index=pd.date_range("2026-01-01", periods=count))
    calls = []

    def fetch(sid, **kwargs):
        assert not engine.active, "provider called inside write transaction"
        calls.append((sid, kwargs))
        return frame.copy()

    puller.fred = SimpleNamespace(get_series_observations=fetch)
    monkeypatch.setattr(fred.time, "sleep", lambda *_args: None)
    return puller, engine, frame, calls


def test_cold_pull_commits_fifty_fifty_twenty_one_and_prepares_first(monkeypatch):
    puller, engine, _frame, calls = make_puller(monkeypatch)
    normalise = fred._normalise_observation_frame

    def checked_normalise(*args):
        assert engine.acquisitions == 0 and not engine.active
        return normalise(*args)

    monkeypatch.setattr(fred, "_normalise_observation_frame", checked_normalise)
    out = puller.pull_series("DFF")
    assert calls == [("DFF", {"observation_start": "1990-01-01"})]
    assert (out["status"], out["rows_inserted"], out["rows_failed"]) == ("SUCCESS", 121, 0)
    assert [t.attempts for t in engine.transactions] == [50, 50, 21]
    assert all(t.outcome == "COMMIT" for t in engine.transactions)


def test_failed_middle_point_rolls_back_then_commits_other_points_and_metadata(monkeypatch):
    puller, engine, frame, _calls = make_puller(monkeypatch)
    engine.bad_date = frame.index[75].date()
    out = puller.pull_series("DFF")
    assert (out["status"], out["rows_inserted"], out["rows_failed"]) == ("PARTIAL", 120, 1)
    assert len(engine.rows) == 121  # 120 successful observations plus 1 failure record
    assert sum(r.get("status") == "FAILED" for r in engine.rows) == 1
    assert engine.transactions[1].outcome == "ROLLBACK"
    assert max(t.attempts for t in engine.transactions) <= 50
    assert engine.transactions[-1].attempts == 1


def test_rerun_skips_revisions_and_failed_rows_do_not_block_observations(monkeypatch):
    puller, engine, frame, _calls = make_puller(monkeypatch, 3)
    engine.rows.append({"sid": "DFF", "od": frame.index[0].date(), "val": 0, "status": "FAILED"})
    assert puller.pull_series("DFF")["rows_inserted"] == 3
    values = [r["val"] for r in engine.rows if r.get("status") != "FAILED"]
    frame["value"] += 99
    again = puller.pull_series("DFF")
    assert (again["status"], again["rows_inserted"]) == ("SUCCESS", 0)
    assert [r["val"] for r in engine.rows if r.get("status") != "FAILED"] == values


@pytest.mark.parametrize("error", [RuntimeError("lost ACK"), db_error("08006"), db_error(None)])
@pytest.mark.parametrize("commit_at", [1, 2])
def test_unknown_commit_preserves_lower_bound_and_pull_all_stops(monkeypatch, error, commit_at):
    puller, engine, _frame, calls = make_puller(monkeypatch)
    engine.commit_error_at, engine.commit_error = commit_at, error
    engine.commit_before_error = True
    results = puller.pull_all(["DFF", "UNRATE"])
    out = results[0]
    assert (out["status"], out["rows_inserted"]) == ("PARTIAL" if commit_at == 2 else "FAILED", 50 * (commit_at - 1))
    assert out["commit_outcome_unknown"] and out["rows_inserted_total"] is None
    assert results[1]["status"] == "SKIPPED" and results[1]["aborted"]
    assert len(calls) == 1 and len(engine.transactions) == commit_at
    assert len(engine.rows) == 50 * commit_at  # fake server committed one unACKed batch


@pytest.mark.parametrize("error", [db_error("08003"), db_error(None),
                                   db_error("23514", invalidated=True), sa_exc.DisconnectionError(),
                                   db_error("57P01")])
def test_statement_connection_failure_has_no_fallback_or_metadata(monkeypatch, error):
    puller, engine, _frame, _calls = make_puller(monkeypatch)
    engine.statement_error = error
    out = puller.pull_series("DFF")
    assert out["status"] == "FAILED" and out["rows_inserted"] == 0 and out["aborted"]
    assert not out.get("commit_outcome_unknown")
    assert len(engine.transactions) == 1 and not engine.rows


def test_acquisition_failure_after_commit_halts_without_replaying(monkeypatch):
    puller, engine, _frame, _calls = make_puller(monkeypatch)
    engine.acquire_error_at = 2
    out = puller.pull_series("DFF")
    assert (out["status"], out["rows_inserted"]) == ("PARTIAL", 50)
    assert out["aborted"] and len(engine.transactions) == 1


@pytest.mark.parametrize("state", ["40001", "40P01", "55P03", "57014"])
def test_answered_usable_sqlstate_rolls_back_and_falls_back(monkeypatch, state):
    puller, engine, _frame, _calls = make_puller(monkeypatch, 3)
    engine.statement_error, engine.statement_error_once = db_error(state), True
    out = puller.pull_series("DFF")
    assert (out["status"], out["rows_inserted"]) == ("SUCCESS", 3)
    assert engine.transactions[0].outcome == "ROLLBACK"
    assert [t.attempts for t in engine.transactions] == [1, 1, 1, 1]


def test_answered_commit_rejection_rolls_back_before_fallback(monkeypatch):
    puller, engine, _frame, _calls = make_puller(monkeypatch, 3)
    engine.commit_error_at, engine.commit_error = 1, db_error("40001")
    out = puller.pull_series("DFF")
    assert out["rows_inserted"] == 3 and out["status"] == "SUCCESS"
    assert engine.transactions[0].outcome == "ROLLBACK"
    assert len(engine.rows) == 3


def test_unacknowledged_rollback_stops_without_point_replay(monkeypatch):
    puller, engine, _frame, _calls = make_puller(monkeypatch, 3)
    engine.statement_error, engine.rollback_error = db_error("23514", sa_exc.IntegrityError), True
    out = puller.pull_series("DFF")
    assert out["aborted"] and len(engine.transactions) == 1


def test_unknown_commit_during_fallback_preserves_prior_ack_and_failed_count(monkeypatch):
    puller, engine, frame, _calls = make_puller(monkeypatch, 4)
    engine.bad_date = frame.index[1].date()
    engine.commit_error_at, engine.commit_error = 2, RuntimeError("lost point ACK")
    engine.commit_before_error = True
    out = puller.pull_series("DFF")
    assert out["commit_outcome_unknown"] and out["rows_inserted_total"] is None
    assert (out["status"], out["rows_inserted"], out["rows_failed"]) == ("PARTIAL", 1, 1)
    assert len(engine.rows) == 2 and len(engine.transactions) == 4


def test_all_points_failed_status_and_separate_failure_metadata(monkeypatch):
    puller, engine, _frame, _calls = make_puller(monkeypatch, 3)
    original = FakeConnection.execute

    def reject_success(self, sql, params):
        if "'SUCCESS'" in str(sql) and "INSERT" in str(sql):
            self.attempts += 1
            raise db_error("23514", sa_exc.IntegrityError)
        return original(self, sql, params)

    monkeypatch.setattr(FakeConnection, "execute", reject_success)
    out = puller.pull_series("DFF")
    assert (out["status"], out["rows_inserted"], out["rows_failed"]) == ("FAILED", 0, 3)
    assert len(engine.rows) == 1 and engine.rows[0]["status"] == "FAILED"
    assert all(t.attempts == 1 for t in engine.transactions)


def test_failure_record_ack_then_close_failure_is_not_counted_as_success(monkeypatch):
    puller, engine, frame, _calls = make_puller(monkeypatch, 1)
    engine.bad_date = frame.index[0].date()
    # Failed batch, failed individual point, then one FAILED metadata insert.
    engine.close_error_at = 3
    out = puller.pull_series("DFF")
    assert (out["status"], out["rows_inserted"], out["rows_failed"]) == ("FAILED", 0, 1)
    assert out["aborted"] and len(engine.rows) == 1 and engine.rows[0]["status"] == "FAILED"


def test_close_failure_after_ack_keeps_confirmed_rows(monkeypatch):
    puller, engine, _frame, _calls = make_puller(monkeypatch, 3)
    engine.close_error_at = 1
    out = puller.pull_series("DFF")
    assert out["aborted"] and out["rows_inserted"] == 3
    assert out["status"] == "PARTIAL" and not out.get("commit_outcome_unknown")


def test_latest_read_failure_stops_before_provider_and_preserves_result_list(monkeypatch):
    puller, engine, _frame, calls = make_puller(monkeypatch)
    engine.acquire_error_at = 1
    out = puller.pull_all(["DFF", "UNRATE"])
    assert [r["status"] for r in out] == ["FAILED", "SKIPPED"]
    assert all(r["aborted"] and r["rows_inserted"] == 0 for r in out)
    assert not calls and not engine.transactions


def test_oversized_direct_batch_refused_before_connection(monkeypatch):
    puller, engine, frame, _calls = make_puller(monkeypatch)
    with pytest.raises(ValueError, match="exceeds 50"):
        puller._store_batch("DFF", [(d.date(), 1.0) for d in frame.index[:51]])
    assert engine.acquisitions == 0
