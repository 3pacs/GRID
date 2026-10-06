"""Offline source-to-row and transaction tests. All market inputs are synthetic."""
from __future__ import annotations

import copy
import json
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone

import pytest

from godview import dealer_gex as dg

DAY = date(2026, 9, 29)
SHA = "a" * 40
BATCH_ID = "00000000-0000-4000-8000-000000000001"
START = datetime(2026, 9, 29, 20, 0, tzinfo=timezone.utc)
COMPLETE = START + timedelta(minutes=1)
CUTOFF = START + timedelta(minutes=10)


def inputs():
    batch = dict(capture_batch_id=BATCH_ID, capture_ordinal=1, snap_date=DAY,
                 ticker="SPY", capture_started_at=START, capture_completed_at=COMPLETE,
                 registered_at=COMPLETE + timedelta(seconds=1), row_count=4,
                 capture_source="synthetic_test", backfilled=False)
    counts = dict(total=4, usable=4, invalid=0)
    profile = dict(ticker="SPY", chain_snap_date=str(DAY), chain_batch_id=BATCH_ID,
                   chain_capture_ordinal=1, chain_capture_started_at=START.isoformat(),
                   chain_capture_completed_at=COMPLETE.isoformat(),
                   chain_created_at=START.isoformat(), chain_created_at_max=COMPLETE.isoformat(),
                   chain_provider_regular_market_at_min=START.isoformat(),
                   chain_provider_regular_market_at_max=START.isoformat(),
                   spot=600.0, spot_source="spy_close_receipt",
                   spot_basis="prior_completed_unadjusted_close", spot_obs_date="2026-09-28",
                   spot_available_at=START.isoformat(), spot_receipt_created_at=START.isoformat(),
                   spot_release_date=str(DAY), spot_vintage_date=str(DAY), spot_receipt_id=11,
                   estimated=True, basis=dg.MODEL_BASIS, gex_aggregate=0.0, gex_normalized=0.0,
                   gamma_flip=None, gamma_flip_crossings=0, regime="NEUTRAL",
                   gamma_wall=600.0, put_wall=600.0, call_wall=600.0, dealer_delta=0.0)
    return profile, batch, counts


def prepare(profile=None, batch=None, counts=None):
    p, b, c = inputs()
    return dg.prepare_row(profile or p, batch or b, counts or c, session_date=DAY, cutoff=CUTOFF)


def test_zero_no_crossing_modeled_units_and_three_distinct_clocks():
    row = prepare()
    assert row["gex_aggregate"] == 0 and row["gamma_flip"] is None
    assert row["provenance"] == "modeled" and row["availability_basis"] == "observed_acquisition"
    assert row["release_at"] == COMPLETE
    assert row["available_at"] == COMPLETE + timedelta(seconds=1)
    ref = json.loads(row["source_ref"])
    assert ref["units"]["gex_aggregate"] == "engine_gamma_x_oi_x_100_x_spot"
    assert ref["spot_receipt_id"] == 11 and ref["capture_batch_id"] == BATCH_ID
    assert not {"net_gex_usd_m", "max_pain_strike", "atm_iv"} & row.keys()


@pytest.mark.parametrize("field,value", [
    ("estimated", False), ("ticker", "QQQ"), ("basis", "observed_inventory"),
    ("chain_batch_id", "00000000-0000-4000-8000-000000000002"),
    ("chain_capture_ordinal", 2), ("chain_snap_date", "2026-09-28"),
    ("chain_capture_started_at", "2026-09-29T00:01:00+00:00"),
    ("chain_created_at_max", "2026-09-29T21:00:00+00:00"),
    ("chain_provider_regular_market_at_max", "2026-09-29T20:02:00+00:00"),
    ("gex_aggregate", float("nan")), ("gex_normalized", float("inf")),
    ("spot", 0), ("spot", True), ("gamma_flip_crossings", True),
    ("gamma_flip", 601), ("regime", "unknown"),
])
def test_unproven_or_malformed_model_is_refused(field, value):
    profile, batch, counts = inputs()
    profile[field] = value
    with pytest.raises(dg.Refused):
        prepare(profile, batch, counts)


@pytest.mark.parametrize("field,value", [
    ("spot_source", "yfinance"), ("spot_basis", "adjusted"),
    ("spot_obs_date", "2026-09-29"), ("spot_obs_date", "2026-09-24"),
    ("spot_available_at", "2026-09-29T20:02:00+00:00"),
    ("spot_receipt_created_at", "2026-09-29T19:59:00+00:00"),
    ("spot_receipt_id", None), ("spot_release_date", "2026-09-30"),
])
def test_spot_lineage_is_never_substituted(field, value):
    profile, batch, counts = inputs()
    profile[field] = value
    with pytest.raises(dg.Refused) as exc:
        prepare(profile, batch, counts)
    assert exc.value.status == "no_verified_spot"


def test_complete_storage_and_model_coverage_are_distinct():
    p, b, c = inputs()
    c["total"] = 3
    with pytest.raises(dg.Refused):
        prepare(p, b, c)
    c.update(total=4, usable=2)
    row = prepare(p, b, c)
    assert row["coverage_fraction"] == 0.5
    assert json.loads(row["source_ref"])["coverage"]["basis"] == "usable_rows_over_registered_rows"


class Result:
    def __init__(self, value=None):
        self.value = value

    def mappings(self):
        return self

    def first(self):
        return self.value

    def scalar(self):
        return self.value


class MemoryEngine:
    """Transactional fake for recovery ordering; PG tests cover actual SQL guards."""
    def __init__(self):
        self.batch = inputs()[1]
        self.counts = inputs()[2]
        self.row = None
        self.runs = []
        self.calls = []
        self.fail = None
        self.rollbacks = 0
        self.commit_fault = None
        self.reconcile_fails = False

    @contextmanager
    def begin(self):
        saved = copy.deepcopy((self.row, self.runs))
        try:
            yield self
        except BaseException:
            self.row, self.runs = saved
            self.rollbacks += 1
            raise
        if self.commit_fault is not None:
            fault, self.commit_fault = self.commit_fault, None
            if fault == "rollback":
                self.row, self.runs = saved
                self.rollbacks += 1
            raise RuntimeError("synthetic uncertain COMMIT acknowledgement")

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.calls.append(sql)
        if "FROM options_capture_batches" in sql:
            return Result(self.batch)
        if "FROM options_snapshots_all" in sql:
            return Result(self.counts)
        if "INSERT INTO dealer_gex_daily" in sql:
            if self.fail == "upsert":
                self.fail = None
                raise RuntimeError("synthetic upsert failure")
            if self.row is None or (self.row["provenance"] is not None and
                                    params["chain_capture_ordinal"] > self.row["chain_capture_ordinal"]):
                self.row = copy.deepcopy(params)
                self.row["provenance"] = "modeled"
                return Result(params["obs_date"])
            return Result()
        if "FROM dealer_gex_daily" in sql:
            return Result(self.row)
        if "FROM godview_runs" in sql:
            if self.reconcile_fails:
                raise RuntimeError("synthetic reconciliation unavailable")
            return Result(next((copy.deepcopy(row) for row in self.runs
                                if row["run_id"] == params["run_id"]), None))
        if "INSERT INTO godview_runs" in sql:
            if self.fail in {"ledger_once", "ledger_always"}:
                if self.fail == "ledger_once":
                    self.fail = None
                raise RuntimeError("synthetic ledger failure")
            if any(row["run_id"] == params["run_id"] for row in self.runs):
                raise RuntimeError("synthetic duplicate ledger primary key")
            self.runs.append(copy.deepcopy(params))
        return Result()


@pytest.fixture
def engine(monkeypatch):
    eng = MemoryEngine()

    class Gamma:
        def __init__(self, read_engine):
            self.engine = read_engine

        def compute_gex_profile(self, ticker, day, *, capture_batch_id):
            with self.engine.connect() as conn:
                assert conn is eng  # no second pool checkout or independent transaction
            assert ticker == "SPY" and day == DAY and capture_batch_id == eng.batch["capture_batch_id"]
            profile = inputs()[0]
            for key, source in (("chain_batch_id", "capture_batch_id"),
                                ("chain_capture_ordinal", "capture_ordinal")):
                profile[key] = eng.batch[source]
            return profile

    monkeypatch.setattr(dg, "DealerGammaEngine", Gamma)
    return eng


def run(engine, **kwargs):
    return dg.materialize_dealer_gex(engine, code_sha=SHA, as_of_ts=CUTOFF, **kwargs)


def test_idempotence_new_batch_and_late_older_replay(engine):
    first = run(engine)
    saved = copy.deepcopy(engine.row)
    second = run(engine)
    assert first.rows_written == 1 and second.run_status == "noop"
    assert engine.row == saved and len(engine.runs) == 2
    engine.batch["capture_ordinal"] = 2
    engine.batch["capture_batch_id"] = "00000000-0000-4000-8000-000000000002"
    assert run(engine).rows_written == 1
    engine.batch = inputs()[1]
    assert run(engine).rows_written == 0
    assert engine.row["chain_capture_ordinal"] == 2


def test_legacy_collision_preserved(engine):
    engine.row = dict(provenance=None, original="evidence")
    result = run(engine)
    assert result.run_status == "partial_blocked_by_legacy" and result.rows_written == 0
    assert engine.row == dict(provenance=None, original="evidence")


@pytest.mark.parametrize("fail", ["upsert", "ledger_once"])
def test_failure_rolls_back_row_then_records_failure_and_recovery(engine, fail):
    engine.fail = fail
    failed = run(engine)
    assert failed.status == "FAILED" and failed.ledger_recorded
    assert engine.row is None and engine.rollbacks == 1
    assert engine.runs[-1]["status"] == "failed"
    assert engine.runs[-1]["rows_written"] == 0
    assert run(engine).rows_written == 1


def test_unrecorded_failure_is_explicit(engine):
    engine.fail = "ledger_always"
    result = run(engine)
    assert result.status == "FAILED" and result.ledger_recorded is False
    assert engine.row is None and engine.runs == []


@pytest.mark.parametrize("day,status", [(date(2026, 9, 26), "non_session"),
                                       (date(2026, 9, 28), "inputs_stale")])
def test_non_session_and_stale_target_never_compute(engine, day, status):
    result = run(engine, session_date=day)
    assert result.run_status == status and engine.row is None
    assert not any("options_capture_batches" in s for s in engine.calls)


def test_no_completed_capture_is_terminal_without_business_row(engine):
    engine.batch = None
    assert run(engine).run_status == "no_completed_capture"
    assert engine.row is None and engine.runs[-1]["rows_skipped"] == 1


def test_single_batch_model_conflict_is_refused(engine):
    run(engine)
    engine.row["source_ref"] = json.dumps({"output_sha256": "conflicting_output"})
    result = run(engine)
    assert result.run_status == "inputs_missing"
    assert result.rows[0].reason == "immutable_batch_model_conflict"


def test_missing_or_invalid_code_sha_is_refused_before_transaction(engine):
    with pytest.raises(ValueError):
        dg.materialize_dealer_gex(engine, code_sha="")
    assert engine.calls == []


def test_durable_commit_with_lost_reply_reconciles_exact_original_run(engine):
    engine.commit_fault = "durable_reply_lost"
    result = run(engine)
    assert result.status == "SUCCESS" and result.rows_written == 1
    assert result.ledger_recorded is True and result.reconciled is True
    assert result.outcome == "confirmed"
    assert result.run_id == engine.row["run_id"] == engine.runs[0]["run_id"]
    assert result.code_sha == engine.row["code_sha"] == engine.runs[0]["code_sha"]
    assert len(engine.runs) == 1
    assert engine.calls.count("SET TRANSACTION READ ONLY") == 1
    assert sum("FROM godview_runs" in query for query in engine.calls) == 1


def test_actual_commit_rollback_without_visible_run_stays_unknown_until_authorized_retry(engine):
    engine.commit_fault = "rollback"
    result = run(engine)
    assert engine.row is None and engine.runs == [] and engine.rollbacks == 1
    assert result.status == "OUTCOME_UNKNOWN" and result.outcome == "unknown"
    assert result.rows_written is None and result.rows_skipped is None
    assert result.ledger_recorded is None and result.run_id
    # Absence at one read is not proof that an uncertain server commit finished.
    assert sum("INSERT INTO godview_runs" in query for query in engine.calls) == 1
    retried = run(engine)  # separate, explicitly requested invocation
    assert retried.status == "SUCCESS" and retried.run_id != result.run_id
    assert engine.row["run_id"] == retried.run_id and len(engine.runs) == 1


def test_unavailable_reconciliation_never_asserts_zero_or_retries_uncertain_write(engine):
    engine.commit_fault = "durable_reply_lost"
    engine.reconcile_fails = True
    result = run(engine)
    assert engine.row is not None and len(engine.runs) == 1
    assert result.status == "OUTCOME_UNKNOWN" and result.run_status == "unknown"
    assert result.rows_written is None and result.rows_skipped is None and result.ledger_recorded is None
    assert result.run_id == engine.runs[0]["run_id"]
    assert result.message == "commit_outcome_unknown"
    assert sum("INSERT INTO godview_runs" in query for query in engine.calls) == 1
    assert sum("FROM godview_runs" in query for query in engine.calls) == 1


def test_separately_requested_replay_after_unknown_durable_commit_preserves_original_identity(engine):
    engine.commit_fault = "durable_reply_lost"
    engine.reconcile_fails = True
    uncertain = run(engine)
    saved = copy.deepcopy(engine.row)
    retried = run(engine)
    assert uncertain.status == "OUTCOME_UNKNOWN" and retried.run_status == "noop"
    assert engine.row == saved and engine.row["run_id"] == uncertain.run_id
    assert len(engine.runs) == 2 and engine.runs[1]["status"] == "noop"


def test_failed_run_commit_reply_loss_is_reconciled_without_duplicate_failure_insert(engine):
    engine.fail = "upsert"  # known body failure; business row rolled back
    engine.commit_fault = "durable_reply_lost"  # failed-run COMMIT only
    result = run(engine)
    assert result.status == "FAILED" and result.rows_written == 0
    assert result.ledger_recorded is True and result.reconciled is True
    assert engine.row is None and len(engine.runs) == 1
    assert engine.runs[0]["status"] == "failed"


def test_reconciliation_commit_reply_loss_retains_already_read_durable_receipt(engine):
    engine.commit_fault = "durable_reply_lost"
    original_execute = engine.execute

    def execute(stmt, params=None):
        receipt = original_execute(stmt, params)
        if "FROM godview_runs" in str(stmt):
            engine.commit_fault = "durable_reply_lost"  # read-only transaction exit
        return receipt

    engine.execute = execute
    result = run(engine)
    assert result.status == "SUCCESS" and result.reconciled is True
    assert result.rows_written == 1 and result.ledger_recorded is True
