"""G5: one modeled SPY GEX row from the latest registered session capture.

Uses the existing DealerGammaEngine, including its exact-batch completeness
and verified prior unadjusted SPY-close gates. No capture, provider call,
fallback spot, scheduling, schema change or legacy evidence mutation occurs.
The callable is inert until invoked; activation is a separate controller gate.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text

from ingestion.market_calendar import is_market_open
from physics.dealer_gamma import DealerGammaEngine

MODEL_BASIS = "options_open_interest_with_assumed_dealer_sign_and_black_scholes"
SIGN_CONVENTION = "assumed long calls (+1), short puts (-1); not observed dealer positioning"
_ET = ZoneInfo("America/New_York")


class Refused(ValueError):
    def __init__(self, status: str, reason: str):
        super().__init__(reason)
        self.status, self.reason = status, reason


@dataclass(frozen=True)
class RowResult:
    obs_date: date
    status: str
    reason: str | None = None


@dataclass(frozen=True)
class MaterializationResult:
    status: str
    run_status: str
    rows_written: int | None
    rows_skipped: int | None
    rows: tuple[RowResult, ...]
    run_id: str
    code_sha: str
    ledger_recorded: bool | None
    message: str
    outcome: str = "confirmed"
    reconciled: bool = False


def _timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("aware timestamp required")
    return value.astimezone(timezone.utc)


def _number(value: Any, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("finite numeric value required")
    if positive and value <= 0:
        raise ValueError("positive numeric value required")
    return float(value)


def prepare_row(
    profile: Mapping[str, Any], batch: Mapping[str, Any], counts: Mapping[str, int],
    *, session_date: date, cutoff: datetime,
) -> dict[str, Any]:
    """Validate and map engine-native outputs to G2; never guess legacy units.

    Coverage describes rows the engine can use / registered rows, not provider
    universe coverage or measured dealer inventory. Completion is the release
    clock. Availability also respects registration and latest input creation.
    """
    if profile.get("error") or profile.get("available") is False:
        status = "no_verified_spot" if profile.get("status") == "unavailable" else "no_completed_capture"
        raise Refused(status, status)
    try:
        started, completed = (_timestamp(batch[k]) for k in ("capture_started_at", "capture_completed_at"))
        registered = _timestamp(batch["registered_at"])
        uuid.UUID(batch["capture_batch_id"])
        ordinal = batch["capture_ordinal"]
        if (batch["ticker"] != "SPY" or batch["snap_date"] != session_date
                or isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal <= 0
                or not started <= completed <= cutoff or registered > cutoff
                or any(ts.date() != session_date or ts.astimezone(_ET).date() != session_date
                       for ts in (started, completed))
                or counts["total"] != batch["row_count"] or counts["total"] <= 0
                or not 0 < counts["usable"] <= counts["total"] or counts["invalid"] != 0):
            raise ValueError("capture contract mismatch")
        expected = {"ticker": "SPY", "chain_snap_date": session_date.isoformat(),
                    "chain_batch_id": batch["capture_batch_id"], "chain_capture_ordinal": ordinal,
                    "estimated": True, "basis": MODEL_BASIS}
        if any(profile[k] != v for k, v in expected.items()) or profile["estimated"] is not True:
            raise ValueError("model contract mismatch")
        if (_timestamp(profile["chain_capture_started_at"]) != started
                or _timestamp(profile["chain_capture_completed_at"]) != completed):
            raise ValueError("mixed capture clocks")
        created_min = _timestamp(profile["chain_created_at"])
        created_max = _timestamp(profile["chain_created_at_max"])
        if not created_min <= created_max <= cutoff or any(
            ts.date() != session_date or ts.astimezone(_ET).date() != session_date
            for ts in (created_min, created_max)
        ):
            raise ValueError("unavailable row acquisition")
        quote_min = _timestamp(profile["chain_provider_regular_market_at_min"])
        quote_max = _timestamp(profile["chain_provider_regular_market_at_max"])
        if not quote_min <= quote_max <= completed or any(
            ts.date() != session_date or ts.astimezone(_ET).date() != session_date
            for ts in (quote_min, quote_max)
        ):
            raise Refused("inputs_stale", "quote_clock_outside_capture_session")
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        if isinstance(exc, Refused):
            raise
        raise Refused("no_completed_capture", "invalid_or_incomplete_registered_capture") from exc

    try:
        spot = _number(profile["spot"], positive=True)
        spot_date = date.fromisoformat(profile["spot_obs_date"])
        spot_available = _timestamp(profile["spot_available_at"])
        receipt_created = _timestamp(profile["spot_receipt_created_at"])
        receipt_id = profile["spot_receipt_id"]
        if (profile["spot_source"] != "spy_close_receipt"
                or profile["spot_basis"] != "prior_completed_unadjusted_close"
                or not 1 <= (session_date - spot_date).days <= 4
                or not datetime.combine(spot_date + timedelta(days=1), datetime.min.time(), timezone.utc)
                <= spot_available <= receipt_created <= completed
                or isinstance(receipt_id, bool) or not isinstance(receipt_id, int) or receipt_id <= 0
                or any(date.fromisoformat(profile[k]) > session_date
                       for k in ("spot_release_date", "spot_vintage_date"))):
            raise ValueError("unverified spot receipt")
    except (KeyError, TypeError, ValueError) as exc:
        raise Refused("no_verified_spot", "invalid_or_stale_verified_spot_receipt") from exc

    try:
        values = {k: _number(profile[k]) for k in ("gex_aggregate", "gex_normalized", "dealer_delta")}
        values["spot"] = spot
        for k in ("gamma_flip", "gamma_wall", "put_wall", "call_wall"):
            values[k] = None if profile[k] is None else _number(profile[k], positive=True)
        crossings = profile["gamma_flip_crossings"]
        if (isinstance(crossings, bool) or not isinstance(crossings, int) or crossings < 0
                or (values["gamma_flip"] is None) != (crossings == 0)
                or profile["regime"] not in {"LONG_GAMMA", "SHORT_GAMMA", "NEUTRAL"}):
            raise ValueError("invalid modeled output")
        values.update(gamma_flip_crossings=crossings, regime=profile["regime"])
    except (KeyError, TypeError, ValueError) as exc:
        raise Refused("inputs_missing", "invalid_modeled_output") from exc
    output_sha = hashlib.sha256(json.dumps(values, sort_keys=True, allow_nan=False).encode()).hexdigest()
    source_ref = {
        "contract": "godview_gex_v1", "capture_batch_id": batch["capture_batch_id"],
        "capture_ordinal": ordinal, "capture_started_at": started.isoformat(),
        "capture_completed_at": completed.isoformat(), "registered_at": registered.isoformat(),
        "capture_source": batch["capture_source"], "backfilled_registration": batch["backfilled"],
        "created_at_min": created_min.isoformat(), "created_at_max": created_max.isoformat(),
        "provider_regular_market_at_min": quote_min.isoformat(),
        "provider_regular_market_at_max": quote_max.isoformat(),
        "spot_receipt_id": receipt_id, "spot_obs_date": spot_date.isoformat(),
        "spot_available_at": spot_available.isoformat(), "spot_receipt_created_at": receipt_created.isoformat(),
        "spot_release_date": profile["spot_release_date"], "spot_vintage_date": profile["spot_vintage_date"],
        "coverage": {**dict(counts), "registered": batch["row_count"],
                     "basis": "usable_rows_over_registered_rows", "provider_universe_fraction": None},
        "model_basis": MODEL_BASIS, "sign_convention": SIGN_CONVENTION, "risk_free_rate": 0.05,
        "units": {"gex_aggregate": "engine_gamma_x_oi_x_100_x_spot",
                  "gex_normalized": "gex_aggregate_over_spot_x_1e6"},
        "output_sha256": output_sha,
    }
    return {**values, "obs_date": session_date, "ticker": "SPY", "provenance": "modeled",
            "model_basis": MODEL_BASIS, "sign_convention": SIGN_CONVENTION,
            "chain_capture_batch_id": batch["capture_batch_id"], "chain_capture_ordinal": ordinal,
            "chain_capture_started_at": started, "chain_capture_completed_at": completed,
            "spot_source": profile["spot_source"], "spot_basis": profile["spot_basis"],
            "spot_obs_date": spot_date, "spot_available_at": spot_available, "spot_receipt_id": receipt_id,
            "release_at": completed, "available_at": max(completed, registered, created_max, receipt_created),
            "availability_basis": "observed_acquisition", "coverage_fraction": counts["usable"] / counts["total"],
            "source_ref": json.dumps(source_ref, sort_keys=True, allow_nan=False)}


_BATCH_SQL = text("""
    SELECT capture_batch_id, ticker, snap_date, capture_ordinal, capture_started_at,
           capture_completed_at, registered_at, row_count, capture_source, backfilled
    FROM options_capture_batches
    WHERE ticker = 'SPY' AND snap_date = :day
      AND capture_completed_at <= :cutoff AND registered_at <= :cutoff
    ORDER BY capture_ordinal DESC LIMIT 1
""")
_COUNTS_SQL = text("""
    SELECT count(*) AS total,
           count(*) FILTER (WHERE expiry > :day AND open_interest > 0
                             AND implied_vol > 0 AND implied_vol < 'Infinity'::float8) AS usable,
           count(*) FILTER (WHERE opt_type IS NULL OR opt_type NOT IN ('call', 'put')
                             OR strike IS NULL OR NOT (strike > 0 AND strike < 'Infinity'::float8)
                             OR open_interest IS NULL OR open_interest < 0) AS invalid
    FROM options_snapshots_all WHERE capture_batch_id = :batch
""")
_EXISTING_SQL = text("""
    SELECT provenance, chain_capture_batch_id, chain_capture_ordinal, source_ref
    FROM dealer_gex_daily WHERE obs_date = :day AND ticker = 'SPY'
""")
_UPSERT_SQL = text("""
    INSERT INTO dealer_gex_daily (
        obs_date, ticker, spot, gex_aggregate, gex_normalized, gamma_flip, gamma_flip_crossings,
        regime, gamma_wall, put_wall, call_wall, dealer_delta, model_basis, sign_convention,
        chain_capture_batch_id, chain_capture_ordinal, chain_capture_started_at, chain_capture_completed_at,
        spot_source, spot_basis, spot_obs_date, spot_available_at, spot_receipt_id,
        release_at, available_at, availability_basis, provenance, coverage_fraction, source_ref,
        run_id, code_sha, updated_at
    ) VALUES (
        :obs_date, :ticker, :spot, :gex_aggregate, :gex_normalized, :gamma_flip, :gamma_flip_crossings,
        :regime, :gamma_wall, :put_wall, :call_wall, :dealer_delta, :model_basis, :sign_convention,
        :chain_capture_batch_id, :chain_capture_ordinal, :chain_capture_started_at, :chain_capture_completed_at,
        :spot_source, :spot_basis, :spot_obs_date, :spot_available_at, :spot_receipt_id,
        :release_at, :available_at, :availability_basis, 'modeled', :coverage_fraction, CAST(:source_ref AS JSONB),
        CAST(:run_id AS UUID), :code_sha, NOW()
    ) ON CONFLICT (obs_date, ticker) DO UPDATE SET
        spot = EXCLUDED.spot, gex_aggregate = EXCLUDED.gex_aggregate, gex_normalized = EXCLUDED.gex_normalized,
        gamma_flip = EXCLUDED.gamma_flip, gamma_flip_crossings = EXCLUDED.gamma_flip_crossings,
        regime = EXCLUDED.regime, gamma_wall = EXCLUDED.gamma_wall, put_wall = EXCLUDED.put_wall,
        call_wall = EXCLUDED.call_wall, dealer_delta = EXCLUDED.dealer_delta,
        model_basis = EXCLUDED.model_basis, sign_convention = EXCLUDED.sign_convention,
        chain_capture_batch_id = EXCLUDED.chain_capture_batch_id,
        chain_capture_ordinal = EXCLUDED.chain_capture_ordinal,
        chain_capture_started_at = EXCLUDED.chain_capture_started_at,
        chain_capture_completed_at = EXCLUDED.chain_capture_completed_at,
        spot_source = EXCLUDED.spot_source, spot_basis = EXCLUDED.spot_basis,
        spot_obs_date = EXCLUDED.spot_obs_date, spot_available_at = EXCLUDED.spot_available_at,
        spot_receipt_id = EXCLUDED.spot_receipt_id,
        release_at = EXCLUDED.release_at, available_at = EXCLUDED.available_at,
        availability_basis = EXCLUDED.availability_basis, source_ref = EXCLUDED.source_ref,
        coverage_fraction = EXCLUDED.coverage_fraction, run_id = EXCLUDED.run_id,
        code_sha = EXCLUDED.code_sha, updated_at = NOW()
    WHERE dealer_gex_daily.provenance = 'modeled'
      AND dealer_gex_daily.chain_capture_ordinal < EXCLUDED.chain_capture_ordinal
      AND dealer_gex_daily.chain_capture_completed_at <= EXCLUDED.chain_capture_completed_at
      AND dealer_gex_daily.available_at <= EXCLUDED.available_at
    RETURNING obs_date
""")
_RUN_SQL = text("""
    INSERT INTO godview_runs (run_id, pillar, started_at, finished_at, status,
                             rows_written, rows_skipped, reasons, input_watermarks, error, code_sha)
    VALUES (CAST(:run_id AS UUID), 'dealer_gex', :started_at, :finished_at, :status,
            :rows_written, :rows_skipped, CAST(:reasons AS JSONB), CAST(:input_watermarks AS JSONB), :error, :code_sha)
""")
_RUN_LOOKUP_SQL = text("""
    SELECT run_id, status, rows_written, rows_skipped, reasons, code_sha
    FROM godview_runs WHERE run_id = CAST(:run_id AS UUID) AND pillar = 'dealer_gex'
""")
_PUBLIC_STATUSES = {"complete": "SUCCESS", "noop": "SUCCESS_NOOP",
                    "partial_blocked_by_legacy": "PARTIAL_BLOCKED_BY_LEGACY", "failed": "FAILED"}
_TERMINAL_STATUSES = frozenset({*_PUBLIC_STATUSES, "inputs_missing", "inputs_stale", "non_session",
                               "no_completed_capture", "no_verified_spot"})


class _ConnectionEngine:
    """Let both existing engine reads share the writer's connection and snapshot.

    No new pool checkout, connection close, commit or rollback belongs to the
    computation. This also lets the existing runner roll back its dry run.
    """
    def __init__(self, conn: Any):
        self.conn = conn

    @contextmanager
    def connect(self) -> Iterator[Any]:
        yield self.conn


def _timeouts(conn: Any) -> None:
    conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text("SET LOCAL statement_timeout = '60s'"))


def _reconcile_run(engine: Any, run_id: str, day: date) -> MaterializationResult | None:
    """One bounded read-only lookup; absence/error cannot disprove an in-flight commit.

    A receipt read before this read-only transaction's exit fails still confirms
    the original publication. No writer, failed-run insert or retry runs here.
    """
    receipt = None
    try:
        with engine.begin() as conn:
            _timeouts(conn)
            conn.execute(text("SET TRANSACTION READ ONLY"))
            row = conn.execute(_RUN_LOOKUP_SQL, {"run_id": run_id}).mappings().first()
            receipt = dict(row) if row is not None else None
    except Exception:
        pass
    if receipt is None:
        return None
    try:
        state, written, skipped = (receipt[k] for k in ("status", "rows_written", "rows_skipped"))
        reasons = receipt["reasons"]
        reasons = json.loads(reasons) if isinstance(reasons, str) else reasons
        sha = receipt["code_sha"]
        if (str(receipt["run_id"]) != run_id or state not in _TERMINAL_STATUSES
                or type(written) is not int or written not in (0, 1)
                or type(skipped) is not int or skipped not in (0, 1)
                or not isinstance(reasons, dict) or len(reasons) > 1
                or not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{7,40}", sha)):
            return None
        row_state = "written" if written else "skipped" if skipped else "noop"
        reason = next(iter(reasons), None)
        return MaterializationResult(_PUBLIC_STATUSES.get(state, "EMPTY"), state, written, skipped,
                                     (RowResult(day, row_state, reason),), run_id, sha, True,
                                     reason or state, reconciled=True)
    except (KeyError, TypeError, ValueError):
        return None


def materialize_dealer_gex(
    engine: Any, *, code_sha: str, session_date: date | None = None,
    as_of_ts: datetime | None = None,
) -> MaterializationResult:
    """Publish at most one business row plus one terminal run in one transaction.

    An explicit earlier cutoff replays its own ET session. It cannot manufacture
    today's row from a prior session, or downgrade a later materialization. A
    body failure cannot commit and gets a bounded sanitized failed-run attempt.
    An exception after the terminal insert may be a lost COMMIT reply. Reconcile
    its exact run once; if unconfirmed, preserve unknown counts and run identity.
    Never automatically retry an uncertain publication or failure-ledger write.
    """
    if not isinstance(code_sha, str) or not re.fullmatch(r"[0-9a-f]{7,40}", code_sha):
        raise ValueError("GEX materialization requires a 7-40 character lowercase hex code_sha")
    started = datetime.now(timezone.utc)
    cutoff = _timestamp(as_of_ts) if as_of_ts is not None else started
    if cutoff > started:
        raise ValueError("as_of_ts cannot be in the future")
    day = session_date or cutoff.astimezone(_ET).date()
    if type(day) is not date:
        raise ValueError("session_date must be a date")
    run_id = str(uuid.uuid4())
    state, reason, written, row_state = "noop", None, 0, "noop"
    publication_ready = False
    watermarks = {"session_date": day.isoformat(), "cutoff": cutoff.isoformat()}

    def ledger(conn: Any, *, error: str | None = None) -> None:
        conn.execute(_RUN_SQL, {"run_id": run_id, "started_at": started,
                               "finished_at": datetime.now(timezone.utc), "status": state,
                               "rows_written": written, "rows_skipped": int(row_state == "skipped"),
                               "reasons": json.dumps({reason: 1} if reason else {}),
                               "input_watermarks": json.dumps(watermarks), "error": error, "code_sha": code_sha})

    def result(recorded: bool) -> MaterializationResult:
        public_status = _PUBLIC_STATUSES.get(state, "EMPTY")
        return MaterializationResult(public_status, state, written, int(row_state == "skipped"),
                                     (RowResult(day, row_state, reason),), run_id, code_sha, recorded,
                                     reason or state)

    def reconcile_or_unknown() -> MaterializationResult:
        reconciled = _reconcile_run(engine, run_id, day)
        if reconciled is not None:
            return reconciled
        return MaterializationResult("OUTCOME_UNKNOWN", "unknown", None, None,
                                     (RowResult(day, "unknown", "commit_outcome_unknown"),),
                                     run_id, code_sha, None, "commit_outcome_unknown", outcome="unknown")

    try:
        with engine.begin() as conn:
            _timeouts(conn)
            try:
                if not is_market_open(day):
                    raise Refused("non_session", "non_session")
                if day != cutoff.astimezone(_ET).date():
                    raise Refused("inputs_stale", "target_outside_cutoff_session")
                batch = conn.execute(_BATCH_SQL, {"day": day, "cutoff": cutoff}).mappings().first()
                if batch is None:
                    raise Refused("no_completed_capture", "no_completed_capture")
                watermarks.update(capture_batch_id=batch["capture_batch_id"],
                                  capture_ordinal=batch["capture_ordinal"])
                counts = conn.execute(_COUNTS_SQL, {"day": day, "batch": batch["capture_batch_id"]}).mappings().first()
                if counts["total"] != batch["row_count"] or counts["usable"] == 0 or counts["invalid"]:
                    raise Refused("no_completed_capture", "invalid_or_incomplete_registered_capture")
                profile = DealerGammaEngine(_ConnectionEngine(conn)).compute_gex_profile(
                    "SPY", day, capture_batch_id=batch["capture_batch_id"])
                row = prepare_row(profile, batch, counts, session_date=day, cutoff=cutoff)
                watermarks.update(json.loads(row["source_ref"]))
                if conn.execute(_UPSERT_SQL, {**row, "run_id": run_id, "code_sha": code_sha}).scalar() is not None:
                    state, written, row_state = "complete", 1, "written"
                else:
                    existing = conn.execute(_EXISTING_SQL, {"day": day}).mappings().first()
                    if existing is None:
                        raise RuntimeError("upsert did not publish or find a conflicting row")
                    if existing["provenance"] is None:
                        raise Refused("partial_blocked_by_legacy", "blocked_by_legacy_row_requires_a2_archive")
                    if existing["chain_capture_batch_id"] == row["chain_capture_batch_id"]:
                        ref = existing["source_ref"]
                        ref = json.loads(ref) if isinstance(ref, str) else ref
                        if not ref or ref.get("output_sha256") != watermarks["output_sha256"]:
                            raise Refused("inputs_missing", "immutable_batch_model_conflict")
                    elif existing["chain_capture_ordinal"] <= row["chain_capture_ordinal"]:
                        raise Refused("inputs_stale", "nonmonotonic_capture_clocks")
                    else:
                        reason = "newer_materialization_exists"
            except Refused as exc:
                state, reason, written, row_state = exc.status, exc.reason, 0, "skipped"
            ledger(conn)
            publication_ready = True
        return result(True)
    except Exception as exc:
        if publication_ready:
            return reconcile_or_unknown()
        # Never retain raw SQL/connection/provider errors in durable notes.
        state, reason, written, row_state = "failed", "materialization_failed", 0, "skipped"
        failed_ledger_ready = False
        try:
            with engine.begin() as conn:
                _timeouts(conn)
                ledger(conn, error=type(exc).__name__)
                failed_ledger_ready = True
        except Exception:
            if failed_ledger_ready:
                return reconcile_or_unknown()
            return result(False)
        return result(True)
