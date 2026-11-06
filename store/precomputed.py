"""Precomputed API results: one row per (endpoint key, parameter set).

Heavy read endpoints (regime analogs, the globe, the association matrices,
the 100x options scan, ...) used to compute inline on every request and ran
for 15 s to well over Cloudflare's 125 s limit. Their computation now runs in
the background (``intelligence/api_precompute.py``, ticked from the
intelligence scheduler) and lands here; the endpoint reads the latest row by
primary key and never computes inline.

Table ``api_precomputed_results`` (migration ``api_precomputed_results_20261106``):

* ``key`` / ``params_hash`` -- the endpoint and the canonical hash of the
  parameter set it was computed for (:func:`params_hash`). Primary key.
* ``payload`` -- the endpoint's JSON response body, exactly as it would have
  been returned inline. ``NULL`` until the first successful run.
* ``computed_at`` -- when the job finished computing ``payload``.
* ``as_of`` -- the data date the payload describes (latest observation it
  used), never the wall clock unless the payload *is* a wall-clock snapshot
  of system state. ``NULL`` when the builder could not establish one.
* ``version`` -- the job's payload version; bumping it invalidates old rows.
* ``inputs`` -- small provenance dict (input hash, row counts, window).
* ``last_attempt_at`` / ``last_status`` / ``last_error`` -- the most recent
  attempt, successful or not. A failed or ``unavailable`` attempt never
  overwrites a good payload (AVAILABILITY_CONTRACT: unavailable results are
  served, not persisted as observations); the old payload keeps being served
  with its true ``computed_at`` and is labelled stale once it ages out.

Freshness is labelled on every response (:func:`freshness_meta`): a payload is
never presented as fresher than its ``computed_at`` / ``as_of``.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

TABLE = "api_precomputed_results"

STATUS_OK = "ok"
STATUS_ERROR = "error"
STATUS_UNAVAILABLE = "unavailable"


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------


def _jsonable(obj: Any) -> Any:
    """Recursively convert to JSON-safe values. NaN/inf become ``None``.

    ``json.dumps`` writes ``NaN`` for float NaN (it never reaches ``default``)
    and PostgreSQL's JSONB rejects it, so floats are checked explicitly.
    """
    if obj is None or isinstance(obj, (str, bool, int)):
        return obj
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, Mapping):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    # numpy / pandas scalars and arrays without importing them here
    if hasattr(obj, "tolist"):
        return _jsonable(obj.tolist())
    if hasattr(obj, "item"):
        try:
            return _jsonable(obj.item())
        except (TypeError, ValueError):
            pass
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    return str(obj)


def dumps(obj: Any) -> str:
    """Serialise ``obj`` for a JSONB column (NaN-free, numpy-safe)."""
    return json.dumps(_jsonable(obj), separators=(",", ":"), allow_nan=False)


def params_hash(params: Mapping[str, Any] | None) -> str:
    """Canonical hash of a parameter set (sorted keys, normalised values)."""
    canonical = json.dumps(_jsonable(dict(params or {})), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]


def _as_utc(ts: Any) -> datetime | None:
    if ts is None:
        return None
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    if isinstance(ts, datetime):
        return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
    if isinstance(ts, date):
        return datetime(ts.year, ts.month, ts.day, tzinfo=timezone.utc)
    return None


def _as_of_value(as_of: date | datetime | None) -> datetime | None:
    """Store a date as midnight UTC; keep a datetime's own instant."""
    return _as_utc(as_of)


def _iso_as_of(ts: datetime | None) -> str | None:
    """Render ``as_of``: a midnight-UTC value was a date, so print a date."""
    if ts is None:
        return None
    ts = _as_utc(ts)
    assert ts is not None
    if ts.hour == ts.minute == ts.second == ts.microsecond == 0:
        return ts.date().isoformat()
    return ts.isoformat()


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PrecomputedRow:
    """One row of ``api_precomputed_results`` (payload kept as JSON text)."""

    key: str
    params_hash: str
    params: dict[str, Any]
    payload_json: str | None
    computed_at: datetime | None
    as_of: datetime | None
    version: str | None
    inputs: dict[str, Any] | None
    duration_s: float | None
    last_attempt_at: datetime | None
    last_status: str | None
    last_error: str | None

    @property
    def has_payload(self) -> bool:
        return self.payload_json is not None and self.computed_at is not None

    def payload(self) -> Any:
        return json.loads(self.payload_json) if self.payload_json is not None else None


def _json_col(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return None
    return value


def _row(r: Any) -> PrecomputedRow:
    return PrecomputedRow(
        key=r[0],
        params_hash=r[1],
        params=_json_col(r[2]) or {},
        payload_json=r[3],
        computed_at=_as_utc(r[4]),
        as_of=_as_utc(r[5]),
        version=r[6],
        inputs=_json_col(r[7]),
        duration_s=float(r[8]) if r[8] is not None else None,
        last_attempt_at=_as_utc(r[9]),
        last_status=r[10],
        last_error=r[11],
    )


def read_result(engine: Engine, key: str, params: Mapping[str, Any] | None) -> PrecomputedRow | None:
    """Return the row for ``(key, params)``, or ``None`` if never attempted.

    A primary-key lookup; this is the only database work an endpoint does.
    """
    with engine.connect() as conn:
        r = conn.execute(
            text(
                "SELECT key, params_hash, params, payload::text, computed_at, as_of, version, "
                "inputs, duration_s, last_attempt_at, last_status, last_error "
                "FROM api_precomputed_results WHERE key = :key AND params_hash = :ph"
            ),
            {"key": key, "ph": params_hash(params)},
        ).fetchone()
    return _row(r) if r is not None else None


def read_key_rows(engine: Engine, keys: list[str]) -> list[PrecomputedRow]:
    """All rows for the given keys (scheduling state; payload not decoded)."""
    if not keys:
        return []
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT key, params_hash, params, NULL::text, computed_at, as_of, version, "
                "inputs, duration_s, last_attempt_at, last_status, last_error "
                "FROM api_precomputed_results WHERE key = ANY(:keys)"
            ),
            {"keys": list(keys)},
        ).fetchall()
    return [_row(r) for r in rows]


def write_success(
    engine: Engine,
    *,
    key: str,
    params: Mapping[str, Any] | None,
    payload: Any,
    as_of: date | datetime | None,
    version: str,
    inputs: Mapping[str, Any] | None = None,
    duration_s: float | None = None,
    computed_at: datetime | None = None,
) -> None:
    """Upsert a successful result (replaces the previous payload)."""
    now = computed_at or datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO api_precomputed_results "
                "(key, params_hash, params, payload, computed_at, as_of, version, inputs, "
                " duration_s, last_attempt_at, last_status, last_error) "
                "VALUES (:key, :ph, CAST(:params AS JSONB), CAST(:payload AS JSONB), :computed_at, "
                " :as_of, :version, CAST(:inputs AS JSONB), :duration_s, :computed_at, :status, NULL) "
                "ON CONFLICT (key, params_hash) DO UPDATE SET "
                " params = EXCLUDED.params, payload = EXCLUDED.payload, "
                " computed_at = EXCLUDED.computed_at, as_of = EXCLUDED.as_of, "
                " version = EXCLUDED.version, inputs = EXCLUDED.inputs, "
                " duration_s = EXCLUDED.duration_s, last_attempt_at = EXCLUDED.last_attempt_at, "
                " last_status = EXCLUDED.last_status, last_error = NULL"
            ),
            {
                "key": key,
                "ph": params_hash(params),
                "params": dumps(dict(params or {})),
                "payload": dumps(payload),
                "computed_at": now,
                "as_of": _as_of_value(as_of),
                "version": version,
                "inputs": dumps(dict(inputs or {})),
                "duration_s": duration_s,
                "status": STATUS_OK,
            },
        )


def record_attempt_failure(
    engine: Engine,
    *,
    key: str,
    params: Mapping[str, Any] | None,
    status: str,
    error: str,
    version: str,
    duration_s: float | None = None,
) -> None:
    """Record a failed / unavailable attempt without touching the payload."""
    now = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO api_precomputed_results "
                "(key, params_hash, params, version, duration_s, last_attempt_at, last_status, last_error) "
                "VALUES (:key, :ph, CAST(:params AS JSONB), :version, :duration_s, :now, :status, :error) "
                "ON CONFLICT (key, params_hash) DO UPDATE SET "
                " last_attempt_at = EXCLUDED.last_attempt_at, "
                " last_status = EXCLUDED.last_status, last_error = EXCLUDED.last_error"
            ),
            {
                "key": key,
                "ph": params_hash(params),
                "params": dumps(dict(params or {})),
                "version": version,
                "duration_s": duration_s,
                "now": now,
                "status": status,
                "error": (error or "")[:2000],
            },
        )


# ---------------------------------------------------------------------------
# Freshness labels
# ---------------------------------------------------------------------------


def freshness_meta(
    row: PrecomputedRow | None,
    *,
    key: str,
    params: Mapping[str, Any] | None,
    stale_after_s: float,
    cadence_s: float,
    now: datetime | None = None,
) -> dict[str, Any]:
    """The ``precompute`` block attached to every precomputed response.

    ``stale`` is ``True`` once ``computed_at`` is older than
    ``stale_after_s``, ``None`` when nothing was ever computed. ``as_of`` is
    the data date the builder established, not the response time.
    """
    now = now or datetime.now(timezone.utc)
    meta: dict[str, Any] = {
        "key": key,
        "params": _jsonable(dict(params or {})),
        "computed_at": None,
        "as_of": None,
        "age_seconds": None,
        "stale": None,
        "stale_after_seconds": int(stale_after_s),
        "refresh_every_seconds": int(cadence_s),
        "version": None,
        "job_duration_s": None,
        "last_attempt_at": None,
        "last_status": None,
        "last_error": None,
    }
    if row is None:
        meta["status"] = "not_computed"
        return meta
    meta.update(
        {
            "version": row.version,
            "last_attempt_at": row.last_attempt_at.isoformat() if row.last_attempt_at else None,
            "last_status": row.last_status,
            "last_error": row.last_error,
        }
    )
    if not row.has_payload:
        meta["status"] = "not_computed"
        return meta
    assert row.computed_at is not None
    age = max(0.0, (now - row.computed_at).total_seconds())
    stale = age > stale_after_s
    meta.update(
        {
            "status": "stale" if stale else "fresh",
            "computed_at": row.computed_at.isoformat(),
            "as_of": _iso_as_of(row.as_of),
            "age_seconds": int(age),
            "stale": stale,
            "job_duration_s": round(row.duration_s, 3) if row.duration_s is not None else None,
        }
    )
    return meta


def splice_meta(payload_json: str, meta: Mapping[str, Any]) -> str:
    """Add ``"precompute": meta`` to a JSON object without decoding it.

    Payloads can be ~10 MB (the correlation matrix); decoding and re-encoding
    on every request would cost more than the lookup. JSONB's text form of an
    object is ``{...}``, so the block is inserted before the closing brace.
    """
    body = payload_json.rstrip()
    meta_json = dumps({"precompute": dict(meta)})[1:-1]
    if not body.startswith("{") or not body.endswith("}"):
        raise ValueError("precomputed payload is not a JSON object")
    inner = body[1:-1].strip()
    if not inner:
        return "{" + meta_json + "}"
    return body[:-1] + "," + meta_json + "}"


def log_write_outcome(key: str, params: Mapping[str, Any] | None, status: str, detail: str = "") -> None:
    log.info(
        "api precompute: {k} {p} -> {s}{d}",
        k=key, p=dict(params or {}), s=status, d=(f" ({detail})" if detail else ""),
    )
