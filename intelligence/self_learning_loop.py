"""
Self-learning loop primitive — CAT-unnumbered.

Shared infrastructure that any GRID module with a scorable output can
opt into. Solves the "how does this module get smarter over time" question
uniformly: record → score → update → persist.

## The contract

A self-learning module:

  1. **Emits** a typed output with a provenance record via
     ``record_emission(module, output, context)``. Every emission gets a
     UUID, a module name, a timestamp, a typed payload, and the raw
     context that produced it.

  2. **Gets scored** later (seconds to days) via
     ``record_outcome(emission_id, outcome_value, metadata)``. The scorer
     is whatever naturally knows the answer — the decision journal, a
     price watcher, a verdict flipper.

  3. **Reads back** its own history via
     ``get_history(module, since=None, min_samples=1)``, which returns
     the scored emissions ready for the module's update function.

  4. **Updates** its parameters in-process via a module-provided
     ``update(history) -> dict`` callback. The returned dict is the new
     parameter set — clamped, validated, persisted to the loop's state
     table so restarts don't erase learning.

The primitive does NOT prescribe the shape of the parameters or the
update rule — each module owns that. It just provides the shared
plumbing: schema, persistence, scoring hooks, defensive error handling,
and a single ``SelfLearningLoop`` class every module can instantiate.

## Why a shared primitive

Before this module, GRID had ~10 ad-hoc self-learning loops:
``per_signal_brier`` (Welford Brier), ``signal_cooccurrence`` (joint-pair
history), ``confidence_bucket_tracker`` (reliability bucket), ``trust_scorer``
(Bayesian actor trust), ``forensic_journal`` (postmortem extraction),
``meta_learning_matrix`` (edge by condition cube), ``regime_conditional_brier``,
``historical_scenario_library`` (analog recall), ``null_hypothesis_forecaster``
(baseline skeptic), and ``options_tracker`` (scanner weights). Each reinvented
its own table schema, update cadence, and persistence logic. This primitive
is the minimum viable subset they all share.

Modules that need sophisticated stratification (per-regime, per-horizon,
per-condition) still own their specialized tables. This primitive is for
the **long tail** of modules that have a single scorable output and
should just record → score → update without reinventing the wheel.

## Example usage

```python
from intelligence.self_learning_loop import SelfLearningLoop

# Inside a signal scorer module
loop = SelfLearningLoop(
    engine=engine,
    module_name="options_flow_sentiment_classifier",
    update_fn=_my_update_fn,
    default_params={"threshold": 0.55, "weight": 1.0},
)

# At emit time
emission_id = loop.emit(
    output={"classification": "bullish", "probability": 0.72},
    context={"ticker": "AAPL", "as_of": "2026-04-14"},
)

# At outcome time (days later)
loop.score(emission_id, outcome={"hit": True, "realized_return": 0.034})

# Periodic (hourly / daily — caller decides cadence)
new_params = loop.update_parameters(min_samples=20)
# `new_params` is the result of your update_fn(history) — already persisted.
```

Every method is defensive: never raises, returns safe defaults on any DB
error, and logs at debug so a broken learning loop cannot break the
module's primary job.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────

DEFAULT_MIN_SAMPLES_TO_UPDATE: int = 20
DEFAULT_HISTORY_LOOKBACK_DAYS: int = 365
MAX_PARAM_BLOB_BYTES: int = 8192  # cap per-module state blob size


# ── DB schema ────────────────────────────────────────────────────────────

_EMISSIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS self_learning_emissions (
    id               TEXT PRIMARY KEY,
    module_name      TEXT NOT NULL,
    emitted_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    output           JSONB NOT NULL,
    context          JSONB,
    scored_at        TIMESTAMPTZ,
    outcome          JSONB,
    outcome_scalar   DOUBLE PRECISION,
    metadata         JSONB
);
CREATE INDEX IF NOT EXISTS idx_sl_emissions_module
    ON self_learning_emissions(module_name, emitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_sl_emissions_unscored
    ON self_learning_emissions(module_name)
    WHERE scored_at IS NULL;
"""

_STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS self_learning_state (
    module_name      TEXT PRIMARY KEY,
    params           JSONB NOT NULL,
    n_samples        INT NOT NULL DEFAULT 0,
    last_updated     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    update_count     INT NOT NULL DEFAULT 0,
    last_error       TEXT
);
CREATE INDEX IF NOT EXISTS idx_sl_state_last_updated
    ON self_learning_state(last_updated);
"""


_initialized_engines: set[int] = set()


def _ensure_schema(engine: Engine) -> None:
    """Idempotent table creation. Caches per-engine-id to avoid re-running."""
    eid = id(engine)
    if eid in _initialized_engines:
        return
    try:
        with engine.begin() as conn:
            for stmt in _EMISSIONS_TABLE_SQL.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
            for stmt in _STATE_TABLE_SQL.split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        _initialized_engines.add(eid)
    except Exception as exc:  # noqa: BLE001
        log.debug("self_learning_loop: schema init failed: {e}", e=str(exc))


def _reset_initialized_engines() -> None:
    """TEST USE ONLY — reset the schema-init cache between tests."""
    _initialized_engines.clear()


# ── Dataclasses ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ScoredEmission:
    """One scored emission — module output + context + outcome.

    Callers rarely construct these directly; they come back from
    ``get_history``. Pass a ``list[ScoredEmission]`` to your update_fn.
    """
    id: str
    module_name: str
    emitted_at: str            # ISO
    output: dict[str, Any]
    context: dict[str, Any]
    scored_at: Optional[str]
    outcome: dict[str, Any]
    outcome_scalar: Optional[float]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "module_name": self.module_name,
            "emitted_at": self.emitted_at,
            "output": dict(self.output),
            "context": dict(self.context),
            "scored_at": self.scored_at,
            "outcome": dict(self.outcome),
            "outcome_scalar": self.outcome_scalar,
        }


@dataclass(frozen=True)
class LoopState:
    """Persisted learning state for one module."""
    module_name: str
    params: dict[str, Any]
    n_samples: int
    last_updated: str
    update_count: int
    last_error: Optional[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "module_name": self.module_name,
            "params": dict(self.params),
            "n_samples": self.n_samples,
            "last_updated": self.last_updated,
            "update_count": self.update_count,
            "last_error": self.last_error,
        }


# ── The primitive ────────────────────────────────────────────────────────


UpdateFn = Callable[[list[ScoredEmission], dict[str, Any]], dict[str, Any]]
"""Module-provided update function.

Receives (scored_history, current_params), returns new_params. The new
params dict must be JSON-serializable and fit in MAX_PARAM_BLOB_BYTES.
Raises are caught by the loop and recorded in ``last_error``; the old
params are preserved on failure.
"""


class SelfLearningLoop:
    """Shared record → score → update → persist plumbing.

    Every module that wants a self-learning loop instantiates one of
    these at startup and uses three methods: ``emit``, ``score``,
    ``update_parameters``. State lives in the shared
    ``self_learning_emissions`` / ``self_learning_state`` tables —
    no per-module schema.
    """

    def __init__(
        self,
        engine: Engine,
        *,
        module_name: str,
        update_fn: UpdateFn,
        default_params: Optional[dict[str, Any]] = None,
    ) -> None:
        if not module_name:
            raise ValueError("module_name is required")
        self._engine = engine
        self._module_name = module_name
        self._update_fn = update_fn
        self._default_params: dict[str, Any] = dict(default_params or {})
        _ensure_schema(engine)

    # ── Emit ──────────────────────────────────────────────────────────

    def emit(
        self,
        *,
        output: dict[str, Any],
        context: Optional[dict[str, Any]] = None,
    ) -> Optional[str]:
        """Record an emission and return its id. None on any failure."""
        eid = uuid.uuid4().hex
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO self_learning_emissions
                            (id, module_name, output, context)
                        VALUES (:id, :m, :o, :c)
                        """
                    ),
                    {
                        "id": eid,
                        "m": self._module_name,
                        "o": json.dumps(output, default=str),
                        "c": json.dumps(context or {}, default=str),
                    },
                )
            return eid
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "self_learning_loop[{m}]: emit failed: {e}",
                m=self._module_name, e=str(exc),
            )
            return None

    # ── Score ─────────────────────────────────────────────────────────

    def score(
        self,
        emission_id: str,
        *,
        outcome: dict[str, Any],
        outcome_scalar: Optional[float] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Attach an outcome to an emission.

        ``outcome_scalar`` is a float representation of the outcome
        suitable for quick aggregations (mean hit rate, mean P&L). It's
        separate from the full ``outcome`` dict so update functions can
        bucket by scalar without re-parsing JSON.
        """
        if not emission_id:
            return False
        try:
            with self._engine.begin() as conn:
                result = conn.execute(
                    text(
                        """
                        UPDATE self_learning_emissions
                        SET outcome        = :o,
                            outcome_scalar = :os,
                            metadata       = :md,
                            scored_at      = NOW()
                        WHERE id = :id
                          AND scored_at IS NULL
                        """
                    ),
                    {
                        "id": emission_id,
                        "o": json.dumps(outcome, default=str),
                        "os": outcome_scalar,
                        "md": json.dumps(metadata or {}, default=str),
                    },
                )
                return result.rowcount > 0
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "self_learning_loop[{m}]: score failed for {eid}: {e}",
                m=self._module_name, eid=emission_id, e=str(exc),
            )
            return False

    # ── Read ──────────────────────────────────────────────────────────

    def get_history(
        self,
        *,
        lookback_days: int = DEFAULT_HISTORY_LOOKBACK_DAYS,
        limit: Optional[int] = None,
        scored_only: bool = True,
    ) -> list[ScoredEmission]:
        """Return scored emissions for this module. Empty list on failure."""
        try:
            with self._engine.connect() as conn:
                sql = """
                    SELECT id, module_name, emitted_at, output, context,
                           scored_at, outcome, outcome_scalar
                    FROM self_learning_emissions
                    WHERE module_name = :m
                      AND emitted_at >= NOW() - (:d || ' days')::interval
                """
                if scored_only:
                    sql += " AND scored_at IS NOT NULL "
                sql += " ORDER BY emitted_at DESC "
                if limit is not None and limit > 0:
                    sql += f" LIMIT {int(limit)} "
                rows = conn.execute(
                    text(sql), {"m": self._module_name, "d": lookback_days}
                ).fetchall()
            return [_row_to_scored_emission(r) for r in rows]
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "self_learning_loop[{m}]: get_history failed: {e}",
                m=self._module_name, e=str(exc),
            )
            return []

    def get_state(self) -> LoopState:
        """Return the persisted state for this module.

        If no state exists yet, returns a fresh LoopState seeded from
        ``default_params`` with n_samples=0 / update_count=0.
        """
        try:
            with self._engine.connect() as conn:
                row = conn.execute(
                    text(
                        """
                        SELECT module_name, params, n_samples, last_updated,
                               update_count, last_error
                        FROM self_learning_state
                        WHERE module_name = :m
                        """
                    ),
                    {"m": self._module_name},
                ).fetchone()
            if row is None:
                return LoopState(
                    module_name=self._module_name,
                    params=dict(self._default_params),
                    n_samples=0,
                    last_updated=datetime.now(timezone.utc).isoformat(),
                    update_count=0,
                    last_error=None,
                )
            params = row[1] if isinstance(row[1], dict) else json.loads(row[1] or "{}")
            return LoopState(
                module_name=row[0],
                params=params,
                n_samples=int(row[2] or 0),
                last_updated=row[3].isoformat() if row[3] else "",
                update_count=int(row[4] or 0),
                last_error=row[5],
            )
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "self_learning_loop[{m}]: get_state failed: {e}",
                m=self._module_name, e=str(exc),
            )
            return LoopState(
                module_name=self._module_name,
                params=dict(self._default_params),
                n_samples=0,
                last_updated="",
                update_count=0,
                last_error=str(exc),
            )

    # ── Update ────────────────────────────────────────────────────────

    def update_parameters(
        self,
        *,
        min_samples: int = DEFAULT_MIN_SAMPLES_TO_UPDATE,
        lookback_days: int = DEFAULT_HISTORY_LOOKBACK_DAYS,
    ) -> LoopState:
        """Run the update cycle: read history → call update_fn → persist.

        Returns the new ``LoopState`` whether or not the update ran.
        When ``n_samples < min_samples``, the update_fn is skipped and
        the prior state is returned unchanged. When the update_fn
        raises, the old state is preserved and ``last_error`` is set.
        Never raises.
        """
        state = self.get_state()
        history = self.get_history(lookback_days=lookback_days)
        n = len(history)
        if n < min_samples:
            log.debug(
                "self_learning_loop[{m}]: skipping update — n={n} < {min}",
                m=self._module_name, n=n, min=min_samples,
            )
            return state

        try:
            new_params = self._update_fn(history, state.params)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "self_learning_loop[{m}]: update_fn raised: {e}",
                m=self._module_name, e=str(exc),
            )
            self._record_error(str(exc))
            return self.get_state()

        if not isinstance(new_params, dict):
            err = f"update_fn returned {type(new_params).__name__}, expected dict"
            log.warning("self_learning_loop[{m}]: {e}", m=self._module_name, e=err)
            self._record_error(err)
            return self.get_state()

        try:
            serialized = json.dumps(new_params, default=str)
        except Exception as exc:  # noqa: BLE001
            err = f"update_fn returned non-serializable params: {exc}"
            log.warning("self_learning_loop[{m}]: {e}", m=self._module_name, e=err)
            self._record_error(err)
            return self.get_state()

        if len(serialized.encode("utf-8")) > MAX_PARAM_BLOB_BYTES:
            err = f"new_params exceeds {MAX_PARAM_BLOB_BYTES}B cap"
            log.warning("self_learning_loop[{m}]: {e}", m=self._module_name, e=err)
            self._record_error(err)
            return self.get_state()

        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO self_learning_state
                            (module_name, params, n_samples, update_count, last_error)
                        VALUES (:m, :p, :n, 1, NULL)
                        ON CONFLICT (module_name) DO UPDATE
                        SET params       = EXCLUDED.params,
                            n_samples    = EXCLUDED.n_samples,
                            update_count = self_learning_state.update_count + 1,
                            last_updated = NOW(),
                            last_error   = NULL
                        """
                    ),
                    {
                        "m": self._module_name,
                        "p": serialized,
                        "n": n,
                    },
                )
        except Exception as exc:  # noqa: BLE001
            log.debug(
                "self_learning_loop[{m}]: state persist failed: {e}",
                m=self._module_name, e=str(exc),
            )

        return self.get_state()

    def _record_error(self, err: str) -> None:
        try:
            with self._engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO self_learning_state
                            (module_name, params, n_samples, last_error)
                        VALUES (:m, :p, 0, :e)
                        ON CONFLICT (module_name) DO UPDATE
                        SET last_error = EXCLUDED.last_error,
                            last_updated = NOW()
                        """
                    ),
                    {
                        "m": self._module_name,
                        "p": json.dumps(self._default_params, default=str),
                        "e": err[:500],
                    },
                )
        except Exception:  # noqa: BLE001
            pass  # last_error recording is best-effort


# ── Helpers ──────────────────────────────────────────────────────────────


def _row_to_scored_emission(row: Any) -> ScoredEmission:
    output = row[3] if isinstance(row[3], dict) else json.loads(row[3] or "{}")
    context = row[4] if isinstance(row[4], dict) else json.loads(row[4] or "{}")
    outcome = row[6] if isinstance(row[6], dict) else json.loads(row[6] or "{}")
    return ScoredEmission(
        id=row[0],
        module_name=row[1],
        emitted_at=row[2].isoformat() if row[2] else "",
        output=output,
        context=context,
        scored_at=row[5].isoformat() if row[5] else None,
        outcome=outcome,
        outcome_scalar=float(row[7]) if row[7] is not None else None,
    )


def list_learning_modules(engine: Engine) -> list[LoopState]:
    """Return every module with a row in self_learning_state.

    Useful for the dashboard ("which modules are learning right now")
    and for the audit script ("which modules have NO loop at all").
    Never raises.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT module_name, params, n_samples, last_updated,
                           update_count, last_error
                    FROM self_learning_state
                    ORDER BY last_updated DESC
                    """
                )
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug("list_learning_modules failed: {e}", e=str(exc))
        return []

    out: list[LoopState] = []
    for row in rows:
        params = row[1] if isinstance(row[1], dict) else json.loads(row[1] or "{}")
        out.append(
            LoopState(
                module_name=row[0],
                params=params,
                n_samples=int(row[2] or 0),
                last_updated=row[3].isoformat() if row[3] else "",
                update_count=int(row[4] or 0),
                last_error=row[5],
            )
        )
    return out
