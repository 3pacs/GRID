"""One-shot historical replay that bootstraps features/per_signal_brier_history
from oracle_predictions. Eliminates the ~30-day cold-start window for the
conviction dial. Idempotent: re-running the script truncates the
per_signal_brier_history table first (unless --append is passed) and replays
from scratch.

Why this exists
---------------

``features.per_signal_brier`` records a per-(signal_source, horizon_days)
running Brier score every time a prediction is scored, which is the input
to the conviction dial. That table is filled forward — meaning until ~30
days of fresh predictions accumulate, the conviction dial has nothing to
say. Months of already-scored ``oracle_predictions`` are sitting unused.

This script walks that historical pile once, decomposes each scored
prediction into per-signal contributions, and bulk-loads the running
buckets via ``record_scored_prediction``. The conviction dial wakes up
immediately.

Signal contribution decomposition (3-layer cascade)
---------------------------------------------------

For each scored prediction we need to attribute the confidence to
contributing signal sources. ``oracle_predictions`` does NOT have a
dedicated ``signal_contributions`` JSONB column today, but the cascade
below covers every shape the table has held over its history:

1. **Layer 1 — direct breakdown.** If a row exposes a
   ``signal_contributions`` JSONB blob (some adapters write one alongside
   ``signals``) we deserialize it and use it verbatim.
2. **Layer 2 — model-family uniform split.** If the row has a
   ``model_name``, look up that model's ``signal_families`` from
   ``oracle_models`` and split the prediction's confidence uniformly
   across the listed sources.
3. **Layer 3 — synthetic aggregate.** If neither is available, attribute
   the entire prediction to the synthetic ``"oracle_aggregate"`` source.
   Degenerate but populates the table so the dial isn't cold-start.

CLI
---

    python -m scripts.bootstrap_per_signal_brier --days 365
    python -m scripts.bootstrap_per_signal_brier --days 30 --append
    python -m scripts.bootstrap_per_signal_brier --dry-run --limit 10 --verbose
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# IMPORT — never reimplement. record_scored_prediction owns the Welford
# update path for per_signal_brier_history.
from features.per_signal_brier import (
    ensure_tables,
    record_scored_prediction,
)


# ── Constants ─────────────────────────────────────────────────────────────

# The exact column list pulled from oracle_predictions. Listed once so the
# row-dict builder and the SQL stay in lock-step.
_ORACLE_COLUMNS: tuple[str, ...] = (
    "id",
    "ticker",
    "created_at",
    "expiry",
    "confidence",
    "verdict",
    "model_name",
    "signals",
    "signal_contributions",
    "model_weights",
)

# Walks oracle_predictions in chronological order — replaying in time
# order makes the running buckets converge to the same value as a live
# stream would. ``created_at`` interval comes from --days.
_ORACLE_QUERY = text(
    """
    SELECT id, ticker, created_at, expiry, confidence, verdict,
           model_name, signals, signal_contributions, model_weights
    FROM oracle_predictions
    WHERE verdict IN ('hit', 'miss', 'partial')
      AND created_at >= NOW() - (:days || ' days')::interval
      AND dedup_keep = TRUE
    ORDER BY created_at ASC
    """
)

# Synthetic source attributed to predictions where no signal breakdown
# can be recovered. Deliberately distinctive so dashboard consumers can
# spot bootstrap-only rows.
ORACLE_AGGREGATE_SOURCE: str = "oracle_aggregate"


# ── Pure helpers ──────────────────────────────────────────────────────────


def verdict_to_outcome(verdict: str) -> float:
    """Map an oracle verdict string to the outcome scalar used by the
    Brier tracker.

    - ``hit`` → 1.0
    - ``partial`` → 0.5
    - ``miss`` → 0.0
    - anything else → 0.0 (defensive default; caller filters upstream)
    """
    if verdict == "hit":
        return 1.0
    if verdict == "partial":
        return 0.5
    return 0.0


def _parse_jsonb(value: Any) -> Any:
    """Parse a value that may be a dict, list, JSON-encoded string, or
    None. Returns None on any failure.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return None
    return None


def _normalize_contributions(raw: Any) -> dict[str, float] | None:
    """Coerce a raw signal_contributions blob into a normalized
    {source: weight} dict summing to ~1.0. Returns None if the blob is
    not a usable mapping.
    """
    parsed = _parse_jsonb(raw) if not isinstance(raw, dict) else raw
    if not isinstance(parsed, dict) or not parsed:
        return None
    cleaned: dict[str, float] = {}
    for k, v in parsed.items():
        if not isinstance(k, str) or not k.strip():
            continue
        try:
            w = float(v)
        except (TypeError, ValueError):
            continue
        if w <= 0:
            continue
        cleaned[k.strip()] = w
    if not cleaned:
        return None
    total = sum(cleaned.values())
    if total <= 0:
        return None
    return {k: v / total for k, v in cleaned.items()}


def extract_signal_contributions(
    prediction_row: dict[str, Any],
    *,
    oracle_models_lookup: dict[str, list[str]],
) -> dict[str, float]:
    """Decompose a scored prediction into per-signal contribution weights.

    Implements the 3-layer cascade described in the module docstring.
    Always returns a non-empty dict — falls through to
    ``{ORACLE_AGGREGATE_SOURCE: 1.0}`` rather than ever yielding empty.
    """
    # Layer 1 — direct breakdown stored on the prediction row.
    #
    # The blob lives in one of two places depending on schema vintage:
    #   * Top-level ``signal_contributions`` column (legacy adapters; absent
    #     on the current schema — _fetch_scored_predictions injects NULL
    #     in that case).
    #   * Nested inside the ``signals`` JSONB column at
    #     ``signals["signal_contributions"]`` — this is where the live
    #     oracle has been writing them. Until 2026-05-13 only the
    #     top-level path was checked, so Layer 1 always returned empty
    #     and the cascade fell through to the model-family uniform
    #     split (Layer 2), producing family-level scorecards
    #     (``vol``/``macro``/``insider``/...) instead of the
    #     per-individual-signal scorecards (``aapl_pcr``/``aapl_iv_atm``/
    #     ...) the oracle actually emits.
    layer1 = _normalize_contributions(prediction_row.get("signal_contributions"))
    if not layer1:
        signals_blob = _parse_jsonb(prediction_row.get("signals"))
        if isinstance(signals_blob, dict):
            layer1 = _normalize_contributions(
                signals_blob.get("signal_contributions")
            )
    if layer1:
        return layer1

    # Layer 2 — uniform split across the model's signal_families.
    model_name = prediction_row.get("model_name")
    if isinstance(model_name, str) and model_name.strip():
        sources = oracle_models_lookup.get(model_name.strip()) or []
        sources = [s for s in sources if isinstance(s, str) and s.strip()]
        if sources:
            weight = 1.0 / len(sources)
            return {s: weight for s in sources}

    # Layer 3 — synthetic aggregate fallback.
    return {ORACLE_AGGREGATE_SOURCE: 1.0}


def _coerce_horizon_days(row: dict[str, Any]) -> int:
    """Compute horizon_days from a row's ``created_at`` and ``expiry``.

    Defensive against NULLs and naive/aware datetime mixing.
    Defaults to 7 (matches per_signal_brier canonical fallback) when the
    values can't be reconciled.
    """
    created = row.get("created_at")
    expiry = row.get("expiry")
    if created is None or expiry is None:
        return 7
    try:
        if isinstance(created, datetime) and isinstance(expiry, datetime):
            delta = (expiry - created).days
        else:
            # expiry may be a date object — still subtract sensibly.
            created_d = created.date() if isinstance(created, datetime) else created
            expiry_d = expiry.date() if isinstance(expiry, datetime) else expiry
            delta = (expiry_d - created_d).days
    except Exception:  # noqa: BLE001
        return 7
    if delta <= 0:
        return 1
    return int(delta)


def _coerce_confidence(value: Any) -> float | None:
    """Parse a confidence cell into a float in [0, 1]. Returns None on
    failure so the caller can skip the row. Handles scientific notation
    strings (e.g. ``"7.5e-1"``) that occasionally appear in legacy rows.
    """
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f:  # NaN check
        return None
    return max(0.0, min(1.0, f))


def _compose_summary(
    replayed: int,
    signal_counts: dict[str, int],
) -> dict[str, Any]:
    """Format the summary payload returned by ``replay_predictions``.

    Returns a dict with ``replayed_count``, ``seeded_signals``, and the
    top-10 sources by sample count (or all of them if fewer than 10).
    Pure function — no engine, easy to unit-test.
    """
    sorted_sources = sorted(
        signal_counts.items(),
        key=lambda kv: (-kv[1], kv[0]),
    )
    top = sorted_sources[:10]
    return {
        "replayed_count": int(replayed),
        "seeded_signals": len(signal_counts),
        "per_signal_after_replay": [
            {"signal_source": s, "sample_count": int(c)} for s, c in top
        ],
    }


# ── DB helpers ────────────────────────────────────────────────────────────


def _load_oracle_models_lookup(engine: Engine) -> dict[str, list[str]]:
    """Cache ``model_name → signal_families`` from the ``oracle_models``
    table. Used by the Layer-2 contribution extractor.

    Wrapped in try/except so a missing table doesn't crash the bootstrap.
    """
    lookup: dict[str, list[str]] = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT name, signal_families FROM oracle_models")
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "bootstrap_per_signal_brier: oracle_models lookup failed: {e}",
            e=str(exc),
        )
        return lookup

    for row in rows or []:
        name = row[0]
        families_raw = row[1]
        if not isinstance(name, str) or not name.strip():
            continue
        parsed = _parse_jsonb(families_raw)
        if isinstance(parsed, list):
            sources = [s for s in parsed if isinstance(s, str) and s.strip()]
            if sources:
                lookup[name.strip()] = sources
    return lookup


def _truncate_per_signal_brier_history(engine: Engine) -> None:
    """Wipe the per_signal_brier_history table so the replay starts from
    a clean slate. Wrapped in try/except — a missing table or a
    permissions problem must not crash the bootstrap.
    """
    try:
        ensure_tables(engine)
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE per_signal_brier_history"))
        log.info("bootstrap_per_signal_brier: truncated per_signal_brier_history")
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "bootstrap_per_signal_brier: truncate failed (continuing): {e}",
            e=str(exc),
        )


def _fetch_scored_predictions(
    engine: Engine,
    *,
    days: int,
) -> list[dict[str, Any]]:
    """Walk oracle_predictions for scored rows in the last ``days`` days,
    returning them as dicts keyed by ``_ORACLE_COLUMNS``.

    Falls back to a column-tolerant SELECT if the optional
    ``signal_contributions`` column is absent on this database.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(_ORACLE_QUERY, {"days": int(days)}).fetchall()
    except Exception as exc:  # noqa: BLE001
        # Column may not exist in older schemas — retry without it.
        log.debug(
            "bootstrap_per_signal_brier: full query failed ({e}); "
            "falling back to legacy column set",
            e=str(exc),
        )
        legacy = text(
            """
            SELECT id, ticker, created_at, expiry, confidence, verdict,
                   model_name, signals, NULL::text AS signal_contributions,
                   model_weights
            FROM oracle_predictions
            WHERE verdict IN ('hit', 'miss', 'partial')
              AND created_at >= NOW() - (:days || ' days')::interval
              AND dedup_keep = TRUE
            ORDER BY created_at ASC
            """
        )
        try:
            with engine.connect() as conn:
                rows = conn.execute(legacy, {"days": int(days)}).fetchall()
        except Exception as exc2:  # noqa: BLE001
            log.error(
                "bootstrap_per_signal_brier: oracle_predictions read failed: {e}",
                e=str(exc2),
            )
            return []

    return [dict(zip(_ORACLE_COLUMNS, row)) for row in rows or []]


# ── Main replay path ──────────────────────────────────────────────────────


def replay_predictions(
    engine: Engine,
    *,
    days: int = 365,
    append: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
    verbose: bool = False,
) -> dict[str, Any]:
    """Walk historical scored predictions and seed
    ``per_signal_brier_history``.

    Parameters
    ----------
    engine
        SQLAlchemy engine. Tests pass a Mock with ``.connect`` and
        ``.begin`` context managers.
    days
        Lookback window in days (default 365).
    append
        If False (default) the table is truncated before replay. If True
        the existing buckets are kept and the replay extends them — used
        for incremental top-ups.
    dry_run
        If True, no writes occur but rows are still walked and counted.
    limit
        If set, stop after this many predictions (handy for testing).
    verbose
        Per-prediction log lines.

    Returns
    -------
    Summary dict from ``_compose_summary``.
    """
    if not append and not dry_run:
        _truncate_per_signal_brier_history(engine)

    oracle_models_lookup = _load_oracle_models_lookup(engine)
    rows = _fetch_scored_predictions(engine, days=days)

    if limit is not None:
        rows = rows[: int(limit)]

    signal_counts: dict[str, int] = {}
    replayed = 0
    skipped = 0

    for row in rows:
        try:
            verdict = row.get("verdict")
            if verdict not in ("hit", "miss", "partial"):
                skipped += 1
                continue

            confidence = _coerce_confidence(row.get("confidence"))
            if confidence is None:
                skipped += 1
                if verbose:
                    log.debug(
                        "skip prediction id={i}: bad confidence",
                        i=row.get("id"),
                    )
                continue

            outcome = verdict_to_outcome(verdict)
            horizon_days = _coerce_horizon_days(row)
            contributions = extract_signal_contributions(
                row,
                oracle_models_lookup=oracle_models_lookup,
            )

            if verbose:
                log.info(
                    "replay id={i} h={h}d conf={c:.3f} verdict={v} "
                    "sources={n}",
                    i=row.get("id"),
                    h=horizon_days,
                    c=confidence,
                    v=verdict,
                    n=len(contributions),
                )

            if not dry_run:
                record_scored_prediction(
                    engine,
                    horizon_days=horizon_days,
                    confidence=confidence,
                    outcome=outcome,
                    signal_contributions=contributions,
                )

            for source in contributions:
                signal_counts[source] = signal_counts.get(source, 0) + 1
            replayed += 1
        except Exception as exc:  # noqa: BLE001
            skipped += 1
            log.warning(
                "bootstrap_per_signal_brier: row replay failed id={i}: {e}",
                i=row.get("id") if isinstance(row, dict) else None,
                e=str(exc),
            )
            continue

    summary = _compose_summary(replayed, signal_counts)
    summary["skipped_count"] = skipped
    summary["dry_run"] = bool(dry_run)
    summary["append"] = bool(append)
    summary["lookback_days"] = int(days)
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    return summary


# ── CLI ───────────────────────────────────────────────────────────────────


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bootstrap_per_signal_brier",
        description=(
            "One-shot historical replay that seeds per_signal_brier_history "
            "from oracle_predictions so the conviction dial isn't cold-start."
        ),
    )
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Lookback window in days (default: 365).",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Don't truncate first — extend existing buckets.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Walk rows and report but never write.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N predictions (testing aid).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Per-prediction log lines.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    # Local import so `--help` works without DB config loaded.
    from db import get_engine  # type: ignore

    engine = get_engine()
    summary = replay_predictions(
        engine,
        days=args.days,
        append=args.append,
        dry_run=args.dry_run,
        limit=args.limit,
        verbose=args.verbose,
    )

    print(json.dumps(summary, indent=2, default=str))
    log.info(
        "bootstrap_per_signal_brier done: replayed={r} signals={s}",
        r=summary["replayed_count"],
        s=summary["seeded_signals"],
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
