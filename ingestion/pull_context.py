"""
GRID — Pull Context Manager.

Wraps every puller invocation with accountability:
  - Logs start/end to pull_log table
  - Tracks rows inserted
  - Records failures with error messages
  - Emits events on completion (for event bus integration)

Usage:
    with PullContext(engine, "yfinance") as ctx:
        rows = do_pull(engine)
        ctx.record_rows(rows)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


class PullContext:
    """Context manager that wraps puller execution with accountability.

    Every puller call gets logged to pull_log with status, row counts,
    and error details. No more silent failures.
    """

    def __init__(
        self,
        engine: Engine,
        puller_name: str,
        source_id: int | None = None,
        node_name: str = "grid-svr",
    ) -> None:
        self._engine = engine
        self._puller_name = puller_name
        self._source_id = source_id
        self._node_name = node_name
        self._log_id: int | None = None
        self._rows_inserted: int = 0
        self._rows_expected: int | None = None
        self._features_affected: list[int] = []
        self._started_at = datetime.now(timezone.utc)

    def record_rows(self, count: int) -> None:
        """Record number of rows inserted during this pull."""
        self._rows_inserted += count

    def set_expected(self, count: int) -> None:
        """Set expected row count for partial detection."""
        self._rows_expected = count

    def add_features(self, feature_ids: list[int]) -> None:
        """Record which features were affected by this pull."""
        self._features_affected.extend(feature_ids)

    @property
    def rows_inserted(self) -> int:
        return self._rows_inserted

    def __enter__(self) -> "PullContext":
        try:
            with self._engine.begin() as conn:
                result = conn.execute(
                    text("""
                        INSERT INTO pull_log
                            (puller_name, source_id, started_at, status, node_name)
                        VALUES (:name, :sid, :started, 'RUNNING', :node)
                        RETURNING id
                    """),
                    {
                        "name": self._puller_name,
                        "sid": self._source_id,
                        "started": self._started_at,
                        "node": self._node_name,
                    },
                )
                row = result.fetchone()
                self._log_id = row[0] if row else None
        except Exception as exc:
            log.warning(
                "PullContext: failed to create pull_log entry for {p}: {e}",
                p=self._puller_name, e=str(exc),
            )
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        now = datetime.now(timezone.utc)

        if exc_type is not None:
            status = "FAILED"
            error_msg = f"{exc_type.__name__}: {exc_val}"
            log.error(
                "Pull FAILED: {p} — {e}",
                p=self._puller_name, e=error_msg,
            )
        elif self._rows_expected and self._rows_inserted < self._rows_expected * 0.5:
            status = "PARTIAL"
            error_msg = (
                f"Expected ~{self._rows_expected} rows, got {self._rows_inserted}"
            )
            log.warning(
                "Pull PARTIAL: {p} — {e}",
                p=self._puller_name, e=error_msg,
            )
        else:
            status = "SUCCESS"
            error_msg = None
            log.info(
                "Pull SUCCESS: {p} — {r} rows",
                p=self._puller_name, r=self._rows_inserted,
            )

        # ── Sanity checks on row counts ───────────────────────────────
        self._sanity_check_row_counts(status)

        if self._log_id is not None:
            try:
                with self._engine.begin() as conn:
                    conn.execute(
                        text("""
                            UPDATE pull_log SET
                                completed_at = :completed,
                                status = :status,
                                rows_inserted = :rows,
                                rows_expected = :expected,
                                error_message = :error,
                                features_affected = :features
                            WHERE id = :id
                        """),
                        {
                            "completed": now,
                            "status": status,
                            "rows": self._rows_inserted,
                            "expected": self._rows_expected,
                            "error": error_msg,
                            "features": self._features_affected or None,
                            "id": self._log_id,
                        },
                    )
            except Exception as db_exc:
                log.warning(
                    "PullContext: failed to update pull_log for {p}: {e}",
                    p=self._puller_name, e=str(db_exc),
                )

        # Emit event for bus integration (best-effort)
        try:
            _emit_pull_event(
                self._engine, self._puller_name, status,
                self._rows_inserted, self._features_affected, self._node_name,
            )
        except Exception:
            pass

        # Don't suppress the original exception
        return False

    def _sanity_check_row_counts(self, current_status: str) -> None:
        """Post-pull sanity checks on row counts.

        Checks:
          - If rows_inserted == 0 and previous run had rows, warn (source down?)
          - If rows_inserted > 10x the historical average, warn (duplicate import?)
          - If rows_inserted deviates > 3 std deviations from historical mean, warn

        Never raises — all warnings are log-only.
        """
        try:
            with self._engine.connect() as conn:
                # Get the last 20 successful pulls for this puller
                hist_rows = conn.execute(
                    text("""
                        SELECT rows_inserted
                        FROM pull_log
                        WHERE puller_name = :name
                        AND status IN ('SUCCESS', 'PARTIAL')
                        AND rows_inserted IS NOT NULL
                        AND id != COALESCE(:current_id, -1)
                        ORDER BY completed_at DESC
                        LIMIT 20
                    """),
                    {
                        "name": self._puller_name,
                        "current_id": self._log_id,
                    },
                ).fetchall()

                if not hist_rows:
                    return  # no history to compare against

                hist_counts = [int(r[0]) for r in hist_rows]
                prev_count = hist_counts[0] if hist_counts else 0
                mean_count = sum(hist_counts) / len(hist_counts)

                # Check 1: zero rows when previous run had data
                if (
                    self._rows_inserted == 0
                    and prev_count > 0
                    and current_status != "FAILED"
                ):
                    log.warning(
                        "SANITY [{p}]: 0 rows inserted but previous run had "
                        "{prev} rows — source may be down",
                        p=self._puller_name, prev=prev_count,
                    )

                # Check 2: >10x the average (possible duplicate import)
                if mean_count > 0 and self._rows_inserted > mean_count * 10:
                    log.warning(
                        "SANITY [{p}]: {n} rows inserted, "
                        ">{mult:.0f}x the average ({avg:.0f}) — "
                        "possible duplicate import",
                        p=self._puller_name, n=self._rows_inserted,
                        mult=self._rows_inserted / mean_count,
                        avg=mean_count,
                    )

                # Check 3: >3 standard deviations from mean
                if len(hist_counts) >= 5 and mean_count > 0:
                    variance = sum(
                        (c - mean_count) ** 2 for c in hist_counts
                    ) / len(hist_counts)
                    std_dev = variance ** 0.5
                    if std_dev > 0:
                        z_score = abs(
                            self._rows_inserted - mean_count
                        ) / std_dev
                        if z_score > 3.0:
                            log.warning(
                                "SANITY [{p}]: {n} rows is {z:.1f} std devs "
                                "from mean ({avg:.0f} +/- {std:.0f})",
                                p=self._puller_name,
                                n=self._rows_inserted,
                                z=z_score,
                                avg=mean_count,
                                std=std_dev,
                            )

        except Exception as exc:
            log.debug(
                "PullContext sanity check failed for {p}: {e}",
                p=self._puller_name, e=str(exc),
            )


def _emit_pull_event(
    engine: Engine,
    puller_name: str,
    status: str,
    rows: int,
    features: list[int],
    node: str,
) -> None:
    """Best-effort emit to event_bus table."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO event_bus (event_type, source_node, payload)
                    VALUES ('pull_completed', :node, :payload)
                """),
                {
                    "node": node,
                    "payload": {
                        "puller": puller_name,
                        "status": status,
                        "rows_inserted": rows,
                        "features_affected": features,
                    },
                },
            )
    except Exception:
        pass


# ── DB-backed idempotency (replaces in-memory _last_run) ───────────────

def should_run_pull(engine: Engine, puller_name: str, period: str) -> bool:
    """Check if a puller should run based on its last successful pull.

    Replaces the in-memory _last_run dict with persistent DB check.
    """
    from datetime import date as _date

    today = _date.today()

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("""
                    SELECT MAX(completed_at)::date
                    FROM pull_log
                    WHERE puller_name = :name
                    AND status IN ('SUCCESS', 'PARTIAL')
                """),
                {"name": puller_name},
            ).fetchone()

            if not row or row[0] is None:
                return True

            last_date = row[0]

            if period == "day":
                return last_date < today
            elif period == "month":
                return (last_date.year, last_date.month) < (today.year, today.month)
            elif period == "year":
                return last_date.year < today.year
    except Exception as exc:
        log.debug("should_run_pull check failed for {p}: {e}", p=puller_name, e=str(exc))
        return True  # fail open — better to re-pull than skip

    return True
