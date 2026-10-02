"""Bounded append-only options publication; requires the reviewed SQL packet.

No provider calls, retries, savepoints or schema creation occur here. Registration
remains the public completion gate. A prepared header is never a registration.
"""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Callable

from sqlalchemy import text

CONTRACT_BATCH_ROWS = 50
_TABLES = (
    "options_capture_batches_all", "options_snapshots_all",
    "options_capture_publications", "options_daily_signals",
    "feature_registry", "resolved_series", "source_catalog",
)


class PublicationBudgetExpired(Exception):
    """Cooperative cancellation before the next commit."""


@dataclass
class TransactionReceipt:
    rows: int = 0
    value: Any = None
    commit_ack: str = "NOT_COMMITTED"
    cleanup_failed: bool = False
    error: str | None = None

    @property
    def stop(self) -> bool:
        return self.commit_ack != "ACKNOWLEDGED" or self.cleanup_failed


def transaction(engine, work: Callable, should_continue: Callable | None = None,
                *, lock_timeout_seconds: int = 5) -> TransactionReceipt:
    """Keep COMMIT acknowledgement separate from connection cleanup.

    The caller never retries this transaction. Unknown COMMIT acknowledgement
    cannot be converted to zero rows. Every failure stops the remaining scope.
    """
    receipt = TransactionReceipt()
    conn = trans = None
    committing = False
    try:
        if lock_timeout_seconds not in (3, 5):
            raise ValueError("unsupported bounded lock timeout")
        if should_continue is not None and not should_continue():
            raise PublicationBudgetExpired
        conn = engine.connect()
        trans = conn.begin()
        conn.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        conn.execute(text(f"SET LOCAL lock_timeout = '{lock_timeout_seconds}s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5s'"))
        conn.execute(text("SET LOCAL idle_in_transaction_session_timeout = '5s'"))
        # DDL cannot change the audited trigger closure between this check and
        # commit. No row locks are retained across independent transactions.
        conn.exec_driver_sql("LOCK TABLE " + ", ".join(_TABLES) + " IN ROW EXCLUSIVE MODE")
        conn.execute(text("SELECT options_bounded_assert_trigger_closure()"))
        conn.execute(text("SET LOCAL grid.options_bounded = 'on'"))
        conn.info["options_transaction_deadline"] = time.monotonic() + 15
        count_sql = text("""
            SELECT COALESCE(SUM(n_tup_ins + n_tup_upd + n_tup_del), 0)::bigint
            FROM pg_stat_xact_user_tables
        """)
        baseline = conn.execute(count_sql).scalar_one()
        receipt.value = work(conn)
        # This counts actual DATA writes globally, including trigger sidewrites,
        # across all user relations, without creating an audit DATA row itself.
        # PG14 may retain counters from earlier transactions until backend
        # statistics are flushed. Measure a delta within this transaction.
        rows = conn.execute(count_sql).scalar_one() - baseline
        if rows < 0 or rows > 50:
            raise RuntimeError("options DATA transaction budget exceeded")
        check_deadline(conn)
        if should_continue is not None and not should_continue():
            raise PublicationBudgetExpired
        committing = True
        trans.commit()
        receipt.commit_ack = "ACKNOWLEDGED"
        receipt.rows = int(rows)
    except Exception as exc:
        receipt.error = type(exc).__name__
        if committing:
            receipt.commit_ack = "UNKNOWN"
        elif trans is not None:
            try:
                trans.rollback()
            except Exception:
                receipt.error = "RollbackAcknowledgementUnknown"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                receipt.cleanup_failed = True
    return receipt


def check_deadline(conn) -> None:
    if time.monotonic() >= conn.info["options_transaction_deadline"]:
        raise RuntimeError("options transaction deadline expired")


_HEADER = text("""
    INSERT INTO options_capture_batches_all
        (capture_batch_id, ticker, snap_date, capture_ordinal,
         capture_started_at, capture_completed_at, row_count,
         spot_price, capture_source, requires_publication)
    VALUES (:batch_id, :ticker, :snap_date, :ordinal, :started_at,
            :completed_at, :row_count, :spot, :source, true)
""")
_CONTRACT = text("""
    INSERT INTO options_snapshots_all
        (ticker, snap_date, expiry, opt_type, strike, last_price, bid, ask,
         volume, open_interest, implied_vol, in_the_money, capture_batch_id,
         capture_ordinal, capture_started_at, capture_completed_at,
         provider_regular_market_at)
    VALUES (:ticker, :snap_date, :expiry, :opt_type, :strike, :last_price,
            :bid, :ask, :volume, :oi, :iv, :itm, :batch_id, :ordinal,
            :started_at, :completed_at, :provider_regular_market_at)
""")


def publish(engine, header: dict, contracts: list[dict], finish: Callable,
            should_continue: Callable | None = None) -> dict:
    """Prepare, append bounded contracts, then atomically register + signals.

    `contracts` is the entire normalized, deduplicated provider scope; `finish`
    writes at most one signal and ten registry/resolved pairs. Older overlapping
    captures are registered without overwriting the latest daily signals.
    """
    if not contracts or len(contracts) != header["row_count"]:
        raise ValueError("complete contract count required")
    acknowledged = snapshots = 0
    receipts: list[TransactionReceipt] = []
    published = latest = False
    payload_rows = 0

    def run(work):
        nonlocal acknowledged
        receipt = transaction(engine, work, should_continue)
        receipts.append(receipt)
        if receipt.commit_ack == "ACKNOWLEDGED":
            acknowledged += receipt.rows
        return receipt

    receipt = run(lambda conn: conn.execute(_HEADER, header))
    if not receipt.stop:
        for offset in range(0, len(contracts), CONTRACT_BATCH_ROWS):
            batch = contracts[offset:offset + CONTRACT_BATCH_ROWS]

            def append(conn):
                for row in batch:
                    check_deadline(conn)
                    written = conn.execute(_CONTRACT, {
                        **row, "ordinal": header["ordinal"],
                        "started_at": header["started_at"], "completed_at": header["completed_at"],
                    })
                    if written.rowcount != 1:
                        raise ValueError("contract insert count unavailable or mismatched")

            receipt = run(append)
            if receipt.commit_ack == "ACKNOWLEDGED":
                snapshots += len(batch)
            if receipt.stop:
                break
        if not receipt.stop:

            def complete(conn):
                conn.execute(text("SELECT pg_advisory_xact_lock(hashtext(:ticker), hashtext(:snap_date))"), header)
                newest = conn.execute(text("""
                    SELECT MAX(capture_ordinal) FROM options_capture_batches
                    WHERE ticker = :ticker AND snap_date = :snap_date
                """), header).scalar_one()
                is_latest = newest is None or newest < header["ordinal"]
                rows = finish(conn) if is_latest else 0
                if rows is None:
                    raise ValueError("signal publication row count unavailable")
                conn.execute(text("""
                    INSERT INTO options_capture_publications (capture_batch_id)
                    VALUES (:batch_id)
                """), header)
                return is_latest, rows

            receipt = run(complete)
            if receipt.commit_ack == "ACKNOWLEDGED":
                latest, payload_rows = receipt.value
                published = True
    return {
        "published": published, "latest_batch": latest,
        "snapshots_inserted": snapshots,
        "rows_inserted": snapshots + payload_rows if receipt.commit_ack != "UNKNOWN" else None,
        "data_rows_acknowledged": acknowledged,
        "commit_ack": receipt.commit_ack, "cleanup_failed": receipt.cleanup_failed,
        "stop_scope": receipt.stop, "error": receipt.error,
        "transaction_rows": [r.rows for r in receipts if r.commit_ack == "ACKNOWLEDGED"],
    }
