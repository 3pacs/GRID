"""Shared guards for the QuiverQuant transition scripts (re-key, gov_contracts re-date).

Both scripts touch ``signal_sources`` on the production database. They share:

* all approved blackout windows: 03:30-10:30, 10:58-11:12 UTC and weekday
  13:25-14:20 UTC; writes reserve five seconds before a boundary;
* a read-only session for the default dry run (``default_transaction_read_only``
  plus statement and lock timeouts, checked before the first query);
* two-second statements/COMMIT, one-second locks and five-second idle limits;
* a marker/window check before each write and immediately before COMMIT.

Nothing here deletes anything.
"""

from __future__ import annotations

import os
import json
import stat
import sys
from contextlib import contextmanager
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, BinaryIO, Iterator, TextIO

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from ingestion.altdata.quiverquant_identity import transition_marker_exists, transition_marker_path
from ingestion.altdata import quiverquant_transactions as tx

from intelligence.people_events_pipeline.readonly import (
    BACKUP_WINDOW_UTC,
    WindowClosed,
    assert_db_window_open,
)

STATEMENT_TIMEOUT_MS = tx.STATEMENT_TIMEOUT_MS
LOCK_TIMEOUT_MS = tx.LOCK_TIMEOUT_MS

__all__ = [
    "BACKUP_WINDOW_UTC",
    "WindowClosed",
    "assert_db_window_open",
    "database_url",
    "open_engine",
    "assert_read_only",
    "check_window",
    "require_guard_closed",
    "is_lock_or_timeout",
    "transition_marker_exists",
    "transition_marker_path",
]


def database_url(db_url_env: str | None) -> str:
    """The database URL: the named env var, else ``config.settings.DB_URL``."""
    if db_url_env:
        return os.environ[db_url_env]
    from config import settings

    return settings.DB_URL


def open_engine(url: str, *, read_only: bool, application_name: str) -> Engine:
    """A one-connection engine; read-only sessions refuse every write at the server."""
    options = [
        f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
        f"-c lock_timeout={LOCK_TIMEOUT_MS}",
        "-c idle_in_transaction_session_timeout=5000",
        f"-c application_name={application_name}",
    ]
    if read_only:
        options.append("-c default_transaction_read_only=on")
    return create_engine(
        url,
        connect_args={"options": " ".join(options)},
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
    )


def assert_read_only(conn: Any) -> None:
    """Refuse to continue unless the session really is read-only."""
    value = conn.execute(text("SHOW transaction_read_only")).scalar()
    if str(value).lower() != "on":
        raise RuntimeError("connection is not read-only; refusing to continue")


def check_window(now: datetime | None = None) -> None:
    """All owner-approved blackout windows, including weekday options/GEM."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    assert_db_window_open(now)
    if time(10, 58) <= now.time() < time(11, 12):
        raise WindowClosed("refusing database access: 10:58-11:12Z change blackout")
    if now.weekday() < 5 and time(13, 25) <= now.time() < time(14, 20):
        raise WindowClosed("refusing database access: weekday 13:25-14:20Z options/GEM blackout")


def write_guard(*, guard_check: bool = True, now: datetime | None = None) -> None:
    """Refuse writes/COMMIT within five seconds of the next blackout too."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    check_window(now)
    check_window(now + timedelta(seconds=tx.TRANSACTION_SECONDS))
    if guard_check:
        require_guard_closed()


def append_audit(audit: Any, records: list[dict[str, Any]]) -> None:
    """Only acknowledged commits are logged; an I/O failure stops execution."""
    import json

    for record in records:
        audit.write(json.dumps(record) + "\n")
    audit.flush()
    os.fsync(audit.fileno())


def preserve_committed(exc: BaseException, moved: int) -> None:
    """Fatal errors carry the acknowledged prefix, excluding uncertain COMMITs."""
    exc.committed_rows = moved
    exc.commit_uncertain = isinstance(exc, tx.CommitUncertain)


@contextmanager
def audit_context(path: Path, *, acknowledged_rows: Callable[[], int]) -> Iterator[TextIO]:
    """Keep resolution state through audit enter/exit failures; never retry I/O.

    The body error is captured before file cleanup can replace it. Counts come
    only from observed COMMIT ACKs; an audit error cannot resolve a pending
    COMMIT. Preserve the initial cause separately from subsequent audit errors.
    """
    body_error: BaseException | None = None
    try:
        with path.open("x", encoding="utf-8") as audit:
            try:
                yield audit
            except BaseException as exc:
                body_error = exc
                raise
        if body_error is not None:
            # A context must not suppress a fatal resolution/audit error.
            raise body_error
    except BaseException as exc:
        exc.committed_rows = acknowledged_rows()
        exc.commit_uncertain = bool(getattr(body_error, "commit_uncertain", isinstance(body_error, tx.CommitUncertain)))
        resolution_cause = getattr(body_error, "resolution_cause", body_error)
        if (resolution_cause is body_error and body_error is not None
                and isinstance(body_error.__cause__, (tx.CommitUncertain, tx.CommitAcknowledgedCleanupError))):
            resolution_cause = body_error.__cause__
        exc.resolution_cause = resolution_cause
        if body_error is not None and exc is not body_error:
            raise exc from body_error
        raise



def validate_output_paths(out: Path | None, audit: Path | None, revert: Path | None) -> None:
    """Refuse existing destinations and aliases before acquiring a database."""
    protected = [p for p in (revert, transition_marker_path()) if p is not None]
    destinations = [p for p in (out, audit) if p is not None]
    for i, path in enumerate(destinations):
        if os.path.lexists(path):
            raise ValueError(f"refusing to overwrite {path}")
        normalized = os.path.normcase(str(path.expanduser().resolve()))
        for other in destinations[i + 1:] + protected:
            if normalized == os.path.normcase(str(other.expanduser().resolve())):
                raise ValueError("report, new audit, revert input and transition marker must have distinct paths")


class ReportFile:
    """Reserve a new output before DB access; publish only to its pinned handle.

    Never unlink/replace any path. An empty or partial failed report remains
    diagnostic evidence, and grants no authority to resume or replay DATA.
    """

    def __init__(self, path: Path):
        self.path = path.expanduser().absolute()
        self.stream: BinaryIO | None = None
        self.identity: tuple[int, int] | None = None
        self.parent_identity: tuple[int, int] | None = None

    @staticmethod
    def identity_of(status: os.stat_result) -> tuple[int, int]:
        return status.st_dev, status.st_ino

    def reserve(self) -> None:
        self.parent_identity = self.identity_of(self.path.parent.stat())
        self.stream = self.path.open("xb", buffering=0)
        self.identity = self.identity_of(os.fstat(self.stream.fileno()))
        self.check_identity()

    def check_identity(self) -> None:
        assert self.stream is not None
        status = os.fstat(self.stream.fileno())
        named = self.path.lstat()
        if (self.identity_of(status) != self.identity or self.identity_of(named) != self.identity
                or self.identity_of(self.path.parent.stat()) != self.parent_identity
                or not stat.S_ISREG(named.st_mode) or status.st_nlink != 1 or status.st_size != 0):
            raise OSError("reserved report identity/scope changed; refusing publication")

    def publish(self, rendered: str) -> None:
        assert self.stream is not None
        self.check_identity()
        encoded = rendered.encode("utf-8")
        if self.stream.write(encoded) != len(encoded):
            raise OSError("partial report write; publication failed")
        self.stream.flush()
        os.fsync(self.stream.fileno())

    def close(self) -> None:
        stream, self.stream = self.stream, None
        if stream is not None:
            stream.close()


def finalize_cli(
    engine_factory: Callable[[], Engine], operation: Callable[[Engine], dict[str, Any]], *, out: Path | None,
) -> int:
    """Capture resolution before guarded, once-only cleanup and reporting.

    An interruption keeps its control-flow identity and observed ACK prefix.
    Secondary cleanup/receipt failures cannot replace the trusted original.
    Each resource is detached before its sole cleanup attempt; failed rendering
    and I/O are never retried, including when fatal receipt construction fails.
    """
    engine = None
    output = ReportFile(out) if out is not None else None
    error: BaseException | None = None
    phase = "reserve_output"
    failed_phase = None
    acknowledged = 0
    uncertain = False
    resolution_cause = None
    completed = False
    secondary: list[tuple[str, BaseException]] = []
    stdout_failed = False
    interrupted = False

    def control_flow(exc: BaseException | None) -> BaseException | None:
        # Only our trusted transaction wrappers may wrap COMMIT/checkin exits.
        while isinstance(exc, (tx.CommitUncertain, tx.CommitAcknowledgedCleanupError)):
            exc = exc.__cause__
        return exc if exc is not None and not isinstance(exc, Exception) else None

    def failure(exc: BaseException, where: str) -> None:
        nonlocal error, failed_phase, interrupted
        interrupted = interrupted or not isinstance(exc, Exception)
        if error is None:
            error, failed_phase = exc, where
        else:
            secondary.append((where, exc))

    try:
        try:
            if output is not None:
                output.reserve()
            phase = "acquire_database"
            engine = engine_factory()
            phase = "resolve"
            report = operation(engine)
            # Capture the completed ACK outcome before disposal/render/I/O.
            acknowledged = report.get("applied", {}).get("moved", 0)
            completed = True
        except BaseException as exc:
            if phase == "resolve":
                acknowledged = getattr(exc, "committed_rows", 0)
                uncertain = bool(getattr(exc, "commit_uncertain", isinstance(exc, tx.CommitUncertain)))
                resolution_cause = getattr(exc, "resolution_cause", exc)
                control = control_flow(resolution_cause) or control_flow(exc)
                if control is not None and control is not exc:
                    failure(control, phase)
            failure(exc, phase)
        finally:
            # Disposal interruption cannot bypass the outer descriptor finally.
            owned_engine, engine = engine, None
            if owned_engine is not None:
                try:
                    owned_engine.dispose()
                except BaseException as exc:
                    failure(exc, "dispose")
        if error is None:
            try:
                phase = "render_report"
                rendered = json.dumps(report, indent=2, sort_keys=True)
                if output is not None:
                    phase = "publish_report"
                    output.publish(rendered + "\n")
            except BaseException as exc:
                failure(exc, phase)
    finally:
        if output is not None:
            owned_output, output = output, None
            try:
                owned_output.close()
            except BaseException as exc:
                failure(exc, "close_report")
    if error is None:
        try:
            print(rendered, flush=True)
        except BaseException as exc:
            stdout_failed = True
            failure(exc, "stdout")
    if error is None:
        return 0

    error.committed_rows = acknowledged
    error.commit_uncertain = uncertain
    error.resolution_cause = resolution_cause
    error.resolution_completed = completed
    error.failure_phase = failed_phase
    error.secondary_errors = tuple(secondary)
    reporting_errors = []
    try:
        receipt = json.dumps({
            "status": "ABORTED", "error_type": type(error).__name__,
            "reason": str(error) if isinstance(error, ValueError) and failed_phase == "resolve" else "database/audit/publication failure; inspect private evidence",
            "acknowledged_committed_rows": acknowledged, "commit_uncertain": uncertain,
            "resolution_error_type": type(resolution_cause).__name__ if resolution_cause is not None else None,
            "resolution_completed": completed, "failure_phase": failed_phase,
            "secondary_error_types": [{"phase": where, "error_type": type(exc).__name__} for where, exc in secondary],
            "action": "stop; reconcile database and audit before a separately reviewed retry",
        })
    except BaseException as exc:
        # No receipt exists: preserve the original, with no recursive rendering.
        error.reporting_errors = (exc,)
        raise error
    try:
        print(receipt, file=sys.stderr, flush=True)
    except BaseException as exc:
        reporting_errors.append(exc)
        if isinstance(exc, Exception) and not stdout_failed and sys.stdout is not sys.stderr:
            try:
                print(receipt, file=sys.stdout, flush=True)
            except BaseException as other:
                reporting_errors.append(other)
            else:
                error.reporting_errors = tuple(reporting_errors)
                if interrupted or failed_phase == "acquire_database":
                    raise error
                return 5
        error.reporting_errors = tuple(reporting_errors)
        raise error
    if interrupted or failed_phase == "acquire_database":
        raise error
    return 3 if isinstance(error, WindowClosed) and not secondary else 5

def require_guard_closed() -> None:
    """Refuse ``--apply`` / ``--revert`` once the transition marker exists.

    Until the marker is created the writer does not pull the guarded endpoints, so
    nothing can land between the dry run and the apply (a pull in between creates
    keyed duplicates next to the legacy rows, or overwrites a calendar-dated
    gov_contracts row with the next quarter's payload). A marker that already exists
    means pulls may already be flowing.
    """
    if transition_marker_exists():
        raise RuntimeError(
            f"refusing to write: the transition marker {transition_marker_path()} exists, so QuiverQuant "
            "pulls may already be running. Remove the marker to re-close the guard (and confirm the "
            "scheduler records quiverquant as SKIPPED), or pass --no-guard-check if QuiverQuant is "
            "paused another way."
        )


def is_lock_or_timeout(exc: BaseException) -> bool:
    """True for a lock_not_available (55P03) or query_canceled (57014) database error.

    Raised when ``lock_timeout`` / ``statement_timeout`` fires, e.g. against a
    concurrent uncommitted write to the same row. The scripts report and skip.
    """
    if not isinstance(exc, OperationalError):
        return False
    return getattr(getattr(exc, "orig", None), "pgcode", None) in {"55P03", "57014"}
