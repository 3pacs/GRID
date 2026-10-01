"""Point-in-time outcome resolution from close prices with honest availability.

A price source answers one question: *the close of instrument X on session
date D as it was observable at instant ``now``*. It returns a
:class:`PriceObs` whose ``available_at`` is the instant the value became
observable -- never earlier than the session's close -- or ``None`` if no
such value was observable yet. :func:`resolve_price_call` then refuses any
observation that claims to be available before the horizon's close or after
``now`` (:class:`~evals.e2.records.PriceSourceLookAhead`), so a buggy or hostile
source cannot slip a future price into a score.

Database sources open nothing themselves: they take a connection the caller
opened with :func:`read_only_connection` (``default_transaction_read_only``
on, short statement timeout) and only ``SELECT``.
"""

from __future__ import annotations

import contextlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Protocol

from evals.e2 import scoring
from evals.e2.records import PriceSourceLookAhead, iso, parse_ts, session_close_utc


@dataclass(frozen=True)
class PriceObs:
    instrument: str
    obs_date: date
    value: float
    available_at: datetime
    source: str
    series_id: str
    vintage: str  # pull timestamp / receipt creation time of the value used

    def receipt(self) -> dict:
        return {"instrument": self.instrument, "obs_date": self.obs_date.isoformat(), "value": self.value,
                "available_at": iso(self.available_at), "source": self.source, "series_id": self.series_id,
                "vintage": self.vintage}


class PriceSource(Protocol):
    name: str

    def close(self, instrument: str, obs_date: date, now: datetime) -> PriceObs | None: ...


class StaticPriceSource:
    """In-memory closes with explicit availability (tests and offline dry runs)."""

    name = "static"

    def __init__(self, rows: Iterable[tuple[str, date, float, datetime, str]]) -> None:
        # (instrument, obs_date, value, available_at, vintage)
        self.rows = sorted(rows, key=lambda r: (r[0], r[1], r[3]))

    def close(self, instrument: str, obs_date: date, now: datetime) -> PriceObs | None:
        seen = [r for r in self.rows if r[0] == instrument and r[1] == obs_date and r[3] <= now]
        if not seen:
            return None
        inst, d, value, available_at, vintage = seen[-1]  # latest vintage observable at now
        return PriceObs(inst, d, float(value), available_at, self.name, f"STATIC:{inst}:close", vintage)


class KnownAtCloseSource:
    """Closes from ``raw_series`` via ``store.observations.read_window_known_at`` (pull evidence only).

    ``series_template`` defaults to ``YF:{instrument}:close`` and ``source`` must
    name ONE puller (``tiingo`` or ``yfinance``): that series id is written by
    several pullers and a mixed read is refused. A value is observable at
    ``max(pull_timestamp, session close)``; vintages pulled after ``now`` are
    ignored (the reader's day bound is tightened to the instant here).
    """

    def __init__(self, conn, *, source: str, series_template: str = "YF:{instrument}:close") -> None:
        if not source:
            raise ValueError("a single named source is required")
        self.conn, self.source_name, self.series_template = conn, source, series_template
        self.name = f"raw_series:{source}"

    def close(self, instrument: str, obs_date: date, now: datetime) -> PriceObs | None:
        from store.observations import read_window_known_at

        series_id = self.series_template.format(instrument=instrument)
        today = now.astimezone(timezone.utc).date()
        obs = None
        # read_window_known_at bounds by UTC *day*; tighten to the instant. If the latest
        # vintage known by the end of today was pulled after `now`, fall back to the vintage
        # known by the end of yesterday (anything pulled earlier today is then simply not
        # used until the next run: late, never early).
        for as_of in (today, today - timedelta(days=1)):
            if as_of < obs_date:
                break
            rows = read_window_known_at(self.conn, series_id, as_of=as_of, lag=None,
                                        source=self.source_name, start=obs_date)
            rows = [o for o in rows if o.obs_date == obs_date and o.pull_timestamp is not None]
            if rows and _aware(rows[0].pull_timestamp) <= now:
                obs = rows[0]
                break
        if obs is None:
            return None
        pulled = _aware(obs.pull_timestamp)
        available = max(pulled, session_close_utc(obs_date))
        if available > now:
            return None
        return PriceObs(instrument, obs_date, float(obs.value), available, self.name, series_id, iso(pulled))


class SpyCloseReceiptSource:
    """SPY closes from ``astrogrid.price_close_receipt`` (contract ``spy_close_v1``)."""

    name = "astrogrid.price_close_receipt:spy_close_v1"
    SQL = ("SELECT obs_date, value, available_at, created_at FROM astrogrid.price_close_receipt "
           "WHERE contract_version = 'spy_close_v1' AND obs_date = :d "
           "AND available_at <= :now AND created_at <= :now ORDER BY created_at DESC LIMIT 1")

    def __init__(self, conn) -> None:
        self.conn = conn

    def close(self, instrument: str, obs_date: date, now: datetime) -> PriceObs | None:
        if instrument != "SPY":
            return None
        from sqlalchemy import text

        row = self.conn.execute(text(self.SQL), {"d": obs_date, "now": now}).fetchone()
        if row is None:
            return None
        available_at, created_at = (_aware(row[2]), _aware(row[3]))
        available = max(available_at, created_at, session_close_utc(obs_date))
        if available > now:
            return None
        return PriceObs("SPY", obs_date, float(row[1]), available, self.name, "YF:SPY:close", iso(created_at))


def _aware(ts: datetime) -> datetime:
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


@contextlib.contextmanager
def read_only_connection(db_url: str, statement_timeout_ms: int = 30_000):
    """A NullPool connection with ``default_transaction_read_only=on``; verified before yielding."""
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import NullPool

    engine = create_engine(
        db_url, poolclass=NullPool,
        connect_args={"options": f"-c default_transaction_read_only=on -c statement_timeout={int(statement_timeout_ms)}"},
    )
    try:
        with engine.connect() as conn:
            if conn.execute(text("SHOW default_transaction_read_only")).scalar() != "on":
                raise RuntimeError("E2 price reads require a read-only session")
            yield conn
    finally:
        engine.dispose()


def resolve_price_call(pred: dict, prices: PriceSource | None, now: datetime, rules: dict) -> dict | None:
    """Resolve a direction / probability / rank_score call from entry and exit closes.

    Returns a resolution dict, or ``None`` while the outcome is not yet observable.
    """
    if prices is None:
        return None
    horizon, target = pred["horizon"], pred["target"]
    instrument = target["instrument"]
    entry_date = date.fromisoformat(horizon["entry_date"])
    exit_date = date.fromisoformat(horizon["exit_date"])
    exit_close = session_close_utc(exit_date)
    if now < exit_close:
        return None
    entry = prices.close(instrument, entry_date, now)
    exit_ = prices.close(instrument, exit_date, now)
    grace = timedelta(days=int(rules["resolution"]["grace_days"]))
    if entry is None or exit_ is None:
        if now >= exit_close + grace:
            return {"status": "void", "reason": "price_missing", "outcome": None, "available_at": iso(now),
                    "receipt": {"price_source": prices.name, "entry": entry.receipt() if entry else None,
                                "exit": exit_.receipt() if exit_ else None}}
        return None
    for obs, close_at in ((entry, session_close_utc(entry_date)), (exit_, exit_close)):
        if obs.available_at < close_at:
            raise PriceSourceLookAhead(f"{pred['prediction_id']}: {obs.source} offered the {obs.obs_date} close as "
                                 f"available at {iso(obs.available_at)}, before the session closed")
        if obs.available_at > now:
            raise PriceSourceLookAhead(f"{pred['prediction_id']}: {obs.source} offered a close not observable until "
                                 f"{iso(obs.available_at)} at run instant {iso(now)}")
    ret = exit_.value / entry.value - 1.0
    outcome = {"entry_price": entry.value, "exit_price": exit_.value, "return": ret}
    call = pred["call"]
    if call["kind"] == "direction" and scoring.direction_hit(int(call["side"]), ret) is None:
        return {"status": "void", "reason": "push", "outcome": outcome, "available_at": iso(exit_.available_at),
                "receipt": {"price_source": prices.name, "entry": entry.receipt(), "exit": exit_.receipt()}}
    if call["kind"] == "probability":
        outcome["y"] = 1 if ret > 0 else 0
    return {"status": "resolved", "reason": None, "outcome": outcome,
            "available_at": iso(max(entry.available_at, exit_.available_at)),
            "receipt": {"price_source": prices.name, "entry": entry.receipt(), "exit": exit_.receipt()}}


def entry_close_after_issue(pred: dict) -> bool:
    """The entry close must come after the prediction was logged (executable, not hindsight)."""
    return session_close_utc(date.fromisoformat(pred["horizon"]["entry_date"])) > parse_ts(pred["issued_at"])
