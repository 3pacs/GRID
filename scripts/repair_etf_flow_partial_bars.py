#!/usr/bin/env python3
"""Repair ETF flow rows that were stored from a partial or unsettled bar.

Until GRID #1496 (2026-11-27) the ETF flow writer stored the session still
trading, with the volume traded so far, and never revised it. Every
``ETF_FLOW:<ETF>:{5d,20d,accel}`` date from 2026-03-30 on was first written on
its own New York date; rows written the next morning before the provider's
volume settled are short too, and on 2026-07-01 the provider served wrong
mid-session prices. The fixed writer revises the last ``_ETF_REVISION_DAYS``
by itself. This job repairs the older dates, once, as new vintages.

What a repaired value is
------------------------
The value the writer would have stored from settled bars **on the price basis
of the run that wrote the row being corrected**. The writer multiplies volume
by yfinance's dividend-adjusted close, and that adjustment moves every past
close on each dividend. Recomputing on today's basis (the writer's
``revise_from``) would also move rows that are correct, by the dividends paid
since, which for TLT and HYG is as large as many of the volume corrections.

The run's dividend state is ``S``, the last session before the New York date
of the write: the provider applies a dividend to the adjusted history from
the day after its ex-date, even to a request made after that day's close.
Each bar ``k`` of the window is priced as that run saw it::

    close_S(k) = AdjClose_now(k) * RawClose(S) / AdjClose_now(S)   for k <= S
    close_S(k) = RawClose(k)                                       for k > S

(the second line is the session the run was trading, on its ex-dividend day
not yet adjusted), and ``settled(D)`` is the writer's arithmetic on
``close_S(k) * settled_volume(k)``. Exact algebra: the adjusted close of bar
``k`` as of ``S`` is the first line, and the 5d, 20d and accel values are
linear in it. Measured on production 2026-11-27: the formula reproduces the
51,678 backfill vintages (dates 2020-01-02..2026-03-26) to 2.5e-6 of the 5d
value at worst, and the 198 revisions the fixed writer wrote that day
exactly; on the 31 own-day rows written on an ex-dividend date, the 23 whose
other bars were settled match with ``S`` the day before and none with the day
itself (found by the partner review). ``TOLERANCE`` (1e-5) sits above that
noise floor.

So on an ex-dividend date the repaired row keeps the pre-dividend basis it was
written on: it differs from what a writer running the next morning would
store by that dividend (0.1 % to 0.7 %). The repair changes the bars that were
wrong, not the basis.

Every repair row records ``S`` and the two closes used, so the arithmetic
can be checked later from the row alone.

The series stays on mixed price bases, as it already was: dates before
2026-03-27 on the basis of their 2026-03-28 backfill, repaired dates on their
own day's, and dates the scheduled writer revises on the basis of its last
run inside its 14-day window. Splits do not enter (dollar volume is split
invariant), but an ETF with a split in the fetched range is refused anyway.

A date the scheduled writer fails to revise inside its window becomes
eligible here the day it leaves the window (the default ``--to`` follows the
window), but only for a plan made after that: a follow-up plan about 15 days
after a repair shows any date the writer missed.

Modes
-----
``plan`` reads only: the database in a read-only transaction,
the provider once per ETF. It writes a plan file: every (series, date) whose
newest stored vintage differs from its settled value by more than the
tolerance, with the vintage it revises and the value to write, and a digest.

``apply`` writes exactly the plan's rows, nothing it computes anew, and
refuses a plan in which an ETF was not planned in full (provider failure,
split, deadline) unless ``--allow-incomplete-plan`` is given. Per
series, in one transaction under the ETF flow write lock of GRID #1501
(``_serialize_etf_write``): the newest stored vintage of each planned date is
read again; the row is written only when that is still the vintage the plan
revises. A date someone revised since is ``superseded`` and left alone; a
date this plan already wrote is ``already_applied``. Rows are dated with the
database clock read after the lock (``received_at``). It is resumable: run
the same command again after a deadline stop, a busy lock or a failure, and
it writes only what is still missing.

``plan`` fails closed on the calendar: the basis session ``S`` comes from the
exchange calendar (``ingestion.market_calendar``), and every exchange session
of a row's window (5, 20 or 10 sessions ending at its date) and ``S`` must be
a bar the provider returned, with no provider bar on a closed day inside the
window. Otherwise the ETF is REFUSED with the sessions named, and ``apply``
refuses the plan. A holiday is not a gap.

``verify --expect-digest D`` reads only and reports integrity, completion
and supersession apart (see ``verify_plan``): it exits 0 only for a plan that
is intact and fully applied (or superseded within the tolerance), 1 on an
integrity problem, 4 for an intact plan not yet fully applied, with a repair
row quarantined since, or with plan gaps (an ETF not planned in full, rows it
could not price) unless ``--accept-plan-gaps``, with items undone unless
``--accept-undone``, or with later vintages further than the tolerance from
the plan unless ``--accept-superseded``. ``apply`` refuses to write again a
repair row someone quarantined unless ``--reapply-quarantined``.
``undo --expect-digest D`` appends, for every planned date whose newest
vintage is still this plan's own repair row (marker naming digest ``D`` and
the revised vintage), a vintage restoring that original row: its value and
payload are read again in the locked transaction, and the row must still be
exactly the one the plan names (source, series, date, value, time, not
quarantined), or that date is refused and nothing is written for it. Dry
unless ``--apply``.

What readers see afterwards
---------------------------
Nothing is overwritten: the old vintage stays and the repair's is newer.

* Current views take the newest vintage: the ``etf_flows`` table after the
  next flow materializer run (its newest 20,000 ticker-dates reach back past
  2026-03-27), the ``etf_flow_spy_*`` features after the next resolver run
  (it reads raw rows by pull time), the flow map and sentiment for today.
  The ETF rows of ``dollar_flows`` are rebuilt only for the last 7 days by
  its daily timer; older dates keep the partial value until a wider
  normalization run, a separate step.
* A point-in-time read for an instant before the repair
  (``read_window_known_at``, a flow map for a past ``as_of``, a replay or an
  evaluation) still returns the partial value, because that is what GRID held
  then. The repair corrects today's view of the past and what is decided from
  now on. It does not change what history saw, and it must not be presented
  as if it did.

Every repair row's payload carries ``revises_value`` (as the writer's own
revisions do) and a ``repair`` object naming the run, the plan digest, the
vintage it revises and the price basis used, so the rows can be told apart,
audited and undone.

Usage (see the run sheet for the production step)::

    python scripts/repair_etf_flow_partial_bars.py plan --plan-out PLAN.json
    python scripts/repair_etf_flow_partial_bars.py apply --plan PLAN.json --expect-digest HEX
    python scripts/repair_etf_flow_partial_bars.py verify --plan PLAN.json
    python scripts/repair_etf_flow_partial_bars.py undo --plan PLAN.json [--apply]

Exit codes: 0 done; 1 a failure (provider, database, a row undo refused, a
verify integrity problem); 2 refused (usage, digest, scope); 3 stopped early
and resumable (deadline, lock busy); 4 verify: intact but not fully applied.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import secrets
import sys
import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
from sqlalchemy import bindparam, text  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402

from ingestion import market_calendar as xnys  # noqa: E402
from ingestion.altdata import institutional_flows as inst  # noqa: E402

RUN_KIND = "etf_flow_partial_bar_repair"
METRICS: tuple[str, ...] = ("5d", "20d", "accel")
# The first date measured off on production (the backfill's last date, written
# the next morning before volume settled). Every earlier stored date matches
# its settled value on its own basis to within the noise floor.
REPAIR_FROM_DEFAULT = date(2026, 3, 27)
# Relative to the settled 5d value of the same ETF and date (accel can be
# near zero, so it is measured on the 5d scale too).
TOLERANCE = 1e-5
# Bars fetched before the first date: the 20-day window and the acceleration
# need 19 and 9 sessions before it; 90 calendar days hold at least 55.
FETCH_PAD_DAYS = 90
PROVIDER_DELAY_S = 1.0
PROVIDER_ATTEMPTS = 3
PROVIDER_BACKOFF_S = (5.0, 15.0)
DEFAULT_DEADLINE_MIN = 15.0

_EXIT_OK, _EXIT_FAILED, _EXIT_REFUSED, _EXIT_STOPPED, _EXIT_INCOMPLETE = 0, 1, 2, 3, 4

# Sessions each metric's value is built from, ending at its own date.
WINDOW_SESSIONS: dict[str, int] = {
    "5d": inst._FLOW_WINDOW_5D, "20d": inst._FLOW_WINDOW_20D, "accel": 2 * inst._FLOW_WINDOW_5D,
}

_monotonic = time.monotonic
_sleep = time.sleep


class Refused(RuntimeError):
    """The job will not run as asked (scope, digest, plan file)."""


class ProviderError(RuntimeError):
    """The provider gave no usable bars for an ETF."""


# ── Provider and arithmetic ──────────────────────────────────────────────


def fetch_settled_bars(yf: Any, ticker: str, start: date, session_day: date) -> pd.DataFrame:
    """One ETF's finished sessions from ``start``, raw and adjusted.

    Returns a frame indexed by session date with ``raw_close``, ``adj_close``,
    ``volume`` and ``split``. The session of ``session_day`` (New York's
    current date) and anything after it is dropped: it may still be trading.
    """
    last_exc: Exception | None = None
    for attempt in range(PROVIDER_ATTEMPTS):
        if attempt:
            _sleep(PROVIDER_BACKOFF_S[min(attempt - 1, len(PROVIDER_BACKOFF_S) - 1)])
        try:
            data = yf.download(
                ticker, start=str(start), end=str(session_day + timedelta(days=1)),
                progress=False, auto_adjust=False, actions=True,
            )
        except Exception as exc:                       # network, provider
            last_exc = exc
            continue
        if data is None or data.empty:
            last_exc = ProviderError(f"no bars for {ticker} from {start}")
            continue
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        missing = {"Close", "Adj Close", "Volume"} - set(data.columns)
        if missing:
            raise ProviderError(f"{ticker}: provider omitted {sorted(missing)}")
        frame = pd.DataFrame({
            "raw_close": data["Close"].astype(float).values,
            "adj_close": data["Adj Close"].astype(float).values,
            "volume": data["Volume"].astype(float).values,
            "split": (data["Stock Splits"].astype(float).values
                      if "Stock Splits" in data.columns else [0.0] * len(data)),
        }, index=[inst._bar_date(s) for s in data.index])
        frame = frame[[d < session_day for d in frame.index]]
        frame = frame[~frame.index.duplicated(keep="last")].sort_index()
        if frame.empty:
            raise ProviderError(f"{ticker}: no finished session from {start}")
        return frame
    raise ProviderError(f"{ticker}: {last_exc}")


def window_value(
    bars: pd.DataFrame, metric: str, d_pos: int, s_pos: int, own_bar: tuple[float, float] | None = None,
) -> float:
    """The writer's ``metric`` for the bar at ``d_pos``, priced as of session ``s_pos``.

    ``bars`` is ``fetch_settled_bars``' frame. Each bar is ``close_S(k) x
    volume(k)`` (see the module docstring). ``own_bar``, a (close, volume)
    pair, stands in for the bar at ``d_pos`` itself: the stored row's own bar,
    to see how well the settled bars explain it. NaN when the window reaches
    before the first fetched bar.
    """
    adj = bars["adj_close"].to_numpy()
    raw = bars["raw_close"].to_numpy()
    vol = bars["volume"].to_numpy()
    factor = raw[s_pos] / adj[s_pos]

    def bar(k: int) -> float:
        if own_bar is not None and k == d_pos:
            return own_bar[0] * own_bar[1]
        close = adj[k] * factor if k <= s_pos else raw[k]
        return close * vol[k]

    def total(end: int, n: int) -> float:
        if end - n + 1 < 0:
            return math.nan
        return sum(bar(k) for k in range(end - n + 1, end + 1))

    if metric == "5d":
        return total(d_pos, inst._FLOW_WINDOW_5D)
    if metric == "20d":
        return total(d_pos, inst._FLOW_WINDOW_20D)
    return total(d_pos, inst._FLOW_WINDOW_5D) - total(d_pos - inst._FLOW_WINDOW_5D, inst._FLOW_WINDOW_5D)


def _as_utc(ts: Any) -> datetime:
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _as_date(d: Any) -> date:
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    return date.fromisoformat(str(d)[:10])


def price_basis_session(pull_ts: Any, sessions: Iterable[date] | None = None) -> date | None:
    """``S``: the last exchange session before the New York date of a write at ``pull_ts``.

    The dividend state of the provider's adjusted history at that write. Taken
    from the exchange calendar (``ingestion.market_calendar``), not from the
    bars a provider happened to return, so a missing bar cannot move it. None
    when ``sessions`` is given and does not contain it.
    """
    ny_day = _as_utc(pull_ts).astimezone(inst._NEW_YORK).date()
    basis = xnys.last_trading_day(ny_day - timedelta(days=1))
    if sessions is not None and basis not in set(sessions):
        return None
    return basis


def calendar_gaps(provider: list[date], d: date, metric: str, basis: date,
                  calendar: list[date], calendar_pos: dict[date, int]) -> tuple[list[date], list[date]] | None:
    """Exchange sessions the value of ``metric`` on ``d`` needs and the provider lacks, and
    provider bars inside that window that are not exchange sessions.

    The window is the ``WINDOW_SESSIONS[metric]`` exchange sessions ending at
    ``d``, plus the basis session ``basis``. None when the calendar list does
    not reach back that far (the caller widens it).
    """
    i = calendar_pos.get(d)
    n = WINDOW_SESSIONS[metric]
    if i is None or i - n + 1 < 0:
        return None
    needed = calendar[i - n + 1:i + 1]
    have = set(provider)
    missing = sorted({x for x in [*needed, basis] if x not in have})
    known = set(calendar)
    unexpected = sorted(x for x in provider if needed[0] <= x <= d and x not in known)
    return missing, unexpected


def _payload(raw: Any) -> dict[str, Any]:
    if isinstance(raw, (str, bytes)):
        try:
            raw = json.loads(raw)
        except ValueError:
            return {}
    return raw if isinstance(raw, dict) else {}


# ── Reading what is stored ───────────────────────────────────────────────

_NEWEST_SQL = text(
    "SELECT id, series_id, obs_date, value, pull_timestamp, raw_payload FROM ("
    " SELECT id, series_id, obs_date, value, pull_timestamp, raw_payload,"
    " ROW_NUMBER() OVER (PARTITION BY series_id, obs_date"
    "  ORDER BY pull_timestamp DESC, id DESC) AS newest_rank"
    " FROM raw_series WHERE source_id = :src AND series_id IN :sids"
    " AND obs_date >= :lo AND obs_date <= :hi AND pull_status != 'QUARANTINED'"
    ") newest WHERE newest_rank = 1"
).bindparams(bindparam("sids", expanding=True))


def newest_vintages(conn: Any, source_id: int, series_ids: list[str], lo: date, hi: date) -> dict[tuple[str, date], dict]:
    """Newest non-quarantined vintage per (series, date), as the writer reads it."""
    out: dict[tuple[str, date], dict] = {}
    for row in conn.execute(_NEWEST_SQL, {"src": source_id, "sids": series_ids, "lo": lo, "hi": hi}):
        out[(row[1], _as_date(row[2]))] = {
            "id": int(row[0]), "value": float(row[3]), "pull_timestamp": _as_utc(row[4]),
            "payload": _payload(row[5]),
        }
    return out


# One snapshot for every read in a read-only transaction: a commit between
# statements cannot make plan or verify compare states that never coexisted.
SNAPSHOT_ISOLATION = "REPEATABLE READ"


@contextmanager
def read_only_connection(engine: Engine) -> Iterator[Any]:
    """A connection whose transaction refuses writes and reads one snapshot (PostgreSQL),
    rolled back at the end.

    Uses the caller's engine, so its pool and session settings (statement
    timeout, search path) apply; only this transaction is read-only and
    ``SNAPSHOT_ISOLATION``. SQLite serialises a connection's reads anyway.
    """
    with engine.connect() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text(f"SET TRANSACTION ISOLATION LEVEL {SNAPSHOT_ISOLATION} READ ONLY"))
            if conn.execute(text("SHOW transaction_read_only")).scalar() != "on":
                raise Refused("the database did not make this transaction read-only")
            if conn.execute(text("SHOW transaction_isolation")).scalar() != SNAPSHOT_ISOLATION.lower():
                raise Refused(f"the database did not give this transaction {SNAPSHOT_ISOLATION}")
        try:
            yield conn
        finally:
            conn.rollback()


# The source the live ETF flow writer stores under (declared for the E1
# provenance gate in evals/e1/test_provenance.py, DERIVED_BASIS_WRITERS).
SOURCE_NAME = "INSTITUTIONAL_FLOWS"


def source_id_of(conn: Any) -> int:
    """The writer's source_catalog id, looked up without creating it."""
    if inst.InstitutionalFlowsPuller.SOURCE_NAME != SOURCE_NAME:
        raise Refused(f"the ETF flow writer now stores under {inst.InstitutionalFlowsPuller.SOURCE_NAME!r}, "
                      f"not {SOURCE_NAME!r}: this repair and its provenance declaration need updating")
    sid = conn.execute(
        text("SELECT id FROM source_catalog WHERE LOWER(name) = LOWER(:n)"), {"n": SOURCE_NAME},
    ).scalar()
    if sid is None:
        raise Refused(f"no source_catalog row for {SOURCE_NAME}")
    return int(sid)


def writer_floor(session_day: date) -> date:
    """The first date the scheduled writer still revises by itself."""
    return session_day - timedelta(days=inst._ETF_REVISION_DAYS)


# ── Plan ─────────────────────────────────────────────────────────────────


def _digest(plan: dict[str, Any]) -> str:
    """Over the whole plan except the digest itself: header, ticker report and items."""
    body = {k: v for k, v in plan.items() if k != "digest"}
    blob = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(blob).hexdigest()


def build_plan(
    engine: Engine,
    yf: Any,
    *,
    from_date: date = REPAIR_FROM_DEFAULT,
    to_date: date | None = None,
    session_day: date | None = None,
    tickers: Iterable[str] | None = None,
    deadline_at: float | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Read-only: what the repair would write, and why each other date is left."""
    session_day = session_day or inst._new_york_today()
    floor = writer_floor(session_day)
    to_date = to_date or (floor - timedelta(days=1))
    if to_date >= floor:
        raise Refused(f"--to {to_date} reaches the scheduled writer's window (from {floor}); it revises those")
    if from_date > to_date:
        raise Refused(f"--from {from_date} is after --to {to_date}")
    tickers = list(tickers or inst.ETF_FLOW_TICKERS)
    now = now or datetime.now(timezone.utc)
    run_id = f"etfpb-{now.strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(3)}"
    series_ids = [f"ETF_FLOW:{t}:{m}" for t in tickers for m in METRICS]
    with read_only_connection(engine) as conn:
        source_id = source_id_of(conn)
        stored = newest_vintages(conn, source_id, series_ids, from_date, to_date)

    items: list[dict] = []
    left: Counter = Counter()
    tickers_report: dict[str, dict] = {}
    fetch_start = from_date - timedelta(days=FETCH_PAD_DAYS)
    for position, ticker in enumerate(tickers):
        if deadline_at is not None and _monotonic() >= deadline_at:
            for t in tickers[position:]:
                tickers_report[t] = {"status": "NOT_ATTEMPTED", "reason": "deadline"}
            break
        if position:
            _sleep(PROVIDER_DELAY_S)
        try:
            bars = fetch_settled_bars(yf, ticker, fetch_start, session_day)
        except ProviderError as exc:
            tickers_report[ticker] = {"status": "FAILED", "error": str(exc)}
            continue
        fetched_at = datetime.now(timezone.utc).isoformat()
        splits = [d.isoformat() for d, v in bars["split"].items() if v and v != 0]
        if splits:
            tickers_report[ticker] = {"status": "REFUSED", "error": f"split(s) in the fetched range: {splits}"}
            continue
        sessions = list(bars.index)
        position_of = {d: k for k, d in enumerate(sessions)}
        calendar = xnys.trading_days_between(fetch_start - timedelta(days=60), session_day - timedelta(days=1))
        calendar_pos = {d: k for k, d in enumerate(calendar)}
        missing_sessions: set[date] = set()
        unexpected_sessions: set[date] = set()
        ticker_items: list[dict] = []
        counts: Counter = Counter()
        for metric in METRICS:
            sid = f"ETF_FLOW:{ticker}:{metric}"
            for (s_sid, obs_date), vint in sorted(stored.items()):
                if s_sid != sid:
                    continue
                if obs_date not in calendar_pos:
                    counts["not_an_exchange_session"] += 1
                    continue
                basis_session = price_basis_session(vint["pull_timestamp"])
                gaps = calendar_gaps(sessions, obs_date, metric, basis_session, calendar, calendar_pos)
                if gaps is None:
                    counts["not_computable"] += 1
                    continue
                if gaps[0] or gaps[1]:
                    # A bar the value needs is missing, or the provider served a
                    # day the exchange was closed: positions would shift and the
                    # window or basis would be wrong. Fail closed for the ETF.
                    missing_sessions.update(gaps[0])
                    unexpected_sessions.update(gaps[1])
                    continue
                d_pos = position_of[obs_date]
                s_pos = position_of[basis_session]
                if s_pos < d_pos - 1:
                    counts["basis_unknown"] += 1          # written before the session after it
                    continue
                value = window_value(bars, metric, d_pos, s_pos)
                scale = abs(window_value(bars, "5d", d_pos, s_pos))
                if not math.isfinite(value) or not math.isfinite(scale) or scale == 0:
                    counts["not_computable"] += 1
                    continue
                rel = (value - vint["value"]) / scale
                if abs(rel) <= TOLERANCE:
                    counts["within_tolerance"] += 1
                    continue
                old = vint["payload"]
                own = None
                try:
                    own = (float(old["close"]), float(old["volume"]))
                except (KeyError, TypeError, ValueError):
                    pass
                explained = (
                    (window_value(bars, metric, d_pos, s_pos, own) - vint["value"]) / scale
                    if own is not None else None
                )
                close = (float(bars["adj_close"].iat[d_pos]) * float(bars["raw_close"].iat[s_pos])
                         / float(bars["adj_close"].iat[s_pos])) if d_pos <= s_pos else float(bars["raw_close"].iat[d_pos])
                ticker_items.append({
                    "series_id": sid, "ticker": ticker, "metric": metric,
                    "obs_date": obs_date.isoformat(),
                    "value": value,
                    "close": close,
                    "volume": int(bars["volume"].iat[d_pos]),
                    "price_basis_session": basis_session.isoformat(),
                    "basis_raw_close": float(bars["raw_close"].iat[s_pos]),
                    "basis_adj_close_now": float(bars["adj_close"].iat[s_pos]),
                    # The stored row against the settled bars with its own bar
                    # put back: near 0 when only its own bar was wrong; larger
                    # when other bars of its window were unsettled too.
                    "stored_bar_residual": explained,
                    "scale_5d": scale,
                    "rel_change": rel,
                    "revises_id": vint["id"],
                    "revises_value": vint["value"],
                    "revises_pull_timestamp": vint["pull_timestamp"].isoformat(),
                    "revises_volume": old.get("volume"),
                    "revises_close": old.get("close"),
                    "fetched_at": fetched_at,
                })
                counts["planned"] += 1
        if missing_sessions or unexpected_sessions:
            tickers_report[ticker] = {
                "status": "REFUSED",
                "error": "the provider's bars do not match the exchange calendar for a window or basis "
                         "session a stored row needs",
                "missing_sessions": [d.isoformat() for d in sorted(missing_sessions)],
                "unexpected_sessions": [d.isoformat() for d in sorted(unexpected_sessions)],
            }
            continue
        items.extend(ticker_items)
        tickers_report[ticker] = {"status": "OK", "sessions": len(sessions), **dict(counts)}
        left.update({k: v for k, v in counts.items() if k != "planned"})

    items.sort(key=lambda i: (i["series_id"], i["obs_date"]))
    by_month: dict[str, Counter] = defaultdict(Counter)
    for i in items:
        by_month[i["obs_date"][:7]][i["metric"]] += 1
    changes = sorted(abs(i["rel_change"]) for i in items if i["metric"] == "5d")

    def q(p: float) -> float | None:
        return round(changes[min(len(changes) - 1, int(p * len(changes)))], 6) if changes else None

    plan = {
        "kind": RUN_KIND, "run_id": run_id, "planned_at": now.isoformat(),
        "session_day": session_day.isoformat(), "writer_floor": floor.isoformat(),
        "from": from_date.isoformat(), "to": to_date.isoformat(),
        "tolerance": TOLERANCE, "source_id": source_id,
        "tickers": tickers_report,
        "summary": {
            "rows": len(items),
            "series": len({i["series_id"] for i in items}),
            "dates": len({i["obs_date"] for i in items}),
            "by_month": {m: dict(c) for m, c in sorted(by_month.items())},
            "left_alone": dict(left),
            "rel_change_5d": {"p50": q(0.5), "p90": q(0.9), "p99": q(0.99), "max": changes[-1] if changes else None},
            "below_settled": sum(1 for i in items if i["value"] > i["revises_value"]),
        },
        "items": items,
    }
    plan["digest"] = _digest(plan)
    return plan


def load_plan(
    path: Path, expect_digest: str | None = None, *, require_complete: bool = False,
) -> dict[str, Any]:
    plan = json.loads(Path(path).read_text())
    if plan.get("kind") != RUN_KIND:
        raise Refused(f"{path} is not a {RUN_KIND} plan")
    actual = _digest(plan)
    if actual != plan.get("digest"):
        raise Refused(f"{path}: the plan does not match its digest (edited?)")
    if expect_digest is not None and actual != expect_digest:
        raise Refused(f"{path}: digest {actual} is not the expected {expect_digest}")
    incomplete = {t: r.get("status") for t, r in plan.get("tickers", {}).items() if r.get("status") != "OK"}
    if require_complete and incomplete:
        raise Refused(f"{path}: not every ETF was planned ({incomplete}); make a new plan, "
                      "or pass --allow-incomplete-plan to repair only the others")
    return plan


# ── Apply, verify, undo ──────────────────────────────────────────────────


def _repair_marker(plan: dict, item: dict) -> dict[str, Any]:
    return {
        "run_id": plan["run_id"], "kind": RUN_KIND, "plan_digest": plan["digest"],
        "revises_id": item["revises_id"], "revises_pull_timestamp": item["revises_pull_timestamp"],
        "revises_volume": item["revises_volume"], "revises_close": item["revises_close"],
        "price_basis_session": item["price_basis_session"],
        "basis_raw_close": item["basis_raw_close"], "basis_adj_close_now": item["basis_adj_close_now"],
        "fetched_at": item["fetched_at"],
    }


def _marker_matches(mark: dict, plan: dict, item: dict) -> bool:
    """A stored repair marker is this plan's row for this item."""
    return (mark.get("run_id") == plan["run_id"] and mark.get("kind") == RUN_KIND
            and mark.get("plan_digest") == plan["digest"]
            and mark.get("revises_id") == item["revises_id"]
            and mark.get("revises_pull_timestamp") == item["revises_pull_timestamp"])


_ORIGINAL_SQL = text(
    "SELECT id, source_id, series_id, obs_date, value, pull_timestamp, raw_payload, pull_status "
    "FROM raw_series WHERE id = :i"
)


def _original_row(conn: Any, row_id: int) -> dict | None:
    r = conn.execute(_ORIGINAL_SQL, {"i": row_id}).fetchone()
    if r is None:
        return None
    return {"id": int(r[0]), "source_id": int(r[1]), "series_id": r[2], "obs_date": _as_date(r[3]),
            "value": float(r[4]), "pull_timestamp": _as_utc(r[5]), "raw_payload": r[6], "pull_status": r[7]}


def _original_problem(original: dict | None, source_id: int, series_id: str, obs_date: date,
                      item: dict) -> str | None:
    """Why the stored row a plan item revised is not exactly the row the plan names, or None."""
    if original is None:
        return "missing"
    if (original["source_id"], original["series_id"], original["obs_date"]) != (source_id, series_id, obs_date):
        return "elsewhere"
    if not math.isclose(original["value"], item["revises_value"], rel_tol=1e-12):
        return "value_changed"
    if original["pull_timestamp"] != _as_utc(item["revises_pull_timestamp"]):
        return "time_changed"
    if original["pull_status"] == "QUARANTINED":
        return "quarantined"
    return None


def _by_series(items: list[dict]) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for i in items:
        out[i["series_id"]].append(i)
    return dict(sorted(out.items()))


def _progress(path: Path | None, line: dict) -> None:
    text_line = json.dumps(line, sort_keys=True, default=str)
    print("PROGRESS " + text_line, flush=True)
    if path is not None:
        with Path(path).open("a") as fh:
            fh.write(text_line + "\n")


def _write_series(
    engine: Engine,
    puller: Any,
    plan: dict,
    series_id: str,
    items: list[dict],
    *,
    mode: str,
    budget: Any,
    session_day: date,
    reapply_quarantined: bool = False,
) -> Counter:
    """One series, one transaction, under the ETF flow write lock.

    ``mode`` is ``apply`` (write the plan's value where the plan's vintage is
    still the newest) or ``undo`` (where this plan's own repair row is still
    the newest, write back the original vintage it revised, read again inside
    this transaction and required to be exactly the row the plan names).
    """
    run_id = plan["run_id"]
    floor = writer_floor(session_day)
    counts: Counter = Counter()
    dates = [date.fromisoformat(i["obs_date"]) for i in items]
    with engine.begin() as conn:
        received_at = puller._serialize_etf_write(conn, budget)
        newest = newest_vintages(conn, puller.source_id, [series_id], min(dates), max(dates))
        quarantined = {_as_date(r[0]) for r in conn.execute(text(
            "SELECT obs_date FROM raw_series WHERE source_id = :src AND series_id = :sid "
            "AND pull_status = 'QUARANTINED' AND CAST(raw_payload AS TEXT) LIKE :pat"),
            {"src": puller.source_id, "sid": series_id, "pat": f"%{run_id}%"})}
        for item, obs_date in zip(items, dates):
            current = newest.get((series_id, obs_date))
            if obs_date >= floor:
                counts["in_writer_window"] += 1
                continue
            mark = (current or {}).get("payload", {}).get("repair") or {}
            mine = mark.get("run_id") == run_id
            if mode == "apply":
                if current is not None and mine:
                    counts["already_applied"] += 1
                    continue
                if current is None or current["id"] != item["revises_id"]:
                    counts["superseded"] += 1
                    continue
                if obs_date in quarantined and not reapply_quarantined:
                    # Someone quarantined this plan's row: a retraction. Writing
                    # it again would silently re-impose a value judged bad.
                    counts["refused_repair_was_quarantined"] += 1
                    continue
                payload = {
                    "ticker": item["ticker"], "close": item["close"], "volume": item["volume"],
                    "revises_value": current["value"], "repair": _repair_marker(plan, item),
                }
                value = item["value"]
            else:
                if current is None or not mine:
                    counts["not_newest"] += 1
                    continue
                if not _marker_matches(mark, plan, item) or not math.isclose(
                        current["value"], item["value"], rel_tol=1e-12):
                    counts["refused_repair_row_mismatch"] += 1
                    continue
                original = _original_row(conn, item["revises_id"])
                problem = _original_problem(original, puller.source_id, series_id, obs_date, item)
                if problem:
                    counts[f"refused_original_{problem}"] += 1
                    continue
                payload = dict(_payload(original["raw_payload"]))
                # The original may itself be an earlier run's repair or undo row:
                # keep its marker as lineage, under a name no verify reads as live.
                for live, kept in (("repair", "restored_repair"), ("repair_undo", "restored_repair_undo")):
                    if live in payload:
                        payload[kept] = payload.pop(live)
                payload["revises_value"] = current["value"]
                payload["repair_undo"] = {"run_id": run_id, "kind": RUN_KIND, "plan_digest": plan["digest"],
                                          "undoes_id": current["id"], "restores_id": original["id"]}
                value = original["value"]
            puller._insert_raw(
                conn=conn, series_id=series_id, obs_date=obs_date, value=value,
                raw_payload=payload, received_at=received_at,
            )
            counts["written"] += 1
    return counts


def run_writes(
    engine: Engine,
    plan: dict,
    *,
    mode: str = "apply",
    deadline_at: float | None = None,
    progress_path: Path | None = None,
    session_day: date | None = None,
    expect_digest: str | None = None,
    reapply_quarantined: bool = False,
) -> dict[str, Any]:
    """Write the plan (``apply``) or its undo, series by series; resumable.

    ``undo`` writes only with ``expect_digest`` equal to the plan's digest,
    recomputed here: the approval names the exact plan being undone.
    """
    session_day = session_day or inst._new_york_today()
    if mode == "undo" and (expect_digest is None or expect_digest != _digest(plan) or expect_digest != plan["digest"]):
        raise Refused("undo writes only for the exact approved plan: pass its digest")
    puller = inst.InstitutionalFlowsPuller(engine)
    if puller.source_id != plan["source_id"]:
        raise Refused(f"plan was made for source_id {plan['source_id']}, this database has {puller.source_id}")
    budget = inst._RunBudget(deadline_at=deadline_at)
    totals: Counter = Counter()
    status = "DONE"
    series = _by_series(plan["items"])
    for position, (series_id, items) in enumerate(series.items()):
        if budget.spent():
            status = "STOPPED_AT_DEADLINE"
            _progress(progress_path, {"mode": mode, "run_id": plan["run_id"], "event": "deadline",
                                      "series_done": position, "series_total": len(series)})
            break
        started = _monotonic()
        try:
            counts = _write_series(engine, puller, plan, series_id, items, mode=mode,
                                   budget=budget, session_day=session_day,
                                   reapply_quarantined=reapply_quarantined)
        except inst._EtfWriteLockBusy as exc:
            counts = Counter({"lock_busy": len(items)})
            status = "STOPPED_LOCK_BUSY" if status == "DONE" else status
            _progress(progress_path, {"mode": mode, "series": series_id, "event": "lock_busy", "error": str(exc)})
        totals.update(counts)
        _progress(progress_path, {
            "mode": mode, "run_id": plan["run_id"], "series": series_id, "planned": len(items),
            "position": position + 1, "series_total": len(series),
            "seconds": round(_monotonic() - started, 3), **dict(counts),
        })
    if status == "DONE" and any(k.startswith("refused_") for k in totals):
        status = "DONE_WITH_REFUSED_ROWS"
    return {"mode": mode, "run_id": plan["run_id"], "status": status, "totals": dict(totals),
            "planned": len(plan["items"])}


# What a plan may leave alone and still have repaired everything it found wrong.
BENIGN_LEFT_ALONE = frozenset({"within_tolerance"})


def plan_gaps(plan: dict) -> dict[str, Any]:
    """What the plan itself did not cover: ETFs not planned in full, rows it could not price."""
    tickers = {t: r.get("status") for t, r in plan.get("tickers", {}).items() if r.get("status") != "OK"}
    left = {k: v for k, v in plan.get("summary", {}).get("left_alone", {}).items()
            if k not in BENIGN_LEFT_ALONE and v}
    return {"tickers_not_ok": tickers, "rows_left_unrepaired": left}


def verify_plan(engine: Engine, plan: dict, *, accept_plan_gaps: bool = False, accept_undone: bool = False,
                accept_superseded: bool = False) -> dict[str, Any]:
    """Read-only receipt of a plan against what is stored.

    Three answers, reported apart:

    * all three reads below come from one read-only snapshot
      (``read_only_connection``, REPEATABLE READ on PostgreSQL).
    * integrity (``integrity_ok``): every revised vintage is still the row the
      plan names; every row carrying this run id is a planned item's, once,
      with this plan's value and a marker naming this digest and that vintage;
      every undo row restores that vintage (``restores_id``, value) over this
      plan's row (``undoes_id``).
    * completion (``complete``): every item is ``applied``, ``undone``,
      ``applied_then_superseded`` or ``superseded_before_apply`` (another
      vintage became newest before the repair reached it). An item whose
      revised vintage is still the newest is ``not_applied``; one whose only
      repair row was later quarantined is ``repair_quarantined``. Both are
      incomplete. So is a plan with ``plan_gaps`` (an ETF not OK, or rows
      left alone for a reason other than being within tolerance), unless
      ``accept_plan_gaps``: those rows were never repaired.
      ``superseded_vs_plan`` says how far the vintages that superseded the
      plan's value are from it (on the date's 5d scale); one further than
      the tolerance is incomplete unless ``accept_superseded``. An ``undone``
      item is incomplete unless ``accept_undone``: after an undo the partial
      value is current again, so "ok" never means both "repaired" and
      "undone" (verify an undo with ``--accept-undone``, which passes only
      once no item is still ``applied``: the undo is finished).
    * ``ok`` is both. The command exits 0 only then: 1 on an integrity
      problem, 4 when the plan is intact but not (fully) applied.
    """
    run_id = plan["run_id"]
    lo = min(date.fromisoformat(i["obs_date"]) for i in plan["items"]) if plan["items"] else date.min
    hi = max(date.fromisoformat(i["obs_date"]) for i in plan["items"]) if plan["items"] else lo
    series_ids = sorted({i["series_id"] for i in plan["items"]})
    states: Counter = Counter()
    problems: list[str] = []
    with read_only_connection(engine) as conn:
        rows = conn.execute(text(
            "SELECT id, series_id, obs_date, value, raw_payload, pull_status, pull_timestamp, source_id "
            "FROM raw_series WHERE source_id = :src AND series_id IN :sids"
        ).bindparams(bindparam("sids", expanding=True)), {"src": plan["source_id"], "sids": series_ids or [""]}).fetchall()
        newest = newest_vintages(conn, plan["source_id"], series_ids or [""], lo, hi)
        stray_elsewhere = conn.execute(text(
            "SELECT COUNT(*) FROM raw_series WHERE source_id = :src AND series_id LIKE 'ETF_FLOW:%' "
            "AND series_id NOT IN :sids AND CAST(raw_payload AS TEXT) LIKE :pat"
        ).bindparams(bindparam("sids", expanding=True)),
            {"pat": f"%{run_id}%", "src": plan["source_id"], "sids": series_ids or [""]}).scalar()
    repairs: dict[tuple[str, date], list] = defaultdict(list)
    quarantined_repairs: dict[tuple[str, date], list] = defaultdict(list)
    undos: dict[tuple[str, date], list] = defaultdict(list)
    by_id = {}
    for r in rows:
        by_id[int(r[0])] = r
        p = _payload(r[4])
        key = (r[1], _as_date(r[2]))
        if (p.get("repair") or {}).get("run_id") == run_id:
            (quarantined_repairs if r[5] == "QUARANTINED" else repairs)[key].append((r, p["repair"]))
        if (p.get("repair_undo") or {}).get("run_id") == run_id:
            undos[key].append((r, p["repair_undo"]))
    planned_keys = set()
    supersessions: list[float] = []
    five_d: dict[date, dict[str, float]] = defaultdict(dict)
    for item in plan["items"]:
        if item["metric"] == "5d":
            five_d[date.fromisoformat(item["obs_date"])][item["ticker"]] = item["value"]
    for item in plan["items"]:
        key = (item["series_id"], date.fromisoformat(item["obs_date"]))
        planned_keys.add(key)
        old = by_id.get(item["revises_id"])
        original = None if old is None else {
            "id": int(old[0]), "source_id": int(old[7]), "series_id": old[1], "obs_date": _as_date(old[2]),
            "value": float(old[3]), "pull_timestamp": _as_utc(old[6]), "pull_status": old[5]}
        why = _original_problem(original, plan["source_id"], key[0], key[1], item)
        if why:
            problems.append(f"{key}: the revised vintage {item['revises_id']} is {why.replace('_', ' ')}")
        found = repairs.get(key, [])
        for r, mark in found:
            if not _marker_matches(mark, plan, item):
                problems.append(f"{key}: repair row {r[0]} carries a marker that is not this plan's for this item")
            if not math.isclose(float(r[3]), item["value"], rel_tol=1e-12):
                problems.append(f"{key}: repair row {r[0]} value differs from the plan")
        if len(found) > 1:
            problems.append(f"{key}: written {len(found)} times")
        for r, mark in undos.get(key, []):
            if (not found or mark.get("undoes_id") != int(found[0][0][0])
                    or mark.get("restores_id") != item["revises_id"]
                    or mark.get("plan_digest") != plan["digest"]
                    or not math.isclose(float(r[3]), item["revises_value"], rel_tol=1e-12)):
                problems.append(f"{key}: undo row {r[0]} does not restore this plan's revised value")
        for r, mark in quarantined_repairs.get(key, []):
            if not _marker_matches(mark, plan, item):
                problems.append(f"{key}: repair row {r[0]} carries a marker that is not this plan's for this item")
        cur = newest.get(key)
        superseded = False
        if found:
            repair_id = int(found[0][0][0])
            if cur is not None and cur["id"] == repair_id:
                states["applied"] += 1
            elif cur is not None and (cur["payload"].get("repair_undo") or {}).get("undoes_id") == repair_id:
                states["undone"] += 1
            else:
                states["applied_then_superseded"] += 1
                superseded = True
        elif quarantined_repairs.get(key):
            states["repair_quarantined"] += 1            # written, then quarantined: the repair is not in force
        elif cur is not None and cur["id"] == item["revises_id"]:
            states["not_applied"] += 1
        else:
            states["superseded_before_apply"] += 1
            superseded = True
        if superseded and cur is not None:
            scale = abs(item.get("scale_5d") or five_d.get(key[1], {}).get(item["ticker"], item["value"])) or 1.0
            supersessions.append(abs(cur["value"] - item["value"]) / scale)
    stray = [k for k in list(repairs) + list(quarantined_repairs) + list(undos) if k not in planned_keys]
    if stray:
        problems.append(f"{len(stray)} row(s) carry this run id outside the plan")
    if stray_elsewhere:
        problems.append(f"{stray_elsewhere} row(s) outside the plan's series mention this run id")
    integrity_ok = not problems
    gaps = plan_gaps(plan)
    has_gaps = bool(gaps["tickers_not_ok"] or gaps["rows_left_unrepaired"])
    over = sum(1 for x in supersessions if x > TOLERANCE)
    complete = (states["not_applied"] == 0 and states["repair_quarantined"] == 0
                and (accept_plan_gaps or not has_gaps)
                and (states["undone"] == 0 or (accept_undone and states["applied"] == 0))
                and (accept_superseded or over == 0))
    return {"run_id": run_id, "digest": plan["digest"], "planned": len(plan["items"]),
            "states": {k: states[k] for k in ("applied", "undone", "applied_then_superseded",
                                              "superseded_before_apply", "not_applied", "repair_quarantined")},
            "plan_gaps": gaps, "plan_gaps_accepted": bool(accept_plan_gaps and has_gaps),
            "repair_in_force": states["applied"],
            "accepted": {"undone": accept_undone and states["undone"] > 0,
                         "superseded_over_tolerance": accept_superseded and over > 0},
            "superseded_vs_plan": {"count": len(supersessions),
                                   "max_rel_diff_of_5d": max(supersessions) if supersessions else None,
                                   "over_tolerance": over},
            "integrity_ok": integrity_ok, "complete": complete, "ok": integrity_ok and complete,
            "problems": problems}


# ── CLI ──────────────────────────────────────────────────────────────────


def _print_plan(plan: dict) -> None:
    s = plan["summary"]
    print(f"PLAN {plan['run_id']}  digest {plan['digest']}")
    print(f"  dates {plan['from']} .. {plan['to']} (scheduled writer revises from {plan['writer_floor']})")
    print(f"  rows to write {s['rows']} across {s['series']} series and {s['dates']} dates")
    for t, r in plan["tickers"].items():
        print(f"  {t:4} {json.dumps(r, sort_keys=True)}")
    for m, c in s["by_month"].items():
        print(f"  {m} {json.dumps(c, sort_keys=True)}")
    print(f"  left alone {json.dumps(s['left_alone'], sort_keys=True)}")
    print(f"  5d relative change {json.dumps(s['rel_change_5d'])}; stored below settled: {s['below_settled']}")


def main(argv: list[str] | None = None, *, yf: Any = None, engine: Engine | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    sub = ap.add_subparsers(dest="mode")
    p = sub.add_parser("plan")
    p.add_argument("--plan-out", required=True, type=Path)
    p.add_argument("--from", dest="from_date", type=date.fromisoformat, default=REPAIR_FROM_DEFAULT)
    p.add_argument("--to", dest="to_date", type=date.fromisoformat)
    p.add_argument("--deadline-minutes", type=float, default=DEFAULT_DEADLINE_MIN)
    for name in ("apply", "undo"):
        w = sub.add_parser(name)
        w.add_argument("--plan", required=True, type=Path)
        w.add_argument("--deadline-minutes", type=float, default=DEFAULT_DEADLINE_MIN)
        if name == "apply":
            w.add_argument("--expect-digest", required=True)
            w.add_argument("--allow-incomplete-plan", action="store_true",
                           help="apply a plan in which some ETF was not planned (they stay unrepaired)")
            w.add_argument("--reapply-quarantined", action="store_true",
                           help="write again a repair row someone quarantined (refused by default)")
        else:
            w.add_argument("--expect-digest", required=True,
                           help="the digest of the plan being undone (as printed when it was made)")
            w.add_argument("--apply", action="store_true", help="write; without it, only count")
    v = sub.add_parser("verify")
    v.add_argument("--plan", required=True, type=Path)
    v.add_argument("--expect-digest", required=True, help="the digest of the plan being verified")
    v.add_argument("--accept-undone", action="store_true",
                   help="exit 0 for items this plan's undo restored (to verify an undo)")
    v.add_argument("--accept-superseded", action="store_true",
                   help="exit 0 although later vintages differ from the plan by more than the tolerance")
    v.add_argument("--accept-plan-gaps", action="store_true",
                   help="exit 0 although the plan left ETFs or rows unrepaired (they are still reported)")
    args = ap.parse_args(argv)
    if args.mode is None:
        ap.print_help()
        return _EXIT_REFUSED

    if engine is None:
        from db import get_engine
        engine = get_engine()
    try:
        if args.mode == "plan":
            if args.plan_out.exists():
                raise Refused(f"{args.plan_out} exists; plans are not overwritten")
            if yf is None:
                import yfinance as yf  # noqa: PLC0415
            deadline_at = _monotonic() + args.deadline_minutes * 60
            plan = build_plan(engine, yf, from_date=args.from_date,
                              to_date=args.to_date, deadline_at=deadline_at)
            args.plan_out.write_text(json.dumps(plan, indent=1, sort_keys=True))
            _print_plan(plan)
            failed = [t for t, r in plan["tickers"].items() if r["status"] != "OK"]
            print(f"  plan written to {args.plan_out}; nothing written to the database")
            if failed:
                print(f"  NOT COMPLETE: {failed} — run plan again for a full plan", file=sys.stderr)
                return _EXIT_FAILED
            return _EXIT_OK
        if args.mode == "verify":
            plan = load_plan(args.plan, args.expect_digest)
            result = verify_plan(engine, plan, accept_plan_gaps=args.accept_plan_gaps,
                                 accept_undone=args.accept_undone, accept_superseded=args.accept_superseded)
            print("VERIFY " + json.dumps(result, sort_keys=True))
            if not result["integrity_ok"]:
                return _EXIT_FAILED
            if not result["complete"]:
                st = result["states"]
                print(f"NOT COMPLETE: {st['not_applied']} not applied, {st['repair_quarantined']} quarantined, "
                      f"{st['undone']} undone, {result['superseded_vs_plan']['over_tolerance']} superseded "
                      f"over tolerance; plan gaps {json.dumps(result['plan_gaps'], sort_keys=True)}", file=sys.stderr)
                return _EXIT_INCOMPLETE
            return _EXIT_OK
        plan = load_plan(args.plan, getattr(args, "expect_digest", None),
                         require_complete=args.mode == "apply" and not args.allow_incomplete_plan)
        progress = args.plan.with_suffix(f".{args.mode}.progress.jsonl")
        if args.mode == "undo" and not args.apply:
            result = verify_plan(engine, plan, accept_plan_gaps=True)
            print("UNDO (dry run: nothing written) would restore up to "
                  f"{result['states']['applied']} row(s); re-run with --apply to write them")
            if not result["integrity_ok"]:
                print("integrity problems: " + json.dumps(result["problems"]), file=sys.stderr)
                return _EXIT_FAILED
            return _EXIT_OK
        deadline_at = _monotonic() + args.deadline_minutes * 60
        result = run_writes(engine, plan, mode=args.mode, deadline_at=deadline_at, progress_path=progress,
                            expect_digest=getattr(args, "expect_digest", None),
                            reapply_quarantined=getattr(args, "reapply_quarantined", False))
        print("RESULT " + json.dumps(result, sort_keys=True))
        if result["status"] == "DONE_WITH_REFUSED_ROWS":
            print("some rows were refused (see totals); nothing was written for them", file=sys.stderr)
            return _EXIT_FAILED
        if result["status"] != "DONE":
            print("stopped early; run the same command again to continue", file=sys.stderr)
            return _EXIT_STOPPED
        return _EXIT_OK
    except Refused as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return _EXIT_REFUSED


if __name__ == "__main__":
    sys.exit(main())
