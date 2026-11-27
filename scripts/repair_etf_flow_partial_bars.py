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
So for a stored row whose run last saw session ``L``::

    settled(D) = recompute_now(D) * RawClose(L) / AdjClose_now(L)

Exact algebra: the adjusted close of bar ``k`` as of ``L`` is
``AdjClose_now(k) * RawClose(L) / AdjClose_now(L)``, and the 5d, 20d and
accel values are linear in it. Measured on production 2026-11-27: the formula
reproduces the 51,678 backfill vintages (dates 2020-01-02..2026-03-26) to
2.5e-6 of the 5d value at worst, and the 198 revisions the fixed writer wrote
that day exactly. ``TOLERANCE`` (1e-5) sits above that noise floor.

Modes
-----
``plan`` reads only: the database in a read-only transaction,
the provider once per ETF. It writes a plan file: every (series, date) whose
newest stored vintage differs from its settled value by more than the
tolerance, with the vintage it revises and the value to write, and a digest.

``apply`` writes exactly the plan's rows, nothing it computes anew. Per
series, in one transaction under the ETF flow write lock of GRID #1501
(``_serialize_etf_write``): the newest stored vintage of each planned date is
read again; the row is written only when that is still the vintage the plan
revises. A date someone revised since is ``superseded`` and left alone; a
date this plan already wrote is ``already_applied``. Rows are dated with the
database clock read after the lock (``received_at``). It is resumable: run
the same command again after a deadline stop, a busy lock or a failure, and
it writes only what is still missing.

``verify`` reads only and checks the plan against what is stored.
``undo`` appends, for every planned date whose newest vintage is still this
plan's row, a vintage carrying the value it replaced. Dry by default.

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

Exit codes: 0 done; 1 a failure (provider, database); 2 refused (usage,
digest, scope); 3 stopped early and resumable (deadline, lock busy).
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
from datetime import date, datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import pandas as pd  # noqa: E402
from sqlalchemy import bindparam, text  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402

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
# Both writers ran the settled-bars code (GRID #1496) from 07:10:33Z / 07:10:48Z
# this day; a row written after this saw no bar for the New York date it was
# written on.
SETTLED_WRITER_SINCE = datetime(2026, 11, 27, 7, 10, 0, tzinfo=timezone.utc)
_MARKET_OPEN = dtime(9, 30)
# Bars fetched before the first date: the 20-day window and the acceleration
# need 19 and 9 sessions before it; 90 calendar days hold at least 55.
FETCH_PAD_DAYS = 90
PROVIDER_DELAY_S = 1.0
PROVIDER_ATTEMPTS = 3
PROVIDER_BACKOFF_S = (5.0, 15.0)
DEFAULT_DEADLINE_MIN = 15.0

_EXIT_OK, _EXIT_FAILED, _EXIT_REFUSED, _EXIT_STOPPED = 0, 1, 2, 3

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


def settled_frame(bars: pd.DataFrame) -> pd.DataFrame:
    """The three series on today's adjusted basis, and each session's basis factor."""
    dv = bars["adj_close"] * bars["volume"]
    out = pd.DataFrame(index=bars.index)
    out["5d"] = dv.rolling(inst._FLOW_WINDOW_5D, min_periods=inst._FLOW_WINDOW_5D).sum()
    out["20d"] = dv.rolling(inst._FLOW_WINDOW_20D, min_periods=inst._FLOW_WINDOW_20D).sum()
    out["accel"] = out["5d"].diff(periods=inst._FLOW_WINDOW_5D)
    # Multiplying a value on today's basis by this puts it on that session's.
    out["basis"] = bars["raw_close"] / bars["adj_close"]
    out["adj_close"] = bars["adj_close"]
    out["volume"] = bars["volume"]
    return out


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


def price_basis_session(pull_ts: Any, sessions: list[date]) -> date | None:
    """The newest session the run that wrote a row at ``pull_ts`` saw.

    Before the market opened, or for the settled-bars writer, that is the
    last session before the New York date of the write; otherwise the
    session of that date (the bar then trading). None if no session is that
    old in ``sessions`` (sorted).
    """
    ts = _as_utc(pull_ts)
    ny = ts.astimezone(inst._NEW_YORK)
    strictly_before = ts >= SETTLED_WRITER_SINCE or ny.time() < _MARKET_OPEN
    best = None
    for s in sessions:
        if s < ny.date() or (s == ny.date() and not strictly_before):
            best = s
        else:
            break
    return best


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


@contextmanager
def read_only_connection(engine: Engine) -> Iterator[Any]:
    """A connection whose transaction refuses writes (PostgreSQL), rolled back at the end.

    Uses the caller's engine, so its pool and session settings (statement
    timeout, search path) apply; only this transaction is read-only.
    """
    with engine.connect() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text("SET TRANSACTION READ ONLY"))
            if conn.execute(text("SHOW transaction_read_only")).scalar() != "on":
                raise Refused("the database did not make this transaction read-only")
        try:
            yield conn
        finally:
            conn.rollback()


def source_id_of(conn: Any) -> int:
    """The writer's source_catalog id, looked up without creating it."""
    sid = conn.execute(
        text("SELECT id FROM source_catalog WHERE LOWER(name) = LOWER(:n)"),
        {"n": inst.InstitutionalFlowsPuller.SOURCE_NAME},
    ).scalar()
    if sid is None:
        raise Refused(f"no source_catalog row for {inst.InstitutionalFlowsPuller.SOURCE_NAME}")
    return int(sid)


def writer_floor(session_day: date) -> date:
    """The first date the scheduled writer still revises by itself."""
    return session_day - timedelta(days=inst._ETF_REVISION_DAYS)


# ── Plan ─────────────────────────────────────────────────────────────────


def _digest(items: list[dict]) -> str:
    blob = json.dumps(items, sort_keys=True, separators=(",", ":")).encode()
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
        calc = settled_frame(bars)
        sessions = list(calc.index)
        counts: Counter = Counter()
        for metric in METRICS:
            sid = f"ETF_FLOW:{ticker}:{metric}"
            for (s_sid, obs_date), vint in sorted(stored.items()):
                if s_sid != sid:
                    continue
                if obs_date not in calc.index:
                    counts["no_settled_bar"] += 1
                    continue
                basis_session = price_basis_session(vint["pull_timestamp"], sessions)
                if basis_session is None or basis_session < obs_date:
                    counts["basis_unknown"] += 1
                    continue
                factor = float(calc.at[basis_session, "basis"])
                value_now = calc.at[obs_date, metric]
                scale = calc.at[obs_date, "5d"]
                if pd.isna(value_now) or pd.isna(scale) or not math.isfinite(factor):
                    counts["not_computable"] += 1
                    continue
                value = float(value_now) * factor
                scale = abs(float(scale) * factor)
                rel = (value - vint["value"]) / scale if scale else math.inf
                if abs(rel) <= TOLERANCE:
                    counts["within_tolerance"] += 1
                    continue
                old = vint["payload"]
                items.append({
                    "series_id": sid, "ticker": ticker, "metric": metric,
                    "obs_date": obs_date.isoformat(),
                    "value": value,
                    "close": float(calc.at[obs_date, "adj_close"]) * factor,
                    "volume": int(calc.at[obs_date, "volume"]),
                    "price_basis_session": basis_session.isoformat(),
                    "rel_change": rel,
                    "revises_id": vint["id"],
                    "revises_value": vint["value"],
                    "revises_pull_timestamp": vint["pull_timestamp"].isoformat(),
                    "revises_volume": old.get("volume"),
                    "revises_close": old.get("close"),
                    "fetched_at": fetched_at,
                })
                counts["planned"] += 1
        tickers_report[ticker] = {"status": "OK", "sessions": len(sessions), **dict(counts)}
        left.update({k: v for k, v in counts.items() if k != "planned"})

    items.sort(key=lambda i: (i["series_id"], i["obs_date"]))
    by_month: dict[str, Counter] = defaultdict(Counter)
    for i in items:
        by_month[i["obs_date"][:7]][i["metric"]] += 1
    changes = sorted(abs(i["rel_change"]) for i in items if i["metric"] == "5d")

    def q(p: float) -> float | None:
        return round(changes[min(len(changes) - 1, int(p * len(changes)))], 6) if changes else None

    return {
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
        "digest": _digest(items),
    }


def load_plan(path: Path, expect_digest: str | None = None) -> dict[str, Any]:
    plan = json.loads(Path(path).read_text())
    if plan.get("kind") != RUN_KIND:
        raise Refused(f"{path} is not a {RUN_KIND} plan")
    actual = _digest(plan["items"])
    if actual != plan.get("digest"):
        raise Refused(f"{path}: items do not match the plan's digest (edited?)")
    if expect_digest is not None and actual != expect_digest:
        raise Refused(f"{path}: digest {actual} is not the expected {expect_digest}")
    return plan


# ── Apply, verify, undo ──────────────────────────────────────────────────


def _repair_marker(plan: dict, item: dict) -> dict[str, Any]:
    return {
        "run_id": plan["run_id"], "kind": RUN_KIND, "plan_digest": plan["digest"],
        "revises_id": item["revises_id"], "revises_pull_timestamp": item["revises_pull_timestamp"],
        "revises_volume": item["revises_volume"], "revises_close": item["revises_close"],
        "price_basis_session": item["price_basis_session"], "fetched_at": item["fetched_at"],
    }


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
) -> Counter:
    """One series, one transaction, under the ETF flow write lock.

    ``mode`` is ``apply`` (write the plan's value where the plan's vintage is
    still the newest) or ``undo`` (write the replaced value back where this
    plan's row is still the newest).
    """
    run_id = plan["run_id"]
    floor = writer_floor(session_day)
    counts: Counter = Counter()
    dates = [date.fromisoformat(i["obs_date"]) for i in items]
    with engine.begin() as conn:
        received_at = puller._serialize_etf_write(conn, budget)
        newest = newest_vintages(conn, puller.source_id, [series_id], min(dates), max(dates))
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
                payload = {
                    "ticker": item["ticker"], "close": item["close"], "volume": item["volume"],
                    "revises_value": current["value"], "repair": _repair_marker(plan, item),
                }
                value = item["value"]
            else:
                if current is None or not mine:
                    counts["not_newest"] += 1
                    continue
                with_old = conn.execute(
                    text("SELECT raw_payload FROM raw_series WHERE id = :i"), {"i": item["revises_id"]}
                ).scalar()
                payload = dict(_payload(with_old))
                payload["revises_value"] = current["value"]
                payload["repair_undo"] = {"run_id": run_id, "undoes_id": current["id"], "kind": RUN_KIND}
                payload.pop("repair", None)
                value = item["revises_value"]
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
) -> dict[str, Any]:
    """Write the plan (``apply``) or its undo, series by series; resumable."""
    session_day = session_day or inst._new_york_today()
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
                                   budget=budget, session_day=session_day)
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
    return {"mode": mode, "run_id": plan["run_id"], "status": status, "totals": dict(totals),
            "planned": len(plan["items"])}


def verify_plan(engine: Engine, plan: dict) -> dict[str, Any]:
    """Read-only check of a plan against what is stored."""
    run_id = plan["run_id"]
    lo = min(date.fromisoformat(i["obs_date"]) for i in plan["items"]) if plan["items"] else date.today()
    hi = max(date.fromisoformat(i["obs_date"]) for i in plan["items"]) if plan["items"] else lo
    series_ids = sorted({i["series_id"] for i in plan["items"]})
    counts: Counter = Counter()
    problems: list[str] = []
    with read_only_connection(engine) as conn:
        rows = conn.execute(text(
            "SELECT id, series_id, obs_date, value, raw_payload, pull_status FROM raw_series "
            "WHERE source_id = :src AND series_id IN :sids"
        ).bindparams(bindparam("sids", expanding=True)), {"src": plan["source_id"], "sids": series_ids or [""]}).fetchall()
        newest = newest_vintages(conn, plan["source_id"], series_ids or [""], lo, hi)
    mine: dict[tuple[str, date], list] = defaultdict(list)
    by_id = {}
    for r in rows:
        by_id[int(r[0])] = r
        p = _payload(r[4])
        if (p.get("repair") or {}).get("run_id") == run_id:
            mine[(r[1], _as_date(r[2]))].append(r)
    planned_keys = set()
    for item in plan["items"]:
        key = (item["series_id"], date.fromisoformat(item["obs_date"]))
        planned_keys.add(key)
        found = mine.get(key, [])
        old = by_id.get(item["revises_id"])
        if old is None or not math.isclose(float(old[3]), item["revises_value"], rel_tol=1e-12):
            problems.append(f"{key}: the revised vintage {item['revises_id']} is missing or changed")
        if len(found) > 1:
            counts["duplicated"] += 1
            problems.append(f"{key}: written {len(found)} times")
        elif len(found) == 1:
            counts["applied"] += 1
            if not math.isclose(float(found[0][3]), item["value"], rel_tol=1e-12):
                problems.append(f"{key}: stored repair value differs from the plan")
            cur = newest.get(key)
            if cur is None or cur["id"] != int(found[0][0]):
                counts["applied_then_superseded"] += 1
        else:
            counts["not_applied"] += 1
    stray = [k for k in mine if k not in planned_keys]
    if stray:
        problems.append(f"{len(stray)} row(s) carry this run id outside the plan")
    return {"run_id": run_id, "planned": len(plan["items"]), **dict(counts),
            "problems": problems, "ok": not problems}


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
        else:
            w.add_argument("--apply", action="store_true", help="write; without it, only count")
    v = sub.add_parser("verify")
    v.add_argument("--plan", required=True, type=Path)
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
            plan = load_plan(args.plan)
            result = verify_plan(engine, plan)
            print("VERIFY " + json.dumps(result, sort_keys=True))
            return _EXIT_OK if result["ok"] else _EXIT_FAILED
        plan = load_plan(args.plan, getattr(args, "expect_digest", None))
        progress = args.plan.with_suffix(f".{args.mode}.progress.jsonl")
        if args.mode == "undo" and not args.apply:
            result = verify_plan(engine, plan)
            print("UNDO (dry run: nothing written) would restore up to "
                  f"{result.get('applied', 0) - result.get('applied_then_superseded', 0)} row(s); "
                  "re-run with --apply to write them")
            return _EXIT_OK
        deadline_at = _monotonic() + args.deadline_minutes * 60
        result = run_writes(engine, plan, mode=args.mode, deadline_at=deadline_at, progress_path=progress)
        print("RESULT " + json.dumps(result, sort_keys=True))
        if result["status"] != "DONE":
            print("stopped early; run the same command again to continue", file=sys.stderr)
            return _EXIT_STOPPED
        return _EXIT_OK
    except Refused as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return _EXIT_REFUSED


if __name__ == "__main__":
    sys.exit(main())
