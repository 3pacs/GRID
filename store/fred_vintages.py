"""FRED/ALFRED vintages: what FRED showed for an observation on a given date.

``raw_series`` keeps one row per pull, so a backfilled series reads as its latest revision (hindsight;
``analysis/research_real_panel.py`` refuses materially revised series for that reason). Some series are revised hard:
the daily Economic Policy Uncertainty index (USEPUINDXD) first printed 2022-06-02 at 60.23 and settled at 144.13 two
months later. This table keeps every published version with the dates it was live, from ALFRED
(``fred/series/observations`` with ``realtime_start``/``realtime_end``).

Contract: :func:`values_as_of` answers "what did FRED show for ``obs_date`` at the end of ``as_of``" only for
``as_of <= obs_date + MAX_AGE_DAYS``. The fetcher (``ingestion/fred_vintages.py``) asks for each observation's
vintages through at least that age, so inside it the answer is complete; past it, later revisions may be missing and
the reader refuses rather than guess. A vintage dated ``realtime_start`` counts as known on that date (FRED posts the
daily indexes in the morning, US Central).

Mutability: :func:`upsert` never changes a row's key, ``value`` or ``fetched_at``, but it moves ``realtime_end`` later
in place. So a read "as GRID knew it at an instant" (``load(fetched_by=)``, the provisional market-state record,
#1312) rebuilds each version's interval from the versions fetched by then and never reads the stored end.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

MAX_AGE_DAYS = 45
OPEN_END = date(9999, 12, 31)

_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS fred_vintages ("
    " series_id TEXT NOT NULL,"
    " obs_date DATE NOT NULL,"
    " realtime_start DATE NOT NULL,"
    " realtime_end DATE NOT NULL,"
    " value DOUBLE PRECISION,"
    " fetched_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    " PRIMARY KEY (series_id, obs_date, realtime_start))"
)
_INDEX_SQL = "CREATE INDEX IF NOT EXISTS idx_fred_vintages_rt ON fred_vintages (series_id, realtime_start)"

# A vintage's end only moves later: a refresh that sees the same version still live extends it.
_UPSERT_SQL = text(
    "INSERT INTO fred_vintages (series_id, obs_date, realtime_start, realtime_end, value) "
    "VALUES (:s, :o, :rs, :re, :v) "
    "ON CONFLICT (series_id, obs_date, realtime_start) DO UPDATE SET "
    "realtime_end = CASE WHEN EXCLUDED.realtime_end > fred_vintages.realtime_end "
    "THEN EXCLUDED.realtime_end ELSE fred_vintages.realtime_end END"
)

_READ_SQL = text(
    "SELECT obs_date, realtime_start, realtime_end, value FROM fred_vintages "
    "WHERE series_id = :s AND obs_date >= :o0 AND obs_date <= :o1 AND realtime_start <= :rt1"
)

# The read for a cutoff, from the columns no write ever changes: the key (series_id, obs_date, realtime_start), value
# and fetched_at (set on first insert only). realtime_end is left out on purpose: upsert moves it later in place.
_READ_FETCHED_SQL = text(
    "SELECT obs_date, realtime_start, value, fetched_at FROM fred_vintages "
    "WHERE series_id = :s AND obs_date >= :o0 AND obs_date <= :o1 AND realtime_start <= :rt1"
)

_LAST_SQL = text("SELECT MAX(realtime_start), MIN(realtime_start), COUNT(*) FROM fred_vintages WHERE series_id = :s")


def ensure_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(_TABLE_SQL))
        conn.execute(text(_INDEX_SQL))


def upsert(engine: Engine, series_id: str, rows: Iterable[dict[str, Any]]) -> int:
    """Store vintages ``{obs_date, realtime_start, realtime_end, value}``; returns rows sent."""
    ensure_table(engine)
    batch = [{"s": series_id, "o": r["obs_date"], "rs": r["realtime_start"], "re": r["realtime_end"],
              "v": r["value"]} for r in rows]
    if not batch:
        return 0
    with engine.begin() as conn:
        conn.execute(_UPSERT_SQL, batch)
    return len(batch)


def coverage(engine: Engine, series_id: str) -> dict[str, Any]:
    try:
        with engine.connect() as conn:
            last, first, n = conn.execute(_LAST_SQL, {"s": series_id}).fetchone()
    except Exception:
        return {"rows": 0, "first_vintage": None, "last_vintage": None}
    return {"rows": int(n or 0), "first_vintage": str(first) if first else None, "last_vintage": str(last) if last else None}


def _fetched_instant(raw: Any) -> datetime | None:
    if raw is None:
        return None
    ts = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def _day(x: Any) -> date:
    return x if isinstance(x, date) else date.fromisoformat(str(x)[:10])


def intervals_known_at(rows: Iterable[tuple]) -> list[tuple[date, date, date, float | None]]:
    """``(obs_date, realtime_start, value)`` versions -> ``(obs_date, realtime_start, realtime_end, value)``.

    Each version is live until the day before the next version of the same observation in ``rows``, and open-ended
    (:data:`OPEN_END`) when there is none. Only immutable facts are used: a stored ``realtime_end`` is a later
    fact, because :func:`upsert` extends it in place when a refresh sees the version still live.
    """
    by_obs: dict[date, list[tuple[date, float | None]]] = {}
    for obs, start, value in rows:
        by_obs.setdefault(_day(obs), []).append((_day(start), value))
    out = []
    for obs in sorted(by_obs):
        versions = sorted(by_obs[obs], key=lambda v: v[0])
        for i, (start, value) in enumerate(versions):
            end = versions[i + 1][0] - timedelta(days=1) if i + 1 < len(versions) else OPEN_END
            out.append((obs, start, end, value))
    return out


def load(engine: Engine, series_id: str, obs_start: date, obs_end: date, rt_until: date, *,
         fetched_by: datetime | None = None) -> pd.DataFrame:
    """Vintage rows for observations in ``[obs_start, obs_end]`` published by ``rt_until``.

    ``fetched_by`` (timezone-aware) answers as GRID knew it at that instant. Only versions fetched by then count
    (``fetched_at``, a naive CURRENT_TIMESTAMP taken as UTC; a row without one is not known). Each one's live interval
    is rebuilt from those versions alone (:func:`intervals_known_at`), never from the stored ``realtime_end``,
    which a later refresh extends in place. So a refresh after the instant cannot change the read. Without
    ``fetched_by`` the read is unchanged and uses the stored ends.
    """
    if fetched_by is not None and fetched_by.utcoffset() is None:
        raise ValueError("fetched_by must be timezone-aware")
    params = {"s": series_id, "o0": obs_start, "o1": obs_end, "rt1": rt_until}
    with engine.connect() as conn:
        if fetched_by is None:
            rows = conn.execute(_READ_SQL, params).fetchall()
        else:
            cut = fetched_by.astimezone(timezone.utc)
            rows = intervals_known_at(r[:3] for r in conn.execute(_READ_FETCHED_SQL, params).fetchall()
                                      if (t := _fetched_instant(r[3])) is not None and t <= cut)
    df = pd.DataFrame(rows, columns=["obs_date", "realtime_start", "realtime_end", "value"])
    for c in ("obs_date", "realtime_start", "realtime_end"):
        # Plain dates: the open end 9999-12-31 is past pandas' nanosecond Timestamp range.
        df[c] = [x if isinstance(x, date) else date.fromisoformat(str(x)[:10]) for x in df[c]]
    return df


def values_as_of(vintages: pd.DataFrame, obs_dates: Iterable[date], as_of: date) -> dict[date, float | None]:
    """{obs_date: value FRED showed at the end of ``as_of``}; None when not yet published (or published as missing).

    Raises ValueError when asked about an observation older than :data:`MAX_AGE_DAYS` at ``as_of``.
    """
    out: dict[date, float | None] = {}
    obs_list = list(obs_dates)
    for o in obs_list:
        if as_of > o + timedelta(days=MAX_AGE_DAYS):
            raise ValueError(f"fred_vintages covers {MAX_AGE_DAYS} days after an observation; {o} at {as_of} is past it")
    if vintages.empty:
        return {o: None for o in obs_list}
    live = vintages[(vintages["realtime_start"] <= as_of) & (vintages["realtime_end"] >= as_of)]
    by_obs = live.sort_values("realtime_start").groupby("obs_date")["value"].last()
    for o in obs_list:
        v = by_obs.get(o)
        out[o] = None if v is None or pd.isna(v) else float(v)
    return out
