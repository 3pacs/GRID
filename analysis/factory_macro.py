"""Module factory, factory 5 (3pacs/GRID#1159): the macro adapter (FRED dailies, point in time).

The macro class reads daily macro series at a decision time T only once they were published. Its input is
one frozen, hash-pinned snapshot (``macro_sha256`` in the run file, ``--macro FILE`` on the run command),
built by :func:`build_snapshot` from GRID's own stores, read-only:

* ``raw_series`` (FRED, ``source_id`` 1, ``pull_status = 'SUCCESS'``) for the market-derived dailies. The
  FRED puller (``ingestion/fred.py``) inserts an observation once and never updates it, so the history is
  one value per date: the backfill of 2026-03-24..04-05 for older dates (latest vintage then), and the
  first value GRID saw for later dates. ``pull_timestamp`` is the first time GRID saw a date.
* ``fred_vintages`` (ALFRED ``realtime_start`` / ``realtime_end``, ``ingestion/fred_vintages.py``) for the
  daily uncertainty indexes, which are revised for weeks after their first print.
* VXVCLS (for edge9's S3) from the staged dev data's ``fred/VXVCLS.csv`` (in the dev manifest, #1154):
  GRID's FRED pull list does not hold it. Lines dated before 2022-11-14 are skipped unparsed.

Nothing dated before 2022-11-14 (the edge9 holdout's end) enters a snapshot, and a snapshot holding an
earlier date is refused.

Point in time (fixed here, never in a run file):

* **Publication rules.** Every series names a publication rule: GRID's reviewer-verified
  :data:`analysis.research_real_panel.PUBLICATIONS` (an observation dated ``d`` is known at ``d + lag``
  business days on the US federal holiday calendar, or calendar days, at a declared UTC time; end of day
  where the time is not verified), plus :data:`EXTRA_PUBLICATIONS` for SOFR. A value counts for a decision
  at T only when its known_at is strictly before T. Every rule publishes after 15:00 ET (the last decision),
  so a macro value is fixed for the whole session: an H.15 yield dated D-1 posts at about 16:15 ET on D and
  is first used at 10:00 ET on D+1.
* **Vintages.** A revised series is used only through its vintages: the value of observation ``o`` at T is
  the one in its latest vintage whose ``realtime_start`` R is known before T, and a vintage dated R counts
  from 00:00 America/Chicago on R + 1 (FRED dates its realtime periods in US Central time; the whole of R
  is allowed for). An observation is known once its first vintage is. A vintage read past
  :data:`store.fred_vintages.MAX_AGE_DAYS` of its observation is refused (the store is complete only up to
  that age). A revised series without vintages is never used.
* **Staleness.** The newest known observation must have become known within ``macro_stale_days`` calendar
  days of T, else the feature is None (a gap is never carried).
* **Missing is missing.** A null value, a missing leg, too little history or a stale latest value gives
  None; nothing is filled.
"""

from __future__ import annotations

import bisect
import csv
import hashlib
import json
import math
import sqlite3
import statistics
import sys
from datetime import date, datetime, time, timedelta, timezone
from itertools import pairwise
from pathlib import Path
from zoneinfo import ZoneInfo

from analysis.research_real_panel import (
    PUBLICATIONS,
    Publication,
    publication_times,
    revised,
)
from store.fred_vintages import MAX_AGE_DAYS

DEV_FIRST = "2022-11-14"
NY = ZoneInfo("America/New_York")
CHICAGO = ZoneInfo("America/Chicago")
SNAPSHOT_SCHEMA = "factory-macro/1"
RAW = "griddb raw_series (FRED, source_id 1, SUCCESS, first row per date)"
VINT = "griddb fred_vintages (ALFRED realtime_start/realtime_end)"
STAGED = "staged dev data fred/{} (dev manifest, #1154)"
VINTAGE_RULE = ("vintage dated R (ALFRED realtime_start) known from 00:00 America/Chicago on R+1; the observation is "
                f"known with its first vintage; reads past {MAX_AGE_DAYS} days of the observation refused")

EXTRA_PUBLICATIONS = {
    "NYFED_SOFR": Publication(
        1, "business", "23:59",
        "SOFR on FRED: the NY Fed publishes D's rate at about 08:00 ET on D+1 and may correct it until 14:30 ET; "
        "next business day, end of day (GRID first saw 148 of 152 dates of 2026-04..11 before this)"),
}
RULES = {**PUBLICATIONS, **EXTRA_PUBLICATIONS}

# Revision status of each series. ``unrevised``: by the publisher's practice (GRID's research_real_panel policy:
# H.15, ICE BofA and Cboe closes are treated as unrevised); no vintage is needed. ``vintages``: revised; read only
# through its stored vintages.
SERIES = {
    "VIXCLS": {"name": "Cboe VIX close", "rule": "CBOE_VIX", "revision": "unrevised", "origin": RAW},
    "VXVCLS": {"name": "Cboe VIX3M close", "rule": "CBOE_VIX", "revision": "unrevised",
               "origin": STAGED.format("VXVCLS.csv")},
    "BAMLH0A0HYM2": {"name": "ICE BofA US High Yield OAS", "rule": "ICE_BOFA", "revision": "unrevised",
                     "origin": RAW},
    "DGS2": {"name": "2-year Treasury constant maturity (H.15)", "rule": "FRB_H15", "revision": "unrevised",
             "origin": RAW},
    "T10Y2Y": {"name": "10-year minus 2-year Treasury (FRED spread of H.15 legs)", "rule": "FRED_H15_SPREAD",
               "revision": "unrevised", "origin": RAW},
    "DFII10": {"name": "10-year TIPS real yield (H.15)", "rule": "FRB_H15", "revision": "unrevised", "origin": RAW},
    "T10YIE": {"name": "10-year breakeven inflation (FRED spread of H.15 legs)", "rule": "FRED_H15_SPREAD",
               "revision": "unrevised", "origin": RAW},
    "DFF": {"name": "effective federal funds rate (H.15; every calendar day)", "rule": "FRB_H15",
            "revision": "unrevised", "origin": RAW},
    "SOFR": {"name": "Secured Overnight Financing Rate (NY Fed)", "rule": "NYFED_SOFR", "revision": "unrevised",
             "origin": RAW},
    "RRPONTSYD": {"name": "Fed overnight reverse repo, Treasury securities sold ($bn)", "rule": "NYFED_RRP",
                  "revision": "unrevised", "origin": RAW},
    "DEXJPUS": {"name": "yen per US dollar, noon buying rate (H.10)", "rule": "FRB_H10", "revision": "unrevised",
                "origin": RAW},
    "USEPUINDXD": {"name": "Economic Policy Uncertainty, daily (Baker, Bloom, Davis)", "rule": "vintage",
                   "revision": "vintages", "origin": VINT},
    "WLEMUINDXD": {"name": "Equity Market-related Economic Uncertainty, daily", "rule": "vintage",
                   "revision": "vintages", "origin": VINT},
}
RAW_SERIES = tuple(s for s, v in SERIES.items() if v["origin"] == RAW)
VINTAGE_SERIES = tuple(s for s, v in SERIES.items() if v["revision"] == "vintages")
STAGED_SERIES = {"VXVCLS": "fred/VXVCLS.csv"}

# Held by GRID but not used, with the reason (reported, never silently dropped).
EXCLUDED = {
    "DTWEXBGS": "revised (broad dollar index history is revised when trade weights are updated; GRID's revised "
                "list) and GRID holds no vintages",
    "DGS10": "GRID's FRED pull stopped at 2026-04-02 (not in FRED_SERIES_LIST); its 10-year leg is in T10Y2Y",
    "THREEFYTP10": "ACM term premium: stale since 2026-03-27 and re-estimated (model output)",
    "PAYEMS, UNRATE, CPIAUCSL, PCEPI, PCEPILFE, ICSA, CCSA, GDP, INDPRO, RSAFS, HOUST, PERMIT, UMCSENT, M2SL, "
    "TOTBKCR, BUSLOANS, DSPIC96, TCU": "revised monthly/weekly series (GRID's revised list); GRID's raw_series "
    "holds only the 2026-03/04 backfill's latest vintage and fred_vintages holds none of them",
    "VXVCLS-based S3 live": "VXVCLS is not in GRID's FRED pull list: research-only until GRID pulls it",
    "yc_*, sofr_rate, fed_funds_effective, treasury_bill_*": "GRID-derived duplicates of H.15/NY Fed series, "
    "only from 2025-10/11",
    "SP500": "not in griddb; S1/S2/S5 use SPY's own settled closes from the staged bars instead",
}

# The macro features: (kind, inputs). Kinds: level / d1 / d20 (latest known observation, minus the one 1 or 20
# observations earlier) / pct20 / spread (a - b on the same date) / ratio (a / b, latest date both known) /
# mean5_vintage / trend200 / drawdown252 / vrp.
FEATURES = {
    "S1": ("trend200", ()),
    "S2": ("drawdown252", ()),
    "S3": ("ratio", ("VIXCLS", "VXVCLS")),
    "S4": ("d20", ("BAMLH0A0HYM2",)),
    "S5": ("vrp", ("VIXCLS",)),
    "HYOAS": ("level", ("BAMLH0A0HYM2",)),
    "HYOAS_D1": ("d1", ("BAMLH0A0HYM2",)),
    "VIX": ("level", ("VIXCLS",)),
    "VIX_D1": ("d1", ("VIXCLS",)),
    "Y2_D1": ("d1", ("DGS2",)),
    "Y2_D20": ("d20", ("DGS2",)),
    "CURVE": ("level", ("T10Y2Y",)),
    "REAL10_D20": ("d20", ("DFII10",)),
    "BE10_D20": ("d20", ("T10YIE",)),
    "SOFRFF": ("spread", ("SOFR", "DFF")),
    "RRP_D20": ("d20", ("RRPONTSYD",)),
    "JPY_D20": ("pct20", ("DEXJPUS",)),
    "EPU5": ("mean5_vintage", ("USEPUINDXD",)),
    "EMU5": ("mean5_vintage", ("WLEMUINDXD",)),
}
SPY_FEATURES = frozenset(("S1", "S2", "S5"))
TREND, DRAWDOWN, RV_DAYS = 200, 252, 20
COUNTS = ("decisions", "value", "not_known", "stale", "history", "missing")


class MacroDataError(ValueError):
    pass


def rule_text(sid: str) -> str:
    rule = SERIES[sid]["rule"]
    if rule == "vintage":
        return VINTAGE_RULE
    p = RULES[rule]
    return f"{rule}: +{p.lag} {p.unit} day(s) at {p.time_utc}Z; {p.basis}"


def known_times(sid: str, dates: list[str]) -> list[datetime]:
    """When observations of ``sid`` dated ``dates`` became known (UTC), by the series' publication rule."""
    if not dates:
        return []
    return [t.to_pydatetime() for t in publication_times(dates, RULES[SERIES[sid]["rule"]])]


def vintage_known(realtime_start: str) -> datetime:
    """A vintage dated ``realtime_start`` counts from 00:00 America/Chicago the next day (UTC)."""
    d = date.fromisoformat(realtime_start) + timedelta(days=1)
    return datetime.combine(d, time(0, 0), tzinfo=CHICAGO).astimezone(timezone.utc)


def _connect(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{Path(path)}?mode=ro", uri=True)


def content_digest(obs: dict, vint: dict) -> str:
    """sha256 over every observation and vintage the snapshot holds (canonical order)."""
    h = hashlib.sha256()
    for sid in sorted(obs):
        for od, v in obs[sid]:
            h.update(f"o|{sid}|{od}|{v!r}\n".encode())
    for sid in sorted(vint):
        for od in sorted(vint[sid]):
            for rs, v in vint[sid][od]:
                h.update(f"v|{sid}|{od}|{rs}|{v!r}\n".encode())
    return h.hexdigest()


# --- the snapshot ------------------------------------------------------------------------------------------------

EXPORT_SQL = {
    "obs": ("select series_id, obs_date, value, to_char(pull_timestamp at time zone 'UTC', "
            "'YYYY-MM-DD\"T\"HH24:MI:SS.US') from raw_series where source_id = 1 and pull_status = 'SUCCESS' "
            "and series_id in ({ids}) and obs_date between '{first}' and '{last}' order by 1, 2, 4"),
    "vintages": ("select series_id, obs_date, realtime_start, realtime_end, value from fred_vintages "
                 "where series_id in ({vids}) and obs_date between '{first}' and '{last}' "
                 "and realtime_start <= '{last}' order by 1, 2, 3"),
}


def export_sql(first: str = DEV_FIRST, last: str = "2026-11-10") -> dict[str, str]:
    """The two read-only SELECTs whose CSV output (psql ``\\copy (...) to stdout csv``) feeds :func:`build_snapshot`."""
    if first < DEV_FIRST:
        raise MacroDataError(f"first {first} is before dev ({DEV_FIRST}): holdout refused")
    ids = ", ".join(f"'{s}'" for s in RAW_SERIES)
    vids = ", ".join(f"'{s}'" for s in VINTAGE_SERIES)
    return {k: v.format(ids=ids, vids=vids, first=first, last=last) for k, v in EXPORT_SQL.items()}


def first_seen_check(sid: str, rows: list[tuple[str, float | None, str | None]]) -> dict:
    """How the publication rule compares with the first time GRID saw each live-pulled date (first seen within
    14 days of its date: the backfill's dates are left out). ``seen_before_rule``: dates GRID held before the rule
    says they were known, so the rule was no earlier than the real publication for them."""
    live = [(od, datetime.fromisoformat(seen).replace(tzinfo=timezone.utc)) for od, _v, seen in rows
            if seen and (datetime.fromisoformat(seen).date() - date.fromisoformat(od)).days <= 14]
    if not live:
        return {"live_dates": 0}
    known = known_times(sid, [od for od, _ in live])
    before = sum(seen <= k for (_, seen), k in zip(live, known))
    lead = sorted((k - seen).total_seconds() / 3600 for (_, seen), k in zip(live, known))
    return {"live_dates": len(live), "first": live[0][0], "last": live[-1][0], "seen_before_rule": before,
            "share_seen_before_rule": round(before / len(live), 4),
            "rule_minus_first_seen_hours": {"min": round(lead[0], 1), "median": round(lead[len(lead) // 2], 1),
                                            "max": round(lead[-1], 1)}}


def build_snapshot(obs_csv: Path, vintages_csv: Path, staged_dir: Path, out: Path, first: str = DEV_FIRST,
                   last: str = "2026-11-10", source: str = "griddb") -> dict:
    """Write the frozen macro snapshot (sqlite, ``factory-macro/1``) and return its meta. Inputs: the CSV output of
    :func:`export_sql` and the staged dev folder (for :data:`STAGED_SERIES`). Refuses anything before dev."""
    if first < DEV_FIRST:
        raise MacroDataError(f"first {first} is before dev ({DEV_FIRST}): holdout refused")
    if "holdout" in str(staged_dir).lower():
        raise MacroDataError(f"holdout path refused: {staged_dir}")
    rows: dict[str, list] = {s: [] for s in (*RAW_SERIES, *STAGED_SERIES)}
    seen_dates: dict[tuple[str, str], tuple] = {}
    duplicates = conflicts = 0
    with open(obs_csv, newline="") as f:
        for sid, od, value, seen in csv.reader(f):
            if sid not in RAW_SERIES:
                raise MacroDataError(f"unexpected series {sid} in {obs_csv}")
            if not first <= od <= last:
                raise MacroDataError(f"{sid} {od} outside {first}..{last}: holdout refused")
            v = None if value == "" else float(value)
            if (sid, od) in seen_dates:  # one value per date: the first GRID saw (rows come ordered by pull time)
                duplicates += 1
                conflicts += seen_dates[(sid, od)][0] != v
                continue
            seen_dates[(sid, od)] = (v, seen)
            rows[sid].append((od, v, seen))
    staged_files = {}
    for sid, rel in STAGED_SERIES.items():
        path = Path(staged_dir) / rel
        staged_files[sid] = {"path": rel, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                             "lines_before_dev_skipped_unparsed": 0}
        with open(path) as f:
            header = f.readline().strip().split(",")
            if header != ["observation_date", sid]:
                raise MacroDataError(f"{path}: unexpected header {header}")
            for line in f:
                if line[:10] < first:  # the date is compared as text; nothing before dev is parsed
                    staged_files[sid]["lines_before_dev_skipped_unparsed"] += 1
                    continue
                od, value = line.strip().split(",")
                if od > last:
                    continue
                rows[sid].append((od, None if value in ("", ".") else float(value), None))
    vint: dict[str, dict[str, list]] = {s: {} for s in VINTAGE_SERIES}
    with open(vintages_csv, newline="") as f:
        for sid, od, rs, re_, value in csv.reader(f):
            if sid not in VINTAGE_SERIES:
                raise MacroDataError(f"unexpected vintage series {sid}")
            if not first <= od <= last or rs > last:
                raise MacroDataError(f"{sid} {od}@{rs} outside {first}..{last}: holdout refused")
            vint[sid].setdefault(od, []).append((rs, re_, None if value == "" else float(value)))
    out = Path(out)
    if out.exists():
        raise MacroDataError(f"{out} exists (write-once)")
    obs_plain = {s: [(od, v) for od, v, _ in sorted(r)] for s, r in rows.items()}
    vint_plain = {s: {od: sorted((rs, v) for rs, _re, v in vs) for od, vs in d.items()} for s, d in vint.items()}
    meta = {
        "schema": SNAPSHOT_SCHEMA,
        "source": source,
        "window": {"first": first, "last": last},
        "content_sha256": content_digest(obs_plain, vint_plain),
        "duplicate_rows_dropped": duplicates,
        "duplicate_rows_conflicting": conflicts,
        "staged_files": staged_files,
        "export_sql": export_sql(first, last),
        "series": {},
    }
    for sid in SERIES:
        if sid in VINTAGE_SERIES:
            d = vint[sid]
            first_lag = [(date.fromisoformat(min(rs for rs, _, _ in vs)) - date.fromisoformat(od)).days
                         for od, vs in d.items()]
            meta["series"][sid] = {
                "observations": len(d), "vintages": sum(len(v) for v in d.values()),
                "first": min(d, default=None), "last": max(d, default=None),
                "revised_observations": sum(len(v) > 1 for v in d.values()),
                "first_vintage_lag_days": {str(k): first_lag.count(k) for k in sorted(set(first_lag))},
                "null_vintages": sum(v is None for vs in d.values() for _, _, v in vs),
            }
        else:
            r = sorted(rows[sid])
            meta["series"][sid] = {"observations": len(r), "first": r[0][0] if r else None,
                                   "last": r[-1][0] if r else None, "null_values": sum(v is None for _, v, _ in r)}
            if SERIES[sid]["origin"] == RAW:
                meta["series"][sid]["first_seen_check"] = first_seen_check(sid, r)
        meta["series"][sid].update(origin=SERIES[sid]["origin"], revision=SERIES[sid]["revision"],
                                   rule=rule_text(sid))
    con = sqlite3.connect(out)
    try:
        con.execute("create table meta (key text primary key, value text not null)")
        con.execute("create table obs (series_id text, obs_date text, value real, first_seen_utc text, "
                    "primary key (series_id, obs_date))")
        con.execute("create table vintages (series_id text, obs_date text, realtime_start text, realtime_end text, "
                    "value real, primary key (series_id, obs_date, realtime_start))")
        con.executemany("insert into obs values (?, ?, ?, ?)",
                        [(s, od, v, seen) for s in sorted(rows) for od, v, seen in sorted(rows[s])])
        con.executemany("insert into vintages values (?, ?, ?, ?, ?)",
                        [(s, od, rs, re_, v) for s in sorted(vint) for od in sorted(vint[s])
                         for rs, re_, v in sorted(vint[s][od])])
        con.executemany("insert into meta values (?, ?)", [(k, json.dumps(v, sort_keys=True)) for k, v in meta.items()])
        con.commit()
    finally:
        con.close()
    return meta


class MacroSnapshot:
    """A frozen ``factory-macro/1`` snapshot, read once, read-only. Refuses any date before dev."""

    def __init__(self, path: Path):
        self.path = Path(path)
        con = _connect(self.path)
        try:
            self.meta = {k: json.loads(v) for k, v in con.execute("select key, value from meta")}
            obs = con.execute("select series_id, obs_date, value from obs order by series_id, obs_date").fetchall()
            vint = con.execute("select series_id, obs_date, realtime_start, value from vintages "
                               "order by series_id, obs_date, realtime_start").fetchall()
        finally:
            con.close()
        if self.meta.get("schema") != SNAPSHOT_SCHEMA:
            raise MacroDataError(f"{self.path} is not a {SNAPSHOT_SCHEMA} snapshot")
        early = min([r[1] for r in obs] + [r[1] for r in vint], default=None)
        if early is None or early < DEV_FIRST:
            raise MacroDataError(f"{self.path} holds dates before dev ({early}): holdout refused")
        self.obs: dict[str, list[tuple[str, float | None]]] = {}
        for sid, od, v in obs:
            self.obs.setdefault(sid, []).append((od, v))
        self.vintages: dict[str, dict[str, list[tuple[str, float | None]]]] = {}
        for sid, od, rs, v in vint:
            self.vintages.setdefault(sid, {}).setdefault(od, []).append((rs, v))
        if content_digest(self.obs, self.vintages) != self.meta.get("content_sha256"):
            raise MacroDataError(f"{self.path}: rows do not match the snapshot's content_sha256")


class _Series:
    """One series' observations in date order with their known_at (UTC), for "latest known before T" lookups."""

    def __init__(self, sid: str, rows: list[tuple[str, float | None]]):
        self.sid, self.dates, self.values = sid, [r[0] for r in rows], [r[1] for r in rows]
        self.known = known_times(sid, self.dates)
        self.pos = {d: i for i, d in enumerate(self.dates)}
        if any(b < a for a, b in zip(self.known, self.known[1:])):
            raise MacroDataError(f"{sid}: publication times are not monotone in date")

    def latest(self, decided: datetime) -> int:
        """Index of the newest observation known strictly before ``decided``; -1 when none."""
        return bisect.bisect_left(self.known, decided) - 1


class _VintageSeries:
    """A revised series: per observation, its vintages (known_at, value) in order."""

    def __init__(self, sid: str, by_obs: dict[str, list[tuple[str, float | None]]]):
        self.sid = sid
        self.dates = sorted(by_obs)
        self.versions = [[(vintage_known(rs), v) for rs, v in by_obs[od]] for od in self.dates]
        self.known = [vs[0][0] for vs in self.versions]
        # A late first print can make an older date known after a newer one: "the newest date known before T" is
        # the largest date index among those whose first vintage is known before T.
        order = sorted(range(len(self.dates)), key=lambda i: (self.known[i], i))
        self._known_sorted = [self.known[i] for i in order]
        self._newest = []
        for i in order:
            self._newest.append(max(i, self._newest[-1]) if self._newest else i)

    def latest(self, decided: datetime) -> int:
        j = bisect.bisect_left(self._known_sorted, decided) - 1
        return self._newest[j] if j >= 0 else -1

    def value_as_of(self, i: int, decided: datetime) -> float | None:
        """Observation i's value in the newest vintage known strictly before ``decided``."""
        od = date.fromisoformat(self.dates[i])
        if decided.astimezone(NY).date() > od + timedelta(days=MAX_AGE_DAYS):
            raise MacroDataError(f"{self.sid} {od}: vintages are complete only {MAX_AGE_DAYS} days after a date")
        live = [v for k, v in self.versions[i] if k < decided]
        return live[-1] if live else None


class MacroInputs:
    """The macro class's inputs for :func:`analysis.module_factory.build_rows`: the snapshot's series and the SPY
    settled closes (for S1, S2 and S5). Keeps coverage counts per feature."""

    def __init__(self, snapshot: MacroSnapshot, spy_closes: dict[str, float | None], stale_days: int = 7):
        self.snapshot, self.stale = snapshot, timedelta(days=stale_days)
        self.series = {sid: _Series(sid, rows) for sid, rows in snapshot.obs.items()}
        self.vseries = {sid: _VintageSeries(sid, d) for sid, d in snapshot.vintages.items()}
        self.closes = sorted((d, c) for d, c in spy_closes.items() if c)
        self.close_days = [d for d, _ in self.closes]
        self.counts = {f: dict.fromkeys(COUNTS, 0) for f in FEATURES}
        self.first_value: dict[str, str] = {}

    # one series, latest known
    def _at(self, sid: str, decided: datetime, back: int = 0):
        """(value now, value ``back`` observations earlier, obs date) of ``sid`` at ``decided``, or a reason."""
        s = self.series.get(sid)
        if s is None:
            return "missing"
        i = s.latest(decided)
        if i < 0:
            return "not_known"
        if decided - s.known[i] > self.stale:
            return "stale"
        if i - back < 0:
            return "history"
        now, then = s.values[i], s.values[i - back]
        if now is None or then is None:
            return "missing"
        return now, then, s.dates[i]

    def _spy_before(self, day: str, n: int) -> list[float] | None:
        j = bisect.bisect_left(self.close_days, day)
        return [c for _, c in self.closes[j - n:j]] if j >= n else None

    def _rv(self, through: str) -> float | None:
        """Annualised % stdev of the 20 SPY daily log returns ending at the close of ``through``."""
        j = bisect.bisect_right(self.close_days, through)
        if j < RV_DAYS + 1 or self.close_days[j - 1] != through:
            return None  # too little SPY history, or no SPY close on the VIX date
        c = [x for _, x in self.closes[j - RV_DAYS - 1:j]]
        rets = [math.log(b / a) for a, b in pairwise(c)]
        return 100 * math.sqrt(252) * statistics.stdev(rets)

    def _feature(self, f: str, day: str, decided: datetime, prior_day: str | None):
        kind, inputs = FEATURES[f]
        if kind in ("trend200", "drawdown252"):
            n = TREND if kind == "trend200" else DRAWDOWN
            c = self._spy_before(day, n)
            j = bisect.bisect_left(self.close_days, day)
            if c is None:
                return "history"
            if prior_day is not None and self.close_days[j - 1] != prior_day:
                return "missing"
            return c[-1] / (sum(c) / n) - 1 if kind == "trend200" else c[-1] / max(c) - 1
        if kind == "vrp":
            got = self._at(inputs[0], decided)
            if isinstance(got, str):
                return got
            rv = self._rv(got[2])
            return "history" if rv is None else got[0] - rv
        if kind in ("level", "d1", "d20", "pct20"):
            back = {"level": 0, "d1": 1, "d20": 20, "pct20": 20}[kind]
            got = self._at(inputs[0], decided, back)
            if isinstance(got, str):
                return got
            now, then, _ = got
            if kind == "level":
                return now
            if kind == "pct20":
                return now / then - 1 if then else "missing"
            return now - then
        if kind == "spread":  # a - b on a's newest known date; b must be known for that date too
            got = self._at(inputs[0], decided)
            if isinstance(got, str):
                return got
            b = self.series.get(inputs[1])
            if b is None:
                return "missing"
            k = bisect.bisect_left(b.dates, got[2])
            if k >= len(b.dates) or b.dates[k] != got[2] or b.known[k] >= decided or b.values[k] is None:
                return "missing"
            return got[0] - b.values[k]
        if kind == "ratio":  # a / b on the newest date where both are known (edge9 S3)
            a, b = self.series.get(inputs[0]), self.series.get(inputs[1])
            if a is None or b is None:
                return "missing"
            ia, ib = a.latest(decided), b.latest(decided)
            if ia < 0 or ib < 0:
                return "not_known"
            ka = ia
            while ka >= 0 and (a.dates[ka] > b.dates[ib] or a.dates[ka] not in b.pos):
                ka -= 1
            if ka < 0:
                return "not_known"
            kb = b.pos[a.dates[ka]]
            if decided - max(a.known[ka], b.known[kb]) > self.stale:
                return "stale"
            va, vb = a.values[ka], b.values[kb]
            return va / vb if va is not None and vb else "missing"
        if kind == "mean5_vintage":
            s = self.vseries.get(inputs[0])
            if s is None:
                return "missing"
            i = s.latest(decided)
            if i < 0:
                return "not_known"
            if decided - s.known[i] > self.stale:
                return "stale"
            if i < 4:
                return "history"
            if decided.astimezone(NY).date() > date.fromisoformat(s.dates[i - 4]) + timedelta(days=MAX_AGE_DAYS):
                return "stale"
            vals = [s.value_as_of(k, decided) for k in range(i - 4, i + 1)]
            return "missing" if any(v is None for v in vals) else sum(vals) / 5
        raise MacroDataError(f"unknown macro kind {kind}")

    def features_at(self, day: str, decided: datetime, wanted: tuple[str, ...], prior_day: str | None = None) -> dict:
        """Every wanted macro feature for a decision at ``decided`` on session ``day``. A series value counts only
        with known_at < ``decided``; SPY closes only of sessions before ``day``. A missing input gives None."""
        out = {}
        for f in wanted:
            got = self._feature(f, day, decided, prior_day)
            c = self.counts[f]
            c["decisions"] += 1
            if isinstance(got, str):
                c[got] += 1
                out[f] = None
            else:
                c["value"] += 1
                self.first_value.setdefault(f, day)
                out[f] = got
        return out

    def coverage(self) -> dict:
        meta = self.snapshot.meta
        return {
            "snapshot": {"schema": meta["schema"], "content_sha256": meta["content_sha256"],
                         "window": meta["window"], "staged_files": meta.get("staged_files"),
                         "duplicate_rows_dropped": meta.get("duplicate_rows_dropped")},
            "stale_days": self.stale.days,
            "pit": {
                "series": "an observation counts only when its publication-rule known_at is strictly before T; "
                          "every rule posts after 15:00 ET, so a value is fixed for the session",
                "vintages": VINTAGE_RULE,
                "spy": "S1, S2: settled closes of sessions before the decision's session; S5: VIX minus the 20-day "
                       "realized vol of SPY closes ending on the VIX observation's date",
                "staleness": f"the newest known observation must have become known within {self.stale.days} days",
            },
            "series": meta["series"],
            "features": {f: {**c, "first_value": self.first_value.get(f)} for f, c in self.counts.items()
                         if c["decisions"]},
        }


def series_report() -> dict:
    """Used and excluded series, each with its rule and revision status (for the PR and the report)."""
    return {"used": {s: {**v, "rule_text": rule_text(s), "in_grid_revised_list": revised(s)}
                     for s, v in SERIES.items()},
            "excluded": EXCLUDED}


def main(argv=None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="factory 5 macro snapshot")
    sub = ap.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export-sql", help="print the two read-only SELECTs")
    e.add_argument("--last", default="2026-11-10")
    b = sub.add_parser("snapshot", help="build the frozen snapshot from the exported CSVs")
    b.add_argument("--obs", required=True, type=Path)
    b.add_argument("--vintages", required=True, type=Path)
    b.add_argument("--staged-dir", required=True, type=Path)
    b.add_argument("--out", required=True, type=Path)
    b.add_argument("--last", default="2026-11-10")
    a = ap.parse_args(argv)
    if a.cmd == "export-sql":
        print(json.dumps(export_sql(last=a.last), indent=1))
        return 0
    meta = build_snapshot(a.obs, a.vintages, a.staged_dir, a.out, last=a.last)
    print(json.dumps({"ok": True, "out": str(a.out), "content_sha256": meta["content_sha256"],
                      "sha256": hashlib.sha256(Path(a.out).read_bytes()).hexdigest()}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
