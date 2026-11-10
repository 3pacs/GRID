#!/usr/bin/env python3
"""Repair plan for ``{ticker}_full`` resolved vintages written on the wrong price basis.

Dry run by default: reads only, reports counts, writes nothing. ``--apply``
performs the repair and is refused without ``--run-tag`` and outside the
operator's explicit go-ahead (this script cannot check that; the person
running it can). Nothing here ever UPDATEs or DELETEs a row.

Background (docs/reference/PRICE_PANEL_BASIS.md, 2026-11-08). Until
normalization/entity_map.py's PRICE BASIS POLICY both ``YF:<T>:adj_close``
and ``YF:<T>:close`` fed ``<t>_full``, so its vintages alternate between
dividend/split-adjusted and raw closes. ``build_price_panel`` read the
latest vintage per date and the universal split adjuster compounded the
flips (XLP first-to-last ratio 8e201, GLD 2,071x, NVDA 366x). The code fix
stops new wrong-basis vintages; the rows already written are still served
for every ``as_of`` that reaches them. This script plans and, on request,
performs the repair of those rows.

Scope (added 2026-11-08 after the first dry run on grid-svr). A feature is
planned only when the entity map feeds it from the policy's single yfinance
price field: ``YF:<T>:adj_close`` (or ``YF:<T>:close`` for
RAW_CLOSE_FULL_FEATURES), with ``<T>`` taken from that feed, never from the
feature name. Everything else is reported with a status and never touched:

* OUT_OF_SCOPE -- static feed is not one yfinance price field, or a yfinance
  field of another ticker (``btc_full`` <- BINANCE/CoinGecko; ``gold_full``'s
  name would have become Barrick's ticker GOLD, which turned every gold
  futures row into an "outlier" against a $30 stock).
* NO_BASIS_SPLIT -- RAW_CLOSE_FULL_FEATURES other than ``spy_full``
  (indices, futures, crypto): no corporate actions, so ``adj_close == close``
  and a value that matches one field only is pull-timing drift on a 24/7 or
  settlement bar, not a basis flip.
* EXCLUDED -- named with ``--exclude``.
* NEEDS_BACKFILL -- in scope and classified, but the retraction would leave
  more than ``--coverage-floor`` (default 1 %) of the served cells with no
  on-basis raw row to re-vintage from (ACN/HON: Tiingo adj_close starts
  2021-04-01, half the 2010+ cells). Reported with the obs_date range to
  backfill; never applied. Backfill first, then re-run: the new raw rows get
  the backfill day's vintage and the repair re-vintages from them.

What the repair does, per IN_SCOPE feature:

1. Classifies every resolved row of the feature by matching its value
   against the raw rows of the two price fields for that obs_date:
   ON_BASIS (equals the target field -- ``adj_close``, or ``close`` for
   RAW_CLOSE_FULL_FEATURES), OFF_BASIS (equals the other field only),
   AMBIGUOUS (equals both; harmless, kept), UNMATCHED (equals neither;
   kept, reported). An ON_BASIS/UNMATCHED row more than 50 % away from
   the median of its neighbouring on-basis cells is an OUTLIER (the
   ~0.6 holiday prints a yfinance writer stamped into XLP/XLB/XLRE
   adj_close).
2. ``--apply`` retracts OFF_BASIS and OUTLIER rows through
   ``resolved_series_retractions`` (append-only, PIT-safe: reads with
   ``as_of`` before the retraction day still reproduce exactly what they
   served; reads from today on no longer see the row). The row itself is
   not touched.
3. A cell left with no visible vintage gets ONE new vintage dated the
   repair day, carrying the latest on-basis raw value (ON CONFLICT DO
   NOTHING on the unique key) -- provided the date has a raw close row (a
   real trading day) and the value is not itself an outlier against the
   neighbouring cells; otherwise the latest sane candidate, or nothing
   (``revintage_rejected`` lists why). The r1/r2 plans re-vintaged XLP's
   0.6 holiday junk from the junk print itself; this rule is the fix. A
   cell with no usable on-basis raw row stays dark and is counted.

Dark-cell accounting: ``cells_dark_before`` (no visible row today: every row
already retracted, typically the backdated fill rows retracted by
scripts/audit_backdated_vintages.py), ``cells_newly_dark`` (served today on
a real trading day -- a SUCCESS raw close row exists for the date -- and dark
after the plan: the number the coverage floor judges),
``cells_phantom_cleared`` (served today on a date with no raw close at all,
i.e. a holiday junk print, dark after the plan: the repair working as
intended), ``cells_restored`` (dark today, re-vintaged by the plan),
``cells_left_dark`` (dark after the plan, before + newly + phantom, for
continuity with the first dry run).

Pre-inception cells (added 2026-11-10). ``xlre_full`` served a 2010-01-01
cell and others before XLRE existed (listed 2015-10-08); the junk value sits
in BOTH raw fields, so the classifier calls it AMBIGUOUS (kept) and the
neighbouring junk cells anchor the outlier median. A ticker listed in
``INCEPTION_DATES`` (an explicit, cited override map, never a data
heuristic) has every row dated before its first trading day classed
PRE_INCEPTION: retracted under its own reason, never re-vintaged, never a
neighbour for the outlier reference, and a pre-inception cell going dark is
``cells_phantom_cleared`` (never against the coverage floor).

Trading-day test (2026-11-10). "A raw close row exists" misfired as the test
for a real trading day (nasdaq_full, russell2000_full, pypl_full, rxt_full
were flagged on ordinary Mondays: their raw close rows were simply absent).
``after_first_on_trading_day`` now asks the market calendar
(``is_trading_day``: ingestion/market_calendar.py plus its documented
corrections). The newly-dark / phantom split used the same proxy and had the
mirror-image fault (a served cell on a real trading day with no raw close row
was called phantom and hidden from the floor): a gone cell is now phantom
only when the calendar says closed AND no raw close row exists, or when it is
pre-inception; otherwise it is a real loss.

Reads are bounded and indexed: ``resolved_series`` by feature_id in one
transaction, ``raw_series`` by (series_id, obs_date range) in one-year
chunks, EACH CHUNK IN ITS OWN READ ONLY TRANSACTION, so no connection is
held for the whole feature (the first dry run held one 60-70 s per large
feature). Never an IN-list over raw_series. Every read-only statement has a
statement_timeout. Like scripts/audit_backdated_vintages.py
it refuses to run between 03:30 and 10:30 UTC (backup/DB window) unless
``--allow-db-window`` is passed.

Usage::

    python scripts/repair_full_panel_vintages.py --feature xlp_full --feature gld_full
    python scripts/repair_full_panel_vintages.py --all-full-features --max-features 40 --out plan.json
    python scripts/repair_full_panel_vintages.py --all-full-features --exclude lly_full --out plan.json
    python scripts/repair_full_panel_vintages.py --feature xlp_full --apply --run-tag full-basis-20261108

Exit status: 0 complete (dry run or apply); 1 any feature reported an error
or the run stopped at the DB window; 2 usage refused (apply without run-tag,
retractions table missing).

Output: JSON on stdout (machine-readable first), one summary line per
feature on stderr. The ``downstream`` block names the records that read the
corrupted panel and must be re-labelled as invalid by their owners; this
script does not touch them.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text  # noqa: E402
from sqlalchemy.engine import Connection, Engine  # noqa: E402

from ingestion.market_calendar import is_market_open, is_weekend
from normalization.entity_map import RAW_CLOSE_FULL_FEATURES, static_feeds  # noqa: E402
from price_close_contract import SPY_CLOSE_FEATURE  # noqa: E402

DB_WINDOW_START = time(3, 30)
DB_WINDOW_END = time(10, 30)
DEFAULT_STATEMENT_TIMEOUT_MS = 60_000
DEFAULT_START = date(2010, 1, 1)
VALUE_TOL_REL = 1e-6
VALUE_TOL_ABS = 1e-6
OUTLIER_REL = 0.50
OUTLIER_NEIGHBOURS = 5
#: A neighbour more than this many calendar days from the date being judged
#: is no evidence about it. Without the bound, a cell at the start of a long
#: dark span was judged against cells years away: r3 (2026-11-09) rejected
#: every honest DIS/DUK/LOW/MCD/LLY/MMM/MSTR 2010-2024 adj_close and the
#: fresh ACN/HON backfill as "-50..-94 % vs neighbours" against 2021+ prices.
OUTLIER_MAX_GAP_DAYS = 30
#: Largest share of today's served cells a plan may leave dark before the
#: feature is NEEDS_BACKFILL instead of applicable.
DEFAULT_COVERAGE_FLOOR = 0.01
RETRACTION_REASON = (
    "wrong price basis for {feature}: value matches YF:{ticker}:{field}, "
    "basis is YF:{ticker}:{target} (PRICE BASIS POLICY 2026-11-08)"
)
OUTLIER_REASON = (
    "outlier vintage for {feature}: {pct:+.0%} vs neighbouring on-basis cells "
    "(junk print, PRICE BASIS POLICY 2026-11-08)"
)

PRE_INCEPTION_REASON = (
    "pre-inception cell for {feature}: obs_date {obs_date} is before {ticker}'s first trading day "
    "{inception} (no such price existed; pre-inception repair 2026-11-10)"
)

#: First exchange trading day per ticker, for ETFs whose ``_full`` feature
#: serves cells before the fund existed. EXPLICIT OVERRIDES, never inferred
#: from data (the junk sits in both raw fields, so data cannot tell). A row
#: with obs_date before the date is retracted and never re-vintaged.
#: Sources: State Street Global Advisors fund pages, read 2026-11-10. Each
#: page's "Inception Date" is the day the fund was seeded, one day before the
#: "Listing Date" (NYSE Arca, first day a price exists); the listing date is
#: the one used here:
#:   XLRE  Inception Oct 07 2015, Listing Date Oct 08 2015
#:         https://www.ssga.com/us/en/intermediary/etfs/funds/real-estate-select-sector-spdr-fund-xlre
#:   XLC   Inception Jun 18 2018, Listing Date Jun 19 2018
#:         https://www.ssga.com/us/en/intermediary/etfs/funds/communication-services-select-sector-spdr-fund-xlc
INCEPTION_DATES: dict[str, date] = {
    "XLRE": date(2015, 10, 8),
    "XLC": date(2018, 6, 19),
}

#: Weekday closures ingestion/market_calendar.py does not list (it covers the
#: standard holidays only): Hurricane Sandy, and the national days of mourning
#: for G. H. W. Bush and J. Carter.
_SPECIAL_CLOSURES = frozenset({date(2012, 10, 29), date(2012, 10, 30), date(2018, 12, 5), date(2025, 1, 9)})
#: NYSE first closed for Juneteenth in 2022; market_calendar lists it for every year.
_JUNETEENTH_FIRST_YEAR = 2022


def _juneteenth_observed(year: int) -> date:
    d = date(year, 6, 19)
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def is_trading_day(d: date) -> bool:
    """US equity market open on ``d``: ``is_market_open`` corrected for pre-2022 Juneteenth and one-off closures."""
    if d in _SPECIAL_CLOSURES:
        return False
    if is_market_open(d):
        return True
    return d.year < _JUNETEENTH_FIRST_YEAR and not is_weekend(d) and d == _juneteenth_observed(d.year)


ON_BASIS = "ON_BASIS"
OFF_BASIS = "OFF_BASIS"
AMBIGUOUS = "AMBIGUOUS"
UNMATCHED = "UNMATCHED"
OUTLIER = "OUTLIER"
PRE_INCEPTION = "PRE_INCEPTION"
ALREADY_RETRACTED = "ALREADY_RETRACTED"

# per-feature plan status
IN_SCOPE = "IN_SCOPE"
OUT_OF_SCOPE = "OUT_OF_SCOPE"
NO_BASIS_SPLIT = "NO_BASIS_SPLIT"
EXCLUDED = "EXCLUDED"
NEEDS_BACKFILL = "NEEDS_BACKFILL"
APPLICABLE_STATUSES = frozenset({IN_SCOPE})

_YF_PRICE_RE = re.compile(r"^YF:(?P<ticker>[^:]+):(?P<field>close|adj_close)$")

#: Records produced from the corrupted panel. Owners re-label; this script
#: reads none of them and writes none of them.
DOWNSTREAM_RECORDS: list[dict[str, str]] = [
    {
        "record": "astrogrid.backtest_run / backtest_result, run_key rotation_variant_*",
        "producer": "alpha_research/strategies/rotation_variant_backtest.py (fleet job rotation_variant)",
        "why": "242 runs 2026-05-08..05-19 read build_price_panel over the mixed-basis panel; "
               "Sharpe 4.5-4.7 and +9,600 % on every config are the vintage-flip artefact",
        "action": "mark invalid; re-run only after this repair and with the gate in "
                  "docs/paper_log/high-sharpe-ensemble-v1-preregistration.md",
    },
    {
        "record": "paper_trades strategy adaptive_rotation_live (wallet grid_rotation_paper_7913a6f5)",
        "producer": "alpha_research/strategies/adaptive_rotation.py via scripts/rotation_paper_trader.py",
        "why": "signals from build_price_panel; 19 of 23 trades at a flat price (stale-price bug)",
        "action": "no usable record; keep as history, exclude from any performance table",
    },
    {
        "record": "QuantaAlpha / LightGBM rank signals and their logged scores (2026)",
        "producer": "alpha_research/signals/quanta_alpha.py over build_price_panel",
        "why": "equity ranks computed on the mixed-basis panel",
        "action": "owner to re-label the 2026 runs invalid; recompute after repair",
    },
    {
        "record": "realized_alpha_daily rows with spy_feature = spy_full",
        "producer": "alpha_research/realized_alpha.py load_spy_path (PITStore, LATEST_AS_OF)",
        "why": "spy_full carried adjusted vintages (2026-04-24/29) next to the receipted raw close",
        "action": "re-score the affected windows after repair; keep the old rows (table is a log)",
    },
]


class DbWindowRefused(RuntimeError):
    pass


def check_window(now: datetime, allow: bool) -> None:
    t = now.astimezone(timezone.utc).time()
    if not allow and DB_WINDOW_START <= t <= DB_WINDOW_END:
        raise DbWindowRefused(
            f"{now.astimezone(timezone.utc).isoformat()} is inside the 03:30-10:30 UTC DB window; "
            "pass --allow-db-window for a non-production database"
        )


def _readonly(conn: Connection, timeout_ms: int) -> None:
    conn.execute(text("SET TRANSACTION READ ONLY"))
    conn.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": str(int(timeout_ms))})


def _timeout(conn: Connection, timeout_ms: int) -> None:
    conn.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": str(int(timeout_ms))})


# ---------------------------------------------------------------------------
# pure planning
# ---------------------------------------------------------------------------

def ticker_for_feature(feature: str) -> str:
    """``xlp_full`` -> ``XLP``, ``brk-b_full`` -> ``BRK-B``, ``btc_usd_full`` -> ``BTC-USD``."""
    if not feature.endswith("_full"):
        raise ValueError(f"not a _full feature: {feature}")
    return feature[: -len("_full")].upper().replace("_", "-")


def target_field(feature: str) -> str:
    return "close" if feature in RAW_CLOSE_FULL_FEATURES else "adj_close"


@dataclass
class Scope:
    """Whether and how a ``_full`` feature may be planned (pure, no DB)."""
    feature: str
    status: str
    ticker: str
    target_field: str
    feed: str  # "static" | "pattern" | "n/a"
    feeds: list[str] = field(default_factory=list)
    reason: str | None = None


def feature_scope(feature: str, *, exclude: Iterable[str] = (),
                  mappings: dict[str, str] | None = None) -> Scope:
    """Decide a feature's status from the entity map, taking the ticker from its feed.

    IN_SCOPE needs exactly one static feed that is ``YF:<T>:<policy field>``,
    or no static feed at all (the ``YF:<T>:adj_close -> <t>_full`` pattern
    fallback, where ``<T>`` is derived from the name). RAW_CLOSE_FULL_FEATURES
    other than ``spy_full`` are NO_BASIS_SPLIT; a feature whose static
    feed is another ticker or another source is OUT_OF_SCOPE.
    """
    target = target_field(feature)
    feeds = static_feeds(feature, mappings)
    ticker = ticker_for_feature(feature)
    feed = "pattern"
    if feeds:
        feed = "static"
        price = [(sid, _YF_PRICE_RE.match(sid)) for sid in feeds]
        if len(feeds) != 1 or price[0][1] is None:
            return Scope(feature, OUT_OF_SCOPE, ticker, target, feed, feeds,
                         f"static feed is not one yfinance price field: {feeds}; the alternating-basis "
                         "defect cannot have happened here and a same-named yfinance ticker is unrelated")
        sid, m = price[0]
        ticker = m.group("ticker")
        if m.group("field") != target:
            return Scope(feature, OUT_OF_SCOPE, ticker, target, feed, feeds,
                         f"static feed {sid} is not the policy basis field {target!r}")
    if feature in set(exclude):
        return Scope(feature, EXCLUDED, ticker, target, feed, feeds, "named with --exclude")
    if feature in RAW_CLOSE_FULL_FEATURES and feature != SPY_CLOSE_FEATURE:
        return Scope(feature, NO_BASIS_SPLIT, ticker, target, feed, feeds,
                     "raw-close basis by allowlist and no corporate actions: adj_close == close, a value "
                     "matching one field only is pull-timing drift, not a basis flip")
    return Scope(feature, IN_SCOPE, ticker, target, feed, feeds)


def _close(a: float, b: float) -> bool:
    return abs(a - b) <= max(VALUE_TOL_ABS, VALUE_TOL_REL * max(abs(a), abs(b)))


@dataclass
class ResolvedRow:
    id: int
    obs_date: date
    vintage_date: date
    value: float
    source_id: int | None
    retracted: bool = False
    cls: str = ""
    matched_field: str | None = None
    outlier_pct: float | None = None


@dataclass
class RawCandidate:
    value: float
    pull_date: date
    source_id: int


def classify_rows(
    rows: list[ResolvedRow],
    raw: dict[str, dict[date, list[RawCandidate]]],
    target: str,
) -> None:
    """Set ``cls`` / ``matched_field`` on every row (in place).

    A row matches a field when a SUCCESS raw row of that field on the same
    obs_date carries the same value (relative 1e-6). Same-vintage matches
    are preferred but any vintage counts: the resolver's vintage is the pull
    date, yet a re-pulled unchanged value is the same observation.
    """
    other = "close" if target == "adj_close" else "adj_close"
    for r in rows:
        if r.retracted:
            r.cls = ALREADY_RETRACTED
            continue
        hit_target = any(_close(r.value, c.value) for c in raw.get(target, {}).get(r.obs_date, []))
        hit_other = any(_close(r.value, c.value) for c in raw.get(other, {}).get(r.obs_date, []))
        if hit_target and hit_other:
            r.cls, r.matched_field = AMBIGUOUS, "both"
        elif hit_target:
            r.cls, r.matched_field = ON_BASIS, target
        elif hit_other:
            r.cls, r.matched_field = OFF_BASIS, other
        else:
            r.cls, r.matched_field = UNMATCHED, None


def latest_per_cell(rows: Iterable[ResolvedRow], keep: Callable[[ResolvedRow], bool]) -> dict[date, ResolvedRow]:
    """LATEST_AS_OF as of today over the rows ``keep`` admits."""
    best: dict[date, ResolvedRow] = {}
    for r in rows:
        if not keep(r):
            continue
        cur = best.get(r.obs_date)
        if cur is None or r.vintage_date > cur.vintage_date:
            best[r.obs_date] = r
    return best


class NeighbourReference:
    """Median of the latest on-basis values of the nearest cells around a date.

    Only cells within ``max_gap_days`` of the date count as neighbours.
    """

    def __init__(self, rows: list[ResolvedRow], neighbours: int = OUTLIER_NEIGHBOURS,
                 max_gap_days: int = OUTLIER_MAX_GAP_DAYS) -> None:
        ref = latest_per_cell(rows, lambda r: r.cls in (ON_BASIS, AMBIGUOUS))
        self._init({d: r.value for d, r in ref.items()}, neighbours, max_gap_days)

    @classmethod
    def from_values(cls, values: dict[date, float], neighbours: int = OUTLIER_NEIGHBOURS,
                    max_gap_days: int = OUTLIER_MAX_GAP_DAYS) -> NeighbourReference:
        """A reference over a plain date -> value map (e.g. raw candidates)."""
        self = cls.__new__(cls)
        self._init(values, neighbours, max_gap_days)
        return self

    def _init(self, values: dict[date, float], neighbours: int, max_gap_days: int) -> None:
        self.ref = values
        self.dates = sorted(values)
        self.pos = {d: i for i, d in enumerate(self.dates)}
        self.neighbours = neighbours
        self.max_gap = timedelta(days=max_gap_days)

    def deviation(self, obs_date: date, value: float) -> float | None:
        """``value / median - 1`` against the neighbouring cells, or None when too few neighbours."""
        if len(self.dates) < 3:
            return None
        i = self.pos.get(obs_date)
        n = self.neighbours
        if i is None:
            # Not a reference cell itself: anchor on the nearest earlier cell.
            i = sum(1 for d in self.dates if d < obs_date)
            window = self.dates[max(0, i - n): i] + self.dates[i: i + n]
        else:
            window = self.dates[max(0, i - n): i] + self.dates[i + 1: i + 1 + n]
        vals = [self.ref[d] for d in window
                if self.ref[d] > 0 and abs(d - obs_date) <= self.max_gap]
        if len(vals) < 3:
            return None
        med = statistics.median(vals)
        if med <= 0:
            return None
        return value / med - 1.0


def flag_outliers(rows: list[ResolvedRow], neighbours: int = OUTLIER_NEIGHBOURS, rel: float = OUTLIER_REL) -> int:
    """Mark ON_BASIS / UNMATCHED / AMBIGUOUS rows far from their neighbours as OUTLIER.

    The reference for a row is the median of the latest on-basis values of
    the ``neighbours`` nearest cells on each side (its own cell excluded).
    Returns the number of rows flagged.
    """
    reference = NeighbourReference(rows, neighbours)
    flagged = 0
    for r in rows:
        if r.cls not in (ON_BASIS, UNMATCHED, AMBIGUOUS):
            continue
        pct = reference.deviation(r.obs_date, r.value)
        if pct is not None and abs(pct) > rel:
            r.cls, r.outlier_pct = OUTLIER, pct
            flagged += 1
    return flagged


def ratio_and_jumps(series: dict[date, float]) -> dict[str, Any]:
    """First-to-last ratio and count of day moves above 10 % for a date->value map."""
    if len(series) < 2:
        return {"n": len(series), "ratio": None, "jumps_gt_10pct": 0}
    ds = sorted(series)
    vals = [series[d] for d in ds]
    jumps = sum(1 for a, b in zip(vals, vals[1:]) if a and abs(b / a - 1.0) > 0.10)
    return {
        "n": len(ds), "first": [ds[0].isoformat(), vals[0]], "last": [ds[-1].isoformat(), vals[-1]],
        "ratio": (vals[-1] / vals[0]) if vals[0] else None, "jumps_gt_10pct": jumps,
    }


@dataclass
class FeaturePlan:
    feature: str
    ticker: str
    target_field: str
    status: str = IN_SCOPE
    scope_reason: str | None = None
    feed: str = "n/a"
    feeds: list[str] = field(default_factory=list)
    feature_id: int | None = None
    rows: int = 0
    cells: int = 0
    by_class: dict[str, int] = field(default_factory=dict)
    retract: list[dict[str, Any]] = field(default_factory=list)
    revintage: list[dict[str, Any]] = field(default_factory=list)
    revintage_rejected: list[dict[str, Any]] = field(default_factory=list)
    cells_dark_before: int = 0
    cells_dark_after: int = 0
    cells_revintaged: int = 0
    cells_left_dark: int = 0
    cells_newly_dark: int = 0
    cells_phantom_cleared: int = 0
    cells_restored: int = 0
    #: ``INCEPTION_DATES`` entry for the ticker (ISO date) and how many of
    #: ``cells_phantom_cleared`` are cells dated before it.
    inception_date: str | None = None
    cells_pre_inception_cleared: int = 0
    newly_dark_share: float | None = None
    newly_dark_range: dict[str, Any] | None = None
    #: False when the first cell the repaired series would serve is not a US
    #: trading day per the market calendar (a holiday junk print survived) or
    #: is before the ticker's inception; None when nothing is served.
    after_first_on_trading_day: bool | None = None
    backfill_hint: str | None = None
    raw_rows_read: dict[str, int] = field(default_factory=dict)
    served_before: dict[str, Any] = field(default_factory=dict)
    served_after: dict[str, Any] = field(default_factory=dict)
    examples: list[dict[str, Any]] = field(default_factory=list)
    applied: dict[str, int] | None = None
    error: str | None = None

    @property
    def applicable(self) -> bool:
        return self.status in APPLICABLE_STATUSES and self.error is None and bool(self.retract or self.revintage)

    def summary(self) -> dict[str, Any]:
        d = asdict(self)
        d["retract"] = len(self.retract)
        d["revintage"] = len(self.revintage)
        d["revintage_rejected"] = len(self.revintage_rejected)
        by_why: dict[str, int] = {}
        for x in self.revintage_rejected:
            key = re.sub(r"\s*\(.*\)$", "", x["why"])
            by_why[key] = by_why.get(key, 0) + 1
        d["revintage_rejected_by_why"] = by_why
        d["applicable"] = self.applicable
        return d


def plan_from_scope(scope: Scope, feature_id: int | None = None) -> FeaturePlan:
    return FeaturePlan(feature=scope.feature, ticker=scope.ticker, target_field=scope.target_field,
                       status=scope.status, scope_reason=scope.reason, feed=scope.feed,
                       feeds=list(scope.feeds), feature_id=feature_id)


def plan_feature(
    feature: str,
    rows: list[ResolvedRow],
    raw: dict[str, dict[date, list[RawCandidate]]],
    *,
    repair_day: date,
    feature_id: int | None = None,
    scope: Scope | None = None,
    coverage_floor: float = DEFAULT_COVERAGE_FLOOR,
) -> FeaturePlan:
    """Pure: classify, flag, simulate the served series before/after, list actions.

    ``scope`` defaults to :func:`feature_scope` (no exclusions); a plan is built
    whatever the status so a dry run can still show what the rows look like,
    but only IN_SCOPE plans are applicable. A plan whose newly-dark share
    exceeds ``coverage_floor`` becomes NEEDS_BACKFILL.
    """
    if scope is None:
        scope = feature_scope(feature)
    target = scope.target_field
    plan = plan_from_scope(scope, feature_id)
    plan.rows, plan.cells = len(rows), len({r.obs_date for r in rows})
    classify_rows(rows, raw, target)
    inception = INCEPTION_DATES.get(plan.ticker)
    plan.inception_date = inception.isoformat() if inception else None
    if inception is not None:
        # Before outlier flagging: a pre-inception row is not an ON_BASIS /
        # AMBIGUOUS neighbour, so the junk cannot anchor its own median.
        for r in rows:
            if r.cls != ALREADY_RETRACTED and r.obs_date < inception:
                r.cls = PRE_INCEPTION
    flag_outliers(rows)
    plan.by_class = {}
    for r in rows:
        plan.by_class[r.cls] = plan.by_class.get(r.cls, 0) + 1
    plan.raw_rows_read = {f: sum(len(v) for v in by_date.values()) for f, by_date in raw.items()}

    all_cells = {r.obs_date for r in rows}
    before = latest_per_cell(rows, lambda r: not r.retracted)
    dark_before = all_cells - set(before)
    plan.cells_dark_before = len(dark_before)
    plan.served_before = ratio_and_jumps({d: r.value for d, r in before.items()})

    to_retract = [r for r in rows if r.cls in (OFF_BASIS, OUTLIER, PRE_INCEPTION)]
    other = "close" if target == "adj_close" else "adj_close"
    for r in to_retract:
        if r.cls == PRE_INCEPTION:
            reason = PRE_INCEPTION_REASON.format(feature=feature, obs_date=r.obs_date.isoformat(),
                                                 ticker=plan.ticker, inception=inception.isoformat())
        elif r.cls == OFF_BASIS:
            reason = RETRACTION_REASON.format(feature=feature, ticker=plan.ticker, field=other, target=target)
        else:
            reason = OUTLIER_REASON.format(feature=feature, pct=r.outlier_pct or 0.0)
        plan.retract.append({"resolved_id": r.id, "obs_date": r.obs_date.isoformat(),
                             "vintage_date": r.vintage_date.isoformat(), "value": r.value,
                             "cls": r.cls, "reason": reason})
    retract_keys = {(r.obs_date, r.vintage_date) for r in to_retract}

    after = latest_per_cell(rows, lambda r: not r.retracted and (r.obs_date, r.vintage_date) not in retract_keys)
    dark = sorted({r.obs_date for r in rows} - set(after))
    plan.cells_dark_after = len(dark)
    # The reference for a re-vintage candidate is the series that survives
    # the retractions (OUTLIER rows are no longer ON_BASIS, so a junk print
    # never anchors its own neighbourhood).
    reference = NeighbourReference(rows)
    # Where no served cell is near (a long dark span: every cell of a stretch
    # was raw-basis), judge a candidate against the candidates of the nearby
    # dark dates instead: the on-basis raw series must agree with itself, so a
    # junk print inside the span is still caught.
    candidate_ref = NeighbourReference.from_values({
        d: max(raw[target][d], key=lambda c: c.pull_date).value
        for d in dark if raw.get(target, {}).get(d) and d in raw.get("close", {})
        and (inception is None or d >= inception)
    })

    def deviation(d: date, value: float) -> float | None:
        pct = reference.deviation(d, value)
        return candidate_ref.deviation(d, value) if pct is None else pct

    closed_day_junk: set[date] = set()  # market-closed dates whose only raw close print is a junk outlier
    for d in dark:
        if inception is not None and d < inception:
            continue  # pre-inception: never re-vintaged, whatever the raw rows say
        cands = raw.get(target, {}).get(d, [])
        if not cands:
            continue
        if d not in raw.get("close", {}):
            # No as-traded print for the date: a holiday / weekend cell that
            # only ever held a junk adj_close row. Nothing honest to serve.
            why = ("no raw close row: not a trading day" if not is_trading_day(d)
                   else "no raw close row on a trading day")
            plan.revintage_rejected.append({"obs_date": d.isoformat(), "why": why})
            continue
        best = max(cands, key=lambda c: c.pull_date)
        pct = deviation(d, best.value)
        if pct is not None and abs(pct) > OUTLIER_REL:
            # The latest on-basis raw value is itself a junk print (the r2 plan
            # would have re-inserted XLP's 0.6 holiday prints under today's
            # vintage). Try the latest sane candidate instead.
            sane = [c for c in sorted(cands, key=lambda c: c.pull_date, reverse=True)
                    if (p2 := deviation(d, c.value)) is None or abs(p2) <= OUTLIER_REL]
            if not sane:
                if not is_trading_day(d):
                    closed_day_junk.add(d)
                plan.revintage_rejected.append({"obs_date": d.isoformat(), "value": best.value,
                                                "why": f"every on-basis raw value is an outlier ({pct:+.0%} vs neighbours)"})
                continue
            best = sane[0]
        if any(r.obs_date == d and r.vintage_date == repair_day for r in rows):
            # Today's key is taken by a row we are retracting; cannot re-vintage today.
            plan.revintage_rejected.append({"obs_date": d.isoformat(), "why": "repair-day vintage key already used"})
            continue
        plan.revintage.append({"obs_date": d.isoformat(), "value": best.value, "source_id": best.source_id,
                               "from_pull_date": best.pull_date.isoformat(), "vintage_date": repair_day.isoformat()})
        after[d] = ResolvedRow(id=-1, obs_date=d, vintage_date=repair_day, value=best.value, source_id=best.source_id)
    plan.cells_revintaged = len(plan.revintage)
    plan.cells_left_dark = plan.cells_dark_after - plan.cells_revintaged
    # A served cell that goes dark is a loss only on a real trading day: a
    # SUCCESS raw close row for that date proves there was a print. A cell on
    # a date with no raw close at all held nothing but a phantom (the ~0.6
    # holiday junk a yfinance writer stamped into XLP/XLB/XLRE/XLI adj_close:
    # r2 showed xli_full with 5,598 cells against ~4,250 trading days); going
    # dark is the repair, so it is reported apart and never counts against
    # the coverage floor. Phantom = before the ticker existed, or the calendar
    # says closed AND no raw close row exists (or the only one is a junk
    # outlier: grid-svr 2026-11-10 holds 0.91-1.0 XLRE/XLP prints on the
    # 2018-12-05 and 2025-01-09 days of mourning); a cell on a calendar
    # trading day without a raw close row is a real loss (it used to hide as
    # phantom).
    gone = set(before) - set(after)
    pre_inception = {d for d in gone if inception is not None and d < inception}
    newly_dark = sorted(d for d in gone - pre_inception - closed_day_junk
                        if d in raw.get("close", {}) or is_trading_day(d))
    plan.cells_newly_dark = len(newly_dark)
    plan.cells_phantom_cleared = len(gone) - len(newly_dark)
    plan.cells_pre_inception_cleared = len(pre_inception)
    plan.cells_restored = len(dark_before & set(after))
    plan.newly_dark_share = (len(newly_dark) / len(before)) if before else 0.0
    if newly_dark:
        plan.newly_dark_range = {"obs_min": newly_dark[0].isoformat(), "obs_max": newly_dark[-1].isoformat(),
                                 "n": len(newly_dark)}
    if plan.status == IN_SCOPE and plan.newly_dark_share > coverage_floor:
        plan.status = NEEDS_BACKFILL
        plan.scope_reason = (
            f"{len(newly_dark)} of {len(before)} served cells ({plan.newly_dark_share:.1%}) have no "
            f"YF:{plan.ticker}:{target} raw row to re-vintage from (floor {coverage_floor:.1%})"
        )
        plan.backfill_hint = (
            f"backfill YF:{plan.ticker}:{target} for obs_date {newly_dark[0].isoformat()}..{newly_dark[-1].isoformat()} "
            f"({len(newly_dark)} cells), then re-run this plan"
        )
    plan.served_after = ratio_and_jumps({d: r.value for d, r in after.items()})
    if after:
        first = min(after)
        plan.after_first_on_trading_day = is_trading_day(first) and (inception is None or first >= inception)

    for r in to_retract[:6]:
        plan.examples.append({"obs_date": r.obs_date.isoformat(), "vintage_date": r.vintage_date.isoformat(),
                              "value": r.value, "cls": r.cls, "matched_field": r.matched_field,
                              "outlier_pct": r.outlier_pct})
    return plan


# ---------------------------------------------------------------------------
# bounded reads
# ---------------------------------------------------------------------------

_RESOLVED_SQL = text("""
    SELECT rs.id, rs.obs_date, rs.vintage_date, rs.value, rs.source_priority_used,
           EXISTS (
               SELECT 1 FROM resolved_series_retractions rr
               WHERE rr.feature_id = rs.feature_id AND rr.obs_date = rs.obs_date
                 AND rr.vintage_date = rs.vintage_date
           ) AS retracted
    FROM resolved_series rs
    WHERE rs.feature_id = :fid AND rs.obs_date >= :start
    ORDER BY rs.obs_date, rs.vintage_date
""")
_RESOLVED_SQL_NO_RETRACTIONS = text("""
    SELECT rs.id, rs.obs_date, rs.vintage_date, rs.value, rs.source_priority_used, FALSE AS retracted
    FROM resolved_series rs
    WHERE rs.feature_id = :fid AND rs.obs_date >= :start
    ORDER BY rs.obs_date, rs.vintage_date
""")
_RAW_CHUNK_SQL = text("""
    SELECT obs_date, value, pull_timestamp, source_id
    FROM raw_series
    WHERE series_id = :sid AND pull_status = 'SUCCESS'
      AND obs_date BETWEEN :lo AND :hi
""")


def _as_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def read_resolved(conn: Connection, feature_id: int, start: date, has_retractions: bool) -> list[ResolvedRow]:
    sql = _RESOLVED_SQL if has_retractions else _RESOLVED_SQL_NO_RETRACTIONS
    out: list[ResolvedRow] = []
    for rid, od, vd, val, src, retracted in conn.execute(sql, {"fid": feature_id, "start": start}):
        out.append(ResolvedRow(id=int(rid), obs_date=_as_date(od), vintage_date=_as_date(vd), value=float(val),
                               source_id=int(src) if src is not None else None, retracted=bool(retracted)))
    return out


def year_chunks(start: date, end: date) -> list[tuple[date, date]]:
    out: list[tuple[date, date]] = []
    lo = start
    while lo <= end:
        hi = min(date(lo.year, 12, 31), end)
        out.append((lo, hi))
        lo = hi + timedelta(days=1)
    return out


def read_raw_chunk(conn: Connection, sid: str, lo: date, hi: date, into: dict[date, list[RawCandidate]]) -> int:
    n = 0
    for od, val, ts, src in conn.execute(_RAW_CHUNK_SQL, {"sid": sid, "lo": lo, "hi": hi}):
        if val is None:
            continue
        pulled = ts.astimezone(timezone.utc).date() if isinstance(ts, datetime) and ts.tzinfo else _as_date(ts)
        into.setdefault(_as_date(od), []).append(RawCandidate(float(val), pulled, int(src)))
        n += 1
    return n


def read_raw_by_field(engine: Engine, ticker: str, start: date, end: date,
                      timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS) -> dict[str, dict[date, list[RawCandidate]]]:
    """SUCCESS raw rows of ``YF:<ticker>:close`` and ``:adj_close``, one short READ ONLY transaction per year chunk."""
    out: dict[str, dict[date, list[RawCandidate]]] = {"close": {}, "adj_close": {}}
    for fld in out:
        sid = f"YF:{ticker}:{fld}"
        for lo, hi in year_chunks(start, end):
            with engine.connect() as conn, conn.begin():
                _readonly(conn, timeout_ms)
                read_raw_chunk(conn, sid, lo, hi, out[fld])
    return out


def retractions_table_exists(conn: Connection) -> bool:
    return bool(conn.execute(text("SELECT to_regclass(:t) IS NOT NULL"), {"t": "resolved_series_retractions"}).scalar())


def plan_feature_db(engine: Engine, feature: str, *, start: date, repair_day: date,
                    timeout_ms: int, scope: Scope | None = None,
                    coverage_floor: float = DEFAULT_COVERAGE_FLOOR) -> FeaturePlan:
    """Plan one IN_SCOPE feature: resolved rows in one transaction, raw rows per year chunk."""
    scope = scope or feature_scope(feature)
    try:
        with engine.connect() as conn, conn.begin():
            _readonly(conn, timeout_ms)
            row = conn.execute(text("SELECT id FROM feature_registry WHERE name = :n"), {"n": feature}).fetchone()
            if row is None:
                p = plan_from_scope(scope)
                p.error = "feature not in feature_registry"
                return p
            fid = int(row[0])
            rows = read_resolved(conn, fid, start, retractions_table_exists(conn))
        end = max((r.obs_date for r in rows), default=start)
        raw = read_raw_by_field(engine, scope.ticker, start, end, timeout_ms)
        return plan_feature(feature, rows, raw, repair_day=repair_day, feature_id=fid, scope=scope,
                            coverage_floor=coverage_floor)
    except Exception as exc:  # one feature's timeout must not hide the others
        p = plan_from_scope(scope)
        p.error = f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
        return p


# ---------------------------------------------------------------------------
# apply (explicit flag only)
# ---------------------------------------------------------------------------

_RETRACT_SQL = text("""
    INSERT INTO resolved_series_retractions (feature_id, obs_date, vintage_date, reason, run_tag)
    VALUES (:fid, :od, :vd, :reason, :tag)
    ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING
""")
_REVINTAGE_SQL = text("""
    INSERT INTO resolved_series
        (feature_id, obs_date, release_date, vintage_date, value, source_priority_used, conflict_flag)
    VALUES (:fid, :od, :vd, :vd, :val, :src, FALSE)
    ON CONFLICT (feature_id, obs_date, vintage_date) DO NOTHING
""")


def apply_plan(engine: Engine, plan: FeaturePlan, *, run_tag: str, timeout_ms: int) -> dict[str, int]:
    """Write the plan's retractions and re-vintages in one transaction. Appends only."""
    if plan.feature_id is None or plan.error:
        raise RuntimeError(f"{plan.feature}: nothing to apply ({plan.error})")
    if plan.status not in APPLICABLE_STATUSES:
        raise RuntimeError(f"{plan.feature}: status {plan.status} is not applicable ({plan.scope_reason})")
    done = {"retracted": 0, "revintaged": 0}
    with engine.begin() as conn:
        _timeout(conn, timeout_ms)
        if not retractions_table_exists(conn):
            raise RuntimeError("resolved_series_retractions does not exist; apply the migration first")
        for r in plan.retract:
            res = conn.execute(_RETRACT_SQL, {"fid": plan.feature_id, "od": date.fromisoformat(r["obs_date"]),
                                              "vd": date.fromisoformat(r["vintage_date"]),
                                              "reason": r["reason"], "tag": run_tag})
            done["retracted"] += max(0, int(res.rowcount or 0))
        for v in plan.revintage:
            res = conn.execute(_REVINTAGE_SQL, {"fid": plan.feature_id, "od": date.fromisoformat(v["obs_date"]),
                                                "vd": date.fromisoformat(v["vintage_date"]),
                                                "val": v["value"], "src": v["source_id"]})
            done["revintaged"] += max(0, int(res.rowcount or 0))
    return done


# ---------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------

def list_full_features(engine: Engine, limit: int, timeout_ms: int) -> list[str]:
    with engine.connect() as conn, conn.begin():
        _readonly(conn, timeout_ms)
        rows = conn.execute(text("SELECT name FROM feature_registry WHERE name LIKE :pat ORDER BY name LIMIT :n"),
                            {"pat": "%\\_full", "n": limit})
        return [r[0] for r in rows]


def run(engine: Engine, features: list[str], *, start: date, apply: bool, run_tag: str | None,
        timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS, allow_db_window: bool = False,
        exclude: Iterable[str] = (), coverage_floor: float = DEFAULT_COVERAGE_FLOOR,
        clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> dict[str, Any]:
    started = clock()
    check_window(started, allow_db_window)
    repair_day = started.astimezone(timezone.utc).date()
    exclude = frozenset(exclude)
    plans: list[dict[str, Any]] = []
    stopped = None
    for f in features:
        try:
            check_window(clock(), allow_db_window)
        except DbWindowRefused as exc:
            stopped = f"db_window: {exc}"
            break
        scope = feature_scope(f, exclude=exclude)
        if scope.status != IN_SCOPE:
            # Out of scope / skipped / excluded: no DB read, no action, reported.
            plan = plan_from_scope(scope)
        else:
            plan = plan_feature_db(engine, f, start=start, repair_day=repair_day, timeout_ms=timeout_ms,
                                   scope=scope, coverage_floor=coverage_floor)
        if apply and plan.applicable:
            try:
                plan.applied = apply_plan(engine, plan, run_tag=run_tag or "", timeout_ms=timeout_ms)
            except Exception as exc:  # noqa: BLE001
                plan.error = f"apply failed: {type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
        print(
            f"{plan.feature:>14} {plan.status:<22} rows={plan.rows:>7} cells={plan.cells:>6} "
            f"retract={len(plan.retract):>6} revintage={len(plan.revintage):>5} "
            f"dark_before={plan.cells_dark_before:>5} newly_dark={plan.cells_newly_dark:>5} "
            f"phantom={plan.cells_phantom_cleared:>5} restored={plan.cells_restored:>5} ratio {plan.served_before.get('ratio')!s:>12} -> "
            f"{plan.served_after.get('ratio')!s:>12} jumps {plan.served_before.get('jumps_gt_10pct')} -> "
            f"{plan.served_after.get('jumps_gt_10pct')}"
            + (f"  {plan.scope_reason}" if plan.scope_reason and plan.status != IN_SCOPE else "")
            + ("  WARNING first served cell is not a trading day" if plan.after_first_on_trading_day is False else "")
            + (f"  ERROR {plan.error}" if plan.error else "")
            + (f"  APPLIED {plan.applied}" if plan.applied else ""),
            file=sys.stderr,
        )
        plans.append(plan.summary())
    by_status: dict[str, int] = {}
    for p in plans:
        by_status[p["status"]] = by_status.get(p["status"], 0) + 1
    applicable = [p for p in plans if p["applicable"]]
    return {
        "generated_at": started.astimezone(timezone.utc).isoformat(), "mode": "apply" if apply else "dry_run",
        "run_tag": run_tag, "repair_day": repair_day.isoformat(), "start": start.isoformat(),
        "statement_timeout_ms": timeout_ms, "coverage_floor": coverage_floor, "exclude": sorted(exclude),
        "features": plans, "stopped": stopped,
        "complete": stopped is None and len(plans) == len(features),
        "errors": sum(1 for p in plans if p["error"]),
        "by_status": by_status,
        "after_first_not_trading_day": sorted(p["feature"] for p in plans if p["after_first_on_trading_day"] is False),
        "totals": {
            "rows": sum(p["rows"] for p in plans), "retract": sum(p["retract"] for p in plans),
            "revintage": sum(p["revintage"] for p in plans), "left_dark": sum(p["cells_left_dark"] for p in plans),
            "dark_before": sum(p["cells_dark_before"] for p in plans),
            "newly_dark": sum(p["cells_newly_dark"] for p in plans),
            "phantom_cleared": sum(p["cells_phantom_cleared"] for p in plans),
            "restored": sum(p["cells_restored"] for p in plans),
        },
        "applicable_totals": {
            "features": len(applicable), "retract": sum(p["retract"] for p in applicable),
            "revintage": sum(p["revintage"] for p in applicable),
            "newly_dark": sum(p["cells_newly_dark"] for p in applicable),
        },
        "downstream": DOWNSTREAM_RECORDS,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--feature", action="append", default=[], help="a _full feature name (repeatable)")
    p.add_argument("--all-full-features", action="store_true", help="every feature_registry name ending in _full")
    p.add_argument("--max-features", type=int, default=100)
    p.add_argument("--exclude", action="append", default=[], help="a _full feature to report but never plan or apply (repeatable)")
    p.add_argument("--coverage-floor", type=float, default=DEFAULT_COVERAGE_FLOOR,
                   help="largest newly-dark share of served cells an applicable plan may leave (default 0.01)")
    p.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START, help="first obs_date (default 2010-01-01)")
    p.add_argument("--apply", action="store_true", help="WRITE retractions and re-vintages (default: dry run)")
    p.add_argument("--run-tag", help="required with --apply; stored on every retraction")
    p.add_argument("--allow-db-window", action="store_true", help="run inside 03:30-10:30 UTC (non-production only)")
    p.add_argument("--statement-timeout-ms", type=int, default=DEFAULT_STATEMENT_TIMEOUT_MS)
    p.add_argument("--out", type=Path, help="write the JSON report here as well as stdout")
    return p


def main(argv: list[str] | None = None, engine: Engine | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.apply and not args.run_tag:
        print("--apply requires --run-tag", file=sys.stderr)
        return 2
    if not args.feature and not args.all_full_features:
        print("name at least one --feature or pass --all-full-features", file=sys.stderr)
        return 2
    if not 0.0 <= args.coverage_floor <= 1.0:
        print("--coverage-floor must be between 0 and 1", file=sys.stderr)
        return 2
    if engine is None:
        from db import get_engine  # noqa: PLC0415 -- deferred: config needs the service env

        engine = get_engine()
    features = list(args.feature)
    if args.all_full_features:
        features += [f for f in list_full_features(engine, args.max_features, args.statement_timeout_ms)
                     if f not in features]
    try:
        report = run(engine, features, start=args.start, apply=args.apply, run_tag=args.run_tag,
                     timeout_ms=args.statement_timeout_ms, allow_db_window=args.allow_db_window,
                     exclude=args.exclude, coverage_floor=args.coverage_floor)
    except DbWindowRefused as exc:
        print(str(exc), file=sys.stderr)
        return 1
    payload = json.dumps(report, indent=1, default=str)
    print(payload)
    if args.out:
        args.out.write_text(payload)
    return 0 if report["complete"] and report["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
