#!/usr/bin/env python3
"""Repair plan for ``{ticker}_full`` resolved vintages written on the wrong price basis.

Review-only: reads bounded chunks and emits a full, content-addressed manifest.
All apply entry points are disabled pending a concurrency-qualified executor.
A digest identifies reviewed bytes; it is not approval or proof of a DB snapshot.

Background (docs/reference/PRICE_PANEL_BASIS.md, 2026-11-08). Until
normalization/entity_map.py's PRICE BASIS POLICY both ``YF:<T>:adj_close``
and ``YF:<T>:close`` fed ``<t>_full``, so its vintages alternate between
dividend/split-adjusted and raw closes. ``build_price_panel`` read the
latest vintage per date and the universal split adjuster compounded the
flips (XLP first-to-last ratio 8e201, GLD 2,071x, NVDA 366x). The code fix
stops new wrong-basis vintages; the rows already written are still served
for every ``as_of`` that reaches them. This script only plans the repair;
execution remains disabled pending independent concurrency qualification.

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
2. Lists exact proposed OFF_BASIS/OUTLIER retractions and replacement
   vintages. No executor is enabled. The future executor must bind its
   reviewed identity and protect the entire read/write scope against races.
3. Rejects re-vintage candidates on dates without raw close evidence or
   with outlier values. A surviving first cell without raw close evidence
   makes the plan UNSAFE_FIRST_CELL, never applicable.

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

Exit status: 0 complete dry run; 1 feature error or DB window stop;
2 usage refused (including every --apply request). --out refuses overwrite.

Output: JSON on stdout (machine-readable first), one summary line per
feature on stderr. The ``downstream`` block names the records that read the
corrupted panel and must be re-labelled as invalid by their owners; this
script does not touch them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
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

ON_BASIS = "ON_BASIS"
OFF_BASIS = "OFF_BASIS"
AMBIGUOUS = "AMBIGUOUS"
UNMATCHED = "UNMATCHED"
OUTLIER = "OUTLIER"
ALREADY_RETRACTED = "ALREADY_RETRACTED"

# per-feature plan status
IN_SCOPE = "IN_SCOPE"
OUT_OF_SCOPE = "OUT_OF_SCOPE"
NO_BASIS_SPLIT = "NO_BASIS_SPLIT"
EXCLUDED = "EXCLUDED"
NEEDS_BACKFILL = "NEEDS_BACKFILL"
UNSAFE_FIRST_CELL = "UNSAFE_FIRST_CELL"
APPLY_DISABLED = "apply disabled: no concurrency-qualified reviewed-manifest executor"
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
    pull_timestamp: str | None = None


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
    """Median of the latest on-basis values of the nearest cells around a date."""

    def __init__(self, rows: list[ResolvedRow], neighbours: int = OUTLIER_NEIGHBOURS) -> None:
        self.ref = latest_per_cell(rows, lambda r: r.cls in (ON_BASIS, AMBIGUOUS))
        self.dates = sorted(self.ref)
        self.pos = {d: i for i, d in enumerate(self.dates)}
        self.neighbours = neighbours

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
        vals = [self.ref[d].value for d in window if self.ref[d].value > 0]
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
    newly_dark_share: float | None = None
    newly_dark_range: dict[str, Any] | None = None
    #: False when the first cell the repaired series would serve has no raw
    #: close row (a holiday junk print survived); None when nothing is served.
    after_first_on_trading_day: bool | None = None
    backfill_hint: str | None = None
    raw_rows_read: dict[str, int] = field(default_factory=dict)
    served_before: dict[str, Any] = field(default_factory=dict)
    served_after: dict[str, Any] = field(default_factory=dict)
    examples: list[dict[str, Any]] = field(default_factory=list)
    applied: dict[str, int] | None = None
    error: str | None = None
    candidate_ties: list[dict[str, Any]] = field(default_factory=list)
    input_counts: dict[str, int] = field(default_factory=dict)
    input_sha256: str | None = None

    @property
    def applicable(self) -> bool:
        return (self.status in APPLICABLE_STATUSES and self.error is None
                and self.after_first_on_trading_day is True and bool(self.retract or self.revintage))

    def summary(self) -> dict[str, Any]:
        d = asdict(self)
        d["retract"] = len(self.retract)
        d["revintage"] = len(self.revintage)
        d["revintage_rejected"] = len(self.revintage_rejected)
        d["applicable"] = self.applicable
        return d


def canonical_bytes(value: Any) -> bytes:
    """Stable UTF-8 JSON; non-finite numbers and unsupported types are refused."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("utf-8")


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def source_identity() -> dict[str, str]:
    root = Path(__file__).resolve().parent.parent
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in (
        "scripts/repair_full_panel_vintages.py", "normalization/entity_map.py", "price_close_contract.py")}


def build_manifest(plans: list[FeaturePlan], *, repair_day: date, start: date,
                   coverage_floor: float, exclude: Iterable[str] = (), run_tag: str | None = None,
                   requested_features: list[str] | None = None, stopped: str | None = None) -> dict[str, Any]:
    """Full review artifact, separate from the backwards-compatible count summary.

    Identity covers actions AND all observed planner inputs. Chunked reads are
    not a coherent PostgreSQL snapshot, and cannot authorize a future write.
    """
    requested = sorted(requested_features if requested_features is not None else [p.feature for p in plans])
    names = [p.feature for p in plans]
    ids = [p.feature_id for p in plans if p.feature_id is not None]
    if len(requested) != len(set(requested)) or len(names) != len(set(names)) or len(ids) != len(set(ids)):
        raise ValueError("duplicate feature name or ID")
    payload = {
        "requested_features": requested, "complete": stopped is None and sorted(names) == requested,
        "stopped": stopped, "errors": sum(p.error is not None for p in plans),
        "schema": "full-panel-review-v1", "executor": "disabled",
        "snapshot_semantics": "observed chunked reads; not a coherent database snapshot",
        "repair_day": repair_day.isoformat(), "start": start.isoformat(), "run_tag": run_tag,
        "code_sha256": source_identity(),
        "policy": {"coverage_floor": coverage_floor, "exclude": sorted(set(exclude)),
                   "value_tol_rel": VALUE_TOL_REL, "value_tol_abs": VALUE_TOL_ABS,
                   "outlier_rel": OUTLIER_REL, "outlier_neighbours": OUTLIER_NEIGHBOURS,
                   "raw_close_full_features": sorted(RAW_CLOSE_FULL_FEATURES)},
        "features": [asdict(p) for p in sorted(plans, key=lambda p: p.feature)],
    }
    return {"sha256": digest(payload), "payload": payload}


def validate_review_manifest(manifest: dict[str, Any], *, reviewed_sha256: str,
                             current_manifest: dict[str, Any]) -> None:
    """Offline identity admission only. Never executes or treats a hash as approval.

    The caller supplies the separately reviewed digest and a freshly reconstructed
    artifact. This detects changes in observed inputs, actions, code or policy;
    it does NOT close the check/write race, so apply remains unconditionally off.
    """
    if not isinstance(reviewed_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", reviewed_sha256):
        raise ValueError("a reviewed manifest SHA-256 identity is required")
    required = {"schema", "executor", "snapshot_semantics", "repair_day", "start", "run_tag",
                "code_sha256", "policy", "features", "requested_features", "complete", "stopped", "errors"}
    for candidate in (manifest, current_manifest):
        if set(candidate) != {"sha256", "payload"} or digest(candidate["payload"]) != candidate["sha256"]:
            raise ValueError("manifest digest mismatch")
        payload = candidate["payload"]
        if set(payload) != required or payload["executor"] != "disabled" or payload["snapshot_semantics"] != (
            "observed chunked reads; not a coherent database snapshot"
        ):
            raise ValueError("invalid review-only manifest schema")
        names = [p["feature"] for p in payload["features"]]
        ids = [p["feature_id"] for p in payload["features"] if p["feature_id"] is not None]
        requested = payload["requested_features"]
        if (len(names) != len(set(names)) or len(ids) != len(set(ids))
                or len(requested) != len(set(requested))):
            raise ValueError("duplicate feature identity")
        if (payload["complete"] is not True or payload["stopped"] is not None or payload["errors"] != 0
                or sorted(names) != sorted(requested)):
            raise ValueError("incomplete or failed review manifest")
        for plan in payload["features"]:
            if plan["error"] or (plan["retract"] or plan["revintage"]) and (
                plan["status"] != IN_SCOPE or plan["after_first_on_trading_day"] is not True
            ):
                raise ValueError("unsafe feature plan")
            for action in ("retract", "revintage"):
                keys = [(row["obs_date"], row["vintage_date"]) for row in plan[action]]
                if len(keys) != len(set(keys)):
                    raise ValueError("duplicate action key")
        if candidate["payload"].get("schema") != "full-panel-review-v1":
            raise ValueError("unsupported manifest schema")
    if manifest["sha256"] != reviewed_sha256 or current_manifest["sha256"] != reviewed_sha256:
        raise ValueError("reviewed identity or current inputs/code/policy changed")


def capture_inputs(rows: list[ResolvedRow], raw: dict[str, dict[date, list[RawCandidate]]]) -> dict[str, Any]:
    resolved = [{"id": r.id, "obs_date": r.obs_date.isoformat(), "vintage_date": r.vintage_date.isoformat(),
                 "value": r.value, "source_id": r.source_id, "retracted": r.retracted} for r in rows]
    candidates = [{"field": fld, "obs_date": day.isoformat(), "value": c.value,
                   "pull_date": c.pull_date.isoformat(), "pull_timestamp": c.pull_timestamp,
                   "source_id": c.source_id}
                  for fld, dates in raw.items() for day, cs in dates.items() for c in cs]
    return {"resolved": sorted(resolved, key=canonical_bytes), "raw": sorted(candidates, key=canonical_bytes)}


def candidate_order(c: RawCandidate) -> tuple:
    # Timestamp precision is retained from the DB. Same-time ties use a stable
    # source/value ordering, never unspecified SQL arrival order.
    return c.pull_date, c.pull_timestamp or "", c.source_id, c.value


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
    if not math.isfinite(coverage_floor) or not 0 <= coverage_floor <= 1:
        raise ValueError("coverage_floor must be a finite share")
    if any(not math.isfinite(r.value) for r in rows) or any(
        not math.isfinite(c.value) for dates in raw.values() for cs in dates.values() for c in cs
    ):
        raise ValueError("non-finite price input")
    keys = [(r.obs_date, r.vintage_date) for r in rows]
    if len(keys) != len(set(keys)) or len({r.id for r in rows}) != len(rows):
        raise ValueError("duplicate resolved identity")
    rows = sorted(rows, key=lambda r: (r.obs_date, r.vintage_date, r.id))
    target = scope.target_field
    plan = plan_from_scope(scope, feature_id)
    observed = capture_inputs(rows, raw)
    plan.input_sha256 = digest(observed)
    plan.input_counts = {k: len(v) for k, v in observed.items()}
    del observed
    plan.rows, plan.cells = len(rows), len({r.obs_date for r in rows})
    classify_rows(rows, raw, target)
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

    to_retract = [r for r in rows if r.cls in (OFF_BASIS, OUTLIER)]
    other = "close" if target == "adj_close" else "adj_close"
    for r in to_retract:
        if r.cls == OFF_BASIS:
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
    for d in dark:
        cands = raw.get(target, {}).get(d, [])
        if not cands:
            continue
        if d not in raw.get("close", {}):
            # No as-traded print for the date: a holiday / weekend cell that
            # only ever held a junk adj_close row. Nothing honest to serve.
            plan.revintage_rejected.append({"obs_date": d.isoformat(), "why": "no raw close row: not a trading day"})
            continue
        best = max(cands, key=candidate_order)
        tied = [c for c in cands if (c.pull_date, c.pull_timestamp) == (best.pull_date, best.pull_timestamp)]
        if len({(c.source_id, c.value) for c in tied}) > 1:
            plan.candidate_ties.append({"obs_date": d.isoformat(),
                                        "why": "same-time candidates differ; deterministic ordering is not source authority"})
            plan.error = "conflicting latest raw candidates require review"
        pct = reference.deviation(d, best.value)
        if pct is not None and abs(pct) > OUTLIER_REL:
            # The latest on-basis raw value is itself a junk print (the r2 plan
            # would have re-inserted XLP's 0.6 holiday prints under today's
            # vintage). Try the latest sane candidate instead.
            sane = [c for c in sorted(cands, key=candidate_order, reverse=True)
                    if (p2 := reference.deviation(d, c.value)) is None or abs(p2) <= OUTLIER_REL]
            if not sane:
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
    # the coverage floor.
    gone = set(before) - set(after)
    newly_dark = sorted(d for d in gone if d in raw.get("close", {}))
    plan.cells_newly_dark = len(newly_dark)
    plan.cells_phantom_cleared = len(gone) - len(newly_dark)
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
        plan.after_first_on_trading_day = bool(raw.get("close", {}).get(min(after)))
        if plan.after_first_on_trading_day is False:
            plan.status = UNSAFE_FIRST_CELL
            plan.scope_reason = "first served cell has no raw close trading-day evidence; review required"

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
        into.setdefault(_as_date(od), []).append(RawCandidate(
            float(val), pulled, int(src),
            ts.astimezone(timezone.utc).isoformat(timespec="microseconds") if isinstance(ts, datetime) and ts.tzinfo else str(ts)))
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

def apply_plan(engine: Engine, plan: FeaturePlan, *, run_tag: str, timeout_ms: int) -> dict[str, int]:
    """Fail before opening any connection, including direct callers."""
    raise RuntimeError(f"{APPLY_DISABLED}; feature status={plan.status}")


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
    if apply:
        raise RuntimeError(APPLY_DISABLED)
    if len(features) != len(set(features)):
        raise ValueError("duplicate requested features")
    started = clock()
    check_window(started, allow_db_window)
    repair_day = started.astimezone(timezone.utc).date()
    exclude = frozenset(exclude)
    plans: list[dict[str, Any]] = []
    full_plans: list[FeaturePlan] = []
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
        full_plans.append(plan)
    by_status: dict[str, int] = {}
    for p in plans:
        by_status[p["status"]] = by_status.get(p["status"], 0) + 1
    applicable = [p for p in plans if p["applicable"]]
    return {
        "generated_at": started.astimezone(timezone.utc).isoformat(), "mode": "apply" if apply else "dry_run",
        "run_tag": run_tag, "repair_day": repair_day.isoformat(), "start": start.isoformat(),
        "statement_timeout_ms": timeout_ms, "coverage_floor": coverage_floor, "exclude": sorted(exclude),
        "features": plans, "stopped": stopped,
        "manifest": build_manifest(full_plans, repair_day=repair_day, start=start,
                                   coverage_floor=coverage_floor, exclude=exclude, run_tag=run_tag,
                                   requested_features=features, stopped=stopped),
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
    p.add_argument("--apply", action="store_true", help="disabled pending concurrency-qualified executor")
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
    if args.apply:
        print(APPLY_DISABLED, file=sys.stderr)
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
    payload = json.dumps(report, indent=1, allow_nan=False)
    print(payload)
    if args.out:
        with args.out.open("x", encoding="utf-8") as output:
            output.write(payload)
    return 0 if report["complete"] and report["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
