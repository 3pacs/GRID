"""Module factory run command (3pacs/GRID#1155, factory 1: the thin run).

One fluid ranking run of SPY intraday modules over the staged dev data, recorded
in a factory ledger (``analysis.ledger_steered_exploration.Ledger.create_factory``).

A run file (JSON) declares everything: decision times, horizons, features,
z thresholds, sides, the per-share cost, alpha, permutations and seed. A module
(a trial) is ``feature z >= +k`` or ``feature z <= -k`` at one decision time,
taken long or short, entered at the open one minute after T (a declared bar-price
proxy, not an executable-quote guarantee), judged through T+horizon minus the cost
(``offline_research_proof`` statistic ``net_when_fires``). The run's bar is the
Westfall-Young step-down max-T over every tested module (selection ``max_t``).

Order of a run (the ledger sees the run before the data does):

1. parse and check the run file (dev window only; any holdout path refused);
2. declare the run on the ledger: run file + hash, the staged data manifest's
   hash, and the module count the run file implies;
3. verify the staged files against the manifest (a mismatch abandons the run);
4. build the point-in-time rows, discover, write the frozen manifest and the
   report (``write_once``: never overwritten);
5. record the result (trials, max-T bar, report and frozen hashes).

Features (factory 1, SPY only; edge9 dial definitions, computed at the bar that
closes at the decision time T, i.e. the bar stamped T-1):

* ``F1``: SPY return since the prior session's close;
* ``F10``: position of SPY in today's range so far (0 low, 1 high);
* ``F11``: position of SPY in the prior session's range;
* ``F1_shift30``: change of ``F1`` over the last 30 minutes.

Each is turned into a robust z against the same feature at the same decision
time over the previous ``z_history`` eligible sessions (median / 1.4826 MAD,
capped at +/-4), so a threshold never uses data after its decision.

Variables are locked to declared pieces: every feature has a spec
(:data:`FEATURE_SPECS`: definition, unit, source, live source, point-in-time
rule) and is scale-free, so SPY's price level never enters a feature. The
target unit is declared too (``target_unit``):

* ``vol`` (default): the SPY change divided by the median absolute change of
  the same (decision time, horizon) window over the previous ``vol_history``
  eligible sessions (a point-in-time risk unit; it removes both the price
  level and the volatility regime);
* ``bps``: the change in basis points of the entry price;
* ``dollars``: the change per share.

The per-share cost is converted into the same unit on every row, and every
module's mean net is also reported in dollars per share (what a live slot
earns). The report carries each feature's spec, its coverage and its median
by year (a drift check). Sessions
with fewer than ``full_bars`` SPY minute bars (half days, gaps) are skipped.
Ranking only: nothing here claims a discovery or promotes anything; the
evidence is forward live recording in gex-signals ``panel-factory``.

  python -m analysis.module_factory run --run-file RUN.json --ledger L --anchor A --out DIR [--genesis ID]
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import math
import sqlite3
import statistics
import sys
from collections import deque
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from analysis import ledger_steered_exploration as lse
from analysis.offline_research_proof import (
    MAX_T_SELECTION,
    NET_WHEN_FIRES,
    Protocol,
    digest,
    discover,
    write_once,
)

ORIGIN = "intraday_dev_ranking"
DEV_FIRST = "2022-11-14"  # edge9's holdout ends before this; nothing earlier is read
NY = ZoneInfo("America/New_York")
FEATURE_SPECS = {
    "F1": {
        "definition": "SPY close at the bar closing at T / prior session's last close - 1",
        "unit": "return (scale-free)",
        "source": "staged edge9/bars.db, SPY 1-minute bars (Databento)",
        "live_source": "gex-signals bars_1m_live, SPY (Schwab)",
        "pit": "bar stamped T-1 (closes at T); prior session complete",
    },
    "F10": {
        "definition": "(close - day low so far) / (day high so far - day low so far), at the bar closing at T",
        "unit": "position in range, 0..1 (scale-free)",
        "source": "staged edge9/bars.db, SPY 1-minute bars (Databento)",
        "live_source": "gex-signals bars_1m_live, SPY (Schwab)",
        "pit": "bars stamped up to T-1",
    },
    "F11": {
        "definition": "(close - prior session low) / (prior session high - prior session low), at the bar closing at T",
        "unit": "position in range (scale-free; outside 0..1 beyond the prior range)",
        "source": "staged edge9/bars.db, SPY 1-minute bars (Databento)",
        "live_source": "gex-signals bars_1m_live, SPY (Schwab)",
        "pit": "bar stamped T-1; prior session complete",
    },
    "F1_shift30": {
        "definition": "F1 at the bar closing at T minus F1 at the bar 30 minutes earlier",
        "unit": "return (scale-free)",
        "source": "staged edge9/bars.db, SPY 1-minute bars (Databento)",
        "live_source": "gex-signals bars_1m_live, SPY (Schwab)",
        "pit": "bars stamped T-31 and T-1",
    },
}
FEATURES = tuple(FEATURE_SPECS)
TARGET_UNITS = ("vol", "bps", "dollars")
SIDES = {"long": 1.0, "short": -1.0}
Z_CAP = 4.0
DATA_FILE = "edge9/bars.db"


class RunFileError(ValueError):
    pass


@dataclass(frozen=True)
class RunFile:
    run_id: str
    first_session: str
    last_session: str
    decision_times: tuple[str, ...]
    horizons: tuple[str, ...]  # "<minutes>m" or "close"
    features: tuple[str, ...]
    z_thresholds: tuple[float, ...]
    sides: tuple[str, ...] = ("long", "short")
    cost_per_share: float = 0.04
    alpha: float = 0.05
    perms: int = 10000
    seed: int = 20261113
    min_n: int = 30
    z_history: int = 60
    full_bars: int = 370
    symbol: str = "SPY"
    target_unit: str = "vol"
    vol_history: int = 20

    @classmethod
    def from_dict(cls, value: dict) -> RunFile:
        known = set(cls.__dataclass_fields__)
        unknown = set(value) - known
        if unknown:
            raise RunFileError(f"unknown run file keys: {sorted(unknown)}")
        tuples = ("decision_times", "horizons", "features", "z_thresholds", "sides")
        run = cls(**{k: tuple(v) if k in tuples else v for k, v in value.items()})
        run.validate()
        return run

    def validate(self) -> None:
        if not self.run_id or any(c.isspace() for c in self.run_id):
            raise RunFileError("run_id must be a non-empty token")
        try:
            first, last = date.fromisoformat(self.first_session), date.fromisoformat(self.last_session)
        except ValueError as e:
            raise RunFileError(f"bad session dates: {e}") from None
        if self.first_session < DEV_FIRST:
            raise RunFileError(f"first_session {self.first_session} is before dev ({DEV_FIRST}): holdout refused")
        if last < first:
            raise RunFileError("last_session is before first_session")
        if self.symbol != "SPY":
            raise RunFileError("factory 1 runs SPY only")
        for t in self.decision_times:
            hm = _minutes(t)
            if not 10 * 60 <= hm <= 15 * 60:
                raise RunFileError(f"decision time {t} outside 10:00..15:00 ET")
        for h in self.horizons:
            if h != "close" and not (h.endswith("m") and h[:-1].isdigit() and 2 <= int(h[:-1]) <= 390):
                raise RunFileError(f"horizon {h!r} is '<minutes>m' (at least 2m) or 'close'")
        if set(self.features) - set(FEATURES):
            raise RunFileError(f"unknown features {sorted(set(self.features) - set(FEATURES))}")
        if set(self.sides) - set(SIDES) or not self.sides:
            raise RunFileError("sides are 'long' and/or 'short'")
        if not self.z_thresholds or any(not (0 < k <= Z_CAP) for k in self.z_thresholds):
            raise RunFileError(f"z thresholds lie in (0, {Z_CAP}]")
        for name, values in (("decision_times", self.decision_times), ("horizons", self.horizons),
                             ("features", self.features), ("z_thresholds", self.z_thresholds),
                             ("sides", self.sides)):
            if len(set(values)) != len(values) or not values:
                raise RunFileError(f"{name} must be non-empty and unique")
        if not (math.isfinite(self.cost_per_share) and self.cost_per_share >= 0):
            raise RunFileError("cost_per_share must be >= 0")
        if not 0 < self.alpha <= 0.10 or self.perms < 99 or self.min_n < 30 or self.z_history < 20:
            raise RunFileError("alpha in (0, 0.10], perms >= 99, min_n >= 30, z_history >= 20")
        if self.target_unit not in TARGET_UNITS or self.vol_history < 10:
            raise RunFileError(f"target_unit is one of {TARGET_UNITS}; vol_history >= 10")
        for t in self.decision_times:
            for h in self.horizons:
                if h != "close" and _minutes(t) + int(h[:-1]) > 16 * 60:
                    raise RunFileError(f"{t} + {h} runs past the close")

    def families(self) -> tuple[str, ...]:
        return tuple(f"SPY|change|{t}+{h}" for t in self.decision_times for h in self.horizons)

    def modules(self) -> tuple[str, ...]:
        """Signal names, the same in every family: feature | z rule | side."""
        return tuple(
            f"{f}|z{op}{k:g}|{side}"
            for f in self.features
            for k in self.z_thresholds
            for op in (">=", "<=")
            for side in self.sides
        )

    def trial_count(self) -> int:
        return len(self.families()) * len(self.modules())


def _minutes(hm: str) -> int:
    h, m = hm.split(":")
    return int(h) * 60 + int(m)


def _hm(minutes: int) -> str:
    h, m = divmod(minutes, 60)
    return f"{h:02d}:{m:02d}"


def robust_z(x, hist):
    """edge9 robust z: median / 1.4826 MAD (then pstdev, then 0), capped; None without a full history."""
    if x is None or len(hist) < hist.maxlen:
        return None
    med = statistics.median(hist)
    s = 1.4826 * statistics.median(abs(h - med) for h in hist)
    if s == 0:
        s = statistics.pstdev(hist)
    if s == 0:
        return 0.0
    return max(-Z_CAP, min(Z_CAP, (x - med) / s))


class Session:
    """One SPY session's minute bars {hm: (o, h, low, c, v)} with close-at-or-before lookups."""

    def __init__(self, bars: dict):
        self.hms = sorted(bars)
        self.bars = bars
        self._hi, self._lo = [], []
        hi = lo = None
        for hm in self.hms:
            b = bars[hm]
            hi = b[1] if hi is None else max(hi, b[1])
            lo = b[2] if lo is None else min(lo, b[2])
            self._hi.append(hi)
            self._lo.append(lo)

    def _at(self, hm):
        return bisect.bisect_right(self.hms, hm) - 1

    def close(self, hm):
        i = self._at(hm)
        return None if i < 0 else self.bars[self.hms[i]][3]

    def hilo(self, hm):
        i = self._at(hm)
        return (None, None) if i < 0 else (self._hi[i], self._lo[i])

    @property
    def last_close(self):
        return self.bars[self.hms[-1]][3]


def raw_features(day: Session, prev: Session, bar: str) -> dict:
    """F1, F10, F11, F1_shift30 at the bar stamped ``bar`` (edge9 definitions, SPY only). Pure."""
    out = dict.fromkeys(FEATURES)
    c, pc = day.close(bar), prev.last_close
    if not c or not pc:
        return out
    out["F1"] = c / pc - 1
    c30 = day.close(_hm(_minutes(bar) - 30))
    if c30:
        out["F1_shift30"] = (c - pc) / pc - (c30 - pc) / pc
    hi, lo = day.hilo(bar)
    if hi is not None and hi > lo:
        out["F10"] = (c - lo) / (hi - lo)
    phi, plo = prev.hilo("23:59")
    if phi is not None and phi > plo:
        out["F11"] = (c - plo) / (phi - plo)
    return out


def load_sessions(db_path: Path, run: RunFile) -> dict[str, Session]:
    """{day: Session} for the run's window, read-only; refuses any day before dev."""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        lo = con.execute("select min(day) from bars where sym = ?", (run.symbol,)).fetchone()[0]
        if lo is None or lo < DEV_FIRST:
            raise RunFileError(f"{db_path} holds sessions before dev ({lo}): holdout refused")
        rows = con.execute(
            "select day, hm, o, h, l, c, v from bars where sym = ? and day between ? and ? order by day, hm",
            (run.symbol, run.first_session, run.last_session),
        ).fetchall()
    finally:
        con.close()
    by_day: dict[str, dict] = {}
    for day, hm, o, h, low, c, v in rows:
        by_day.setdefault(day, {})[hm] = (o, h, low, c, v)
    return {d: Session(b) for d, b in by_day.items()}


def build_rows(sessions: dict[str, Session], run: RunFile) -> tuple[dict[str, list[dict]], dict]:
    """Point-in-time rows per family, and a coverage summary.

    The z history of a (feature, decision time) holds only earlier eligible
    sessions' raw values, appended after the session's own z is taken.
    """
    days = sorted(sessions)
    eligible = [d for d in days if len(sessions[d].hms) >= run.full_bars]
    history = {(f, t): deque(maxlen=run.z_history) for f in run.features for t in run.decision_times}
    moves = {fam: deque(maxlen=run.vol_history) for fam in run.families()}  # |dollar change|, earlier sessions
    modules = run.modules()
    families = {f: [] for f in run.families()}
    skipped_no_label = skipped_no_scale = 0
    diag = {(f, t): {"rows": 0, "z": 0, "raw_by_year": {}, "z_by_year": {}} for f in run.features for t in run.decision_times}
    prev = None
    eligible_set = set(eligible)
    for day in days:
        session = sessions[day]
        prior, prev = prev, session  # previous observed session, including half-days
        if prior is None or day not in eligible_set:
            continue
        # Without an authenticated historical session calendar, a short prior
        # may be a half-day or a broken capture. Never substitute an older day.
        expected_prior = {_hm(i) for i in range(9 * 60 + 30, 16 * 60)}
        if set(prior.hms) != expected_prior:
            skipped_no_label += len(run.families())
            continue
        for t in run.decision_times:
            bar = _hm(_minutes(t) - 1)
            required = {bar, _hm(_minutes(t) + 1)}
            if "F1_shift30" in run.features:
                required.add(_hm(_minutes(bar) - 30))
            if not required.issubset(session.bars):
                skipped_no_label += len(run.horizons)
                continue
            raw = raw_features(session, prior, bar)
            z = {f: robust_z(raw[f], history[(f, t)]) for f in run.features}
            for f in run.features:
                d = diag[(f, t)]
                d["rows"] += 1
                if raw[f] is not None:
                    history[(f, t)].append(raw[f])
                    d["raw_by_year"].setdefault(day[:4], []).append(raw[f])
                if z[f] is not None:
                    d["z"] += 1
                    d["z_by_year"].setdefault(day[:4], []).append(z[f])
            entry_bar = _hm(_minutes(t) + 1)
            entry = session.bars[entry_bar][0]
            decided = datetime.fromisoformat(f"{day}T{t}").replace(tzinfo=NY)
            signals = {}
            for name in modules:
                feature, rule, side = name.split("|")
                k = float(rule[3:])
                zf = z[feature]
                fires = zf is not None and (zf >= k if rule[1:3] == ">=" else zf <= -k)
                signals[name] = {"value": SIDES[side] if fires else None, "known_at": decided.isoformat()}
            for h in run.horizons:
                if h == "close":
                    exit_bar, ends = "15:59", decided.replace(hour=16, minute=0)
                else:
                    exit_bar = _hm(_minutes(bar) + int(h[:-1]))
                    ends = decided + timedelta(minutes=int(h[:-1]))
                exit_px = session.bars[exit_bar][3] if exit_bar in session.bars else None
                if not entry or not exit_px:
                    skipped_no_label += 1
                    continue
                family = f"SPY|change|{t}+{h}"
                change = float(exit_px - entry)
                past = moves[family]
                if run.target_unit == "vol":
                    scale = statistics.median(past) if len(past) == past.maxlen else None
                elif run.target_unit == "bps":
                    scale = entry / 1e4
                else:
                    scale = 1.0
                past.append(abs(change))  # after use: a session never scales itself
                if not scale:
                    skipped_no_scale += 1
                    continue
                families[family].append({
                    "origin": ORIGIN,
                    "decision_at": decided.isoformat(),
                    "entry_at": (decided + timedelta(minutes=1)).isoformat(),
                    "entry_price_basis": "next_minute_open_proxy",
                    "label_end": ends.isoformat(),
                    "target_known_at": ends.isoformat(),
                    "horizon": f"{t}+{h}",
                    "label": "change",
                    "target": change / scale,
                    "cost": run.cost_per_share / scale,
                    "target_dollars": change,
                    "features": signals,
                })
    coverage = {
        "sessions_read": len(days),
        "sessions_eligible": len(eligible),
        "sessions_skipped_short": len(days) - len(eligible),
        "first_eligible": eligible[0] if eligible else None,
        "last_eligible": eligible[-1] if eligible else None,
        "rows_per_family": {f: len(r) for f, r in families.items()},
        "labels_missing": skipped_no_label,
        "labels_unscaled": skipped_no_scale,
        "target_unit": run.target_unit,
        "entry_rule": "open at T+1 minute; quote fills/slippage not established",
        "features": {
            f"{f}@{t}": {
                "coverage_z": round(d["z"] / d["rows"], 4) if d["rows"] else None,
                "median_raw_by_year": {y: round(statistics.median(v), 6) for y, v in sorted(d["raw_by_year"].items())},
                "median_z_by_year": {y: round(statistics.median(v), 3) for y, v in sorted(d["z_by_year"].items())},
                "z_drift": round(max(m := [statistics.median(v) for v in d["z_by_year"].values()]) - min(m), 3)
                if d["z_by_year"] else None,
            }
            for (f, t), d in diag.items()
        },
    }
    return families, coverage


def protocol_for(run: RunFile) -> Protocol:
    first = datetime.fromisoformat(f"{run.first_session}T00:00").replace(tzinfo=NY)
    split = datetime.fromisoformat(f"{run.last_session}T00:00").replace(tzinfo=NY) + timedelta(days=1)
    return Protocol(
        run_id=run.run_id,
        features=run.modules(),
        families=run.families(),
        start=first.isoformat(),
        split=split.isoformat(),
        end=(split + timedelta(days=1)).isoformat(),
        min_n=run.min_n,
        origin=ORIGIN,
        sampling="horizon_spaced",
        perms=run.perms,
        seed=run.seed,
        statistic=NET_WHEN_FIRES,
        selection=MAX_T_SELECTION,
        selection_alpha=run.alpha,
        cost=0.0,  # every row carries its own cost in the run's target unit
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def check_data(data_dir: Path) -> tuple[dict, str]:
    """(manifest, manifest sha256) of a staged dev data folder; refuses a holdout path."""
    if "holdout" in str(data_dir).lower():
        raise RunFileError(f"holdout path refused: {data_dir}")
    manifest_path = data_dir / "MANIFEST.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("dev_first", "") < DEV_FIRST:
        raise RunFileError("the staged manifest starts before dev: holdout refused")
    return manifest, sha256_file(manifest_path)


def verify_data(data_dir: Path, manifest: dict, needed=(DATA_FILE,)) -> None:
    files = {f["path"]: f for f in manifest["files"]}
    for name in needed:
        entry = files.get(name)
        if entry is None:
            raise RunFileError(f"{name} is not in the staged manifest")
        if sha256_file(data_dir / name) != entry["sha256"]:
            raise RunFileError(f"{name} does not match its manifest sha256")


def dollars_net(families: dict, trials: list[dict], cost: float) -> dict:
    """trial_id -> mean net in dollars per share over its fires (side * change - cost)."""
    out = {}
    for t in trials:
        fired = [(row["features"][t["feature"]]["value"], row["target_dollars"])
                 for row in families[t["family"]] if row["features"][t["feature"]]["value"] is not None]
        out[t["trial_id"]] = (sum(s * d for s, d in fired) / len(fired) - cost) if fired else None
    return out


def report(run: RunFile, frozen: dict, coverage: dict, families: dict, top: int = 25) -> dict:
    payload = frozen["payload"]
    trials = payload["ledger"]
    dollars = dollars_net(families, trials, run.cost_per_share)
    for t in trials:
        t["net_dollars"] = dollars[t["trial_id"]]
    tested = sorted((t for t in trials if t["status"] == "tested"), key=lambda t: -t["t"])
    return {
        "run_id": run.run_id,
        "state": "INTRADAY_DEV_RANKING_ONLY",
        "promotion_allowed": False,
        "frozen_sha256": frozen["sha256"],
        "method": payload["method"],
        "target_unit": run.target_unit,
        "feature_specs": {f: FEATURE_SPECS[f] for f in run.features},
        "coverage": coverage,
        "trial_count": payload["trial_count"],
        "tested": len(tested),
        "untestable": payload["untestable_count"],
        "max_t": payload["max_t"],
        "selected": [t["trial_id"] for t in tested if t["selected"]],
        "ranking": [
            {k: t[k] for k in ("trial_id", "family", "feature", "n", "r", "net_dollars", "t", "p", "adjusted_p", "selected")}
            for t in tested[:top]
        ],
        "note": "dev ranking only; a selected module is a candidate for a panel-factory live slot, not a finding",
    }


def report_markdown(rep: dict) -> str:
    mt = rep["max_t"]
    lines = [
        f"# Factory run {rep['run_id']}",
        "",
        f"State: {rep['state']} (ranking only; forward live recording is the test).",
        "",
        f"- Modules declared {rep['trial_count']}, tested {rep['tested']}, untestable {rep['untestable']}.",
        (
            f"- Sessions eligible {rep['coverage']['sessions_eligible']} "
            f"({rep['coverage']['first_eligible']}..{rep['coverage']['last_eligible']}), "
            f"short sessions skipped {rep['coverage']['sessions_skipped_short']}."
        ),
        (
            f"- Bar: best-of-shuffled critical t {mt['critical_t']:.3f} at the run alpha "
            f"(block {mt['block']}, {mt['sessions']} sessions)."
            if mt.get("critical_t") is not None
            else "- Bar: none (nothing tested)."
        ),
        f"- Selected {len(rep['selected'])} (adjusted p <= alpha and mean net > 0).",
        "",
        f"Target unit: {rep['target_unit']}; mean net is in that unit, and also in dollars per share.",
        "",
        f"| # | family | module | fires | mean net ({rep['target_unit']}) | $/share | t | p | adjusted p | selected |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for i, r in enumerate(rep["ranking"], 1):
        lines.append(
            f"| {i} | {r['family']} | {r['feature']} | {r['n']} | {r['r']:+.4f} | {r['net_dollars']:+.4f} | "
            f"{r['t']:.2f} | {r['p']:.4f} | {r['adjusted_p']:.4f} | {'yes' if r['selected'] else ''} |"
        )
    lines += ["", "| feature @ time | coverage | z drift (max - min yearly median z) |", "|---|---|---|"]
    for name, f in rep["coverage"]["features"].items():
        lines.append(f"| {name} | {f['coverage_z']} | {f['z_drift']} |")
    lines += ["", f"Method: {rep['method']}", ""]
    return "\n".join(lines)


def open_ledger(ledger_path: Path, anchor_path: Path, genesis: str | None) -> lse.Ledger:
    if genesis:
        return lse.Ledger.create_factory(ledger_path, ledger_id=genesis, anchor=anchor_path)
    ledger = lse.Ledger(ledger_path, anchor=anchor_path)
    if not ledger.is_factory:
        raise RunFileError("that ledger is not a factory ledger")
    return ledger


def run(run_file: Path, data_dir: Path, ledger: lse.Ledger, out: Path) -> dict:
    """One factory run, declared on ``ledger`` before any data is read. Returns the report."""
    body = json.loads(Path(run_file).read_text())
    spec = RunFile.from_dict(body)
    manifest, manifest_sha = check_data(Path(data_dir))
    out = Path(out)
    out.mkdir(parents=True, exist_ok=False)
    lse.record_factory_declared(
        ledger, run_id=spec.run_id, run_file=body, data_manifest_sha256=manifest_sha,
        trial_count=spec.trial_count(),
    )
    try:
        verify_data(Path(data_dir), manifest)
        sessions = load_sessions(Path(data_dir) / DATA_FILE, spec)
        families, coverage = build_rows(sessions, spec)
        frozen = discover(protocol_for(spec), families)
        write_once(out / "discovery-frozen.json", frozen)
        rep = report(spec, frozen, coverage, families)
        write_once(out / "report.json", rep)
        with (out / "report.md").open("x", encoding="utf-8") as stream:
            stream.write(report_markdown(rep))
    except Exception as e:  # the declared run is closed, never left open
        lse.record_factory_abandoned(ledger, reason=f"{type(e).__name__}: {e}")
        raise
    trials = [
        {k: t.get(k) for k in ("trial_id", "family", "feature", "status", "n", "r", "net_dollars", "t", "p",
                               "adjusted_p", "selected")}
        for t in frozen["payload"]["ledger"]
    ]
    lse.record_factory_result(
        ledger, trials=trials, max_t=frozen["payload"]["max_t"],
        report_sha256=digest(rep), frozen_sha256=frozen["sha256"],
    )
    return rep


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--run-file", required=True, type=Path)
    r.add_argument("--data-dir", required=True, type=Path)
    r.add_argument("--ledger", required=True, type=Path)
    r.add_argument("--anchor", required=True, type=Path)
    r.add_argument("--out", required=True, type=Path)
    r.add_argument("--genesis", metavar="LEDGER_ID", help="create the factory ledger first (once)")
    s = sub.add_parser("summary")
    s.add_argument("--ledger", required=True, type=Path)
    s.add_argument("--anchor", required=True, type=Path)
    a = ap.parse_args(argv)
    if a.cmd == "summary":
        print(json.dumps(lse.summarize(lse.Ledger(a.ledger, anchor=a.anchor)), indent=1))
        return 0
    try:
        ledger = open_ledger(a.ledger, a.anchor, a.genesis)
        rep = run(a.run_file, a.data_dir, ledger, a.out)
    except RunFileError as e:
        print(json.dumps({"ok": False, "refused": str(e)}))
        return 2
    print(json.dumps({"ok": True, "run_id": rep["run_id"], "trials": rep["trial_count"], "tested": rep["tested"],
                      "selected": len(rep["selected"]), "critical_t": rep["max_t"]["critical_t"], "out": str(a.out)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
