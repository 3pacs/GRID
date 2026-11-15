"""Cross-section module factory: rank the stocks of an index on a per-stock signal, long the top, short the bottom.

The SPY factory (``analysis/module_factory.py``) asks "when X, does SPY move over H". This one asks "do the stocks a
signal ranks highest out-earn the ones it ranks lowest over H", across about 500 names a night, pooled or inside each
sector. Breadth gives it statistical power one index cannot have (docs/reference/CROSS_SECTION_FACTORY.md).

It reuses the factory's ledger records and report shape, so a run is declared before its data is read and the desk
evidence adapters can read its report. It brings its own null. The SPY factory shuffles the session order of the
target, which tests whether a signal's *timing* matters; a long-short spread trades every night, so that shuffle
leaves its mean unchanged. Here each draw flips the sign of every night's spread, the same flip for every module,
and the run's best flipped t is the bar (Westfall-Young step-down, one-sided).

Discovery only. It reads development days (``DEV_FIRST`` .. ``DEV_LAST``) and drops every later bar on load; the
years after stay unread for a confirmation look. A selected module is a candidate, never a finding.

    python -m analysis.cross_section_factory run --run-file RUN.json --data-dir DIR --state-db state.db \\
        --ledger L.jsonl --anchor A.jsonl --out OUT [--genesis LEDGER_ID]

``DIR`` holds ``prices/<TICKER>.csv`` (Tiingo daily columns), ``members.csv`` (date, tickers), ``sectors.csv``
(Symbol, GICS Sector) and ``renames.json``. ``state.db`` is the sector state record (desk-sector-state/1).
"""
from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import json
import math
import sqlite3
import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from analysis import ledger_steered_exploration as lse
from analysis.offline_research_proof import digest, write_once

STATE = "CROSS_SECTION_DEV_RANKING_ONLY"
DEV_FIRST = "2010-01-04"
DEV_LAST = "2022-12-29"       # last decision day discovery may use
DEV_LAST_BAR = "2022-12-30"   # its outcome session; every later bar is dropped on load
BAR_TOL = 0.005
SIGNAL_CLIP = 0.10
MIN_PRICE = 5.0
MIN_DOLLAR_VOLUME = 10e6
DOLLAR_VOLUME_WINDOW = 21
SIDES = {"overnight": 4, "intraday": 4, "close_1d": 4}   # sides traded per night per $1 long and $1 short
SIGNAL_KINDS = ("mean_overnight", "mean_intraday", "momentum", "reversal", "high_52w")
RANKINGS = ("pooled", "within_sector")
GROUPS = {
    "cyclical": ("Energy", "Materials", "Industrials", "Financials", "Consumer Discretionary", "Real Estate"),
    "growth": ("Information Technology", "Communication Services", "Health Care"),
    "defensive": ("Consumer Staples", "Utilities"),
}
GROUP_OF = {s: g for g, names in GROUPS.items() for s in names}


class RunFileError(ValueError):
    """A run file the factory refuses."""


@dataclass(frozen=True)
class RunFile:
    run_id: str
    first_session: str
    last_session: str
    signals: tuple
    rankings: tuple
    fractions: tuple
    cost_bp_per_side: float = 1.0
    alpha: float = 0.05
    perms: int = 10_000
    seed: int = 20261107
    min_ranked: int = 400
    min_session_bars: int = 300
    cards: int = 5
    _keys = ("run_id", "first_session", "last_session", "signals", "rankings", "fractions", "cost_bp_per_side", "alpha",
             "perms", "seed", "min_ranked", "min_session_bars", "cards")

    @classmethod
    def from_dict(cls, value: dict) -> RunFile:
        if not isinstance(value, dict) or set(value) - set(cls._keys):
            raise RunFileError("unknown run-file keys")
        try:
            run = cls(**{**value, "signals": tuple(json.dumps(s, sort_keys=True) for s in value["signals"]),
                         "rankings": tuple(value["rankings"]), "fractions": tuple(value["fractions"])})
        except (KeyError, TypeError) as exc:
            raise RunFileError(f"malformed run file: {exc}") from exc
        run.validate()
        return run

    def signal_specs(self) -> list[dict]:
        return [json.loads(s) for s in self.signals]

    def validate(self) -> None:
        if not (isinstance(self.run_id, str) and self.run_id.strip()):
            raise RunFileError("run_id required")
        if not (DEV_FIRST <= self.first_session <= self.last_session <= DEV_LAST):
            raise RunFileError(f"window must lie inside the development days {DEV_FIRST} .. {DEV_LAST}")
        names = set()
        for s in self.signal_specs():
            if set(s) - {"name", "kind", "lookback", "skip", "sign", "horizons"} or s.get("kind") not in SIGNAL_KINDS:
                raise RunFileError(f"unknown signal: {s}")
            if not isinstance(s.get("name"), str) or s["name"] in names or "|" in s["name"]:
                raise RunFileError("each signal needs its own name, without '|'")
            names.add(s["name"])
            if type(s.get("lookback")) is not int or not 2 <= s["lookback"] <= 504:
                raise RunFileError("lookback must be 2 .. 504 sessions")
            if s.get("sign", 1) not in (1, -1) or type(s.get("skip", 0)) is not int or not 0 <= s.get("skip", 0) <= 63:
                raise RunFileError("sign is 1 or -1, skip is 0 .. 63")
            if not s.get("horizons") or set(s["horizons"]) - set(SIDES):
                raise RunFileError(f"horizons must be among {sorted(SIDES)}")
        if not names or not self.rankings or set(self.rankings) - set(RANKINGS):
            raise RunFileError(f"rankings must be among {RANKINGS}")
        if not self.fractions or any(not (isinstance(q, float) and 0.01 <= q <= 0.5) for q in self.fractions):
            raise RunFileError("fractions must be between 0.01 and 0.5")
        if not (0 < self.alpha <= 0.10 and 1_000 <= self.perms <= 100_000 and 0 <= self.cost_bp_per_side <= 50):
            raise RunFileError("alpha in (0, 0.10], perms 1,000 .. 100,000, cost 0 .. 50 bp")
        if self.min_ranked < 20 or self.min_session_bars < 20 or not 0 <= self.cards <= 50:
            raise RunFileError("min_ranked and min_session_bars at least 20, cards 0 .. 50")

    def modules(self) -> list[dict]:
        out = []
        for s in self.signal_specs():
            for h in s["horizons"]:
                for rank in self.rankings:
                    for q in self.fractions:
                        feature = f"{s['name']}|{rank}|q{round(q * 100):02d}"
                        out.append({"trial_id": f"{feature}|{h}", "family": f"XS|{h}", "feature": feature, "signal": s["name"],
                                    "ranking": rank, "fraction": q, "horizon": h})
        return out

    def trial_count(self) -> int:
        return len(self.modules())


# ── Data ──────────────────────────────────────────────────────────────────

def manifest(data_dir: Path) -> tuple[dict, str]:
    """Name, size and sha256 of every input file. Hashing is the only read before the run is declared."""
    files = {}
    for p in sorted([*Path(data_dir).glob("prices/*.csv"), *(Path(data_dir) / n for n in ("members.csv", "sectors.csv", "renames.json"))]):
        files[str(p.relative_to(data_dir))] = {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
    if len(files) < 4:
        raise RunFileError("data directory needs prices/*.csv, members.csv, sectors.csv and renames.json")
    body = {"schema": "cross-section-data-manifest/1", "files": files}
    return body, digest(body)


@dataclass
class Panel:
    sessions: list
    cols: list
    on: np.ndarray            # overnight return ending at each session's open
    intra: np.ndarray         # open to close of each session
    cc: np.ndarray            # close to close ending at each session
    adj: np.ndarray           # adjusted close
    base: np.ndarray          # member, valid bar, price and liquidity floors (signal presence is added per signal)
    member: np.ndarray
    sector: np.ndarray        # sector index per column, -1 = none
    sectors: list
    assignment: dict = field(default_factory=dict)


def load_panel(data_dir: Path, run: RunFile) -> Panel:
    cols_data: dict[str, dict] = {k: {} for k in ("open", "close", "high", "low", "adjOpen", "adjClose", "volume")}
    for p in sorted((Path(data_dir) / "prices").glob("*.csv")):
        df = pd.read_csv(p, usecols=["date", *cols_data])
        if df.empty:
            continue
        df["date"] = df["date"].str[:10]
        df = df[df["date"] <= DEV_LAST_BAR].drop_duplicates("date").set_index("date")   # nothing later is ever held
        for k, store in cols_data.items():
            store[p.stem] = df[k]
    f = {k: pd.DataFrame(v).sort_index() for k, v in cols_data.items()}
    counts = f["close"].notna().sum(axis=1)
    sessions = [d for d, n in counts.items() if n >= run.min_session_bars]
    if not sessions or sessions[-1] > DEV_LAST_BAR:
        raise RunFileError("no development sessions in the data")
    f = {k: v.reindex(sessions) for k, v in f.items()}
    lo, hi = f["low"] * (1 - BAR_TOL), f["high"] * (1 + BAR_TOL)
    ok = ((f["open"] > 0) & (f["close"] > 0) & (f["high"] > 0) & (f["low"] > 0) & (f["adjOpen"] > 0) & (f["adjClose"] > 0)
          & (f["open"] >= lo) & (f["open"] <= hi) & (f["close"] >= lo) & (f["close"] <= hi))
    prev_ok = ok.shift(1, fill_value=False)
    on = (f["adjOpen"] / f["adjClose"].shift(1) - 1).where(ok & prev_ok)
    intra = (f["close"] / f["open"] - 1).where(ok)
    cc = (f["adjClose"] / f["adjClose"].shift(1) - 1).where(ok & prev_ok)
    cols = list(on.columns)

    with open(Path(data_dir) / "members.csv", newline="") as fh:
        rows = sorted((r["date"], frozenset(t.strip() for t in r["tickers"].split(",") if t.strip())) for r in csv.DictReader(fh))
    mdates = [d for d, _ in rows]
    renames = dict(json.loads((Path(data_dir) / "renames.json").read_text())["renames"])
    at = {c: j for j, c in enumerate(cols)}
    member = np.zeros((len(sessions), len(cols)), dtype=bool)
    last_i, last_cols = None, []
    for r, d in enumerate(sessions):
        i = bisect.bisect_right(mdates, d) - 1
        if i != last_i:
            last_i = i
            last_cols = [at[x] for x in (renames.get(t, t) for t in (rows[i][1] if i >= 0 else ())) if x in at]
        member[r, last_cols] = True

    dollar = (f["close"] * f["volume"]).rolling(DOLLAR_VOLUME_WINDOW, min_periods=10).median().shift(1)
    base = member & ok.values & (f["close"].shift(1) >= MIN_PRICE).values & (dollar >= MIN_DOLLAR_VOLUME).values

    with open(Path(data_dir) / "sectors.csv", newline="") as fh:
        labels = {r["Symbol"].strip(): r["GICS Sector"].strip() for r in csv.DictReader(fh)}
    sectors = sorted(set(labels.values()))
    if set(sectors) - set(GROUP_OF):
        raise RunFileError(f"sectors without a group: {sorted(set(sectors) - set(GROUP_OF))}")
    code, how = assign_sectors(cc.values, member, cols, labels, sectors)
    return Panel(sessions, cols, on.values, intra.values, cc.values, f["adjClose"].values, base, member, code, sectors, how)


def assign_sectors(cc: np.ndarray, member: np.ndarray, cols: list, labels: dict, sectors: list) -> tuple[np.ndarray, dict]:
    """Sector index per column. A member with no label takes the labelled sector whose daily return (less the
    universe's) its own follows most closely; the same method, run on labelled names with each left out of its
    own sector, gives the accuracy that is reported."""
    with warnings.catch_warnings():          # the first session has no return yet: an all-NaN row is expected
        warnings.simplefilter("ignore", RuntimeWarning)
        univ = np.nanmean(np.where(member, cc, np.nan), axis=1)
    resid = cc - univ[:, None]
    code = np.array([sectors.index(labels[c]) if c in labels else -1 for c in cols])
    inside = member & ~np.isnan(resid)
    x0 = np.where(inside, resid, 0.0)
    ssum = np.stack([x0[:, code == s].sum(axis=1) for s in range(len(sectors))])
    scnt = np.stack([inside[:, code == s].sum(axis=1) for s in range(len(sectors))]).astype(float)

    def best(j: int, own: int) -> int:
        have = ~np.isnan(resid[:, j])
        if have.sum() < 120:
            return -1
        y = resid[have, j]
        top, top_c = -1, -2.0
        for s in range(len(sectors)):
            num, den = ssum[s, have], scnt[s, have]
            if s == own:
                mine = inside[have, j]
                num, den = num - np.where(mine, y, 0.0), den - mine
            idx = np.where(den > 0, num / np.where(den > 0, den, 1), np.nan)
            m = ~np.isnan(idx)
            if m.sum() < 120 or idx[m].std() == 0 or y[m].std() == 0:
                continue
            c = float(np.corrcoef(y[m], idx[m])[0, 1])
            if c > top_c:
                top, top_c = s, c
        return top

    hit = tried = 0
    for j in np.flatnonzero(code >= 0):
        if member[:, j].any():
            got = best(j, int(code[j]))
            if got >= 0:
                tried += 1
                hit += got == code[j]
    out = code.copy()
    placed = unplaced = 0
    for j in np.flatnonzero(code < 0):
        if member[:, j].any():
            out[j] = best(j, -1)
            placed += out[j] >= 0
            unplaced += out[j] < 0
    return out, {"labelled_checked": int(tried), "leave_one_out_accuracy": (hit / tried) if tried else None,
                 "placed_by_correlation": int(placed), "unplaced": int(unplaced)}


# ── Signals (row d = what is known on decision day d before its close) ────

def signal_matrix(panel: Panel, spec: dict) -> np.ndarray:
    L, skip = spec["lookback"], spec.get("skip", 0)
    need = math.ceil(0.8 * L)
    adj = pd.DataFrame(panel.adj)
    kind = spec["kind"]
    if kind == "mean_overnight":      # sessions d-L+1 .. d: the last overnight ended at the open of d
        s = pd.DataFrame(panel.on).clip(-SIGNAL_CLIP, SIGNAL_CLIP).rolling(L, min_periods=need).mean()
    elif kind == "mean_intraday":     # sessions d-L .. d-1: day d's own session is not finished
        s = pd.DataFrame(panel.intra).clip(-SIGNAL_CLIP, SIGNAL_CLIP).rolling(L, min_periods=need).mean().shift(1)
    elif kind == "momentum":          # return over L sessions ending skip sessions before the close of d-1
        s = adj.shift(1 + skip) / adj.shift(1 + skip + L) - 1
    elif kind == "reversal":          # the opposite of the last L sessions' return through the close of d-1
        s = -(adj.shift(1) / adj.shift(1 + L) - 1)
    else:                             # high_52w: close of d-1 against the highest close of the L sessions through d-1
        s = adj.shift(1) / adj.shift(1).rolling(L, min_periods=need).max()
    return (s * spec.get("sign", 1)).values


def outcome(panel: Panel, horizon: str) -> np.ndarray:
    """Row d = each stock's return over the horizon that starts at the close of d."""
    src = {"overnight": panel.on, "intraday": panel.intra, "close_1d": panel.cc}[horizon]
    out = np.full_like(src, np.nan)
    out[:-1] = src[1:]
    return out


def _spread(sig: np.ndarray, tick: np.ndarray, idx: np.ndarray, k: int, y: np.ndarray) -> float:
    order = idx[np.lexsort((tick[idx], sig[idx]))]
    lo, hi = y[order[:k]], y[order[-k:]]
    lo, hi = lo[~np.isnan(lo)], hi[~np.isnan(hi)]
    return float(hi.mean() - lo.mean()) if len(lo) and len(hi) else float("nan")


def evaluate(panel: Panel, run: RunFile) -> dict:
    """trial_id -> {"gross": per-session spread, "per_sector": sessions x sectors or None, "names": per session}."""
    tick = np.array(panel.cols, dtype=object)
    rows = [r for r, d in enumerate(panel.sessions[:-1]) if run.first_session <= d <= run.last_session]
    n_d, n_s = len(panel.sessions), len(panel.sectors)
    specs = {s["name"]: s for s in run.signal_specs()}
    out = {}
    for name, spec in specs.items():
        sig = signal_matrix(panel, spec)
        eligible = panel.base & ~np.isnan(sig)
        mods = [m for m in run.modules() if m["signal"] == name]
        ys = {h: outcome(panel, h) for h in spec["horizons"]}
        for m in mods:
            out[m["trial_id"]] = {"gross": np.full(n_d, np.nan), "names": np.zeros(n_d),
                                  "per_sector": np.full((n_d, n_s), np.nan) if m["ranking"] == "within_sector" else None,
                                  "k_sector": np.zeros((n_d, n_s)) if m["ranking"] == "within_sector" else None}
        for r in rows:
            idx = np.flatnonzero(eligible[r])
            if len(idx) < run.min_ranked:
                continue
            by_sector = [idx[panel.sector[idx] == s] for s in range(n_s)]
            for m in mods:
                y, q, res = ys[m["horizon"]][r], m["fraction"], out[m["trial_id"]]
                if m["ranking"] == "pooled":
                    k = math.floor(q * len(idx))
                    res["gross"][r], res["names"][r] = _spread(sig[r], tick, idx, k, y), k
                    continue
                floor = max(8, math.ceil(1 / q))
                num = den = 0.0
                for s, sidx in enumerate(by_sector):
                    if len(sidx) < floor:
                        continue
                    k = max(1, math.floor(q * len(sidx)))
                    v = _spread(sig[r], tick, sidx, k, y)
                    res["per_sector"][r, s], res["k_sector"][r, s] = v, k
                    if not math.isnan(v):
                        num, den = num + k * v, den + k
                res["gross"][r], res["names"][r] = (num / den if den else float("nan")), den
    return out


# ── The bar ───────────────────────────────────────────────────────────────

def _t(sums: np.ndarray, sumsq: np.ndarray, n: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        mean = sums / n
        var = (sumsq - n * mean ** 2) / (n - 1)
        t = mean / np.sqrt(var / n)
    return np.where((n >= 30) & (var > 1e-18), t, -np.inf)


def sign_flip_max_t(x: np.ndarray, perms: int, seed: int, alpha: float) -> dict:
    """x: sessions x modules net spreads (NaN = not traded). One-sided step-down max-T under joint sign flips."""
    have = ~np.isnan(x)
    x0 = np.where(have, x, 0.0)
    n = have.sum(axis=0).astype(float)
    sumsq = (x0 ** 2).sum(axis=0)
    observed = _t(x0.sum(axis=0), sumsq, n)
    rng = np.random.default_rng(seed)
    null = np.empty((perms, x.shape[1]), dtype=np.float32)
    for s in range(0, perms, 500):
        m = min(500, perms - s)
        null[s:s + m] = _t(rng.choice((-1.0, 1.0), size=(m, x.shape[0])) @ x0, sumsq, n)
    tested = np.isfinite(observed)
    marginal = np.ones(x.shape[1])
    adjusted = np.ones(x.shape[1])
    if tested.any():
        cols = np.flatnonzero(tested)
        obs, nul = observed[cols], null[:, cols]
        marginal[cols] = (1 + (nul >= obs[None, :] - 1e-6).sum(axis=0)) / (perms + 1)
        order = np.argsort(-obs, kind="stable")
        tail_max = np.maximum.accumulate(nul[:, order[::-1]], axis=1)[:, ::-1]
        step = np.maximum.accumulate((1 + (tail_max >= obs[order][None, :] - 1e-6).sum(axis=0)) / (perms + 1))
        adjusted[cols[order]] = np.minimum(1.0, step)
        critical = float(np.quantile(nul.max(axis=1), 1 - alpha))
    else:
        critical = None
    return {"t": observed, "n": n, "p": marginal, "adjusted_p": adjusted, "tested": tested, "critical_t": critical}


# ── The conditional card ──────────────────────────────────────────────────

def load_state(path: Path) -> dict:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return {d: json.loads(body) for d, body in con.execute("SELECT data_through, record_json FROM desk_state")}
    finally:
        con.close()


def _cut(v: np.ndarray, cost: float) -> dict:
    v = v[~np.isnan(v)]
    if len(v) < 30:
        return {"nights": len(v)}
    mean, sd = float(v.mean()), float(v.std(ddof=1))
    return {"nights": len(v), "gross_bp": round(mean * 1e4, 3), "net_bp": round((mean - cost) * 1e4, 3),
            "sd_bp": round(sd * 1e4, 2), "t_gross": round(mean / (sd / math.sqrt(len(v))), 3) if sd else None}


def card(panel: Panel, module: dict, res: dict, state: dict, cost: float) -> dict:
    """Where the module paid: by year and by the states recorded the evening before each decision day."""
    gross = res["gross"]
    label = {"market_phase": [None] * len(gross), "fed_balance_sheet": [None] * len(gross), "vix_third": [None] * len(gross),
             "spike_state": [None] * len(gross)}
    recs = [None] * len(gross)
    for r in range(1, len(gross)):
        rec = state.get(panel.sessions[r - 1])
        if rec is None or math.isnan(gross[r]):
            continue
        recs[r] = rec
        label["market_phase"][r] = rec["market"]["phase"]
        label["fed_balance_sheet"][r] = rec["fed_balance_sheet"].get("phase")
        label["vix_third"][r] = rec["vix"].get("third")
        label["spike_state"][r] = rec["vix"].get("spike_state")
    out: dict = {"overall": _cut(gross, cost), "state_rows_matched": sum(r is not None for r in recs)}
    years = np.array([d[:4] for d in panel.sessions])
    out["year"] = {y: _cut(gross[years == y], cost) for y in sorted(set(years[~np.isnan(gross)]))}
    for name, values in label.items():
        arr = np.array(values, dtype=object)
        out[name] = {str(v): _cut(gross[arr == v], cost) for v in sorted({x for x in values if x is not None})}
    if res["per_sector"] is not None:
        ps, ks = res["per_sector"], res["k_sector"]

        def pooled(pick: np.ndarray) -> np.ndarray:
            use = pick & ~np.isnan(ps) & (ks > 0)
            w = np.where(use, ks, 0.0)
            tot = w.sum(axis=1)
            return np.where(tot > 0, (np.where(use, ps, 0.0) * w).sum(axis=1) / np.where(tot > 0, tot, 1), np.nan)

        n_s = len(panel.sectors)
        out["sector"] = {s: _cut(ps[:, j], cost) for j, s in enumerate(panel.sectors)}
        out["group"] = {g: _cut(pooled(np.tile([GROUP_OF[s] == g for s in panel.sectors], (len(gross), 1))), cost) for g in GROUPS}
        phase = np.empty((len(gross), n_s), dtype=object)
        leading = np.zeros((len(gross), n_s), dtype=object)
        leading[:] = None
        for r, rec in enumerate(recs):
            if rec is None:
                continue
            for j, s in enumerate(panel.sectors):
                phase[r, j] = rec["sectors"].get(s, {}).get("phase")
                leading[r, j] = rec["sectors"].get(s, {}).get("leading")
        out["sector_phase"] = {str(p): _cut(pooled(phase == p), cost) for p in sorted({x for x in phase.ravel() if x})}
        out["sector_vs_market"] = {"leading": _cut(pooled(leading == True), cost),
                                   "lagging": _cut(pooled(leading == False), cost)}
    return out


# ── A run ─────────────────────────────────────────────────────────────────

def open_ledger(ledger_path: Path, anchor_path: Path, genesis: str | None) -> lse.Ledger:
    if genesis:
        return lse.Ledger.create_factory(ledger_path, ledger_id=genesis, anchor=anchor_path)
    ledger = lse.Ledger(ledger_path, anchor=anchor_path)
    if not ledger.is_factory:
        raise RunFileError("that ledger is not a factory ledger")
    return ledger


def run(run_file: Path, data_dir: Path, state_db: Path, ledger: lse.Ledger, out: Path) -> dict:
    """One cross-section run, declared on ``ledger`` before any price is parsed. Returns the report."""
    body = json.loads(Path(run_file).read_text())
    spec = RunFile.from_dict(body)
    man, man_sha = manifest(Path(data_dir))
    out = Path(out)
    out.mkdir(parents=True, exist_ok=False)
    lse.record_factory_declared(ledger, run_id=spec.run_id, run_file=body, data_manifest_sha256=man_sha,
                                trial_count=spec.trial_count())
    try:
        write_once(out / "data-manifest.json", man)
        panel = load_panel(Path(data_dir), spec)
        results = evaluate(panel, spec)
        modules = spec.modules()
        cost = spec.cost_bp_per_side * 1e-4
        net = np.column_stack([results[m["trial_id"]]["gross"] - SIDES[m["horizon"]] * cost for m in modules])
        bar = sign_flip_max_t(net, spec.perms, spec.seed, spec.alpha)
        trials = []
        for j, m in enumerate(modules):
            g = results[m["trial_id"]]["gross"]
            g = g[~np.isnan(g)]
            tested = bool(bar["tested"][j])
            mean_net = float(g.mean() - SIDES[m["horizon"]] * cost) if len(g) else None
            sd = float(g.std(ddof=1)) if len(g) > 1 else None
            trials.append({"trial_id": m["trial_id"], "family": m["family"], "feature": m["feature"],
                           "status": "tested" if tested else "untestable", "n": len(g),
                           "r": (mean_net / sd) if tested and sd else None, "net_dollars": mean_net if tested else None,
                           "gross_bp": float(g.mean() * 1e4) if len(g) else None,
                           "t": float(bar["t"][j]) if tested else None, "p": float(bar["p"][j]), "adjusted_p": float(bar["adjusted_p"][j]),
                           "selected": bool(tested and bar["adjusted_p"][j] <= spec.alpha)})
        max_t = {"critical_t": bar["critical_t"], "alpha": spec.alpha, "perms": spec.perms, "seed": spec.seed,
                 "null": "joint sign flips of nightly net spreads, step-down"}
        payload = {"method": {"statistic": "t of the mean net spread per night", "selection": "max_t_sign_flip",
                              "cost_bp_per_side": spec.cost_bp_per_side, "sides_per_night": SIDES},
                   "run_file": body, "data_manifest_sha256": man_sha, "trial_count": len(trials),
                   "untestable_count": sum(t["status"] != "tested" for t in trials), "max_t": max_t, "ledger": trials}
        frozen = {"payload": payload, "sha256": digest(payload)}
        write_once(out / "discovery-frozen.json", frozen)
        tested_rows = sorted((t for t in trials if t["status"] == "tested"), key=lambda t: -t["t"])
        state = load_state(Path(state_db))
        by_id = {m["trial_id"]: m for m in modules}
        carded = [t["trial_id"] for t in tested_rows[:spec.cards]]
        carded += [t["trial_id"] for t in tested_rows if t["selected"] and t["trial_id"] not in carded]
        member_days = panel.member[[r for r, d in enumerate(panel.sessions) if spec.first_session <= d <= spec.last_session]]
        rep = {
            "run_id": spec.run_id, "state": STATE, "promotion_allowed": False, "frozen_sha256": frozen["sha256"],
            "method": payload["method"], "target_unit": "net return per $1 long and $1 short per night (fraction)",
            "coverage": {"sessions": len(panel.sessions), "first": panel.sessions[0], "last": panel.sessions[-1],
                         "price_files": len(panel.cols), "members_mean": float(member_days.sum(axis=1).mean()),
                         "sector_assignment": panel.assignment, "data_manifest_sha256": man_sha},
            "trial_count": len(trials), "tested": len(tested_rows), "untestable": payload["untestable_count"], "max_t": max_t,
            "selected": [t["trial_id"] for t in tested_rows if t["selected"]],
            "ranking": [{k: t[k] for k in ("trial_id", "family", "feature", "n", "r", "net_dollars", "gross_bp", "t", "p",
                                           "adjusted_p", "selected")} for t in tested_rows],
            "cards": {tid: card(panel, by_id[tid], results[tid], state, SIDES[by_id[tid]["horizon"]] * cost) for tid in carded},
            "note": "development ranking only; a selected module is a candidate for one confirmation look, not a finding",
        }
        write_once(out / "report.json", rep)
        with (out / "report.md").open("x", encoding="utf-8") as stream:
            stream.write(report_markdown(rep))
    except Exception as e:  # the declared run is closed, never left open
        lse.record_factory_abandoned(ledger, reason=f"{type(e).__name__}: {e}")
        raise
    lse.record_factory_result(
        ledger, trials=[{k: t.get(k) for k in ("trial_id", "family", "feature", "status", "n", "r", "net_dollars", "t", "p",
                                               "adjusted_p", "selected")} for t in trials],
        max_t=max_t, report_sha256=digest(rep), frozen_sha256=frozen["sha256"])
    return rep


def report_markdown(rep: dict) -> str:
    bar = rep["max_t"]["critical_t"]
    lines = [f"# Cross-section factory run `{rep['run_id']}`", "",
             f"Development ranking only. {rep['tested']} modules tested, {len(rep['selected'])} cleared the bar "
             f"(t {bar:.2f} at {rep['max_t']['alpha']}, best of {rep['max_t']['perms']:,} sign-flipped searches)."
             if bar is not None else "Development ranking only. No module could be tested.", "",
             "| Module | Nights | Gross bp | Net bp | t | Adjusted p | Cleared |", "|---|---|---|---|---|---|---|"]
    for t in rep["ranking"]:
        lines.append(f"| {t['trial_id']} | {t['n']:,} | {t['gross_bp']:.2f} | {t['net_dollars'] * 1e4:.2f} | {t['t']:.2f} | "
                     f"{t['adjusted_p']:.4f} | {'yes' if t['selected'] else 'no'} |")
    for tid, c in rep["cards"].items():
        lines += ["", f"## {tid}", "", "| Cut | Value | Nights | Gross bp | Net bp | t |", "|---|---|---|---|---|---|"]
        for name, cells in c.items():
            if not isinstance(cells, dict) or name == "overall":
                continue
            for value, v in cells.items():
                if "gross_bp" in v:
                    lines.append(f"| {name} | {value} | {v['nights']:,} | {v['gross_bp']:.2f} | {v['net_bp']:.2f} | {v['t_gross']} |")
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    for name in ("--run-file", "--data-dir", "--state-db", "--ledger", "--anchor", "--out"):
        r.add_argument(name, required=True, type=Path)
    r.add_argument("--genesis", metavar="LEDGER_ID", help="create the factory ledger first (once)")
    a = ap.parse_args(argv)
    try:
        rep = run(a.run_file, a.data_dir, a.state_db, open_ledger(a.ledger, a.anchor, a.genesis), a.out)
    except RunFileError as e:
        print(json.dumps({"ok": False, "refused": str(e)}))
        return 2
    print(json.dumps({"ok": True, "run_id": rep["run_id"], "trials": rep["trial_count"], "tested": rep["tested"],
                      "selected": len(rep["selected"]), "critical_t": rep["max_t"]["critical_t"], "out": str(a.out)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
