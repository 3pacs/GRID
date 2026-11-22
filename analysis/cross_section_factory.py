"""Cross-section module factory: rank the stocks of an index on a per-stock signal, long the top, short the bottom.

The SPY factory (``analysis/module_factory.py``) asks "when X, does SPY move over H". This one asks "do the stocks a
signal ranks highest out-earn the ones it ranks lowest over H", across about 500 names a night, pooled or inside each
sector. Breadth gives it statistical power one index cannot have (docs/reference/CROSS_SECTION_FACTORY.md).

It reuses the factory's ledger records and report shape, so a run is declared before its data is read and the desk
evidence adapters can read its report. It brings its own null. The SPY factory shuffles the session order of the
target, which tests whether a signal's *timing* matters; a long-short spread trades every night, so that shuffle
leaves its mean unchanged. Here each draw flips the sign of blocks of consecutive nights' spreads (10 sessions unless
the run file says otherwise), the same flips for every module, and modules pass by Westfall-Young step-down, one-sided.
The blocks keep runs of good and bad nights together, so clustered spreads are not judged as if each night were new.

Discovery only. It reads development days (``DEV_FIRST`` .. ``DEV_LAST``) and drops every later bar on load; the
years after stay unread for a confirmation look. A selected module is a candidate, never a finding.

    python -m analysis.cross_section_factory run --run-file RUN.json --data-dir DIR --state-db state.db \\
        --ledger L.jsonl --anchor A.jsonl --out OUT [--genesis LEDGER_ID]

``DIR`` holds ``prices/<TICKER>.csv`` (Tiingo daily columns), ``members.csv`` (date, tickers), ``sectors.csv``
(Symbol, GICS Sector) and ``renames.json``. ``state.db`` is the sector state record (desk-sector-state/1).

Two things a run on another universe may change. The price and dollar-volume floors are run-file fields
(``min_price``, ``min_dollar_volume``) that default to ``MIN_PRICE`` and ``MIN_DOLLAR_VOLUME``. A ``sectors.csv`` that
holds its header and no rows says the store has no sector labels: the run ranks pooled, and a run file that asks for
a sector ranking, a sector gate or ``sector_momentum`` is refused.

A store may be a completed export: a directory that is written once and carries ``panel-manifest.json``
(``seal_export``), which lists every file's hash and names the first day discovery may use on it. The factory checks
the directory against that manifest before it declares a run, refuses a window that starts earlier, and binds the
manifest into the declared data manifest. A store with no sector labels must be such an export. Every input file is
read once, and its bytes are checked against the hash the run declared, so a file replaced after the declaration
closes the run as abandoned and two versions of a store never mix under one run id.
"""
from __future__ import annotations

import argparse
import bisect
import csv
import hashlib
import io
import json
import math
import sqlite3
import sys
import warnings
from dataclasses import dataclass, field
from datetime import date
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
MEMBERS_FIELD_LIMIT = 16 * 1024 * 1024   # one members.csv row lists every member; csv's default field limit is 131,072
EXPORT_MANIFEST = "panel-manifest.json"   # a completed export's own manifest, written once by seal_export
EXPORT_SCHEMA = "cross-section-panel-export/1"
_EXPORT_KEYS = {"schema", "complete", "generation", "discovery_first_session", "source", "files"}
SIDES = {"overnight": 4, "intraday": 4, "close_1d": 4}   # sides traded per night per $1 long and $1 short
SIGNAL_KINDS = ("mean_overnight", "mean_intraday", "momentum", "reversal", "high_52w", "sector_momentum")
PAID_MIN_NIGHTS = 100   # a card cut with fewer traded nights is not named in where it paid or did not
RANKINGS = ("pooled", "within_sector")
GROUPS = {
    "cyclical": ("Energy", "Materials", "Industrials", "Financials", "Consumer Discretionary", "Real Estate"),
    "growth": ("Information Technology", "Communication Services", "Health Care"),
    "defensive": ("Consumer Staples", "Utilities"),
}
GROUP_OF = {s: g for g, names in GROUPS.items() for s in names}
NIGHT_GATES = ("market_phase", "fed_balance_sheet", "vix_third", "spike_state")   # one value per night
SECTOR_GATES = ("sector_phase", "sector_leading", "groups")                        # one value per sector per night
PHASES = ("rising, speeding up", "rising, slowing", "falling, speeding up", "falling, slowing")
GATE_VALUES = {"market_phase": PHASES, "sector_phase": PHASES, "fed_balance_sheet": ("expanding", "flat", "shrinking"),
               "vix_third": ("calm", "middling", "fearful"), "spike_state": ("spike day", "aftermath", "normal"),
               "groups": tuple(GROUPS)}   # the words the sector state record (desk-sector-state/1) writes


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
    min_ranked_gated: int = 60
    cards: int = 5
    null_block_sessions: int = 10
    gates_chosen_after_seeing: bool | None = None
    prior_dev_looks: tuple = ()
    min_price: float = MIN_PRICE                      # prior close a stock needs to be ranked
    min_dollar_volume: float = MIN_DOLLAR_VOLUME      # median daily dollar volume it needs over the window before
    _keys = ("run_id", "first_session", "last_session", "signals", "rankings", "fractions", "cost_bp_per_side", "alpha",
             "perms", "seed", "min_ranked", "min_session_bars", "min_ranked_gated", "cards", "null_block_sessions",
             "gates_chosen_after_seeing", "prior_dev_looks", "min_price", "min_dollar_volume")

    @classmethod
    def from_dict(cls, value: dict) -> RunFile:
        if not isinstance(value, dict) or set(value) - set(cls._keys):
            raise RunFileError("unknown run-file keys")
        try:
            run = cls(**{**value, "signals": tuple(json.dumps(s, sort_keys=True) for s in value["signals"]),
                         "rankings": tuple(value["rankings"]), "fractions": tuple(value["fractions"]),
                         "prior_dev_looks": tuple(value.get("prior_dev_looks", ()))})
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
            if set(s) - {"name", "kind", "lookback", "skip", "sign", "horizons", "gate"} or s.get("kind") not in SIGNAL_KINDS:
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
            gate = s.get("gate", {})
            if not isinstance(gate, dict) or set(gate) - set(NIGHT_GATES) - set(SECTOR_GATES):
                raise RunFileError(f"a gate may use {NIGHT_GATES + SECTOR_GATES}")
            for key, allowed in gate.items():
                if key == "sector_leading":
                    if type(allowed) is not bool:
                        raise RunFileError("sector_leading is true or false")
                elif not (isinstance(allowed, list) and allowed and all(isinstance(v, str) for v in allowed)):
                    raise RunFileError(f"gate {key} needs a list of values")
                elif set(allowed) - set(GATE_VALUES[key]):
                    raise RunFileError(f"gate {key} values must be among {list(GATE_VALUES[key])}")
        if any(s["kind"] == "sector_momentum" for s in self.signal_specs()) and tuple(self.rankings) != ("pooled",):
            raise RunFileError("sector_momentum gives every stock of a sector the same value, so it ranks pooled only")
        if not all(isinstance(v, str) and v.strip() for v in self.prior_dev_looks):
            raise RunFileError("prior_dev_looks lists the run ids of earlier development looks at the same effect")
        if not names or not self.rankings or set(self.rankings) - set(RANKINGS):
            raise RunFileError(f"rankings must be among {RANKINGS}")
        if not self.fractions or any(not (isinstance(q, float) and 0.01 <= q <= 0.5) for q in self.fractions):
            raise RunFileError("fractions must be between 0.01 and 0.5")
        if not (0 < self.alpha <= 0.10 and 1_000 <= self.perms <= 100_000 and 0 <= self.cost_bp_per_side <= 50):
            raise RunFileError("alpha in (0, 0.10], perms 1,000 .. 100,000, cost 0 .. 50 bp")
        if min(self.min_ranked, self.min_session_bars) < 20 or self.min_ranked_gated < 10 or not 0 <= self.cards <= 50:
            raise RunFileError("min_ranked and min_session_bars at least 20, min_ranked_gated at least 10, cards 0 .. 50")
        if type(self.null_block_sessions) is not int or not 1 <= self.null_block_sessions <= 63:
            raise RunFileError("null_block_sessions is 1 .. 63 consecutive sessions")
        for name, low, high in (("min_price", 1.0, 10_000.0), ("min_dollar_volume", 1e5, 1e10)):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, int | float) or not low <= v <= high:
                raise RunFileError(f"{name} must be a number from {low:,.0f} to {high:,.0f}")
        gated = any(s.get("gate") for s in self.signal_specs())
        if gated and type(self.gates_chosen_after_seeing) is not bool:
            raise RunFileError("a run with gates must say gates_chosen_after_seeing (true when a gate was picked after "
                               "seeing an earlier result, which makes the run a source of hypotheses only)")
        if not gated and self.gates_chosen_after_seeing is not None:
            raise RunFileError("gates_chosen_after_seeing applies only to a run with gates")

    @property
    def hypothesis_only(self) -> bool:
        return self.gates_chosen_after_seeing is True

    @property
    def default_floors(self) -> bool:
        return (self.min_price, self.min_dollar_volume) == (MIN_PRICE, MIN_DOLLAR_VOLUME)

    def sector_uses(self) -> list[str]:
        """What in this run file needs sector labels; empty for a run that ranks pooled with no sector condition."""
        specs = self.signal_specs()
        uses = ["the within_sector ranking"] if "within_sector" in self.rankings else []
        uses += [f"signal {s['name']} (sector_momentum)" for s in specs if s["kind"] == "sector_momentum"]
        uses += [f"gate {k} on signal {s['name']}" for s in specs for k in SECTOR_GATES if k in s.get("gate", {})]
        return uses

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

def input_files(data_dir: Path) -> dict:
    """Name, size and sha256 of every panel file under ``data_dir``."""
    data_dir = Path(data_dir)
    files = {}
    for p in sorted([*data_dir.glob("prices/*.csv"), *(data_dir / n for n in ("members.csv", "sectors.csv", "renames.json"))]):
        files[str(p.relative_to(data_dir))] = {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
    if len(files) < 4:
        raise RunFileError("data directory needs prices/*.csv, members.csv, sectors.csv and renames.json")
    return files


def _is_day(value: object) -> bool:
    try:
        return isinstance(value, str) and date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


def seal_export(data_dir: Path, *, discovery_first_session: str, source: dict) -> dict:
    """Close a finished panel directory as an export: write ``panel-manifest.json``, once, over the files as they are.

    ``discovery_first_session`` is the first day a discovery window may start on this store. ``source`` says where the
    files came from (provider, requested start, vintage). The generation id is the hash of everything else in the
    manifest, so a directory whose files or terms differ carries a different id. Nothing may change afterwards."""
    if not _is_day(discovery_first_session):
        raise RunFileError("discovery_first_session must be a day, YYYY-MM-DD")
    core = {"schema": EXPORT_SCHEMA, "complete": True, "discovery_first_session": discovery_first_session,
            "source": source, "files": input_files(data_dir)}
    body = {**core, "generation": "g-" + digest(core)[:16]}
    write_once(Path(data_dir) / EXPORT_MANIFEST, body)
    return body


def read_export(data_dir: Path, files: dict) -> dict | None:
    """The export manifest of ``data_dir`` checked against ``files`` (from ``input_files``), or None when the
    directory carries none. Refuses an unfinished export, a manifest whose generation id is not its own hash, and a
    directory whose files are not exactly the ones the manifest lists."""
    path = Path(data_dir) / EXPORT_MANIFEST
    if not path.is_file():
        return None
    raw = path.read_bytes()
    try:
        exp = json.loads(raw)
        generation = "g-" + digest({k: v for k, v in exp.items() if k != "generation"})[:16]
    except (ValueError, AttributeError) as exc:
        raise RunFileError(f"{EXPORT_MANIFEST} cannot be read as an export manifest") from exc
    if exp.get("schema") != EXPORT_SCHEMA or exp.get("complete") is not True:
        raise RunFileError(f"{EXPORT_MANIFEST} is not a completed export ({EXPORT_SCHEMA})")
    if set(exp) != _EXPORT_KEYS or exp["generation"] != generation or not _is_day(exp["discovery_first_session"]):
        raise RunFileError(f"{EXPORT_MANIFEST} does not carry its own generation id and a discovery start day")
    listed = exp["files"] if isinstance(exp["files"], dict) else {}
    if listed != files:
        differ = sorted(n for n in set(listed) | set(files) if listed.get(n) != files.get(n))
        raise RunFileError(f"the data directory is not the export its manifest describes: {len(differ)} file(s) differ, "
                           f"first {differ[:3]}")
    return {"file": EXPORT_MANIFEST, "sha256": hashlib.sha256(raw).hexdigest(), "generation": generation,
            "discovery_first_session": exp["discovery_first_session"]}


def manifest(data_dir: Path, state_db: Path) -> tuple[dict, str]:
    """Name, size and sha256 of every input file, and the export manifest when the store is an export.

    Before the run is declared the factory hashes the files, reads the export manifest, and looks at whether
    ``sectors.csv`` has any row. No price, member or state row is parsed until after the declaration."""
    files = input_files(data_dir)
    if not Path(state_db).is_file():
        raise RunFileError("the sector state record (--state-db) is missing")
    body = {"schema": "cross-section-data-manifest/2", "files": files,
            "state_db": {"bytes": Path(state_db).stat().st_size, "sha256": hashlib.sha256(Path(state_db).read_bytes()).hexdigest()}}
    export = read_export(data_dir, files)
    if export:
        body["panel_export"] = export
    elif not any(line.strip() for line in (Path(data_dir) / "sectors.csv").read_bytes().splitlines()[1:]):
        raise RunFileError(f"a store with no sector labels must be a completed export: {EXPORT_MANIFEST} is missing")
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


@dataclass
class Universe:
    sessions: list
    cols: list
    frames: dict              # one frame per bar field, indexed by session, one column per price file
    ok: pd.DataFrame          # a valid bar: positive unadjusted and adjusted prices, open and close inside the day's range
    member: np.ndarray
    base: np.ndarray          # member, valid bar, price and liquidity floors


def _read(data_dir: Path, name: str, files: dict | None) -> bytes:
    """One input file's bytes. With ``files`` (the declared manifest's file table) the bytes must hash to what was
    declared, so a file replaced since then is never parsed."""
    try:
        raw = (Path(data_dir) / name).read_bytes()
    except FileNotFoundError:
        if files is None:
            raise
        raw = None
    if files is not None and (raw is None or hashlib.sha256(raw).hexdigest() != files.get(name, {}).get("sha256")):
        raise RunFileError(f"{name} changed between the run's declaration and its read; read a fixed copy")
    return raw


def universe(data_dir: Path, *, min_session_bars: int, min_price: float, min_dollar_volume: float,
             files: dict | None = None) -> Universe:
    """Which stock may be ranked on which session. This is the one definition: ``load_panel`` builds on it, and a
    tool that counts a store's universe (``analysis/smallcap_universe.py``) calls it, so the two cannot disagree.

    A session is a day on which at least ``min_session_bars`` price files have a bar. A stock may be ranked on session
    d when it is a member, has a valid bar on d, closed at ``min_price`` or more on the session before, and its median
    dollar volume over the ``DOLLAR_VOLUME_WINDOW`` sessions ending the session before is at least
    ``min_dollar_volume``. "The session before" is the store's previous session, not the stock's previous bar: a
    stock with no bar on it has no prior close and is out. Bars after ``DEV_LAST_BAR`` are dropped."""
    data_dir = Path(data_dir)
    on_disk = sorted(str(p.relative_to(data_dir)) for p in sorted((data_dir / "prices").glob("*.csv")))
    names = on_disk if files is None else sorted(n for n in files if n.startswith("prices/"))
    if names != on_disk:
        raise RunFileError("the price files changed between the run's declaration and its read; read a fixed copy")
    cols_data: dict[str, dict] = {k: {} for k in ("open", "close", "high", "low", "adjOpen", "adjClose", "volume")}
    for name in names:
        df = pd.read_csv(io.BytesIO(_read(data_dir, name, files)), usecols=["date", *cols_data])
        if df.empty:
            continue
        df["date"] = df["date"].str[:10]
        df = df[df["date"] <= DEV_LAST_BAR].drop_duplicates("date").set_index("date")   # nothing later is ever held
        for k, store in cols_data.items():
            store[Path(name).stem] = df[k]
    f = {k: pd.DataFrame(v).sort_index() for k, v in cols_data.items()}
    counts = f["close"].notna().sum(axis=1)
    sessions = [d for d, n in counts.items() if n >= min_session_bars]
    if not sessions or sessions[-1] > DEV_LAST_BAR:
        raise RunFileError("no development sessions in the data")
    f = {k: v.reindex(sessions) for k, v in f.items()}
    lo, hi = f["low"] * (1 - BAR_TOL), f["high"] * (1 + BAR_TOL)
    ok = ((f["open"] > 0) & (f["close"] > 0) & (f["high"] > 0) & (f["low"] > 0) & (f["adjOpen"] > 0) & (f["adjClose"] > 0)
          & (f["open"] >= lo) & (f["open"] <= hi) & (f["close"] >= lo) & (f["close"] <= hi))
    cols = list(ok.columns)

    limit = csv.field_size_limit(MEMBERS_FIELD_LIMIT)   # a universe of thousands writes a row longer than csv's default
    try:
        with io.StringIO(_read(data_dir, "members.csv", files).decode(), newline="") as fh:
            rows = sorted((r["date"], frozenset(t.strip() for t in r["tickers"].split(",") if t.strip())) for r in csv.DictReader(fh))
    finally:
        csv.field_size_limit(limit)
    mdates = [d for d, _ in rows]
    renames = dict(json.loads(_read(data_dir, "renames.json", files))["renames"])
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
    base = member & ok.values & (f["close"].shift(1) >= min_price).values & (dollar >= min_dollar_volume).values
    return Universe(sessions, cols, f, ok, member, base)


def load_panel(data_dir: Path, run: RunFile, files: dict | None = None) -> Panel:
    """The bars a run evaluates. ``files`` is the declared manifest's file table; ``run`` always passes it, so every
    file parsed is the one that was hashed before the declaration."""
    u = universe(data_dir, min_session_bars=run.min_session_bars, min_price=run.min_price,
                 min_dollar_volume=run.min_dollar_volume, files=files)
    f, ok, cols, member = u.frames, u.ok, u.cols, u.member
    prev_ok = ok.shift(1, fill_value=False)
    on = (f["adjOpen"] / f["adjClose"].shift(1) - 1).where(ok & prev_ok)
    intra = (f["close"] / f["open"] - 1).where(ok)
    cc = (f["adjClose"] / f["adjClose"].shift(1) - 1).where(ok & prev_ok)

    with io.StringIO(_read(data_dir, "sectors.csv", files).decode(), newline="") as fh:
        labels = {r["Symbol"].strip(): r["GICS Sector"].strip() for r in csv.DictReader(fh)}
    sectors = sorted(set(labels.values()))
    if set(sectors) - set(GROUP_OF):
        raise RunFileError(f"sectors without a group: {sorted(set(sectors) - set(GROUP_OF))}")
    if sectors:
        code, how = assign_sectors(cc.values, member, cols, labels, sectors)
    else:   # a sectors.csv with its header and no rows: the store has no labels, so nothing may depend on a sector
        if run.sector_uses():
            raise RunFileError(f"this store has no sector labels, so the run cannot use {'; '.join(run.sector_uses())}")
        code = np.full(len(cols), -1)
        how = {"labels": "none", "labelled_checked": 0, "leave_one_out_accuracy": None, "placed_by_correlation": 0,
               "unplaced": int(member.any(axis=0).sum())}
    return Panel(u.sessions, cols, on.values, intra.values, cc.values, f["adjClose"].values, u.base, member, code, sectors, how)


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
    elif kind == "high_52w":          # close of d-1 against the highest close of the L sessions through d-1
        s = adj.shift(1) / adj.shift(1).rolling(L, min_periods=need).max()
    else:                             # sector_momentum: the equal-weighted mean momentum of the stock's sector's members
        own = (adj.shift(1 + skip) / adj.shift(1 + skip + L) - 1).values
        inside = panel.member & ~np.isnan(own)
        s = np.full_like(own, np.nan)
        for k in range(len(panel.sectors)):
            cols = panel.sector == k
            if not cols.any():
                continue
            cnt = inside[:, cols].sum(axis=1)
            tot = np.where(inside[:, cols], own[:, cols], 0.0).sum(axis=1)
            mean = np.where(cnt >= 5, tot / np.where(cnt > 0, cnt, 1), np.nan)
            s[:, cols] = mean[:, None]
        s = pd.DataFrame(s)
    return (s * spec.get("sign", 1)).values


def outcome(panel: Panel, horizon: str) -> np.ndarray:
    """Row d = each stock's return over the horizon that starts at the close of d."""
    src = {"overnight": panel.on, "intraday": panel.intra, "close_1d": panel.cc}[horizon]
    out = np.full_like(src, np.nan)
    out[:-1] = src[1:]
    return out


def _spread(sig: np.ndarray, tick: np.ndarray, idx: np.ndarray, k: int, y: np.ndarray) -> tuple[float, np.ndarray, np.ndarray, int]:
    """(top-minus-bottom mean outcome, bottom picks, top picks, picks dropped) of the k lowest and k highest signals in idx.

    A pick whose outcome bar is missing or invalid (a halt, a delisting, a bad print) is dropped from its side's mean,
    not charged a loss: the data holds no delisting return. The count is reported so the bias can be sized."""
    order = idx[np.lexsort((tick[idx], sig[idx]))]
    lo_i, hi_i = order[:k], order[-k:]
    lo, hi = y[lo_i], y[hi_i]
    dropped = int(np.isnan(lo).sum() + np.isnan(hi).sum())
    lo, hi = lo[~np.isnan(lo)], hi[~np.isnan(hi)]
    return (float(hi.mean() - lo.mean()) if len(lo) and len(hi) else float("nan")), lo_i, hi_i, dropped


@dataclass
class States:
    """What the state record said the evening before each decision day (row r = the record of sessions[r-1])."""
    night: dict               # field -> list of values, None where no record
    sector_phase: np.ndarray  # sessions x sectors, object
    sector_leading: np.ndarray
    matched: int


def state_arrays(panel: Panel, state: dict) -> States:
    n_d, n_s = len(panel.sessions), len(panel.sectors)
    night = {k: [None] * n_d for k in NIGHT_GATES}
    phase = np.full((n_d, n_s), None, dtype=object)
    leading = np.full((n_d, n_s), None, dtype=object)
    matched = 0
    for r in range(1, n_d):
        rec = state.get(panel.sessions[r - 1])
        if rec is None:
            continue
        matched += 1
        night["market_phase"][r] = rec["market"]["phase"]
        night["fed_balance_sheet"][r] = rec["fed_balance_sheet"].get("phase")
        night["vix_third"][r] = rec["vix"].get("third")
        night["spike_state"][r] = rec["vix"].get("spike_state")
        for j, s in enumerate(panel.sectors):
            sec = rec["sectors"].get(s, {})
            phase[r, j], leading[r, j] = sec.get("phase"), sec.get("leading")
    return States(night, phase, leading, matched)


def _sectors_allowed(gate: dict, states: States, panel: Panel, r: int) -> np.ndarray | None:
    """Which sectors a gate lets through on decision day r; None when the gate names no sector condition."""
    if not set(gate) & set(SECTOR_GATES):
        return None
    ok = np.ones(len(panel.sectors), dtype=bool)
    if "groups" in gate:
        ok &= np.array([GROUP_OF[s] in gate["groups"] for s in panel.sectors])
    if "sector_phase" in gate:
        ok &= np.array([p in gate["sector_phase"] for p in states.sector_phase[r]])
    if "sector_leading" in gate:
        ok &= np.array([v is gate["sector_leading"] for v in states.sector_leading[r]])
    return ok


def _turnover(prev: tuple | None, lo: np.ndarray, hi: np.ndarray) -> float:
    """Share of each side replaced since the previous night's picks (1.0 = everything is new)."""
    if prev is None or not len(lo) or not len(hi):
        return 1.0
    return 0.5 * ((1 - len(np.intersect1d(prev[0], lo)) / len(lo)) + (1 - len(np.intersect1d(prev[1], hi)) / len(hi)))


def evaluate(panel: Panel, run: RunFile, states: States) -> dict:
    """trial_id -> gross spread per session, names a side, turnover, and per-sector spreads for within-sector modules.

    A night a module's gate turns away is NaN (not traded). Turnover is 1.0 on the overnight and intraday horizons,
    which open and close every position; on ``close_1d`` it is the share of names replaced since the previous
    traded night, so a slow signal is charged for what it actually trades."""
    tick = np.array(panel.cols, dtype=object)
    rows = [r for r, d in enumerate(panel.sessions[:-1]) if run.first_session <= d <= run.last_session]
    n_d, n_s = len(panel.sessions), len(panel.sectors)
    out = {}
    for spec in run.signal_specs():
        name, gate = spec["name"], spec.get("gate", {})
        sig = signal_matrix(panel, spec)
        eligible = panel.base & ~np.isnan(sig)
        mods = [m for m in run.modules() if m["signal"] == name]
        ys = {h: outcome(panel, h) for h in spec["horizons"]}
        prev: dict = {}
        prev_sector: dict = {}     # trial -> (night, {sector: (bottom picks, top picks)}) of its last traded night
        for m in mods:
            within = m["ranking"] == "within_sector"
            out[m["trial_id"]] = {"gross": np.full(n_d, np.nan), "names": np.zeros(n_d), "turnover": np.full(n_d, np.nan),
                                  "picks": np.zeros(n_d), "dropped": np.zeros(n_d),
                                  "per_sector": np.full((n_d, n_s), np.nan) if within else None,
                                  "k_sector": np.zeros((n_d, n_s)) if within else None,
                                  "turnover_sector": np.full((n_d, n_s), np.nan) if within else None}
        for r in rows:
            idx = np.flatnonzero(eligible[r])
            if len(idx) < run.min_ranked:
                continue
            if any(states.night[k][r] not in gate[k] for k in NIGHT_GATES if k in gate):
                continue
            allowed = _sectors_allowed(gate, states, panel, r)
            if allowed is not None:
                sec = panel.sector[idx]
                idx = idx[(sec >= 0) & allowed[np.clip(sec, 0, None)]]
                if len(idx) < run.min_ranked_gated:
                    continue
            by_sector = [idx[panel.sector[idx] == s] for s in range(n_s)]
            for m in mods:
                y, q, res = ys[m["horizon"]][r], m["fraction"], out[m["trial_id"]]
                if m["ranking"] == "pooled":
                    k = math.floor(q * len(idx))
                    if k < 1:
                        continue
                    v, lo, hi, dropped = _spread(sig[r], tick, idx, k, y)
                    names = k
                else:
                    floor = max(8, math.ceil(1 / q))
                    num = den = 0.0
                    los, his = [], []
                    picked: dict = {}
                    dropped = 0
                    for s, sidx in enumerate(by_sector):
                        if len(sidx) < floor:
                            continue
                        k = max(1, math.floor(q * len(sidx)))
                        sv, lo_s, hi_s, d_s = _spread(sig[r], tick, sidx, k, y)
                        res["per_sector"][r, s], res["k_sector"][r, s] = sv, k
                        picked[s] = (lo_s, hi_s)
                        los.append(lo_s)
                        his.append(hi_s)
                        dropped += d_s
                        if not math.isnan(sv):
                            num, den = num + k * sv, den + k
                    if not den:
                        continue
                    v, names = num / den, den
                    lo, hi = np.concatenate(los), np.concatenate(his)
                res["gross"][r], res["names"][r] = v, names
                res["picks"][r], res["dropped"][r] = len(lo) + len(hi), dropped
                last = prev.get(m["trial_id"])
                adjacent = last is not None and last[0] == r - 1
                res["turnover"][r] = _turnover(last[1] if adjacent else None, lo, hi) if m["horizon"] == "close_1d" else 1.0
                prev[m["trial_id"]] = (r, (lo, hi))
                if m["ranking"] != "pooled":
                    # each sector's own share replaced against its own picks the night before; their k-weighted mean
                    # is the module's turnover, since a name stays in its sector
                    before = prev_sector[m["trial_id"]][1] if adjacent else {}
                    for s, (lo_s, hi_s) in picked.items():
                        res["turnover_sector"][r, s] = (_turnover(before.get(s), lo_s, hi_s)
                                                        if m["horizon"] == "close_1d" else 1.0)
                    prev_sector[m["trial_id"]] = (r, picked)
    return out


# ── The bar ───────────────────────────────────────────────────────────────

def _t(sums: np.ndarray, sumsq: np.ndarray, n: np.ndarray) -> np.ndarray:
    with np.errstate(divide="ignore", invalid="ignore"):
        mean = sums / n
        var = (sumsq - n * mean ** 2) / (n - 1)
        t = mean / np.sqrt(var / n)
    return np.where((n >= 30) & (var > 1e-18), t, -np.inf)


def sign_flip_max_t(x: np.ndarray, perms: int, seed: int, alpha: float, block: int = 1) -> dict:
    """x: sessions x modules net spreads (NaN = not traded). One-sided step-down max-T under joint sign flips.

    Each draw flips the sign of whole blocks of ``block`` consecutive sessions, the same flips for every module. Within
    a block the nights keep their order and their run of good or bad nights, so a spread whose nights cluster (a
    factor episode, a crash year) meets a wider null than independent flips would give it. ``block=1`` is the plain
    night-by-night flip. The statistic is the ordinary t; the blocks widen its null, not its formula."""
    have = ~np.isnan(x)
    x0 = np.where(have, x, 0.0)
    n = have.sum(axis=0).astype(float)
    sumsq = (x0 ** 2).sum(axis=0)
    observed = _t(x0.sum(axis=0), sumsq, n)
    rng = np.random.default_rng(seed)
    block_of = np.arange(x.shape[0]) // max(1, int(block))
    n_blocks = int(block_of[-1]) + 1 if x.shape[0] else 0
    null = np.empty((perms, x.shape[1]), dtype=np.float32)
    for s in range(0, perms, 500):
        m = min(500, perms - s)
        flips = rng.choice((-1.0, 1.0), size=(m, n_blocks))[:, block_of]
        null[s:s + m] = _t(flips @ x0, sumsq, n)
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
    return {"t": observed, "n": n, "p": marginal, "adjusted_p": adjusted, "tested": tested, "critical_t": critical,
            "block": max(1, int(block))}


# ── The conditional card ──────────────────────────────────────────────────

def load_state(path: Path) -> dict:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        return {d: json.loads(body) for d, body in con.execute("SELECT data_through, record_json FROM desk_state")}
    finally:
        con.close()


def _cut(gross: np.ndarray, cost: np.ndarray) -> dict:
    """Summary of the nights where ``gross`` is known; ``cost`` is the module's cost on those same nights."""
    keep = ~np.isnan(gross)
    v, c = gross[keep], cost[keep]
    if len(v) < 30:
        return {"nights": len(v)}
    mean, sd = float(v.mean()), float(v.std(ddof=1))
    return {"nights": len(v), "gross_bp": round(mean * 1e4, 3), "net_bp": round((mean - float(np.nanmean(c))) * 1e4, 3),
            "sd_bp": round(sd * 1e4, 2), "t_gross": round(mean / (sd / math.sqrt(len(v))), 3) if sd else None}


def card(panel: Panel, res: dict, states: States, rate: float) -> dict:
    """Where the module paid: by year and by the states recorded the evening before each decision day.

    ``rate`` is the cost of a turnover of 1 (sides x cost per side). A cut of whole nights pays the module's turnover;
    a sector pays its own, and a cut of several sectors pays their turnovers weighted like its spread."""
    gross = res["gross"]
    cost = rate * res["turnover"]
    traded = ~np.isnan(gross)

    def where(mask: np.ndarray) -> dict:
        return _cut(np.where(mask, gross, np.nan), cost)

    out: dict = {"overall": _cut(gross, cost), "state_rows_matched": states.matched}
    years = np.array([d[:4] for d in panel.sessions])
    out["year"] = {y: where(years == y) for y in sorted(set(years[traded]))}
    for name in NIGHT_GATES:
        arr = np.array(states.night[name], dtype=object)
        out[name] = {str(v): where(arr == v) for v in sorted({x for x, t in zip(states.night[name], traded) if t and x is not None})}
    if res["per_sector"] is not None:
        ps, ks = res["per_sector"], res["k_sector"]
        sector_cost = rate * res["turnover_sector"]

        def pooled(pick: np.ndarray) -> dict:
            use = pick & ~np.isnan(ps) & (ks > 0)
            w = np.where(use, ks, 0.0)
            tot = w.sum(axis=1)
            den = np.where(tot > 0, tot, 1)
            spread = np.where(tot > 0, (np.where(use, ps, 0.0) * w).sum(axis=1) / den, np.nan)
            return _cut(spread, np.where(tot > 0, (np.where(use, sector_cost, 0.0) * w).sum(axis=1) / den, np.nan))

        every = np.ones_like(ps, dtype=bool)
        out["sector"] = {s: _cut(ps[:, j], sector_cost[:, j]) for j, s in enumerate(panel.sectors)}
        out["group"] = {g: pooled(every & np.array([GROUP_OF[s] == g for s in panel.sectors])[None, :]) for g in GROUPS}
        seen = sorted({x for x in states.sector_phase[traded].ravel() if x})
        out["sector_phase"] = {str(p): pooled(states.sector_phase == p) for p in seen}
        lead = np.vectorize(lambda v: v is True, otypes=[bool])(states.sector_leading)
        lag = np.vectorize(lambda v: v is False, otypes=[bool])(states.sector_leading)
        out["sector_vs_market"] = {"leading": pooled(lead), "lagging": pooled(lag)}
    out["where_it_paid"], out["where_it_did_not"] = paid_split(out)
    return out


def paid_split(c: dict) -> tuple[list, list]:
    """Every card cut with at least PAID_MIN_NIGHTS traded nights, split by the sign of its net return and sorted by
    size: where the module paid after cost and where it did not. A description of a searched sample, not a test."""
    rows = []
    for name, cells in c.items():
        if name in ("overall", "state_rows_matched", "where_it_paid", "where_it_did_not") or not isinstance(cells, dict):
            continue
        for value, v in cells.items():
            if isinstance(v, dict) and v.get("nights", 0) >= PAID_MIN_NIGHTS and "net_bp" in v:
                rows.append({"cut": name, "value": value, "nights": v["nights"], "net_bp": v["net_bp"], "t_gross": v["t_gross"]})
    paid = sorted((r for r in rows if r["net_bp"] > 0), key=lambda r: -r["net_bp"])
    not_paid = sorted((r for r in rows if r["net_bp"] <= 0), key=lambda r: r["net_bp"])
    return paid, not_paid


# ── A run ─────────────────────────────────────────────────────────────────

def open_ledger(ledger_path: Path, anchor_path: Path, genesis: str | None) -> lse.Ledger:
    if genesis:
        return lse.Ledger.create_factory(ledger_path, ledger_id=genesis, anchor=anchor_path)
    ledger = lse.Ledger(ledger_path, anchor=anchor_path)
    if not ledger.is_factory:
        raise RunFileError("that ledger is not a factory ledger")
    return ledger


def run(run_file: Path, data_dir: Path, state_db: Path, ledger: lse.Ledger, out: Path) -> dict:
    """One cross-section run, declared on ``ledger`` before any price or state row is parsed. Returns the report."""
    body = json.loads(Path(run_file).read_text())
    spec = RunFile.from_dict(body)
    run_sha = digest(body)
    for rec in ledger.records:   # one look per run file: a run id or run file this ledger declared before is refused
        if rec.get("kind") == lse.FACTORY_DECLARED and (rec.get("run_id") == spec.run_id or rec.get("run_file_sha256") == run_sha):
            raise RunFileError(f"this ledger already declared run {rec.get('run_id')} (same run id or run file)")
    out = Path(out)
    if out.exists():
        raise RunFileError(f"output directory {out} already exists")
    man, man_sha = manifest(Path(data_dir), Path(state_db))
    export = man.get("panel_export")
    if export and spec.first_session < export["discovery_first_session"]:
        raise RunFileError(f"discovery on this export starts {export['discovery_first_session']}; the run file's window "
                           f"starts {spec.first_session}")
    lse.record_factory_declared(ledger, run_id=spec.run_id, run_file=body, data_manifest_sha256=man_sha,
                                trial_count=spec.trial_count())
    try:
        out.mkdir(parents=True, exist_ok=False)
        write_once(out / "data-manifest.json", man)
        panel = load_panel(Path(data_dir), spec, man["files"])
        state_rows = load_state(Path(state_db))
        if hashlib.sha256(Path(state_db).read_bytes()).hexdigest() != man["state_db"]["sha256"]:
            raise RunFileError("the state record changed between the run's declaration and its read; read a fixed copy")
        states = state_arrays(panel, state_rows)
        results = evaluate(panel, spec, states)
        modules = spec.modules()
        gates = {s["name"]: s.get("gate", {}) for s in spec.signal_specs()}
        per_side = spec.cost_bp_per_side * 1e-4
        rates = {m["trial_id"]: SIDES[m["horizon"]] * per_side for m in modules}
        costs = {tid: rates[tid] * results[tid]["turnover"] for tid in rates}
        net = np.column_stack([results[m["trial_id"]]["gross"] - costs[m["trial_id"]] for m in modules])
        bar = sign_flip_max_t(net, spec.perms, spec.seed, spec.alpha, spec.null_block_sessions)
        counts_as_cleared = not spec.hypothesis_only
        window = sum(1 for d in panel.sessions[:-1] if spec.first_session <= d <= spec.last_session)
        trials = []
        for j, m in enumerate(modules):
            x = net[:, j]
            x = x[~np.isnan(x)]
            g = results[m["trial_id"]]["gross"]
            g = g[~np.isnan(g)]
            tested = bool(bar["tested"][j])
            sd = float(x.std(ddof=1)) if len(x) > 1 else None
            res_m = results[m["trial_id"]]
            picks = float(res_m["picks"].sum())
            passed = bool(tested and bar["adjusted_p"][j] <= spec.alpha)
            trials.append({"trial_id": m["trial_id"], "family": m["family"], "feature": m["feature"],
                           "passed_step_down": passed, "dropped_pick_share": (float(res_m["dropped"].sum()) / picks) if picks else None,
                           "status": "tested" if tested else "untestable", "n": len(x),
                           "r": (float(x.mean()) / sd) if tested and sd else None, "net_dollars": float(x.mean()) if tested else None,
                           "gross_bp": float(g.mean() * 1e4) if len(g) else None,
                           "turnover": float(np.nanmean(results[m["trial_id"]]["turnover"])) if len(g) else None,
                           "share_of_nights": len(x) / window if window else None, "gate": gates[m["signal"]],
                           "t": float(bar["t"][j]) if tested else None, "p": float(bar["p"][j]), "adjusted_p": float(bar["adjusted_p"][j]),
                           "selected": passed and counts_as_cleared})
        passed_t = [t["t"] for t in trials if t["passed_step_down"]]
        max_t = {"critical_t": bar["critical_t"], "alpha": spec.alpha, "perms": spec.perms, "seed": spec.seed,
                 "null": f"joint sign flips of nightly net spreads in blocks of {bar['block']} consecutive sessions, step-down",
                 "block_sessions": bar["block"],
                 "critical_t_is": "the first step's bar (the best flipped t's 1-alpha quantile); later steps compare each "
                                  "module with the best flipped t of the modules ranked below it, so a module may pass with "
                                  "a t under this figure",
                 "lowest_passing_t": min(passed_t) if passed_t else None,
                 "multiplicity": "within this run only; earlier runs and earlier looks are not counted"
                                 + (f" (earlier development looks at this effect: {', '.join(spec.prior_dev_looks)})"
                                    if spec.prior_dev_looks else ""),
                 "prior_dev_looks": list(spec.prior_dev_looks),
                 "hypothesis_only": spec.hypothesis_only}
        payload = {"method": {"statistic": "t of the mean net spread per traded night", "selection": "max_t_sign_flip",
                              "cost_bp_per_side": spec.cost_bp_per_side, "sides_per_night": SIDES,
                              "cost": "sides x cost x turnover; turnover is 1 except on close_1d, where it is the share of names replaced"},
                   "run_file": body, "data_manifest_sha256": man_sha, "trial_count": len(trials),
                   "untestable_count": sum(t["status"] != "tested" for t in trials), "max_t": max_t, "ledger": trials}
        frozen = {"payload": payload, "sha256": digest(payload)}
        write_once(out / "discovery-frozen.json", frozen)
        tested_rows = sorted((t for t in trials if t["status"] == "tested"), key=lambda t: -t["t"])
        carded = [t["trial_id"] for t in tested_rows[:spec.cards]]
        carded += [t["trial_id"] for t in tested_rows if t["passed_step_down"] and t["trial_id"] not in carded]
        member_days = panel.member[[r for r, d in enumerate(panel.sessions) if spec.first_session <= d <= spec.last_session]]
        # A run file that sets its own floors has them written beside the coverage; one on the defaults reads as before.
        floors = {} if spec.default_floors else {"floors": {"min_price": spec.min_price, "min_dollar_volume": spec.min_dollar_volume,
                                                            "dollar_volume_window": DOLLAR_VOLUME_WINDOW}}
        sector_limit = (("sector labels are today's; past members without a label are placed by correlation over "
                         "the whole window, so a within-sector module's grouping uses later returns") if panel.sectors else
                        "this store has no sector labels: every module ranks pooled, and no card is cut by sector")
        rep = {
            "run_id": spec.run_id, "state": STATE, "promotion_allowed": False, "frozen_sha256": frozen["sha256"],
            "method": payload["method"], "target_unit": "net return per $1 long and $1 short per traded night (fraction)",
            "coverage": {"sessions": len(panel.sessions), "first": panel.sessions[0], "last": panel.sessions[-1],
                         "price_files": len(panel.cols), "members_mean": float(member_days.sum(axis=1).mean()),
                         "sector_assignment": panel.assignment, "state_rows_matched": states.matched,
                         "data_manifest_sha256": man_sha, **floors,
                         **({"panel_export": {k: export[k] for k in ("generation", "discovery_first_session", "sha256")}}
                            if export else {})},
            "trial_count": len(trials), "tested": len(tested_rows), "untestable": payload["untestable_count"], "max_t": max_t,
            "selected": [t["trial_id"] for t in tested_rows if t["selected"]],
            "ranking": [{k: t[k] for k in ("trial_id", "family", "feature", "n", "r", "net_dollars", "gross_bp", "turnover",
                                           "share_of_nights", "gate", "t", "p", "adjusted_p", "passed_step_down",
                                           "dropped_pick_share", "selected")} for t in tested_rows],
            "cards": {tid: card(panel, results[tid], states, rates[tid]) for tid in carded},
            "hypothesis_only": spec.hypothesis_only,
            "note": ("development ranking only; a selected module is a candidate for one confirmation look, not a finding"
                     if counts_as_cleared else
                     "development ranking only, and a source of hypotheses only: its gates were chosen after seeing an "
                     "earlier result, so the step-down test here does not count the search that chose them and no module "
                     "is selected; a module that passed it may be registered as a new hypothesis for fresh data"),
            "known_limits": [("a pick whose outcome bar is missing (halt, delisting, bad print) is dropped, not charged a "
                              "loss; dropped_pick_share sizes it"),
                             sector_limit,
                             "cost is sides x cost x turnover at a flat rate per side, with no borrow fee on the short side"],
        }
        write_once(out / "report.json", rep)
        with (out / "report.md").open("x", encoding="utf-8") as stream:
            stream.write(report_markdown(rep))
    except BaseException as e:  # the declared run is closed, never left open (Ctrl-C and SIGTERM's exit included)
        lse.record_factory_abandoned(ledger, reason=f"{type(e).__name__}: {e}")
        raise
    lse.record_factory_result(
        ledger, trials=[{k: t.get(k) for k in ("trial_id", "family", "feature", "status", "n", "r", "net_dollars", "t", "p",
                                               "adjusted_p", "selected")} for t in trials],
        max_t=max_t, report_sha256=digest(rep), frozen_sha256=frozen["sha256"])
    return rep


def report_markdown(rep: dict) -> str:
    mt = rep["max_t"]
    bar, low = mt["critical_t"], mt.get("lowest_passing_t")
    passed = sum(bool(t.get("passed_step_down")) for t in rep["ranking"])
    head = (f"Development ranking only. {rep['tested']} modules tested; {passed} passed the step-down test at "
            f"{mt['alpha']} ({mt['perms']:,} draws flipping blocks of {mt.get('block_sessions', 1)} sessions). The first "
            f"step's bar is t {bar:.2f}" + (f"; the lowest t that passed is {low:.2f}." if low is not None else ".")
            if bar is not None else "Development ranking only. No module could be tested.")
    if rep.get("hypothesis_only"):
        head += (" Its gates were chosen after seeing an earlier result: nothing here is selected, and a module that passed"
                 " is a hypothesis for fresh data only.")
    lines = [f"# Cross-section factory run `{rep['run_id']}`", "", head, "",
             "| Module | Nights | Share of nights | Turnover | Dropped picks | Gross bp | Net bp | t | Adjusted p | Passed |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for t in rep["ranking"]:
        dp = t.get("dropped_pick_share")
        lines.append(f"| {t['trial_id']} | {t['n']:,} | {t['share_of_nights']:.2f} | {t['turnover']:.2f} | "
                     f"{'' if dp is None else f'{dp:.4f}'} | {t['gross_bp']:.2f} | "
                     f"{t['net_dollars'] * 1e4:.2f} | {t['t']:.2f} | {t['adjusted_p']:.4f} | {'yes' if t.get('passed_step_down') else 'no'} |")
    for tid, c in rep["cards"].items():
        lines += ["", f"## {tid}", "", "| Cut | Value | Nights | Gross bp | Net bp | t |", "|---|---|---|---|---|---|"]
        for name, cells in c.items():
            if not isinstance(cells, dict) or name == "overall":
                continue
            for value, v in cells.items():
                if "gross_bp" in v:
                    lines.append(f"| {name} | {value} | {v['nights']:,} | {v['gross_bp']:.2f} | {v['net_bp']:.2f} | {v['t_gross']} |")

        def said(rows: list) -> str:
            return "; ".join(f"{r['cut']} {r['value']} {r['net_bp']:+.2f} bp ({r['nights']:,} nights)" for r in rows[:6]) or "nowhere"
        lines += ["", f"Paid after cost (cuts of {PAID_MIN_NIGHTS}+ nights, largest first): {said(c.get('where_it_paid', []))}.",
                  f"Did not: {said(c.get('where_it_did_not', []))}."]
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
