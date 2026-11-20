"""Module factory, factory 4 (3pacs/GRID#1158): the options-positioning adapter.

The class reads SPY options positioning at a decision time T, point in time, through the live GEX
Levels page's own book function, so research and the page share one definition:

* **The shared function.** ``levels.horizon_book`` and its level rule ``levels.position_levels``
  (POSITION_RULE ``nearest-position/1``) come from gex ``src/gex-watch/levels.py``, vendored byte for
  byte under ``analysis/gex_vendor/`` with ``volume_quality.py`` (its one import). :data:`VENDORED` pins
  the gex commit, git blob and sha256 of each file; :func:`load_levels` refuses a copy whose bytes
  differ and executes exactly the bytes it hashed. A copy cannot drift silently: a test re-hashes the
  files, and a parity test compares them with a gex checkout when one is present (``GEX_REPO``).
  To move to a newer gex ``levels.py``: copy both files from one gex commit and update the pins.
* **0DTE book (gexhist, the full dev window).** The staged ``edge2/gexhist.db`` (gex edge4 fetcher,
  Databento OPRA.PILLAR): per session D, the open interest of the SPY options expiring on D (OPRA
  ``statistics`` stat_type 9, the OCC number for D's open, effective at the prior session's close) at
  $1 strikes within 8% of D's open, and the consolidated bid/ask in force at 5-minute marks. Each
  contract's IV is inverted from the mark's mid; ``horizon_book`` then computes Black-Scholes gamma
  from that IV, exactly as the page does for a row without a provider gamma.
* **All-expiry OI (the OCC archive, its trailing year only).** A frozen copy of gex-oi-capture's
  ``oi.db`` (Databento OCC open interest, every SPY expiry): position levels over every unexpired
  expiry (the page's ALL horizon). OI only, no gamma: the archive holds no quotes.

Point in time (fixed here, never in a run file):

* SPY spot at T is the close of the bar stamped T-1 (as every factory feature).
* **Quotes:** a mark labelled ``m`` holds the last quote received during minute ``m`` (the fetcher
  keeps records whose receive minute is <= m), so it is taken as known only at m + 1 minute. A
  decision at T uses the latest mark with m + 1 <= T - 1, i.e. m <= T - :data:`QUOTE_LAG_MINUTES`
  (09:55 for a 10:00 decision). Its IV is inverted at the spot of the bar stamped ``m``.
* **Open interest:** OCC publishes D's open interest at about 06:30 ET on D (the archive's
  ``published_at`` is 06:30 ET on every day it holds) and the archive's capture window closes at
  07:30 ET. OI for D counts only for decisions after :data:`OI_KNOWN_ET` (07:30 ET) on D, or after
  its archive ``published_at`` when that is later. gexhist carries no publication clock; on the 246
  sessions both sources hold, every one of gexhist's 48,098 0DTE OI values equals the archive's
  06:30 ET value, which is the evidence for applying the same clock to gexhist.
* Nothing from a later session ever enters (each session is read on its own).

Measurement status (#1158 wall-hardening checklist, not done): #1132 (measurement) and #1108
(canonical snapshots) are open, so walls and the signed dealer gamma are diagnostics:
``measurement-unqualified``. Unsigned OI concentration (position levels) is kept apart from the
modeled gamma walls and from the assumed dealer sign (see ``basis`` on each feature spec).

Formula (named; nothing converted): :data:`FORMULA`. GRID's daily-context ``physics/dealer_gamma.py``
measures a different product (gamma x OI x 100 x S, dollars per $1 move, prior-close spot, dte > 0
only); the factory does not use it and does not convert either into the other.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sqlite3
import statistics
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
from scipy.special import ndtr

NY = ZoneInfo("America/New_York")
DEV_FIRST = "2022-11-14"
GEX_REPO = "3pacs/gex"
GEX_COMMIT = "3c8d8451d79df130bdf0b8ce3a847cc09ab0e97d"
VENDOR_DIR = Path(__file__).resolve().with_name("gex_vendor")
VENDORED = {
    "volume_quality.py": {"gex_path": "src/gex-watch/volume_quality.py",
                          "blob": "2b462c74bdb16d1605009d47ddb8a8dcfce7420b",
                          "sha256": "9d69be921749d04197ffcf7043be46da261e8b9543cc670603bf1bafc145335b"},
    "levels.py": {"gex_path": "src/gex-watch/levels.py",
                  "blob": "924021bf00c2182edd24e45404befbaee4f55476",
                  "sha256": "f68a6c3615edaa2db9fe7052529cc45e22dd04abcc5b4c8fa00474de4ec63d82"},
}
FORMULA = {
    "id": "gex-live-0dte/levels.horizon_book",
    "unit": "$M per 1% SPY move: gamma x OI x 100 x S^2 x 0.01 / 1e6, per contract",
    "gamma": "Black-Scholes from each contract's IV (levels.bs_gamma: r 0.04, q 0.012, time to 16:00 ET floored at "
             "5 minutes); IV inverted here from the 5-minute mark's mid with the same r and q",
    "sign": "assumed: dealers long every call (+), short every put (-); an assumption, not observed inventory",
    "not_used": "GRID physics/dealer_gamma.py (daily context: gamma x OI x 100 x S, $ per $1 move, prior-close spot, "
                "dte > 0, r 0.05) is a different product. Not converted, not compared: gex_per_1pct = "
                "grid_per_dollar x S x 0.01 holds only for identical contracts, spot, IV, clock, rates and "
                "ownership, and the two universes do not even overlap (0DTE here, dte > 0 there).",
}
DATA_FILE = "edge2/gexhist.db"  # in the staged dev manifest (factory 1's data)
QUOTE_LAG_MINUTES = 2  # mark m is known at m + 1; m + 1 <= T - 1
MARK_FIRST, MARK_LAST, MARK_STEP = 9 * 60 + 35, 16 * 60, 5
OI_KNOWN_ET = (7, 30)  # the archive's capture window end; OCC publishes at 06:30 ET
MIN_OI_STRIKES = 20  # edge4: OI > 0 on >= 20 strikes above and below the open, else no book
MIN_IV_STRIKES = 10  # gamma features need a valid IV on >= 10 strikes each side of spot within the window
IV_LO, IV_HI, IV_ITERS = 1e-4, 5.0, 80  # edge4 bisection bounds
IV_COVERAGE_PCT = 1.0  # coverage diagnostic: share of OI within this % of spot whose contract has an IV
OI_ARCHIVE_ROOT = "SPY"

_levels_cache: dict = {}


class OptionsDataError(ValueError):
    pass


def git_blob(data: bytes) -> str:
    """The git blob id of ``data`` (what ``git rev-parse <commit>:<path>`` prints)."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def _exec(name: str, data: bytes, path: Path, inject: dict):
    spec = importlib.util.spec_from_loader(f"analysis.gex_vendor.{name}", loader=None, origin=str(path))
    mod = importlib.util.module_from_spec(spec)
    mod.__file__ = str(path)
    saved = {k: sys.modules.get(k) for k in inject}
    sys.modules.update(inject)
    try:
        exec(compile(data, str(path), "exec"), mod.__dict__)  # noqa: S102 - pinned, hash-checked bytes
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v
    return mod


def load_levels(vendor_dir: Path = VENDOR_DIR):
    """The vendored gex ``levels`` module, after checking every pinned file's sha256 and git blob.

    ``levels.py`` does ``import volume_quality``; the pinned copy is supplied for that import only."""
    key = str(vendor_dir)
    if key in _levels_cache:
        return _levels_cache[key]
    data = {}
    for name, pin in VENDORED.items():
        raw = (vendor_dir / name).read_bytes()
        if hashlib.sha256(raw).hexdigest() != pin["sha256"] or git_blob(raw) != pin["blob"]:
            raise OptionsDataError(f"vendored {name} does not match gex {GEX_COMMIT[:8]}:{pin['gex_path']}")
        data[name] = raw
    vq = _exec("volume_quality", data["volume_quality.py"], vendor_dir / "volume_quality.py", {})
    levels = _exec("levels", data["levels.py"], vendor_dir / "levels.py", {"volume_quality": vq})
    _levels_cache[key] = levels
    return levels


def pin_record() -> dict:
    return {"repo": GEX_REPO, "commit": GEX_COMMIT,
            "files": {name: dict(pin) for name, pin in VENDORED.items()},
            "function": "levels.horizon_book (dte0 / all horizons) and its levels.position_levels"}


def _minutes(hm: str) -> int:
    h, m = hm.split(":")
    return int(h) * 60 + int(m)


def _hm(minutes: int) -> str:
    return "{:02d}:{:02d}".format(*divmod(minutes, 60))


def mark_for(t: str) -> str | None:
    """The 5-minute mark a decision at ``t`` may read: the latest label m with m <= t - QUOTE_LAG_MINUTES."""
    limit = _minutes(t) - QUOTE_LAG_MINUTES
    if limit < MARK_FIRST:
        return None
    return _hm(MARK_FIRST + (min(limit, MARK_LAST) - MARK_FIRST) // MARK_STEP * MARK_STEP)


def et(day: str, hm: str) -> datetime:
    return datetime.fromisoformat(f"{day}T{hm}").replace(tzinfo=NY)


def oi_known_at(day: str, published_at: str | None = None) -> datetime:
    """When D's open interest may be used: 07:30 ET on D, or its archive publication time when later."""
    known = et(day, _hm(OI_KNOWN_ET[0] * 60 + OI_KNOWN_ET[1]))
    if published_at:
        pub = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        known = max(known, pub)
    return known


def _bs_price(S, K, T, iv, is_call, r, q):
    sq = np.sqrt(T)
    d1 = (np.log(S / K) + (r - q + 0.5 * iv * iv) * T) / (iv * sq)
    d2 = d1 - iv * sq
    call = S * np.exp(-q * T) * ndtr(d1) - K * np.exp(-r * T) * ndtr(d2)
    put = K * np.exp(-r * T) * ndtr(-d2) - S * np.exp(-q * T) * ndtr(-d1)
    return np.where(is_call, call, put)


def implied_vols(S, K, T, mid, is_call, r, q):
    """Bisection Black-Scholes IV per contract (edge4 bounds); NaN where the mid has no IV in [IV_LO, IV_HI]."""
    K, mid, is_call = np.asarray(K, float), np.asarray(mid, float), np.asarray(is_call, bool)
    if len(K) == 0:
        return np.array([])
    lo, hi = np.full(len(K), IV_LO), np.full(len(K), IV_HI)
    ok = (mid > _bs_price(S, K, T, lo, is_call, r, q)) & (mid < _bs_price(S, K, T, hi, is_call, r, q)) & \
        np.isfinite(mid)
    for _ in range(IV_ITERS):
        m = 0.5 * (lo + hi)
        up = _bs_price(S, K, T, m, is_call, r, q) < mid
        lo, hi = np.where(up, m, lo), np.where(up, hi, m)
    return np.where(ok, 0.5 * (lo + hi), np.nan)


@dataclass
class OptionDay:
    """One session's 0DTE inputs: OI {(cp, strike): oi}, its known_at, and marks {hm: {(cp, strike): (bid, ask)}}."""
    day: str
    oi: dict
    known_at: datetime
    marks: dict = field(default_factory=dict)


def _connect(path: Path) -> sqlite3.Connection:
    """Read-only, on a frozen hash-checked copy (``immutable``: no lock or side file is created)."""
    return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


class GexHist:
    """The staged gexhist store, read-only, one session at a time; only the marks a run's decisions may read."""

    def __init__(self, path: Path, decision_times: tuple[str, ...]):
        self.path = Path(path)
        self.wanted = sorted({m for m in (mark_for(t) for t in decision_times) if m is not None})
        con = _connect(self.path)
        try:
            lo, hi, n = con.execute("select min(day), max(day), count(*) from sessions").fetchone()
        finally:
            con.close()
        if lo is None or lo < DEV_FIRST:
            raise OptionsDataError(f"{self.path} holds sessions before dev ({lo}): holdout refused")
        self.first, self.last, self.sessions = lo, hi, n

    def day(self, day: str) -> OptionDay | None:
        con = _connect(self.path)
        try:
            if con.execute("select 1 from sessions where day = ?", (day,)).fetchone() is None:
                return None
            oi = {(cp, int(k)): int(v) for cp, k, v in
                  con.execute("select cp, strike, oi from oi where day = ?", (day,))}
            marks: dict = {}
            if self.wanted:
                qs = ",".join("?" * len(self.wanted))
                for cp, k, hm, bid, ask in con.execute(
                        f"select cp, strike, hm, bid, ask from q5 where day = ? and hm in ({qs})",  # noqa: S608
                        (day, *self.wanted)):
                    marks.setdefault(hm, {})[(cp, int(k))] = (bid, ask)
        finally:
            con.close()
        return OptionDay(day, oi, oi_known_at(day), marks)


class OIArchive:
    """A frozen copy of gex-oi-capture's OCC open-interest archive (``oi.db``), read-only: per session, OI per
    strike and side summed over every expiry on or after it (the page's ALL horizon), with its publication."""

    def __init__(self, path: Path, root: str = OI_ARCHIVE_ROOT):
        self.path, self.root = Path(path), root
        con = _connect(self.path)
        try:
            rows = con.execute("select day, published_at from fetch_log where root = ? and status = 'ok' "
                               "order by day", (root,)).fetchall()
        finally:
            con.close()
        if not rows:
            raise OptionsDataError(f"{self.path} holds no ok {root} days")
        if rows[0][0] < DEV_FIRST:
            raise OptionsDataError(f"{self.path} holds days before dev ({rows[0][0]}): holdout refused")
        self.published = dict(rows)
        self.first, self.last, self.sessions = rows[0][0], rows[-1][0], len(rows)

    def day(self, day: str) -> dict | None:
        if day not in self.published:
            return None
        con = _connect(self.path)
        try:
            per = {(k / 1000.0, "call" if cp == "C" else "put"): float(v) for k, cp, v in con.execute(
                "select strike_milli, cp, sum(oi) from oi where day = ? and root = ? and expiry >= ? "
                "group by strike_milli, cp", (day, self.root, day))}
        finally:
            con.close()
        return {"known_at": oi_known_at(day, self.published[day]), "published_at": self.published[day], "oi": per}


def dte0_rows(oday: OptionDay, levels, mark: str | None, spot_at_mark: float | None) -> tuple[list[dict], int]:
    """``horizon_book`` rows for the session's own expiry: every contract with OI > 0 (its OI always counts
    toward the position levels), with an IV from ``mark``'s mid where one exists, else ``iv`` None (zero
    modeled gamma; counted, never filled). Returns (rows, contracts with an IV). Pure."""
    keys = sorted(k for k, v in oday.oi.items() if v and v > 0)
    quotes = (oday.marks.get(mark) or {}) if mark else {}
    iv = {}
    if quotes and spot_at_mark:
        seen = []
        for k in keys:
            ba = quotes.get(k)
            if not ba or ba[0] is None or ba[1] is None:
                continue
            bid, ask = float(ba[0]), float(ba[1])
            if ask <= 0 or bid < 0 or ask < bid:
                continue
            seen.append((k, 0.5 * (bid + ask)))
        if seen:
            known = et(oday.day, mark) + timedelta(minutes=1)
            T = max((et(oday.day, "16:00") - known).total_seconds() / (365.0 * 86400.0), levels.T_FLOOR)
            vols = implied_vols(spot_at_mark, [float(k[1]) for k, _ in seen], T, [m for _, m in seen],
                                [k[0] == "C" for k, _ in seen], levels.RATE, levels.DIVY)
            iv = {k: float(v) for (k, _), v in zip(seen, vols) if np.isfinite(v)}
    rows = [{"expiry": oday.day, "strike": float(k[1]), "side": "call" if k[0] == "C" else "put",
             "oi": float(oday.oi[k]), "gamma": None, "iv": iv.get(k), "volume": None, "volume_src": None,
             "src": "gexhist"} for k in keys]
    return rows, len(iv)


def archive_rows(arch: dict, day: str) -> list[dict]:
    """``horizon_book`` rows from the archive's per-strike sums (one row per strike and side, carrying the session's
    own expiry so it is unexpired at every decision). ``position_levels`` reads only per-strike OI sums, so this
    equals the contract rows; gamma is not computable (no quotes) and is never read from this book. Pure."""
    return [{"expiry": day, "strike": k, "side": side, "oi": oi, "gamma": None, "iv": None, "volume": None,
             "volume_src": None, "src": "occ_archive"} for (k, side), oi in sorted(arch["oi"].items()) if oi > 0]


def _levels_features(book: dict, spot: float, suffix: str = "") -> dict:
    pl = book["position_levels"]
    r1 = pl["ceilings"][0]["strike"] if pl["ceilings"] else None
    s1 = pl["floors"][0]["strike"] if pl["floors"] else None
    out = {f"OIR1{suffix}": None if r1 is None else (r1 - spot) / spot,
           f"OIS1{suffix}": None if s1 is None else (spot - s1) / spot}
    if not suffix:
        out["OIMID"] = (spot - s1) / (r1 - s1) if r1 is not None and s1 is not None and r1 > s1 else None
    return out


def gamma_ok(rows: list[dict], spot: float, window_pct: float) -> bool:
    """Gamma features need a valid IV on >= MIN_IV_STRIKES strikes on each side of spot within the window."""
    win = spot * window_pct / 100.0
    above = {r["strike"] for r in rows if r["iv"] and spot < r["strike"] <= spot + win}
    below = {r["strike"] for r in rows if r["iv"] and spot - win <= r["strike"] < spot}
    return len(above) >= MIN_IV_STRIKES and len(below) >= MIN_IV_STRIKES


def wall_features(book: dict, spot: float) -> dict:
    """Walls and the modeled dealer gamma from a 0DTE book, as scale-free distances and shares. Pure."""
    per = book["per_strike"]
    gross = sum(v["abs_gex_m"] for v in per)
    flip = book["gamma_flip"]
    return {
        "CWALL": (book["call_wall"] - spot) / spot,
        "PWALL": (spot - book["put_wall"]) / spot,
        "HWALL": (book["hedge_wall"] - spot) / spot,
        "FLIP": None if flip is None else (spot - flip) / spot,
        "NGEX": sum(v["net_gex_m"] for v in per) / gross if gross > 0 else None,
    }


DTE0_FEATURES = ("OIR1", "OIS1", "OIMID", "CWALL", "PWALL", "HWALL", "FLIP", "NGEX")
GAMMA_FEATURES = ("CWALL", "PWALL", "HWALL", "FLIP", "NGEX")
ARCHIVE_FEATURES = ("OIR1A", "OIS1A")
COUNTS = ("decisions", "no_session", "thin_oi", "oi_not_known", "no_mark", "book", "gamma", "archive_decisions",
          "archive_no_day", "archive_not_known", "archive_book")


class OptionsInputs:
    """The options class's inputs for :func:`analysis.module_factory.build_rows`: the vendored level book, the
    gexhist store and (optionally) the OCC archive. Reads one session at a time; keeps coverage counts."""

    def __init__(self, gexhist: GexHist | None, archive: OIArchive | None = None, levels=None):
        self.levels = levels or load_levels()
        self.gexhist, self.archive = gexhist, archive
        self._day = self._arch_day = (None, None)
        self.counts = dict.fromkeys(COUNTS, 0)
        self.marks_used: dict[str, str | None] = {}
        self.iv_coverage: list[float] = []
        self.iv_contracts: list[int] = []
        self.sessions_with_book: set[str] = set()
        self.sessions_with_archive: set[str] = set()

    def _session(self, day: str) -> OptionDay | None:
        if self._day[0] != day:
            self._day = (day, self.gexhist.day(day) if self.gexhist is not None else None)
        return self._day[1]

    def _archive(self, day: str) -> dict | None:
        if self._arch_day[0] != day:
            self._arch_day = (day, self.archive.day(day) if self.archive is not None else None)
        return self._arch_day[1]

    def features_at(self, day: str, session, t: str, decided: datetime, wanted: tuple[str, ...]) -> dict:
        """Every wanted options feature for the decision at ``t`` (``decided``) on ``day``. ``session``: the SPY
        :class:`analysis.module_factory.Session`. Reads spot from bars stamped at or before T-1, quotes from the
        mark :func:`mark_for` allows and OI only with known_at < ``decided``; a missing input gives None."""
        out = dict.fromkeys(wanted)
        bar = _hm(_minutes(t) - 1)
        spot = session.close(bar)
        if any(f in DTE0_FEATURES for f in wanted):
            out.update({k: v for k, v in self._dte0(day, session, t, decided, spot).items() if k in wanted})
        if any(f in ARCHIVE_FEATURES for f in wanted):
            out.update({k: v for k, v in self._all(day, decided, spot).items() if k in wanted})
        return out

    def _dte0(self, day, session, t, decided, spot) -> dict:
        c = self.counts
        c["decisions"] += 1
        oday = self._session(day)
        if oday is None or not spot:
            c["no_session"] += 1
            return {}
        if oday.known_at >= decided:
            c["oi_not_known"] += 1
            return {}
        open_px = session.bars.get("09:30", (None,))[0]
        strikes = {k for (_cp, k), v in oday.oi.items() if v and v > 0}
        if not open_px or sum(k > open_px for k in strikes) < MIN_OI_STRIKES or \
                sum(k < open_px for k in strikes) < MIN_OI_STRIKES:
            c["thin_oi"] += 1
            return {}
        mark = mark_for(t)
        self.marks_used[t] = mark
        if not oday.marks.get(mark):
            c["no_mark"] += 1
        rows, n_iv = dte0_rows(oday, self.levels, mark, session.close(mark) if mark else None)
        book = self.levels.horizon_book(rows, spot, decided, "dte0", day)
        if not book:
            return {}
        c["book"] += 1
        self.sessions_with_book.add(day)
        self.iv_contracts.append(n_iv)
        near = [r for r in rows if abs(r["strike"] - spot) <= spot * IV_COVERAGE_PCT / 100.0]
        total = sum(r["oi"] for r in near)
        if total > 0:
            self.iv_coverage.append(sum(r["oi"] for r in near if r["iv"]) / total)
        out = _levels_features(book, spot)
        if gamma_ok(rows, spot, self.levels.POSITION_WINDOW_PCT):
            c["gamma"] += 1
            out.update(wall_features(book, spot))
        return out

    def _all(self, day, decided, spot) -> dict:
        c = self.counts
        c["archive_decisions"] += 1
        arch = self._archive(day)
        if arch is None or not spot:
            c["archive_no_day"] += 1
            return {}
        if arch["known_at"] >= decided:
            c["archive_not_known"] += 1
            return {}
        book = self.levels.horizon_book(archive_rows(arch, day), spot, decided, "all", None)
        if not book:
            return {}
        c["archive_book"] += 1
        self.sessions_with_archive.add(day)
        return _levels_features(book, spot, suffix="A")

    def coverage(self) -> dict:
        def q(xs, p):
            return round(float(np.quantile(xs, p)), 4) if xs else None
        return {
            "levels_function": pin_record(),
            "level_rule": {"rule": self.levels.POSITION_RULE, "oi_frac": self.levels.POSITION_OI_FRAC,
                           "window_pct": self.levels.POSITION_WINDOW_PCT},
            "formula": FORMULA,
            "gexhist": None if self.gexhist is None else {
                "file": DATA_FILE, "first": self.gexhist.first, "last": self.gexhist.last,
                "sessions": self.gexhist.sessions, "marks_read": self.gexhist.wanted},
            "oi_archive": None if self.archive is None else {
                "root": self.archive.root, "first": self.archive.first, "last": self.archive.last,
                "sessions": self.archive.sessions},
            "pit": {
                "spot": "close of the bar stamped T-1",
                "quotes": f"mark m known at m+1 minute; a decision at T reads the latest mark m <= T-{QUOTE_LAG_MINUTES} "
                          "minutes; IV inverted at the spot of the bar stamped m",
                "open_interest": "D's OCC open interest only after 07:30 ET on D (published 06:30 ET; the archive's "
                                 "capture window end), or its archive published_at when later",
            },
            "counts": dict(self.counts),
            "marks_used": dict(sorted(self.marks_used.items())),
            "sessions_with_dte0_book": len(self.sessions_with_book),
            "first_dte0_book": min(self.sessions_with_book, default=None),
            "last_dte0_book": max(self.sessions_with_book, default=None),
            "sessions_with_archive_book": len(self.sessions_with_archive),
            "first_archive_book": min(self.sessions_with_archive, default=None),
            "last_archive_book": max(self.sessions_with_archive, default=None),
            "contracts_with_iv_median": statistics.median(self.iv_contracts) if self.iv_contracts else None,
            f"oi_share_with_iv_within_{IV_COVERAGE_PCT:g}pct": {"p10": q(self.iv_coverage, 0.1),
                                                                "median": q(self.iv_coverage, 0.5)},
            "thresholds": {"min_oi_strikes_each_side_of_open": MIN_OI_STRIKES,
                           "min_iv_strikes_each_side_for_gamma": MIN_IV_STRIKES},
        }


def gex_file_parity(repo: Path) -> dict:
    """{vendored file: True when a gex checkout's file at ``repo`` has the pinned bytes}."""
    return {name: hashlib.sha256((Path(repo) / pin["gex_path"]).read_bytes()).hexdigest() == pin["sha256"]
            for name, pin in VENDORED.items()}

