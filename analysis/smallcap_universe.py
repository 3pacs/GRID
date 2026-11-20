"""Small-cap universe: every company on Tiingo's permanent id, with a price file the cross-section factory can read.

Usage:
    python -m analysis.smallcap_universe fetch  --meta META.json --data-dir DIR [--start 2009-01-02] [--pause 0.7]
    python -m analysis.smallcap_universe build  --meta META.json --data-dir DIR
    python -m analysis.smallcap_universe counts --data-dir DIR --min-price 2 --min-dollar-volume 1e6

``META.json`` is Tiingo's company list (``/tiingo/fundamentals/meta``), saved as served. ``fetch`` reads the key
from ``TIINGO_API_KEY`` and never prints it. Reference: ``docs/reference/SMALLCAP_UNIVERSE.md``. Issue: #1210.

Why permanent ids. Tiingo reuses tickers: ``/tiingo/daily/PARA/prices`` serves the company that holds PARA today
for every past date. The same endpoint takes a permanent id (``US000000013059``) and serves that company only, so
each price file here is named by the id and a ticker is a label, never a key.

``fetch`` keeps Tiingo's files as served in ``DIR/prices``. ``build`` writes ``DIR/panel``, the directory
``analysis/cross_section_factory.py`` reads: ``prices/<id>.csv`` cut to the company's traded life (Tiingo carries an
ended company forward as flat bars with no volume, which would otherwise rank as a stock that never moves),
``members.csv`` (one row: every id with a traded bar; a stock counts on a day only when it has a bar that day),
``renames.json`` (empty) and ``sectors.csv`` (the header alone: this Tiingo plan serves no sector, industry or SEC
number outside the Dow 30, so sector by date needs another source). ``counts`` reads the panel.
"""
import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from analysis.cross_section_factory import BAR_TOL, DOLLAR_VOLUME_WINDOW

SCHEMA = "smallcap-universe/1"
PRICES_URL = "https://api.tiingo.com/tiingo/daily/{perma}/prices?startDate={start}&format=csv"
PRICE_HEADER = b"date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor"
# The two answers Tiingo serves, with status 200, for an id it holds no bars for: an empty list, or the header alone.
NO_PRICES_BODIES = (b"[]", PRICE_HEADER)
DEFAULT_START = "2009-01-02"
PANEL = "panel"          # the directory build writes for the factory, under the data directory
ENDED_GAP_SESSIONS = 5   # a company whose last bar is more than this many sessions before the store's last has ended
KINDS = ("common", "unit", "warrant", "right", "preferred", "spac", "fund")
_PERMA = re.compile(r"^[A-Z]{2}\d{12}$")
_NAME_KINDS = (   # first match wins; checked against the lower-cased name
    ("unit", re.compile(r"\bunits?\b")),
    ("warrant", re.compile(r"\bwarrants?\b|\bwts?\b")),
    ("right", re.compile(r"- rights?\b|\brights? \(|\brts\b")),
    ("preferred", re.compile(r"\bpreferred\b|\bpfd\b|\bdepositary\b")),
    ("fund", re.compile(r"\betf\b|\betn\b|\bfund\b")),
    ("spac", re.compile(r"\bacquisition\b|\bblank check\b")),
)
_TICKER_KINDS = {"u": "unit", "un": "unit", "w": "warrant", "ws": "warrant", "wt": "warrant", "r": "right", "rt": "right",
                 "p": "preferred"}


class UniverseError(ValueError):
    """The company list or the data directory is not what this module can build from."""


@dataclass(frozen=True)
class Company:
    perma: str
    ticker: str
    name: str
    active: bool
    adr: bool
    kind: str

    @property
    def included(self) -> bool:
        """Common shares of a US-listed company that is not a depositary receipt and not a blank-check shell.
        The two non-US ids in the list (prefix AU) have no price file: Tiingo answers "not found" for them."""
        return self.kind == "common" and not self.adr and self.perma.startswith("US")


def classify(name: str | None, ticker: str | None) -> str:
    """What the listing is, from its name and ticker suffix. A missing name is taken as common shares."""
    suffix = (ticker or "").lower().rpartition("-")[2] if "-" in (ticker or "") else ""
    if suffix in _TICKER_KINDS:
        return _TICKER_KINDS[suffix]
    low = (name or "").lower()
    for kind, pattern in _NAME_KINDS:
        if pattern.search(low):
            return kind
    return "common"


def load_companies(meta_path: Path) -> list[Company]:
    """Tiingo's company list as served. Refuses a list with a repeated or malformed permanent id."""
    rows = json.loads(Path(meta_path).read_text())
    if not isinstance(rows, list) or not rows:
        raise UniverseError("company list is empty or not a list")
    out, seen = [], set()
    for r in rows:
        perma = (r.get("permaTicker") or "").strip()
        if not _PERMA.match(perma):
            raise UniverseError(f"malformed permanent id: {perma!r}")
        if perma in seen:
            raise UniverseError(f"permanent id listed twice: {perma}")
        seen.add(perma)
        ticker, name = (r.get("ticker") or "").strip(), (r.get("name") or "").strip()
        out.append(Company(perma, ticker.upper(), name, bool(r.get("isActive")), bool(r.get("isADR")), classify(name, ticker)))
    return sorted(out, key=lambda c: c.perma)


def tiingo_get(url: str, tries: int = 4) -> tuple[int, bytes]:
    """One GET with the key in the Authorization header (never in the URL). Retries a busy or failing server."""
    key = os.environ.get("TIINGO_API_KEY") or os.environ.get("TIINGO_KEY")
    if not key:
        raise UniverseError("TIINGO_API_KEY is not set")
    req = urllib.request.Request(url, headers={"Authorization": f"Token {key}", "User-Agent": "grid-research"})
    for i in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504) or i == tries - 1:
                return e.code, b""
        except (urllib.error.URLError, TimeoutError):
            if i == tries - 1:
                return -1, b""
        time.sleep(5 * (i + 1))
    return -1, b""


def fetch_prices(companies: Iterable[Company], data_dir: Path, *, start: str = DEFAULT_START, pause: float = 0.7,
                 get: Callable[[str], tuple[int, bytes]] = tiingo_get, sleep: Callable[[float], None] = time.sleep) -> dict:
    """Daily bars by permanent id for every included company. Resumable: an id with a file or a no-data mark is
    skipped. A failed request leaves nothing behind, so the next run asks again."""
    data_dir = Path(data_dir)
    prices, empty = data_dir / "prices", data_dir / "no-prices"
    prices.mkdir(parents=True, exist_ok=True)
    empty.mkdir(parents=True, exist_ok=True)
    tally: Counter = Counter()
    with open(data_dir / "fetch-log.jsonl", "a") as log_fh:
        for c in companies:
            if not c.included:
                tally["not_included"] += 1
                continue
            target = prices / f"{c.perma}.csv"
            if target.exists() or (empty / c.perma).exists():
                tally["already_done"] += 1
                continue
            status, body = get(PRICES_URL.format(perma=c.perma, start=start))
            lines = body.count(b"\n") if body.startswith(b"date,") else 0
            if status == 200 and lines >= 2:
                tmp = target.with_suffix(".csv.part")
                tmp.write_bytes(body)
                tmp.replace(target)
                outcome = "prices"
            elif status == 200 and body.strip() in NO_PRICES_BODIES:
                (empty / c.perma).touch()
                outcome = "no_prices"
            else:                                  # an error, or a 200 that is neither bars nor the empty answer
                outcome = "failed"
            tally[outcome] += 1
            log_fh.write(json.dumps({"perma": c.perma, "status": status, "bytes": len(body), "outcome": outcome}) + "\n")
            log_fh.flush()
            sleep(pause)
    return dict(tally)


def qualifies(df: pd.DataFrame, min_price: float, min_dollar_volume: float) -> pd.Series:
    """The factory's ranking rule with the floors as arguments: a valid bar on day d, unadjusted close on d-1 at
    least ``min_price``, and median dollar volume over the 21 sessions ending d-1 at least ``min_dollar_volume``.
    ``df`` is one company's bars indexed by date, oldest first."""
    lo, hi = df["low"] * (1 - BAR_TOL), df["high"] * (1 + BAR_TOL)
    ok = ((df["open"] > 0) & (df["close"] > 0) & (df["high"] > 0) & (df["low"] > 0)
          & (df["open"] >= lo) & (df["open"] <= hi) & (df["close"] >= lo) & (df["close"] <= hi))
    dollar = (df["close"] * df["volume"]).rolling(DOLLAR_VOLUME_WINDOW, min_periods=10).median().shift(1)
    return ok & (df["close"].shift(1) >= min_price) & (dollar >= min_dollar_volume)


def _panel_prices(data_dir: Path) -> list[Path]:
    """The cut price files ``build`` wrote. The files as served are never ranked: they carry bars after a company ended."""
    files = sorted((Path(data_dir) / PANEL / "prices").glob("*.csv"))
    if not files:
        raise UniverseError(f"no {PANEL}/prices under {data_dir}: run build first")
    return files


def traded_life(raw: bytes) -> tuple[bytes, str, str, int, int]:
    """One price file cut to its traded life: (bytes, first traded day, last traded day, traded days, rows cut).

    Rows before the first day with volume and after the last are removed; a day without volume in between (a halt)
    stays. A file with no traded day gives empty bytes."""
    lines = raw.splitlines(keepends=True)
    if not lines or lines[0].strip() != PRICE_HEADER:
        raise UniverseError("price file does not start with Tiingo's daily header")
    rows = [ln for ln in lines[1:] if ln.strip()]
    days = [ln[:10].decode() for ln in rows]
    if days != sorted(days):
        raise UniverseError("price file is not in date order")
    traded = [i for i, ln in enumerate(rows) if float(ln.split(b",")[5]) > 0]
    if not traded:
        return b"", "", "", 0, len(rows)
    a, b = traded[0], traded[-1]
    return lines[0] + b"".join(rows[a:b + 1]), days[a], days[b], len(traded), len(rows) - (b + 1 - a)


def _bars(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, usecols=["date", "open", "high", "low", "close", "volume"])
    df["date"] = df["date"].str[:10]
    return df.drop_duplicates("date").sort_values("date").set_index("date")


def daily_counts(data_dir: Path, min_price: float, min_dollar_volume: float) -> dict[str, int]:
    """How many included companies qualify on each session, one price file in memory at a time."""
    counts: Counter = Counter()
    for p in _panel_prices(data_dir):
        q = qualifies(_bars(p), min_price, min_dollar_volume)
        counts.update(q.index[q.values])
    return dict(sorted(counts.items()))


def universe_on(data_dir: Path, day: str, min_price: float, min_dollar_volume: float) -> list[str]:
    """Permanent ids of the companies that qualify on ``day``."""
    out = []
    for p in _panel_prices(data_dir):
        q = qualifies(_bars(p), min_price, min_dollar_volume)
        if bool(q.get(day, False)):
            out.append(p.stem)
    return out


def build(meta_path: Path, data_dir: Path) -> dict:
    """Write ``companies.csv`` and ``universe-report.json``, and the factory's directory ``panel/`` (price files cut
    to each company's traded life, ``members.csv``, ``renames.json``, ``sectors.csv``), from the files on disk."""
    data_dir = Path(data_dir)
    companies = load_companies(meta_path)
    raw_files = sorted((data_dir / "prices").glob("*.csv"))
    strays = sorted({p.stem for p in raw_files} - {c.perma for c in companies if c.included})
    if strays:
        raise UniverseError(f"price files for ids the company list does not include: {strays[:5]}")
    panel = data_dir / PANEL
    (panel / "prices").mkdir(parents=True, exist_ok=True)
    spans: dict[str, tuple[str, str, int]] = {}
    seen_days: set[str] = set()
    cut_rows = cut_files = 0
    for p in raw_files:
        # Tiingo carries an ended company forward as flat bars with no volume; only traded days mark its life.
        try:
            body, first, last, n, cut = traded_life(p.read_bytes())
        except UniverseError as e:
            raise UniverseError(f"{p.name}: {e}") from e
        spans[p.stem] = (first, last, n)
        cut_rows, cut_files = cut_rows + cut, cut_files + bool(cut)
        if n:
            seen_days.update(ln[:10].decode() for ln in body.splitlines()[1:])
            tmp = panel / "prices" / f"{p.stem}.csv.part"
            tmp.write_bytes(body)
            tmp.replace(panel / "prices" / p.name)
    if not seen_days:
        raise UniverseError("no price files: run fetch first")
    for old in (panel / "prices").glob("*.csv"):       # a file an earlier build wrote for an id that no longer has bars
        if not spans.get(old.stem, ("", "", 0))[2]:
            old.unlink()
    sessions = sorted(seen_days)           # every day any company has a bar
    store_last = sessions[-1]
    recent = set(sessions[-ENDED_GAP_SESSIONS - 1:])
    ended_by_year: Counter = Counter()
    started_by_year: Counter = Counter()
    with open(data_dir / "companies.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["perma", "ticker", "name", "active", "adr", "kind", "included", "first_bar", "last_bar", "bars", "ended"])
        for c in companies:
            first, last, n = spans.get(c.perma, ("", "", 0))
            ended = bool(n) and last not in recent
            if n:
                started_by_year[first[:4]] += 1
                if ended:
                    ended_by_year[last[:4]] += 1
            w.writerow([c.perma, c.ticker, c.name, int(c.active), int(c.adr), c.kind, int(c.included), first, last, n, int(ended)])
    with open(panel / "members.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "tickers"])
        w.writerow([sessions[0], ",".join(sorted(p for p, (_, _, n) in spans.items() if n))])
    (panel / "renames.json").write_text(json.dumps({"renames": {}}) + "\n")
    (panel / "sectors.csv").write_text("Symbol,GICS Sector\n")     # the header alone: this store has no sector labels
    kinds = Counter(c.kind for c in companies)
    included = [c for c in companies if c.included]
    report = {
        "schema": SCHEMA, "store_last_bar": store_last, "companies": len(companies), "adr": sum(c.adr for c in companies),
        "kinds": dict(sorted(kinds.items())), "included": len(included), "included_with_prices": len(spans),
        "included_with_prices_never_traded": sum(1 for _, _, n in spans.values() if not n),
        "included_without_prices": len(included) - len(spans),
        "included_active_without_prices": sum(1 for c in included if c.active and c.perma not in spans),
        "ended_by_last_bar_year": dict(sorted(ended_by_year.items())),
        "first_bar_year": dict(sorted(started_by_year.items())),
        "tickers_shared_by_included_companies": sum(1 for n in Counter(c.ticker for c in included).values() if n > 1),
        "no_sector_source": True, "panel": PANEL, "price_files_cut": cut_files, "rows_cut_outside_traded_life": cut_rows,
    }
    (data_dir / "universe-report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--meta", required=True, type=Path)
    f.add_argument("--data-dir", required=True, type=Path)
    f.add_argument("--start", default=DEFAULT_START)
    f.add_argument("--pause", type=float, default=0.7)
    b = sub.add_parser("build")
    b.add_argument("--meta", required=True, type=Path)
    b.add_argument("--data-dir", required=True, type=Path)
    c = sub.add_parser("counts")
    c.add_argument("--data-dir", required=True, type=Path)
    c.add_argument("--min-price", required=True, type=float)
    c.add_argument("--min-dollar-volume", required=True, type=float)
    a = ap.parse_args(argv)
    try:
        if a.cmd == "fetch":
            out = fetch_prices(load_companies(a.meta), a.data_dir, start=a.start, pause=a.pause)
        elif a.cmd == "build":
            out = build(a.meta, a.data_dir)
        else:
            counts = daily_counts(a.data_dir, a.min_price, a.min_dollar_volume)
            last_of_year = {d[:4]: d for d in counts}
            out = {"min_price": a.min_price, "min_dollar_volume": a.min_dollar_volume,
                   "qualifying_on_last_session_of_year": {y: counts[d] for y, d in last_of_year.items()}}
    except UniverseError as e:
        print(json.dumps({"ok": False, "refused": str(e)}))
        return 2
    print(json.dumps({"ok": True, **out}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
