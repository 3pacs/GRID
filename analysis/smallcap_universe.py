"""Small-cap universe: every company on Tiingo's permanent id, with a price file the cross-section factory can read.

Usage:
    python -m analysis.smallcap_universe fetch  --meta META.json --data-dir DIR --vintage 2026-11-19 [--start 2009-01-02]
    python -m analysis.smallcap_universe bind-existing --meta META.json --data-dir DIR --start 2009-01-02 --vintage 2026-11-19
    python -m analysis.smallcap_universe build  --meta META.json --data-dir DIR [--discovery-first 2016-01-04]
    python -m analysis.smallcap_universe counts --data-dir DIR --min-price 2 --min-dollar-volume 1e6

``META.json`` is Tiingo's company list (``/tiingo/fundamentals/meta``), saved as served. ``fetch`` reads the key
from ``TIINGO_API_KEY`` and never prints it. Reference: ``docs/reference/SMALLCAP_UNIVERSE.md``. Issue: #1210.

Why permanent ids. Tiingo reuses tickers: ``/tiingo/daily/PARA/prices`` serves the company that holds PARA today
for every past date. The same endpoint takes a permanent id (``US000000013059``) and serves that company only, so
each price file here is named by the id and a ticker is a label, never a key.

``fetch`` keeps Tiingo's files as served in ``DIR/prices``. An answer is written only when it is exactly Tiingo's
daily file: the header, then rows of thirteen fields in date order inside the requested days. A pull is one start
day and one vintage (the last day asked for), recorded in ``DIR/fetch-state.json``; a resume with another start,
vintage or company list is refused, so one store never holds two pulls.

``build`` writes one panel generation, ``DIR/panel/g-<id>``, the directory ``analysis/cross_section_factory.py``
reads: ``prices/<id>.csv`` cut to the company's traded life (Tiingo carries an ended company forward as flat bars
with no volume, which would otherwise rank as a stock that never moves), ``members.csv`` (one row: every id with a
traded bar; a stock counts on a day only when it has a bar that day), ``renames.json`` (empty), ``sectors.csv`` (the
header alone: this Tiingo plan serves no sector, industry or SEC number outside the Dow 30, so sector by date needs
another source) and ``panel-manifest.json``, the factory's export manifest. A generation is built in a hidden
directory, sealed, and renamed into place; it is never edited afterwards, and ``DIR/panel/CURRENT`` names the latest.
``counts`` reads that generation through the factory's own ``universe``, so a count and a run cannot disagree.
"""
import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from analysis import cross_section_factory as xs

SCHEMA = "smallcap-universe/1"
FETCH_SCHEMA = "smallcap-fetch/1"
FETCH_STATE = "fetch-state.json"   # the one pull a data directory holds: start, vintage, company list
PRICES_URL = "https://api.tiingo.com/tiingo/daily/{perma}/prices?startDate={start}&endDate={vintage}&format=csv"
PRICE_HEADER = b"date,close,high,low,open,volume,adjClose,adjHigh,adjLow,adjOpen,adjVolume,divCash,splitFactor"
# The two answers Tiingo serves, with status 200, for an id it holds no bars for: an empty list, or the header alone.
NO_PRICES_BODIES = (b"[]", PRICE_HEADER)
DEFAULT_START = "2009-01-02"
PANEL = "panel"          # holds one directory per built generation, and CURRENT naming the latest
CURRENT = "CURRENT"
# Owner decision 2026-11-20: Tiingo is missing most companies that ended before 2016. This is a floor, not a default:
# build seals a later day when asked, never an earlier one.
DISCOVERY_FIRST = "2016-01-04"
SESSION_BARS = 300       # the factory's default: a day is a session when at least this many price files have a bar
ENDED_GAP_SESSIONS = 5   # a company whose last bar is more than this many sessions before the store's last has ended
KINDS = ("common", "unit", "warrant", "right", "preferred", "spac", "fund")
_PERMA = re.compile(r"^[A-Z]{2}\d{12}$")
_GENERATION = re.compile(r"^g-[0-9a-f]{16}$")
_ROW_DAY = re.compile(rb"^(\d{4}-\d{2}-\d{2})(T[0-9:.]{8,12}Z)?$")
# Fields that may be blank in a row Tiingo serves: the eight prices (close, high, low, open and their adjusted
# forms), by position after the date. Volume, adjusted volume, dividend and split factor may not.
_PRICE_FIELDS = (0, 1, 2, 3, 5, 6, 7, 8)
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


def _day(value: str, what: str) -> str:
    try:
        if date.fromisoformat(value).isoformat() == value:
            return value
    except (TypeError, ValueError):
        pass
    raise UniverseError(f"{what} must be a day, YYYY-MM-DD")


def price_rows(raw: bytes, start: str, vintage: str) -> list[tuple[str, float, bytes, bool]]:
    """One Tiingo daily file checked whole: (day, volume, line as served, has a blank price) per row.

    Refuses anything that is not exactly the daily file: another header, a row without thirteen fields, a date that
    is not a day, a number that does not parse or is not finite, a negative volume, days out of order or repeated,
    and a day outside ``start`` .. ``vintage``.

    One thing Tiingo does serve is a blank price: on some days without a trade the low is empty (seen in 10 of the
    9,576 files pulled 2026-11-20, 2,238 rows, always the low and the adjusted low). A blank price field is kept as
    served and reported; the factory reads it as a missing value, so that day is not a valid bar. A blank volume,
    adjusted volume, dividend or split factor is still refused."""
    lines = raw.splitlines(keepends=True)
    if not lines or lines[0].strip(b"\r\n") != PRICE_HEADER:
        raise UniverseError("does not start with Tiingo's daily header")
    rows, prev = [], ""
    for n, line in enumerate(lines[1:], start=2):
        fields = line.strip(b"\r\n").split(b",")
        m = _ROW_DAY.match(fields[0])
        if len(fields) != 13 or not m:
            raise UniverseError(f"line {n} is not a daily row of 13 fields")
        blank = [i for i in _PRICE_FIELDS if not fields[1 + i].strip()]
        try:
            day = date.fromisoformat(m.group(1).decode()).isoformat()
            numbers = [float("nan") if i in blank else float(x) for i, x in enumerate(fields[1:])]
        except ValueError as exc:
            raise UniverseError(f"line {n} holds a date or a number that cannot be read") from exc
        if not all(math.isfinite(x) for i, x in enumerate(numbers) if i not in blank) or numbers[4] < 0:
            raise UniverseError(f"line {n} holds a number that is not finite, or a negative volume")
        if day <= prev:
            raise UniverseError(f"line {n} is out of date order or repeats a day")
        if not start <= day <= vintage:
            raise UniverseError(f"line {n} is dated {day}, outside the pull {start} .. {vintage}")
        rows.append((day, numbers[4], line, bool(blank)))
        prev = day
    return rows


def _write_whole(path: Path, data: bytes) -> None:
    tmp = path.with_name(path.name + ".part")
    tmp.write_bytes(data)
    tmp.replace(path)


def fetch_state(data_dir: Path) -> dict:
    """The pull this data directory holds. Refuses a directory with none."""
    path = Path(data_dir) / FETCH_STATE
    if not path.is_file():
        raise UniverseError(f"no {FETCH_STATE} under {data_dir}: run fetch, or bind-existing for an older store")
    state = json.loads(path.read_text())
    if state.get("schema") != FETCH_SCHEMA or set(state) != {"schema", "start", "vintage", "company_list_sha256"}:
        raise UniverseError(f"{FETCH_STATE} is not a {FETCH_SCHEMA} record")
    return state


def _bind_fetch(data_dir: Path, start: str, vintage: str, company_list_sha256: str) -> dict:
    """Record the pull on a new store, or check that a resume asks for the same one."""
    data_dir = Path(data_dir)
    want = {"schema": FETCH_SCHEMA, "start": _day(start, "start"), "vintage": _day(vintage, "vintage"),
            "company_list_sha256": company_list_sha256}
    if start > vintage:
        raise UniverseError("start is after the vintage")
    if (data_dir / FETCH_STATE).is_file():
        have = fetch_state(data_dir)
        if have != want:
            differ = sorted(k for k in want if have[k] != want[k])
            raise UniverseError(f"this store holds another pull ({', '.join(differ)} differ: start {have['start']}, vintage "
                                f"{have['vintage']}); a different pull needs its own data directory")
        return have
    if any(True for sub in ("prices", "no-prices") if (data_dir / sub).is_dir() for _ in (data_dir / sub).iterdir()):
        raise UniverseError(f"this store has price files but no {FETCH_STATE}: record the start and vintage it was "
                            "pulled with using bind-existing")
    data_dir.mkdir(parents=True, exist_ok=True)
    _write_whole(data_dir / FETCH_STATE, (json.dumps(want, indent=2, sort_keys=True) + "\n").encode())
    return want


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def fetch_prices(companies: Iterable[Company], data_dir: Path, *, vintage: str, company_list_sha256: str,
                 start: str = DEFAULT_START, pause: float = 0.7, today: str | None = None,
                 get: Callable[[str], tuple[int, bytes]] = tiingo_get, sleep: Callable[[float], None] = time.sleep) -> dict:
    """Daily bars by permanent id for every included company, from ``start`` through ``vintage``.

    Resumable inside one pull: an id with a file or a no-data mark is skipped, and the start, vintage and company
    list must be the ones the store was begun with. The vintage is a finished day, so a file asked for today and
    one asked for next week hold the same rows. An answer is written only after ``price_rows`` accepts all of it;
    anything else leaves nothing behind and is asked again."""
    data_dir = Path(data_dir)
    if _day(vintage, "vintage") >= (today or datetime.now(UTC).date().isoformat()):
        raise UniverseError("the vintage must be a finished day, before today (UTC)")
    _bind_fetch(data_dir, start, vintage, company_list_sha256)
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
            status, body = get(PRICES_URL.format(perma=c.perma, start=start, vintage=vintage))
            entry = {"perma": c.perma, "status": status, "bytes": len(body)}
            if status == 200 and body.strip() in NO_PRICES_BODIES:
                (empty / c.perma).touch()
                outcome = "no_prices"
            elif status == 200:
                try:
                    price_rows(body, start, vintage)
                except UniverseError as e:         # a 200 that is not the daily file: never written, never "done"
                    outcome, entry["refused"] = "failed", str(e)
                else:
                    _write_whole(target, body)
                    outcome = "prices"
            else:
                outcome = "failed"
            tally[outcome] += 1
            log_fh.write(json.dumps({**entry, "outcome": outcome}) + "\n")
            log_fh.flush()
            sleep(pause)
    return dict(tally)


def _raw_files(data_dir: Path, companies: list[Company]) -> list[Path]:
    raw_files = sorted((Path(data_dir) / "prices").glob("*.csv"))
    strays = sorted({p.stem for p in raw_files} - {c.perma for c in companies if c.included})
    if strays:
        raise UniverseError(f"price files for ids the company list does not include: {strays[:5]}")
    return raw_files


def collection(data_dir: Path, companies: list[Company]) -> dict:
    """How the pull ended for every included company: ids with a price file, ids Tiingo confirmed it has no bars
    for (a no-prices mark), and ids with neither. An id with neither was never asked, or its request failed; that is
    not the same as "no prices", and a store holding one is not a finished pull."""
    data_dir = Path(data_dir)
    included = {c.perma for c in companies if c.included}
    with_prices = {p.stem for p in _raw_files(data_dir, companies)}
    marks = {p.name for p in (data_dir / "no-prices").iterdir()} if (data_dir / "no-prices").is_dir() else set()
    if marks - included:
        raise UniverseError(f"no-prices marks for ids the company list does not include: {sorted(marks - included)[:5]}")
    if marks & with_prices:
        raise UniverseError(f"ids with both a price file and a no-prices mark: {sorted(marks & with_prices)[:5]}")
    return {"included": len(included), "with_prices": len(with_prices), "confirmed_no_prices": len(marks),
            "unresolved": sorted(included - with_prices - marks)}


def bind_existing(meta_path: Path, data_dir: Path, *, start: str, vintage: str) -> dict:
    """Give a store pulled before ``fetch-state.json`` existed its record, without asking Tiingo for anything.

    Every price file on disk must be a whole daily file inside ``start`` .. ``vintage``; one that is not refuses the
    whole store."""
    data_dir = Path(data_dir)
    if (data_dir / FETCH_STATE).exists():
        raise UniverseError(f"this store already has {FETCH_STATE}")
    files = _raw_files(data_dir, load_companies(meta_path))
    if not files:
        raise UniverseError("no price files to bind")
    for p in files:
        try:
            price_rows(p.read_bytes(), _day(start, "start"), _day(vintage, "vintage"))
        except UniverseError as e:
            raise UniverseError(f"{p.name}: {e}") from e
    state = {"schema": FETCH_SCHEMA, "start": start, "vintage": vintage, "company_list_sha256": sha256_file(meta_path)}
    _write_whole(data_dir / FETCH_STATE, (json.dumps(state, indent=2, sort_keys=True) + "\n").encode())
    return {**state, "price_files_checked": len(files)}


def _life(header: bytes, rows: list[tuple[str, float, bytes, bool]]) -> tuple[bytes, str, str, int, int]:
    traded = [i for i, row in enumerate(rows) if row[1] > 0]
    if not traded:
        return b"", "", "", 0, len(rows)
    a, b = traded[0], traded[-1]
    return (header + b"".join(row[2] for row in rows[a:b + 1]), rows[a][0], rows[b][0], len(traded),
            len(rows) - (b + 1 - a))


def traded_life(raw: bytes, start: str = "0001-01-01", vintage: str = "9999-12-31") -> tuple[bytes, str, str, int, int]:
    """One price file cut to its traded life: (bytes, first traded day, last traded day, traded days, rows cut).

    The file is checked whole first (``price_rows``). Rows before the first day with volume and after the last are
    removed; a day without volume in between (a halt) stays. A file with no traded day gives empty bytes."""
    return _life(raw.splitlines(keepends=True)[0] if raw else b"", price_rows(raw, start, vintage))


def current_panel(data_dir: Path) -> tuple[Path, dict, dict]:
    """The latest built generation, checked: (its directory, its file table, its export record).

    Refuses a store that has not been built, and a generation whose files are not the ones its manifest lists. The
    files as served are never ranked: they carry bars after a company ended."""
    pointer = Path(data_dir) / PANEL / CURRENT
    if not pointer.is_file():
        raise UniverseError(f"no {PANEL}/{CURRENT} under {data_dir}: run build first")
    generation = pointer.read_text().strip()
    gen_dir = pointer.parent / generation
    if not _GENERATION.match(generation) or not gen_dir.is_dir():
        raise UniverseError(f"{PANEL}/{CURRENT} does not name a built generation")
    try:
        files = xs.input_files(gen_dir)
        export = xs.read_export(gen_dir, files)
    except (xs.RunFileError, FileNotFoundError) as e:
        raise UniverseError(f"generation {generation} is not a whole export: {e}") from e
    if export is None or export["generation"] != generation:
        raise UniverseError(f"generation {generation} does not carry its own export manifest")
    return gen_dir, files, export


def _universe(data_dir: Path, min_price: float, min_dollar_volume: float, min_session_bars: int) -> xs.Universe:
    gen_dir, files, _ = current_panel(data_dir)
    try:
        return xs.universe(gen_dir, min_session_bars=min_session_bars, min_price=min_price,
                           min_dollar_volume=min_dollar_volume, files=files)
    except xs.RunFileError as e:
        raise UniverseError(str(e)) from e


def daily_counts(data_dir: Path, min_price: float, min_dollar_volume: float, min_session_bars: int = SESSION_BARS) -> dict[str, int]:
    """How many companies the factory would rank on each of its sessions, at these floors.

    This is the factory's ``universe`` on the current generation, not a second copy of its rule: the store's own
    sessions, the adjusted-bar checks, and no bar after the factory's last development bar."""
    u = _universe(data_dir, min_price, min_dollar_volume, min_session_bars)
    return {d: int(n) for d, n in zip(u.sessions, u.base.sum(axis=1), strict=True)}


def universe_on(data_dir: Path, day: str, min_price: float, min_dollar_volume: float, min_session_bars: int = SESSION_BARS) -> list[str]:
    """Permanent ids of the companies the factory would rank on ``day``; empty when ``day`` is not one of its sessions."""
    u = _universe(data_dir, min_price, min_dollar_volume, min_session_bars)
    if day not in u.sessions:
        return []
    return sorted(c for c, on in zip(u.cols, u.base[u.sessions.index(day)], strict=True) if on)


def build(meta_path: Path, data_dir: Path, discovery_first: str = DISCOVERY_FIRST) -> dict:
    """Write one panel generation for the factory, and ``companies.csv`` and ``universe-report.json`` beside it.

    Refused before anything is written: a discovery start earlier than ``DISCOVERY_FIRST`` (a later one is allowed),
    and a pull that is not finished, meaning an included id with neither a price file nor a no-prices mark. Build
    never asks Tiingo for anything.

    The generation is assembled in a hidden directory, sealed with the factory's export manifest, and renamed to
    ``panel/g-<id>`` in one step. Its id is the hash of its content, so the same inputs give the same generation
    and an existing one is never written again. Nothing in a generation is edited after the rename."""
    data_dir = Path(data_dir)
    if _day(discovery_first, "discovery-first") < DISCOVERY_FIRST:
        raise UniverseError(f"discovery on this source starts no earlier than {DISCOVERY_FIRST}; {discovery_first} is refused")
    companies = load_companies(meta_path)
    raw_files = _raw_files(data_dir, companies)
    if not raw_files:
        raise UniverseError("no price files: run fetch first")
    state = fetch_state(data_dir)
    if state["company_list_sha256"] != sha256_file(meta_path):
        raise UniverseError("the company list is not the one this store was pulled with")
    pulled = collection(data_dir, companies)
    if pulled["unresolved"]:               # a failed or unasked request is not "no prices": the pull is not finished
        raise UniverseError(f"the pull is not finished: {len(pulled['unresolved'])} included id(s) have neither a price "
                            f"file nor a no-prices mark, first {pulled['unresolved'][:5]}; run fetch again")
    panel = data_dir / PANEL
    work = panel / f".building-{os.getpid()}-{time.time_ns()}"
    (work / "prices").mkdir(parents=True)
    spans: dict[str, tuple[str, str, int]] = {}
    seen_days: set[str] = set()
    cut_rows = cut_files = blank_rows = blank_files = 0
    try:
        for p in raw_files:
            # Tiingo carries an ended company forward as flat bars with no volume; only traded days mark its life.
            raw = p.read_bytes()
            try:
                rows = price_rows(raw, state["start"], state["vintage"])
            except UniverseError as e:
                raise UniverseError(f"{p.name}: {e}") from e
            body, first, last, n, cut = _life(raw.splitlines(keepends=True)[0], rows)
            blank = sum(row[3] for row in rows)
            blank_rows, blank_files = blank_rows + blank, blank_files + bool(blank)
            spans[p.stem] = (first, last, n)
            cut_rows, cut_files = cut_rows + cut, cut_files + bool(cut)
            if n:
                seen_days.update(ln[:10].decode() for ln in body.splitlines()[1:])
                (work / "prices" / p.name).write_bytes(body)
        if not seen_days:
            raise UniverseError("no price file has a traded day")
        sessions = sorted(seen_days)           # every day any company has a bar
        with open(work / "members.csv", "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["date", "tickers"])
            w.writerow([sessions[0], ",".join(sorted(p for p, (_, _, n) in spans.items() if n))])
        (work / "renames.json").write_text(json.dumps({"renames": {}}) + "\n")
        (work / "sectors.csv").write_text("Symbol,GICS Sector\n")     # the header alone: this store has no sector labels
        sealed = xs.seal_export(work, discovery_first_session=discovery_first, source={
            "provider": "tiingo", "endpoint": "daily prices by permanent id", "start": state["start"],
            "vintage": state["vintage"], "company_list_sha256": state["company_list_sha256"], "builder": SCHEMA,
            "collection": {k: pulled[k] for k in ("included", "with_prices", "confirmed_no_prices")}})
        generation = sealed["generation"]
        gen_dir = panel / generation
        unchanged = gen_dir.is_dir()
        if not unchanged:
            work.rename(gen_dir)               # one step: the generation is either wholly there or not there
            for path in sorted(gen_dir.rglob("*"), reverse=True):
                path.chmod(0o555 if path.is_dir() else 0o444)
            gen_dir.chmod(0o555)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    _write_whole(panel / CURRENT, (generation + "\n").encode())
    current_panel(data_dir)                    # the generation CURRENT now names is whole, or this build fails here

    store_last = sessions[-1]
    recent = set(sessions[-ENDED_GAP_SESSIONS - 1:])
    ended_by_year: Counter = Counter()
    started_by_year: Counter = Counter()
    with open(data_dir / "companies.csv.part", "w", newline="") as fh:
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
    (data_dir / "companies.csv.part").replace(data_dir / "companies.csv")
    kinds = Counter(c.kind for c in companies)
    included = [c for c in companies if c.included]
    report = {
        "schema": SCHEMA, "store_last_bar": store_last, "companies": len(companies), "adr": sum(c.adr for c in companies),
        "kinds": dict(sorted(kinds.items())), "included": len(included), "included_with_prices": len(spans),
        "included_with_prices_never_traded": sum(1 for _, _, n in spans.values() if not n),
        "included_without_prices": pulled["confirmed_no_prices"],   # each one a confirmed "no prices" answer
        "included_unresolved": 0,                                   # a build with any unresolved id is refused above
        "included_active_without_prices": sum(1 for c in included if c.active and c.perma not in spans),
        "ended_by_last_bar_year": dict(sorted(ended_by_year.items())),
        "first_bar_year": dict(sorted(started_by_year.items())),
        "tickers_shared_by_included_companies": sum(1 for n in Counter(c.ticker for c in included).values() if n > 1),
        "no_sector_source": True, "panel": f"{PANEL}/{generation}", "generation": generation,
        "generation_already_built": unchanged, "discovery_first_session": discovery_first,
        "fetch": {"start": state["start"], "vintage": state["vintage"]},
        "price_files_cut": cut_files, "rows_cut_outside_traded_life": cut_rows,
        "price_files_with_a_blank_price": blank_files, "rows_with_a_blank_price": blank_rows,   # as served, cut rows included
    }
    _write_whole(data_dir / "universe-report.json", (json.dumps(report, indent=2) + "\n").encode())
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch")
    f.add_argument("--meta", required=True, type=Path)
    f.add_argument("--data-dir", required=True, type=Path)
    f.add_argument("--start", default=DEFAULT_START)
    f.add_argument("--vintage", required=True, help="the last day asked for; a finished day")
    f.add_argument("--pause", type=float, default=0.7)
    e = sub.add_parser("bind-existing")
    e.add_argument("--meta", required=True, type=Path)
    e.add_argument("--data-dir", required=True, type=Path)
    e.add_argument("--start", required=True)
    e.add_argument("--vintage", required=True)
    b = sub.add_parser("build")
    b.add_argument("--meta", required=True, type=Path)
    b.add_argument("--data-dir", required=True, type=Path)
    b.add_argument("--discovery-first", default=DISCOVERY_FIRST)
    c = sub.add_parser("counts")
    c.add_argument("--data-dir", required=True, type=Path)
    c.add_argument("--min-price", required=True, type=float)
    c.add_argument("--min-dollar-volume", required=True, type=float)
    c.add_argument("--min-session-bars", type=int, default=SESSION_BARS)
    a = ap.parse_args(argv)
    try:
        if a.cmd == "fetch":
            out = fetch_prices(load_companies(a.meta), a.data_dir, start=a.start, vintage=a.vintage,
                               company_list_sha256=sha256_file(a.meta), pause=a.pause)
        elif a.cmd == "bind-existing":
            out = bind_existing(a.meta, a.data_dir, start=a.start, vintage=a.vintage)
        elif a.cmd == "build":
            out = build(a.meta, a.data_dir, a.discovery_first)
        else:
            counts = daily_counts(a.data_dir, a.min_price, a.min_dollar_volume, a.min_session_bars)
            last_of_year = {d[:4]: d for d in counts}
            out = {"min_price": a.min_price, "min_dollar_volume": a.min_dollar_volume,
                   "generation": current_panel(a.data_dir)[2]["generation"],
                   "qualifying_on_last_session_of_year": {y: counts[d] for y, d in last_of_year.items()}}
    except UniverseError as e:
        print(json.dumps({"ok": False, "refused": str(e)}))
        return 2
    print(json.dumps({"ok": True, **out}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
