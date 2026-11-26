#!/usr/bin/env python3
"""Collect evidence on lapsed tickers for a person to read (read-only; proposes no entry).

``smart:tiingo`` goes PARTIAL every few days when a ticker stops trading
(KPTI, ROKU, BKH, GWHT, UTZ, FNGR in 2026-11) and stays PARTIAL until someone
marks the ticker in ``analysis/sector_map_ticker_status.yaml``. Codex HOLD
6031219314: no vendor listing may clear it on its own. Codex HOLD 6080189252:
filing metadata is a lead, not a finding about this issuer's security, so this
tool prints **candidate evidence as comments, never a sidecar entry**. The
person reads the accession(s), establishes the security, class, venue, last
session and acquirer, and writes the entry.

Per ticker it reads three independent sources:

* Tiingo's own listing (found / endDate / name): reported as a vendor claim,
  never as the last session;
* the closes GRID stored (newest TIINGO and yfinance close, date and value);
* EDGAR submissions (``data.sec.gov``): Form 25 / 25-NSE, Form 15 family and
  8-K Items 2.01 / 3.01 / 1.03, with accession numbers, and the issuer's
  current tickers.

It prints one of:

* ``LEAD``: filings of those kinds fall in a window bounded on **both** sides
  of the last stored TIINGO close (``LEAD_BEFORE_DAYS`` before to
  ``LEAD_AFTER_DAYS`` after). That is triage. It says what the tool did not
  establish: that the filing concerns this issuer's listed security (an 8-K
  Item 2.01 also covers the filer's own acquisitions and asset dispositions; a
  Form 15 alone does not show the listing stopped trading), the class, the
  venue, the last session and the acquirer. A current-symbols list that omits
  the ticker is reported the same way: it is not an old-to-new change record.
* ``STATUS NOT ESTABLISHED``: no such filing in the window, or no CIK, or no
  stored close to anchor the window. It names what was checked.

It never writes: no sidecar edit, no database write, no status change. It
fails loudly: if Tiingo, EDGAR, the SEC ticker file or the database cannot be
read, or EDGAR answers with an envelope that is not the expected shape (or
whose recent filings do not reach back to the window), that ticker gets no
block, the reason goes to stderr and the exit code is 2. Only a well-formed
EDGAR answer with no such filings counts as "none".

    python scripts/propose_ticker_status.py --tickers FNGR UTZ
    python scripts/propose_ticker_status.py --from-pull-log            # names in the latest PARTIAL
    python scripts/propose_ticker_status.py --tickers X --cik X=0001234567

Related: ``scripts/classify_tiingo_dead_tickers.py`` lists sector-map tickers
whose Tiingo ``endDate`` is stale (Tiingo meta only; it guesses ``delisted``
from that alone and swallows errors). This tool is the evidence-reading step
for the tickers a pull actually names.
"""

from __future__ import annotations

import argparse
import math
import re
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TIINGO_META_URL = "https://api.tiingo.com/tiingo/daily/{}"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{}.json"
LEAD_BEFORE_DAYS = 14         # a lead filing is filed at most this long before the last stored close
LEAD_AFTER_DAYS = 30          # ... and at most this long after it
PULL_NAMES = ("smart:tiingo", "Tiingo_Prices")
_NAMED = re.compile(r"(?:^|, )([A-Z0-9][A-Z0-9.\-=^]*)(?: \([^)]*\))?")
_RECENT_KEYS = ("form", "filingDate", "accessionNumber", "items")


class EvidenceUnavailable(RuntimeError):
    """A source could not be read (or answered in an unexpected shape): no block for the ticker."""


@dataclass(frozen=True)
class Filing:
    form: str
    filed: date
    accession: str
    items: tuple[str, ...] = ()


@dataclass
class Evidence:
    ticker: str
    tiingo_found: bool
    tiingo_name: str = ""
    tiingo_end: date | None = None
    last_tiingo_close: tuple[date, float] | None = None
    last_yfinance_close: tuple[date, float] | None = None
    cik: str | None = None
    cik_source: str = ""
    edgar_name: str = ""
    edgar_tickers: tuple[str, ...] = ()
    filings: list[Filing] = field(default_factory=list)


@dataclass
class Triage:
    ticker: str
    lead: bool
    window: tuple[date, date] | None
    filings: list[Filing] = field(default_factory=list)      # the lead filings, in the window
    symbol_lead: str = ""                                     # current-symbols observation, not a change record
    checked: list[str] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)


# ── evidence ─────────────────────────────────────────────────────────────────

HttpGet = Callable[[str, dict[str, str]], tuple[int, Any]]


def http_get(url: str, headers: dict[str, str]) -> tuple[int, Any]:
    """``(status, parsed JSON | None)``; a network failure or a non-JSON 200 is unavailable."""
    import requests

    try:
        resp = requests.get(url, headers=headers, timeout=30)
    except requests.RequestException as exc:
        raise EvidenceUnavailable(f"{url.split('?')[0]}: {type(exc).__name__}") from exc
    if resp.status_code != 200:
        return resp.status_code, None
    try:
        return 200, resp.json()
    except ValueError as exc:
        raise EvidenceUnavailable(f"{url}: HTTP 200 but not JSON") from exc


def _date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def read_tiingo(ticker: str, get: HttpGet, headers: dict[str, str]) -> tuple[bool, str, date | None]:
    """``(found, name, endDate)``. 404 is an answer ("not found"); anything else non-200 is unavailable."""
    status, meta = get(TIINGO_META_URL.format(ticker), headers)
    if status == 404:
        return False, "", None
    if status != 200 or not isinstance(meta, dict):
        raise EvidenceUnavailable(f"Tiingo listing for {ticker}: HTTP {status}")
    return True, str(meta.get("name") or "").strip(), _date(meta.get("endDate"))


def read_submissions(
    cik: str, get: HttpGet, headers: dict[str, str], since: date | None = None,
) -> tuple[str, tuple[str, ...], list[Filing]]:
    """The issuer's name, current tickers and recent filings from EDGAR submissions.

    Only the expected envelope counts as evidence: a ``filings.recent`` object with
    the four aligned arrays, a ``tickers`` list and the requested CIK. Anything else
    (a 200 with a different shape, misaligned arrays, a bad row, recent filings that
    do not reach back to ``since`` while older pages exist) is unavailable, never
    "no filings".
    """
    where = f"EDGAR submissions for CIK {cik}"
    status, doc = get(SUBMISSIONS_URL.format(cik), headers)
    if status != 200 or not isinstance(doc, dict):
        raise EvidenceUnavailable(f"{where}: HTTP {status}")
    if str(doc.get("cik") or "").zfill(10) != cik:
        raise EvidenceUnavailable(f"{where}: the answer is for CIK {doc.get('cik')!r}")
    filings = doc.get("filings")
    recent = filings.get("recent") if isinstance(filings, dict) else None
    if not isinstance(recent, dict) or not all(isinstance(recent.get(k), list) for k in _RECENT_KEYS):
        raise EvidenceUnavailable(f"{where}: no filings.recent with {', '.join(_RECENT_KEYS)} arrays")
    if len({len(recent[k]) for k in _RECENT_KEYS}) != 1:
        raise EvidenceUnavailable(f"{where}: recent filings arrays disagree in length")
    if not isinstance(doc.get("tickers"), list) or not isinstance(doc.get("name"), str):
        raise EvidenceUnavailable(f"{where}: no name / tickers")
    out: list[Filing] = []
    for form, filed, accession, items in zip(*(recent[k] for k in _RECENT_KEYS)):
        when = _date(filed)
        if when is None or not isinstance(form, str) or not isinstance(accession, str) or not isinstance(items, str):
            raise EvidenceUnavailable(f"{where}: a filing row is malformed ({form!r}, {filed!r}, {accession!r})")
        out.append(Filing(form, when, accession, tuple(x.strip() for x in items.split(",") if x.strip())))
    if since is not None and (filings.get("files") or []) and (not out or min(f.filed for f in out) > since):
        raise EvidenceUnavailable(f"{where}: recent filings start after {since.isoformat()} and older pages exist (not read)")
    return doc["name"], tuple(str(t).upper() for t in doc["tickers"]), out


def gather(
    ticker: str,
    *,
    get: HttpGet,
    tiingo_headers: dict[str, str],
    sec_headers: dict[str, str],
    cik_map: dict[str, str],
    cik_override: str | None,
    last_closes: Callable[[str], dict[str, tuple[date, float] | None]],
) -> Evidence:
    """Read every source for ``ticker``; raises ``EvidenceUnavailable`` if any cannot be read."""
    found, name, end = read_tiingo(ticker, get, tiingo_headers)
    closes = last_closes(ticker)
    ev = Evidence(ticker=ticker, tiingo_found=found, tiingo_name=name, tiingo_end=end,
                  last_tiingo_close=closes.get("TIINGO"), last_yfinance_close=closes.get("yfinance"))
    cik = cik_override or cik_map.get(ticker)
    if cik:
        ev.cik, ev.cik_source = cik.zfill(10), ("--cik" if cik_override else "SEC ticker file")
        anchor = ev.last_tiingo_close[0] if ev.last_tiingo_close else None
        try:
            since = anchor - timedelta(days=LEAD_BEFORE_DAYS) if anchor else None
        except OverflowError as exc:
            raise EvidenceUnavailable(f"stored close date {anchor!r} is out of range") from exc
        ev.edgar_name, ev.edgar_tickers, ev.filings = read_submissions(ev.cik, get, sec_headers, since)
    return ev


# ── triage ───────────────────────────────────────────────────────────────────

def _kind(f: Filing) -> str | None:
    if f.form.startswith("25"):
        return "delisting form (Form 25)"
    if f.form == "15" or f.form.startswith("15-"):
        return "deregistration form (Form 15)"
    if f.form == "8-K" and "2.01" in f.items:
        return "8-K Item 2.01 (completion of an acquisition or a disposition of assets)"
    if f.form == "8-K" and "3.01" in f.items:
        return "8-K Item 3.01 (notice of delisting or failure to satisfy a listing rule)"
    if f.form == "8-K" and "1.03" in f.items:
        return "8-K Item 1.03 (bankruptcy or receivership)"
    return None


def triage(ev: Evidence) -> Triage:
    """Leads only. Nothing here is an issuer/security outcome (see the module docstring)."""
    anchor = ev.last_tiingo_close[0] if ev.last_tiingo_close else None
    end_text = f", endDate {ev.tiingo_end.isoformat()} (a vendor claim)" if ev.tiingo_end else ", no endDate"
    checked = [
        "Tiingo listing: " + ("not found" if not ev.tiingo_found else "found" + end_text),
        "stored closes: TIINGO " + (f"{ev.last_tiingo_close[0]} at {ev.last_tiingo_close[1]:g}"
                                    if ev.last_tiingo_close else "none")
        + ", yfinance " + (f"{ev.last_yfinance_close[0]} at {ev.last_yfinance_close[1]:g}"
                           if ev.last_yfinance_close else "none"),
    ]
    missing: list[str] = []
    window = None
    leads: list[Filing] = []
    symbol_lead = ""
    if anchor is None:
        checked.append("no stored TIINGO close to anchor a filing window")
        missing.append("a last stored close, to bound the filings that could relate to it")
    else:
        window = (anchor - timedelta(days=LEAD_BEFORE_DAYS), anchor + timedelta(days=LEAD_AFTER_DAYS))
        if not ev.cik:
            checked.append("EDGAR: no CIK for this symbol in the SEC ticker file")
            missing.append("the issuer's CIK (pass --cik TICKER=CIK once the issuer is identified) and its filings")
        else:
            leads = [f for f in ev.filings if _kind(f) and window[0] <= f.filed <= window[1]]
            span = f"{window[0].isoformat()} and {window[1].isoformat()}"
            checked.append(
                f"EDGAR CIK {ev.cik} ({ev.cik_source}, {ev.edgar_name or 'unnamed'}), current tickers "
                f"{', '.join(ev.edgar_tickers) or 'none'}: "
                + (f"{len(leads)} candidate filing(s) between {span}" if leads else
                   f"no Form 25 / Form 15 / 8-K Item 2.01, 3.01 or 1.03 filed between {span}"))
            if ev.edgar_tickers and ev.ticker not in ev.edgar_tickers:
                symbol_lead = (f"EDGAR now lists {', '.join(ev.edgar_tickers)} for CIK {ev.cik}, not {ev.ticker}: "
                               "a current-symbols list, not an old-to-new listing record")
            elif ev.ticker in ev.edgar_tickers and not leads:
                checked.append(f"EDGAR still lists {ev.ticker} for this issuer: consistent with a Tiingo-only gap")
            if not leads and not symbol_lead:
                missing.append("a Form 25 / 15 or an 8-K Item 2.01 / 3.01 / 1.03 for this issuer near the last close")
    return Triage(ev.ticker, bool(leads or symbol_lead), window, leads, symbol_lead, checked, missing)


# ── output (comments only) ───────────────────────────────────────────────────

UNESTABLISHED = ("not established by this tool: that the filing(s) concern this issuer's listed security "
                 "(class, venue), the last trading session, the acquirer or a successor symbol. Read the "
                 "accession(s), then write the sidecar entry yourself.")


def _one_line(text: str) -> str:
    """Display value on one physical line: any line break or control character becomes a space."""
    return re.sub(r"[\x00-\x1f\x7f\x85\u2028\u2029]+", " ", str(text)).strip()


def comment_block(lines: list[str]) -> str:
    """Every physical line of the output starts with ``#``, whatever the values contain."""
    out: list[str] = []
    for line in lines:
        for physical in str(line).splitlines() or [""]:
            out.append(physical if physical.startswith("#") else "# " + physical)
    return "\n".join(out)


def render(t: Triage, ev: Evidence) -> str:
    return comment_block(_render_lines(t, ev))


def _render_lines(t: Triage, ev: Evidence) -> list[str]:
    if not t.lead:
        lines = [f"# {_one_line(t.ticker)}: STATUS NOT ESTABLISHED (no entry proposed)"]
        lines += [f"#   checked: {_one_line(c)}" for c in t.checked]
        lines += [f"#   would settle it: {_one_line(m)}" for m in t.missing]
        return lines
    lines = [f"# {_one_line(t.ticker)}: LEAD (triage, not a finding; no entry proposed)"]
    lines += [f"#   checked: {_one_line(c)}" for c in t.checked]
    for f in t.filings:
        items = f" (Items: {', '.join(f.items)})" if f.items else ""
        lines.append(_one_line(f"#   candidate: {_kind(f)}: Form {f.form} {f.accession} filed {f.filed.isoformat()}{items}"))
    if t.symbol_lead:
        lines.append(f"#   candidate: {_one_line(t.symbol_lead)}")
    lines.append(f"#   {UNESTABLISHED}")
    return lines


# ── inputs ───────────────────────────────────────────────────────────────────

def names_from_pull_error(message: str) -> list[str]:
    """Tickers named after "got none for it: " in a Tiingo PARTIAL message."""
    tail = message.split("got none for it:", 1)[-1].strip()
    return list(dict.fromkeys(_NAMED.findall(tail)))


def _read_only_connection(engine: Any):
    from sqlalchemy import text

    conn = engine.connect()
    conn.execute(text("SET TRANSACTION READ ONLY"))
    return conn


def db_last_closes(engine: Any) -> Callable[[str], dict[str, tuple[date, float] | None]]:
    from sqlalchemy import text

    # The newest row of each source, so a source that stopped long ago is still reported.
    sql = text(
        "SELECT DISTINCT ON (sc.name) sc.name, r.obs_date, r.value "
        "FROM raw_series r JOIN source_catalog sc ON sc.id = r.source_id "
        "WHERE r.series_id = :sid AND sc.name IN ('TIINGO', 'yfinance') AND r.pull_status = 'SUCCESS' "
        "ORDER BY sc.name, r.obs_date DESC")

    def read(ticker: str) -> dict[str, tuple[date, float] | None]:
        try:
            conn = _read_only_connection(engine)
            try:
                rows = conn.execute(sql, {"sid": f"YF:{ticker}:close"}).fetchall()
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001
            raise EvidenceUnavailable(f"stored closes for {ticker}: {type(exc).__name__}") from exc
        out: dict[str, tuple[date, float] | None] = {"TIINGO": None, "yfinance": None}
        for name, obs, value in rows:
            if out.get(name) is not None:
                continue
            when = _date(obs)
            try:
                price = float(value)
            except (TypeError, ValueError):
                price = float("nan")
            if when is None or not (date(1970, 1, 1) <= when <= date.today() + timedelta(days=1)) \
                    or not math.isfinite(price):        # noqa: DTZ011 -- calendar bound
                raise EvidenceUnavailable(f"stored close for {ticker} ({name}) is not a valid date/value: {obs!r}, {value!r}")
            out[name] = (when, price)
        return out

    return read


def latest_pull_names(engine: Any) -> list[str]:
    from sqlalchemy import text

    try:
        conn = _read_only_connection(engine)
        try:
            row = conn.execute(text(
                "SELECT error_message FROM pull_log WHERE puller_name = ANY(:n) AND status = 'PARTIAL' "
                "AND error_message LIKE '%got none for it:%' ORDER BY started_at DESC LIMIT 1"),
                {"n": list(PULL_NAMES)}).fetchone()
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        raise EvidenceUnavailable(f"pull_log: {type(exc).__name__}") from exc
    if not row or not row[0]:
        raise EvidenceUnavailable("pull_log: no PARTIAL Tiingo run naming lapsed tickers")
    return names_from_pull_error(str(row[0]))


def run(
    tickers: list[str],
    *,
    get: HttpGet,
    tiingo_headers: dict[str, str],
    sec_headers: dict[str, str],
    cik_map: dict[str, str],
    cik_overrides: dict[str, str],
    last_closes: Callable[[str], dict[str, tuple[date, float] | None]],
    out: Callable[[str], None] = print,
    err: Callable[[str], None] = lambda s: print(s, file=sys.stderr),
) -> int:
    """Print one block per ticker; exit code 2 if any ticker's evidence could not be read."""
    unavailable = leads = 0
    for ticker in tickers:
        try:
            ev = gather(ticker, get=get, tiingo_headers=tiingo_headers, sec_headers=sec_headers,
                        cik_map=cik_map, cik_override=cik_overrides.get(ticker), last_closes=last_closes)
        except EvidenceUnavailable as exc:
            unavailable += 1
            err(f"# {ticker}: EVIDENCE UNAVAILABLE, nothing reported: {exc}")
            continue
        result = triage(ev)
        leads += result.lead
        out(render(result, ev))
    out(f"# propose_ticker_status: {len(tickers)} tickers, {leads} with a lead, "
        f"{len(tickers) - leads - unavailable} not established, {unavailable} unavailable")
    if unavailable:
        err(f"propose_ticker_status: {unavailable} ticker(s) have no block because a source could not be read")
        return 2
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--tickers", nargs="+", default=[], help="lapsed tickers to review")
    p.add_argument("--from-pull-log", action="store_true", help="the tickers named by the latest PARTIAL Tiingo run")
    p.add_argument("--cik", action="append", default=[], metavar="TICKER=CIK", help="issuer CIK for a ticker")
    args = p.parse_args(argv)

    from db import get_engine
    from ingestion.altdata.sec_xbrl_financials import _SEC_HEADERS, _fetch_ticker_to_cik_map
    from ingestion.tiingo_pull import _tiingo_headers

    engine = get_engine()
    try:
        tickers = list(dict.fromkeys([t.upper() for t in args.tickers]
                                     + (latest_pull_names(engine) if args.from_pull_log else [])))
    except EvidenceUnavailable as exc:
        print(f"propose_ticker_status: {exc}", file=sys.stderr)
        return 2
    if not tickers:
        print("propose_ticker_status: no tickers (use --tickers or --from-pull-log)", file=sys.stderr)
        return 2
    overrides = {}
    for item in args.cik:
        key, _, value = item.partition("=")
        if not value.strip().isdigit():
            print(f"propose_ticker_status: bad --cik {item!r}", file=sys.stderr)
            return 2
        overrides[key.strip().upper()] = value.strip()
    cik_map = _fetch_ticker_to_cik_map()
    if not cik_map:
        print("propose_ticker_status: the SEC ticker file could not be read", file=sys.stderr)
        return 2
    return run(tickers, get=http_get, tiingo_headers=_tiingo_headers(), sec_headers=_SEC_HEADERS,
               cik_map=cik_map, cik_overrides=overrides, last_closes=db_last_closes(engine))


if __name__ == "__main__":
    sys.exit(main())
