#!/usr/bin/env python3
"""Propose sector-map lifecycle entries for lapsed tickers, with evidence (read-only).

``smart:tiingo`` goes PARTIAL every few days when a ticker stops trading
(KPTI, ROKU, BKH, GWHT, UTZ, FNGR in 2026-11) and stays PARTIAL until someone
marks the ticker in ``analysis/sector_map_ticker_status.yaml``. Codex HOLD
6031219314 settled that no vendor listing may clear the status on its own, so
the mark stays a person's decision. This script makes that decision quick and
checkable instead of automatic: for each lapsed ticker it reads three
independent kinds of evidence and prints a ready-to-review sidecar entry with
the accession numbers behind it, or says the status is **not established**.

Evidence, per ticker:

* Tiingo's own listing (``/tiingo/daily/{ticker}``: found / endDate / name);
* the closes GRID stored (last TIINGO and yfinance close date and value);
* EDGAR (``data.sec.gov/submissions``): Form 25 / 25-NSE (exchange delisting),
  Form 15 (deregistration) and 8-K Items 2.01 (completed acquisition), 3.01
  (delisting notice), 1.03 (bankruptcy), plus the entity's current tickers.

What it concludes (``classify``; no magnitude or price rule):

* **acquired**: a Form 25 or Form 15 and an 8-K Item 2.01 in the window;
* **delisted**: a Form 25 or Form 15 and no Item 2.01;
* **renamed**: EDGAR's current tickers for the issuer no longer include the
  symbol and name a successor (needs the CIK, from the SEC ticker file or
  ``--cik``);
* otherwise **status not established**, naming what was checked and what is
  missing. A Tiingo gap alone, a falling price, a missing yfinance row or no
  CIK never establishes anything.

It never writes: no sidecar edit, no database write, no status change. It
fails loudly: if Tiingo, EDGAR, the SEC ticker file or the database cannot be
read for a ticker, that ticker gets no entry (an entry with missing evidence
would look like a finding), the reason goes to stderr and the exit code is 2.
An acquirer's name is not extracted from the filing: the entry carries a
``REVIEW:`` placeholder naming the accession to read, so it cannot be
committed unreviewed by accident.

    python scripts/propose_ticker_status.py --tickers FNGR UTZ
    python scripts/propose_ticker_status.py --from-pull-log            # names in the latest PARTIAL
    python scripts/propose_ticker_status.py --tickers X --cik X=0001234567

Related: ``scripts/classify_tiingo_dead_tickers.py`` lists sector-map tickers
whose Tiingo ``endDate`` is stale (Tiingo meta only; it guesses ``delisted``
from that alone and swallows errors). This script is the evidence-based
review step for the tickers a pull actually names.
"""

from __future__ import annotations

import argparse
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
WINDOW_DAYS = 60              # filings from this many days before the last session count
PULL_NAMES = ("smart:tiingo", "Tiingo_Prices")
_NAMED = re.compile(r"(?:^|, )([A-Z0-9][A-Z0-9.\-=^]*)(?: \([^)]*\))?")

NOT_ESTABLISHED = "not established"


class EvidenceUnavailable(RuntimeError):
    """A source could not be read, so no entry is printed for the ticker."""


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
class Proposal:
    ticker: str
    status: str | None                 # acquired | delisted | renamed | None (= not established)
    last_session: date | None
    successor: str | None = None
    cited: list[str] = field(default_factory=list)       # what supports it, with accessions
    checked: list[str] = field(default_factory=list)     # what was read, for a "not established"
    missing: list[str] = field(default_factory=list)     # what would settle it


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


def read_submissions(cik: str, get: HttpGet, headers: dict[str, str]) -> tuple[str, tuple[str, ...], list[Filing]]:
    status, doc = get(SUBMISSIONS_URL.format(cik), headers)
    if status != 200 or not isinstance(doc, dict):
        raise EvidenceUnavailable(f"EDGAR submissions for CIK {cik}: HTTP {status}")
    recent = ((doc.get("filings") or {}).get("recent")) or {}
    forms, filed = recent.get("form") or [], recent.get("filingDate") or []
    accessions, items = recent.get("accessionNumber") or [], recent.get("items") or []
    if not (len(forms) == len(filed) == len(accessions)):
        raise EvidenceUnavailable(f"EDGAR submissions for CIK {cik}: recent filings arrays disagree")
    out: list[Filing] = []
    for i, form in enumerate(forms):
        when = _date(filed[i])
        if when is None:
            continue
        raw_items = items[i] if i < len(items) else ""
        out.append(Filing(str(form), when, str(accessions[i]),
                          tuple(x.strip() for x in str(raw_items).split(",") if x.strip())))
    tickers = tuple(str(t).upper() for t in (doc.get("tickers") or []))
    return str(doc.get("name") or ""), tickers, out


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
        ev.edgar_name, ev.edgar_tickers, ev.filings = read_submissions(ev.cik, get, sec_headers)
    return ev


# ── conclusion ───────────────────────────────────────────────────────────────

def _is_delisting(f: Filing) -> bool:
    return f.form.startswith("25") or f.form == "15" or f.form.startswith("15-")


def classify(ev: Evidence) -> Proposal:
    last = ev.tiingo_end or (ev.last_tiingo_close[0] if ev.last_tiingo_close else None)
    start = (last or date.today()) - timedelta(days=WINDOW_DAYS)      # noqa: DTZ011 -- calendar bound
    window = [f for f in ev.filings if f.filed >= start]
    delisting = [f for f in window if _is_delisting(f)]
    acquisition = [f for f in window if f.form == "8-K" and "2.01" in f.items]
    notice = [f for f in window if f.form == "8-K" and "3.01" in f.items]
    bankrupt = [f for f in window if f.form == "8-K" and "1.03" in f.items]

    def cite(fs: list[Filing], label: str) -> list[str]:
        return [f"{label} {f.accession} filed {f.filed.isoformat()}" for f in fs]

    cited = [f"Form {f.form} {f.accession} filed {f.filed.isoformat()}" for f in delisting]
    cited += cite(acquisition, "8-K Item 2.01")
    cited += cite(notice, "8-K Item 3.01")
    cited += cite(bankrupt, "8-K Item 1.03")

    if delisting and acquisition:
        return Proposal(ev.ticker, "acquired", last, cited=cited)
    if delisting:
        return Proposal(ev.ticker, "delisted", last, cited=cited)
    if ev.cik and ev.edgar_tickers and ev.ticker not in ev.edgar_tickers:
        return Proposal(ev.ticker, "renamed", last, successor=ev.edgar_tickers[0],
                        cited=[f"EDGAR CIK {ev.cik} ({ev.edgar_name}) now lists {', '.join(ev.edgar_tickers)}, "
                               f"not {ev.ticker}"])

    checked = [
        "Tiingo listing: " + (
            "not found" if not ev.tiingo_found
            else f"found{', endDate ' + ev.tiingo_end.isoformat() if ev.tiingo_end else ', no endDate'}"),
        "stored closes: TIINGO " + (f"{ev.last_tiingo_close[0]} at {ev.last_tiingo_close[1]:g}"
                                    if ev.last_tiingo_close else "none")
        + ", yfinance " + (f"{ev.last_yfinance_close[0]} at {ev.last_yfinance_close[1]:g}"
                           if ev.last_yfinance_close else "none"),
    ]
    missing: list[str] = []
    if ev.cik:
        checked.append(f"EDGAR CIK {ev.cik} ({ev.cik_source}, {ev.edgar_name or 'unnamed'}), tickers "
                       f"{', '.join(ev.edgar_tickers) or 'none'}: no Form 25/25-NSE/15 and no 8-K Item 2.01 "
                       f"since {start.isoformat()}")
        if notice or bankrupt:
            checked.append("; ".join(cited) + " (a notice or a bankruptcy, not a delisting)")
        if ev.ticker in ev.edgar_tickers:
            checked.append(f"EDGAR still lists {ev.ticker} for this issuer: consistent with a Tiingo-only gap")
        missing.append("a Form 25/25-NSE/15 or an 8-K Item 2.01 for this issuer")
    else:
        checked.append("EDGAR: no CIK for this symbol in the SEC ticker file")
        missing.append("the issuer's CIK (pass --cik TICKER=CIK once the issuer is identified) and its filings")
    return Proposal(ev.ticker, None, last, checked=checked, missing=missing)


# ── output ───────────────────────────────────────────────────────────────────

def _q(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', "'") + '"'


def render(p: Proposal, ev: Evidence) -> str:
    if p.status is None:
        lines = [f"# {p.ticker}: STATUS NOT ESTABLISHED (no entry proposed)"]
        lines += [f"#   checked: {c}" for c in p.checked]
        lines += [f"#   would settle it: {m}" for m in p.missing]
        return "\n".join(lines)
    note = (f"{p.status}: " + "; ".join(p.cited)
            + (f". Tiingo: {ev.tiingo_name}" if ev.tiingo_name else "")
            + ". Proposed by scripts/propose_ticker_status.py; reviewer to confirm before merging.")
    lines = [f"  {p.ticker}:", f"    status: {p.status}"]
    if p.status == "renamed":
        lines.append(f"    successor: {p.successor}")
    if p.status == "acquired":
        accessions = ", ".join(c.split()[-3] for c in p.cited if "Item 2.01" in c) or "the 8-K"
        lines.append(f"    acquirer: {_q('REVIEW: name the acquirer from 8-K ' + accessions)}")
    if p.last_session:
        lines.append(f"    last_session: {p.last_session.isoformat()}")
    lines.append(f"    note: {_q(note)}")
    return "\n".join(lines)


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
            if out.get(name) is None:
                out[name] = (_date(obs) or date.min, float(value))
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
    unavailable = 0
    established = 0
    for ticker in tickers:
        try:
            ev = gather(ticker, get=get, tiingo_headers=tiingo_headers, sec_headers=sec_headers,
                        cik_map=cik_map, cik_override=cik_overrides.get(ticker), last_closes=last_closes)
        except EvidenceUnavailable as exc:
            unavailable += 1
            err(f"# {ticker}: EVIDENCE UNAVAILABLE, no entry proposed: {exc}")
            continue
        proposal = classify(ev)
        established += proposal.status is not None
        out(render(proposal, ev))
    out(f"# propose_ticker_status: {len(tickers)} tickers, {established} proposed, "
        f"{len(tickers) - established - unavailable} not established, {unavailable} unavailable")
    if unavailable:
        err(f"propose_ticker_status: {unavailable} ticker(s) have no entry because a source could not be read")
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
