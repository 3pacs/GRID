"""One tracker run (pre-registration §11): filings, signals, entries, marks, exits.

Everything is appended under the ``ForwardLog`` lock in one batch, after all
reads are done, so a run that fails half-way writes nothing.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from ingestion.altdata.insider_filings import _normalize_insider_name
from paper_log.trade_edge import source
from paper_log.trade_edge.config import (
    BENCHMARK,
    CANDIDATE_LOOKBACK,
    COST_BPS_ROUND_TRIP,
    EASTERN,
    HORIZONS,
    PRICE_GRACE_SESSIONS,
    PREREG_PATH,
    PREREG_SHA256,
    ST_CLOSED,
    ST_CLOSED_DELISTED,
    ST_NO_PRICE,
    ST_OPENED,
    ST_UNRESOLVED,
    VERSION,
)
from paper_log.trade_edge.events import (
    EXCL_LATE_FILING,
    build_purchases,
    cap_bucket,
    close_instant,
    compute_known_at,
    entry_session,
    group_positions,
    last_completed_session,
    line_exclusion,
    sessions_after,
    shift_sessions,
    yahoo_symbol,
)
from paper_log.trade_edge.scoreboard import build_scoreboard, cost_round_trip
from paper_log.trade_edge.sec import FetchDeferred, SubmissionError

EXCL_NO_FILING_DATE = "no_filing_date"


def _iso(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return v


def _d(v: str) -> date:
    return date.fromisoformat(str(v)[:10])


def header_record(now: datetime, code_sha: str) -> dict:
    from paper_log.trade_edge import config as c

    return {
        "kind": "header",
        "version": VERSION,
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": PREREG_SHA256,
        "rules": {
            "horizons": list(HORIZONS),
            "primary_horizon": c.PRIMARY_HORIZON,
            "large_line_usd": c.LARGE_LINE_USD,
            "cost_bps_round_trip": dict(COST_BPS_ROUND_TRIP),
            "looks": list(c.LOOKS),
            "look_t": c.LOOK_T,
            "benchmark": BENCHMARK,
            "price_source": "yfinance",
            "forward_admission": "entry close instant after this record's run_at",
        },
        "promotion_allowed": False,
    }


# ── state ───────────────────────────────────────────────────────────────────


class State:
    def __init__(self, records: list[dict]) -> None:
        self.header = records[0] if records else None
        self.filings: dict[str, dict] = {}
        self.signals: dict[str, dict] = {}
        self.first_signal_at: dict[str, str] = {}
        self.entries: dict[str, dict] = {}
        self.exits: dict[tuple[str, int], dict] = {}
        self.marks: list[dict] = []
        for r in records:
            self.add(r)

    def add(self, r: dict) -> None:
        kind = r.get("kind")
        if kind == "filing":
            self.filings[r["accession"]] = r
        elif kind == "signal":
            self.signals[r["position_id"]] = r
            self.first_signal_at.setdefault(r["position_id"], r["run_at"])
        elif kind == "entry":
            self.entries[r["position_id"]] = r
        elif kind == "exit":
            self.exits[(r["position_id"], int(r["horizon"]))] = r
        elif kind == "marks":
            self.marks.append(r)

    def mark_series(self, pid: str) -> list[tuple[date, float]]:
        out = []
        for m in self.marks:
            v = m["closes"].get(pid)
            if v:
                out.append((_d(v[0]), float(v[1])))
        return sorted(set(out))


# ── filings ─────────────────────────────────────────────────────────────────


def make_filing_record(info: dict, sub: dict | None, now: datetime, sec_error: str | None) -> dict:
    """§2.1-§2.4 for one accession: lines with their exclusion, known_at, entry."""
    if sub is not None:
        filing_date = sub["filing_date"]
        acceptance = sub["acceptance_at"]
        stype = sub["submission_type"]
        owners = sub["owners"]
        ticker = sub["ticker"] or (info.get("ticker") or "")
        issuer_cik, issuer_name = sub["issuer_cik"], sub["issuer_name"]
        lines = [dict(line) for line in sub["lines"]]
        ciks = [o["cik"] for o in owners if o.get("cik")]
        if ciks:
            actor = f"cik:{min(ciks)}"
        else:
            actor = f"name:{_normalize_insider_name(owners[0]['name']) if owners else '?'}"
        src = "sec"
    else:
        filing_date = _d(info["filing_date"]) if info.get("filing_date") else None
        acceptance = None
        stype = "4"
        names = sorted({line["insider_name"] for line in info["db_lines"] if line["insider_name"]})
        owners = [{"cik": None, "name": n} for n in names]
        ticker = info.get("ticker") or ""
        issuer_cik, issuer_name = None, ""
        lines = [{k: v for k, v in line.items() if k != "insider_name"} for line in info["db_lines"]]
        actor = f"name:{_normalize_insider_name(names[0]) if names else '?'}"
        src = "grid_db"

    first_ingest = info["first_ingest_at"]
    if filing_date is None:
        known_at, entry = None, None
        for line in lines:
            line["exclusion"] = EXCL_NO_FILING_DATE
    else:
        known_at = compute_known_at(filing_date, acceptance, first_ingest)
        entry = entry_session(known_at)
        for line in lines:
            line["exclusion"] = line_exclusion(line, filing_date, stype)
    return {
        "kind": "filing",
        "run_at": now.isoformat(),
        "accession": info["accession"],
        "source": src,
        "sec_error": sec_error,
        "issuer_cik": issuer_cik,
        "issuer_name": issuer_name,
        "ticker": ticker,
        "submission_type": stype,
        "filing_date": _iso(filing_date),
        "acceptance_at": _iso(acceptance),
        "first_ingest_at": _iso(first_ingest),
        "known_at": _iso(known_at),
        "entry_session": _iso(entry),
        "owners": owners,
        "actor": actor,
        "lines": [{k: _iso(v) for k, v in line.items()} for line in lines],
    }


# ── exits ───────────────────────────────────────────────────────────────────


def _on_or_before(series: dict[date, float], d: date) -> tuple[date, float] | None:
    keys = [k for k in series if k <= d]
    if not keys:
        return None
    k = max(keys)
    return k, series[k]


def compute_exit(entry: dict, horizon: int, adj: dict[date, float], spy: dict[date, float],
                 marks: list[tuple[date, float]], last_done: date, now: datetime) -> dict | None:
    """§4/§5 exit of one opened position at one horizon, or None to wait."""
    e = _d(entry["entry_session"])
    x = shift_sessions(e, horizon)
    if spy.get(e) is None or spy.get(x) is None:
        return None  # benchmark data not available yet: retry next run
    pe, px = adj.get(e), adj.get(x)
    if pe and px:
        status, end, ret, basis = ST_CLOSED, x, px / pe - 1.0, "adjusted"
    else:
        if sessions_after(x, last_done) < PRICE_GRACE_SESSIONS:
            return None
        status = ST_CLOSED_DELISTED
        if pe:
            later = [d for d in adj if e < d <= x]
            end = max(later) if later else e
            ret, basis = adj[end] / pe - 1.0, "adjusted_last_available"
        else:
            base = entry.get("entry_close_unadjusted")
            m = [(d, c) for d, c in marks if e < d <= x]
            if m and base:
                end, c = max(m)
                ret, basis = c / base - 1.0, "unadjusted_marks"
            else:
                end, ret, basis = e, 0.0, "entry_close_only"
    spy_end = spy.get(end)
    if spy_end is None:
        found = _on_or_before(spy, end)
        spy_end = found[1] if found else spy[e]
    spy_ret = spy_end / spy[e] - 1.0
    gross = ret - spy_ret
    cost = cost_round_trip(entry["cap_bucket"])
    return {
        "kind": "exit",
        "run_at": now.isoformat(),
        "position_id": entry["position_id"],
        "horizon": horizon,
        "status": status,
        "exit_session": x.isoformat(),
        "end_session": end.isoformat(),
        "price_basis": basis,
        "return": round(ret, 8),
        "spy_return": round(spy_ret, 8),
        "gross_excess": round(gross, 8),
        "cost_round_trip": cost,
        "net_excess": round(gross - cost, 8),
    }


# ── run ─────────────────────────────────────────────────────────────────────


def run_once(log, *, conn, now: datetime, code_sha: str, sec, prices) -> dict:
    """One run. Returns ``{"written": [...], "records": [...], "freshness": {...}}``."""
    with log.locked():
        records = log.read_all()
        new: list[dict] = []
        if not records:
            new.append(header_record(now, code_sha))
        state = State(records + new)
        genesis = datetime.fromisoformat(state.header["run_at"])

        def emit(rec: dict) -> None:
            new.append(rec)
            state.add(rec)

        freshness = source.freshness(conn)
        accs = source.group_accessions(source.candidate_rows(conn, genesis - CANDIDATE_LOOKBACK))
        counts = {"candidate_accessions": len(accs), "new_filings": 0, "sec_deferred": 0,
                  "grid_db_fallback": 0}
        for acc in sorted(accs, key=lambda a: (accs[a]["first_ingest_at"], a)):
            if acc in state.filings:
                continue
            info = accs[acc]
            try:
                sub, err = sec.read(info["filing_url"], acc), None
            except FetchDeferred:
                counts["sec_deferred"] += 1
                continue
            except SubmissionError as exc:
                sub, err = None, str(exc)
                counts["grid_db_fallback"] += 1
            emit(make_filing_record(info, sub, now, err))
            counts["new_filings"] += 1

        positions = group_positions(build_purchases(state.filings.values()))
        admitted = {pid: p for pid, p in positions.items()
                    if datetime.fromisoformat(p["entry_close_at"]) > genesis}

        # signals (§9): new positions and positions whose accession set grew
        today = now.astimezone(EASTERN).date()
        todo = [pid for pid in sorted(admitted) if pid not in state.entries and (
            pid not in state.signals or state.signals[pid]["accessions"] != admitted[pid]["accessions"])]
        need_cap = {admitted[pid]["ticker"] for pid in todo
                    if pid not in state.signals and admitted[pid]["ticker_resolved"]}
        caps = source.market_caps(conn, need_cap, today) if need_cap else {}
        for t in sorted(need_cap - set(caps)):
            mc = prices.market_cap(yahoo_symbol(t))
            if mc:
                caps[t] = {"market_cap_usd": mc, "as_of": now.isoformat(), "source": "yfinance:fast_info"}
        for pid in todo:
            p = admitted[pid]
            prev = state.signals.get(pid)
            if prev is not None:
                cap = {k: prev[k] for k in ("market_cap_usd", "cap_as_of", "cap_source", "cap_bucket")}
            else:
                c = caps.get(p["ticker"]) if p["ticker_resolved"] else None
                cap = {"market_cap_usd": c["market_cap_usd"] if c else None,
                       "cap_as_of": c["as_of"] if c else None,
                       "cap_source": c["source"] if c else None,
                       "cap_bucket": cap_bucket(c["market_cap_usd"] if c else None)}
            emit({"kind": "signal", "run_at": now.isoformat(),
                  "revision": (prev["revision"] + 1) if prev else 1, **p, **cap})

        # entries (§4)
        last_done = last_completed_session(now)
        due = [pid for pid in sorted(admitted)
               if pid not in state.entries and _d(admitted[pid]["entry_session"]) <= last_done]
        symbols = {yahoo_symbol(admitted[pid]["ticker"]) for pid in due if admitted[pid]["ticker_resolved"]}
        raw = {}
        if symbols:
            start = min(_d(admitted[pid]["entry_session"]) for pid in due) - timedelta(days=7)
            raw = prices.closes(symbols, start, last_done, adjusted=False)
        for pid in due:
            p, sig = admitted[pid], state.signals[pid]
            close = None
            if not p["ticker_resolved"]:
                status = ST_UNRESOLVED
            else:
                close = raw.get(yahoo_symbol(p["ticker"]), {}).get(_d(p["entry_session"]))
                if close:
                    status = ST_OPENED
                elif sessions_after(_d(p["entry_session"]), last_done) >= PRICE_GRACE_SESSIONS:
                    status = ST_NO_PRICE
                else:
                    continue
            first_seen = state.first_signal_at[pid]
            emit({
                "kind": "entry", "run_at": now.isoformat(), "position_id": pid, "status": status,
                "ticker": p["ticker"], "yahoo_symbol": yahoo_symbol(p["ticker"]) if p["ticker_resolved"] else None,
                "issuer_key": p["issuer_key"], "issuer_name": p["issuer_name"],
                "entry_session": p["entry_session"], "entry_close_at": p["entry_close_at"],
                "entry_close_unadjusted": close, "stratum": p["stratum"],
                "cap_bucket": sig["cap_bucket"], "market_cap_usd": sig["market_cap_usd"],
                "cap_source": sig["cap_source"], "accessions": p["accessions"], "actors": p["actors"],
                "n_actors": p["n_actors"], "n_purchases": p["n_purchases"],
                "total_value": p["total_value"], "largest_value": p["largest_value"],
                "first_known_at": p["first_known_at"], "first_signal_run_at": first_seen,
                "late_logged": datetime.fromisoformat(first_seen) >= datetime.fromisoformat(p["entry_close_at"]),
            })

        # marks (§5): once per completed session, latest unadjusted close of open positions
        open_pos = [e for pid, e in state.entries.items() if e["status"] == ST_OPENED
                    and any((pid, h) not in state.exits for h in HORIZONS)]
        last_mark = max((_d(m["session"]) for m in state.marks), default=None)
        if open_pos and (last_mark is None or last_mark < last_done):
            got = prices.closes({e["yahoo_symbol"] for e in open_pos}, last_done - timedelta(days=10),
                                last_done, adjusted=False)
            closes = {}
            for e in open_pos:
                series = {d: v for d, v in got.get(e["yahoo_symbol"], {}).items() if d >= _d(e["entry_session"])}
                found = _on_or_before(series, last_done)
                if found:
                    closes[e["position_id"]] = [found[0].isoformat(), found[1]]
            emit({"kind": "marks", "run_at": now.isoformat(), "session": last_done.isoformat(), "closes": closes})

        # exits (§4, §5)
        due_x = [(pid, h) for pid, e in sorted(state.entries.items()) if e["status"] == ST_OPENED
                 for h in HORIZONS
                 if (pid, h) not in state.exits and shift_sessions(_d(e["entry_session"]), h) <= last_done]
        if due_x:
            syms = {state.entries[pid]["yahoo_symbol"] for pid, _ in due_x} | {BENCHMARK}
            start = min(_d(state.entries[pid]["entry_session"]) for pid, _ in due_x) - timedelta(days=10)
            adj = prices.closes(syms, start, last_done, adjusted=True)
            for pid, h in due_x:
                e = state.entries[pid]
                rec = compute_exit(e, h, adj.get(e["yahoo_symbol"], {}), adj.get(BENCHMARK, {}),
                                   state.mark_series(pid), last_done, now)
                if rec is not None:
                    emit(rec)

        late_lines = sum(1 for f in state.filings.values() for line in f["lines"]
                         if line.get("exclusion") == EXCL_LATE_FILING)
        grid_db = sum(1 for f in state.filings.values() if f["source"] == "grid_db")
        board = build_scoreboard(state.entries.values(), state.exits.values(), late_lines, grid_db)
        emit({
            "kind": "run", "run_at": now.isoformat(), "code_sha": code_sha,
            "last_completed_session": last_done.isoformat(), "genesis": genesis.isoformat(),
            "counts": {**counts, "admitted_positions": len(admitted),
                       "written": {k: sum(1 for r in new if r["kind"] == k)
                                   for k in ("filing", "signal", "entry", "marks", "exit")}},
            "sec_fetches": getattr(sec, "fetches", None),
            "price_failures": list(getattr(prices, "failures", [])),
            "freshness": freshness,
            "label": board["label"]["label"],
        })
        written = log.append_locked(new)
    return {"written": written, "records": records + written, "freshness": freshness,
            "admitted": admitted, "scoreboard": board, "now": now, "last_completed_session": last_done}
