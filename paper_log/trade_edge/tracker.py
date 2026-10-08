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
    PRIMARY_HORIZON,
    ST_CLOSED,
    ST_CLOSED_DELISTED,
    ST_NO_PRICE,
    ST_OPENED,
    ST_UNRESOLVED,
    STRATUM_LARGE,
    VERSION,
)
from paper_log.trade_edge.events import (
    EXCL_LATE_FILING,
    LATE_MEMBER_KIND,
    IdentityPolicyError,
    MembershipProofError,
    Witness,
    build_purchases,
    cap_bucket,
    close_instant,
    compute_known_at,
    entry_session,
    group_positions,
    last_completed_session,
    learn_aliases,
    line_exclusion,
    reconcile_positions,
    sessions_after,
    shift_sessions,
    yahoo_symbol,
)
from paper_log.trade_edge.scoreboard import (
    LOOK_KIND,
    TERMINAL_LABELS,
    LookPolicyError,
    build_scoreboard,
    cost_round_trip,
)
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
    """Replay of the journal. ``look``, ``alias`` and ``late_member`` records
    (all append-only) carry the decisions later runs must consume: a decided
    look is never re-evaluated, a ticker -> CIK association never changes and a
    late purchase of an entered slot is noted once.
    """

    def __init__(self, records: list[dict]) -> None:
        self.header = records[0] if records else None
        self.filings: dict[str, dict] = {}
        self.signals: dict[str, dict] = {}
        self.first_signal_at: dict[str, str] = {}
        self.entries: dict[str, dict] = {}
        self.exits: dict[tuple[str, int], dict] = {}
        self.marks: list[dict] = []
        self.looks: dict[int, dict] = {}
        self.aliases: dict[str, int] = {}
        self.late_members: list[dict] = []
        self.run_labels: list[str] = []
        for r in records:
            self.add(r)
        self.check_policy()

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
        elif kind == LOOK_KIND:
            self.looks[int(r["n"])] = r
        elif kind == "alias":
            self.aliases.setdefault(r["ticker"].strip().upper(), int(r["cik"]))
        elif kind == LATE_MEMBER_KIND:
            self.late_members.append(r)
        elif kind == "run" and r.get("label") is not None:
            self.run_labels.append(r["label"])

    def check_policy(self) -> None:
        """Refuse a journal whose recorded label was decided without a look record.

        A ``run`` record labelled SUPPORTED/CONTRARY/NOT_SUPPORTED with no
        terminal ``look`` record was written under the earlier re-selection
        rule; this code neither rewrites that history nor re-decides it. The
        boundary is an owner decision (a new v3 log), so the run refuses.
        """
        terminal_runs = [lab for lab in self.run_labels if lab in TERMINAL_LABELS]
        if terminal_runs and not any(rec.get("terminal") for rec in self.looks.values()):
            raise LookPolicyError(
                f"journal run records carry the terminal label {terminal_runs[-1]!r} but hold no look record; "
                "it was decided under the pre-look rule. Refusing to re-decide or rewrite it: starting a new "
                "log (v3) is an owner decision")

    def pending_primary_exit_sessions(self) -> list[str]:
        """Exit sessions of opened primary-stratum positions whose primary-horizon exit is not recorded."""
        return [shift_sessions(_d(e["entry_session"]), PRIMARY_HORIZON).isoformat()
                for pid, e in self.entries.items()
                if e["status"] == ST_OPENED and e["stratum"] == STRATUM_LARGE
                and (pid, PRIMARY_HORIZON) not in self.exits]

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
        # §4 fallback chain: the last usable close from the exit fetch, else the
        # tracker's own daily marks (unadjusted, against the recorded unadjusted
        # entry close), else the entry close (0%). An exit fetch that holds the
        # entry close but no later close is "no usable post-entry close", not a
        # 0% return: the retained marks decide before entry/0% does.
        status = ST_CLOSED_DELISTED
        later = [d for d in adj if e < d <= x and adj[d]]
        base = entry.get("entry_close_unadjusted")
        m = [(d, c) for d, c in marks if e < d <= x]
        if pe and later:
            end = max(later)
            ret, basis = adj[end] / pe - 1.0, "adjusted_last_available"
        elif m and base:
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


# ── entries and their proof ────────────────────────────────────────────────


def entry_record(pid: str, p: dict, sig: dict, first_seen: str, status: str, close: float | None,
                 run_at: str) -> dict:
    """The ``entry`` record of position ``p``: identity and membership frozen from
    ``p``, the cap observation from its latest signal ``sig``, the price
    observation (``status``, ``close``) as fetched. :func:`prove_membership`
    checks a journal entry against this projection of its own prefix witness."""
    return {
        "kind": "entry", "run_at": run_at, "position_id": pid, "status": status,
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
    }


# Within one run the tracker appends filings, then aliases, then late notes, then
# signals, then entries (then marks, exits, looks and the run record).
_STAGE = {"filing": 0, "alias": 1, LATE_MEMBER_KIND: 2, "signal": 3, "entry": 4}
_CAP_FIELDS = ("market_cap_usd", "cap_as_of", "cap_source", "cap_bucket")
_SIGNAL_META = frozenset({"kind", "run_at", "revision", "prev_sha256", *_CAP_FIELDS})
_ABSENT = object()


def _mismatch(got: dict, want: dict, skip: frozenset = frozenset()) -> list[str]:
    """Fields (other than ``skip``) whose values differ, a missing field included; exact, no tolerance."""
    return sorted(k for k in (set(got) | set(want)) - skip if got.get(k, _ABSENT) != want.get(k, _ABSENT))


def _unproved(where: str, reason: str) -> MembershipProofError:
    return MembershipProofError(
        f"{where}: {reason}. Refusing this journal before any append: its original membership is not proved "
        "by the ordered records before it (no reset, rewrite, backdating or provider inference; continuing "
        "an unproved history is an owner decision)")


def _prefix_reconciliation(i: int, run_at: Any, filings: dict[str, dict], aliases: dict[str, int],
                           witness: Witness) -> tuple[dict[str, dict], list[dict], dict[str, list[tuple]]]:
    """The run's reconciliation, recomputed from the complete ordered prefix before journal record ``i``:
    the filings and alias records then present and the membership proved by earlier runs."""
    where = f"prefix before journal record {i} (run_at {run_at})"
    taught = learn_aliases(filings.values(), aliases)
    if taught:
        raise _unproved(where, f"its filings teach the association {taught[0]} but no alias record holds it")
    positions = group_positions(build_purchases(filings.values(), aliases))
    try:
        out, _, late, members = reconcile_positions(positions, filings, witness, aliases)
    except IdentityPolicyError as exc:
        raise _unproved(where, f"the registered reconciliation of this prefix refuses ({exc})") from exc
    return out, late, members


def prove_membership(records: list[dict]) -> Witness:
    """Prove every recorded membership from the journal's complete ordered prefixes.

    Walks ``records`` (``ForwardLog.read_all()`` order; never sorted, never the
    final :class:`State` dictionaries) keeping only what each prefix held:
    filings (an accession journaled twice refuses: an overwrite cannot be
    proved), aliases (each must be the next ticker -> CIK association its
    prefix's filings teach, first association wins) and the membership proved
    so far. At the first ``late_member``/``signal``/``entry`` record of a run
    (records sharing a ``run_at``, in the tracker's append order) it recomputes
    that run's reconciliation from the prefix before it: exact de-duplicated
    purchases, registered grouping, the folding justified by earlier proved
    owners (:func:`~paper_log.trade_edge.events.reconcile_positions`). Then:

    * a ``late_member`` must be a note that reconciliation produces, once;
    * a ``signal`` must equal its reconciled position field for field, be the
      next revision, revise only on an accession change, keep its first cap
      observation and not follow the position's entry;
    * an ``entry`` needs its own witness, not its signal's: it must equal
      :func:`entry_record` of its own reconciled position with the cap of the
      latest signal and the first-signal time before it, be due at its
      ``run_at``, and carry a status the entry rule gives.

    The proved records' purchase keys (a signal's latest revision, an entry's
    frozen set) and late notes form the returned witness. Anything else --
    a missing filing, an unjournaled alias, a fold the registered rules do not
    give, an inconsistent overwrite -- raises :class:`MembershipProofError`
    naming the record (0-based index) or prefix and the reason.
    """
    witness = Witness()
    filings: dict[str, dict] = {}
    aliases: dict[str, int] = {}
    filed_at: dict[str, int] = {}
    signals: dict[str, dict] = {}
    first_signal_at: dict[str, str] = {}
    entered_at: dict[str, int] = {}
    genesis = (datetime.fromisoformat(records[0]["run_at"])
               if records and records[0].get("kind") == "header" else None)
    run_at: Any = _ABSENT
    stage, batch, noted = -1, None, []
    for i, r in enumerate(records):
        kind = r.get("kind")
        if kind not in _STAGE:
            continue
        where = (f"journal record {i} ({kind} {r.get('position_id') or r.get('accession') or r.get('ticker')}, "
                 f"run_at {r.get('run_at')})")
        if genesis is None:
            raise _unproved(where, "no journal header precedes it")
        if not isinstance(r.get("run_at"), str):
            raise _unproved(where, "it carries no run_at")
        if r["run_at"] != run_at:
            run_at, stage, batch, noted = r["run_at"], -1, None, []
        if _STAGE[kind] < stage:
            raise _unproved(where, "it follows a later-stage record of its run (the tracker appends filings, "
                                   "aliases, late notes, signals, entries in that order)")
        stage = _STAGE[kind]
        if kind == "filing":
            if r.get("accession") in filed_at:
                raise _unproved(where, f"it repeats the accession of journal record {filed_at[r.get('accession')]}")
            filed_at[r["accession"]] = i
            filings[r["accession"]] = r
            continue
        if kind == "alias":
            got = {"ticker": r.get("ticker"), "cik": r.get("cik"), "accession": r.get("accession")}
            taught = learn_aliases(filings.values(), aliases)
            if not taught or taught[0] != got:
                raise _unproved(where, f"it is not the next ticker -> CIK association its prefix's filings "
                                       f"teach ({taught[0] if taught else 'none'})")
            aliases[got["ticker"]] = got["cik"]
            continue
        if batch is None:
            batch = _prefix_reconciliation(i, run_at, filings, aliases, witness)
        out, late, members = batch
        pid = r.get("position_id")
        if kind == LATE_MEMBER_KIND:
            note = {k: v for k, v in r.items() if k not in ("kind", "run_at", "prev_sha256")}
            if note not in late or note in noted:
                raise _unproved(where, "it is not a late note the registered reconciliation of its prefix "
                                       "produces (or it repeats one)")
            noted.append(note)
            witness.late.append(r)
            continue
        if pid in entered_at:
            raise _unproved(where, f"{pid!r} was entered at journal record {entered_at[pid]}; an entry is frozen")
        p = out.get(pid)
        if p is None:
            raise _unproved(where, "the registered reconciliation of its prefix holds no such position")
        if datetime.fromisoformat(p["entry_close_at"]) <= genesis:
            raise _unproved(where, "its entry close is not after the journal's genesis, so it was never admitted")
        prev = signals.get(pid)
        if kind == "signal":
            diff = _mismatch({k: v for k, v in r.items() if k not in _SIGNAL_META}, p)
            if diff:
                raise _unproved(where, f"it does not match the registered reconciliation of its prefix on {diff}")
            if r.get("revision") != (prev["revision"] + 1 if prev else 1):
                raise _unproved(where, f"revision {r.get('revision')!r} is not the next revision")
            if prev is not None and prev["accessions"] == r["accessions"]:
                raise _unproved(where, "it revises the signal without an accession change")
            if prev is not None and any(r.get(k) != prev.get(k) for k in _CAP_FIELDS):
                raise _unproved(where, "it changes the cap observation of the first signal")
            if r.get("cap_bucket") != cap_bucket(r.get("market_cap_usd")):
                raise _unproved(where, "its cap bucket is not the one its market cap gives")
            signals[pid] = r
            first_signal_at.setdefault(pid, r["run_at"])
        else:
            if prev is None:
                raise _unproved(where, "no signal of this position precedes it")
            status, close = r.get("status"), r.get("entry_close_unadjusted")
            want = entry_record(pid, p, prev, first_signal_at[pid], status, close, r["run_at"])
            diff = _mismatch(r, want, frozenset({"prev_sha256"}))
            if diff:
                raise _unproved(where, f"it does not match its own prefix witness on {diff}")
            last_done = last_completed_session(datetime.fromisoformat(r["run_at"]))
            if _d(p["entry_session"]) > last_done:
                raise _unproved(where, f"its session {p['entry_session']} had not completed at its run_at")
            if not p["ticker_resolved"]:
                rule = status == ST_UNRESOLVED and close is None
            elif status == ST_OPENED:
                rule = bool(close)
            else:
                rule = (status == ST_NO_PRICE and close is None
                        and sessions_after(_d(p["entry_session"]), last_done) >= PRICE_GRACE_SESSIONS)
            if not rule:
                raise _unproved(where, f"status {status!r} with entry close {close!r} is not one the entry rule gives")
            entered_at[pid] = i
        witness.recorded[pid] = r
        witness.members[pid] = members[pid]
    return witness


# ── run ─────────────────────────────────────────────────────────────────────


def run_once(log, *, conn, now: datetime, code_sha: str, sec, prices) -> dict:
    """One run. Returns ``{"written": [...], "records": [...], "freshness": {...}}``."""
    with log.locked():
        records = log.read_all()
        new: list[dict] = []
        if not records:
            new.append(header_record(now, code_sha))
        state = State(records + new)
        # every recorded membership is proved from the journal's ordered prefixes before anything is
        # fetched or appended; an unproved history refuses here (MembershipProofError)
        witness = prove_membership(records)
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

        # issuer identity (§2.3): ticker -> CIK associations are learned once and
        # appended; a position recorded before its ticker was enriched keeps its id.
        # Ownership is exact purchase membership (proved for recorded positions); a
        # purchase that joins an already entered slot is noted once, stays bound to
        # that slot and is never scored.
        for assoc in learn_aliases(state.filings.values(), state.aliases):
            emit({"kind": "alias", "run_at": now.isoformat(), **assoc})
        positions = group_positions(build_purchases(state.filings.values(), state.aliases))
        positions, reconciled, late, _ = reconcile_positions(positions, state.filings, witness, state.aliases)
        counts["identity_reconciled"] = len(reconciled)
        for note in late:
            emit({"kind": LATE_MEMBER_KIND, "run_at": now.isoformat(), **note})
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
            emit(entry_record(pid, p, sig, state.first_signal_at[pid], status, close, now.isoformat()))

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
        # label (§10): decided once per complete look, appended as ``look`` records
        board = build_scoreboard(state.entries.values(), state.exits.values(), late_lines, grid_db,
                                 looks=state.looks, pending_exit_sessions=state.pending_primary_exit_sessions(),
                                 pending_filings=counts["sec_deferred"], decide=True)
        for rec in board["new_looks"]:
            emit({"kind": LOOK_KIND, "run_at": now.isoformat(), **rec})
        emit({
            "kind": "run", "run_at": now.isoformat(), "code_sha": code_sha,
            "last_completed_session": last_done.isoformat(), "genesis": genesis.isoformat(),
            "counts": {**counts, "admitted_positions": len(admitted),
                       "written": {k: sum(1 for r in new if r["kind"] == k)
                                   for k in ("filing", "signal", "entry", "marks", "exit", LOOK_KIND, "alias",
                                             LATE_MEMBER_KIND)}},
            "sec_fetches": getattr(sec, "fetches", None),
            "price_failures": list(getattr(prices, "failures", [])),
            "freshness": freshness,
            "label": board["label"]["label"],
            "looks_done": board["label"]["looks_done"],
            "look_pending": board["label"]["look_pending"],
        })
        written = log.append_locked(new)
    return {"written": written, "records": records + written, "freshness": freshness,
            "admitted": admitted, "scoreboard": board, "now": now, "last_completed_session": last_done}
