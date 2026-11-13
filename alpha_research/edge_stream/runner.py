"""One edge-stream cycle: run due looks, confirm passes, register the next proposal.

Order inside a cycle (under an exclusive lock on the data dir):

1. every open **history** registration gets its one look now; every open **forward**
   registration only counts the sessions whose bars exist (presence, never a price
   compared) and gets its look once ``n_target_sessions`` are in or ``max_sessions``
   weekdays have passed;
2. a stage-1 PASS registers its forward confirmation (stage 2, alpha 0.05/k);
3. at most one new stage-1 registration per weekday, from :data:`families.QUEUE`, then
   ``inbox/*.json`` proposals, each through :func:`sanity.check` first;
4. ``status.json`` (machine-readable state) and ``DIGEST.md`` are rewritten.

A look is a pure function of the registration and the cached bars. Its report is
written (0444) before the ``edge_look`` record; a crash between the two recomputes the
same bytes next cycle (different bytes = the registration is abandoned, never looked
twice). ``dry_run`` appends nothing and computes no outcome.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from analysis.ledger_steered_exploration import Ledger
from loguru import logger as log  # same logger config.log re-exports; no settings import

from . import ledger as L
from . import policy as P
from . import sanity
from .data import BarSource, RequestCapReached, SourceError, manifest_sha256, today_et
from .ledger import sha256_bytes
from .families import FAMILIES, QUEUE, build_proposal, code_sha256
from .stats import judge

REPORT_SCHEMA = "edge-look-report/1"
STATUS_SCHEMA = "edge-stream-status/1"
WARMUP_DAYS = 10
MAX_GAP_DAYS = 4  # prior session no further back than a long weekend


class NotReady(Exception):
    pass


class CodeChanged(Exception):
    pass


# --- the look -----------------------------------------------------------------------


def _weekdays_between(start: date, end: date) -> int:
    n, d = 0, start
    while d < end:
        n += d.weekday() < 5
        d += timedelta(days=1)
    return n


def _read(reg: dict, source: BarSource, today: date) -> tuple[dict, date, date]:
    start = date.fromisoformat(reg["window"]["start"])
    end = date.fromisoformat(reg["window"]["end"]) if reg["lane"] == "history" else today
    complete = reg["lane"] == "history"  # a history look waits for every session; forward counts what is in
    bars = {t: source.sessions(t, start - timedelta(days=WARMUP_DAYS), end, require_complete=complete)
            for t in reg["instruments"]}
    return bars, start, end


def _is_event(family) -> bool:
    return getattr(family, "kind", "session") == "event"


def _event_sources(extras: dict | None) -> tuple:
    """(daily source, event source) of a multi-day event family, or SourceError."""
    if not extras or "daily" not in extras or "events" not in extras:
        raise SourceError("daily-bar and filing sources are not configured")
    why = extras["events"].ready()
    if why:
        raise SourceError(why)
    return extras["daily"], extras["events"]


def forward_progress(reg: dict, source: BarSource, today: date, extras: dict | None = None) -> dict:
    """Sessions available to a forward registration (presence only; no outcome)."""
    family = FAMILIES[reg["family"]]
    if _is_event(family):
        daily, _ = _event_sources(extras)
        start, end = date.fromisoformat(reg["window"]["start"]), today
        days = [b[0] for b in daily.daily(family.hedge, start, end)]
    else:
        bars, start, end = _read(reg, source, today)
        days = sorted({d for t, s in bars.items() for d, b in s.items()
                       if d >= start.isoformat() and family.needed(b)})
    target = reg["window"]["n_target_sessions"]
    elapsed = _weekdays_between(start, end)
    ready = len(days) >= target or elapsed >= reg["window"]["max_sessions"]
    cut = days[:target]
    window_end = (date.fromisoformat(cut[-1]) + timedelta(days=1)).isoformat() if cut else start.isoformat()
    return {"sessions": len(days), "target": target, "weekdays_elapsed": elapsed, "ready": ready,
            "window_end": window_end}


def _event_look(reg_sha: str, reg: dict, family, today: date, extras: dict | None) -> tuple[dict, bytes]:
    """The look of a multi-day event family: trades from filings and daily bars, pooled, one trial."""
    daily, events = _event_sources(extras)
    start = date.fromisoformat(reg["window"]["start"])
    if reg["lane"] == "history":
        end = date.fromisoformat(reg["window"]["end"])
    else:
        prog = forward_progress(reg, None, today, extras)
        if not prog["ready"]:
            raise NotReady(f"{prog['sessions']}/{prog['target']} sessions")
        end = date.fromisoformat(prog["window_end"])
    got = family.collect(list(reg["instruments"]), start, end, daily, events)
    trades = got["trades"]
    result = {"trial_id": reg["trials"][0]["trial_id"], **judge(trades, reg["alpha_trial"], reg["directional"])}
    manifest = sha256_bytes(P.canonical_bytes(got["used"]))
    data = {"source": f"{type(daily).__name__} + {type(events).__name__}", "manifest_sha256": manifest,
            "window_start": start.isoformat(), "window_end": end.isoformat(),
            "sessions_in_window": sum(1 for d in got["sessions"] if d >= start.isoformat()),
            "events_skipped": dict(sorted(got["skipped"].items())),
            "instruments_left_out": dict(sorted(got["left_out"].items())),
            "resampling_unit": "ISO week of entry (the 'sessions' count of the trial is weeks)",
            "costs": "policy floor per side on four sides (stock and hedge, in and out) + short carry; "
                     "daily bars carry no bid/ask"}
    return _finish(reg_sha, reg, result, {}, data, trades)


def compute_look(reg_sha: str, reg: dict, source: BarSource, today: date,
                 extras: dict | None = None) -> tuple[dict, bytes]:
    family = FAMILIES.get(reg["family"])
    if family is None or code_sha256(family) != reg["code_sha256"]:
        raise CodeChanged(f"family {reg['family']} code differs from the registered sha")
    if _is_event(family):
        return _event_look(reg_sha, reg, family, today, extras)
    bars, start, end = _read(reg, source, today)
    window_end = end.isoformat()
    if reg["lane"] == "forward":
        prog = forward_progress(reg, source, today)
        if not prog["ready"]:
            raise NotReady(f"{prog['sessions']}/{prog['target']} sessions")
        window_end = prog["window_end"]
    trades, skipped, used = [], {}, {}
    for t in reg["instruments"]:
        days = sorted(bars[t])
        used[t] = {}
        n_skip = 0
        for i, day in enumerate(days):
            if not (start.isoformat() <= day < window_end) or i == 0:
                continue
            prev = days[i - 1]
            if (date.fromisoformat(day) - date.fromisoformat(prev)).days > MAX_GAP_DAYS:
                n_skip += 1
                continue
            used[t][prev] = bars[t][prev]
            used[t][day] = bars[t][day]
            trade = family.session_trade(bars[t][prev], bars[t][day], t, day)
            if trade is None:
                n_skip += 1
            else:
                trades.append(trade)
        skipped[t] = n_skip
    trades.sort(key=lambda x: (x["session"], x["instrument"]))
    trial = reg["trials"][0]
    result = {"trial_id": trial["trial_id"], **judge(trades, reg["alpha_trial"], reg["directional"])}
    per_instrument = {
        t: {k: v for k, v in judge([x for x in trades if x["instrument"] == t], reg["alpha_trial"],
                                   reg["directional"]).items() if k not in ("bootstrap_b", "bootstrap_seed")}
        for t in reg["instruments"]
    }
    for v in per_instrument.values():
        v["role"] = "descriptive only (the registered trial is pooled)"
    data = {"source": type(source).__name__, "manifest_sha256": manifest_sha256(used),
            "window_start": start.isoformat(), "window_end": window_end,
            "sessions_skipped": skipped,
            "costs": "policy floor per side: max(usd, bp x price); Polygon bars carry no bid/ask"}
    return _finish(reg_sha, reg, result, per_instrument, data, trades)


def _finish(reg_sha: str, reg: dict, result: dict, per_instrument: dict, data: dict,
            trades: list[dict]) -> tuple[dict, bytes]:
    """The look record and the canonical report bytes of one judged trial."""
    report = {
        "schema": REPORT_SCHEMA,
        "registration_sha256": reg_sha,
        "proposal_id": reg["proposal_id"],
        "stage": reg["stage"],
        "confirms": reg.get("confirms"),
        "family": reg["family"],
        "policy_sha256": reg["policy_sha256"],
        "code_sha256": reg["code_sha256"],
        "verdict": result["verdict"],
        "trial": result,
        "per_instrument": per_instrument,
        "data": data,
        "consumption_disclosure": [c for c in P.PRIOR_CONSUMPTION if c["group"] == reg["group"]],
        "trades": [{k: (round(v, 10) if isinstance(v, float) else v) for k, v in x.items()} for x in trades],
        "promotion_allowed": False,
    }
    body = P.canonical_bytes(report)
    look = {
        "verdict": result["verdict"],
        "trials": [{k: result.get(k) for k in ("trial_id", "verdict", "n", "n_long", "n_short", "sessions",
                                               "mean_net", "mean_long", "mean_short", "bootstrap_lower", "t_lower",
                                               "always_long_mean_net", "reasons")}],
        "data": {"manifest_sha256": data["manifest_sha256"], "window_start": data["window_start"],
                 "window_end": data["window_end"]},
        "code_sha256": reg["code_sha256"],
    }
    return look, body


# --- the cycle ----------------------------------------------------------------------


@contextlib.contextmanager
def cycle_lock(data_dir: Path):
    data_dir.mkdir(parents=True, exist_ok=True)
    with open(data_dir / "cycle.lock", "a") as fh:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("another edge-stream cycle holds the lock") from None
        yield


def _append_jsonl(path: Path, row: dict) -> None:
    with open(path, "ab") as fh:
        fh.write(P.canonical_bytes(row) + b"\n")


def _jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_bytes().splitlines() if x.strip()]


def _attempt_failed(ledger: Ledger, data_dir: Path, reg_sha: str, reg: dict, err: Exception, now: datetime,
                    dry_run: bool) -> str:
    """Record a failed fetch/evaluation. Abandon only after failures on enough distinct days.

    The cycle's request cap is not a failure of the registration and is never counted.
    """
    if dry_run:
        return f"dry run: {type(err).__name__} ({err})"
    if isinstance(err, RequestCapReached):
        return "deferred: request cap reached this cycle"
    path = data_dir / "attempts.jsonl"
    _append_jsonl(path, {"registration_sha256": reg_sha, "at": now.isoformat(),
                         "error": f"{type(err).__name__}: {err}"})
    days = {a["at"][:10] for a in _jsonl(path) if a["registration_sha256"] == reg_sha}
    if reg["lane"] == "history":
        age = (now.date() - date.fromisoformat(reg["recorded_at"][:10])).days
        give_up = len(days) >= P.FETCH_ATTEMPTS_BEFORE_ABANDON or age > P.FETCH_DAYS_BEFORE_ABANDON
    else:
        give_up = len(days) >= P.FORWARD_FAILURE_DAYS_BEFORE_ABANDON
    if give_up:
        L.append_abandoned(ledger, reg_sha, f"look failed on {len(days)} day(s); last: {type(err).__name__}: {err}")
        return "abandoned"
    return f"retry later (failed on {len(days)} day(s))"


def _write_report(data_dir: Path, reg_sha: str, body: bytes) -> tuple[Path, bool]:
    looks = data_dir / "looks"
    looks.mkdir(exist_ok=True)
    path = looks / f"{reg_sha[:16]}.json"
    if path.exists():
        return path, path.read_bytes() == body
    tmp = path.with_suffix(".tmp")
    if tmp.exists():  # a crash between chmod and replace left a read-only tmp
        tmp.unlink()
    with open(tmp, "wb") as fh:
        fh.write(body)
        fh.flush()
        os.fsync(fh.fileno())
    os.chmod(tmp, 0o444)
    os.replace(tmp, path)
    return path, True


def _inbox(data_dir: Path) -> list[dict]:
    out = []
    box = data_dir / "inbox"
    if box.is_dir():
        for f in sorted(box.glob("*.json")):
            try:
                item = json.loads(f.read_text())
                out.append({"family": item["family"], "group": item["group"],
                            "instruments": tuple(item["instruments"]), "lane": item["lane"],
                            "proposer": f"inbox:{item.get('proposer', f.stem)}"})
            except (ValueError, KeyError, TypeError) as exc:
                out.append({"invalid": f.name, "error": str(exc)})
    return out


def _proposal_day(item: dict, today: date, now: datetime) -> date:
    """History windows end at today's ET date; a forward window starts after the record's UTC date too."""
    return today if item["lane"] == "history" else max(today, now.date())


def run_cycle(ledger, source: BarSource, data_dir: str | Path, *, now: datetime | None = None,
              dry_run: bool = False, register: bool = True, extras: dict | None = None,
              only_family: str | None = None) -> dict:
    """One cycle. ``ledger`` is a Ledger, or a zero-argument opener called inside the lock
    (the CLI passes one, so a second process never works from a stale copy). ``extras``:
    ``{"daily": DailySource, "events": EventSource}`` for multi-day event families.
    ``only_family`` (dry runs only) previews that family's next queue item instead of the first."""
    if only_family and not dry_run:
        raise ValueError("only_family is a dry-run preview; a real cycle takes the queue in order")
    now = now or datetime.now(timezone.utc)
    today = today_et(now)
    data_dir = Path(data_dir)
    summary: dict = {"at": now.isoformat(), "today_et": today.isoformat(), "dry_run": dry_run,
                     "looks": [], "forward": [], "registered": [], "rejected": [], "abandoned": [], "errors": [],
                     "deferred": []}
    with cycle_lock(data_dir):
        if callable(ledger) and not isinstance(ledger, Ledger):
            ledger = ledger()
        state = L.load_state(ledger)
        # 1. looks
        for reg_sha, reg in state.open_registrations():
            try:
                if reg["lane"] == "forward":
                    prog = forward_progress(reg, source, today, extras)
                    summary["forward"].append({"registration": reg_sha[:16], **prog})
                    if not prog["ready"] or dry_run:
                        continue
                elif dry_run:
                    if _is_event(FAMILIES.get(reg["family"])):
                        summary["looks"].append({"registration": reg_sha[:16], "would_look": True})
                        continue
                    bars, start, _ = _read(reg, source, today)
                    summary["looks"].append({"registration": reg_sha[:16], "would_look": True,
                                             "sessions_cached": {t: len(s) for t, s in bars.items()}})
                    continue
                look, body = compute_look(reg_sha, reg, source, today, extras)
            except CodeChanged as exc:
                if not dry_run:
                    L.append_abandoned(ledger, reg_sha, str(exc))
                summary["abandoned"].append({"registration": reg_sha[:16], "reason": str(exc)})
                continue
            except NotReady:
                continue
            except Exception as exc:  # SourceError or a bug: never take the whole cycle down
                if not isinstance(exc, SourceError):
                    log.warning("edge_stream look {r} failed: {e}", r=reg_sha[:12], e=repr(exc))
                summary["looks"].append({"registration": reg_sha[:16],
                                         "status": _attempt_failed(ledger, data_dir, reg_sha, reg, exc, now,
                                                                   dry_run)})
                continue
            path, same = _write_report(data_dir, reg_sha, body)
            if not same:
                L.append_abandoned(ledger, reg_sha, "look recomputed to different bytes (nondeterministic)")
                summary["abandoned"].append({"registration": reg_sha[:16], "reason": "nondeterministic"})
                continue
            L.append_look(ledger, reg_sha, {**look, "report_path": str(path),
                                            "report_sha256": L.sha256_bytes(body)})
            summary["looks"].append({"registration": reg_sha[:16], "proposal_id": reg["proposal_id"],
                                     "verdict": look["verdict"]})
            log.info("edge_stream look {p}: {v}", p=reg["proposal_id"], v=look["verdict"])
        # the code's policy must be the policy in force before anything new is sealed; an open
        # registration sealed under the old one was closed above (its code hash carries the policy)
        if not dry_run:
            try:
                amended = L.amend_policy(ledger, f"code policy {P.POLICY_VERSION}", now.isoformat())
                if amended:
                    summary["policy_amended"] = {"amendment": amended[:16], "policy_sha256": P.policy_sha256()}
                    log.info("edge_stream policy amended to {v}", v=P.POLICY_VERSION)
            except L.StreamRefused as exc:
                summary["errors"].append({"policy": str(exc)})
                register = False
        state = L.load_state(ledger)
        policy_ok = dry_run or state.policy_sha256 == P.policy_sha256()
        # 2. confirmations
        for reg_sha, look in state.looks.items():
            reg = state.registrations[reg_sha]
            if reg["stage"] != 1 or look["verdict"] != "PASS" or dry_run or not policy_ok:
                continue
            if any(r.get("confirms") == reg_sha for r in state.registrations.values()):
                continue
            item = {"family": reg["family"], "group": reg["group"], "instruments": reg["instruments"],
                    "lane": "forward"}
            prop = build_proposal(item, _proposal_day(item, today, now), proposer="confirmation",
                                  confirms=reg_sha, stage=2)
            _register_or_reject(ledger, state, prop, today, now, data_dir, summary, dry_run)
            state = L.load_state(ledger)
        # 3. one new stage-1 registration a weekday
        if register and today.weekday() < 5 and state.registered_on(now.date()) < P.MAX_NEW_REGISTRATIONS_PER_DAY:
            done = {(r["family"], r["group"], r["lane"]) for r in state.registrations.values() if r["stage"] == 1}
            for item in [dict(q, proposer="template") for q in QUEUE] + _inbox(data_dir):
                if "invalid" in item:
                    summary["rejected"].append(item)
                    continue
                if (item["family"], item["group"], item["lane"]) in done or item["family"] not in FAMILIES:
                    continue
                if only_family and item["family"] != only_family:
                    continue
                if _is_event(FAMILIES[item["family"]]):
                    # sealed only when its sources can be read: a registration that could never
                    # look would be abandoned with its alpha and window spent
                    try:
                        _event_sources(extras)
                    except SourceError as exc:
                        summary["deferred"].append({"family": item["family"], "group": item["group"],
                                                    "reason": str(exc)})
                        continue
                try:
                    prop = build_proposal(item, _proposal_day(item, today, now), proposer=item["proposer"])
                except Exception as exc:  # a malformed inbox proposal
                    summary["rejected"].append({"proposal": str(item)[:200], "error": repr(exc)})
                    continue
                if _register_or_reject(ledger, state, prop, today, now, data_dir, summary, dry_run, source,
                                       extras):
                    break
        state = L.load_state(ledger)
        status = build_status(ledger, state, now, summary)
        if not dry_run:
            (data_dir / "status.json").write_bytes(P.canonical_bytes(status) + b"\n")
            (data_dir / "DIGEST.md").write_text(digest(status))
        summary["status"] = {k: status[k] for k in ("epoch", "open", "verdicts", "candidates")}
    return summary


def preflight(prop: dict, source: BarSource, today: date, extras: dict | None = None) -> dict:
    """Dry-run data check for a would-be registration: sessions whose needed bars exist.

    Presence only (no price is compared, no trade formed), so a preflight spends nothing.
    For an event family: sessions in the window, filings found, and the hour of day they
    were accepted (a check of the filing clock), with no price read beyond the session dates.
    """
    family = FAMILIES[prop["family"]]
    if _is_event(family):
        daily, events = _event_sources(extras)
        start, end = date.fromisoformat(prop["window"]["start"]), date.fromisoformat(prop["window"]["end"])
        sessions = [b[0] for b in daily.daily(family.hedge, start, end)]
        hours: dict[str, int] = {}
        n_events = left_out = 0
        for t in prop["instruments"]:
            found = events.events(t, start - timedelta(days=family.events_lookback_days), end)
            if found is None:
                left_out += 1
                continue
            for e in family.company_events(found):
                if e["filing_date"] >= start.isoformat():
                    n_events += 1
                    hours[e["accepted_et"][11:13]] = hours.get(e["accepted_et"][11:13], 0) + 1
        return {"sessions": len(sessions), "instruments": len(prop["instruments"]), "left_out": left_out,
                "events_in_window": n_events, "accepted_hour_et": dict(sorted(hours.items()))}
    bars, start, _ = _read(prop, source, today)
    return {t: sum(1 for d, b in s.items() if d >= start.isoformat() and family.needed(b)) for t, s in bars.items()}


def _register_or_reject(ledger: Ledger, state: L.StreamState, prop: dict, today: date, now: datetime,
                        data_dir: Path, summary: dict, dry_run: bool, source: BarSource | None = None,
                        extras: dict | None = None) -> bool:
    # one clock for the record: recorded_at and the budget epoch are UTC; windows are ET dates
    prop = {**prop, "recorded_at": now.isoformat(), "epoch": now.year}
    try:
        report = sanity.check(prop, state, today)
    except Exception as exc:  # a malformed proposal must not crash the cycle
        report = {"G0_wellformed": f"malformed proposal: {exc!r}"}
    if sanity.passed(report):
        prop["sanity"] = report
        if dry_run:
            row = {"would_register": prop["proposal_id"], "window": prop["window"]}
            if source is not None and prop["lane"] == "history":
                try:
                    row["preflight_sessions_with_bars"] = preflight(prop, source, today, extras)
                except SourceError as exc:
                    row["preflight_error"] = str(exc)
            summary["registered"].append(row)
            return True
        try:
            sha = L.append_registration(ledger, prop)
        except L.StreamRefused as exc:  # the ledger's own re-check refused it: a rejection, not a crash
            report = {**report, "ledger": str(exc)}
            return _reject(prop, report, today, data_dir, summary, dry_run)
        summary["registered"].append({"registration": sha[:16], "proposal_id": prop["proposal_id"]})
        log.info("edge_stream registered {p} ({s})", p=prop["proposal_id"], s=sha[:12])
        return True
    return _reject(prop, report, today, data_dir, summary, dry_run)


def _reject(prop: dict, report: dict, today: date, data_dir: Path, summary: dict, dry_run: bool) -> bool:
    failed = {k: v for k, v in report.items() if v != "ok"}
    row = {"day": today.isoformat(), "proposal_id": prop["proposal_id"], "proposer": prop["proposer"],
           "failed": failed}
    summary["rejected"].append(row)
    if not dry_run:
        path = data_dir / "rejections.jsonl"
        if row not in _jsonl(path):
            _append_jsonl(path, row)
    return False


# --- status + digest ----------------------------------------------------------------


def build_status(ledger: Ledger, state: L.StreamState, now: datetime, summary: dict | None = None) -> dict:
    regs = []
    for sha, r in state.registrations.items():
        look = state.looks.get(sha)
        regs.append({
            "registration_sha256": sha, "proposal_id": r["proposal_id"], "stage": r["stage"],
            "confirms": r.get("confirms"), "lane": r["lane"], "group": r["group"],
            "instruments": r["instruments"], "window": r["window"], "alpha_trial": r["alpha_trial"],
            "state": "looked" if look else ("abandoned" if sha in state.abandoned else "open"),
            "verdict": look["verdict"] if look else None,
            "report_sha256": look["report_sha256"] if look else None,
            "trial": look["trials"][0] if look else None,
        })
    year = now.year
    candidates = [x for x in regs if x["stage"] == 2 and x["verdict"] == "PASS"]
    return {
        "schema": STATUS_SCHEMA,
        "generated_at": now.isoformat(),
        "ledger_id": ledger.genesis["ledger_id"],
        "ledger_head_sha256": ledger.head,
        "records": len(ledger.records),
        "policy_sha256": state.policy_sha256,
        "epoch": {"year": year, "registrations": state.stage1_count(year),
                  "max": P.MAX_REGISTRATIONS_PER_YEAR, "alpha_per_look": P.ALPHA_PER_LOOK},
        "open": sum(1 for x in regs if x["state"] == "open"),
        "verdicts": {v: sum(1 for x in regs if x["verdict"] == v) for v in L.VERDICTS},
        "candidates": candidates,
        "registrations": regs,
        "consumption": state.consumption(),
        "last_cycle": summary,
        "promotion_allowed": False,
    }


def _pct(x) -> str:
    return "—" if x is None else f"{x * 1e4:+.2f} bp"


def digest(status: dict) -> str:
    e = status["epoch"]
    lines = [
        "# Edge stream digest",
        "",
        f"{status['generated_at'][:16]}Z · ledger head `{status['ledger_head_sha256'][:12]}` · "
        f"{e['year']} looks {e['registrations']}/{e['max']} (alpha {e['alpha_per_look']:g} each) · "
        f"open {status['open']} · PASS {status['verdicts']['PASS']} FAIL {status['verdicts']['FAIL']} "
        f"UNDERPOWERED {status['verdicts']['UNDERPOWERED']}",
        "",
        "Machine-readable source: `status.json` (this file is a view of it). Nothing here is a trade; "
        "a confirmed PASS is a candidate for Anik's decision only.",
        "",
        "| registration | stage | lane | window | state | n | mean net | lower bound |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for x in status["registrations"]:
        t = x["trial"] or {}
        w = x["window"]
        lines.append(f"| {x['proposal_id']} | {x['stage']} | {x['lane']} | {w['start']}..{w.get('end') or 'open'} | "
                     f"{x['verdict'] or x['state']} | {t.get('n', '—')} | {_pct(t.get('mean_net'))} | "
                     f"{_pct(t.get('bootstrap_lower'))} |")
    if status["candidates"]:
        lines += ["", "**Confirmed candidates (need Anik's go before anything trades):** "
                  + ", ".join(c["proposal_id"] for c in status["candidates"])]
    return "\n".join(lines) + "\n"
