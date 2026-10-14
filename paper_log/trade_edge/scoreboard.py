"""Scoreboard and label (pre-registration §7, §8, §10). Pure functions.

Look decisions (§10) are made once and persisted as ``look`` journal records;
see :func:`label_state`.
"""

from __future__ import annotations

import math
import statistics
from typing import Any, Iterable

from paper_log.trade_edge.config import (
    BANNER_SUFFIX,
    BUCKETS,
    COST_BPS_ROUND_TRIP,
    DELIST_PENALTY,
    HORIZONS,
    LABEL_CONTRARY,
    LABEL_NOT_SUPPORTED,
    LABEL_SUPPORTED,
    LABEL_UNPROVEN,
    LOOK_T,
    LOOKS,
    MIN_CLUSTERS_AT_LOOK,
    MISSING_LABEL_WARNING,
    PRIMARY_HORIZON,
    ST_CLOSED_DELISTED,
    ST_NO_PRICE,
    ST_OPENED,
    ST_UNRESOLVED,
    STRATUM_LARGE,
    STRATUM_SMALL,
    V1_COST_BPS_ROUND_TRIP,
)


def cost_round_trip(bucket: str) -> float:
    """Round-trip cost as a return fraction (§7)."""
    return COST_BPS_ROUND_TRIP[bucket] / 10_000.0


def clustered_t(values: list[float], clusters: list[Any]) -> tuple[float | None, int]:
    """t of the mean with a CR1 cluster-robust standard error.

    SE^2 = G/(G-1) * sum_g (sum_{i in g} (x_i - mean))^2 / n^2. With one
    observation per cluster this is the ordinary s^2/n.
    """
    n = len(values)
    if n != len(clusters):
        raise ValueError("values and clusters differ in length")
    groups: dict[Any, float] = {}
    if n == 0:
        return None, 0
    mean = sum(values) / n
    for v, c in zip(values, clusters):
        groups[c] = groups.get(c, 0.0) + (v - mean)
    g = len(groups)
    if n < 2 or g < 2:
        return None, g
    var = (g / (g - 1)) * sum(s * s for s in groups.values()) / (n * n)
    if var <= 0:
        return None, g
    return mean / math.sqrt(var), g


def _r(x: float | None, nd: int = 6) -> float | None:
    return None if x is None else round(x, nd)


def summarize(rows: list[dict], n_open: int = 0) -> dict:
    """Metrics over closed rows (each: net, gross, entry_session, status, bucket)."""
    net = [r["net_excess"] for r in rows]
    out: dict[str, Any] = {
        "n_open": n_open,
        "n_closed": len(rows),
        "n_closed_delisted": sum(r["status"] == ST_CLOSED_DELISTED for r in rows),
    }
    if not rows:
        out.update({k: None for k in ("mean_net_excess", "median_net_excess", "win_rate", "clustered_t",
                                      "mean_gross_excess", "mean_net_v1_cost", "mean_net_delist_penalty")})
        out["n_clusters"] = 0
        return out
    t, g = clustered_t(net, [r["entry_session"] for r in rows])
    v1 = V1_COST_BPS_ROUND_TRIP / 10_000.0
    penal = [r["net_excess"] + (DELIST_PENALTY if r["status"] == ST_CLOSED_DELISTED else 0.0) for r in rows]
    out.update(
        {
            "mean_net_excess": _r(sum(net) / len(net)),
            "median_net_excess": _r(statistics.median(net)),
            "win_rate": _r(sum(x > 0 for x in net) / len(net), 4),
            "clustered_t": _r(t, 3),
            "n_clusters": g,
            "mean_gross_excess": _r(sum(r["gross_excess"] for r in rows) / len(rows)),
            "mean_net_v1_cost": _r(sum(r["gross_excess"] - v1 for r in rows) / len(rows)),
            "mean_net_delist_penalty": _r(sum(penal) / len(penal)),
        }
    )
    return out


LOOK_KIND = "look"
TERMINAL_LABELS = frozenset({LABEL_SUPPORTED, LABEL_CONTRARY, LABEL_NOT_SUPPORTED})


class LookPolicyError(ValueError):
    """The journal's look decisions are incompatible with the registered look rule.

    Raised instead of rewriting or re-deciding anything: the boundary is a
    migration/policy question for the owner (a new log), never a code path.
    """


def ordered_closed(closed: Iterable[dict]) -> list[dict]:
    """§10 order: exit session, then position id."""
    return sorted(closed, key=lambda r: (r["exit_session"], r["position_id"]))


def look_statistics(sample: list[dict]) -> dict:
    """The statistics §10 reads at a look, over exactly ``sample``."""
    net = [r["net_excess"] for r in sample]
    t, g = clustered_t(net, [r["entry_session"] for r in sample])
    return {"n": len(sample), "clustered_t": _r(t, 3), "n_clusters": g,
            "median_net_excess": _r(statistics.median(net)), "mean_net_excess": _r(sum(net) / len(net))}


def look_decision(sample: list[dict], last_look: bool) -> tuple[str, dict]:
    """§10 decision at one look: SUPPORTED / CONTRARY (terminal), NOT_SUPPORTED at the last look, else UNPROVEN."""
    net = [r["net_excess"] for r in sample]
    t, g = clustered_t(net, [r["entry_session"] for r in sample])
    med = statistics.median(net)
    stats = look_statistics(sample)
    if t is not None and t >= LOOK_T and med > 0 and g >= MIN_CLUSTERS_AT_LOOK:
        return LABEL_SUPPORTED, stats
    if t is not None and t <= -LOOK_T:
        return LABEL_CONTRARY, stats
    return (LABEL_NOT_SUPPORTED if last_look else LABEL_UNPROVEN), stats


def _validate_looks(looks: dict[int, dict]) -> list[int]:
    ns = sorted(int(n) for n in looks)
    if ns != list(LOOKS[:len(ns)]):
        raise LookPolicyError(f"journal look records {ns} are not a prefix of the registered looks {list(LOOKS)}")
    for i, n in enumerate(ns):
        rec = looks[n]
        if int(rec.get("n", n)) != n or len(rec.get("members", ())) != n:
            raise LookPolicyError(f"journal look record at n={n} does not hold exactly {n} members")
        if rec["decision"] in TERMINAL_LABELS and i != len(ns) - 1:
            raise LookPolicyError(f"journal look at n={n} is terminal ({rec['decision']}) but a later look exists")
    return ns


def _late_arrivals(ordered: list[dict], rec: dict) -> int:
    """Closed rows that now sort before a decided look's last member but were not in it."""
    members = set(rec["members"])
    last = (rec["boundary_exit_session"], rec["members"][-1])
    return sum(1 for r in ordered
               if (r["exit_session"], r["position_id"]) <= last and r["position_id"] not in members)


def label_state(closed: list[dict], looks: dict[int, dict] | None = None, *,
                pending_exit_sessions: Iterable[str] = (), pending_filings: int = 0,
                decide: bool = True) -> dict:
    """§10, decided once per look and then read back from the journal.

    ``looks`` are the journal's ``look`` records (``n`` -> record); a decided
    look is never re-evaluated, whatever closes later. The next look at ``n`` is
    decided only when its membership is complete: at least ``n`` closed rows,
    and no admitted position whose primary exit session is on or before the
    boundary (the n-th row's exit session) still lacks its exit
    (``pending_exit_sessions``), and no candidate filing was deferred this run
    (``pending_filings``). Then exactly the first ``n`` rows in §10 order are
    the look. With ``decide=False`` nothing is decided; the state reports what
    the journal holds and whether the next look is decidable.

    Returns the label dict plus ``new_looks``: the look records this call
    decided (empty unless ``decide``), which the caller appends to the journal.
    """
    looks = {int(n): rec for n, rec in (looks or {}).items()}
    _validate_looks(looks)
    ordered = ordered_closed(closed)
    pending = sorted(pending_exit_sessions)
    new_looks: list[dict] = []
    stats = None
    base = {"n_closed": len(ordered), "next_look_at_n_closed": None, "decided_at_look": None,
            "look_pending": None, "late_arrivals_after_look": 0, "new_looks": new_looks}
    for i, n in enumerate(LOOKS):
        rec = looks.get(n)
        if rec is not None:
            stats = rec["stats"]
            late = _late_arrivals(ordered, rec)
            if rec["decision"] in TERMINAL_LABELS:
                return {**base, "label": rec["decision"], "looks_done": i + 1, "decided_at_look": n,
                        "look_stats": stats, "basis": "journal", "late_arrivals_after_look": late}
            base["late_arrivals_after_look"] = late
            continue
        if len(ordered) < n:
            return {**base, "label": LABEL_UNPROVEN, "looks_done": i, "next_look_at_n_closed": n,
                    "look_stats": stats, "basis": "journal" if looks else "interim"}
        boundary = ordered[n - 1]["exit_session"]
        blocking = [s for s in pending if s <= boundary]
        if not decide or blocking or pending_filings:
            return {**base, "label": LABEL_UNPROVEN, "looks_done": i, "next_look_at_n_closed": n,
                    "look_stats": stats, "basis": "journal" if looks else "interim",
                    "look_pending": {"n": n, "boundary_exit_session": boundary,
                                     "blocked_by_positions": len(blocking),
                                     "deferred_filings": int(pending_filings),
                                     "decidable": not blocking and not pending_filings}}
        sample = ordered[:n]
        decision, stats = look_decision(sample, last_look=(i == len(LOOKS) - 1))
        rec = {"kind": LOOK_KIND, "n": n, "boundary_exit_session": boundary,
               "members": [r["position_id"] for r in sample], "stats": stats,
               "decision": decision, "terminal": decision in TERMINAL_LABELS}
        looks[n] = rec
        new_looks.append(rec)
        if rec["terminal"]:
            return {**base, "label": decision, "looks_done": i + 1, "decided_at_look": n,
                    "look_stats": stats, "basis": "decided_this_run"}
    raise LookPolicyError("every registered look was evaluated without a terminal decision")


def banner(label: str) -> str:
    return f"{label} — {BANNER_SUFFIX}"


def build_scoreboard(entries: Iterable[dict], exits: Iterable[dict], late_filing_lines: int = 0,
                     grid_db_accessions: int = 0, *, looks: dict[int, dict] | None = None,
                     pending_exit_sessions: Iterable[str] = (), pending_filings: int = 0,
                     decide: bool = False) -> dict:
    """Every horizon x stratum x bucket, the missing-label counts and the label.

    The label is read from the journal's ``looks`` and, only when ``decide``
    is set by the tracker run, the next complete look is decided; the decided
    records are returned under ``new_looks`` for the caller to append. Without
    ``looks`` (e.g. a caller that only has entry/exit records) the label is the
    interim view and never decides anything.
    """
    entries = list(entries)
    exits = list(exits)
    opened = {e["position_id"]: e for e in entries if e["status"] == ST_OPENED}
    by_h: dict[int, dict[str, dict]] = {h: {} for h in HORIZONS}
    for x in exits:
        if x["position_id"] in opened:
            by_h[x["horizon"]][x["position_id"]] = x

    def rows_for(h: int, strat: str, bucket: str | None) -> tuple[list[dict], int]:
        rows, n_open = [], 0
        for pid, e in opened.items():
            if e["stratum"] != strat or (bucket is not None and e["cap_bucket"] != bucket):
                continue
            x = by_h[h].get(pid)
            if x is None:
                n_open += 1
            else:
                rows.append({**x, "entry_session": e["entry_session"], "position_id": pid})
        return rows, n_open

    tables: dict[str, Any] = {}
    for h in HORIZONS:
        for strat in (STRATUM_LARGE, STRATUM_SMALL):
            key = f"h{h}_{strat}"
            rows, n_open = rows_for(h, strat, None)
            tables[key] = {"all": summarize(rows, n_open)}
            for b in BUCKETS:
                brows, bopen = rows_for(h, strat, b)
                tables[key][b] = summarize(brows, bopen)

    primary_rows, _ = rows_for(PRIMARY_HORIZON, STRATUM_LARGE, None)
    label = label_state(primary_rows, looks, pending_exit_sessions=pending_exit_sessions,
                        pending_filings=pending_filings, decide=decide)
    new_looks = label.pop("new_looks")

    def count(strat: str | None, status: str) -> int:
        return sum(1 for e in entries if e["status"] == status and (strat is None or e["stratum"] == strat))

    primary_total = sum(1 for e in entries if e["stratum"] == STRATUM_LARGE)
    primary_missing = (count(STRATUM_LARGE, ST_NO_PRICE) + count(STRATUM_LARGE, ST_UNRESOLVED)
                       + sum(1 for x in by_h[PRIMARY_HORIZON].values()
                             if x["status"] == ST_CLOSED_DELISTED and opened[x["position_id"]]["stratum"] == STRATUM_LARGE))
    share = primary_missing / primary_total if primary_total else 0.0
    missing = {
        "positions_total": len(entries),
        "opened": count(None, ST_OPENED),
        "no_price": count(None, ST_NO_PRICE),
        "unresolved_ticker": count(None, ST_UNRESOLVED),
        "closed_delisted_h30": sum(1 for x in by_h[PRIMARY_HORIZON].values() if x["status"] == ST_CLOSED_DELISTED),
        "late_filing_lines": late_filing_lines,
        "grid_db_accessions": grid_db_accessions,
        "late_logged": sum(1 for e in entries if e.get("late_logged")),
        "primary_missing_share": round(share, 4),
        "survivorship_warning": share > MISSING_LABEL_WARNING,
    }
    return {"label": label, "banner": banner(label["label"]), "tables": tables, "missing_labels": missing,
            "new_looks": new_looks}
