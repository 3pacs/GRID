"""Daily JSON + Markdown output (pre-registration §9).

Reports are derived files: the JSONL log is the record. Each run writes
``reports/trade_edge_v2_<UTC stamp>.{json,md}`` and refreshes
``reports/LATEST.{json,md}``.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from paper_log.trade_edge.config import (
    BUCKETS,
    EASTERN,
    LABEL_UNPROVEN,
    PRIMARY_HORIZON,
    PREREG_SHA256,
    STRATUM_LARGE,
    STRATUM_SMALL,
    VERSION,
)

SIGNAL_FIELDS = (
    "ticker", "issuer_name", "insider_names", "n_actors", "n_purchases", "total_value",
    "largest_value", "filing_dates", "acceptance_at", "first_known_at", "entry_session",
    "entry_close_at", "cap_bucket", "market_cap_usd", "stratum", "sources", "position_id", "revision",
)


def build_report(result: dict, code_sha: str) -> dict:
    now: datetime = result["now"]
    written = result["written"]
    board = result["scoreboard"]
    label = board["label"]["label"]
    today = now.astimezone(EASTERN).date().isoformat()
    signals = [r for r in written if r["kind"] == "signal"]
    entered = {r["position_id"] for r in written if r["kind"] == "entry"}
    entries = {r["position_id"]: r for r in result["records"] if r["kind"] == "entry"}
    latest_sig = {}
    for r in result["records"]:
        if r["kind"] == "signal":
            latest_sig[r["position_id"]] = r

    def view(r: dict) -> dict:
        out = {k: r.get(k) for k in SIGNAL_FIELDS}
        out["label"] = label
        return out

    pending = [view(s) for pid, s in sorted(latest_sig.items())
               if pid not in entries and s["entry_session"] >= today]
    return {
        "version": VERSION,
        "banner": board["banner"],
        "label": board["label"],
        "interim": label == LABEL_UNPROVEN,
        "run_at": now.isoformat(),
        "run_at_et": now.astimezone(EASTERN).isoformat(),
        "code_sha": code_sha,
        "prereg_sha256": PREREG_SHA256,
        "last_completed_session": result["last_completed_session"].isoformat(),
        "insider_feed": result["freshness"],
        "new_signals": [view(s) for s in signals if s["revision"] == 1],
        "updated_signals": [view(s) for s in signals if s["revision"] > 1],
        "pending_entries": pending,
        "entered_this_run": [{k: r.get(k) for k in ("position_id", "ticker", "status", "entry_session",
                                                    "entry_close_unadjusted", "stratum", "cap_bucket",
                                                    "late_logged")}
                             for r in written if r["kind"] == "entry"],
        "closed_this_run": [{k: r.get(k) for k in ("position_id", "horizon", "status", "exit_session",
                                                   "return", "spy_return", "net_excess")}
                            for r in written if r["kind"] == "exit"],
        "n_entered_this_run": len(entered),
        "scoreboard": board["tables"],
        "missing_labels": board["missing_labels"],
    }


def _usd(v) -> str:
    if v is None:
        return "-"
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.2f}M"
    return f"${v / 1e3:.0f}K"


def _pct(v) -> str:
    return "-" if v is None else f"{100 * v:+.2f}%"


def _num(v) -> str:
    return "-" if v is None else f"{v:.2f}"


def _row(name: str, s: dict) -> str:
    win = "-" if s["win_rate"] is None else f"{100 * s['win_rate']:.0f}%"
    return (f"| {name} | {s['n_open']} | {s['n_closed']} | {_pct(s['mean_net_excess'])} | "
            f"{_pct(s['median_net_excess'])} | {win} | "
            f"{_num(s['clustered_t'])} ({s['n_clusters']}) | {s['n_closed_delisted']} |")


def render_markdown(rep: dict) -> str:
    lab = rep["label"]
    lines = [
        f"# trade_edge v2 — {rep['run_at_et'][:16].replace('T', ' ')} ET",
        "",
        f"**{rep['banner']}**",
        "",
        f"Label: `{lab['label']}` (looks done {lab['looks_done']}; next look at n_closed = "
        f"{lab['next_look_at_n_closed']}; primary = largest line >= $500K, {PRIMARY_HORIZON} sessions, "
        "net of cost, vs SPY)." + (" Numbers below are INTERIM." if rep["interim"] else ""),
        "",
        f"Insider feed: latest SEC_INSIDER BUY pull {rep['insider_feed']['latest_insider_buy_pull']}; "
        f"latest insider_trades row {rep['insider_feed']['latest_insider_trades_created_at']} "
        f"({rep['insider_feed']['insider_trades_rows_24h']} rows in 24h).",
        "",
        "## New signals this run",
        "",
    ]
    new = rep["new_signals"]
    if not new:
        lines.append("None.")
    else:
        lines += ["| ticker | issuer | insiders | total | largest line | filed | accepted (ET) | entry close | cap | stratum |",
                  "|---|---|---|---|---|---|---|---|---|---|"]
        for s in sorted(new, key=lambda s: (s["stratum"] != STRATUM_LARGE, -(s["largest_value"] or 0))):
            ins = ", ".join(s["insider_names"][:3]) + (f" +{len(s['insider_names']) - 3}" if len(s["insider_names"]) > 3 else "")
            acc = ", ".join(a[11:16] + " " + a[5:10] for a in s["acceptance_at"]) or "n/a (22:00 ET rule)"
            lines.append(f"| {s['ticker'] or '?'} | {s['issuer_name'] or '-'} | {ins or '-'} ({s['n_actors']}) | "
                         f"{_usd(s['total_value'])} | {_usd(s['largest_value'])} | {', '.join(s['filing_dates'])} | "
                         f"{acc} | {s['entry_session']} | {s['cap_bucket']} | {s['stratum']} |")
    if rep["updated_signals"]:
        lines += ["", f"Updated (new filings joined an existing position): "
                      + ", ".join(f"{s['ticker']} {s['entry_session']}" for s in rep["updated_signals"])]
    lines += ["", f"Pending entries (entry close not yet scored): {len(rep['pending_entries'])}. "
                  f"Entered this run: {len(rep['entered_this_run'])}. Exits this run: {len(rep['closed_this_run'])}.",
              "", "## Scoreboard (net excess vs SPY after round-trip cost)", ""]
    hdr = ["| slice | open | closed | mean | median | win | clustered t (G) | delisted |",
           "|---|---|---|---|---|---|---|---|"]
    for h, title in ((PRIMARY_HORIZON, f"{PRIMARY_HORIZON} sessions (primary)"), (5, "5 sessions"), (20, "20 sessions")):
        lines += [f"### {title}", ""] + hdr
        big = rep["scoreboard"][f"h{h}_{STRATUM_LARGE}"]
        small = rep["scoreboard"][f"h{h}_{STRATUM_SMALL}"]
        lines.append(_row("large (>= $500K line), all caps", big["all"]))
        if h == PRIMARY_HORIZON:
            for b in BUCKETS:
                lines.append(_row(f"large, {b}", big[b]))
        lines.append(_row("small (< $500K line), all caps", small["all"]))
        lines.append("")
    m = rep["missing_labels"]
    lines += [
        "## Missing labels and data quality",
        "",
        f"positions {m['positions_total']} (opened {m['opened']}, no_price {m['no_price']}, "
        f"unresolved_ticker {m['unresolved_ticker']}); closed_delisted at {PRIMARY_HORIZON}: {m['closed_delisted_h30']}; "
        f"late_filing lines {m['late_filing_lines']}; accessions without EDGAR re-read {m['grid_db_accessions']}; "
        f"late_logged positions {m['late_logged']}; primary missing share {100 * m['primary_missing_share']:.1f}%"
        + (" — **SURVIVORSHIP_WARNING**" if m["survivorship_warning"] else "") + ".",
        "",
        "Costs: 10 bps round trip >= $2B, 30 bps $300M-2B, 100 bps < $300M or unknown. "
        "Clustered t clusters by entry session and ignores overlap between windows (optimistic).",
        "",
        f"Rules: docs/paper_log/trade-edge-v2-preregistration.md (sha256 {rep['prereg_sha256'][:12]}), code {rep['code_sha'][:12]}.",
        "",
    ]
    return "\n".join(lines)


def write_reports(out_dir: Path, rep: dict) -> tuple[Path, Path]:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.fromisoformat(rep["run_at"]).strftime("%Y%m%dT%H%MZ")
    js = json.dumps(rep, indent=2, sort_keys=True, default=str)
    md = render_markdown(rep)
    jpath = out_dir / f"trade_edge_v2_{stamp}.json"
    mpath = out_dir / f"trade_edge_v2_{stamp}.md"
    for path, body in ((jpath, js), (mpath, md), (out_dir / "LATEST.json", js), (out_dir / "LATEST.md", md)):
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(body + ("\n" if not body.endswith("\n") else ""), encoding="utf-8")
        tmp.replace(path)
    return jpath, mpath
