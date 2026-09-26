"""S09: one read-only real-panel scan through the latest-vintage research contract.

Run (on a host that can reach griddb, env loaded, never printing it):

    python -m scripts.run_real_panel_scan NEW_OUTPUT_DIRECTORY --code-sha SHA

Read-only by construction: a dedicated NullPool engine whose sessions start
with ``default_transaction_read_only=on``, ``statement_timeout`` <= 60 s and
autocommit (one short transaction per statement). The only SQL executed is
``store.observations.read_window`` -- one bounded ``series_id`` + date-window
read per declared series. No writes, no registry, no route, no timer; the
output directory must be new.

Universe (declared here, frozen in the manifest): 29 FRED/AAII series that are
single-source and single-valued per date on griddb (bounded read-only probe,
2026-09-26), none on the adapter's revised-series denylist (DTWEXBGS was
dropped in S09b: the broad dollar index history is revised with trade
weights), none yfinance-sourced (historical ``YF:*:close`` rows carry more than
one close per date, S07/#642), no ``snap:*``/LLM/astro series. Every series
declares its publication source (``research_real_panel.PUBLICATIONS``).
Targets are non-price: forward *changes* of VIX, the 2-year yield, the 10y-2y
slope and the HY OAS. Trials whose feature is in the target's declared proxy
group are ``self_lag``: measured, never selectable (S09b).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from analysis.offline_research_proof import (
    LATEST_VINTAGE_ORIGIN,
    Protocol,
    run_proof,
    write_once,
)
from analysis.research_real_panel import (
    SeriesSpec,
    TargetSpec,
    load_latest_vintage_panel,
)

REPO = Path(__file__).resolve().parent.parent
MAX_STATEMENT_TIMEOUT_S = 60

WEEKLY = {"stale_sessions": 10}

FEATURES: tuple[SeriesSpec, ...] = (
    # Treasury curve / real yields / policy (H.15, daily, not revised)
    SeriesSpec("DGS1", "diff", "FRB_H15"),
    SeriesSpec("DGS2", "diff", "FRB_H15"),
    SeriesSpec("DGS5", "diff", "FRB_H15"),
    SeriesSpec("DGS30", "diff", "FRB_H15"),
    SeriesSpec("DFII10", "diff", "FRB_H15"),
    SeriesSpec("DFF", "diff", "FRB_H15"),
    # FRED-computed spreads / breakevens from H.15 legs
    SeriesSpec("T10Y2Y", "diff", "FRED_H15_SPREAD"),
    SeriesSpec("T10Y3M", "diff", "FRED_H15_SPREAD"),
    SeriesSpec("T10YIE", "diff", "FRED_H15_SPREAD"),
    SeriesSpec("T5YIE", "diff", "FRED_H15_SPREAD"),
    # Credit (ICE BofA OAS, daily)
    SeriesSpec("BAMLH0A0HYM2", "diff", "ICE_BOFA"),
    SeriesSpec("BAMLC0A0CM", "diff", "ICE_BOFA"),
    SeriesSpec("BAMLH0A1HYBB", "diff", "ICE_BOFA"),
    SeriesSpec("BAMLH0A2HYB", "diff", "ICE_BOFA"),
    SeriesSpec("BAMLH0A3HYC", "diff", "ICE_BOFA"),
    SeriesSpec("BAMLC0A4CBBB", "diff", "ICE_BOFA"),
    SeriesSpec("BAMLHE00EHYIOAS", "diff", "ICE_BOFA"),
    SeriesSpec("BAMLEMHBHYCRPIOAS", "diff", "ICE_BOFA"),
    # Volatility
    SeriesSpec("VIXCLS", "diff", "CBOE_VIX"),
    # FX (H.10 bilateral rates; the broad index DTWEXBGS is revised: refused)
    SeriesSpec("DEXJPUS", "pct", "FRB_H10"),
    SeriesSpec("DEXUSEU", "pct", "FRB_H10"),
    SeriesSpec("DEXCAUS", "pct", "FRB_H10"),
    SeriesSpec("DEXSZUS", "pct", "FRB_H10"),
    SeriesSpec("DEXUSUK", "pct", "FRB_H10"),
    # Liquidity / balance sheet (H.4.1 Wednesday levels, Thursday release)
    SeriesSpec("WALCL", "pct", "FRB_H41", **WEEKLY),
    SeriesSpec("WTREGEN", "pct", "FRB_H41", **WEEKLY),
    SeriesSpec("RRPONTSYD", "diff", "NYFED_RRP"),
    # Housing finance / sentiment (weekly, Thursday-dated)
    SeriesSpec("MORTGAGE30US", "diff", "FREDDIE_PMMS", **WEEKLY),
    SeriesSpec("aaii.bull_bear_spread", "diff", "AAII", **WEEKLY),
)
TARGETS: tuple[TargetSpec, ...] = (
    TargetSpec("VIXCLS", "change", "CBOE_VIX"),
    TargetSpec("DGS2", "change", "FRB_H15"),
    TargetSpec("T10Y2Y", "change", "FRED_H15_SPREAD"),
    TargetSpec("BAMLH0A0HYM2", "change", "ICE_BOFA"),
)
HORIZONS = (1, 5, 20)


def file_sha256(relative: str) -> str:
    data = (REPO / relative).read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()


def read_only_engine(statement_timeout_s: int, application_name: str = "s09_real_panel_scan"):
    """NullPool engine: read-only sessions, bounded statements, autocommit."""
    if not 0 < statement_timeout_s <= MAX_STATEMENT_TIMEOUT_S:
        raise ValueError(f"statement timeout must be 1..{MAX_STATEMENT_TIMEOUT_S} s")
    if not application_name.replace("_", "").isalnum():
        raise ValueError("application_name must be alphanumeric/underscore")
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    from config import settings

    options = (
        f"-c statement_timeout={statement_timeout_s * 1000} "
        "-c default_transaction_read_only=on "
        "-c idle_in_transaction_session_timeout=30000 "
        f"-c application_name={application_name}"
    )
    return create_engine(
        settings.DB_URL,
        poolclass=NullPool,
        isolation_level="AUTOCOMMIT",
        connect_args={"options": options},
    )


def bh_threshold(ledger: list[dict], q: float) -> dict:
    """The BH step-up cut over the whole run: largest k with p_(k) <= k q / m."""
    p = sorted(t["p"] for t in ledger)
    m = len(p)
    k = max((i + 1 for i, v in enumerate(p) if v <= (i + 1) * q / m), default=0)
    return {
        "q": q,
        "m": m,
        "rejections": k,
        "critical_p": p[k - 1] if k else None,
        "first_rank_cut": q / m,
        "smallest_p": p[0] if p else None,
    }


def scan(conn, output: Path, args) -> dict:
    started = time.time()
    panel = load_latest_vintage_panel(
        conn,
        FEATURES,
        TARGETS,
        start=date.fromisoformat(args.read_start),
        as_of=date.fromisoformat(args.as_of),
        as_of_ts=datetime.fromisoformat(args.as_of_ts),
    )
    read_seconds = time.time() - started
    # Holdout runs to the end of the as_of day; nothing after the read is labelled.
    end = datetime.combine(
        date.fromisoformat(args.as_of) + timedelta(days=1), datetime.min.time(), timezone.utc
    )
    protocol = Protocol(
        run_id=f"s09-real-panel-{args.as_of}",
        features=panel.feature_names(),
        split=f"{args.split}T00:00:00+00:00",
        end=end.isoformat(),
        origin=LATEST_VINTAGE_ORIGIN,
        families=panel.family_names(HORIZONS),
        fdr_q=0.10,
        alpha=0.05,
        sampling="horizon_spaced",
        step=5,
        perms=args.perms,
        seed=args.seed,
        statistic="spearman",
        start=f"{args.discovery_start}T00:00:00+00:00",
        read_receipt=panel.receipt_sha,
        self_lag=panel.self_lag(panel.family_names(HORIZONS)),
    )
    discovery = panel.family_rows(protocol, "discovery")
    holdout = panel.family_rows(protocol, "holdout")
    result = run_proof(protocol, discovery, holdout, output / "run", panel=panel)
    frozen = json.loads((output / "run" / "discovery-frozen.json").read_text())
    payload = frozen["payload"]
    ledger = payload["ledger"]
    with (output / "trial-ledger.csv").open("x", newline="", encoding="utf-8") as f:
        fields = ["trial_id", "family", "feature", "n", "r", "p", "adjusted_p",
                  "status", "block", "selected", "self_lag_r", "self_lag_p"]
        writer = csv.DictWriter(f, fields)
        writer.writeheader()
        for t in ledger:
            writer.writerow({k: t.get(k) for k in fields})
    survivors = [t for t in ledger if t["selected"]]
    summary = {
        "state": result["state"],
        "promotion_allowed": False,
        "origin": LATEST_VINTAGE_ORIGIN,
        "code_sha": args.code_sha,
        "file_sha256": {
            f: file_sha256(f)
            for f in (
                "analysis/offline_research_proof.py",
                "analysis/research_real_panel.py",
                "store/observations.py",
                "scripts/run_real_panel_scan.py",
            )
        },
        "as_of": args.as_of,
        "as_of_ts": args.as_of_ts,
        "read_start": args.read_start,
        "discovery_start": args.discovery_start,
        "holdout_start": args.split,
        "holdout_end_exclusive": protocol.end,
        "reader": panel.receipt["reader"],
        "read_receipt_sha256": panel.receipt_sha,
        "vintage": panel.receipt["vintage"],
        "publications": panel.receipt["publications"],
        "proxy_groups": panel.receipt["proxy_groups"],
        "self_lag_trials": payload["self_lag_count"],
        "series_read": panel.receipt["series"],
        "universe": [s.series_id for s in FEATURES],
        "feature_specs": panel.receipt["features"],
        "targets": panel.receipt["targets"],
        "horizons_sessions": list(HORIZONS),
        "families": list(protocol.families),
        "features_declared": len(protocol.features),
        "trials": payload["trial_count"],
        "testable": payload["tested_count"],
        "untestable": payload["untestable_count"],
        "untestable_by_status": {
            s: sum(t["status"] == s for t in ledger)
            for s in sorted({t["status"] for t in ledger})
        },
        "rows_per_family": {
            "discovery": {k: len(v) for k, v in discovery.items()},
            "holdout": {k: len(v) for k, v in holdout.items()},
        },
        "blocks": payload["blocks"],
        "block_basis": payload["block_basis"],
        "holdout_blocks": result["holdout_blocks"],
        "min_attainable_p": payload["min_attainable_p"],
        "raw_p_lt_0.05": sum(t["status"] == "tested" and t["p"] < 0.05 for t in ledger),
        "bh": bh_threshold(ledger, protocol.fdr_q),
        "survivors_discovery_bh": len(survivors),
        "survivors": [
            {k: t[k] for k in ("trial_id", "family", "feature", "n", "r", "p", "adjusted_p")}
            for t in survivors
        ],
        "holdout_checks": result["holdout_checks"],
        "frozen_candidates": result["candidates"],
        "discovery_manifest_sha256": frozen["sha256"],
        "method": payload["method"],
        "caveats": payload["caveats"],
        "read_seconds": round(read_seconds, 1),
        "total_seconds": round(time.time() - started, 1),
    }
    write_once(output / "summary.json", summary)
    write_once(output / "frozen-candidates.json", result["candidates"])
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    parser.add_argument("--code-sha", required=True)
    parser.add_argument("--as-of", default="2026-09-25")
    parser.add_argument("--as-of-ts", default="2026-09-26T00:00:00+00:00")
    parser.add_argument("--read-start", default="2003-01-01")
    parser.add_argument("--discovery-start", default="2004-01-02")
    parser.add_argument("--split", default="2018-01-02")
    parser.add_argument("--perms", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=20260926)
    parser.add_argument("--statement-timeout-s", type=int, default=60)
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=False)
    engine = read_only_engine(args.statement_timeout_s)
    try:
        with engine.connect() as conn:
            summary = scan(conn, output, args)
    finally:
        engine.dispose()
    print(
        json.dumps(
            {k: summary[k] for k in ("trials", "testable", "untestable",
                                     "survivors_discovery_bh", "bh", "state")},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
