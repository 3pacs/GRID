#!/usr/bin/env python3
"""Form 4 match-rate estimate for a security_master seed artifact. Files only; never opens a database.

Re-runs the people-events dry run's Form 4 path (``form4_from_form345`` -> ``merge_candidates``) on the SEC
Form 3/4/5 parquet for some quarters and resolves the events with
``intelligence.people_events_pipeline.security.resolve_securities`` against the identifiers in a seed
artifact, optionally next to a baseline set of identifiers (the rows already in ``security_master``,
dumped to JSONL by whoever has read access).

Three readings per identifier set:

``full``          CIK + ticker, exactly as the materializer resolves (what ``match_rate`` in the
                  dry-run report means).
``ticker_only``   the same events with the issuer CIK blanked, so only the filed ticker inside its
                  validity window can match. This is how QuiverQuant and EDGAR-native events (no CIK)
                  resolve, and it is the test of the dated ticker windows.
``agreement``     of the events that resolve on BOTH CIK and ticker-only, the share whose ticker-only
                  entity equals the CIK entity (a wrong-entity ticker match would lower it).

Caution: the artifact is built from the same Form 3/4/5 file, so ``full`` is close to 100% by
construction for issuers the file already contains. It is an upper bound for issuers that first file after the
build, which stay unmatched until the artifact is rebuilt. ``ticker_only`` and ``agreement`` are the
informative numbers.

Usage::

    python -m scripts.security_master_match_rate --seed-dir <dir> \\
        --form345 /data/sec/form345/derived/nonderiv_transactions.parquet \\
        --quarters 2025q3,2025q4,2026q1,2026q2 --out report.json [--baseline-rows existing.jsonl]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Optional

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd  # noqa: E402

from intelligence.people_events_pipeline import adapters as A  # noqa: E402
from intelligence.people_events_pipeline import merge as M  # noqa: E402
from intelligence.people_events_pipeline import security as S  # noqa: E402


def identifiers_from_jsonl(path: Path) -> pd.DataFrame:
    """``si`` lines of a seed artifact (or a baseline dump of ``security_identifiers`` in the same shape)."""
    rows = []
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            rec = json.loads(line)
            if rec.pop("t", "si") == "si":
                rows.append(rec)
    return pd.DataFrame(rows, columns=S.IDENTIFIER_COLUMNS) if rows else pd.DataFrame(columns=S.IDENTIFIER_COLUMNS)


def _summ(resolved: pd.DataFrame) -> dict[str, Any]:
    issuers = resolved[resolved["security_match_basis"] != "not_an_issuer"]
    n = int(len(issuers))
    matched = int(issuers["security_id"].notna().sum())
    return {
        "issuer_events": n,
        "matched": matched,
        "match_rate": round(matched / n, 6) if n else None,
        "by_basis": {k: int(v) for k, v in resolved["security_match_basis"].value_counts().sort_index().items()},
        "conflict_flagged": int(resolved["security_conflict"].sum()),
        "distinct_entities": int(resolved["security_id"].dropna().nunique()),
    }


def estimate(events: pd.DataFrame, identifiers: pd.DataFrame) -> dict[str, Any]:
    full = S.resolve_securities(events, identifiers)
    ticker_only = S.resolve_securities(events.assign(entity_cik=pd.array([pd.NA] * len(events), dtype="Int64")), identifiers)
    has_ticker = events["entity_ticker"].notna()
    both = full["security_id"].notna() & ticker_only["security_id"].notna() & (full["security_match_basis"] == "cik")
    agree = int((full.loc[both, "security_id"] == ticker_only.loc[both, "security_id"]).sum())
    out_full = _summ(full)
    t_only = _summ(ticker_only[has_ticker])
    t_only["events_with_a_filed_ticker"] = int(has_ticker.sum())
    return {
        "full": out_full,
        "ticker_only": t_only,
        "agreement": {
            "events_resolved_by_cik_and_by_ticker": int(both.sum()),
            "same_entity": agree,
            "agreement_rate": round(agree / int(both.sum()), 6) if int(both.sum()) else None,
        },
    }


def run(form345: pd.DataFrame, identifiers: pd.DataFrame, baseline: Optional[pd.DataFrame] = None) -> dict[str, Any]:
    sec, skips = A.form4_from_form345(form345)
    merged = M.merge_candidates(sec)
    events = merged.events
    events = events[events["channel"] == "form4"].reset_index(drop=True)
    report: dict[str, Any] = {
        "form345_rows": int(len(form345)),
        "form4_events": int(len(events)),
        "identifiers_rows": int(len(identifiers)),
        "after": estimate(events, identifiers),
        "skips": {k: int(v) for k, v in sorted(skips.items()) if v},
    }
    if baseline is not None:
        report["baseline_identifiers_rows"] = int(len(baseline))
        report["before"] = estimate(events, baseline)
    return report


def main(argv: Optional[list[str]] = None) -> int:
    from scripts.people_events_dry_run import load_form345

    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed-dir", type=Path, required=True)
    ap.add_argument("--form345", type=Path, required=True, help="derived/nonderiv_transactions.parquet")
    ap.add_argument("--quarters", required=True, help="e.g. 2025q3,2025q4,2026q1,2026q2")
    ap.add_argument("--baseline-rows", type=Path, help="JSONL dump of the identifiers already in security_identifiers")
    ap.add_argument("--out", type=Path, required=True, help="JSON report path (must not exist)")
    args = ap.parse_args(argv)
    if args.out.exists():
        print(f"refusing to overwrite {args.out}", file=sys.stderr)
        return 2
    form345 = load_form345(args.form345, [q.strip() for q in args.quarters.split(",")], None, 0)
    report = run(
        form345,
        identifiers_from_jsonl(args.seed_dir / "security_master_seed.jsonl"),
        identifiers_from_jsonl(args.baseline_rows) if args.baseline_rows else None,
    )
    report["args"] = {k: str(v) for k, v in vars(args).items()}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"after": report["after"]["full"]["match_rate"], "ticker_only": report["after"]["ticker_only"]["match_rate"],
                      "agreement": report["after"]["agreement"]["agreement_rate"],
                      "before": report.get("before", {}).get("full", {}).get("match_rate")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
