"""People-events dry run: report what the materializer WOULD write. Writes nothing to any database.

Reads:
  * the SEC Form 3/4/5 derived parquet (``--form345``), optionally limited to
    some quarters (``--quarters 2026q1,2026q2``) or a random issuer sample
    (``--issuer-sample N --seed S``);
  * optionally a read-only database session (``--db``): ``signal_sources``
    by people-linked ``source_type`` (indexed, ``signal_date >= --since``,
    ``--limit-per-source`` rows each), ``institutional_holdings``,
    ``security_identifiers`` and the existing ``people_events`` rows. The
    session is ``default_transaction_read_only``, has a 20 s statement
    timeout, and refuses to start between 03:30 and 10:30 UTC.

Writes: the JSON report to ``--out`` (a local file) and a short summary to stdout.

Examples (grid-svr)::

    python -m scripts.people_events_dry_run \\
        --form345 /data/sec/form345/derived/nonderiv_transactions.parquet \\
        --out ~/research/people_events_dryrun_20261001/form345_full.json

    python -m scripts.people_events_dry_run --db \\
        --form345 /data/sec/form345/derived/nonderiv_transactions.parquet --quarters 2026q1,2026q2 \\
        --since 2025-10-01 --out ~/research/people_events_dryrun_20261001/live_overlap.json
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from intelligence.people_events_pipeline import dryrun as D


def load_form345(path: Path, quarters: list[str] | None, issuer_sample: int | None, seed: int) -> pd.DataFrame:
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    names = set(pq.ParquetFile(path).schema_arrow.names)
    cols = [c for c in D.form345_columns() + ["quarter"] if c in names]
    filters = [("quarter", "in", quarters)] if quarters else None
    table = pq.read_table(path, columns=cols, filters=filters)
    if issuer_sample:
        issuers = pc.unique(table.column("issuer_cik")).to_pylist()
        issuers = sorted(i for i in issuers if i)
        rng = __import__("random").Random(seed)
        keep = set(rng.sample(issuers, min(issuer_sample, len(issuers))))
        table = table.filter(pc.is_in(table.column("issuer_cik"), value_set=__import__("pyarrow").array(sorted(keep))))
    return table.to_pandas()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--form345", type=Path, help="derived/nonderiv_transactions.parquet")
    ap.add_argument("--quarters", help="comma-separated quarter labels, e.g. 2026q1,2026q2")
    ap.add_argument("--issuer-sample", type=int, help="random sample of N issuer CIKs")
    ap.add_argument("--seed", type=int, default=20261001)
    ap.add_argument("--db", action="store_true", help="also read live sources (read-only session)")
    ap.add_argument("--db-url-env", help="env var holding the database URL (default: config.settings.DB_URL)")
    ap.add_argument("--since", default="2025-01-01", help="signal_date floor for signal_sources reads")
    ap.add_argument("--limit-per-source", type=int, default=250_000)
    ap.add_argument("--observed-at", help="ISO timestamp for this run's observation time (default: the moment "
                    "the inputs finished loading); must not precede that moment unless --allow-past-observed-at")
    ap.add_argument("--allow-past-observed-at", action="store_true",
                    help="fixtures/tests only: accept an --observed-at earlier than the read")
    ap.add_argument("--out", type=Path, required=True, help="JSON report path (must not exist)")
    args = ap.parse_args(argv)

    if args.out.exists():
        print(f"refusing to overwrite {args.out}", file=sys.stderr)
        return 2
    t0 = time.perf_counter()
    form345 = None
    if args.form345:
        quarters = [q.strip() for q in args.quarters.split(",")] if args.quarters else None
        form345 = load_form345(args.form345, quarters, args.issuer_sample, args.seed)
    load_s = round(time.perf_counter() - t0, 3)

    frames: dict[str, pd.DataFrame] = {}
    db_context = None
    if args.db:
        import os

        from intelligence.people_events_pipeline import readonly

        if args.db_url_env:
            url = os.environ[args.db_url_env]
        else:
            from config import settings

            url = settings.DB_URL
        got = readonly.read_inputs(url, since=date.fromisoformat(args.since), limit_per_source=args.limit_per_source)
        frames = got["frames"]
        db_context = {
            "source_counts": got["source_counts"], "security_master": got["security_master"],
            "people_events_counts": got["people_events_counts"], "since": args.since,
            "limit_per_source": args.limit_per_source,
        }

    # The observation time is taken AFTER every input was read: a row ingested
    # while the parquet/DB load was running must never get a first_seen bound
    # earlier than the moment this run could actually have seen it.
    read_done = datetime.now(timezone.utc)
    if args.observed_at:
        observed_at = datetime.fromisoformat(args.observed_at.replace("Z", "+00:00"))
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        if observed_at < read_done and not args.allow_past_observed_at:
            print("refusing --observed-at earlier than the input read (first_seen would be too early)",
                  file=sys.stderr)
            return 2
    else:
        observed_at = read_done

    report = D.run_dry_run(
        form345=form345,
        signal_sources=frames.get("signal_sources"),
        holdings=frames.get("institutional_holdings"),
        identifiers=frames.get("security_identifiers"),
        stored=frames.get("people_events"),
        observed_at=observed_at,
        db_context=db_context,
    )
    report["timings"]["load_form345_s"] = load_s
    report["timings"]["total_s"] = round(time.perf_counter() - t0, 3)
    try:
        import resource  # POSIX only

        report["peak_rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    except ImportError:
        report["peak_rss_mb"] = None
    report["args"] = {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()}
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(D.to_jsonable(report), indent=2, sort_keys=True, default=str))
    total = report["channels"].get("total", {})
    print(json.dumps({"out": str(args.out), "candidates": total.get("candidates"), "events": total.get("events"),
                      "dedup_rate": total.get("dedup_rate"), "pit_invariants": report["pit_invariants"],
                      "total_s": report["timings"]["total_s"], "peak_rss_mb": report["peak_rss_mb"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
