"""VS1 v6 price-admission probe (GD4 + v4-v6 §2.3): vendor fetches to files, then a read-only probe.

    # 1. vendor files (network; files only, never the DB; resumable)
    TWELVEDATA_API_KEY=... python -m scripts.run_price_admission_probe fetch-twelvedata \\
        --tickers-file ISSUERS.json --benchmark XLK --out-dir /data/sec/vs6/twelvedata [--wait-for-reset]
    TIINGO_API_KEY=... python -m scripts.run_price_admission_probe fetch-tiingo-meta \\
        --tickers-file ISSUERS.json --benchmark XLK --out-dir /data/sec/vs6/tiingo_meta
    # 2. the probe (read-only DB session; reads nothing on or after 2020-01-01)
    python -m scripts.run_price_admission_probe probe --tickers-file ISSUERS.json --benchmark XLK \\
        --submissions /data/sec/form345/derived/submissions.parquet --sic-map issuer_sic_map.jsonl \\
        --twelvedata-dir /data/sec/vs6/twelvedata --tiingo-meta-dir /data/sec/vs6/tiingo_meta \\
        --code-sha SHA --out NEW_DIR
    # 3. issuer-level coverage (no price)
    python -m scripts.run_price_admission_probe coverage --issuers ISSUERS.json \\
        --manifest NEW_DIR/price_manifest.json --out NEW_DIR/coverage.json

``probe`` writes, into a directory that must not exist yet: ``crosscheck_report.json`` (per ticker
TwelveData agreement statistics), ``tiingo_meta_report.json`` (startDate, endDate, name,
exchangeCode, entity check), ``probe_report.json`` (basis checks, source filtering, splice check,
C1 interval coverage, admission and reasons), ``price_manifest.json`` (the v6 harness's
``PriceManifest``, carrying the three reports' sha256 and ``listed_from``) and ``sha256s.txt``.
No return is aligned to an insider event and no label is computed; nothing is written to the DB.

``ISSUERS.json``: a JSON list of tickers, or ``{"issuers": [{"ticker", "cik", "current_tickers", ...}]}``
(the probe needs ``cik`` and ``current_tickers`` for the C1 interval report and the entity check).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from analysis import price_admission_fetch as fetch
from analysis import price_admission_probe as gd4


def _log(message: str) -> None:
    print(f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {message}", file=sys.stderr, flush=True)


def _fetch_tickers(args) -> list[str]:
    tickers = fetch.tickers_from_file(Path(args.tickers_file))
    return sorted(set(tickers) | {args.benchmark})


def cmd_fetch_twelvedata(args) -> None:
    try:
        result = fetch.fetch_twelvedata(
            _fetch_tickers(args), Path(args.out_dir), benchmark=args.benchmark, key=fetch.api_key(fetch.TD_KEY_ENV),
            spacing_s=args.spacing_s, daily_reserve=args.daily_reserve, wait_for_reset=args.wait_for_reset,
            progress=_log)
    except fetch.FetchStopped as exc:
        raise SystemExit(str(exc)) from None
    print(json.dumps(result, indent=2, sort_keys=True))


def cmd_fetch_tiingo_meta(args) -> None:
    try:
        result = fetch.fetch_tiingo_meta(_fetch_tickers(args), Path(args.out_dir),
                                         key=fetch.api_key(fetch.TIINGO_KEY_ENV), spacing_s=args.spacing_s,
                                         progress=_log)
    except fetch.FetchStopped as exc:
        raise SystemExit(str(exc)) from None
    print(json.dumps(result, indent=2, sort_keys=True))


def _issuers(path: Path) -> list[dict]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    items = raw["issuers"] if isinstance(raw, dict) else raw
    out = []
    for x in items:
        if isinstance(x, dict):
            out.append({"ticker": str(x["ticker"]).strip().upper(), "cik": x.get("cik"),
                        "current_tickers": list(x.get("current_tickers") or [x["ticker"]])})
        else:
            out.append({"ticker": str(x).strip().upper(), "cik": None, "current_tickers": [str(x).strip().upper()]})
    if not out or any(not m["ticker"] for m in out):
        raise SystemExit("tickers file has no tickers or an empty ticker")
    return out


def cmd_probe(args) -> None:
    lo, hi = date.fromisoformat(args.start), date.fromisoformat(args.end)
    try:
        gd4.check_window(lo, hi)
        gd4.check_source(args.source)
    except gd4.ProbeRefused as exc:
        raise SystemExit(str(exc)) from exc
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    issuers = _issuers(Path(args.tickers_file))
    tickers = sorted({m["ticker"] for m in issuers})
    snapshot = datetime.now(timezone.utc)
    if args.as_of_ts:
        requested = datetime.fromisoformat(args.as_of_ts)
        if requested.tzinfo is None:
            raise SystemExit("--as-of-ts needs a UTC offset")
        if requested > snapshot:
            raise SystemExit("--as-of-ts lies in the future")
        snapshot = requested.astimezone(timezone.utc)
    sec_names = (gd4.sec_names_by_ticker(Path(args.sic_map), issuers, pinned=not args.unpinned_sic_map)
                 if args.sic_map else {})
    interval = (gd4.C1Interval.from_submissions(Path(args.submissions), issuers) if args.submissions else None)
    vendors = gd4.VendorFiles(Path(args.twelvedata_dir), Path(args.tiingo_meta_dir))
    from scripts.run_real_panel_scan import read_only_engine

    engine = read_only_engine(args.statement_timeout_s, "vs1_v6_price_probe")

    def progress(i: int, n: int, t: str) -> None:
        if i % 25 == 0 or i == n:
            _log(f"probed {i}/{n}")

    try:
        with engine.connect() as conn:
            probe = gd4.run_probe(conn, tickers, benchmark=args.benchmark, source=args.source, lo=lo, hi=hi,
                                  as_of_ts=snapshot, vendors=vendors, sec_names=sec_names, interval=interval,
                                  progress=progress)
    finally:
        engine.dispose()
    inputs = {"tickers_file": Path(args.tickers_file).name,
              "tickers_file_sha256": gd4.file_sha256(Path(args.tickers_file)), "tickers": len(tickers),
              "sic_map_sha256": gd4.file_sha256(Path(args.sic_map)) if args.sic_map else None,
              "submissions_sha256": gd4.file_sha256(Path(args.submissions)) if args.submissions else None,
              "twelvedata_fetch_log_sha256": fetch.fetch_log_sha256(Path(args.twelvedata_dir)),
              "tiingo_meta_fetch_log_sha256": fetch.fetch_log_sha256(Path(args.tiingo_meta_dir))}
    written = gd4.write_outputs(out, probe, tickers=tickers, benchmark=args.benchmark, lo=lo, hi=hi,
                                code_sha=args.code_sha, snapshot_as_of_ts=snapshot, inputs=inputs)
    print(json.dumps(written, indent=2, sort_keys=True))


def cmd_coverage(args) -> None:
    issuers = json.loads(Path(args.issuers).read_text(encoding="utf-8"))["issuers"]
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    cov = gd4.event_coverage(issuers, manifest["admitted"])
    doc = {"what": "filings-admitted issuer purchase events whose issuer's price ticker is price-admitted "
                   "(issuer level; no price used)",
           "issuers_file_sha256": gd4.file_sha256(Path(args.issuers)),
           "manifest_file_sha256": gd4.file_sha256(Path(args.manifest)),
           "issuers": cov.issuers, "issuers_price_admitted": cov.issuers_price_admitted,
           "issuers_with_events": cov.issuers_with_events,
           "issuers_with_events_price_admitted": cov.issuers_with_events_price_admitted,
           "events": cov.events, "events_price_admitted": cov.events_price_admitted,
           "event_fraction": round(cov.event_fraction, 6), "not_price_admitted": cov.not_admitted}
    sha = gd4.write_json_once(Path(args.out), doc)
    print(json.dumps({k: v for k, v in doc.items() if k != "not_price_admitted"} | {"sha256": sha},
                     indent=2, sort_keys=True))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("fetch-twelvedata")
    f.add_argument("--tickers-file", required=True)
    f.add_argument("--benchmark", default=gd4.BENCHMARK)
    f.add_argument("--out-dir", required=True)
    f.add_argument("--spacing-s", type=float, default=fetch.TD_SPACING_S)
    f.add_argument("--daily-reserve", type=int, default=fetch.TD_DAILY_RESERVE)
    f.add_argument("--wait-for-reset", action="store_true", help="sleep through the UTC daily reset and continue")
    f.set_defaults(func=cmd_fetch_twelvedata)
    m = sub.add_parser("fetch-tiingo-meta")
    m.add_argument("--tickers-file", required=True)
    m.add_argument("--benchmark", default=gd4.BENCHMARK)
    m.add_argument("--out-dir", required=True)
    m.add_argument("--spacing-s", type=float, default=fetch.TIINGO_SPACING_S)
    m.set_defaults(func=cmd_fetch_tiingo_meta)
    p = sub.add_parser("probe")
    p.add_argument("--tickers-file", required=True)
    p.add_argument("--benchmark", default=gd4.BENCHMARK)
    p.add_argument("--source", default=gd4.PRICE_SOURCE)
    p.add_argument("--start", default=gd4.DEFAULT_WINDOW[0].isoformat())
    p.add_argument("--end", default=gd4.DEFAULT_WINDOW[1].isoformat())
    p.add_argument("--submissions", help="derived/submissions.parquet (C1 ticker-interval report)")
    p.add_argument("--sic-map", help="the pinned issuer_sic_map.jsonl (SEC names for the entity check)")
    p.add_argument("--unpinned-sic-map", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--twelvedata-dir", required=True)
    p.add_argument("--tiingo-meta-dir", required=True)
    p.add_argument("--as-of-ts", help="snapshot instant (default: now); every read is bounded by it")
    p.add_argument("--code-sha", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--statement-timeout-s", type=int, default=60)
    p.set_defaults(func=cmd_probe)
    c = sub.add_parser("coverage")
    c.add_argument("--issuers", required=True)
    c.add_argument("--manifest", required=True)
    c.add_argument("--out", required=True)
    c.set_defaults(func=cmd_coverage)
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
