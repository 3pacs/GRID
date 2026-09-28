"""GD4 price-admission basis probe for VS1 v3 (read-only; see ``analysis/price_admission_probe.py``).

    python -m scripts.run_price_admission_probe probe --tickers-file TICKERS.json --benchmark XLK \\
        --source TIINGO --code-sha SHA --out NEW_DIR [--statement-timeout-s 60]
    python -m scripts.run_price_admission_probe coverage --issuers ISSUERS.json \\
        --manifest NEW_DIR/price_manifest.json --out NEW_DIR/coverage.json

``probe`` writes ``probe_report.json`` (per ticker: admitted yes/no and why, source, basis,
coverage dates, checks), ``price_manifest.json`` (the harness's schema, carrying the report's
sha256) and ``sha256s.txt`` into a directory that must not exist yet. Only provenance and basis
are examined; no return is computed and nothing is aligned to insider-event dates. Nothing on or
after 2020-01-01 is read. The session is read-only with a statement timeout.

``coverage`` uses no price at all: the share of filings-admitted issuer purchase events whose
issuer's price ticker is on the manifest (issuer-level; no event date meets a price).

``TICKERS.json``: a JSON list of tickers, or ``{"issuers": [{"ticker": ...}, ...]}``.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from analysis import price_admission_probe as gd4


def _tickers(path: Path) -> list[str]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    items = raw["issuers"] if isinstance(raw, dict) else raw
    out = [str(x["ticker"] if isinstance(x, dict) else x).strip().upper() for x in items]
    if not out or any(not t for t in out):
        raise SystemExit("tickers file has no tickers or an empty ticker")
    return sorted(set(out))


def cmd_probe(args) -> None:
    lo, hi = date.fromisoformat(args.start), date.fromisoformat(args.end)
    try:
        gd4.check_window(lo, hi)
    except gd4.ProbeRefused as exc:
        raise SystemExit(str(exc)) from exc
    if gd4.is_refused_source(args.source):
        raise SystemExit(f"source {args.source!r} is refused by the pre-registration")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    tickers = _tickers(Path(args.tickers_file))
    snapshot = datetime.now(timezone.utc)
    from scripts.run_real_panel_scan import read_only_engine

    engine = read_only_engine(args.statement_timeout_s, "vs1_gd4_price_probe")

    def progress(i: int, n: int, t: str) -> None:
        if i % 25 == 0 or i == n:
            print(f"probed {i}/{n}", file=sys.stderr, flush=True)

    try:
        with engine.connect() as conn:
            probe = gd4.run_probe(conn, tickers, benchmark=args.benchmark, source=args.source, lo=lo, hi=hi,
                                  progress=progress)
    finally:
        engine.dispose()
    inputs = {"tickers_file": Path(args.tickers_file).name,
              "tickers_file_sha256": gd4.file_sha256(Path(args.tickers_file)), "tickers": len(tickers)}
    report = gd4.build_report(probe, tickers=tickers, benchmark=args.benchmark, lo=lo, hi=hi,
                              code_sha=args.code_sha, snapshot_as_of_ts=snapshot, inputs=inputs)
    report_sha = gd4.write_json_once(out / "probe_report.json", report)
    assert report_sha == gd4.file_sha256(out / "probe_report.json")
    hashes = {"probe_report.json": report_sha}
    if report["benchmark_admitted"]:
        hashes["price_manifest.json"] = gd4.write_json_once(out / "price_manifest.json",
                                                            gd4.build_manifest(report, report_sha))
    with open(out / "sha256s.txt", "x", encoding="utf-8", newline="\n") as stream:
        stream.writelines(f"{h}  {name}\n" for name, h in sorted(hashes.items()))
    print(json.dumps({"out": str(out), "summary": report["summary"], "benchmark_admitted":
                      report["benchmark_admitted"], "sha256": hashes}, indent=2, sort_keys=True))


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
    p = sub.add_parser("probe")
    p.add_argument("--tickers-file", required=True)
    p.add_argument("--benchmark", default="XLK")
    p.add_argument("--source", default="TIINGO")
    p.add_argument("--start", default=gd4.DEFAULT_WINDOW[0].isoformat())
    p.add_argument("--end", default=gd4.DEFAULT_WINDOW[1].isoformat())
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
