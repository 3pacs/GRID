"""VS1 holdout-period basis-report CLI (v6 §2.3, §8; read-only, write-once).

    python -m scripts.run_holdout_basis_probe \\
        --manifest PRICE_MANIFEST.json --log-dir REGISTRY_DIR --vault-repo VAULT_CLONE \\
        --twelvedata-dir HOLDOUT_TD_DIR --tiingo-meta-dir HOLDOUT_META_DIR \\
        [--calendar-manifest ARCHIVAL_XNYS_MANIFEST.json] --as-of-ts ISO_UTC \\
        --allow-holdout --prereg-sha256 HEX64 [--code-sha SHA] --out NEW_DIR

Refuses unless a witnessed ``discovery_frozen`` exists in ``--log-dir`` / ``--vault-repo``,
canonical study-specific price-manifest and calendar pins are present and match,
``--as-of-ts`` is strictly later than ``discovery_frozen.run_at``, ``--allow-holdout`` is set,
``--prereg-sha256`` matches the registry, ``--out`` does not exist yet, the window lies inside
``2020-01-01 .. 2026-06-30``, and no Form 4 input is supplied.

Writes ``holdout_basis_report.json``, ``crosscheck_report.json``, and ``sha256s.txt`` once.
The resulting ``holdout_basis_report.json`` plugs into ``open-holdout --holdout-probe-report``.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from analysis import holdout_basis_probe as hb
from analysis import price_admission_fetch as fetch
from analysis import price_admission_probe as gd4
from analysis import research_forward_log as fl
from scripts.run_price_admission_probe import PROBE_MAX_STATEMENT_TIMEOUT_S, _issuers, _log

REPO = Path(__file__).resolve().parent.parent


def resolve_code_sha(repo_root: Path = REPO) -> str:
    """Compute the running code's commit SHA (VERSION file or clean git HEAD); refuses dirty trees."""
    try:
        return fl.resolve_code_sha(repo_root)
    except RuntimeError as exc:
        raise hb.HoldoutProbeRefused(str(exc)) from exc


def _parse_as_of_ts(raw: str) -> datetime:
    text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        ts = datetime.fromisoformat(text)
    except ValueError as exc:
        raise hb.HoldoutProbeRefused(f"--as-of-ts must be an ISO instant: {raw!r}") from exc
    if ts.tzinfo is None:
        raise hb.HoldoutProbeRefused("--as-of-ts requires a UTC offset")
    now = datetime.now(timezone.utc)
    if ts.astimezone(timezone.utc) > now:
        raise hb.HoldoutProbeRefused("--as-of-ts lies in the future")
    return ts.astimezone(timezone.utc)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--manifest", required=True, help="frozen discovery price_manifest.json")
    parser.add_argument("--log-dir", required=True, help="study registry directory")
    parser.add_argument("--vault-repo", required=True, help="local git repo to fetch the pinned vault main into")
    parser.add_argument("--twelvedata-dir", required=True, help="holdout-window TwelveData cache dir")
    parser.add_argument("--tiingo-meta-dir", required=True, help="holdout-window Tiingo metadata cache dir")
    parser.add_argument(
        "--calendar-manifest",
        default=None,
        help=(
            "optional immutable archival path to grid.xnys-session-manifest.v1 JSON "
            "(defaults to the study's canonical path; bytes are always re-hashed against the study pin)"
        ),
    )
    parser.add_argument(
        "--calendar-sha256",
        help="optional assertion: if given, must equal the study's canonical pinned calendar sha256 (cannot override)",
    )
    parser.add_argument("--as-of-ts", required=True, help="UTC snapshot instant (must be > discovery_frozen.run_at)")
    parser.add_argument("--allow-holdout", action="store_true", help="explicit authorization to read holdout basis")
    parser.add_argument("--prereg-sha256", required=True, help="pinned preregistration body sha256")
    parser.add_argument(
        "--code-sha",
        help="optional assertion: if given, must equal the computed running code SHA (VERSION or clean git HEAD)",
    )
    parser.add_argument("--out", required=True, help="write-once output directory (must not exist)")
    parser.add_argument("--start", default=hb.HOLDOUT_START.isoformat(), help="holdout read start (default 2020-01-01)")
    parser.add_argument("--end", default=hb.HOLDOUT_END.isoformat(), help="holdout read end (default 2026-06-30)")
    parser.add_argument(
        "--submissions", required=True,
        help="derived/submissions.parquet (required for C1 ticker-interval bounds, v6 §2.3/§8)",
    )
    parser.add_argument(
        "--issuers", required=True,
        help="issuers JSON with cik/current_tickers (required for C1 and entity checks, v6 §2.3/§8)",
    )
    parser.add_argument(
        "--sic-map", required=True,
        help="pinned issuer_sic_map.jsonl (required for SEC entity check, v6 §2.3/§8)",
    )
    parser.add_argument("--unpinned-sic-map", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--statement-timeout-s", type=int, default=60,
        help=f"per-statement bound, 1..{PROBE_MAX_STATEMENT_TIMEOUT_S} s (operational; default 60)",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    try:
        hb.check_no_form4_cli_args(raw_argv)
    except hb.HoldoutProbeRefused as exc:
        raise SystemExit(str(exc)) from exc

    args = _build_parser().parse_args(raw_argv)
    try:
        if not args.allow_holdout:
            raise hb.HoldoutProbeRefused("holdout basis probe requires explicit --allow-holdout")
        lo, hi = date.fromisoformat(args.start), date.fromisoformat(args.end)
        hb.check_holdout_window(lo, hi)
        snapshot = _parse_as_of_ts(args.as_of_ts)
        out = hb.check_write_once_out(Path(args.out))
        code_sha = resolve_code_sha(REPO)
        if args.code_sha is not None and args.code_sha.strip() != code_sha:
            raise hb.HoldoutProbeRefused(
                f"--code-sha ({args.code_sha!r}) does not match computed running code SHA {code_sha}"
            )
        proof, manifest, calendar = hb.prepare_holdout_inputs(
            Path(args.log_dir),
            Path(args.vault_repo),
            manifest_path=Path(args.manifest),
            prereg_sha256=args.prereg_sha256,
            snapshot_as_of_ts=snapshot,
            allow_holdout=bool(args.allow_holdout),
            lo=lo,
            hi=hi,
            calendar_manifest_path=Path(args.calendar_manifest) if args.calendar_manifest else None,
            calendar_sha256=args.calendar_sha256,
            repo_root=REPO,
        )
        hb.check_submissions_not_form4(Path(args.submissions))
        issuers = _issuers(Path(args.issuers))
        by_ticker = {m["ticker"]: m for m in issuers}
        missing_issuers = [t for t in manifest.admitted_issuers if t not in by_ticker or by_ticker[t].get("cik") is None]
        if missing_issuers:
            raise hb.HoldoutProbeRefused(
                f"--issuers is missing CIK mappings for manifest issuers: {missing_issuers[:10]}"
            )
        sec_names = gd4.sec_names_by_ticker(Path(args.sic_map), issuers, pinned=not args.unpinned_sic_map)
        interval = hb.load_c1_interval(Path(args.submissions), issuers)

        vendors = hb.HoldoutVendorFiles(
            Path(args.twelvedata_dir), Path(args.tiingo_meta_dir),
            lo=lo, hi=hi,
            boundary_anchor=calendar.boundary_anchor if lo == hb.HOLDOUT_START else None,
            discovery_frozen_run_at=proof.run_at_dt,
        )
        from scripts.run_real_panel_scan import read_only_engine

        engine = read_only_engine(
            args.statement_timeout_s, "vs1_holdout_basis_probe",
            max_statement_timeout_s=PROBE_MAX_STATEMENT_TIMEOUT_S,
        )

        def progress(i: int, n: int, _t: str) -> None:
            if i % 25 == 0 or i == n:
                _log(f"holdout-probed {i}/{n}")

        try:
            with engine.connect() as conn:
                probe = hb.run_holdout_probe(
                    conn, manifest=manifest, calendar=calendar, lo=lo, hi=hi,
                    as_of_ts=snapshot, vendors=vendors, sec_names=sec_names,
                    interval=interval, progress=progress, proof=proof,
                )
        finally:
            engine.dispose()

        inputs = {
            "manifest_file": Path(args.manifest).name,
            "manifest_file_sha256": manifest.file_sha256,
            "manifest_digest": manifest.digest,
            "calendar_manifest": calendar.path.name,
            "calendar_manifest_sha256": calendar.sha256,
            "twelvedata_fetch_log_sha256": fetch.fetch_log_sha256(Path(args.twelvedata_dir)),
            "tiingo_meta_fetch_log_sha256": fetch.fetch_log_sha256(Path(args.tiingo_meta_dir)),
            "issuers_sha256": gd4.file_sha256(Path(args.issuers)),
            "sic_map_sha256": gd4.file_sha256(Path(args.sic_map)),
            "submissions_sha256": gd4.file_sha256(Path(args.submissions)),
        }
        written = hb.write_holdout_outputs(
            out, probe, proof=proof, manifest=manifest, calendar=calendar, lo=lo, hi=hi,
            code_sha=code_sha, snapshot_as_of_ts=snapshot, inputs=inputs,
        )
    except (hb.HoldoutProbeRefused, FileExistsError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(written, indent=2, sort_keys=True))


if __name__ == "__main__":
    main(sys.argv[1:])
