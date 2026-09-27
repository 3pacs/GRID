"""VS1: Technology insider open-market-buy density vs XLK (panel harness CLI).

Pre-registration: ``docs/paper_log/vs1-insider-density-v1-preregistration.md``
(body sha256 pinned in ``analysis.panel_insider_density.PREREG_BODY_SHA256``).
Every stage refuses to run when the repository pre-registration no longer
hashes to the pinned value. Output directories must be new (write-once).

Inputs (files only, built off-DB on grid-svr):

* ``--form4``: ``/data/sec/form345/derived/nonderiv_transactions.parquet``
  (non-derivative transaction lines x reporting owner).
* ``--submissions``: ``/data/sec/form345/derived/submissions.parquet``, every
  accession of the SUBMISSION table (Section 16 activity, pre-registration
  §2.1/§2.2: holdings-only Form 3s and derivative-only Form 4s included).
  Built from the same quarterly zips by ``scripts/build_form345_submissions.py``.
* ``--issuer-map``: SEC ``company_tickers.json``.
* ``--price-manifest`` and ``--probe-report``: the admitted-price manifest and
  the GD4 probe report it names by sha256.

Stages, in order. Stages 1, 3, 4 and 5 append to the local hash-chained
registry (``--log-dir``); discovery and holdout are one-shot through it:

    # 0. hash of the pre-registration body (prints it; reads nothing else)
    python -m scripts.run_vs1_insider_density hash-prereg

    # 1. register the pre-registration in the local hash-chained registry
    python -m scripts.run_vs1_insider_density register --log-dir DIR --code-sha SHA

    # 2. Stage-0 power gate: Form 4 feature data only, synthetic outcomes, no DB.
    #    Runs only at the pre-registered 200 simulations x 999 sign-flips.
    python -m scripts.run_vs1_insider_density power \
        --form4 .../nonderiv_transactions.parquet --submissions .../submissions.parquet \
        --issuer-map company_tickers.json --out NEW_DIR

    # 3. freeze every input's hash (and as_of_ts) into the registry, before any price read
    python -m scripts.run_vs1_insider_density freeze-inputs --log-dir DIR \
        --form4 ... --submissions ... --issuer-map ... \
        --price-manifest admitted_prices.json --probe-report probe.json \
        --power NEW_DIR/power.json --as-of-ts 2026-09-27T00:00:00+00:00 --code-sha SHA \
        [--accept-underpowered]

    # 4. discovery (2012-01 .. 2019-12): appends discovery_opened before the first
    #    price read (refused if any discovery was opened before), then discovery_frozen
    python -m scripts.run_vs1_insider_density discover --log-dir DIR \
        --form4 ... --submissions ... --issuer-map ... --price-manifest ... \
        --probe-report ... --power NEW_DIR/power.json --out NEW_RUN_DIR

    # 5. holdout (2020-01 .. 2026-06): refused without the flag, the hash and a chain
    #    whose discovery_frozen is this run's file; appends holdout_opened before the
    #    first holdout price read (a second open is refused), then holdout_result
    python -m scripts.run_vs1_insider_density holdout --log-dir DIR --run-dir NEW_RUN_DIR \
        --form4 ... --submissions ... --issuer-map ... --price-manifest ... \
        --probe-report ... --power NEW_DIR/power.json \
        --allow-holdout --prereg-sha256 <pinned body sha256>

``inputs_frozen`` pins the hashes of the price manifest, probe report, Form 4
file, submissions file, issuer map, power.json (content digest), the harness
code files and ``as_of_ts``; discover and holdout recompute them from the files
they are given and refuse any difference, so both run from the frozen code
(check out the frozen ``code_sha``). The read instant is always the frozen
``as_of_ts``. Every refusal happens before the one-shot record is appended.

Database access (stages 4-5 only) is read-only by construction: the S09
``read_only_engine`` (NullPool, ``default_transaction_read_only=on``,
``statement_timeout`` <= 60 s, autocommit), and the only SQL executed is
``store.observations.read_window`` with an explicit ``source=`` for each
admitted ticker. No writes, no migrations, no timers.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from analysis import panel_insider_density as vs1
from analysis.offline_research_proof import digest, stamp

REPO = Path(__file__).resolve().parent.parent
PRICE_WARMUP_DAYS = 60  # >= MOMENTUM_SESSIONS sessions before the window's first decision
CODE_FILES = (
    "analysis/panel_insider_density.py",
    "analysis/offline_research_proof.py",
    "analysis/research_forward_log.py",
    "store/observations.py",
    "scripts/run_vs1_insider_density.py",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _universe(args) -> tuple:
    sector_map = vs1.load_sector_map(REPO)
    issuer_map = vs1.load_issuer_map(Path(args.issuer_map))
    universe, info = vs1.sector_universe(args.sector, sector_map, issuer_map)
    info["issuer_map_sha256"] = vs1.data_sha256(Path(args.issuer_map))
    info["universe_sha256"] = digest(universe.to_dict("records"))
    return universe, info


def _events(args, universe):
    owners = Path(args.owners) if getattr(args, "owners", None) else None
    return vs1.load_events(
        Path(args.form4), owners, issuers=universe["cik"].astype(int), submissions_path=Path(args.submissions)
    )


def _spec(args) -> vs1.RunSpec:
    run_k = vs1.VS1_RUN_K if args.sector == vs1.VS1_SECTOR else vs1.OTHER_SECTORS_RUN_K
    return vs1.RunSpec(
        run_id=f"{vs1.VERSION}:{args.sector}",
        sector=args.sector,
        run_k=run_k,
        trials=vs1.trial_names(),
    )


def _code_files() -> dict:
    return {f: vs1.file_sha256(REPO / f) for f in CODE_FILES}


def _power_inputs(events, info) -> dict:
    return {
        "form4_receipt_sha256": events.receipt_sha256,
        "universe_sha256": info["universe_sha256"],
        "prereg_sha256": vs1.PREREG_BODY_SHA256,
    }


def _load_power(args, events, info) -> dict:
    """The Stage-0 power file: pre-registered settings, computed on these inputs."""
    power = json.loads(Path(args.power).read_text(encoding="utf-8"))
    vs1.verify_power(power)
    if power.get("inputs") != _power_inputs(events, info):
        raise SystemExit("power file was computed on other inputs")
    return power


def _manifest(args) -> vs1.PriceManifest:
    manifest = vs1.PriceManifest.from_file(Path(args.price_manifest))
    probe = vs1.data_sha256(Path(args.probe_report))
    if probe != manifest.probe_report_sha256:
        raise SystemExit("probe report does not hash to the manifest's probe_report_sha256")
    return manifest


def _observed(args, manifest: vs1.PriceManifest, power: dict) -> dict:
    """Hashes of the files this invocation was given (compared with inputs_frozen)."""
    return {
        "sector": args.sector,
        "price_manifest_sha256": manifest.digest(),
        "probe_report_sha256": vs1.data_sha256(Path(args.probe_report)),
        "form4_sha256": vs1.data_sha256(Path(args.form4)),
        "submissions_sha256": vs1.data_sha256(Path(args.submissions)),
        "owners_sha256": vs1.data_sha256(Path(args.owners)) if getattr(args, "owners", None) else None,
        "issuer_map_sha256": vs1.data_sha256(Path(args.issuer_map)),
        "power_sha256": digest(power),
        "code_file_sha256": _code_files(),
    }


def cmd_hash(args) -> None:
    path = Path(args.path) if args.path else REPO / vs1.PREREG_PATH
    actual = vs1.prereg_body_sha256(path)
    print(json.dumps({"path": str(path), "body_sha256": actual,
                      "pinned": vs1.PREREG_BODY_SHA256,
                      "matches_pinned": actual == vs1.PREREG_BODY_SHA256}, indent=2))


def cmd_register(args) -> None:
    vs1.check_prereg(REPO)
    records = vs1.register(Path(args.log_dir), _now(), args.code_sha)
    log = vs1.registry(Path(args.log_dir))
    print(json.dumps({"appended": [r["kind"] for r in records], "chain": log.verify_chain()}, indent=2))


def cmd_power(args) -> None:
    vs1.check_prereg(REPO)
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=False)
    universe, info = _universe(args)
    events = _events(args, universe)
    features = vs1.power_features(events, universe, "discovery")
    power = vs1.stage0_power(features)
    power["inputs"] = _power_inputs(events, info)
    vs1.write_frozen(output, "power.json", power)
    vs1.write_frozen(output, "form4-receipt.json", events.receipt)
    vs1.write_frozen(output, "universe.json", {**info, "members": universe.to_dict("records")})
    summary = {trial: [(r["target_ic"], r["power"], r["usable_dates"]) for r in rows]
               for trial, rows in power["table"].items()}
    print(json.dumps({"gate_passed": power["gate_passed"], "power": summary}, indent=2))


def cmd_freeze_inputs(args) -> None:
    """Pin every input's hash and the read instant in the registry (no price read)."""
    vs1.check_prereg(REPO)
    as_of_ts = stamp(args.as_of_ts)
    manifest = _manifest(args)
    universe, info = _universe(args)
    events = _events(args, universe)
    power = _load_power(args, events, info)
    if not power["gate_passed"] and not args.accept_underpowered:
        raise SystemExit(
            "Stage-0 power gate failed: the pre-registration halts here before any price "
            "read (owner decision: re-register v2 with an expanded universe, or freeze with "
            "--accept-underpowered to run v1 as a declared underpowered calibration run)"
        )
    inputs = {
        **_observed(args, manifest, power),
        "accept_underpowered": bool(args.accept_underpowered),
        "as_of_ts": as_of_ts.isoformat(),
        "code_sha": args.code_sha,
        "form4_receipt_sha256": events.receipt_sha256,
        "universe_sha256": info["universe_sha256"],
    }
    record = vs1.freeze_inputs(Path(args.log_dir), _now(), inputs)
    print(json.dumps({"appended": record["kind"], "inputs": record["inputs"]}, indent=2))


def _prices(args, universe, window: str, key):
    manifest = _manifest(args)
    admitted = universe[universe["ticker"].isin(manifest.admitted)]
    excluded = sorted(set(universe["ticker"]) - set(admitted["ticker"]))
    lo, hi = vs1.window_bounds(window)
    from scripts.run_real_panel_scan import read_only_engine

    engine = read_only_engine(args.statement_timeout_s, "vs1_insider_density")
    try:
        with engine.connect() as conn:
            prices = vs1.load_price_panel(
                conn,
                manifest,
                list(admitted["ticker"]),
                start=lo.date() - timedelta(days=PRICE_WARMUP_DAYS),
                as_of=hi.date() - timedelta(days=1),
                window=window,
                key=key,
            )
    finally:
        engine.dispose()
    return manifest, admitted, excluded, prices


def _ledger_csv(path: Path, ledger: list[dict]) -> None:
    fields = ["trial", "n", "mean_ic", "p", "p_one_sided_positive", "holm_adjusted_p",
              "bh_adjusted_p", "selected", "status", "block"]
    with open(path, "x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in ledger:
            writer.writerow(row)


def cmd_discover(args) -> None:
    vs1.check_prereg(REPO)
    log_dir = Path(args.log_dir)
    output = Path(args.out)
    if output.exists():
        raise SystemExit(f"{output} exists: discovery output directories are write-once")
    # Every check below happens before discovery_opened is appended, so a
    # refused invocation does not consume the one-shot discovery.
    manifest = _manifest(args)
    universe, info = _universe(args)
    events = _events(args, universe)
    power = _load_power(args, events, info)
    observed = _observed(args, manifest, power)
    frozen_inputs = vs1.latest_frozen_inputs(log_dir)
    if not power["gate_passed"] and frozen_inputs["accept_underpowered"] is not True:
        raise SystemExit("Stage-0 power gate failed and inputs_frozen did not accept an underpowered run")
    key = vs1.open_discovery(log_dir, _now(), observed)  # appends discovery_opened
    output.mkdir(parents=True, exist_ok=False)
    _, admitted, excluded, prices = _prices(args, universe, "discovery", key)
    panels = vs1.build_trial_panels(events, admitted, prices, "discovery")
    inputs = {
        "code_sha": key.inputs.get("code_sha"),
        "file_sha256": observed["code_file_sha256"],
        "inputs_frozen_sha256": key.inputs_frozen_sha256,
        "frozen_inputs": key.inputs,
        "sector_map_sha256": vs1.SECTOR_MAP_SHA256,
        "universe": dict(info),
        "price_excluded_not_admitted": excluded,
        "price_manifest_sha256": manifest.digest(),
        "price_receipt_sha256": prices.receipt_sha,
        "form4_receipt_sha256": events.receipt_sha256,
        "power": {"gate_passed": power["gate_passed"],
                  "accept_underpowered": key.inputs["accept_underpowered"],
                  "sha256": digest(power)},
    }
    frozen = vs1.discover_panel(_spec(args), panels, inputs=inputs, repo_root=REPO)
    vs1.write_frozen(output, "discovery-frozen.json", frozen)
    vs1.write_frozen(output, "price-receipt.json", prices.receipt)
    vs1.write_frozen(output, "form4-receipt.json", events.receipt)
    vs1.write_frozen(output, "power.json", power)
    _ledger_csv(output / "ledger.csv", frozen["payload"]["ledger"])
    vs1.seal_discovery(log_dir, _now(), key, frozen)  # appends discovery_frozen
    print(json.dumps({"sha256": frozen["sha256"], "calibration": frozen["payload"]["calibration"],
                      "selected": [t["trial"] for t in frozen["payload"]["ledger"] if t["selected"]]},
                     indent=2))


def cmd_holdout(args) -> None:
    run_dir = Path(args.run_dir)
    frozen = json.loads((run_dir / "discovery-frozen.json").read_text(encoding="utf-8"))
    payload = vs1.check_holdout_request(frozen, allow_holdout=args.allow_holdout,
                                        prereg_sha256=args.prereg_sha256, repo_root=REPO)
    args.sector = payload["spec"]["sector"]
    # Every input check happens before holdout_opened is appended and before
    # any holdout price is read.
    universe, info = _universe(args)
    events = _events(args, universe)
    if events.receipt_sha256 != payload["inputs"]["form4_receipt_sha256"] or (
        info["universe_sha256"] != payload["inputs"]["universe"]["universe_sha256"]
    ):
        raise SystemExit("Form 4 events or universe differ from the frozen discovery")
    manifest = _manifest(args)
    if manifest.digest() != payload["inputs"]["price_manifest_sha256"]:
        raise SystemExit("price manifest differs from the frozen discovery")
    power = _load_power(args, events, info)
    if digest(power) != payload["inputs"]["power"]["sha256"]:
        raise SystemExit("power file differs from the frozen discovery")
    if power.get("gate_passed") is not True and not payload["inputs"]["power"]["accept_underpowered"]:
        raise SystemExit("power record inconsistent with the frozen discovery")
    observed = _observed(args, manifest, power)
    key = vs1.open_holdout(frozen, allow_holdout=args.allow_holdout, prereg_sha256=args.prereg_sha256,
                           log_dir=Path(args.log_dir), now=_now(), observed=observed,
                           repo_root=REPO)  # appends holdout_opened
    _, admitted, _, prices = _prices(args, universe, "holdout", key)
    panels = vs1.build_trial_panels(events, admitted, prices, "holdout")
    result = vs1.evaluate_panel_holdout(frozen, panels, key, power=power)
    result["price_receipt_sha256"] = prices.receipt_sha
    vs1.write_frozen(run_dir, "holdout-result.json", result)
    vs1.write_frozen(run_dir, "holdout-price-receipt.json", prices.receipt)
    vs1.seal_holdout(Path(args.log_dir), _now(), key, result)  # appends holdout_result
    print(json.dumps(result["verdict"], indent=2))


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("hash-prereg")
    p.add_argument("path", nargs="?")
    p.set_defaults(func=cmd_hash)

    p = sub.add_parser("register")
    p.add_argument("--log-dir", required=True)
    p.add_argument("--code-sha", required=True)
    p.set_defaults(func=cmd_register)

    def inputs(p, prices: bool) -> None:
        p.add_argument("--form4", required=True)
        p.add_argument("--submissions", required=True,
                       help="derived SUBMISSION table (scripts/build_form345_submissions.py)")
        p.add_argument("--owners", help="REPORTINGOWNER table, only if --form4 lacks owner CIKs")
        p.add_argument("--issuer-map", required=True, help="SEC company_tickers.json or a ticker,cik CSV")
        p.add_argument("--sector", default=vs1.VS1_SECTOR, choices=[vs1.VS1_SECTOR])
        if prices:
            p.add_argument("--log-dir", required=True, help="the registry directory (hash chain)")
            p.add_argument("--price-manifest", required=True)
            p.add_argument("--probe-report", required=True)
            p.add_argument("--power", required=True)

    p = sub.add_parser("power")
    inputs(p, prices=False)
    p.add_argument("--out", required=True)
    p.set_defaults(func=cmd_power)

    p = sub.add_parser("freeze-inputs")
    inputs(p, prices=True)
    p.add_argument("--as-of-ts", required=True, help="price read instant (ISO, tz-aware, not in the future)")
    p.add_argument("--code-sha", required=True)
    p.add_argument("--accept-underpowered", action="store_true")
    p.set_defaults(func=cmd_freeze_inputs)

    p = sub.add_parser("discover")
    inputs(p, prices=True)
    p.add_argument("--out", required=True)
    p.add_argument("--statement-timeout-s", type=int, default=60)
    p.set_defaults(func=cmd_discover)

    p = sub.add_parser("holdout")
    inputs(p, prices=True)
    p.add_argument("--run-dir", required=True)
    p.add_argument("--allow-holdout", action="store_true")
    p.add_argument("--prereg-sha256", required=True)
    p.add_argument("--statement-timeout-s", type=int, default=60)
    p.set_defaults(func=cmd_holdout)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
