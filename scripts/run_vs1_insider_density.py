"""VS1: Technology insider open-market-buy density vs XLK (panel harness CLI).

Pre-registration: ``docs/paper_log/vs1-insider-density-v1-preregistration.md``
(body sha256 pinned in ``analysis.panel_insider_density.PREREG_BODY_SHA256``).
Every stage refuses to run when the repository pre-registration no longer
hashes to the pinned value. Output directories must be new (write-once).

Stages, in order:

    # 0. hash of the pre-registration body (prints it; reads nothing else)
    python -m scripts.run_vs1_insider_density hash-prereg

    # 1. register the pre-registration in the local hash-chained registry
    python -m scripts.run_vs1_insider_density register --log-dir DIR --code-sha SHA

    # 2. Stage-0 power gate: Form 4 feature data only, synthetic outcomes, no DB
    python -m scripts.run_vs1_insider_density power \
        --form4 /data/sec/form345/derived/nonderiv_transactions.parquet \
        --issuer-map company_tickers.json --out NEW_DIR

    # 3. discovery (2012-01 .. 2019-12): prices read only up to the split
    python -m scripts.run_vs1_insider_density discover --form4 ... --issuer-map ... \
        --price-manifest admitted_prices.json --power NEW_DIR/power.json \
        --code-sha SHA --out NEW_RUN_DIR

    # 4. holdout (2020-01 .. 2026-06): refused without both of these
    python -m scripts.run_vs1_insider_density holdout --run-dir NEW_RUN_DIR \
        --form4 ... --issuer-map ... --price-manifest ... \
        --allow-holdout --prereg-sha256 <pinned body sha256>

Database access (stages 3-4 only) is read-only by construction: the S09
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
from dataclasses import asdict
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


def _universe(args) -> tuple:
    sector_map = vs1.load_sector_map(REPO)
    issuer_map = vs1.load_issuer_map(Path(args.issuer_map))
    universe, info = vs1.sector_universe(args.sector, sector_map, issuer_map)
    info["issuer_map_sha256"] = vs1.data_sha256(Path(args.issuer_map))
    info["universe_sha256"] = digest(universe.to_dict("records"))
    return universe, info


def _events(args, universe):
    owners = Path(args.owners) if getattr(args, "owners", None) else None
    return vs1.load_events(Path(args.form4), owners, issuers=universe["cik"].astype(int))


def _spec(args) -> vs1.RunSpec:
    run_k = vs1.VS1_RUN_K if args.sector == vs1.VS1_SECTOR else vs1.OTHER_SECTORS_RUN_K
    return vs1.RunSpec(
        run_id=f"{vs1.VERSION}:{args.sector}",
        sector=args.sector,
        run_k=run_k,
        trials=vs1.trial_names(),
    )


def _code(code_sha: str) -> dict:
    return {
        "code_sha": code_sha,
        "file_sha256": {f: vs1.file_sha256(REPO / f) for f in CODE_FILES},
    }


def cmd_hash(args) -> None:
    path = Path(args.path) if args.path else REPO / vs1.PREREG_PATH
    actual = vs1.prereg_body_sha256(path)
    print(json.dumps({"path": str(path), "body_sha256": actual,
                      "pinned": vs1.PREREG_BODY_SHA256,
                      "matches_pinned": actual == vs1.PREREG_BODY_SHA256}, indent=2))


def cmd_register(args) -> None:
    vs1.check_prereg(REPO)
    records = vs1.register(Path(args.log_dir), datetime.now(timezone.utc), args.code_sha)
    log = vs1.registry(Path(args.log_dir))
    print(json.dumps({"appended": [r["kind"] for r in records], "chain": log.verify_chain()}, indent=2))


def cmd_power(args) -> None:
    vs1.check_prereg(REPO)
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=False)
    universe, info = _universe(args)
    events = _events(args, universe)
    features = vs1.power_features(events, universe, "discovery")
    power = vs1.stage0_power(features, sims=args.sims, perms=args.perms)
    power["inputs"] = {
        "form4_receipt_sha256": events.receipt_sha256,
        "universe_sha256": info["universe_sha256"],
        "prereg_sha256": vs1.PREREG_BODY_SHA256,
    }
    vs1.write_frozen(output, "power.json", power)
    vs1.write_frozen(output, "form4-receipt.json", events.receipt)
    vs1.write_frozen(output, "universe.json", {**info, "members": universe.to_dict("records")})
    summary = {trial: [(r["target_ic"], r["power"], r["usable_dates"]) for r in rows]
               for trial, rows in power["table"].items()}
    print(json.dumps({"gate_passed": power["gate_passed"], "power": summary}, indent=2))


def _prices(args, universe, window: str, key=None):
    manifest = vs1.PriceManifest.from_file(Path(args.price_manifest))
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
                as_of_ts=stamp(args.as_of_ts) if args.as_of_ts else datetime.now(timezone.utc),
                window=window,
                holdout_key=key,
            )
    finally:
        engine.dispose()
    return manifest, admitted, excluded, prices


def _manifest_sha(manifest: vs1.PriceManifest) -> str:
    return digest({**asdict(manifest), "admitted": list(manifest.admitted)})


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
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=False)
    universe, info = _universe(args)
    events = _events(args, universe)
    power = json.loads(Path(args.power).read_text(encoding="utf-8")) if args.power else None
    if power is None:
        raise SystemExit("Stage-0 power file required (run the power stage first)")
    if power["inputs"] != {
        "form4_receipt_sha256": events.receipt_sha256,
        "universe_sha256": info["universe_sha256"],
        "prereg_sha256": vs1.PREREG_BODY_SHA256,
    }:
        raise SystemExit("power file was computed on other inputs")
    if not power["gate_passed"] and not args.accept_underpowered:
        raise SystemExit(
            "Stage-0 power gate failed: the pre-registration halts here before any price "
            "read (owner decision: re-register v2 with an expanded universe, or rerun with "
            "--accept-underpowered to run v1 as a declared underpowered calibration run)"
        )
    manifest, admitted, excluded, prices = _prices(args, universe, "discovery")
    panels = vs1.build_trial_panels(events, admitted, prices, "discovery")
    inputs = {
        **_code(args.code_sha),
        "sector_map_sha256": vs1.SECTOR_MAP_SHA256,
        "universe": dict(info),
        "price_excluded_not_admitted": excluded,
        "price_manifest_sha256": _manifest_sha(manifest),
        "price_receipt_sha256": prices.receipt_sha,
        "form4_receipt_sha256": events.receipt_sha256,
        "power": {"gate_passed": power["gate_passed"], "accept_underpowered": bool(args.accept_underpowered),
                  "sha256": digest(power)},
    }
    frozen = vs1.discover_panel(_spec(args), panels, inputs=inputs, repo_root=REPO)
    vs1.write_frozen(output, "discovery-frozen.json", frozen)
    vs1.write_frozen(output, "price-receipt.json", prices.receipt)
    vs1.write_frozen(output, "form4-receipt.json", events.receipt)
    vs1.write_frozen(output, "power.json", power)
    _ledger_csv(output / "ledger.csv", frozen["payload"]["ledger"])
    print(json.dumps({"sha256": frozen["sha256"], "calibration": frozen["payload"]["calibration"],
                      "selected": [t["trial"] for t in frozen["payload"]["ledger"] if t["selected"]]},
                     indent=2))


def cmd_holdout(args) -> None:
    run_dir = Path(args.run_dir)
    frozen = json.loads((run_dir / "discovery-frozen.json").read_text(encoding="utf-8"))
    key = vs1.open_holdout(frozen, allow_holdout=args.allow_holdout, prereg_sha256=args.prereg_sha256,
                           repo_root=REPO)
    payload = frozen["payload"]
    args.sector = payload["spec"]["sector"]
    universe, info = _universe(args)
    events = _events(args, universe)
    if events.receipt_sha256 != payload["inputs"]["form4_receipt_sha256"] or (
        info["universe_sha256"] != payload["inputs"]["universe"]["universe_sha256"]
    ):
        raise SystemExit("Form 4 events or universe differ from the frozen discovery")
    manifest, admitted, _, prices = _prices(args, universe, "holdout", key)
    if _manifest_sha(manifest) != payload["inputs"]["price_manifest_sha256"]:
        raise SystemExit("price manifest differs from the frozen discovery")
    panels = vs1.build_trial_panels(events, admitted, prices, "holdout")
    power = json.loads((run_dir / "power.json").read_text(encoding="utf-8"))
    if power.get("gate_passed") is not True and not payload["inputs"]["power"]["accept_underpowered"]:
        raise SystemExit("power record inconsistent with the frozen discovery")
    result = vs1.evaluate_panel_holdout(frozen, panels, key, power=power)
    result["price_receipt_sha256"] = prices.receipt_sha
    vs1.write_frozen(run_dir, "holdout-result.json", result)
    vs1.write_frozen(run_dir, "holdout-price-receipt.json", prices.receipt)
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
        p.add_argument("--owners", help="REPORTINGOWNER table, only if --form4 lacks owner CIKs")
        p.add_argument("--issuer-map", required=True, help="SEC company_tickers.json or a ticker,cik CSV")
        p.add_argument("--sector", default=vs1.VS1_SECTOR, choices=sorted(vs1.EQUITY_SECTORS))
        if prices:
            p.add_argument("--price-manifest", required=True)
            p.add_argument("--as-of-ts", help="read instant (ISO, tz-aware); default now")
            p.add_argument("--statement-timeout-s", type=int, default=60)

    p = sub.add_parser("power")
    inputs(p, prices=False)
    p.add_argument("--out", required=True)
    p.add_argument("--sims", type=int, default=vs1.POWER_SIMS)
    p.add_argument("--perms", type=int, default=vs1.POWER_PERMS)
    p.set_defaults(func=cmd_power)

    p = sub.add_parser("discover")
    inputs(p, prices=True)
    p.add_argument("--out", required=True)
    p.add_argument("--power", required=True)
    p.add_argument("--code-sha", required=True)
    p.add_argument("--accept-underpowered", action="store_true")
    p.set_defaults(func=cmd_discover)

    p = sub.add_parser("holdout")
    inputs(p, prices=True)
    p.add_argument("--run-dir", required=True)
    p.add_argument("--allow-holdout", action="store_true")
    p.add_argument("--prereg-sha256", required=True)
    p.set_defaults(func=cmd_holdout)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
