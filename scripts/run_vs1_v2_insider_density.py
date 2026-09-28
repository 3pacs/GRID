"""VS1 v2: SIC-expanded Technology insider open-market-buy density vs XLK (panel harness CLI).

Pre-registration: ``docs/paper_log/vs1-insider-density-v2-preregistration.md``
(body sha256 pinned in ``analysis.panel_insider_density_v2.PREREG_BODY_SHA256``).
This is a separate registry from VS1 v1 (``scripts/run_vs1_insider_density.py``,
left unchanged): its own hash chain (``granular_panel_prereg_v2.jsonl``), its own
pinned off-host witness (``05-GRID/Paper-Log/vs1/granular_panel_prereg_v2.anchors.jsonl``
on ``main`` of the GitHub vault), the same stage order and the same one-shot rules.

Inputs (files only, built off-DB on grid-svr):

* ``--form4``: ``/data/sec/form345/derived/nonderiv_transactions.parquet``.
* ``--submissions``: ``/data/sec/form345/derived/submissions.parquet`` (every accession,
  with ``issuer_ticker`` = ISSUERTRADINGSYMBOL as filed: the ticker rule's input).
* ``--issuer-map``: the pinned SEC ``company_tickers.json`` (sha256
  ``016ae8ff...``, the file v1's Stage-0 used).
* ``--sic-map``: the pinned ``issuer_sic_map.jsonl`` from ``scripts/fetch_sec_issuer_sic.py``.
* ``--price-manifest`` and ``--probe-report`` (price stages only).

Stages (``...`` = the same input flags):

    python -m scripts.run_vs1_v2_insider_density hash-prereg
    python -m scripts.run_vs1_v2_insider_density register --log-dir DIR   # re-materialise the pinned one
    python -m scripts.run_vs1_v2_insider_density power ... --out NEW_DIR  # Stage-0, no DB, no price
    python -m scripts.run_vs1_v2_insider_density freeze-inputs --log-dir DIR ... --power NEW_DIR/power.json \\
        --as-of-ts ISO --code-sha SHA [--accept-underpowered]
    python -m scripts.run_vs1_v2_insider_density open-discovery --log-dir DIR ... --vault-repo CLONE \\
        [--vault-worktree VAULT_WORKTREE]
    # commit + push the v2 witness file to vault main, then:
    python -m scripts.run_vs1_v2_insider_density discover --log-dir DIR ... --vault-repo CLONE --out RUN_DIR
    python -m scripts.run_vs1_v2_insider_density open-holdout ... --run-dir RUN_DIR --allow-holdout \\
        --prereg-sha256 <pinned v2 body sha256> [--vault-worktree VAULT_WORKTREE]
    python -m scripts.run_vs1_v2_insider_density holdout ... --run-dir RUN_DIR --allow-holdout \\
        --prereg-sha256 <pinned v2 body sha256> --vault-repo CLONE
    python -m scripts.run_vs1_v2_insider_density export-anchors --log-dir DIR --vault-worktree VAULT_WORKTREE

The same CLI drives VS1 v3 (``scripts/run_vs1_v3_insider_density.py`` passes the v3
harness). v2 is superseded by v3: its ``open-discovery`` and ``discover`` refuse
(pinned ``SUPERSEDED_BY``); its remaining use is ``hash-prereg``, ``register`` (a copy
of the pinned registration) and ``power`` (the Stage-0 record). For any version,
``open-discovery`` and ``discover`` refuse unless the version is not superseded, no
later version's witness file is on the pinned vault ``main``, and every earlier
version's witness still covers only its 2 registration records.

Database access (discover and holdout only) is read-only by construction (the S09
``read_only_engine``) and limited to ``store.observations.read_window`` with an
explicit ``source=`` for admitted tickers. No writes, no migrations, no timers.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis.offline_research_proof import digest, stamp

REPO = Path(__file__).resolve().parent.parent
PRICE_WARMUP_DAYS = 60
CODE_FILES = (
    "analysis/panel_insider_density.py",
    "analysis/panel_insider_density_v2.py",
    "analysis/panel_insider_density_v3.py",
    "analysis/offline_research_proof.py",
    "analysis/research_forward_log.py",
    "store/observations.py",
    "scripts/run_vs1_v2_insider_density.py",
    "scripts/run_vs1_v3_insider_density.py",
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _universe(args) -> tuple:
    h = args.h
    sector_map = v1.load_sector_map(REPO)
    issuer_map = h.load_issuer_map(Path(args.issuer_map))
    sic_map = h.load_sic_map(Path(args.sic_map))
    universe, info = h.v2_universe(sector_map, issuer_map, sic_map)
    info["issuer_map_sha256"] = v1.data_sha256(Path(args.issuer_map))
    info["sic_map_sha256"] = v1.data_sha256(Path(args.sic_map))
    info["universe_sha256"] = digest(universe.to_dict("records"))
    return universe, info


def _events(args, universe):
    owners = Path(args.owners) if getattr(args, "owners", None) else None
    return args.h.load_inputs(Path(args.form4), Path(args.submissions), universe, owners)


def _code_files() -> dict:
    return {f: v1.file_sha256(REPO / f) for f in CODE_FILES}


def _power_inputs(h, events, admission, info) -> dict:
    return {
        "form4_receipt_sha256": events.receipt_sha256,
        "admission_receipt_sha256": admission.receipt_sha256,
        "universe_sha256": info["universe_sha256"],
        "prereg_sha256": h.PREREG_BODY_SHA256,
    }


def _load_power(args, events, admission, info) -> dict:
    power = json.loads(Path(args.power).read_text(encoding="utf-8"))
    args.h.verify_power(power)
    if power.get("inputs") != _power_inputs(args.h, events, admission, info):
        raise SystemExit("power file was computed on other inputs")
    return power


def _manifest(args) -> v2.PriceManifest:
    manifest = args.h.PriceManifest.from_file(Path(args.price_manifest))
    if v1.data_sha256(Path(args.probe_report)) != manifest.probe_report_sha256:
        raise SystemExit("probe report does not hash to the manifest's probe_report_sha256")
    return manifest


def _observed(args, manifest: v2.PriceManifest, power: dict) -> dict:
    return {
        "sector": v1.VS1_SECTOR,
        "price_manifest_sha256": manifest.digest(),
        "probe_report_sha256": v1.data_sha256(Path(args.probe_report)),
        "form4_sha256": v1.data_sha256(Path(args.form4)),
        "submissions_sha256": v1.data_sha256(Path(args.submissions)),
        "owners_sha256": v1.data_sha256(Path(args.owners)) if getattr(args, "owners", None) else None,
        "issuer_map_sha256": v1.data_sha256(Path(args.issuer_map)),
        "sic_map_sha256": v1.data_sha256(Path(args.sic_map)),
        "power_sha256": digest(power),
        "code_file_sha256": _code_files(),
    }


def cmd_hash(args) -> None:
    h = args.h
    path = Path(args.path) if args.path else REPO / h.PREREG_PATH
    actual = h.prereg_body_sha256(path)
    print(json.dumps({"path": str(path), "body_sha256": actual, "pinned": h.PREREG_BODY_SHA256,
                      "matches_pinned": actual == h.PREREG_BODY_SHA256}, indent=2))


def cmd_register(args) -> None:
    """The one v2 registration (before the pins exist), or a copy of the pinned one."""
    h = args.h
    h.check_prereg(REPO)
    if h.REGISTERED_RECORD_SHA256 is None:
        if not args.code_sha:
            raise SystemExit("the first v2 registration needs --code-sha (the commit carrying the final text)")
        records = h.register(Path(args.log_dir), _now(), args.code_sha)
    else:
        records = h.register(Path(args.log_dir), h.REGISTERED_AT, h.REGISTERED_CODE_SHA)
    log = h.registry(Path(args.log_dir))
    print(json.dumps({"appended": [r["kind"] for r in records], "chain": log.verify_chain(),
                      "anchor_line": (Path(args.log_dir) / h.REGISTRY_ANCHORS).read_text(encoding="utf-8")},
                     indent=2))


def _print_witness_instructions(opened: dict, args) -> None:
    h = args.h
    appended = h.export_anchors(Path(args.log_dir), Path(args.vault_worktree)) if args.vault_worktree else []
    print(json.dumps({
        "appended": opened["kind"], "records": opened["records"], "head_sha256": opened["head_sha256"],
        "witness": f"{h.WITNESS_REMOTE_URL} {h.WITNESS_BRANCH}:{h.WITNESS_PATH}",
        "anchor_lines_written": appended,
        "next": f"commit {h.WITNESS_PATH} in the vault worktree and push it to {h.WITNESS_BRANCH}, then run "
                + ("discover" if opened["kind"] == "discovery_opened" else "holdout"),
    }, indent=2))


def cmd_export_anchors(args) -> None:
    h = args.h
    appended = h.export_anchors(Path(args.log_dir), Path(args.vault_worktree))
    print(json.dumps({"anchor_lines_written": appended,
                      "next": f"commit {h.WITNESS_PATH} and push it to {h.WITNESS_BRANCH}"}, indent=2))


def cmd_power(args) -> None:
    h = args.h
    h.check_prereg(REPO)
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=False)
    universe, info = _universe(args)
    events, admission = _events(args, universe)
    power = h.stage0_power(h.power_features(events, admission, universe, "discovery"))
    power["inputs"] = _power_inputs(h, events, admission, info)
    report = h.admission_report(events, admission, universe, "discovery")
    h.write_frozen(output, "power.json", power)
    h.write_frozen(output, "form4-receipt.json", events.receipt)
    h.write_frozen(output, "admission-receipt.json", admission.receipt)
    h.write_frozen(output, "admission-report.json", report)
    members = universe.assign(current_tickers=universe["current_tickers"].map(list)).to_dict("records")
    h.write_frozen(output, "universe.json", {**info, "members": members})
    summary = {trial: [(r["target_ic"], r["power"], r["usable_dates"]) for r in rows]
               for trial, rows in power["table"].items()}
    print(json.dumps({"gate_passed": power["gate_passed"], "power": summary, "admission": report}, indent=2))


def cmd_freeze_inputs(args) -> None:
    h = args.h
    h.check_prereg(REPO)
    as_of_ts = stamp(args.as_of_ts)
    manifest = _manifest(args)
    universe, info = _universe(args)
    events, admission = _events(args, universe)
    power = _load_power(args, events, admission, info)
    if not power["gate_passed"] and not args.accept_underpowered:
        raise SystemExit(
            "Stage-0 power gate failed: the v2 pre-registration halts here before any price read "
            "(owner decision, v2 section 10)"
        )
    inputs = {
        **_observed(args, manifest, power),
        "accept_underpowered": bool(args.accept_underpowered),
        "as_of_ts": as_of_ts.isoformat(),
        "code_sha": args.code_sha,
        "form4_receipt_sha256": events.receipt_sha256,
        "admission_receipt_sha256": admission.receipt_sha256,
        "universe_sha256": info["universe_sha256"],
    }
    record = h.freeze_inputs(Path(args.log_dir), _now(), inputs)
    print(json.dumps({"appended": record["kind"], "inputs": record["inputs"]}, indent=2))


def cmd_open_discovery(args) -> None:
    h = args.h
    h.check_prereg(REPO)
    manifest = _manifest(args)
    universe, info = _universe(args)
    events, admission = _events(args, universe)
    power = _load_power(args, events, admission, info)
    frozen_inputs = h.latest_frozen_inputs(Path(args.log_dir))
    if not power["gate_passed"] and frozen_inputs["accept_underpowered"] is not True:
        raise SystemExit("Stage-0 power gate failed and inputs_frozen did not accept an underpowered run")
    witness = h.check_offhost(Path(args.vault_repo))  # the pinned vault main (earlier versions included)
    opened = h.open_discovery(Path(args.log_dir), _now(), _observed(args, manifest, power), witness)
    _print_witness_instructions(opened, args)


def _prices(args, universe, window: str, key):
    h = args.h
    manifest = _manifest(args)
    admitted = universe[universe["ticker"].isin(manifest.admitted)]
    excluded = sorted(set(universe["ticker"]) - set(admitted["ticker"]))
    lo, hi = v1.window_bounds(window)
    from scripts.run_real_panel_scan import read_only_engine

    engine = read_only_engine(args.statement_timeout_s, "vs1_v2_insider_density")
    try:
        with engine.connect() as conn:
            prices = h.load_price_panel(conn, manifest, list(admitted["ticker"]),
                                         start=lo.date() - timedelta(days=PRICE_WARMUP_DAYS),
                                         as_of=hi.date() - timedelta(days=1), window=window, key=key)
    finally:
        engine.dispose()
    return manifest, admitted, excluded, prices


def _ledger_csv(path: Path, ledger: list[dict]) -> None:
    fields = ["trial", "n", "mean_ic", "p", "p_one_sided_positive", "p_one_sided_negative", "holm_adjusted_p",
              "bh_adjusted_p", "selected", "status", "block"]
    with open(path, "x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in ledger:
            writer.writerow(row)


def cmd_discover(args) -> None:
    h = args.h
    h.check_prereg(REPO)
    log_dir, output = Path(args.log_dir), Path(args.out)
    if output.exists():
        raise SystemExit(f"{output} exists: discovery output directories are write-once")
    manifest = _manifest(args)
    universe, info = _universe(args)
    events, admission = _events(args, universe)
    power = _load_power(args, events, admission, info)
    observed = _observed(args, manifest, power)
    witness = h.check_offhost(Path(args.vault_repo))  # the pinned vault main (earlier versions included)
    key = h.resume_discovery(log_dir, observed, witness)
    if not power["gate_passed"] and key.inputs["accept_underpowered"] is not True:
        raise SystemExit("Stage-0 power gate failed and inputs_frozen did not accept an underpowered run")
    output.mkdir(parents=True, exist_ok=False)
    _, admitted, excluded, prices = _prices(args, universe, "discovery", key)
    panels = h.build_trial_panels(events, admission, admitted, prices, "discovery")
    inputs = {
        "code_sha": key.inputs.get("code_sha"),
        "file_sha256": observed["code_file_sha256"],
        "inputs_frozen_sha256": key.inputs_frozen_sha256,
        "frozen_inputs": key.inputs,
        "offhost_witness": witness.receipt(),
        "sector_map_sha256": v1.SECTOR_MAP_SHA256,
        "universe": {k: v for k, v in info.items() if k != "members"},
        "price_excluded_not_admitted": excluded,
        "price_manifest_sha256": manifest.digest(),
        "price_receipt_sha256": prices.receipt_sha,
        "form4_receipt_sha256": events.receipt_sha256,
        "admission_receipt_sha256": admission.receipt_sha256,
        "power": {"gate_passed": power["gate_passed"], "accept_underpowered": key.inputs["accept_underpowered"],
                  "sha256": digest(power)},
    }
    frozen = h.discover_panel(h.run_spec(), panels, inputs=inputs, repo_root=REPO)
    h.write_frozen(output, "discovery-frozen.json", frozen)
    h.write_frozen(output, "price-receipt.json", prices.receipt)
    h.write_frozen(output, "form4-receipt.json", events.receipt)
    h.write_frozen(output, "admission-receipt.json", admission.receipt)
    h.write_frozen(output, "power.json", power)
    _ledger_csv(output / "ledger.csv", frozen["payload"]["ledger"])
    h.seal_discovery(log_dir, _now(), key, frozen)
    print(json.dumps({"sha256": frozen["sha256"], "calibration": frozen["payload"]["calibration"]["state"],
                      "selected": [t["trial"] for t in frozen["payload"]["ledger"] if t["selected"]],
                      "next": "export the registry's new anchor line (discovery_frozen) to the off-host log"},
                     indent=2))


def _holdout_inputs(args):
    h = args.h
    run_dir = Path(args.run_dir)
    frozen = json.loads((run_dir / "discovery-frozen.json").read_text(encoding="utf-8"))
    payload = h.check_holdout_request(frozen, allow_holdout=args.allow_holdout,
                                       prereg_sha256=args.prereg_sha256, repo_root=REPO)
    universe, info = _universe(args)
    events, admission = _events(args, universe)
    if (events.receipt_sha256 != payload["inputs"]["form4_receipt_sha256"]
            or admission.receipt_sha256 != payload["inputs"]["admission_receipt_sha256"]
            or info["universe_sha256"] != payload["inputs"]["universe"]["universe_sha256"]):
        raise SystemExit("Form 4 events, admission or universe differ from the frozen discovery")
    manifest = _manifest(args)
    if manifest.digest() != payload["inputs"]["price_manifest_sha256"]:
        raise SystemExit("price manifest differs from the frozen discovery")
    power = _load_power(args, events, admission, info)
    if digest(power) != payload["inputs"]["power"]["sha256"]:
        raise SystemExit("power file differs from the frozen discovery")
    if power.get("gate_passed") is not True and not payload["inputs"]["power"]["accept_underpowered"]:
        raise SystemExit("power record inconsistent with the frozen discovery")
    return frozen, universe, events, admission, power, _observed(args, manifest, power)


def cmd_open_holdout(args) -> None:
    h = args.h
    frozen, _, _, _, _, observed = _holdout_inputs(args)
    opened = h.open_holdout(frozen, allow_holdout=args.allow_holdout, prereg_sha256=args.prereg_sha256,
                             log_dir=Path(args.log_dir), now=_now(), observed=observed, repo_root=REPO)
    _print_witness_instructions(opened, args)


def cmd_holdout(args) -> None:
    h = args.h
    run_dir = Path(args.run_dir)
    frozen, universe, events, admission, power, observed = _holdout_inputs(args)
    witness = h.check_offhost(Path(args.vault_repo))
    key = h.resume_holdout(frozen, allow_holdout=args.allow_holdout, prereg_sha256=args.prereg_sha256,
                            log_dir=Path(args.log_dir), observed=observed, witness=witness, repo_root=REPO)
    _, admitted, _, prices = _prices(args, universe, "holdout", key)
    panels = h.build_trial_panels(events, admission, admitted, prices, "holdout")
    result = h.evaluate_panel_holdout(frozen, panels, key, power=power)
    result["price_receipt_sha256"] = prices.receipt_sha
    h.write_frozen(run_dir, "holdout-result.json", result)
    h.write_frozen(run_dir, "holdout-price-receipt.json", prices.receipt)
    h.seal_holdout(Path(args.log_dir), _now(), key, result)
    print(json.dumps(result["verdict"], indent=2))


def main(argv: list[str] | None = None, h=v2) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("hash-prereg")
    p.add_argument("path", nargs="?")
    p.set_defaults(func=cmd_hash)

    p = sub.add_parser("register", help="the one v2 registration, or a copy of the pinned one")
    p.add_argument("--log-dir", required=True)
    p.add_argument("--code-sha", help="only for the first registration (before the pins exist)")
    p.set_defaults(func=cmd_register)

    def inputs(p, prices: bool) -> None:
        p.add_argument("--form4", required=True)
        p.add_argument("--submissions", required=True)
        p.add_argument("--owners", help="REPORTINGOWNER table, only if --form4 lacks owner CIKs")
        p.add_argument("--issuer-map", required=True, help="the pinned SEC company_tickers.json")
        p.add_argument("--sic-map", required=True, help="the pinned issuer_sic_map.jsonl")
        if prices:
            p.add_argument("--log-dir", required=True)
            p.add_argument("--price-manifest", required=True)
            p.add_argument("--probe-report", required=True)
            p.add_argument("--power", required=True)

    def holdout_request(p) -> None:
        p.add_argument("--run-dir", required=True)
        p.add_argument("--allow-holdout", action="store_true")
        p.add_argument("--prereg-sha256", required=True)

    p = sub.add_parser("export-anchors")
    p.add_argument("--log-dir", required=True)
    p.add_argument("--vault-worktree", required=True)
    p.set_defaults(func=cmd_export_anchors)

    p = sub.add_parser("power")
    inputs(p, prices=False)
    p.add_argument("--out", required=True)
    p.set_defaults(func=cmd_power)

    p = sub.add_parser("freeze-inputs")
    inputs(p, prices=True)
    p.add_argument("--as-of-ts", required=True)
    p.add_argument("--code-sha", required=True)
    p.add_argument("--accept-underpowered", action="store_true")
    p.set_defaults(func=cmd_freeze_inputs)

    p = sub.add_parser("open-discovery", help="append discovery_opened; reads no price")
    inputs(p, prices=True)
    p.add_argument("--vault-repo", required=True, help="local git repo to fetch the pinned vault main into")
    p.add_argument("--vault-worktree")
    p.set_defaults(func=cmd_open_discovery)

    p = sub.add_parser("discover")
    inputs(p, prices=True)
    p.add_argument("--vault-repo", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--statement-timeout-s", type=int, default=60)
    p.set_defaults(func=cmd_discover)

    p = sub.add_parser("open-holdout", help="append holdout_opened; reads no price")
    inputs(p, prices=True)
    holdout_request(p)
    p.add_argument("--vault-worktree")
    p.set_defaults(func=cmd_open_holdout)

    p = sub.add_parser("holdout")
    inputs(p, prices=True)
    holdout_request(p)
    p.add_argument("--vault-repo", required=True)
    p.add_argument("--statement-timeout-s", type=int, default=60)
    p.set_defaults(func=cmd_holdout)

    parser.set_defaults(h=h)
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main(sys.argv[1:])
