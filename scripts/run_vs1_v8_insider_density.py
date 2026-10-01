"""VS1 v8 one-shot research CLI. Bind the v7 STOP head and v8 registry/witness pins before use.

The v8 discovery start (2008-01-01) is scoped to this invocation; v1-v7 keep theirs.
The power command needs the witnessed v8 registration and exact earlier-window
non-outcome admission reports. No command here silently opens a holdout.
``register`` is a dry run unless ``--execute``. ``stop`` (dry run unless ``--execute``)
and ``verify-stop`` record and check the terminal STOP when either gated Stage-0
model is below 0.50.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from analysis import panel_insider_density_v8 as v8
from scripts.run_vs1_v2_insider_density import main as _main
from scripts.run_vs1_v7_insider_density import _drop_pairs, _option

PROBE_START = date(2007, 11, 2)
PROBE_END = date(2019, 12, 31)


def check_early_probe(probe_path: Path, crosscheck_path: Path) -> None:
    """Old v6/v7 window receipts refuse even if their manifest names the same tickers."""
    probe = json.loads(Path(probe_path).read_text(encoding="utf-8"))
    cross = json.loads(Path(crosscheck_path).read_text(encoding="utf-8"))
    expected_identity = {"study": v8.VERSION, "body_sha256": v8.PREREG_BODY_SHA256}
    for name, report in (("probe", probe), ("TwelveData", cross)):
        if any((report.get("prereg") or {}).get(k) != value for k, value in expected_identity.items()):
            raise PermissionError(f"v8 {name} report is not bound to the v8 preregistration")
    expected = {"start": PROBE_START.isoformat(), "end": PROBE_END.isoformat()}
    if probe.get("read_window") is None or any(probe["read_window"].get(k) != v for k, v in expected.items()):
        raise PermissionError("v8 requires a new 2007-11-02..2019-12-31 probe")
    if cross.get("read_window") != expected or (cross.get("rule") or {}).get("discovery_window") != [
        PROBE_START.isoformat(), PROBE_END.isoformat()
    ]:
        raise PermissionError("v8 requires an exact earlier-window TwelveData cross-check")
    if (probe.get("tolerances") or {}).get("crosscheck", {}).get("discovery_window") != [
        PROBE_START.isoformat(), PROBE_END.isoformat()
    ]:
        raise PermissionError("v8 probe carries an old TwelveData window")
    if probe.get("benchmark_admitted") is not True or probe.get("source", {}).get("name") != v6.PRICE_SOURCE:
        raise PermissionError("v8 benchmark or TIINGO source was not price admitted")


def _registered(log_dir: Path, vault_repo: Path) -> None:
    witness = v8.check_offhost(vault_repo)
    v8.require_witness(log_dir, witness, 2)
    if witness.census["records"].get(v8.REGISTRY_ID, 0) < 2:
        raise PermissionError("v8 registration is not witnessed")


def _run_at(args: list[str]):
    from datetime import datetime, timezone

    from datetime import timedelta

    raw = _option(args, "--run-at")
    try:
        run_at = datetime.fromisoformat(raw[:-1] + "+00:00" if raw.endswith("Z") else raw)
    except ValueError as exc:
        raise SystemExit("--run-at must be an ISO instant such as 2026-10-02T03:00:00+00:00") from exc
    if run_at.utcoffset() != timedelta(0) or run_at > datetime.now(timezone.utc):
        raise SystemExit("--run-at must be an explicit UTC instant (+00:00) that is not in the future")
    return run_at


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        raise SystemExit("v8 command required")
    command = args[0]
    if command == "register":
        # register --log-dir NEW --v7-log-dir V7REG --vault-repo CLONE --code-sha SHA --run-at ISO [--execute]
        log_dir = Path(_option(args, "--log-dir"))
        v7_log_dir = Path(_option(args, "--v7-log-dir"))
        code_sha = _option(args, "--code-sha")
        run_at = _run_at(args)
        v8.check_prereg()
        witness = v7.check_offhost(Path(_option(args, "--vault-repo")))
        if "--execute" not in args:
            stop = v7.verify_terminal_stop(v7_log_dir, witness)
            v8.check_census(witness.census, stop_head_sha256=stop["head_sha256"], witness_repo=witness.repo)
            records = v8.registration_records(run_at, code_sha)
            print(json.dumps({"dry_run": True, "v7_stop": stop,
                              "would_register_sha256": v1.chained_sha256(records)}, indent=2, sort_keys=True))
            return
        records = v8.register(log_dir, run_at, code_sha, v7_log_dir=v7_log_dir, v7_witness=witness)
        print(json.dumps({"appended": [r["kind"] for r in records],
                          "chain": v8.registry(log_dir).verify_chain(),
                          "anchor_line": (log_dir / v8.REGISTRY_ANCHORS).read_text(encoding="utf-8")},
                         indent=2, sort_keys=True))
        return
    if command in {"stop", "verify-stop"}:
        # stop --log-dir DIR --vault-repo CLONE --power POWER.json --decision-ref TEXT
        #      --expected-prev-sha256 HEX --run-at ISO [--execute --expected-stop-head HEX]
        log_dir = Path(_option(args, "--log-dir"))
        v8.check_prereg()
        witness = v8.check_offhost(Path(_option(args, "--vault-repo")))
        if command == "verify-stop":
            print(json.dumps(v8.verify_terminal_stop(log_dir, witness), indent=2, sort_keys=True))
            return
        run_at = _run_at(args)
        kwargs = {"power_path": Path(_option(args, "--power")), "decision_ref": _option(args, "--decision-ref"),
                  "expected_prev_sha256": _option(args, "--expected-prev-sha256"), "witness": witness}
        out = v8.append_stop_status(log_dir, run_at, dry_run=True, **kwargs)
        if "--execute" in args:
            if out["would_be_head_sha256"] != _option(args, "--expected-stop-head"):
                raise SystemExit("--expected-stop-head differs from the dry-run head: nothing appended")
            out = v8.append_stop_status(log_dir, run_at, **kwargs)
            out = {"appended": out, "head_sha256": v1._record_sha256(out),
                   "chain": v8.registry(log_dir).verify_chain(),
                   "next": f"publish the anchor to {v8.WITNESS_PATH}, then run verify-stop"}
        print(json.dumps(out, indent=2, sort_keys=True))
        return
    if command in {"power", "freeze-inputs", "open-discovery", "discover", "open-holdout", "holdout"}:
        if "--accept-underpowered" in args:
            raise PermissionError("v8 has no underpowered override")
        check_early_probe(Path(_option(args, "--probe-report")),
                          Path(_option(args, "--crosscheck-report")))
        if command in {"power", "freeze-inputs"}:
            _registered(Path(_option(args, "--log-dir")), Path(_option(args, "--vault-repo")))
            args = _drop_pairs(args, "--vault-repo")
            if command == "power":
                args = _drop_pairs(args, "--log-dir")
    with v1.discovery_window(v8.DISCOVERY_START):
        _main(args, h=v8)


if __name__ == "__main__":
    main()
