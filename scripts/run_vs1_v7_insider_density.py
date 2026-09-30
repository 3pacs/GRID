"""VS1 v7 one-shot research CLI. Bind STOP/code/registry/witness pins before use.

The v7 discovery start is scoped to this invocation; v1-v6 retain 2012-01-01.
The power command needs the witnessed v7 registration and exact earlier-window
non-outcome admission reports. No command here silently opens a holdout.
``stop`` (dry run unless ``--execute``) and ``verify-stop`` record and check the
terminal STOP that supersedes v7 unopened before its Stage-0 (pinned E0 evidence).
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v6 as v6
from analysis import panel_insider_density_v7 as v7
from scripts.run_vs1_v2_insider_density import main as _main

PROBE_START = date(2011, 8, 2)
PROBE_END = date(2019, 12, 31)


def _option(argv: list[str], name: str) -> str:
    try:
        return argv[argv.index(name) + 1]
    except (ValueError, IndexError) as exc:
        raise SystemExit(f"v7 requires {name}") from exc


def _drop_pairs(argv: list[str], *names: str) -> list[str]:
    out: list[str] = []
    i = 0
    while i < len(argv):
        if argv[i] in names:
            i += 2
        else:
            out.append(argv[i])
            i += 1
    return out


def check_early_probe(probe_path: Path, crosscheck_path: Path) -> None:
    """Old v6 window receipts refuse even if their manifest names the same tickers."""
    probe = json.loads(Path(probe_path).read_text(encoding="utf-8"))
    cross = json.loads(Path(crosscheck_path).read_text(encoding="utf-8"))
    expected_identity = {"study": v7.VERSION, "body_sha256": v7.PREREG_BODY_SHA256}
    for name, report in (("probe", probe), ("TwelveData", cross)):
        if any((report.get("prereg") or {}).get(k) != value for k, value in expected_identity.items()):
            raise PermissionError(f"v7 {name} report is not bound to the v7 preregistration")
    expected = {"start": PROBE_START.isoformat(), "end": PROBE_END.isoformat()}
    if probe.get("read_window") is None or any(probe["read_window"].get(k) != v for k, v in expected.items()):
        raise PermissionError("v7 requires a new 2011-08-02..2019-12-31 probe")
    if cross.get("read_window") != expected or (cross.get("rule") or {}).get("discovery_window") != [
        PROBE_START.isoformat(), PROBE_END.isoformat()
    ]:
        raise PermissionError("v7 requires an exact earlier-window TwelveData cross-check")
    if (probe.get("tolerances") or {}).get("crosscheck", {}).get("discovery_window") != [
        PROBE_START.isoformat(), PROBE_END.isoformat()
    ]:
        raise PermissionError("v7 probe carries the old TwelveData window")
    if probe.get("benchmark_admitted") is not True or probe.get("source", {}).get("name") != v6.PRICE_SOURCE:
        raise PermissionError("v7 benchmark or TIINGO source was not price admitted")


def _registered(log_dir: Path, vault_repo: Path) -> None:
    witness = v7.check_offhost(vault_repo)
    v7.require_witness(log_dir, witness, 2)
    if witness.census["records"].get(v7.REGISTRY_ID, 0) < 2:
        raise PermissionError("v7 registration is not witnessed")


def main(argv: list[str] | None = None) -> None:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        raise SystemExit("v7 command required")
    command = args[0]
    if command == "register":
        log_dir = Path(_option(args, "--log-dir"))
        v6_log_dir = Path(_option(args, "--v6-log-dir"))
        vault_repo = Path(_option(args, "--vault-repo"))
        code_sha = _option(args, "--code-sha")
        v7.check_prereg()
        witness = v6.check_offhost(vault_repo)
        from datetime import datetime, timezone
        records = v7.register(log_dir, datetime.now(timezone.utc), code_sha,
                              v6_log_dir=v6_log_dir, v6_witness=witness)
        print(json.dumps({"appended": [r["kind"] for r in records],
                          "chain": v7.registry(log_dir).verify_chain()}, indent=2))
        return
    if command in {"stop", "verify-stop"}:
        # stop --log-dir DIR --vault-repo CLONE --e0-scorecard SCORECARD.json --decision-ref TEXT
        #      --expected-prev-sha256 HEX --run-at 2026-10-01T03:00:00+00:00
        #      [--execute --expected-stop-head HEX]   (dry run unless --execute; the head is the dry run's)
        from datetime import datetime, timezone
        log_dir = Path(_option(args, "--log-dir"))
        v7.check_prereg()
        witness = v7.check_offhost(Path(_option(args, "--vault-repo")))
        if command == "verify-stop":
            print(json.dumps(v7.verify_terminal_stop(log_dir, witness), indent=2, sort_keys=True))
            return
        run_at = datetime.fromisoformat(_option(args, "--run-at"))
        if run_at.utcoffset() is None or run_at > datetime.now(timezone.utc):
            raise SystemExit("--run-at must be an explicit UTC instant that is not in the future")
        stop_kwargs = {"e0_scorecard": Path(_option(args, "--e0-scorecard")),
                       "decision_ref": _option(args, "--decision-ref"),
                       "expected_prev_sha256": _option(args, "--expected-prev-sha256"), "witness": witness}
        out = v7.append_stop_status(log_dir, run_at, dry_run=True, **stop_kwargs)
        if "--execute" in args:
            if out["would_be_head_sha256"] != _option(args, "--expected-stop-head"):
                raise SystemExit("--expected-stop-head differs from this STOP's dry-run head: nothing appended")
            out = v7.append_stop_status(log_dir, run_at, **stop_kwargs)
            out = {"appended": out, "head_sha256": v7.v1._record_sha256(out),
                   "chain": v7.registry(log_dir).verify_chain(),
                   "next": f"publish the anchor to {v7.WITNESS_PATH}, then run verify-stop"}
        print(json.dumps(out, indent=2, sort_keys=True))
        return
    if command in {"power", "freeze-inputs", "open-discovery", "discover", "open-holdout", "holdout"}:
        if "--accept-underpowered" in args:
            raise PermissionError("v7 has no underpowered override")
        check_early_probe(Path(_option(args, "--probe-report")),
                          Path(_option(args, "--crosscheck-report")))
        if command in {"power", "freeze-inputs"}:
            _registered(Path(_option(args, "--log-dir")), Path(_option(args, "--vault-repo")))
            args = _drop_pairs(args, "--vault-repo")
            if command == "power":
                args = _drop_pairs(args, "--log-dir")
    with v1.discovery_window(v7.DISCOVERY_START):
        _main(args, h=v7)


if __name__ == "__main__":
    main()
