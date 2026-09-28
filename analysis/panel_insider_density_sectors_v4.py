"""VS1 other-10-sector joint run, pre-registration "sectors v4" (registry only; research only).

Pre-registration: ``docs/paper_log/vs1-sectors-v4-preregistration.md`` (body
sha256 pinned in :data:`PREREG_BODY_SHA256`). It supersedes "sectors v3"
(``analysis.panel_insider_density_sectors_v3``, registered 2026-09-28, never
opened). Sectors v3's §7 gated its opening on VS1 **v3**'s sealed holdout and
admitted no VS1 registry beyond v1, v2 and v3. v3 was superseded by v4, v5 and
v6 before any use, so sectors v3 could never open (#701 review round 3). Sectors
v4 gates on the Technology registry that is the terminal member of the pinned
supersession chain, named explicitly as VS1 **v6** (:data:`TECHNOLOGY_RUN`).
Everything else (universe, trials, statistic, selection, Stage-0, alarms) is
sectors v3's, reused unchanged.

This module holds the pinned registration (own chain, own canonical witness
``05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v4.anchors.jsonl``), the
Technology-chain check (:func:`technology_terminal`) and a reference
implementation of the §7 census check (:func:`check_census`). **It contains no
stage that opens a run or reads a price.** Nothing here is a trading signal.
"""

from __future__ import annotations

import importlib
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v2 as s2
from analysis import panel_insider_density_sectors_v3 as s3
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v6 as v6
from analysis.panel_insider_density_sectors_v3 import (  # noqa: F401  (sectors v3's run, unchanged)
    LATE_ETF_START,
    PRIMARY_TRIAL,
    RUN_ALPHA,
    RUN_K,
    SECONDARY_TRIALS,
    SECTOR_ETF,
    SECTOR_SIC_RANGES,
    STAGE0_PERMS,
    STAGE0_SIMS,
    STAGE0_THRESHOLD,
    check_ranges,
    sector_universes,
    sic_sector,
    stage0_attainable,
)

REPO = v2.REPO
PREREG_PATH = Path("docs/paper_log/vs1-sectors-v4-preregistration.md")
PREREG_BODY_SHA256 = "e3f41ace1bfbfed12c82e16b3b438a62ba759bc1c54dfdf743fb8b2d27b4e712"
VERSION = "vs1-sectors-v4"
REGISTRY_ID = "sectors-v4"  # its key in the VS1 witness census

#: §7: the Technology run gating this run, named explicitly. It must be the terminal member of the
#: pinned Technology supersession chain; any later Technology registration needs a new sectors registration.
TECHNOLOGY_RUN: dict[str, Any] = {
    "version": v6.VERSION,
    "registry_id": "vs1-v6",
    "prereg_sha256": v6.PREREG_BODY_SHA256,
    "registry_head_sha256": v6.REGISTERED_RECORD_SHA256[1],
    "witness_path": v6.WITNESS_PATH,
}
#: §7: every other registry that may be on main, each at exactly its 2 registration records.
FROZEN_REGISTRIES = ("vs1-v1", "vs1-v2", "vs1-v3", "vs1-v4", "vs1-v5", "sectors-v2", "sectors-v3")
REGISTRATION_RECORDS = v2.V1_REGISTRATION_RECORDS

REGISTRY_LOG = "granular_panel_prereg_sectors_v4.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_sectors_v4.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_sectors_v4.lock"
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_PATH = v1.canonical_witness_path(REGISTRY_ID)

REGISTERED_AT: datetime | None = None
REGISTERED_CODE_SHA: str | None = None
REGISTERED_RECORD_SHA256: tuple[str, str] | None = None
REGISTERED_ANCHOR_LINE: bytes | None = None
SUPERSEDED_BY: dict | None = None


def _technology_modules() -> list[ModuleType]:
    """VS1 v1 and every ``analysis.panel_insider_density_v<n>`` present, in version order."""
    modules = [v1]
    n = 2
    while importlib.util.find_spec(f"analysis.panel_insider_density_v{n}") is not None:
        modules.append(importlib.import_module(f"analysis.panel_insider_density_v{n}"))
        n += 1
    return modules


def technology_terminal() -> dict[str, Any]:
    """The terminal member of the pinned Technology supersession chain (§7).

    It is the one Technology version whose code pins no ``SUPERSEDED_BY``. It must be
    the highest version, and every earlier version's pin must name it with its
    body and registry head. Raises ``PermissionError`` otherwise.
    """
    modules = _technology_modules()
    unsuperseded = [m for m in modules if m.SUPERSEDED_BY is None]
    if len(unsuperseded) != 1 or unsuperseded[0] is not modules[-1]:
        raise PermissionError(f"the Technology chain has no single terminal member: {[m.VERSION for m in unsuperseded]}")
    terminal = modules[-1]
    if terminal.REGISTERED_RECORD_SHA256 is None:
        raise PermissionError(f"the terminal Technology version {terminal.VERSION} is not registered")
    expected = {"version": terminal.VERSION, "prereg_sha256": terminal.PREREG_BODY_SHA256,
                "registry_head_sha256": terminal.REGISTERED_RECORD_SHA256[1]}
    for m in modules[:-1]:
        pin = {k: m.SUPERSEDED_BY.get(k) for k in expected}
        if pin != expected:
            raise PermissionError(f"{m.VERSION}'s supersession pin does not name the terminal {terminal.VERSION}")
    return {"version": terminal.VERSION, "registry_id": f"vs1-v{len(modules)}",
            "prereg_sha256": terminal.PREREG_BODY_SHA256,
            "registry_head_sha256": terminal.REGISTERED_RECORD_SHA256[1], "witness_path": terminal.WITNESS_PATH}


def check_technology_run() -> dict[str, Any]:
    """§7: the terminal Technology member is exactly the pinned :data:`TECHNOLOGY_RUN` (v6), else refuse."""
    terminal = technology_terminal()
    if terminal != TECHNOLOGY_RUN:
        raise PermissionError(
            f"the terminal Technology registration is {terminal['version']}, not {TECHNOLOGY_RUN['version']}: "
            "a later Technology registration requires a new sectors registration"
        )
    return terminal


def check_census(census: dict, *, technology_sealed_records: int | None = None) -> None:
    """Reference implementation of the §7 census part of the opening check (no I/O).

    ``census`` is :func:`analysis.panel_insider_density.vs1_witness_census` at the pinned tip.
    ``technology_sealed_records`` is the record count at which the gating
    Technology run's chain ends in a witnessed ``holdout_result``. The caller
    establishes it by verifying v6's registry chain against v6's witness
    (``verify_chain(external_anchors=...)``, last record ``holdout_result``, verdict
    file present). It is None when that has not been established.
    """
    if census.get("unknown"):
        raise PermissionError(f"unknown VS1 witness files on main: {census['unknown']}")
    files, records = census["files"], census["records"]
    for key, path in files.items():
        if path != v1.canonical_witness_path(key):
            raise PermissionError(f"{key}'s witness {path!r} is not its canonical path")
    allowed = {*FROZEN_REGISTRIES, TECHNOLOGY_RUN["registry_id"], REGISTRY_ID}
    if set(files) != allowed:
        raise PermissionError(
            f"the witnessed VS1 registries on main must be exactly {sorted(allowed)}; extra "
            f"{sorted(set(files) - allowed)}, missing {sorted(allowed - set(files))}"
        )
    frozen = {k: records.get(k) for k in FROZEN_REGISTRIES if records.get(k) != REGISTRATION_RECORDS}
    if frozen:
        raise PermissionError(f"registries past their registration records: {frozen}")
    tech = records.get(TECHNOLOGY_RUN["registry_id"])
    if technology_sealed_records is None or technology_sealed_records <= REGISTRATION_RECORDS \
            or tech != technology_sealed_records:
        raise PermissionError(
            f"{TECHNOLOGY_RUN['version']}'s witness covers {tech} records: this run opens only once its chain "
            "ends at a witnessed holdout_result and the witness extends no further"
        )


def check_open() -> None:
    """No harness can open sectors v4 yet (§0): the joint-run harness must implement §7 and be reviewed."""
    raise PermissionError("no sectors-v4 joint-run harness exists: it must implement §7 and be reviewed first")


def registration_records(now: datetime, code_sha: str, prereg_sha256: str | None = None) -> list[dict]:
    """The sectors-v4 header and ``preregistration`` records (before chaining)."""
    prereg_sha256 = prereg_sha256 or PREREG_BODY_SHA256
    header = {"kind": "header", "version": VERSION, "run_at": now.isoformat(), "code_sha": code_sha,
              "prereg_path": PREREG_PATH.as_posix(), "prereg_sha256": prereg_sha256, "promotion_allowed": False}
    record = {
        "kind": "preregistration",
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": prereg_sha256,
        "registry_id": REGISTRY_ID,
        "witness_path": WITNESS_PATH,
        "sector_map_sha256": v1.SECTOR_MAP_SHA256,
        "issuer_map_sha256": v2.ISSUER_MAP_SHA256,
        "sic_map_sha256": v2.SIC_MAP_SHA256,
        "sector_sic_ranges": {s: [list(r) for r in ranges] for s, ranges in SECTOR_SIC_RANGES.items()},
        "ledger_id": v1.LEDGER_ID,
        "run": {"sectors": list(SECTOR_ETF), "benchmarks": SECTOR_ETF, "k": RUN_K, "alpha": RUN_ALPHA,
                "trials_per_sector": list(v1.trial_names()), "primary_trial": PRIMARY_TRIAL,
                "secondary_trials": list(SECONDARY_TRIALS), "holm_family": "all 40 trials"},
        "stage0": {"sims": STAGE0_SIMS, "perms": STAGE0_PERMS, "threshold": STAGE0_THRESHOLD,
                   "target_ics": list(v1.POWER_TARGET_ICS), "gate": "recorded per sector, not gating"},
        "technology_run": {**TECHNOLOGY_RUN,
                           "definition": "terminal member of the pinned Technology supersession chain; "
                                         "a later Technology registration requires a new sectors registration",
                           "opening": "only once its chain ends at a witnessed holdout_result"},
        "frozen_registries": list(FROZEN_REGISTRIES),
        "supersedes": {
            "sectors_v3": {"prereg_sha256": s3.PREREG_BODY_SHA256,
                           "registry_head_sha256": s3.REGISTERED_RECORD_SHA256[1],
                           "reason": "its opening rule gated on VS1 v3, superseded by v4-v6: it could never open"},
            "sectors_v2": {"prereg_sha256": s2.PREREG_BODY_SHA256,
                           "registry_head_sha256": s2.REGISTERED_RECORD_SHA256[1]},
            "plan": "VS1 v1 §13 other-10-sector plan (carried into v2 §13 and v3 §13)",
            "v1_prereg_sha256": v1.PREREG_BODY_SHA256,
            "v1_registry_head_sha256": v1.REGISTERED_RECORD_SHA256[1],
        },
        "windows": {"discovery_start": v1.DISCOVERY_START, "split": v1.SPLIT, "end": v1.END},
        "promotion_allowed": False,
    }
    return [header, record]


def registry(log_dir: Path):
    from analysis.research_forward_log import ForwardLog

    return ForwardLog(log_dir, log_filename=REGISTRY_LOG, anchor_filename=REGISTRY_ANCHORS,
                      lock_filename=REGISTRY_LOCK, prereg_sha256=PREREG_BODY_SHA256)


def check_prereg(repo_root: Path = REPO) -> str:
    actual = v1.prereg_body_sha256(Path(repo_root) / PREREG_PATH)
    if actual != PREREG_BODY_SHA256:
        raise ValueError(f"sectors-v4 pre-registration body hashes to {actual[:12]}, pinned {PREREG_BODY_SHA256[:12]}")
    return actual


def register(log_dir: Path, now: datetime, code_sha: str) -> list[dict]:
    """The one sectors-v4 registration (before the pins exist), afterwards only a copy of it."""
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    check_prereg()
    check_technology_run()
    records = registration_records(now, code_sha)
    if REGISTERED_RECORD_SHA256 is not None and tuple(v1.chained_sha256(records)) != REGISTERED_RECORD_SHA256:
        raise PermissionError("sectors-v4 is already registered; another time, code or body is a fork")
    log = registry(log_dir)
    with log.locked():
        check = log.verify_chain()
        if not check["ok"]:
            raise RuntimeError(f"registry chain is broken: {check['detail']}")
        if log.read_all():
            raise ValueError("this pre-registration is already registered in this directory")
        return log.append_locked(records)


def main(argv: list[str] | None = None) -> None:
    """``python -m analysis.panel_insider_density_sectors_v4 {hash-prereg|register --log-dir D [--code-sha S]}``."""
    import argparse
    import json

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("hash-prereg")
    p = sub.add_parser("register")
    p.add_argument("--log-dir", required=True)
    p.add_argument("--code-sha")
    args = parser.parse_args(argv)
    if args.command == "hash-prereg":
        actual = v1.prereg_body_sha256(REPO / PREREG_PATH)
        print(json.dumps({"body_sha256": actual, "pinned": PREREG_BODY_SHA256,
                          "matches_pinned": actual == PREREG_BODY_SHA256}, indent=2))
        return
    if REGISTERED_RECORD_SHA256 is None:
        if not args.code_sha:
            raise SystemExit("the first registration needs --code-sha")
        records = register(Path(args.log_dir), datetime.now(timezone.utc), args.code_sha)
    else:
        records = register(Path(args.log_dir), REGISTERED_AT, REGISTERED_CODE_SHA)
    print(json.dumps({"appended": [r["kind"] for r in records], "chain": registry(Path(args.log_dir)).verify_chain(),
                      "anchor_line": (Path(args.log_dir) / REGISTRY_ANCHORS).read_text(encoding="utf-8")}, indent=2))


if __name__ == "__main__":
    import sys

    main(sys.argv[1:])
