"""VS1 other-10-sector joint run, pre-registration "sectors v3" (registry only; research only).

Pre-registration: ``docs/paper_log/vs1-sectors-v3-preregistration.md`` (body
sha256 pinned in :data:`PREREG_BODY_SHA256`). It supersedes "sectors v2"
(``analysis.panel_insider_density_sectors_v2``, registered 2026-09-28, never
opened). Sectors v2's §10 Stage-0 success threshold (p <= alpha_2 / 40 ~ 0.000417)
was below the smallest p attainable with 999 sign-flips (0.001), so its power was
0 by construction (#701 review round 2, R4). v3 uses 9,999 sign-flips for the
sector Stage-0 (:data:`STAGE0_PERMS`) and states the future joint-run harness's
opening and contamination rules. The universe (SIC ranges, assignment order),
trials, statistic, selection and alarms are sectors v2's, reused from that
module unchanged.

This module holds the pinned registration (own chain, own canonical witness
``05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v3.anchors.jsonl``) and
re-exports the universe definition. **It contains no stage that opens a run or
reads a price.** Nothing here is a trading signal.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_sectors_v2 as s2
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3
from analysis.panel_insider_density_sectors_v2 import (  # noqa: F401  (sectors v2's universe, unchanged)
    LATE_ETF_START,
    PRIMARY_TRIAL,
    RUN_ALPHA,
    RUN_K,
    SECONDARY_TRIALS,
    SECTOR_ETF,
    SECTOR_SIC_RANGES,
    check_ranges,
    sector_universes,
    sic_sector,
)

REPO = v2.REPO
PREREG_PATH = Path("docs/paper_log/vs1-sectors-v3-preregistration.md")
PREREG_BODY_SHA256 = "7b6eecae453cc71a0af021d96c259c65de5ab3d03f835746c7d413dcda7e4103"
VERSION = "vs1-sectors-v3"
REGISTRY_ID = "sectors-v3"  # its key in the VS1 witness census

#: Sector Stage-0 (§10): 9,999 sign-flips so that the smallest attainable p (1e-4) is below
#: the success threshold alpha_2 / 40 (the smallest Holm threshold of the 40-trial run).
STAGE0_PERMS = 9_999
STAGE0_SIMS = v1.POWER_SIMS
STAGE0_THRESHOLD = RUN_ALPHA / 40

REGISTRY_LOG = "granular_panel_prereg_sectors_v3.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_sectors_v3.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_sectors_v3.lock"
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_PATH = v1.canonical_witness_path(REGISTRY_ID)

#: The one sectors-v3 registration: registered once, locally, on 2026-09-28T03:11:31Z against
#: code 4dcd7db8, chain head a9a34982... at 2 records. The original lives in the operator's
#: ``Documents/Codex/2026-09-14/wha/outputs/vs1-sectors-v3-prereg-registry/``.
REGISTERED_AT: datetime | None = datetime(2026, 9, 28, 3, 11, 31, 686738, tzinfo=timezone.utc)
REGISTERED_CODE_SHA: str | None = "4dcd7db8b4183db3881430f351aa15001d580eb6"
REGISTERED_RECORD_SHA256: tuple[str, str] | None = (
    "5a08d8284d83197d8d03d4c5897514e8b950c855c83e8f996bfd9202d3d7e380",  # header
    "a9a349823cba1dd92b23d9716222c6f3924a759fd99803d4a2a591dcbcdf7120",  # preregistration (head at 2)
)
#: The first line every committed version of the sectors-v3 witness file starts with.
REGISTERED_ANCHOR_LINE: bytes | None = (
    b'{"head_sha256":"a9a349823cba1dd92b23d9716222c6f3924a759fd99803d4a2a591dcbcdf7120",'
    b'"prev_anchor_sha256":null,"records":2,"run_at":"2026-09-28T03:11:31.686738+00:00"}'
)
SUPERSEDED_BY: dict | None = None


def stage0_attainable() -> bool:
    """The Stage-0 success threshold is above the smallest p the sign-flip null can produce."""
    return 1.0 / (STAGE0_PERMS + 1) < STAGE0_THRESHOLD


def registration_records(now: datetime, code_sha: str, prereg_sha256: str | None = None) -> list[dict]:
    """The sectors-v3 header and ``preregistration`` records (before chaining)."""
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
        "supersedes": {
            "sectors_v2": {"prereg_sha256": s2.PREREG_BODY_SHA256,
                           "registry_head_sha256": s2.REGISTERED_RECORD_SHA256[1],
                           "reason": "Stage-0 success threshold alpha_2/40 unattainable with 999 sign-flips"},
            "plan": "VS1 v1 §13 other-10-sector plan (carried into v2 §13 and v3 §13)",
            "v1_prereg_sha256": v1.PREREG_BODY_SHA256,
            "v1_registry_head_sha256": v1.REGISTERED_RECORD_SHA256[1],
            "v3_prereg_sha256": v3.PREREG_BODY_SHA256,
            "v3_registry_head_sha256": v3.REGISTERED_RECORD_SHA256[1],
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
        raise ValueError(f"sectors-v3 pre-registration body hashes to {actual[:12]}, pinned {PREREG_BODY_SHA256[:12]}")
    return actual


def register(log_dir: Path, now: datetime, code_sha: str) -> list[dict]:
    """The one sectors-v3 registration (before the pins exist), afterwards only a copy of it."""
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    check_prereg()
    records = registration_records(now, code_sha)
    if REGISTERED_RECORD_SHA256 is not None and tuple(v1.chained_sha256(records)) != REGISTERED_RECORD_SHA256:
        raise PermissionError("sectors-v3 is already registered; another time, code or body is a fork")
    log = registry(log_dir)
    with log.locked():
        check = log.verify_chain()
        if not check["ok"]:
            raise RuntimeError(f"registry chain is broken: {check['detail']}")
        if log.read_all():
            raise ValueError("this pre-registration is already registered in this directory")
        return log.append_locked(records)


def main(argv: list[str] | None = None) -> None:
    """``python -m analysis.panel_insider_density_sectors_v3 {hash-prereg|register --log-dir D [--code-sha S]}``."""
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
