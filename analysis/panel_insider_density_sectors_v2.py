"""VS1 other-10-sector joint run, pre-registration "sectors v2" (registry and universe only; research only).

Pre-registration: ``docs/paper_log/vs1-sectors-v2-preregistration.md`` (body
sha256 pinned in :data:`PREREG_BODY_SHA256`). It replaces the sector plan of
VS1 v1 §13 (carried unchanged into v2 §13 and v3 §13) for the 10 non-Technology
equity sectors, per the owner decision of 2026-09-28 (all sectors on 5 sessions):

* universe per sector: the v1 sector-map primary-sector members plus every CIK
  with a current ticker whose current SEC SIC is in the sector's pre-registered
  ranges (:data:`SECTOR_SIC_RANGES`), the v3 method; issuers of the v3
  Technology universe are excluded, and every CIK belongs to at most one sector;
* v3's admission rules, ticker-reuse rule, reported items and alarms (adapted
  to one joint family), 5-session primary ``A90|fwd5`` per sector with the
  20-session trials secondary, all 40 trials in one Holm family at the ledger's
  run k = 2 level (alpha = 0.10/6, unchanged from v1).

This module holds the pinned registration (its own hash chain and its own
off-host witness ``05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v2.anchors.jsonl``)
and the universe definition. **It contains no stage that opens a run or reads a
price**: the joint-run harness is not built. It must be built on the pinned
``Harness`` (``analysis.panel_insider_density_v2``) and reviewed before any
sector run, and it may open only under the opening rule of its §7.
Nothing here is a trading signal.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v3 as v3

REPO = v2.REPO
PREREG_PATH = Path("docs/paper_log/vs1-sectors-v2-preregistration.md")
PREREG_BODY_SHA256 = "ed7cacb99cd010963dedfa842677784d0e7ffa95ec4534a2a005238a03a6815e"
VERSION = "vs1-sectors-v2"
REGISTRY_ID = "sectors-v2"  # its key in the VS1 witness census

#: Pre-registered SIC ranges (inclusive) per sector. Disjoint across sectors and
#: disjoint from the Technology ranges (3570-3579, 3660-3679, 7370-7379).
#: Ambiguous codes are left out of every sector (see the pre-registration §2.2).
SECTOR_SIC_RANGES: dict[str, tuple[tuple[int, int], ...]] = {
    "Energy": ((1220, 1229), (1300, 1389), (2910, 2912), (2990, 2999), (4610, 4619), (4922, 4922),
               (5171, 5172)),
    "Materials": ((1000, 1099), (1400, 1499), (2410, 2429), (2600, 2659), (2800, 2829), (2850, 2899),
                  (3210, 3299), (3310, 3399), (3410, 3412)),
    "Healthcare": ((2830, 2836), (3841, 3845), (3851, 3851), (5047, 5047), (5122, 5122), (6324, 6324),
                   (8000, 8099)),
    "Financials": ((6000, 6299), (6300, 6323), (6325, 6411), (6700, 6769), (6771, 6791), (6793, 6794),
                   (6796, 6797), (6799, 6799)),
    "Real Estate": ((6500, 6553), (6798, 6798)),
    "Utilities": ((4900, 4921), (4923, 4949), (4960, 4991)),
    "Communication Services": ((2710, 2749), (4800, 4899), (7310, 7319), (7810, 7849)),
    "Consumer Staples": ((100, 299), (2000, 2099), (2100, 2199), (2840, 2844), (5140, 5149), (5180, 5182),
                         (5400, 5499), (5912, 5912)),
    "Consumer Discretionary": ((1520, 1531), (2300, 2399), (2510, 2599), (3140, 3149), (3630, 3639),
                               (3651, 3652), (3710, 3716), (3750, 3751), (3942, 3949), (5200, 5299),
                               (5300, 5330), (5332, 5399), (5500, 5540), (5542, 5599), (5600, 5699),
                               (5700, 5799), (5810, 5813), (5900, 5911), (5913, 5999), (7000, 7099),
                               (7900, 7999), (8200, 8299)),
    "Industrials": ((1540, 1799), (3400, 3409), (3413, 3499), (3500, 3569), (3580, 3599), (3600, 3629),
                    (3640, 3649), (3690, 3699), (3720, 3729), (3730, 3749), (3760, 3769), (3812, 3812),
                    (4000, 4599), (4700, 4723), (4725, 4799), (4950, 4959), (5000, 5044), (5046, 5046),
                    (5048, 5064), (5066, 5099), (7320, 7369), (7380, 7389), (8700, 8730), (8732, 8748)),
}
#: The sector benchmark ETFs (v1 §13) and the first admitted close of the late ones.
SECTOR_ETF: dict[str, str] = {s: e for s, e in v1.EQUITY_SECTORS.items() if s != v1.VS1_SECTOR}
LATE_ETF_START: dict[str, str] = {"XLRE": "2015-10", "XLC": "2018-06"}
PRIMARY_TRIAL = "A90|fwd5"
SECONDARY_TRIALS: tuple[str, ...] = tuple(t for t in v1.trial_names() if t != PRIMARY_TRIAL)
RUN_K = v1.OTHER_SECTORS_RUN_K  # 2
RUN_ALPHA = v1.run_alpha(RUN_K)  # 0.10 / 6

REGISTRY_LOG = "granular_panel_prereg_sectors_v2.jsonl"
REGISTRY_ANCHORS = "granular_panel_prereg_sectors_v2.anchors.jsonl"
REGISTRY_LOCK = ".granular_panel_prereg_sectors_v2.lock"
WITNESS_REMOTE_URL = v1.WITNESS_REMOTE_URL
WITNESS_BRANCH = v1.WITNESS_BRANCH
WITNESS_PATH = "05-GRID/Paper-Log/vs1/granular_panel_prereg_sectors_v2.anchors.jsonl"

#: The one sectors-v2 registration: registered once, locally, on 2026-09-28T02:34:55Z against
#: code f6588eba, chain head bcfc31b0... at 2 records. The original lives in the operator's
#: ``Documents/Codex/2026-09-14/wha/outputs/vs1-sectors-v2-prereg-registry/``.
#: Superseded by "sectors v3" before any use (its §10 Stage-0 threshold was unattainable).
SUPERSEDED_BY: dict | None = {
    "version": "vs1-sectors-v3",
    "prereg_sha256": "7b6eecae453cc71a0af021d96c259c65de5ab3d03f835746c7d413dcda7e4103",
    "registry_head_sha256": None,
}
REGISTERED_AT: datetime | None = datetime(2026, 9, 28, 2, 34, 55, 80814, tzinfo=timezone.utc)
REGISTERED_CODE_SHA: str | None = "f6588eba8aa54f0b6e45215bff5b2afcabfdb8a1"
REGISTERED_RECORD_SHA256: tuple[str, str] | None = (
    "ec4f1534bf119ea10d4305e739b5fb0380e6da6cd9af8d1b44c45509f1687745",  # header
    "bcfc31b0f355dc04bfbd252b1705a5bd441701649bcd2b9bb4e136adbff23a04",  # preregistration (head at 2)
)
#: The first line every committed version of the sectors-v2 witness file starts with.
REGISTERED_ANCHOR_LINE: bytes | None = (
    b'{"head_sha256":"bcfc31b0f355dc04bfbd252b1705a5bd441701649bcd2b9bb4e136adbff23a04",'
    b'"prev_anchor_sha256":null,"records":2,"run_at":"2026-09-28T02:34:55.080814+00:00"}'
)


def check_open() -> None:
    """Sectors v2 can never open a run: it is superseded by sectors v3 (pinned)."""
    if SUPERSEDED_BY:
        raise PermissionError(
            f"VS1 sectors-v2 is superseded by {SUPERSEDED_BY['version']} (pinned in code): it can never open a run"
        )


def check_ranges() -> None:
    """The sector ranges are disjoint from each other and from the Technology ranges."""
    owner: dict[int, str] = {}
    for sector, ranges in SECTOR_SIC_RANGES.items():
        if sector not in SECTOR_ETF:
            raise ValueError(f"{sector}: not one of the 10 other sectors")
        for lo, hi in ranges:
            for code in range(lo, hi + 1):
                if v2.sic_in_ranges(code):
                    raise ValueError(f"SIC {code} ({sector}) is a Technology code")
                if code in owner:
                    raise ValueError(f"SIC {code} is in {owner[code]} and {sector}")
                owner[code] = sector
    if set(SECTOR_SIC_RANGES) != set(SECTOR_ETF):
        raise ValueError("every one of the 10 other sectors needs SIC ranges")


def sic_sector(sic: Any) -> str | None:
    """The sector whose pre-registered ranges contain ``sic`` (None: no sector)."""
    try:
        code = int(sic)
    except (TypeError, ValueError):
        return None
    for sector, ranges in SECTOR_SIC_RANGES.items():
        if any(lo <= code <= hi for lo, hi in ranges):
            return sector
    return None


def sector_universes(sector_map: Mapping[str, Any], issuer_map: pd.DataFrame,
                     sic_map: pd.DataFrame) -> tuple[dict[str, pd.DataFrame], dict]:
    """One universe per other sector (§2.2): one row per CIK, each CIK in at most one sector.

    Order of assignment: (1) a CIK in the v3 Technology universe belongs to no
    other sector; (2) a v1 sector-map member belongs to its primary sector
    (v1 tie rule, member ticker); (3) any other CIK with a current ticker belongs
    to the sector of its current SIC. Share-class and price-ticker collisions
    keep the lowest CIK (v2 rule).
    """
    check_ranges()
    tech, _ = v2.v2_universe(sector_map, issuer_map, sic_map)
    tech_ciks = set(tech["cik"].astype(int))
    by_cik = issuer_map.groupby("cik")["ticker"].apply(lambda s: sorted(set(s)))
    sic = sic_map.set_index("cik")["sic"]
    rows: dict[int, dict] = {}
    info: dict[str, Any] = {"excluded_technology": 0, "sector_map_conflicts_resolved_to_sector_map": 0}
    for sector in SECTOR_ETF:
        members, _ = v1.sector_universe(sector, sector_map, issuer_map)
        for row in members.itertuples(index=False):
            cik = int(row.cik)
            if cik in tech_ciks:
                info["excluded_technology"] += 1
                continue
            if cik not in rows:
                rows[cik] = {"sector": sector, "ticker": row.ticker, "cik": cik, "source": "sector_map"}
    for cik, value in sic.items():
        cik = int(cik)
        sector = sic_sector(value)
        if sector is None or cik not in by_cik.index or cik in tech_ciks:
            continue
        if cik in rows:
            if rows[cik]["sector"] == sector:
                rows[cik]["source"] = "both"
            else:
                info["sector_map_conflicts_resolved_to_sector_map"] += 1
            continue
        rows[cik] = {"sector": sector, "ticker": v2._price_ticker(by_cik[cik]), "cik": cik, "source": "sic"}
    out: dict[str, pd.DataFrame] = {}
    counts: dict[str, dict] = {}
    for sector in SECTOR_ETF:
        chosen = [dict(r) for r in rows.values() if r["sector"] == sector]
        for r in chosen:
            value = sic.get(r["cik"])
            r["sic"] = int(value) if value is not None and not pd.isna(value) else None
            r["current_tickers"] = sorted({v2.canonical_symbol(t) for t in by_cik.get(r["cik"], [])})
            r["sic_group"] = sector
        frame = pd.DataFrame(sorted(chosen, key=lambda r: (r["ticker"], r["cik"])),
                             columns=["ticker", "cik", "source", "sic", "sic_group", "current_tickers"], dtype=object)
        frame["cik"] = frame["cik"].astype("int64")
        frame = frame[~frame["ticker"].duplicated(keep="first")].reset_index(drop=True)
        out[sector] = frame
        counts[sector] = {"universe": int(len(frame)),
                          "by_source": {k: int(v) for k, v in frame["source"].value_counts().sort_index().items()}}
    info["sectors"] = counts
    return out, info


def registration_records(now: datetime, code_sha: str, prereg_sha256: str | None = None) -> list[dict]:
    """The sectors-v2 header and ``preregistration`` records (before chaining)."""
    prereg_sha256 = prereg_sha256 or PREREG_BODY_SHA256
    header = {"kind": "header", "version": VERSION, "run_at": now.isoformat(), "code_sha": code_sha,
              "prereg_path": PREREG_PATH.as_posix(), "prereg_sha256": prereg_sha256, "promotion_allowed": False}
    record = {
        "kind": "preregistration",
        "run_at": now.isoformat(),
        "code_sha": code_sha,
        "prereg_path": PREREG_PATH.as_posix(),
        "prereg_sha256": prereg_sha256,
        "sector_map_sha256": v1.SECTOR_MAP_SHA256,
        "issuer_map_sha256": v2.ISSUER_MAP_SHA256,
        "sic_map_sha256": v2.SIC_MAP_SHA256,
        "sector_sic_ranges": {s: [list(r) for r in ranges] for s, ranges in SECTOR_SIC_RANGES.items()},
        "ledger_id": v1.LEDGER_ID,
        "run": {"sectors": list(SECTOR_ETF), "benchmarks": SECTOR_ETF, "k": RUN_K, "alpha": RUN_ALPHA,
                "trials_per_sector": list(v1.trial_names()), "primary_trial": PRIMARY_TRIAL,
                "secondary_trials": list(SECONDARY_TRIALS), "holm_family": "all 40 trials"},
        "supersedes": {
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
        raise ValueError(f"sectors-v2 pre-registration body hashes to {actual[:12]}, pinned {PREREG_BODY_SHA256[:12]}")
    return actual


def register(log_dir: Path, now: datetime, code_sha: str) -> list[dict]:
    """The one sectors-v2 registration (before the pins exist), afterwards only a copy of it."""
    if now.tzinfo is None:
        raise ValueError("now must carry a timezone")
    check_prereg()
    records = registration_records(now, code_sha)
    if REGISTERED_RECORD_SHA256 is not None and tuple(v1.chained_sha256(records)) != REGISTERED_RECORD_SHA256:
        raise PermissionError("sectors-v2 is already registered; another time, code or body is a fork")
    log = registry(log_dir)
    with log.locked():
        check = log.verify_chain()
        if not check["ok"]:
            raise RuntimeError(f"registry chain is broken: {check['detail']}")
        if log.read_all():
            raise ValueError("this pre-registration is already registered in this directory")
        return log.append_locked(records)


def main(argv: list[str] | None = None) -> None:
    """``python -m analysis.panel_insider_density_sectors_v2 {hash-prereg|register --log-dir D [--code-sha S]}``."""
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
