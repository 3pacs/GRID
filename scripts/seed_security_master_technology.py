#!/usr/bin/env python3
"""GD1 seed script — build ``security_master`` rows for Technology, dry-run.

Builds proposed ``security_master`` / ``security_identifiers`` /
``security_sector_membership`` rows from two sources only:

    1. ``analysis.sector_map.SECTOR_MAP`` (Technology subsectors' company
       actors).
    2. SEC ``company_tickers.json`` (live CIK crosswalk), fetched with the
       repo's existing contact-bearing SEC User-Agent string
       (``grid.signals.sponsor_resolver.SEC_UA``) — the same one already
       used against this URL by ``sponsor_resolver._load_sec_tickers`` and
       ``small_cap_enrichment``'s companyfacts/submissions calls.

This is intentionally the same two-source cross-check GD0's audit already
ran by hand (``GRID-GD0-SECURITY-MASTER-AUDIT-20260927.md`` §2, §4): of the
102 Technology tickers in the sector map, 98 resolve to a live SEC CIK; the
other four (``CFLT``, ``CYBR``, ``JNPR``, ``PSTG``) do not, and are
*candidates* for delisted/acquired, not confirmed (GD0 §6 item 2 leaves the
delisting-corroboration bar as an owner decision — this script never flips
``is_active``, only lists the candidates in its report).

Dry run by default: it only ever prints a summary and writes a JSON +
Markdown report under ``outputs/security_master/``. Nothing is written to
any database unless ``--apply`` is passed AND ``--db-url`` (or the
environment's ``Settings().DB_URL``) points somewhere. **This script must
never be pointed at prod griddb from this slice** — GD1's own migration
(``migrations/versions/security_master_20260927.py``) has not merged yet,
so prod has no ``security_master`` table to write into; ``--apply`` exists
for testing against a disposable scratch database only.

Usage::

    python3 scripts/seed_security_master_technology.py                  # dry run (default)
    python3 scripts/seed_security_master_technology.py --dry-run
    python3 scripts/seed_security_master_technology.py --apply --db-url postgresql://...
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

# Ensure project root is on sys.path so imports work when run standalone
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from loguru import logger as log

from grid.signals.sponsor_resolver import SEC_COMPANY_TICKERS, SEC_UA
from intelligence.actor_identity import ticker_actor_id
from intelligence.security_master import (
    ID_SCHEME_ACTOR_CORP,
    ID_SCHEME_CIK,
    ID_SCHEME_TICKER,
    compute_sector_weights,
    entity_id_for_cik,
    entity_id_for_ticker,
    evaluate_delisting_candidate,
    propose_primary_sector,
)

DEFAULT_SECTOR = "Technology"
DEFAULT_TAXONOMY = "sector_map_v1"
SEC_TIMEOUT_S = 15
OUTPUT_DIR_NAME = "security_master"


# ── SEC company_tickers.json (injectable http_get for tests) ──────────────


def fetch_sec_company_tickers(
    http_get: Callable[..., Any],
) -> dict[str, dict[str, Any]]:
    """``{TICKER: {"cik": int, "name": str}}`` from SEC ``company_tickers.json``.

    Empty dict on any network/parse failure — callers then treat every
    ticker as unmatched, which is the conservative direction (surfaces as
    "no CIK found", never fabricates one).
    """
    try:
        resp = http_get(
            SEC_COMPANY_TICKERS,
            headers={"User-Agent": SEC_UA, "Accept": "application/json"},
            timeout=SEC_TIMEOUT_S,
        )
        status = getattr(resp, "status_code", 200)
        if status != 200:
            log.warning("seed_security_master: SEC company_tickers.json HTTP {s}", s=status)
            return {}
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        log.warning("seed_security_master: SEC company_tickers.json failed: {e}", e=str(exc))
        return {}
    return parse_sec_company_tickers(data)


def parse_sec_company_tickers(data: Any) -> dict[str, dict[str, Any]]:
    """Pure parse of a decoded ``company_tickers.json`` payload (test seam)."""
    out: dict[str, dict[str, Any]] = {}
    entries = data.values() if isinstance(data, dict) else (data or [])
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        ticker = str(entry.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        cik = entry.get("cik_str")
        try:
            cik_int = int(str(cik).strip()) if cik is not None and str(cik).strip() else None
        except (TypeError, ValueError):
            cik_int = None
        name = str(entry.get("title") or "").strip()
        # SEC lists share classes (GOOG/GOOGL) as separate ticker rows already
        # keyed uniquely — no collision handling needed at this level.
        out[ticker] = {"cik": cik_int, "name": name or None}
    return out


# ── Technology universe from the sector map ────────────────────────────────


def build_sector_universe(sector_map: dict[str, Any], sector: str) -> list[dict[str, Any]]:
    """Distinct ``{ticker, name, description}`` company actors under ``sector``.

    Only ``type: company`` entries count — the sector map also lists people,
    funds, sovereigns and ETF proxies (``BITO``, ``TLT``, ``UUP``) in the same
    per-subsector actor lists, none of which are securities this table
    represents.
    """
    sector_data = (sector_map or {}).get(sector) or {}
    seen: dict[str, dict[str, Any]] = {}
    for subsector_data in (sector_data.get("subsectors") or {}).values():
        for actor in subsector_data.get("actors") or []:
            if actor.get("type") != "company":
                continue
            ticker = str(actor.get("ticker") or "").strip().upper()
            if not ticker or ticker in seen:
                continue
            seen[ticker] = {
                "ticker": ticker,
                "name": str(actor.get("name") or "").strip() or ticker,
                "description": actor.get("description"),
            }
    return sorted(seen.values(), key=lambda r: r["ticker"])


# ── Per-ticker row building (pure) ──────────────────────────────────────────


@dataclass
class TickerPlan:
    ticker: str
    entity_id: str
    has_cik: bool
    is_multi_sector: bool
    proposed_primary_sector: Optional[str]
    security_master: dict[str, Any]
    security_identifiers: list[dict[str, Any]] = field(default_factory=list)
    security_sector_membership: list[dict[str, Any]] = field(default_factory=list)


def build_ticker_plan(
    ticker: str,
    sector_map_name: str,
    sec_entry: Optional[dict[str, Any]],
    full_sector_map: dict[str, Any],
    as_of: date,
    run_source: str = "seed_security_master_technology",
) -> TickerPlan:
    """Build the proposed rows for one ticker. No I/O — pure over its inputs."""
    ticker = ticker.strip().upper()
    cik = (sec_entry or {}).get("cik")
    name = (sec_entry or {}).get("name") or sector_map_name or ticker
    entity_id = entity_id_for_cik(cik) if cik else entity_id_for_ticker(ticker)

    weights = compute_sector_weights(full_sector_map, ticker)
    proposed_primary, is_multi = propose_primary_sector(weights)

    provenance = {
        "seed_script": run_source,
        "built_at": as_of.isoformat(),
        "cik_source": "sec_company_tickers" if cik else None,
    }

    # Owner decision #2 (adopted 2026-09-28): SEC absence alone is a
    # candidate signal, never sufficient to flip is_active — see
    # intelligence.security_master.evaluate_delisting_candidate. This seed
    # script has no second-source corroboration wired in, so every ticker
    # without a live CIK stays active with the candidate basis recorded.
    delisting = evaluate_delisting_candidate(has_live_cik=bool(cik))

    security_master_row = {
        "entity_id": entity_id,
        "cik": cik,
        "name": name,
        "security_type": "equity",
        "is_active": delisting.is_active,
        "delisted_at": None,
        "delisted_reason": delisting.delisted_reason,
        "delisted_basis": delisting.delisted_basis,
        "sic": None,
        "source": "sector_map+sec_company_tickers" if cik else "sector_map",
        "provenance": provenance,
    }

    identifiers = [{
        "entity_id": entity_id,
        "id_scheme": ID_SCHEME_TICKER,
        "id_value": ticker,
        "valid_from": as_of.isoformat(),
        "valid_to": None,
        "is_primary": True,
        "source": "sector_map",
        "conflict_flag": False,
        "conflict_detail": None,
    }, {
        "entity_id": entity_id,
        "id_scheme": ID_SCHEME_ACTOR_CORP,
        "id_value": ticker_actor_id(ticker),
        "valid_from": as_of.isoformat(),
        "valid_to": None,
        "is_primary": True,
        "source": "actor_identity.ticker_actor_id",
        "conflict_flag": False,
        "conflict_detail": None,
    }]
    if cik:
        identifiers.append({
            "entity_id": entity_id,
            "id_scheme": ID_SCHEME_CIK,
            "id_value": str(cik),
            "valid_from": as_of.isoformat(),
            "valid_to": None,
            "is_primary": True,
            "source": "sec_company_tickers",
            "conflict_flag": False,
            "conflict_detail": None,
        })

    sector_rows = []
    for sector_name, weight in sorted(weights.items()):
        is_primary = sector_name == proposed_primary
        sector_rows.append({
            "entity_id": entity_id,
            "taxonomy": DEFAULT_TAXONOMY,
            "sector": sector_name,
            "subsector": None,
            "is_primary": is_primary,
            "tie_break_method": "subsector_weight_proposed" if is_multi else None,
            "weight": weight,
            "source": "sector_map",
            # Every multi-sector row is flagged, even the proposed winner —
            # GD0 §3 found no primary-sector rule anywhere in the codebase,
            # so a proposal from this script is not a settled fact.
            "conflict_flag": is_multi,
            "conflict_detail": {"weights": weights} if is_multi else None,
            "valid_from": as_of.isoformat(),
            "valid_to": None,
        })

    return TickerPlan(
        ticker=ticker,
        entity_id=entity_id,
        has_cik=bool(cik),
        is_multi_sector=is_multi,
        proposed_primary_sector=proposed_primary,
        security_master=security_master_row,
        security_identifiers=identifiers,
        security_sector_membership=sector_rows,
    )


# ── Plan assembly + report ─────────────────────────────────────────────────


def build_seed_plan(
    sector_map: dict[str, Any],
    sec_map: dict[str, dict[str, Any]],
    sector: str = DEFAULT_SECTOR,
    as_of: Optional[date] = None,
) -> dict[str, Any]:
    """Full dry-run plan + diff report for ``sector``. No I/O."""
    as_of = as_of or date.today()
    universe = build_sector_universe(sector_map, sector)

    ticker_plans = [
        build_ticker_plan(row["ticker"], row["name"], sec_map.get(row["ticker"]), sector_map, as_of)
        for row in universe
    ]

    unmatched = [tp.ticker for tp in ticker_plans if not tp.has_cik]
    multi_sector = [tp for tp in ticker_plans if tp.is_multi_sector]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sector": sector,
        "as_of": as_of.isoformat(),
        "counts": {
            "total_tickers": len(ticker_plans),
            "matched_cik": len(ticker_plans) - len(unmatched),
            "unmatched_no_cik": len(unmatched),
            "multi_sector": len(multi_sector),
        },
        "unmatched_no_cik": sorted(unmatched),
        "multi_sector": [
            {
                "ticker": tp.ticker,
                "proposed_primary_sector": tp.proposed_primary_sector,
                "weights": tp.security_sector_membership[0]["conflict_detail"]["weights"]
                if tp.security_sector_membership and tp.security_sector_membership[0]["conflict_detail"]
                else {},
            }
            for tp in multi_sector
        ],
        "rows": {
            "security_master": [tp.security_master for tp in ticker_plans],
            "security_identifiers": [row for tp in ticker_plans for row in tp.security_identifiers],
            "security_sector_membership": [row for tp in ticker_plans for row in tp.security_sector_membership],
        },
    }


def write_report(plan: dict[str, Any], output_dir: Path) -> Path:
    """Write the plan as JSON + a short Markdown summary; returns the JSON path."""
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = plan["generated_at"].replace(":", "").replace("+00:00", "Z")
    json_path = output_dir / f"seed_plan_{plan['sector'].lower()}_{stamp}.json"
    md_path = output_dir / f"seed_plan_{plan['sector'].lower()}_{stamp}.md"

    json_path.write_text(json.dumps(plan, indent=2, sort_keys=True), encoding="utf-8")

    counts = plan["counts"]
    lines = [
        f"# security_master seed plan — {plan['sector']} ({plan['as_of']})",
        "",
        f"- Total tickers: {counts['total_tickers']}",
        f"- Matched to a live SEC CIK: {counts['matched_cik']}",
        f"- Unmatched (no live CIK — delisting/acquisition candidates, NOT confirmed): "
        f"{counts['unmatched_no_cik']}",
        f"- Multi-sector (primary-sector tie-break is a PROPOSAL pending owner sign-off): "
        f"{counts['multi_sector']}",
        "",
        "## Unmatched tickers (no live SEC CIK)",
        "",
    ]
    lines.extend([f"- {t}" for t in plan["unmatched_no_cik"]] or ["- (none)"])
    lines.extend([
        "",
        "## Multi-sector proposals",
        "",
    ])
    for row in plan["multi_sector"]:
        lines.append(f"- {row['ticker']}: proposed primary = {row['proposed_primary_sector']!r}, weights = {row['weights']}")
    if not plan["multi_sector"]:
        lines.append("- (none)")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path


# ── --apply (never against prod from this slice; see module docstring) ─────


def apply_plan(engine: Any, plan: dict[str, Any]) -> dict[str, int]:
    """Upsert the plan's rows. Caller is responsible for pointing ``engine``
    at a database that actually has the GD1 tables (run
    :func:`intelligence.security_master.ensure_tables` first) and for never
    pointing this at prod griddb from this slice (see module docstring).
    """
    from sqlalchemy import text

    from intelligence.security_master import ensure_tables

    ensure_tables(engine)
    counts = {"security_master": 0, "security_identifiers": 0, "security_sector_membership": 0}

    with engine.begin() as conn:
        for row in plan["rows"]["security_master"]:
            conn.execute(
                text(
                    "INSERT INTO security_master "
                    "(entity_id, cik, name, security_type, is_active, delisted_at, "
                    " delisted_reason, delisted_basis, sic, source, provenance) "
                    "VALUES (:entity_id, :cik, :name, :security_type, :is_active, :delisted_at, "
                    " :delisted_reason, :delisted_basis, :sic, :source, CAST(:provenance AS jsonb)) "
                    "ON CONFLICT (entity_id) DO UPDATE SET "
                    " name = EXCLUDED.name, updated_at = NOW()"
                ),
                {**row, "provenance": json.dumps(row["provenance"])},
            )
            counts["security_master"] += 1

        for row in plan["rows"]["security_identifiers"]:
            conn.execute(
                text(
                    "INSERT INTO security_identifiers "
                    "(entity_id, id_scheme, id_value, valid_from, valid_to, is_primary, "
                    " source, conflict_flag, conflict_detail) "
                    "VALUES (:entity_id, :id_scheme, :id_value, :valid_from, :valid_to, :is_primary, "
                    " :source, :conflict_flag, CAST(:conflict_detail AS jsonb)) "
                    "ON CONFLICT (entity_id, id_scheme, id_value, valid_from) DO NOTHING"
                ),
                {**row, "conflict_detail": json.dumps(row["conflict_detail"]) if row["conflict_detail"] else None},
            )
            counts["security_identifiers"] += 1

        for row in plan["rows"]["security_sector_membership"]:
            conn.execute(
                text(
                    "INSERT INTO security_sector_membership "
                    "(entity_id, taxonomy, sector, subsector, is_primary, tie_break_method, "
                    " weight, source, conflict_flag, conflict_detail, valid_from, valid_to) "
                    "VALUES (:entity_id, :taxonomy, :sector, :subsector, :is_primary, :tie_break_method, "
                    " :weight, :source, :conflict_flag, CAST(:conflict_detail AS jsonb), :valid_from, :valid_to) "
                    "ON CONFLICT (entity_id, taxonomy, sector, valid_from) DO NOTHING"
                ),
                {**row, "conflict_detail": json.dumps(row["conflict_detail"]) if row["conflict_detail"] else None},
            )
            counts["security_sector_membership"] += 1

    return counts


# ── CLI ──────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sector", default=DEFAULT_SECTOR, help=f"Sector to seed (default: {DEFAULT_SECTOR})")
    parser.add_argument(
        "--output-dir", default=None,
        help="Report output directory (default: <repo>/outputs/security_master)",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help=(
            "Write the plan via --db-url. NEVER point this at prod griddb from this "
            "slice (GD1's migration has not merged there yet). Without it the script "
            "only plans and writes the JSON/Markdown report (the default)."
        ),
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Explicitly plan only (the default when --apply is absent).",
    )
    parser.add_argument("--db-url", default=None, help="SQLAlchemy URL to apply against (with --apply only)")
    args = parser.parse_args()

    if args.apply and args.dry_run:
        parser.error("--apply and --dry-run are mutually exclusive")

    import requests

    from analysis.sector_map import SECTOR_MAP

    sec_map = fetch_sec_company_tickers(requests.get)
    plan = build_seed_plan(SECTOR_MAP, sec_map, sector=args.sector)

    output_dir = Path(args.output_dir) if args.output_dir else Path(_PROJECT_ROOT) / "outputs" / OUTPUT_DIR_NAME
    report_path = write_report(plan, output_dir)
    log.info("seed_security_master: report written to {p}", p=report_path)
    print(json.dumps(plan["counts"], indent=2, sort_keys=True))

    if args.apply:
        if not args.db_url:
            parser.error("--apply requires --db-url (never prod griddb from this slice)")
        from sqlalchemy import create_engine

        engine = create_engine(args.db_url)
        applied = apply_plan(engine, plan)
        print(json.dumps({"applied": applied}, indent=2, sort_keys=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
