"""Backfill supply_chain_edges from harvester `no_matching_edge` log rows.

Phase 4e's LLM harvester found 52 citation-backed concentration percentages
that could not be written because no matching ``supply_chain_edges`` row
existed for the ticker + counterparty pair. The harvester logged each of
these to ``supply_chain_enrichment_log`` with ``reason='no_matching_edge'``.

This one-shot script reverses the flow: we re-read the log, resolve the
counterparty to a node (creating a ``private_company`` if it's new, or
canonicalizing to an existing ``ticker`` node like ``wmt`` / ``cost``),
and upsert the missing customer edge with ``pct_upstream_revenue`` set.

Idempotent: rows already covered by an existing edge are skipped; the
script uses ``ON CONFLICT`` on the edge unique key so re-runs are a
no-op once every counterparty has been reconciled.

Run on the server:

    cd /data/grid_v4/astrogrid_dedup && source ~/grid_v4/venv/bin/activate
    python3 scripts/backfill_harvester_edges.py
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from db import get_engine


# ── Tunables ────────────────────────────────────────────────────────────────

# Generic / anonymous counterparty tokens we refuse to materialize as a
# node. These LLM findings are accurate disclosures (NVDA's 22% customer
# really is some hyperscaler) but we have no way to attribute them to a
# concrete actor, so they stay rejected.
_ANONYMOUS_TOKENS: tuple[str, ...] = (
    "not specified",
    "one customer",
    "one direct customer",
    "another direct customer",
    "top five",
    "top 5",
    "five largest",
    "largest customer",  # only if bare, handled below
    "ai research",
    "semiconductor solutions customer",
    "client and gaming segment customer",
    "direct customer",
    "one distributor",
    "undisclosed",
    "anonymous",
    "unnamed",
    "certain",
)

# Hard-coded canonical name → existing node id overrides. Any LLM counterparty
# name that contains one of these keys (case-insensitive) is mapped to the
# existing ticker node rather than a new private_company.
_NAME_TO_NODE: tuple[tuple[str, str], ...] = (
    ("walmart", "wmt"),
    ("wal-mart", "wmt"),
    ("sam's club", "wmt"),
    ("costco", "cost"),
    ("kroger", "kr"),
    ("target corp", "tgt"),
    ("amazon.com", "amzn"),
    ("coca-cola europacific", "ccep"),
    ("coca-cola consolidated", "coke"),
    ("td synnex", "snx"),
    ("synnex", "snx"),
    ("arrow electronics", "arw"),
    ("avnet", "avt"),
    ("ingram micro", "im"),
    ("procter & gamble", "pg"),
    ("the procter & gamble", "pg"),
    ("apple inc", "aapl"),
    ("microsoft corp", "msft"),
    ("google", "googl"),
    ("meta platforms", "meta"),
    ("tesla inc", "tsla"),
    ("nvidia corp", "nvda"),
)


# ── Data containers ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class LogRow:
    log_id: int
    ticker: str
    upstream_label: str
    parsed_pct: float
    parsed_citation: str
    raw_response: str


@dataclass
class Summary:
    log_rows_considered: int = 0
    skipped_anonymous: int = 0
    skipped_bad_direction: int = 0
    skipped_already_present: int = 0
    nodes_created: int = 0
    edges_created: int = 0
    edges_updated: int = 0
    errors: int = 0
    examples: list[dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.examples is None:
            self.examples = []


# ── Helpers ─────────────────────────────────────────────────────────────────


_LEGAL_SUFFIX_RE = re.compile(
    r",?\s+(?:Inc\.?|LLC|L\.L\.C\.?|Ltd\.?|Limited|Corp\.?|Corporation|"
    r"Company|Co\.?|Holdings|Group|plc|PLC|AG|SA|N\.V\.?|S\.A\.?|"
    r"Technologies|International|Incorporated|Stores|and\s+its\s+affiliates|"
    r"and\s+its\s+subsidiaries|and\s+affiliates|and\s+subsidiaries)\s*$",
    re.IGNORECASE,
)


def _clean_name(raw: str) -> str:
    """Strip legal suffix(es) and paren aliases; return trimmed name."""
    name = (raw or "").strip().strip(' "\'.,;:')
    # Drop trailing parenthetical alias.
    name = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
    # Apply legal-suffix stripping up to two times (handles "Inc., Ltd.").
    for _ in range(2):
        stripped = _LEGAL_SUFFIX_RE.sub("", name).strip().rstrip(",.")
        if stripped == name or not stripped:
            break
        name = stripped
    return name.strip() or raw.strip()


def _slugify(name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "_", name.lower()).strip("_")
    slug = re.sub(r"_+", "_", slug)
    return slug[:60] or "unknown"


def _is_anonymous(name: str) -> bool:
    ln = (name or "").lower().strip()
    if not ln or len(ln) < 4:
        return True
    for token in _ANONYMOUS_TOKENS:
        if token in ln:
            # "largest customer" alone is anonymous; "walmart, our largest
            # customer" is fine. So require token to be most of the name.
            if token == "largest customer" and len(ln) > 25:
                continue
            return True
    return False


def _override_node_id(name: str) -> str | None:
    ln = (name or "").lower()
    for key, node_id in _NAME_TO_NODE:
        if key in ln:
            return node_id
    return None


def _infer_direction(raw_response: str, citation: str) -> str:
    """Return 'customer' or 'supplier' based on the harvester finding.

    Fall back to citation heuristics if the JSON ``direction`` field is
    missing or looks wrong.
    """
    try:
        data = json.loads(raw_response or "")
    except (ValueError, TypeError):
        data = {}
    findings = (data or {}).get("findings") or []
    if findings and isinstance(findings, list):
        direction = str(findings[0].get("direction", "")).lower()
        if direction in ("customer", "supplier"):
            return direction
    # Heuristic fallback from citation language.
    cit = (citation or "").lower()
    if (
        "sales to" in cit
        or "revenue from" in cit
        or "largest customer" in cit
        or "net sales" in cit
        or "consolidated net sales" in cit
        or "accounted for" in cit and "customer" in cit
        or "represented" in cit and "sales" in cit
    ):
        return "customer"
    if "cost of" in cit or "purchases from" in cit or "supplier" in cit:
        return "supplier"
    return "customer"  # vast majority of harvester findings are customer-side


# ── DB operations ───────────────────────────────────────────────────────────


def _fetch_log_rows(engine: Engine) -> list[LogRow]:
    sql = text(
        """
        SELECT id, ticker, upstream_label, parsed_pct, parsed_citation,
               raw_response
          FROM supply_chain_enrichment_log
         WHERE reason = 'no_matching_edge'
           AND parsed_pct IS NOT NULL
           AND parsed_citation IS NOT NULL
         ORDER BY id
        """
    )
    out: list[LogRow] = []
    with engine.connect() as conn:
        for r in conn.execute(sql).fetchall():
            out.append(
                LogRow(
                    log_id=int(r[0]),
                    ticker=str(r[1] or "").upper(),
                    upstream_label=str(r[2] or "").strip(),
                    parsed_pct=float(r[3]),
                    parsed_citation=str(r[4] or ""),
                    raw_response=str(r[5] or ""),
                )
            )
    return out


def _resolve_or_create_node(
    engine: Engine,
    raw_name: str,
    summary: Summary,
) -> str | None:
    """Return the node id to use for this counterparty, creating a new
    ``private_company`` row if necessary.
    """
    if _is_anonymous(raw_name):
        return None

    # 1. Hard-coded overrides to existing tickers.
    override = _override_node_id(raw_name)
    if override:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT id FROM supply_chain_nodes WHERE id = :i"),
                {"i": override},
            ).fetchone()
            if row:
                return override

    cleaned = _clean_name(raw_name)

    # 2. Case-insensitive name match on existing nodes.
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id FROM supply_chain_nodes
                 WHERE lower(name) = lower(:n)
                    OR lower(name) LIKE lower(:lk)
                 ORDER BY
                    CASE WHEN type = 'ticker' THEN 0 ELSE 1 END,
                    length(name)
                 LIMIT 1
                """
            ),
            {"n": cleaned, "lk": f"{cleaned}%"},
        ).fetchone()
        if row:
            return str(row[0])

    # 3. Create new private_company node.
    node_id = _slugify(cleaned)
    if not node_id or len(node_id) < 3:
        return None
    try:
        with engine.begin() as conn:
            # Avoid clobbering a pre-existing id.
            existing = conn.execute(
                text("SELECT id FROM supply_chain_nodes WHERE id = :i"),
                {"i": node_id},
            ).fetchone()
            if existing:
                return node_id
            conn.execute(
                text(
                    """
                    INSERT INTO supply_chain_nodes (id, name, type, notes)
                    VALUES (:id, :name, 'private_company',
                            'auto-created from 10-K LLM harvester backfill')
                    ON CONFLICT (id) DO NOTHING
                    """
                ),
                {"id": node_id, "name": cleaned},
            )
        summary.nodes_created += 1
        return node_id
    except Exception as exc:
        log.warning("node create failed for {n}: {e}", n=node_id, e=str(exc))
        summary.errors += 1
        return None


def _ticker_node_exists(engine: Engine, ticker: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM supply_chain_nodes WHERE id = :i"),
            {"i": ticker.lower()},
        ).fetchone()
        return bool(row)


def _ensure_ticker_node(engine: Engine, ticker: str) -> str | None:
    tid = ticker.lower()
    if _ticker_node_exists(engine, tid):
        return tid
    # Don't silently create ticker nodes; tickers should already exist.
    log.warning("ticker node missing: {t} — skipping", t=tid)
    return None


def _upsert_customer_edge(
    engine: Engine,
    seller_id: str,
    buyer_id: str,
    pct: float,
    citation: str,
) -> str:
    """Upsert a customer edge (seller → buyer). Returns
    'created' | 'updated' | 'already_set' | 'error'.
    """
    snippet = (citation or "")[:200].replace("\n", " ")
    source = f"10-K LLM backfill: {snippet}"
    try:
        with engine.begin() as conn:
            # Does an identical-key edge already exist?
            row = conn.execute(
                text(
                    """
                    SELECT id, pct_upstream_revenue FROM supply_chain_edges
                     WHERE upstream_id = :u
                       AND downstream_id = :d
                       AND relationship = 'customer'
                       AND as_of IS NOT DISTINCT FROM :as_of
                    """
                ),
                {"u": seller_id, "d": buyer_id, "as_of": None},
            ).fetchone()
            if row:
                if row[1] is not None and abs(float(row[1]) - pct) < 0.005:
                    return "already_set"
                conn.execute(
                    text(
                        """
                        UPDATE supply_chain_edges
                           SET pct_upstream_revenue = :pct,
                               confidence = 'derived',
                               source = :src
                         WHERE id = :id
                        """
                    ),
                    {"pct": pct, "src": source, "id": int(row[0])},
                )
                return "updated"
            conn.execute(
                text(
                    """
                    INSERT INTO supply_chain_edges (
                        upstream_id, downstream_id, relationship, tier,
                        pct_upstream_revenue, confidence, source
                    ) VALUES (
                        :u, :d, 'customer', 1, :pct, 'derived', :src
                    )
                    ON CONFLICT (upstream_id, downstream_id, relationship, as_of)
                    DO UPDATE SET
                        pct_upstream_revenue = EXCLUDED.pct_upstream_revenue,
                        confidence = EXCLUDED.confidence,
                        source = EXCLUDED.source
                    """
                ),
                {"u": seller_id, "d": buyer_id, "pct": pct, "src": source},
            )
        return "created"
    except Exception as exc:
        log.warning(
            "edge upsert failed {u}->{d}: {e}", u=seller_id, d=buyer_id, e=str(exc)
        )
        return "error"


def _upsert_supplier_edge(
    engine: Engine,
    supplier_id: str,
    buyer_id: str,
    pct: float,
    citation: str,
) -> str:
    snippet = (citation or "")[:200].replace("\n", " ")
    source = f"10-K LLM backfill: {snippet}"
    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT id, pct_downstream_cogs FROM supply_chain_edges
                     WHERE upstream_id = :u
                       AND downstream_id = :d
                       AND relationship IN ('component','raw_material','supplier')
                     LIMIT 1
                    """
                ),
                {"u": supplier_id, "d": buyer_id},
            ).fetchone()
            if row:
                if row[1] is not None and abs(float(row[1]) - pct) < 0.005:
                    return "already_set"
                conn.execute(
                    text(
                        """
                        UPDATE supply_chain_edges
                           SET pct_downstream_cogs = :pct,
                               confidence = 'derived',
                               source = :src
                         WHERE id = :id
                        """
                    ),
                    {"pct": pct, "src": source, "id": int(row[0])},
                )
                return "updated"
            conn.execute(
                text(
                    """
                    INSERT INTO supply_chain_edges (
                        upstream_id, downstream_id, relationship, tier,
                        pct_downstream_cogs, confidence, source
                    ) VALUES (
                        :u, :d, 'component', 1, :pct, 'derived', :src
                    )
                    ON CONFLICT (upstream_id, downstream_id, relationship, as_of)
                    DO UPDATE SET
                        pct_downstream_cogs = EXCLUDED.pct_downstream_cogs,
                        confidence = EXCLUDED.confidence,
                        source = EXCLUDED.source
                    """
                ),
                {"u": supplier_id, "d": buyer_id, "pct": pct, "src": source},
            )
        return "created"
    except Exception as exc:
        log.warning(
            "supplier edge upsert failed {u}->{d}: {e}",
            u=supplier_id, d=buyer_id, e=str(exc),
        )
        return "error"


# ── Main loop ───────────────────────────────────────────────────────────────


def backfill(engine: Engine) -> Summary:
    summary = Summary()
    rows = _fetch_log_rows(engine)
    summary.log_rows_considered = len(rows)

    # De-dup identical (ticker, counterparty, pct) tuples up front to avoid
    # writing the same edge 5x.
    seen: set[tuple[str, str, float]] = set()

    for row in rows:
        if _is_anonymous(row.upstream_label):
            summary.skipped_anonymous += 1
            continue

        direction = _infer_direction(row.raw_response, row.parsed_citation)

        # Filter out the PG-as-supplier-to-PG nonsense and similar self-edges.
        cleaned = _clean_name(row.upstream_label)
        if cleaned.lower() == row.ticker.lower() or _slugify(cleaned) == row.ticker.lower():
            summary.skipped_bad_direction += 1
            continue

        # Verify the citation supports the direction. Direction='supplier'
        # with pure marketing language is almost always a hallucination.
        cit_l = row.parsed_citation.lower()
        if direction == "supplier" and not (
            "cost" in cit_l or "purchases from" in cit_l or "supplier" in cit_l
        ):
            summary.skipped_bad_direction += 1
            continue

        filer_id = _ensure_ticker_node(engine, row.ticker)
        if not filer_id:
            summary.errors += 1
            continue

        counterparty_id = _resolve_or_create_node(
            engine, row.upstream_label, summary
        )
        if not counterparty_id:
            summary.skipped_anonymous += 1
            continue

        if counterparty_id == filer_id:
            summary.skipped_bad_direction += 1
            continue

        if direction == "customer":
            seller, buyer = filer_id, counterparty_id
            key = (seller, buyer, round(row.parsed_pct, 4))
            if key in seen:
                continue
            seen.add(key)
            result = _upsert_customer_edge(
                engine, seller, buyer, row.parsed_pct, row.parsed_citation
            )
            target_pct_field = "pct_upstream_revenue"
        else:  # supplier
            supplier, buyer = counterparty_id, filer_id
            key = (supplier, buyer, round(row.parsed_pct, 4))
            if key in seen:
                continue
            seen.add(key)
            result = _upsert_supplier_edge(
                engine, supplier, buyer, row.parsed_pct, row.parsed_citation
            )
            target_pct_field = "pct_downstream_cogs"

        if result == "created":
            summary.edges_created += 1
            summary.examples.append(
                {
                    "log_id": row.log_id,
                    "direction": direction,
                    "filer": row.ticker,
                    "counterparty": cleaned,
                    "counterparty_id": counterparty_id,
                    "field": target_pct_field,
                    "pct": row.parsed_pct,
                    "citation": row.parsed_citation[:140],
                }
            )
        elif result == "updated":
            summary.edges_updated += 1
        elif result == "already_set":
            summary.skipped_already_present += 1
        else:
            summary.errors += 1

    return summary


def main() -> int:
    engine = get_engine()
    log.info("backfill_harvester_edges: starting")
    summary = backfill(engine)
    log.info(
        "backfill_harvester_edges: considered={c} created={cr} updated={up} "
        "nodes_created={n} already={a} skipped_anon={sa} skipped_dir={sd} "
        "errors={e}",
        c=summary.log_rows_considered,
        cr=summary.edges_created,
        up=summary.edges_updated,
        n=summary.nodes_created,
        a=summary.skipped_already_present,
        sa=summary.skipped_anonymous,
        sd=summary.skipped_bad_direction,
        e=summary.errors,
    )
    if summary.examples:
        log.info("sample new edges:")
        for ex in summary.examples[:10]:
            log.info(
                "  {filer} -> {cp} ({cid}) {field}={pct} [{direction}]",
                filer=ex["filer"],
                cp=ex["counterparty"],
                cid=ex["counterparty_id"],
                field=ex["field"],
                pct=ex["pct"],
                direction=ex["direction"],
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
