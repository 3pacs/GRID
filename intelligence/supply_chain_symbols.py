"""Supply-chain graph node -> tradable US symbol (3pacs/GRID#1179).

``supply_chain_nodes`` mixes listed companies, private companies, commodities,
countries and regulators, and many listed companies are stored under a slug
(``micron``, ``tsmc``) rather than their symbol. Contagion scoring used to
guess ("no underscore and at most 8 characters" is a ticker), so OPENAI,
CARGILL and TSMC were scored as symbols and the commodity ``corn`` as the
CORN fund. :func:`symbol_for` is the one rule every consumer uses instead:

1. a node whose type is not ``ticker`` (private_company, commodity, country,
   regulator, region) is untradable, named by its type;
2. a slug in :data:`NODE_SYMBOLS` maps to its US-listed symbol (each one
   checked to have a ``YF:<symbol>:close`` series in griddb on 2026-11-13);
3. a delisted or acquired company in :data:`DELISTED` is untradable with
   the reason; an acquirer's price says nothing about its target;
4. an id with a foreign exchange suffix (``005930.ks``, ``2317.tw``), or a
   foreign company whose slug looks like a US symbol (:data:`FOREIGN_SLUGS`:
   ``lindt``, ``lonza``), is untradable: no US listing is scored;
5. otherwise, only for a node typed ``ticker``, the upper-cased id is the
   symbol if it looks like a US symbol (1-5 letters, optional class suffix
   such as ``BF-B``). A node whose type is unknown (``node_type`` None: no
   ``supply_chain_nodes`` row) is untradable, "type unknown": ``corn`` alone
   would otherwise be scored as the CORN fund.

Callers still treat a symbol without a price series as untestable; this map
only stops non-symbols from being scored as symbols. Pure except
:func:`load_node_types` (one read-only query).
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text

NON_TRADABLE_TYPES = frozenset({"private_company", "commodity", "country", "regulator", "region"})

# Slug -> US-listed symbol. Every symbol had a YF close series in griddb on 2026-11-13.
NODE_SYMBOLS: dict[str, str] = {
    "air_products": "APD",
    "akamai": "AKAM",
    "albemarle": "ALB",
    "ardagh": "AMBP",  # Ardagh Metal Packaging
    "bf_b": "BF-B",
    "bunge": "BG",
    "cameco": "CCJ",
    "canadian_solar": "CSIQ",
    "cloudflare": "NET",
    "coherent": "COHR",
    "corning": "GLW",
    "crown_holdings": "CCK",
    "fastly": "FSLY",
    "flowers_foods": "FLO",
    "hain_celestial": "HAIN",
    "intel": "INTC",
    "international_paper": "IP",
    "kfc_parent": "YUM",
    "linde": "LIN",
    "lumentum": "LITE",
    "micron": "MU",
    "mp_materials": "MP",
    "nextdecade": "NEXT",
    "oi_glass": "OI",
    "packaging_corp": "PKG",
    "post_holdings": "POST",
    "sq": "XYZ",  # Block renamed its symbol from SQ
    "sysco": "SYY",
    "treehouse": "THS",
    "tsmc": "TSM",  # NYSE ADR
    "tyson": "TSN",
    "venture_global": "VG",
    "westrock": "SW",  # merged into Smurfit Westrock
}

DELISTED: dict[str, str] = {
    "catalent": "acquired by Novo Holdings (2024)",
    "livent": "merged into Arcadium Lithium, since acquired",
    "pfpt": "Proofpoint, taken private (2021)",
    "pxd": "Pioneer, acquired by ExxonMobil (2024)",
    "splk": "Splunk, acquired by Cisco (2024)",
    "us_silica": "taken private (2024)",
}

# Foreign companies whose slug looks like a US symbol (no YF close series under it, 2026-11-13).
FOREIGN_SLUGS: dict[str, str] = {
    "cmoc": "CMOC Group (Shanghai / Hong Kong)",
    "divis": "Divi's Laboratories (India)",
    "jinko": "JinkoSolar (Shanghai; US ADR delisted)",
    "jsr": "JSR Corporation (Tokyo; taken private)",
    "lindt": "Lindt & Spruengli (SIX Swiss)",
    "longi": "LONGi Green Energy (Shanghai)",
    "lonza": "Lonza Group (SIX Swiss)",
    "lynas": "Lynas Rare Earths (ASX)",
    "sumco": "SUMCO Corp (Tokyo)",
    "trina": "Trina Solar (Shanghai)",
}
UNKNOWN_LISTING: frozenset[str] = frozenset({"cyrk"})

_FOREIGN_SUFFIX = re.compile(r"\.(ks|kq|sz|ss|hk|tw|two|t|de|pa|l|as|sw|mi|to|ax|f)$", re.IGNORECASE)
_US_SYMBOL = re.compile(r"^[A-Z]{1,5}(-[A-Z])?$")


@dataclass(frozen=True)
class Tradable:
    node_id: str
    symbol: str | None
    reason: str  # "type:<t>", "type unknown", "alias", "id", "delisted: ...", "foreign listing", "not a US symbol"


def symbol_for(node_id: str, node_type: str | None = None) -> Tradable:
    """The US symbol a contagion call on ``node_id`` is scored on, or why there is none."""
    nid = (node_id or "").strip()
    key = nid.lower()
    if not key:
        return Tradable(nid, None, "empty id")
    if node_type in NON_TRADABLE_TYPES:
        return Tradable(nid, None, f"type:{node_type}")
    if key in NODE_SYMBOLS:
        return Tradable(nid, NODE_SYMBOLS[key], "alias")
    if key in DELISTED:
        return Tradable(nid, None, f"delisted: {DELISTED[key]}")
    if _FOREIGN_SUFFIX.search(key) or key in FOREIGN_SLUGS:
        return Tradable(nid, None, "foreign listing")
    if key in UNKNOWN_LISTING:
        return Tradable(nid, None, "no listing found")
    if node_type != "ticker":
        return Tradable(nid, None, "type unknown" if node_type is None else f"type:{node_type}")
    symbol = nid.upper()
    if not _US_SYMBOL.match(symbol):
        return Tradable(nid, None, "not a US symbol")
    return Tradable(nid, symbol, "id")


def load_node_types(conn: Any, ids: Iterable[str]) -> dict[str, str]:
    """``{lower(id): type}`` for the given node ids (one read-only query).

    A database error propagates: a caller that cannot read the types must not
    score, because :func:`symbol_for` without a type fails closed only per id.
    """
    wanted = sorted({str(i).strip() for i in ids if i})
    if not wanted:
        return {}
    rows = conn.execute(
        text("SELECT id, type FROM supply_chain_nodes WHERE LOWER(id) = ANY(:ids)"),
        {"ids": [w.lower() for w in wanted]},
    ).fetchall()
    return {str(r[0]).lower(): str(r[1]) for r in rows}
