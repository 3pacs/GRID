"""
GRID Intelligence — actor sector tagging (word-boundary, category and
neighbour signals).

The old tagger (``intelligence/actors/graph.py::_infer_sector``) scanned
``title + name + positions`` for *substrings*, so "Craig" matched the AI
keyword ``"ai"``, "Megan" matched ``"gene"`` ... and it ran per actor inside
a web request. This module is the replacement:

* keywords match on word boundaries through one precompiled regex per sector
  (``\\bai\\b`` no longer matches "Craig"); short, ambiguous tokens (``AI``,
  ``LNG``, ``FDA`` ...) are matched case-sensitively as acronyms only;
* a person's *name* is never scanned (a donor called "Gold" is not a metals
  actor) — only organisations' names are;
* the ``"<Sector> — <TICKER>"`` titles the curated institution rows carry
  are read as a direct sector label;
* categories give a sector where they imply one (central banks -> Macro,
  ICIJ rows -> Offshore, PEPs -> Government ...);
* actors that still have no sector can inherit the majority sector of their
  graph neighbours (insiders -> their company), computed offline by
  ``intelligence/actors/universe.py``.

Everything here is pure (no DB). ``tag_sector`` handles one actor;
``tag_sectors_frame`` handles a whole pandas frame (the offline job);
``propagate_sectors`` does the neighbour vote over edge arrays.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from typing import Any

# Sector labels. The first nine are the PWA's priority sectors
# (api/routers/intelligence_actors.py::_DEFAULT_SECTORS).
OTHER = "Other"

# ── Category -> sector (only categories that really imply one) ─────────────
CATEGORY_SECTOR: dict[str, str] = {
    "central_bank": "Macro",
    "swf": "Macro",
    "government": "Government",
    "government_research": "Government",
    "politician": "Government",
    "pep": "Government",
    "lobbying_firm": "Government",
    "fund": "Finance",
    "pension_fund": "Finance",
    "institution": "Finance",
    "dynasty": "Finance",
    "kingmaker": "Finance",
    "billionaire": "Finance",
    "crypto": "Crypto",
    "pharmaceutical_sponsor": "Gene Therapy & Pharma",
    "clinical_sponsor": "Gene Therapy & Pharma",
    "academic_research": "Research",
    "icij_entity": "Offshore",
    "icij_officer": "Offshore",
    "icij_intermediary": "Offshore",
    "icij_other": "Offshore",
    "donor": "Political Money",
    "political_donor": "Political Money",
    "media_owner": "Media & Telecom",
}

# Categories whose rows are people: their *names* are never keyword-scanned.
PERSON_CATEGORIES: frozenset[str] = frozenset({
    "individual", "insider", "pep", "person_of_interest", "politician",
    "donor", "political_donor", "billionaire", "icij_officer", "activist",
    "dynasty", "kingmaker", "media_owner", "sanctioned", "ofac_sanctioned",
})

# Categories that never get a market sector from keywords or neighbours:
# bulk screening / leak rows. Their sector is their category's (or Other).
NO_INFERENCE_CATEGORIES: frozenset[str] = frozenset({
    "icij_entity", "icij_officer", "icij_intermediary", "icij_other",
    "pep", "person_of_interest", "entity_of_interest", "donor",
    "sanctioned", "ofac_sanctioned",
})

# ── "<Sector> — <TICKER>" title prefixes (curated institution rows) ────────
TITLE_SECTOR_PREFIX: dict[str, str] = {
    "technology": "Tech",
    "semiconductors": "Semiconductors",
    "energy": "Energy",
    "utilities": "Energy",
    "materials": "Metals",
    "financial": "Finance",
    "financials": "Finance",
    "healthcare": "Healthcare",
    "health care": "Healthcare",
    "industrials": "Industrials",
    "consumer staples": "Consumer",
    "consumer discretionary": "Consumer",
    "retail": "Consumer",
    "automotive": "Industrials",
    "real estate": "Real Estate",
    "communications": "Media & Telecom",
    "communication services": "Media & Telecom",
}
_TITLE_PREFIX_RE = re.compile(r"^\s*([A-Za-z &]+?)\s+[—–-]\s+\S")

# ── Keyword rules, in priority order ───────────────────────────────────────
# (sector, case-insensitive words/phrases, case-sensitive acronyms)
SECTOR_RULES: list[tuple[str, tuple[str, ...], tuple[str, ...]]] = [
    ("AI", (
        "artificial intelligence", "machine learning", "deep learning",
        "neural network", "neural networks", "openai", "anthropic", "deepmind",
        "large language model", "generative ai",
    ), ("AI", "A.I.", "LLM", "LLMs", "GPU", "GPUs", "AGI")),
    ("Crypto", (
        "crypto", "cryptocurrency", "cryptocurrencies", "bitcoin", "ethereum",
        "blockchain", "binance", "coinbase", "stablecoin", "stablecoins", "web3",
        "digital assets", "digital asset",
    ), ("DeFi", "BTC", "ETH", "NFT")),
    ("Robotics", (
        "robot", "robots", "robotic", "robotics", "boston dynamics",
        "industrial automation", "drone", "drones", "autonomous vehicles",
        "autonomous driving",
    ), ()),
    ("Gene Therapy & Pharma", (
        "gene therapy", "gene editing", "genetic", "genetics", "genomic",
        "genomics", "crispr", "biotech", "biotechnology", "pharma",
        "pharmaceutical", "pharmaceuticals", "clinical trial", "clinical trials",
        "therapeutics", "biopharma", "oncology", "vaccine", "vaccines",
    ), ("FDA", "mRNA", "CRISPR")),
    ("Nuclear", (
        "nuclear", "uranium", "fission", "fusion energy", "reactor", "reactors",
        "enrichment",
    ), ("NRC", "SMR")),
    ("Energy", (
        "energy", "solar", "wind power", "wind farm", "oil", "natural gas",
        "petroleum", "opec", "renewable", "renewables", "battery", "batteries",
        "power grid", "electric utility", "refinery",
        "refining", "lng",
    ), ("LNG", "EIA", "OPEC")),
    ("Metals", (
        "metal", "metals", "gold", "silver", "copper", "lithium", "platinum",
        "palladium", "mining", "miner", "miners", "steel", "aluminum",
        "aluminium", "cobalt", "nickel", "rare earth", "rare earths",
    ), ()),
    ("Semiconductors", (
        "semiconductor", "semiconductors", "chipmaker", "chipmakers",
        "wafer", "wafers", "foundry", "tsmc", "nvidia", "asml", "qualcomm",
        "broadcom", "intel corporation", "advanced micro devices",
    ), ("AMD", "TSMC")),
    ("Tech", (
        "technology", "software", "cloud computing", "saas", "microsoft",
        "apple inc", "alphabet", "google", "amazon", "meta platforms",
        "data center", "data centers", "internet", "cybersecurity",
    ), ("SaaS",)),
]


def _compile_rule(words: Sequence[str], acronyms: Sequence[str]) -> re.Pattern[str]:
    """One regex per sector: case-insensitive words, case-sensitive acronyms."""
    parts: list[str] = []
    if words:
        alt = "|".join(re.escape(w) for w in sorted(words, key=len, reverse=True))
        parts.append(rf"(?i:\b(?:{alt})\b)")
    if acronyms:
        alt = "|".join(re.escape(a) for a in sorted(acronyms, key=len, reverse=True))
        # (?<![\w.]) / (?![\w]) instead of \b so "A.I." works.
        parts.append(rf"(?<![\w.])(?:{alt})(?!\w)")
    return re.compile("|".join(parts))


SECTOR_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (label, _compile_rule(words, acr)) for label, words, acr in SECTOR_RULES
]

# Basis labels stored next to the sector so consumers can weigh it.
BASIS_TITLE = "title_sector"
BASIS_KEYWORD = "keyword"
BASIS_CATEGORY = "category"
BASIS_NEIGHBOR = "neighbor"
BASIS_DEFAULT = "default"


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return " ".join(_as_text(v) for v in value.values())
    if isinstance(value, Iterable):
        return " ".join(_as_text(v) for v in value)
    return str(value)


def sector_from_title_prefix(title: str | None) -> str | None:
    """``"Technology — SAP"`` -> ``"Tech"``; anything else -> None."""
    if not title:
        return None
    m = _TITLE_PREFIX_RE.match(title)
    if not m:
        return None
    return TITLE_SECTOR_PREFIX.get(m.group(1).strip().lower())


def sector_from_keywords(text: str) -> str | None:
    """First sector (priority order) whose keywords occur as whole words."""
    if not text:
        return None
    for label, pattern in SECTOR_PATTERNS:
        if pattern.search(text):
            return label
    return None


def tag_sector(
    category: str | None,
    name: str | None = "",
    title: str | None = "",
    positions: Any = None,
    board_seats: Any = None,
) -> tuple[str, str]:
    """Sector and the basis it came from for one actor.

    Order: title prefix -> keywords (title, positions, board seats, and the
    name for organisations) -> category -> ``("Other", "default")``. Bulk
    screening / leak categories skip keyword inference entirely.
    """
    cat = (category or "").strip()
    if cat in NO_INFERENCE_CATEGORIES:
        return CATEGORY_SECTOR.get(cat, OTHER), (BASIS_CATEGORY if cat in CATEGORY_SECTOR else BASIS_DEFAULT)

    from_title = sector_from_title_prefix(title)
    if from_title:
        return from_title, BASIS_TITLE

    parts = [title or "", _as_text(positions), _as_text(board_seats)]
    if cat not in PERSON_CATEGORIES:
        parts.append(name or "")
    kw = sector_from_keywords(" | ".join(p for p in parts if p))
    if kw:
        return kw, BASIS_KEYWORD

    if cat in CATEGORY_SECTOR:
        return CATEGORY_SECTOR[cat], BASIS_CATEGORY
    return OTHER, BASIS_DEFAULT


def tag_sectors_frame(frame: Any) -> tuple[Any, Any]:
    """Vectorised :func:`tag_sector` over a pandas frame.

    ``frame`` needs ``category``, ``name`` and ``title`` columns; optional
    ``positions_text`` / ``board_text``. Returns ``(sector, basis)`` Series
    aligned to ``frame.index``. Each sector regex runs once over the column
    (in C), not once per actor in Python.
    """
    import pandas as pd

    n = len(frame)
    category = frame["category"].fillna("").astype(str)
    title = frame["title"].fillna("").astype(str)
    name = frame["name"].fillna("").astype(str)
    extra = pd.Series([""] * n, index=frame.index)
    for col in ("positions_text", "board_text"):
        if col in frame.columns:
            extra = extra + " | " + frame[col].fillna("").astype(str)

    sector = pd.Series([None] * n, index=frame.index, dtype=object)
    basis = pd.Series([None] * n, index=frame.index, dtype=object)

    no_infer = category.isin(NO_INFERENCE_CATEGORIES)

    # 1. title prefix
    prefix = title.str.extract(_TITLE_PREFIX_RE, expand=False).fillna("").str.strip().str.lower()
    from_title = prefix.map(TITLE_SECTOR_PREFIX)
    hit = from_title.notna() & ~no_infer
    sector[hit] = from_title[hit]
    basis[hit] = BASIS_TITLE

    # 2. keywords
    is_person = category.isin(PERSON_CATEGORIES)
    scan = title + extra + (" | " + name).where(~is_person, "")
    todo = sector.isna() & ~no_infer & (scan.str.strip(" |") != "")
    for label, pattern in SECTOR_PATTERNS:
        if not todo.any():
            break
        found = scan[todo].str.contains(pattern, regex=True, na=False)
        idx = found[found].index
        sector[idx] = label
        basis[idx] = BASIS_KEYWORD
        todo[idx] = False

    # 3. category
    from_cat = category.map(CATEGORY_SECTOR)
    hit = sector.isna() & from_cat.notna()
    sector[hit] = from_cat[hit]
    basis[hit] = BASIS_CATEGORY

    # 4. default
    rest = sector.isna()
    sector[rest] = OTHER
    basis[rest] = BASIS_DEFAULT
    return sector, basis


def propagate_sectors(
    src: Any,
    dst: Any,
    sector_codes: Any,
    eligible: Any,
    *,
    unknown_code: int,
    min_votes: int = 1,
) -> Any:
    """One round of neighbour majority vote over an undirected edge list.

    ``src``/``dst`` are int node indices, ``sector_codes`` an int array of
    sector codes per node (``unknown_code`` = no confident sector), and
    ``eligible`` a bool array of nodes allowed to inherit. A node takes the
    most common confident sector among its neighbours (ties -> lowest code)
    when it has at least ``min_votes`` such neighbours. Returns a new code
    array; nodes that do not qualify keep their code.
    """
    import numpy as np

    codes = np.asarray(sector_codes).copy()
    src = np.asarray(src)
    dst = np.asarray(dst)
    eligible = np.asarray(eligible, dtype=bool)
    if src.size == 0:
        return codes
    a = np.concatenate([src, dst])
    b = np.concatenate([dst, src])
    voter = codes[b]
    keep = eligible[a] & (codes[a] == unknown_code) & (voter != unknown_code)
    if not keep.any():
        return codes
    a = a[keep]
    voter = voter[keep]
    n_codes = int(max(codes.max(), unknown_code)) + 1
    pair = a.astype(np.int64) * n_codes + voter.astype(np.int64)
    uniq, counts = np.unique(pair, return_counts=True)
    node = uniq // n_codes
    code = uniq % n_codes
    # Sort by node, then count desc, then code asc; take the first per node.
    order = np.lexsort((code, -counts, node))
    node, code, counts = node[order], code[order], counts[order]
    first = np.ones(node.size, dtype=bool)
    first[1:] = node[1:] != node[:-1]
    node, code, counts = node[first], code[first], counts[first]
    ok = counts >= min_votes
    codes[node[ok]] = code[ok]
    return codes
