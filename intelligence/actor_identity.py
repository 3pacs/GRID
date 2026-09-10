"""Canonical actor identity for SEC filer names.

SEC EDGAR full-text search returns filer display names shaped like
``"BlackRock Inc.  (BLK)  (CIK 0001364742)"`` or
``"CENOVUS ENERGY INC.  (CVE, CVE-PB)  (CIK 0001071297)"``. The connection
spider (``intelligence/spider/sources/sec_crossref.py`` ->
``intelligence/spider/discovery.py``) turned every one of those strings into
its own ``corporation_<slug>`` node, so the curated Louvain run of 2026-09-10
grew three communities (255 / 245 / 197 nodes) made only of filer names wired
to each other, while the ``CVE Corp`` / ``BLK Corp`` ticker actors that
represent the same companies sat in the market clusters.

This module resolves such a display name to the actor that already stands for
the same entity:

* ``(TICKER)`` in the display name  ->  ``corp_<TICKER>`` (named ``<TICKER> Corp``)
* CIK -> ticker via the SEC ``company_tickers.json`` cache already loaded once
  per process by :mod:`grid.signals.sponsor_resolver`  ->  ``corp_<TICKER>``
* person filers (``SCRIVNER DOUGLAS G``) -> the insider actor whose name
  normalises to the same key, when one exists.

Nothing is invented. :func:`propose_canonical` only reads the string;
:func:`resolve_canonical_actor_id` returns an id only when the caller's
``exists`` predicate confirms it, so a filer with no counterpart in the graph
keeps its own node.

Consumers:
  * ``intelligence/spider/discovery.py`` — resolve before creating a node.
  * ``scripts/fold_actor_aliases.py`` — fold the nodes already in the graph.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Iterable

from loguru import logger as log

# ── Canonical ticker-actor shape ──────────────────────────────────────────
# Matches scripts/enrich_connections.py, which is what created the "CVE Corp"
# nodes the filer names should fold into.
TICKER_ACTOR_PREFIX = "corp_"


def ticker_actor_id(ticker: str) -> str:
    """``"cve"`` -> ``"corp_CVE"``."""
    return f"{TICKER_ACTOR_PREFIX}{ticker.strip().upper()}"


def ticker_actor_name(ticker: str) -> str:
    """``"cve"`` -> ``"CVE Corp"``."""
    return f"{ticker.strip().upper()} Corp"


# ── Display-name parsing ──────────────────────────────────────────────────

_PAREN_RE = re.compile(r"\(([^()]*)\)")
_CIK_RE = re.compile(r"^CIK\s*0*([0-9]{1,10})$", re.IGNORECASE)
# Tickers as SEC writes them: BLK, CVE-PB, BRK.B. Deliberately stricter than a
# bare [A-Z]+ so words like "THE" or "LP" in a parenthetical are not tickers.
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9]{0,5}(?:[.\-][A-Z0-9]{1,4})?$")
# EDGAR conformed company names carry their own parentheticals — "COCA COLA CO
# (THE)  (KO)  (CIK 0000021344)" — and those words are ticker-shaped. Without
# this the parser would read "THE" as the ticker and miss KO entirely. Kept
# deliberately tight: only words EDGAR uses this way, never a real ticker.
_NON_TICKER_WORDS = frozenset({"THE", "NEW", "OLD", "FORMERLY", "DBA", "ET", "AL"})
_WORD_RE = re.compile(r"[A-Za-z0-9&']+")

# Tokens that mark a filer as an organisation rather than a person. Only used
# to rule *out* the person branch, so over-inclusion is safe.
_COMPANY_TOKENS = frozenset({
    "inc", "incorporated", "corp", "corporation", "co", "company", "cos",
    "llc", "lc", "lp", "llp", "plc", "ltd", "limited", "trust", "trusts",
    "fund", "funds", "etf", "reit", "bank", "bancorp", "bancshares",
    "group", "holding", "holdings", "partners", "partnership", "capital",
    "management", "mgmt", "advisors", "advisers", "advisory", "asset",
    "assets", "investment", "investments", "investors", "securities",
    "financial", "finance", "insurance", "associates", "ventures",
    "global", "international", "worldwide", "national", "american",
    "systems", "technologies", "technology", "industries", "industrial",
    "energy", "resources", "properties", "realty", "pharmaceuticals",
    "pharma", "therapeutics", "biosciences", "laboratories", "labs",
    "sa", "nv", "ag", "se", "gmbh", "spa", "ab", "as", "oy", "pte", "kk",
    "sarl", "sas", "srl", "bv", "aps", "plc's", "the", "and", "of",
})

# Personal-name suffixes dropped before keying (SEC writes "SMITH JOHN A JR").
_NAME_SUFFIXES = frozenset({
    "jr", "sr", "ii", "iii", "iv", "v", "md", "phd", "esq", "cpa", "dr",
})


@dataclass(frozen=True)
class FilerIdentity:
    """What a SEC filer display name says about itself.

    Attributes:
        raw: The display name exactly as SEC emitted it.
        base_name: The name with every parenthetical group removed.
        tickers: Tickers listed in the name, primary listing first.
        cik: Zero-padded 10-digit CIK, or None when the name carries none.
        is_person: True when the name looks like a natural person.
    """

    raw: str
    base_name: str
    tickers: tuple[str, ...]
    cik: str | None
    is_person: bool


def parse_filer_display_name(raw: str | None) -> FilerIdentity:
    """Split a SEC display name into base name, tickers and CIK.

    ``"BlackRock Inc.  (BLK)  (CIK 0001364742)"`` ->
    tickers ``("BLK",)``, cik ``"0001364742"``, base ``"BlackRock Inc."``.
    Never raises: junk in gives an identity with no tickers and no CIK.
    """
    text = (raw or "").strip()
    tickers: list[str] = []
    cik: str | None = None

    for group in _PAREN_RE.findall(text):
        group = group.strip()
        if not group:
            continue
        cik_match = _CIK_RE.match(group)
        if cik_match:
            cik = cik_match.group(1).zfill(10)
            continue
        # A ticker group is a comma-separated list of ticker-shaped tokens.
        parts = [p.strip().upper() for p in group.split(",") if p.strip()]
        if not parts or not all(_TICKER_RE.match(p) for p in parts):
            continue
        if all(p in _NON_TICKER_WORDS for p in parts):
            continue        # "(THE)" from the conformed name, not a ticker
        tickers.extend(
            p for p in parts if p not in tickers and p not in _NON_TICKER_WORDS
        )

    base_name = _PAREN_RE.sub(" ", text)
    base_name = re.sub(r"\s+", " ", base_name).strip()

    return FilerIdentity(
        raw=text,
        base_name=base_name,
        tickers=tuple(tickers),
        cik=cik,
        is_person=_looks_like_person(base_name, bool(tickers)),
    )


def _looks_like_person(base_name: str, has_ticker: bool) -> bool:
    """True when a filer base name reads as a natural person.

    A ticker or any organisational token settles it as an organisation; what
    is left must be two to five alphabetic name tokens ("SCRIVNER DOUGLAS G").
    """
    if has_ticker or not base_name:
        return False
    tokens = _WORD_RE.findall(base_name.lower())
    if not tokens:
        return False
    if any(tok in _COMPANY_TOKENS for tok in tokens):
        return False
    if any(not tok.replace("'", "").isalpha() for tok in tokens):
        return False
    meaningful = [t for t in tokens if t not in _NAME_SUFFIXES]
    return 2 <= len(meaningful) <= 5


def person_name_key(name: str | None) -> str:
    """Order-independent key for a personal name.

    ``"SCRIVNER DOUGLAS G"``, ``"Douglas G. Scrivner"`` and
    ``"Scrivner, Douglas"`` all key to ``"douglas scrivner"``: SEC writes
    surname-first, insider feeds do not, so the key sorts the tokens and drops
    middle initials and suffixes.
    """
    tokens = _WORD_RE.findall((name or "").lower())
    kept = [
        tok for tok in tokens
        if len(tok) > 1 and tok not in _NAME_SUFFIXES
    ]
    return " ".join(sorted(kept))


# ── Resolution ────────────────────────────────────────────────────────────

RULE_TICKER_IN_NAME = "ticker_in_name"
RULE_CIK_TO_TICKER = "cik_to_ticker"
RULE_PERSON_NAME = "person_name"


@dataclass(frozen=True)
class CanonicalProposal:
    """A candidate canonical identity for a filer name.

    Exactly one of ``actor_id`` / ``person_key`` is set: the ticker rules name
    the actor outright, the person rule can only offer a key that the caller
    must look up among the insider actors it knows about.
    """

    rule: str
    actor_id: str | None = None
    person_key: str | None = None


def propose_canonical(
    display_name: str | None,
    *,
    cik_hint: str | None = None,
) -> CanonicalProposal | None:
    """Propose the canonical identity behind a filer display name.

    Pure apart from the SEC ``company_tickers.json`` cache the CIK rule reads.
    Returns None when no rule fires (``iSHARES TRUST  (CIK 0001100663)`` — a
    fund complex with no listed equity — stays itself).

    Parameters:
        display_name: The filer name as stored on the actor row.
        cik_hint: CIK from elsewhere on the row (``metadata->>'cik'`` on the
            ``inst_13f_*`` actors), used when the name carries none.
    """
    identity = parse_filer_display_name(display_name)

    if identity.tickers:
        return CanonicalProposal(
            rule=RULE_TICKER_IN_NAME,
            actor_id=ticker_actor_id(identity.tickers[0]),
        )

    cik = identity.cik or _padded_cik(cik_hint)
    if cik:
        ticker = _sec_ticker_for_cik(cik)
        if ticker:
            return CanonicalProposal(
                rule=RULE_CIK_TO_TICKER,
                actor_id=ticker_actor_id(ticker),
            )

    if identity.is_person:
        key = person_name_key(identity.base_name)
        if key:
            return CanonicalProposal(rule=RULE_PERSON_NAME, person_key=key)

    return None


def build_person_index(rows: Iterable[tuple[str, str]]) -> dict[str, str]:
    """``person_name_key`` -> actor id, over ``(actor_id, name)`` pairs.

    A key claimed by more than one actor is dropped: for the person-filer rule
    an ambiguous match is worse than leaving the filer node where it is.
    """
    index: dict[str, str] = {}
    ambiguous: set[str] = set()
    for actor_id, name in rows:
        key = person_name_key(name)
        if not key:
            continue
        existing = index.get(key)
        if existing is None:
            index[key] = actor_id
        elif existing != actor_id:
            ambiguous.add(key)
    for key in ambiguous:
        index.pop(key, None)
    if ambiguous:
        log.debug("actor_identity: dropped {n} ambiguous person keys", n=len(ambiguous))
    return index


def _padded_cik(cik: str | int | None) -> str | None:
    digits = str(cik).strip() if cik is not None else ""
    return digits.zfill(10) if digits.isdigit() else None


def _sec_ticker_for_cik(cik: str) -> str | None:
    """CIK -> ticker via the shared SEC cache; None when it is unavailable."""
    try:
        from grid.signals.sponsor_resolver import sec_ticker_for_cik
    except Exception as exc:  # noqa: BLE001 - optional dependency at import time
        log.warning("actor_identity: SEC ticker map unavailable: {e}", e=str(exc))
        return None
    return sec_ticker_for_cik(cik)


def resolve_canonical_actor_id(
    display_name: str | None,
    *,
    exists: Callable[[str], bool],
    person_lookup: Callable[[str], str | None] | None = None,
    cik_hint: str | None = None,
) -> tuple[str, str] | None:
    """``(canonical_actor_id, rule)`` for a filer name, or None.

    Only ever returns an id that ``exists`` confirms — this folds filers into
    actors the graph already has and never conjures a new canonical node.

    Parameters:
        display_name: The filer name to resolve.
        exists: Predicate telling whether an actor id is present.
        person_lookup: Maps a :func:`person_name_key` to an existing insider
            actor id. When None the person rule is skipped.
        cik_hint: CIK from elsewhere on the row, for names without one.
    """
    proposal = propose_canonical(display_name, cik_hint=cik_hint)
    if proposal is None:
        return None

    if proposal.actor_id is not None:
        return (proposal.actor_id, proposal.rule) if exists(proposal.actor_id) else None

    if proposal.person_key is not None and person_lookup is not None:
        actor_id = person_lookup(proposal.person_key)
        if actor_id and exists(actor_id):
            return actor_id, proposal.rule

    return None


# ── actors.merged_into ────────────────────────────────────────────────────
#
# migrations/0061_actors_merged_into.sql is the contract for this column. The
# DDL is mirrored here — idempotently — so a tree that has not run the
# migration degrades to a self-created column rather than a failed job. That
# is the convention documented in migrations/0060_sponsor_ticker_map.sql.

MERGED_INTO_DDL = "ALTER TABLE actors ADD COLUMN IF NOT EXISTS merged_into TEXT"
MERGED_INTO_INDEX_DDL = (
    "CREATE INDEX IF NOT EXISTS idx_actors_merged_into ON actors (merged_into) "
    "WHERE merged_into IS NOT NULL"
)


def ensure_merged_into_column() -> None:
    """Idempotently ensure ``actors.merged_into`` and its partial index exist."""
    from db import execute_sql

    execute_sql(MERGED_INTO_DDL)
    execute_sql(MERGED_INTO_INDEX_DDL)
