"""News ticker resolver — extract real ticker symbols from news title+content.

The ingestion pipeline for ``news_articles`` previously stored the 12-char
hash suffix of ``raw_series.series_id`` (``tiingo_news.<hash>``) inside the
``tickers`` array instead of real symbols. This module provides a clean
resolver that can be used by the live ingestor and by a one-shot backfill
script to reconstruct real tickers from article title + description.

Resolution strategy (stops at first non-empty match for each technique,
but combines techniques — i.e. we union the results of all strategies):

1. **Cashtag regex** (``$AAPL``, ``$MSFT``) — highest precision, rarely
   false-positive.
2. **Exchange prefix** (``NYSE: XPOF``, ``NASDAQ:AAPL``) — also high
   precision, used in earnings press releases.
3. **Known ticker word match** against the sector_map universe
   (~1500 symbols) as standalone words, case-sensitive to avoid matching
   common English words like ``FOR``, ``ARE``, ``IT``.
4. **Company name alias match** — walks the sector_map name list plus a
   small top-200 alias table, case-insensitive but whole-word.
5. **Existing tiingo raw_payload tickers** (when available via the backfill
   joiner) — trusted, uppercased, filtered.

All resolved tickers are uppercased, deduplicated, sorted, and passed
through a blacklist to reject noise. The blacklist includes English words
and filler that appear in the SECTOR_MAP as tickers (rare but possible).

Public API:
    ``resolve_tickers(title, description=None, fallback_payload_tickers=None)``
        → ``list[str]``  (uppercase, sorted, deduped, filtered)

Used by:
    * ``ingestion/altdata/news_scraper.py`` (live ingestion)
    * ``scripts/backfill_news_tickers.py`` (one-shot migration)
"""

from __future__ import annotations

import re
from functools import lru_cache

# ── Blacklist (reject cashtag / word matches that look like tickers
# but are English words, common abbreviations, or junk) ─────────────
_TICKER_BLACKLIST: frozenset[str] = frozenset({
    # English words that collide with tickers
    "A", "AN", "ARE", "AS", "AT", "BE", "BY", "DO", "FOR", "GO", "HAS",
    "HAD", "HE", "IF", "IN", "IS", "IT", "NO", "NOT", "OF", "ON", "OR",
    "SO", "THE", "TO", "UP", "WE", "ALL", "AND", "BUT", "CAN", "HER",
    "HIM", "HIS", "HOW", "NEW", "NOW", "OUR", "OUT", "SEE", "SHE", "TWO",
    "WHO", "WHY", "YOU", "ANY", "DAY", "GET", "ITS", "LET", "MAY", "OFF",
    "ONE", "SAY", "TOP", "WAY", "WHO", "YES",
    # Common noise from Tiingo raw payloads
    "NEWS", "LTD", "INC", "CORP", "CEO", "CFO", "COO", "CTO", "GDP", "CPI",
    "IMF", "ECB", "FED", "EPS", "REV", "USA", "USD", "EUR", "JPY", "GBP",
    "CNY", "BTC", "ETH", "LLC", "PLC", "AG", "NV", "SE", "UK", "EU", "US",
    "NYSE", "NASDAQ", "SEC", "DOJ", "FBI", "CIA", "NSA", "OPEC", "G7", "G20",
    "BC", "PR", "TV", "PM", "AM", "ET", "PT", "CT", "MT", "EST", "PST", "GMT",
    "Q1", "Q2", "Q3", "Q4", "FY", "YTD", "YOY", "MOM", "QOQ",
    # Tiingo-specific noise observed in raw_payload
    "NE-WS", "BESS", "OEC", "OESX", "ORN",
})

# ── Top-200 company name aliases (case-insensitive whole-word match).
# SECTOR_MAP has canonical names like "NVIDIA" but news uses "Nvidia Corp",
# "NVIDIA Corporation", etc. This table provides extra forms that would
# otherwise not match. Keep this focused — too many aliases = false
# positives. ──────────────────────────────────────────────────────────
# Ambiguous company names that collide with common English words.
# These names will NEVER resolve to a ticker via the alias or sector_map
# name-match path. They must be matched by cashtag ($AAPL), exchange
# prefix (NYSE: XPOF), or an unambiguous long-form company name
# ("Apollo Global", "Block Inc", etc.)
#
# NOTE: Only listed here if the collision is frequent enough to dominate
# real matches. Proper-noun-capitalised words (Meta, Ford, Disney, Nike)
# ARE safe in English prose because the English word is lowercase — we
# handle those via a case-sensitive check in ``_extract_aliases`` rather
# than a blanket blacklist.
_AMBIGUOUS_COMPANY_NAMES: frozenset[str] = frozenset({
    "block",         # Block Inc (SQ) vs "to block", "block of..."
    "square",        # Square vs "square one"
    "target",        # Target Corp vs "target audience"
    "visa",          # Visa Inc vs "visa approval"
    "apollo",        # Apollo Global vs "Apollo mission"
    "travelers",     # Travelers Companies vs "travelers in airport"
    "fidelity",      # Fidelity vs "fidelity" (the word)
    "new",           # any *NEW ticker vs "new"
    "see",           # Sealed Air (SEE) vs "see"
    "one",           # vs "one"
    "now",           # ServiceNow vs "now" — use "servicenow" alias
    "any",           # vs "any"
    "all",           # Allstate vs "all" — use "allstate" alias
    "key",           # KeyCorp vs "key"
    "peak",          # Healthpeak vs "peak"
    "power",         # vs "power"
    "real",          # Real Brokerage vs "real"
    "good",          # vs "good"
    "hope",          # Hope Bancorp vs "hope"
    "open",          # Opendoor vs "open"
    "match",         # Match Group vs "match"
    "rocket",        # Rocket Companies vs "rocket"
})


_EXTRA_ALIASES: dict[str, str] = {
    # Mega caps — most common aliases seen in headlines
    "apple": "AAPL",
    "apple inc": "AAPL",
    "microsoft": "MSFT",
    "microsoft corp": "MSFT",
    "google": "GOOGL",
    "alphabet": "GOOGL",
    "amazon": "AMZN",
    "meta": "META",
    "facebook": "META",
    "tesla": "TSLA",
    "nvidia": "NVDA",
    "berkshire": "BRK.B",
    "berkshire hathaway": "BRK.B",
    "jpmorgan": "JPM",
    "jp morgan": "JPM",
    "goldman sachs": "GS",
    "goldman": "GS",
    "morgan stanley": "MS",
    "bank of america": "BAC",
    "wells fargo": "WFC",
    "citigroup": "C",
    "exxon": "XOM",
    "exxonmobil": "XOM",
    "chevron": "CVX",
    "walmart": "WMT",
    "costco": "COST",
    "home depot": "HD",
    # "target": "TGT",  # removed — collides with "target audience"
    "target corporation": "TGT",
    "coca-cola": "KO",
    "coca cola": "KO",
    "pepsi": "PEP",
    "pepsico": "PEP",
    "pfizer": "PFE",
    "merck": "MRK",
    "eli lilly": "LLY",
    "johnson & johnson": "JNJ",
    "johnson and johnson": "JNJ",
    "unitedhealth": "UNH",
    "cvs": "CVS",
    "disney": "DIS",
    "netflix": "NFLX",
    "comcast": "CMCSA",
    "verizon": "VZ",
    "at&t": "T",
    "intel": "INTC",
    "amd": "AMD",
    "broadcom": "AVGO",
    "qualcomm": "QCOM",
    "tsmc": "TSM",
    "taiwan semiconductor": "TSM",
    "asml": "ASML",
    "cisco": "CSCO",
    "oracle": "ORCL",
    "salesforce": "CRM",
    "adobe": "ADBE",
    "paypal": "PYPL",
    # "visa": "V",  # removed — collides with "visa approval"
    "visa inc": "V",
    "mastercard": "MA",
    "boeing": "BA",
    "lockheed": "LMT",
    "lockheed martin": "LMT",
    "raytheon": "RTX",
    "general electric": "GE",
    "caterpillar": "CAT",
    "deere": "DE",
    "ford": "F",
    "general motors": "GM",
    "uber": "UBER",
    "lyft": "LYFT",
    "airbnb": "ABNB",
    "palantir": "PLTR",
    "coinbase": "COIN",
    "robinhood": "HOOD",
    "mcdonald's": "MCD",
    "mcdonalds": "MCD",
    "starbucks": "SBUX",
    "nike": "NKE",
    "procter & gamble": "PG",
    "colgate": "CL",
    "3m": "MMM",
    "honeywell": "HON",
    "ibm": "IBM",
    "hp": "HPQ",
    "dell": "DELL",
    "micron": "MU",
    "applied materials": "AMAT",
    "lam research": "LRCX",
    "kla": "KLAC",
    "texas instruments": "TXN",
    "analog devices": "ADI",
    "servicenow": "NOW",
    "snowflake": "SNOW",
    "datadog": "DDOG",
    "crowdstrike": "CRWD",
    "zscaler": "ZS",
    "mongodb": "MDB",
    "workday": "WDAY",
    # "square": "SQ",  # removed — too ambiguous
    # "block": "SQ",   # removed — "block" is a common English word
    "block inc": "SQ",
    "square inc": "SQ",
    "shopify": "SHOP",
    "spotify": "SPOT",
    "zoom": "ZM",
    "docusign": "DOCU",
    "atlassian": "TEAM",
    "okta": "OKTA",
    "twilio": "TWLO",
    "cloudflare": "NET",
    "fastly": "FSLY",
    "roblox": "RBLX",
    "unity": "U",
    "roku": "ROKU",
    "pinterest": "PINS",
    "snap": "SNAP",
    "snapchat": "SNAP",
    "chipotle": "CMG",
    "domino's": "DPZ",
    "dominos": "DPZ",
    "kroger": "KR",
    "walgreens": "WBA",
    "rite aid": "RAD",
    "altria": "MO",
    "philip morris": "PM",
    "anheuser-busch": "BUD",
    "constellation brands": "STZ",
    "monster beverage": "MNST",
    "kraft heinz": "KHC",
    "mondelez": "MDLZ",
    "general mills": "GIS",
    "kellogg": "K",
    "tyson foods": "TSN",
    "hershey": "HSY",
    "estee lauder": "EL",
    "l'oreal": "LRLCY",
    "loreal": "LRLCY",
    "unilever": "UL",
    "diageo": "DEO",
    "lvmh": "LVMUY",
    "kering": "PPRUY",
    "richemont": "CFRUY",
    "blackrock": "BLK",
    "vanguard": "VFIAX",  # not a ticker, but nearest match
    "state street": "STT",
    "blackstone": "BX",
    "kkr": "KKR",
    "carlyle": "CG",
    # "apollo": "APO",  # removed — "Apollo mission" false positive
    "apollo global": "APO",
    "apollo global management": "APO",
    "schwab": "SCHW",
    "charles schwab": "SCHW",
    "fidelity": "FNF",  # loose — FNF is First American; true Fidelity is private
    "interactive brokers": "IBKR",
    "nasdaq": "NDAQ",  # the company, not the index
    "cboe": "CBOE",
    "cme": "CME",
    "ice": "ICE",
    # Crypto-adjacent
    "microstrategy": "MSTR",
    "marathon digital": "MARA",
    "riot platforms": "RIOT",
    "cleanspark": "CLSK",
    "hut 8": "HUT",
}


# ── Regex patterns ──────────────────────────────────────────────────
_CASHTAG_RE = re.compile(r"\$([A-Z]{1,6}(?:\.[A-Z])?)\b")
_EXCHANGE_RE = re.compile(
    r"(?:NYSE|NASDAQ|AMEX|LSE|TSX|HKEX|SEHK|SZSE|SSE)\s*:\s*([A-Z]{1,6}(?:\.[A-Z])?)",
    re.IGNORECASE,
)
_WORD_BOUNDARY = re.compile(r"[A-Za-z0-9.&'\-]+")


@lru_cache(maxsize=1)
def _load_sector_universe() -> tuple[frozenset[str], dict[str, str]]:
    """Load the (tickers, name→ticker) pair from SECTOR_MAP.

    Cached so we load once per process. Returns:
        (uppercase_tickers, name_lower_to_ticker)
    """
    try:
        from analysis.sector_map import SECTOR_MAP
    except Exception:
        return frozenset(), {}

    tickers: set[str] = set()
    name_map: dict[str, str] = {}

    def walk(obj):
        if isinstance(obj, dict):
            if "actors" in obj and isinstance(obj["actors"], list):
                for a in obj["actors"]:
                    if not isinstance(a, dict):
                        continue
                    tk = a.get("ticker")
                    nm = a.get("name")
                    if tk and isinstance(tk, str):
                        tk_u = tk.upper().strip()
                        if tk_u and tk_u not in _TICKER_BLACKLIST:
                            tickers.add(tk_u)
                            if nm and isinstance(nm, str):
                                name_map[nm.lower().strip()] = tk_u
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for x in obj:
                walk(x)

    walk(SECTOR_MAP)
    return frozenset(tickers), name_map


def _filter(raw: set[str], *, skip_blacklist: bool = False) -> list[str]:
    """Upper-case, strip, blacklist, sort, dedupe.

    Parameters:
        raw: The collected candidate tickers (mixed case, possibly
            junk).
        skip_blacklist: If True, do not apply the English-word
            blacklist. Used for upstream-curated payload tickers where
            ``ALL`` (Allstate) and ``NEW`` (legitimate symbols) should
            survive.
    """
    out: set[str] = set()
    for t in raw:
        if not t or not isinstance(t, str):
            continue
        u = t.upper().strip()
        # Basic sanity — ticker must be 1-6 alphanums with optional .X suffix
        if not re.fullmatch(r"[A-Z]{1,6}(?:\.[A-Z])?", u):
            continue
        if not skip_blacklist and u in _TICKER_BLACKLIST:
            continue
        out.add(u)
    return sorted(out)


def _extract_cashtags(text: str) -> set[str]:
    return {m.group(1) for m in _CASHTAG_RE.finditer(text)}


def _extract_exchange_tagged(text: str) -> set[str]:
    return {m.group(1).upper() for m in _EXCHANGE_RE.finditer(text)}


def _extract_known_tickers(text: str, universe: frozenset[str]) -> set[str]:
    """Match standalone uppercase ticker words (case-sensitive, 3+ chars).

    2-char tickers are skipped because they collide with English ("IT",
    "NO", "OR", "IS"). 1-char tickers (T, F, V, K) are also skipped.
    """
    found: set[str] = set()
    # Use case-sensitive scan for all-caps words of length 3-5
    for match in re.finditer(r"\b[A-Z]{3,5}\b", text):
        word = match.group(0)
        if word in universe and word not in _TICKER_BLACKLIST:
            found.add(word)
    return found


def _extract_by_company_name(
    text: str,
    name_map: dict[str, str],
) -> set[str]:
    """Match company names in the lower-cased text.

    Uses a simple substring check with word boundaries — fast and good
    enough for headlines. Prefers longer names first to avoid short
    matches winning over long ones (e.g. "Apple" vs "Apple Inc").

    Single-word names that collide with common English words are
    skipped via ``_AMBIGUOUS_COMPANY_NAMES`` — those must be matched
    via cashtag, exchange prefix, or long-form alias instead.
    """
    found: set[str] = set()
    lower = text.lower()
    # Sort names by length descending — longer first
    names = sorted(name_map.keys(), key=len, reverse=True)
    for name in names:
        if len(name) < 4:
            continue
        if name in _AMBIGUOUS_COMPANY_NAMES:
            continue
        # Whole-word match — name must be surrounded by non-word chars
        pattern = r"(?<![a-z0-9])" + re.escape(name) + r"(?![a-z0-9])"
        if re.search(pattern, lower):
            found.add(name_map[name])
    return found


def _extract_aliases(text: str) -> set[str]:
    """Match company names from the top-200 alias table.

    Single-word English-word collisions are filtered via
    ``_AMBIGUOUS_COMPANY_NAMES``.
    """
    found: set[str] = set()
    lower = text.lower()
    names = sorted(_EXTRA_ALIASES.keys(), key=len, reverse=True)
    for name in names:
        if name in _AMBIGUOUS_COMPANY_NAMES:
            continue
        pattern = r"(?<![a-z0-9])" + re.escape(name) + r"(?![a-z0-9])"
        if re.search(pattern, lower):
            found.add(_EXTRA_ALIASES[name])
    return found


def resolve_tickers(
    title: str | None,
    description: str | None = None,
    fallback_payload_tickers: list[str] | None = None,
) -> list[str]:
    """Resolve real ticker symbols from article text.

    Parameters:
        title: Article headline.
        description: Optional longer summary / body text.
        fallback_payload_tickers: Optional list of tickers that were
            already associated with the article by the upstream data
            provider (e.g. Tiingo's ``tickers`` field). These are
            trusted but uppercased + filtered through the blacklist.

    Returns:
        Sorted, deduplicated, uppercase list of ticker symbols. May be
        empty if no ticker was found.
    """
    if not title and not description and not fallback_payload_tickers:
        return []

    combined = f"{title or ''}\n{description or ''}"
    universe, name_map = _load_sector_universe()

    # Upstream-curated tickers bypass the English-word blacklist because
    # Tiingo writes legitimate symbols like "all" (Allstate) and "new"
    # (e.g. NEW:NYSE). Text-extracted tickers still go through the full
    # blacklist because their provenance is heuristic.
    trusted: set[str] = set()
    if fallback_payload_tickers:
        for t in fallback_payload_tickers:
            if isinstance(t, str):
                trusted.add(t.upper().strip())

    heuristic: set[str] = set()
    # Cashtags (e.g. $AAPL)
    heuristic |= _extract_cashtags(combined)
    # Exchange prefix (e.g. NYSE: XPOF)
    heuristic |= _extract_exchange_tagged(combined)
    # Known ticker words
    heuristic |= _extract_known_tickers(combined, universe)
    # Company name — sector_map
    heuristic |= _extract_by_company_name(combined, name_map)
    # Company name — top-200 alias table
    heuristic |= _extract_aliases(combined)

    merged: set[str] = set(_filter(trusted, skip_blacklist=True))
    merged.update(_filter(heuristic, skip_blacklist=False))
    return sorted(merged)


__all__ = ["resolve_tickers"]
