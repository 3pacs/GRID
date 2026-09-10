"""
grid/signals/sponsor_resolver.py

Layered, cached, validated clinical-trial sponsor -> ticker resolution.

Used by BOTH ``grid.ingestors.trial_ingestor`` (catalyst_calendar rows) and
``grid.signals.trial_signal`` (trial_signals rows) so the two never disagree
about which sponsor maps to which listed equity.

Resolution layers, in order (first decisive answer wins):

1. **Hard reject non-investable sponsors.** CT.gov v2 lead-sponsor ``class``
   other than ``INDUSTRY`` (OTHER / NIH / FED / NETWORK / OTHER_GOV / INDIV),
   or a name matching university / hospital / institute / NIH / NCI /
   foundation / medical-center patterns -> ``ticker=None, reason='non_industry'``.
2. **Persistent cache** ``sponsor_ticker_map`` (migration 0060; also created
   at runtime by :func:`ensure_sponsor_map_table`). Every resolution —
   including negatives with their reason — is written here so nothing is
   recomputed. Negative *unresolved* rows expire after ``NEGATIVE_TTL_DAYS``
   so a sponsor that lists later gets another chance; ``non_industry`` and
   positive rows do not expire.
3. **SEC ``company_tickers.json``** exact + normalized match (the fuzzy
   matcher that used to live in ``trial_signal._resolve_ticker_sec``; that
   name is kept there as a thin alias).
4. **GRID's own name maps**: ``intelligence.news_ticker_resolver
   ._load_sector_universe()`` (whole-name, case-insensitive) and
   ``company_profiles.name``.
5. **Local LLM last resort, INDUSTRY sponsors only** — ``llm.router
   .get_llm(Tier.LOCAL)``. The answer is accepted only when it is a ticker
   present in the SEC set (``source='llm_local'``, ``confidence=0.6``).
   Paid tiers are never called.

Every layer degrades to "no answer" with a warning on network / DB / LLM
failure; the resolver itself never raises.

Public API
----------
``resolve_sponsor(engine, sponsor_name, sponsor_class=None) -> ResolvedSponsor``
``resolve_many(engine, sponsors) -> dict[str, ResolvedSponsor]``
``normalize_sponsor_name(name) -> str``
``is_non_industry_name(name) -> bool``
``resolve_ticker_sec(name) -> str | None`` (SEC-only fuzzy layer)
``sec_ticker_set() -> frozenset[str]``, ``sec_cik_for_ticker(ticker) -> str | None``
``ensure_sponsor_map_table(engine)``, ``clear_caches()``
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

import requests
from loguru import logger as log
from sqlalchemy import text

# ── Config ────────────────────────────────────────────────────────────────────

SEC_COMPANY_TICKERS = "https://www.sec.gov/files/company_tickers.json"
SEC_UA = "GRID Research grid@stepdad.finance"
SEC_TIMEOUT_S = 15

NEGATIVE_TTL_DAYS = 30           # unresolved (not non_industry) rows are retried after this
LLM_CONFIDENCE = 0.6
LLM_MAX_TOKENS = 16
LLM_PROMPT = (
    "US-listed parent company ticker for the clinical-trial sponsor '{name}'. "
    "Answer with the ticker only, or NONE."
)

# Sponsor classes CT.gov v2 emits for leadSponsor.class
INDUSTRY_CLASSES = frozenset({"INDUSTRY"})

# Names that are never investable equities, regardless of class.
_NON_INDUSTRY_RE = re.compile(
    r"(?:\b|^)(?:"
    r"universit(?:y|ies|at|ät|é|e|a|aet)|universidad(?:e)?|univ\.?|college|"
    r"hospital(?:s|es)?|h[oô]pital|hospices?|clinic(?:s)?|klinik(?:um)?|"
    r"institut(?:e|es|o|ion|ions)?|"
    r"nih|national institutes? of health|nci|national cancer institute|"
    r"national (?:heart|eye|human|center|centre)|"
    r"foundation|fondation|fundaci[oó]n|stiftung|trust\b|charit(?:y|able)|"
    r"medical (?:center|centre|school|college|university|research council)|"
    r"health (?:system|authority|service|network|board)|nhs|"
    r"cancer (?:center|centre|research|institute|society)|"
    r"m\.?\s?d\.? anderson|mayo clinic|memorial sloan|cleveland clinic|"
    r"school of medicine|faculty of medicine|academ(?:y|ic|ia)|"
    r"ministry|ministerio|department of (?:veterans|health|defense|defence)|"
    r"veterans affairs|government|county|municipal|"
    r"society|association|consortium|cooperative group|research (?:council|group|network)|"
    r"children'?s|"
    r"assistance publique|hospices civils|"
    r"centre hospitalier|centro hospitalar|azienda ospedaliera|"
    r"ospedale|ziekenhuis|krankenhaus|sjukhus|sykehus"
    r")(?:\b|$)",
    re.IGNORECASE,
)

# Legal / corporate suffixes stripped by the normaliser (longest first).
_SUFFIXES: tuple[str, ...] = (
    "incorporated", "corporation", "limited", "company", "holdings",
    "inc", "ltd", "llc", "plc", "corp", "co", "sa", "s a", "ag", "se", "nv", "n v",
    "gmbh", "pty", "srl", "bv", "b v", "kk", "k k", "ab", "as", "oy", "spa", "s p a",
    "lp", "l p", "llp", "pte", "sas", "sarl",
)
_SUFFIX_RE = re.compile(
    r"(?:\s+(?:" + "|".join(re.escape(s) for s in sorted(_SUFFIXES, key=len, reverse=True)) + r"))+$"
)
_PUNCT_RE = re.compile(r"[.,;:()\[\]'\"&/\\\-–—+*]+")
_AND_RE = re.compile(r"(?:^|\s)and(?=\s|$)")
_WS_RE = re.compile(r"\s+")
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,7}$")


# ── Result type ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ResolvedSponsor:
    """Outcome of one sponsor lookup.

    ``ticker`` is None when the sponsor is not investable or could not be
    mapped; ``reason`` then says why (``non_industry``, ``unresolved``,
    ``llm_none``, ``llm_not_in_sec`` ...). ``source`` names the layer that
    produced the answer (``cache:`` prefix when served from
    ``sponsor_ticker_map``).
    """

    ticker: Optional[str]
    source: str
    confidence: float
    reason: Optional[str] = None

    @property
    def resolved(self) -> bool:
        return self.ticker is not None


# ── Normalisation ─────────────────────────────────────────────────────────────


def normalize_sponsor_name(name: str | None) -> str:
    """Lower-case, strip punctuation and legal suffixes, collapse whitespace.

    ``"Hoffmann-La Roche, Ltd."`` -> ``"hoffmann la roche"``;
    ``"Moderna, Inc."`` -> ``"moderna"``. Pure function; used as the cache key.
    """
    if not name:
        return ""
    s = str(name).strip().lower()
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    # "Eli Lilly and Company" / "ELI LILLY & Co" -> same key: drop connective "and".
    s = _AND_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    # Strip suffixes repeatedly ("Acme Pharma Holdings Inc" -> "acme pharma").
    prev = None
    while prev != s:
        prev = s
        s = _SUFFIX_RE.sub("", s).strip()
    return s


def is_non_industry_name(name: str | None) -> bool:
    """True when the sponsor name matches a university / hospital / NIH / foundation pattern."""
    if not name:
        return True
    return bool(_NON_INDUSTRY_RE.search(str(name)))


def is_industry_class(sponsor_class: str | None) -> bool | None:
    """``True``/``False`` for a known CT.gov class, ``None`` when the class is missing."""
    if not sponsor_class:
        return None
    return str(sponsor_class).strip().upper() in INDUSTRY_CLASSES


# ── SEC company_tickers.json (loaded once per process) ────────────────────────

_SEC_LOADED = False
_SEC_NAME_TO_TICKER: dict[str, str] = {}      # raw lower-cased SEC title -> ticker
_SEC_NORM_TO_TICKER: dict[str, str] = {}      # normalised SEC title -> ticker
_SEC_TICKERS: set[str] = set()
_SEC_TICKER_TO_CIK: dict[str, str] = {}


def _load_sec_tickers() -> None:
    """Download SEC company_tickers.json once and build the name/ticker/CIK maps."""
    global _SEC_LOADED
    if _SEC_LOADED:
        return
    _SEC_LOADED = True  # even on failure — do not hammer sec.gov every call
    try:
        resp = requests.get(SEC_COMPANY_TICKERS, headers={"User-Agent": SEC_UA}, timeout=SEC_TIMEOUT_S)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:  # noqa: BLE001
        log.warning("sponsor_resolver: SEC company_tickers.json unavailable: {e}", e=str(exc))
        return
    _ingest_sec_payload(data)
    log.info("sponsor_resolver: loaded {n} SEC tickers", n=len(_SEC_TICKERS))


def _ingest_sec_payload(data: Any) -> None:
    """Populate the SEC maps from a decoded company_tickers.json payload (test seam)."""
    entries = data.values() if isinstance(data, dict) else (data or [])
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        title = str(entry.get("title") or "").strip().lower()
        ticker = str(entry.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        _SEC_TICKERS.add(ticker)
        cik = entry.get("cik_str")
        if cik is not None and str(cik).strip():
            _SEC_TICKER_TO_CIK.setdefault(ticker, str(cik).strip().zfill(10))
        if title:
            # Prefer the common share class when several classes share a title
            # (the file lists the primary listing first).
            _SEC_NAME_TO_TICKER.setdefault(title, ticker)
            norm = normalize_sponsor_name(title)
            if norm:
                _SEC_NORM_TO_TICKER.setdefault(norm, ticker)


def sec_ticker_set() -> frozenset[str]:
    """All tickers in SEC company_tickers.json (empty when the download failed)."""
    _load_sec_tickers()
    return frozenset(_SEC_TICKERS)


def sec_cik_for_ticker(ticker: str | None) -> str | None:
    """10-digit zero-padded CIK for a ticker, or None."""
    if not ticker:
        return None
    _load_sec_tickers()
    return _SEC_TICKER_TO_CIK.get(str(ticker).strip().upper())


_PHARMA_HINTS = ("pharma", "thera", "bio", "onco", "medic", "genetic", "genom", "immun", "health", "life sciences")


def _resolve_sec_layer(sponsor_name: str) -> tuple[str | None, str, float]:
    """SEC layer: (ticker, source, confidence). ``source`` is 'sec_none' on miss."""
    _load_sec_tickers()
    if not _SEC_NORM_TO_TICKER:
        return None, "sec_unavailable", 0.0

    raw = str(sponsor_name).strip().lower()
    if raw in _SEC_NAME_TO_TICKER:
        return _SEC_NAME_TO_TICKER[raw], "sec_exact", 0.95

    norm = normalize_sponsor_name(sponsor_name)
    if not norm:
        return None, "sec_none", 0.0
    if norm in _SEC_NORM_TO_TICKER:
        return _SEC_NORM_TO_TICKER[norm], "sec_normalized", 0.9

    for variant in (
        f"{norm} inc", f"{norm} corp", f"{norm} pharmaceuticals", f"{norm} pharmaceutical",
        f"{norm} therapeutics", f"{norm} biosciences", f"{norm} biotherapeutics",
        f"{norm} holdings", f"{norm} group",
    ):
        if variant in _SEC_NORM_TO_TICKER:
            return _SEC_NORM_TO_TICKER[variant], "sec_normalized", 0.85

    # Word-prefix match, both directions:
    #   forward  — the sponsor name is the leading whole words of an SEC name
    #              ("arcus bio" -> "arcus biosciences");
    #   reverse  — an SEC name is the leading whole words of the sponsor name
    #              ("novartis pharmaceuticals" -> "novartis"), i.e. a subsidiary
    #              or divisional label on the listed parent.
    # Short / single generic words are too ambiguous for either direction.
    if len(norm) < 4 or (" " not in norm and len(norm) < 6):
        return None, "sec_none", 0.0
    prefix = norm + " "
    matches: list[tuple[str, str]] = [
        (sec_name, tk) for sec_name, tk in _SEC_NORM_TO_TICKER.items() if sec_name.startswith(prefix)
    ]
    if not matches:
        words = norm.split(" ")
        # longest SEC name that is a whole-word prefix of the sponsor (>= 2 words or >= 6 chars)
        for k in range(len(words) - 1, 0, -1):
            candidate = " ".join(words[:k])
            if (k >= 2 or len(candidate) >= 6) and candidate in _SEC_NORM_TO_TICKER:
                return _SEC_NORM_TO_TICKER[candidate], "sec_fuzzy", 0.7
        return None, "sec_none", 0.0
    tickers = {tk for _, tk in matches}
    if len(tickers) == 1:
        return matches[0][1], "sec_fuzzy", 0.75
    pharma = [(n, tk) for n, tk in matches if any(h in n for h in _PHARMA_HINTS)]
    if len({tk for _, tk in pharma}) == 1:
        return pharma[0][1], "sec_fuzzy", 0.65
    # Ambiguous — refuse rather than guess (the old matcher returned matches[0]).
    return None, "sec_ambiguous", 0.0


def resolve_ticker_sec(sponsor_name: str) -> Optional[str]:
    """SEC-only fuzzy layer (exact -> normalized -> word-prefix). Back-compat helper."""
    if not sponsor_name:
        return None
    ticker, _source, _conf = _resolve_sec_layer(sponsor_name)
    return ticker


# ── GRID name maps (sector map + company_profiles) ────────────────────────────

_SECTOR_NORM_MAP: dict[str, str] | None = None
_PROFILE_NORM_MAP: dict[str, str] | None = None


def _sector_name_map() -> dict[str, str]:
    """Normalised name -> ticker from ``analysis.sector_map`` via news_ticker_resolver."""
    global _SECTOR_NORM_MAP
    if _SECTOR_NORM_MAP is not None:
        return _SECTOR_NORM_MAP
    out: dict[str, str] = {}
    try:
        from intelligence.news_ticker_resolver import _load_sector_universe

        _tickers, name_map = _load_sector_universe()
        for name, tk in name_map.items():
            norm = normalize_sponsor_name(name)
            if norm:
                out.setdefault(norm, tk)
    except Exception as exc:  # noqa: BLE001
        log.warning("sponsor_resolver: sector map unavailable: {e}", e=str(exc))
    _SECTOR_NORM_MAP = out
    return out


_PROFILE_NAMES_SQL = text(
    "SELECT ticker, name FROM company_profiles WHERE name IS NOT NULL AND ticker IS NOT NULL"
)


def _profile_name_map(engine: Any) -> dict[str, str]:
    """Normalised ``company_profiles.name`` -> ticker (loaded once per process)."""
    global _PROFILE_NORM_MAP
    if _PROFILE_NORM_MAP is not None:
        return _PROFILE_NORM_MAP
    out: dict[str, str] = {}
    if engine is not None:
        try:
            with engine.connect() as conn:
                rows = conn.execute(_PROFILE_NAMES_SQL).fetchall()
            for row in rows:
                norm = normalize_sponsor_name(row[1])
                tk = str(row[0] or "").strip().upper()
                if norm and tk:
                    out.setdefault(norm, tk)
        except Exception as exc:  # noqa: BLE001
            log.warning("sponsor_resolver: company_profiles names unavailable: {e}", e=str(exc))
    _PROFILE_NORM_MAP = out
    return out


def _resolve_name_maps(engine: Any, norm: str) -> tuple[str | None, str, float]:
    if not norm:
        return None, "maps_none", 0.0
    tk = _sector_name_map().get(norm)
    if tk:
        return tk, "sector_map", 0.85
    tk = _profile_name_map(engine).get(norm)
    if tk:
        return tk, "company_profiles", 0.85
    return None, "maps_none", 0.0


# ── Persistent cache: sponsor_ticker_map ──────────────────────────────────────

_ENSURE_TABLE_SQL = text(
    """
    CREATE TABLE IF NOT EXISTS sponsor_ticker_map (
        sponsor_norm  TEXT PRIMARY KEY,
        ticker        TEXT,
        source        TEXT NOT NULL,
        confidence    NUMERIC(4,3) NOT NULL DEFAULT 0,
        resolved_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        notes         TEXT
    )
    """
)
_ENSURE_INDEX_SQL = text(
    "CREATE INDEX IF NOT EXISTS idx_sponsor_ticker_map_ticker "
    "ON sponsor_ticker_map (ticker) WHERE ticker IS NOT NULL"
)
_CACHE_GET_SQL = text(
    "SELECT ticker, source, confidence, resolved_at, notes "
    "FROM sponsor_ticker_map WHERE sponsor_norm = :norm"
)
_CACHE_PUT_SQL = text(
    """
    INSERT INTO sponsor_ticker_map (sponsor_norm, ticker, source, confidence, resolved_at, notes)
    VALUES (:norm, :ticker, :source, :confidence, NOW(), :notes)
    ON CONFLICT (sponsor_norm) DO UPDATE SET
        ticker      = EXCLUDED.ticker,
        source      = EXCLUDED.source,
        confidence  = EXCLUDED.confidence,
        resolved_at = NOW(),
        notes       = EXCLUDED.notes
    WHERE sponsor_ticker_map.source <> 'curated'
    """
)

_TABLE_ENSURED: set[int] = set()
_MEMO: dict[str, ResolvedSponsor] = {}


def ensure_sponsor_map_table(engine: Any) -> bool:
    """Create ``sponsor_ticker_map`` if missing (idempotent; never raises). Returns success."""
    if engine is None:
        return False
    key = id(engine)
    if key in _TABLE_ENSURED:
        return True
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_TABLE_SQL)
            conn.execute(_ENSURE_INDEX_SQL)
        _TABLE_ENSURED.add(key)
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning("sponsor_resolver: ensure_sponsor_map_table failed: {e}", e=str(exc))
        return False


def _cache_get(engine: Any, norm: str) -> ResolvedSponsor | None:
    if engine is None or not norm:
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(_CACHE_GET_SQL, {"norm": norm}).first()
    except Exception as exc:  # noqa: BLE001
        log.warning("sponsor_resolver: cache read failed: {e}", e=str(exc))
        return None
    if row is None:
        return None
    ticker = str(row[0]).strip().upper() if row[0] else None
    source = str(row[1] or "cache")
    confidence = float(row[2] or 0.0)
    resolved_at = row[3]
    notes = row[4]
    if ticker is None and source not in ("non_industry", "curated"):
        # Negative, non-structural answer: retry after the TTL.
        if isinstance(resolved_at, datetime):
            ts = resolved_at if resolved_at.tzinfo else resolved_at.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - ts > timedelta(days=NEGATIVE_TTL_DAYS):
                return None
    return ResolvedSponsor(
        ticker=ticker,
        source=f"cache:{source}",
        confidence=confidence,
        reason=None if ticker else (str(notes) if notes else "unresolved"),
    )


def _cache_put(engine: Any, norm: str, result: ResolvedSponsor, raw_name: str) -> None:
    if engine is None or not norm:
        return
    if not ensure_sponsor_map_table(engine):
        return
    notes = result.reason if result.ticker is None else raw_name[:200]
    try:
        with engine.begin() as conn:
            conn.execute(
                _CACHE_PUT_SQL,
                {
                    "norm": norm,
                    "ticker": result.ticker,
                    "source": result.source,
                    "confidence": round(float(result.confidence), 3),
                    "notes": notes,
                },
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("sponsor_resolver: cache write failed for {n!r}: {e}", n=norm, e=str(exc))


# ── Local LLM last resort ─────────────────────────────────────────────────────

_PAID_MARKERS = ("openai", "openrouter", "anthropic", "huggingface")


def _client_is_paid(client: Any) -> bool:
    """Defensive check that the router did not hand us a paid-tier client."""
    label = " ".join(
        str(x) for x in (
            getattr(client, "_health_provider", ""), getattr(client, "provider", ""),
            getattr(client, "_log_prefix", ""), type(client).__name__,
        )
    ).lower()
    return any(m in label for m in _PAID_MARKERS)


def _parse_llm_ticker(answer: str | None, sec: frozenset[str] | set[str] | None = None) -> str | None:
    """Ticker named in an LLM answer, or None.

    Tokens are upper-cased and stripped of ``$``/punctuation. When ``sec`` is
    given, the first token that is a member of the SEC set wins (so a chatty
    "The ticker is MRNA." still resolves); without it the first ticker-shaped
    token is returned. ``NONE`` / empty -> None.
    """
    if not answer:
        return None
    tokens = [t.strip().strip("$`'\"*.,;:()[]").upper() for t in str(answer).strip().split()]
    tokens = [t for t in tokens if t]
    if not tokens or tokens[0] == "NONE":
        return None
    if sec:
        for t in tokens:
            if t in sec:
                return t
        return None
    token = tokens[0]
    if token == "NONE" or not _TICKER_RE.match(token):
        return None
    return token


def _resolve_llm_layer(sponsor_name: str) -> tuple[str | None, str, float, str | None]:
    """(ticker, source, confidence, reason) from the LOCAL tier; accepted only if in the SEC set."""
    sec = sec_ticker_set()
    if not sec:
        return None, "llm_skipped", 0.0, "sec_set_unavailable"
    try:
        from llm.router import Tier, get_llm

        client = get_llm(Tier.LOCAL)
    except Exception as exc:  # noqa: BLE001
        log.warning("sponsor_resolver: local LLM unavailable: {e}", e=str(exc))
        return None, "llm_skipped", 0.0, "llm_unavailable"
    if client is None or not getattr(client, "is_available", True) or not hasattr(client, "chat"):
        return None, "llm_skipped", 0.0, "llm_unavailable"
    if _client_is_paid(client):
        log.warning("sponsor_resolver: refusing paid LLM client {c}", c=type(client).__name__)
        return None, "llm_skipped", 0.0, "llm_paid_refused"
    try:
        answer = client.chat(
            [{"role": "user", "content": LLM_PROMPT.format(name=str(sponsor_name)[:200])}],
            temperature=0.0,
            num_predict=LLM_MAX_TOKENS,
        )
    except TypeError:
        try:
            answer = client.chat([{"role": "user", "content": LLM_PROMPT.format(name=str(sponsor_name)[:200])}])
        except Exception as exc:  # noqa: BLE001
            log.warning("sponsor_resolver: LLM call failed: {e}", e=str(exc))
            return None, "llm_skipped", 0.0, "llm_error"
    except Exception as exc:  # noqa: BLE001
        log.warning("sponsor_resolver: LLM call failed: {e}", e=str(exc))
        return None, "llm_skipped", 0.0, "llm_error"
    if answer is None:
        return None, "llm_local", 0.0, "llm_none"
    token = _parse_llm_ticker(answer, sec)
    if token is None:
        if _parse_llm_ticker(answer) is None:
            return None, "llm_local", 0.0, "llm_none"
        return None, "llm_local", 0.0, "llm_not_in_sec"
    return token, "llm_local", LLM_CONFIDENCE, None


# ── Public API ────────────────────────────────────────────────────────────────


def resolve_sponsor(
    engine: Any,
    sponsor_name: str,
    sponsor_class: str | None = None,
    *,
    use_llm: bool = True,
) -> ResolvedSponsor:
    """Resolve one sponsor through the layered pipeline (see module docstring).

    ``engine`` is a SQLAlchemy Engine (or None — then the persistent cache and
    ``company_profiles`` layers are skipped and the result is memoised only
    for this process).
    """
    raw = str(sponsor_name or "").strip()
    norm = normalize_sponsor_name(raw)
    if not norm:
        return ResolvedSponsor(None, "non_industry", 1.0, "empty_name")

    memo = _MEMO.get(norm)
    if memo is not None:
        return memo

    # 1. hard reject (class, then name patterns)
    industry = is_industry_class(sponsor_class)
    if industry is False or is_non_industry_name(raw):
        result = ResolvedSponsor(None, "non_industry", 1.0, "non_industry")
        _MEMO[norm] = result
        _cache_put(engine, norm, result, raw)
        return result

    # 2. persistent cache
    cached = _cache_get(engine, norm)
    if cached is not None:
        _MEMO[norm] = cached
        return cached

    # 3. SEC company_tickers.json
    ticker, source, conf = _resolve_sec_layer(raw)
    reason: str | None = None
    if ticker is None:
        # 4. GRID name maps (sector map, company_profiles)
        maps_ticker, maps_source, maps_conf = _resolve_name_maps(engine, norm)
        if maps_ticker is not None:
            ticker, source, conf = maps_ticker, maps_source, maps_conf
    if ticker is None:
        # 5. local LLM — INDUSTRY-class sponsors only, answer must be in the SEC set
        if use_llm and industry is True:
            ticker, source, conf, reason = _resolve_llm_layer(raw)
        elif source != "sec_ambiguous":
            source = "unresolved"
        if ticker is None:
            conf = 0.0
            if reason is None:
                reason = "sec_ambiguous" if source == "sec_ambiguous" else "unresolved"

    if ticker is not None and not _TICKER_RE.match(ticker):
        ticker, source, conf, reason = None, "unresolved", 0.0, "bad_ticker_shape"

    result = ResolvedSponsor(ticker, source, conf, reason)
    _MEMO[norm] = result
    _cache_put(engine, norm, result, raw)
    return result


def resolve_many(
    engine: Any,
    sponsors: Iterable[tuple[str, str | None]],
    *,
    use_llm: bool = True,
    pause_s: float = 0.0,
) -> dict[str, ResolvedSponsor]:
    """Resolve many ``(sponsor_name, sponsor_class)`` pairs; keyed by the raw sponsor name."""
    out: dict[str, ResolvedSponsor] = {}
    for name, cls in sponsors:
        raw = str(name or "").strip()
        if not raw or raw in out:
            continue
        out[raw] = resolve_sponsor(engine, raw, cls, use_llm=use_llm)
        if pause_s and out[raw].source in ("llm_local",):
            time.sleep(pause_s)
    return out


def clear_caches() -> None:
    """Reset every process-level cache (tests)."""
    global _SEC_LOADED, _SECTOR_NORM_MAP, _PROFILE_NORM_MAP
    _SEC_LOADED = False
    _SEC_NAME_TO_TICKER.clear()
    _SEC_NORM_TO_TICKER.clear()
    _SEC_TICKERS.clear()
    _SEC_TICKER_TO_CIK.clear()
    _SECTOR_NORM_MAP = None
    _PROFILE_NORM_MAP = None
    _TABLE_ENSURED.clear()
    _MEMO.clear()
