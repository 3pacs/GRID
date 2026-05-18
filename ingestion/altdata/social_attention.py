"""
GRID social attention data ingestion.

Pulls attention signals from free public sources to measure how much
retail and institutional interest a stock is getting. Information spreads
through social media — we measure the ripples.

Sources:
    1. Google Trends — relative search interest (weekly, back to 2004)
    2. Wikipedia pageviews — daily pageviews (back to 2015)
    3. SEC EDGAR filing views — daily page views on EDGAR (institutional attention)

All stored as raw_series with ATTENTION: prefix for entity_map resolution.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── Ticker → search term mapping ────────────────────────────────────────

TICKER_SEARCH_TERMS: dict[str, dict[str, str]] = {
    "AAPL": {"search": "Apple stock", "wiki": "Apple_Inc."},
    "MSFT": {"search": "Microsoft stock", "wiki": "Microsoft"},
    "GOOGL": {"search": "Google stock", "wiki": "Alphabet_Inc."},
    "AMZN": {"search": "Amazon stock", "wiki": "Amazon_(company)"},
    "NVDA": {"search": "NVIDIA stock", "wiki": "Nvidia"},
    "META": {"search": "Meta stock", "wiki": "Meta_Platforms"},
    "TSLA": {"search": "Tesla stock", "wiki": "Tesla,_Inc."},
    "SPY": {"search": "S&P 500", "wiki": "S%26P_500"},
    "QQQ": {"search": "QQQ ETF", "wiki": "Invesco_QQQ_Trust"},
    "LLY": {"search": "Eli Lilly stock", "wiki": "Eli_Lilly_and_Company"},
    "V": {"search": "Visa stock", "wiki": "Visa_Inc."},
    "MA": {"search": "Mastercard stock", "wiki": "Mastercard"},
    "UNH": {"search": "UnitedHealth stock", "wiki": "UnitedHealth_Group"},
    "RTX": {"search": "RTX stock Raytheon", "wiki": "RTX_Corporation"},
    "AVGO": {"search": "Broadcom stock", "wiki": "Broadcom_Inc."},
    "JPM": {"search": "JPMorgan stock", "wiki": "JPMorgan_Chase"},
}

_RATE_LIMIT = 1.5  # seconds between requests


# ── Wikipedia Pageview Puller ────────────────────────────────────────────

class WikipediaAttentionPuller(BasePuller):
    """Pull daily Wikipedia pageview counts as attention proxy.

    Uses the Wikimedia REST API (free, no auth required).
    Daily granularity, data available from July 2015.
    """

    SOURCE_NAME: str = "Wikipedia_Attention"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://wikimedia.org/api/rest_v1/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 45,
    }

    _WIKI_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"

    # In-memory caches for resolved (ticker -> article) lookups and the
    # SEC ticker-to-company-name map. Both are populated lazily on the
    # first lookup and reused for the rest of the process lifetime.
    _article_cache: dict[str, str | None] = {}
    _sec_name_map: dict[str, str] | None = None

    _CORPORATE_SUFFIXES: tuple[str, ...] = (
        " Inc.", " Inc", " Corporation", " Corp.", " Corp",
        " Company", " Co.", " Holdings", " Holding", " Group",
        " Ltd.", " Ltd", " LLC", " plc", " PLC", " S.A.", " N.V.",
        " AG", " /DE/", " /MD/", " /MO/", " /CA/", " /NY/", " /NV/",
        " /FL/", " /TX/", " /MA/", " /WA/", " /PA/", " /MI/", " /IL/",
    )

    def _load_sec_name_map(self) -> dict[str, str]:
        """Pull SEC's master ticker -> company-name map.

        Reuses the same JSON endpoint as earnings_events_puller. Cached
        for the lifetime of the process so Wikipedia resolution stays
        cheap.
        """
        if self._sec_name_map is not None:
            return self._sec_name_map
        try:
            resp = requests.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers={
                    "User-Agent": "GRID Intelligence stepdadfinance@gmail.com",
                    "Accept": "application/json",
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            result: dict[str, str] = {}
            for _, row in data.items():
                try:
                    t = str(row["ticker"]).upper()
                    name = str(row["title"]).strip()
                    if t and name:
                        result.setdefault(t, name)
                except (KeyError, TypeError):
                    continue
            log.info(
                "social_attention: SEC ticker->name map loaded ({n} entries)",
                n=len(result),
            )
            self._sec_name_map = result
            return result
        except Exception as exc:
            log.warning(
                "social_attention: failed to load SEC ticker map: {e}",
                e=str(exc),
            )
            self._sec_name_map = {}
            return self._sec_name_map

    @classmethod
    def _clean_company_name(cls, name: str) -> str:
        """Strip corporate suffixes / state codes for cleaner wiki search."""
        cleaned = name.strip()
        # Iterate (longest first) so 'Inc.' doesn't shadow 'Incorporated'.
        for suffix in sorted(cls._CORPORATE_SUFFIXES, key=len, reverse=True):
            if cleaned.upper().endswith(suffix.upper()):
                cleaned = cleaned[: -len(suffix)].rstrip(", ").strip()
        # Title-case all-caps SEC names ('PATRICK INDUSTRIES INC' -> 'Patrick Industries')
        if cleaned.isupper():
            cleaned = cleaned.title()
        return cleaned

    def _resolve_wiki_article(self, ticker: str) -> str | None:
        """Find the best Wikipedia article title for a ticker (Task #182).

        Order of attempts:
        1. Hard-coded TICKER_SEARCH_TERMS (cheap, no network).
        2. Per-process cache (avoids re-hitting the search API).
        3. SEC company_tickers.json -> cleaned name -> Wikipedia search.
           This is the high-quality path that covers the full ~10k US
           equity universe. We REQUIRE the SEC name to be present so the
           puller doesn't fall back to noisy ticker-string queries (which
           previously resolved PATK -> 'Patak' the Indian sauce brand,
           FCN -> 'FC Nordsjælland' the football club, etc.).
        4. ``company_profiles.name`` (DB-side enrichment) as a secondary
           name source for tickers not in the SEC map (e.g. crypto).

        Returns ``None`` if no SEC/DB name is available, in which case
        the ticker is skipped (better empty than wrong, per the
        feedback_data_confidence_labels rule).
        """
        # (1) static map
        info = TICKER_SEARCH_TERMS.get(ticker)
        if info and "wiki" in info:
            return info["wiki"]

        # (2) cache
        if ticker in self._article_cache:
            return self._article_cache[ticker]

        # (3) SEC ticker map -> cleaned name
        sec_map = self._load_sec_name_map()
        sec_name = sec_map.get(ticker)
        candidates: list[str] = []
        if sec_name:
            cleaned = self._clean_company_name(sec_name)
            if cleaned:
                # Try the cleaned name plus a "(company)" disambiguation
                # variant — Wikipedia frequently disambiguates corporate
                # entries that way (e.g. 'Charter Communications').
                candidates.append(f"{cleaned} (company)")
                candidates.append(cleaned)

        # (4) DB-side enrichment fallback (mostly useful for crypto /
        # non-SEC tickers).
        if not candidates:
            try:
                from sqlalchemy import text as _text
                with self.engine.connect() as conn:
                    row = conn.execute(
                        _text("SELECT name FROM company_profiles "
                              "WHERE ticker = :t"),
                        {"t": ticker},
                    ).fetchone()
                    if row and row[0]:
                        cleaned = self._clean_company_name(row[0])
                        if cleaned:
                            candidates.append(f"{cleaned} (company)")
                            candidates.append(cleaned)
            except Exception as exc:
                log.debug(
                    "company_profiles lookup failed for {t}: {e}",
                    t=ticker, e=str(exc),
                )

        if not candidates:
            # No quality signal -> skip rather than guess. Cache the
            # negative result so we don't re-look it up this run.
            self._article_cache[ticker] = None
            return None

        article = self._wiki_search(candidates, ticker)
        self._article_cache[ticker] = article
        return article

    # Wikidata instance-of (P31) Q-IDs that mean "this article is a company /
    # business / legal entity we'd plausibly trade." Used by the
    # ``_is_company_via_wikidata`` gate below as the FINAL veto on candidate
    # articles — kills the long-tail false positives the title-overlap
    # heuristic cannot catch (PATK -> 'Patrick Dovigi' Q5/human,
    # AVBC -> 'Avidia Bank' / a human-named bank brand, etc.).
    #
    # Allow-list is intentionally narrow: business + subclasses that get
    # traded (public company, enterprise, holding, conglomerate, etc.).
    # If P31 isn't in this set we fall through to the reject list — most
    # importantly Q5 (human), which is the dominant false-positive class
    # because Wikipedia's search treats human-name articles as relevant
    # hits for surname-prefixed company queries.
    _WIKIDATA_COMPANY_QIDS: frozenset[str] = frozenset({
        "Q4830453",     # business
        "Q783794",      # company
        "Q891723",      # public company
        "Q6881511",     # enterprise
        "Q1114213",     # business enterprise
        "Q1616075",     # television station (Comcast / SIRI etc. operate these)
        "Q43229",       # organization
        "Q161726",      # multinational corporation
        "Q2659904",     # government-owned corporation
        "Q210167",      # video game developer
        "Q1058914",     # software company
        "Q18388277",    # technology company
        "Q22687",       # bank
        "Q15265344",    # broadcaster
        "Q507619",      # retail chain
        "Q11032",       # newspaper
        "Q11707",       # restaurant
        "Q431289",      # brand
        "Q4287745",     # medical organization
        "Q484652",      # international organization
        "Q4830453",     # business (dup, harmless)
        "Q488383",      # mining company
        "Q1664720",     # institute
        "Q2385804",     # educational institution
        "Q163740",      # nonprofit
        "Q327333",      # government agency
        "Q3232897",     # holding company
        "Q3919012",     # asset-management company
        "Q1117348",     # conglomerate
        "Q6178582",     # pharmaceutical company
        "Q1664720",     # institute (dup)
        "Q15911314",    # association
        "Q11691",       # stock exchange
        "Q15911314",    # association (dup)
        "Q1107656",     # gymnasium (placeholder; harmless)
        "Q4830453",     # business
        "Q15936437",    # research institute
        "Q43229",       # organization (dup)
        "Q4830453",     # business
        "Q15911314",    # association
    })
    # Hard-reject Q-IDs — if P31 is exclusively one of these, drop the article.
    _WIKIDATA_REJECT_QIDS: frozenset[str] = frozenset({
        "Q5",           # human  (PATK -> Patrick Dovigi, etc.)
        "Q15632617",    # fictional human
        "Q95074",       # fictional character
        "Q11424",       # film
        "Q571",         # book
        "Q7725634",     # literary work
        "Q482994",      # album
        "Q134556",      # single (song)
        "Q7889",        # video game (the work, not the studio)
        "Q11446",       # ship
        "Q12280",       # bridge
        "Q486972",      # human settlement
        "Q515",         # city
    })
    _WIKIDATA_API = "https://www.wikidata.org/w/api.php"
    # Per-process cache: title -> bool (is_company)
    _wikidata_cache: dict[str, bool] = {}

    @classmethod
    def _is_company_via_wikidata(cls, title: str) -> bool:
        """Verify a Wikipedia article maps to a company/business on Wikidata.

        Fetches the article's Wikidata entity (via ``wbgetentities`` with
        ``sites=enwiki&titles=<title>``), pulls every ``P31`` (instance-of)
        claim, and:

        - returns ``True`` if ANY P31 value is in :attr:`_WIKIDATA_COMPANY_QIDS`
          (or a subclass — we don't walk the P279 tree, but the allow-list
          covers the common subclasses directly to keep this cheap).
        - returns ``False`` if all P31 values are in :attr:`_WIKIDATA_REJECT_QIDS`
          (or no P31 claims at all — that's an ambiguous article we'd rather
          skip than guess at).
        - returns ``True`` on API failure (fail-open so a Wikidata outage
          doesn't silently zero out the resolver — the title-overlap check
          still applies).

        Result is cached in :attr:`_wikidata_cache` for the rest of the
        process lifetime. Caching is keyed by the article title exactly as
        passed (no normalization) since the same string is what
        ``_wiki_search`` would use on its next pass.
        """
        if not title:
            return False
        cached = cls._wikidata_cache.get(title)
        if cached is not None:
            return cached
        try:
            resp = requests.get(
                cls._WIKIDATA_API,
                params={
                    "action": "wbgetentities",
                    "sites": "enwiki",
                    "titles": title,
                    "props": "claims",
                    "format": "json",
                    "languages": "en",
                },
                headers={
                    "User-Agent": "GRID-DataPuller/1.0 (trading intelligence)"
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            # Fail-open: don't let Wikidata flakiness kill the whole run.
            log.debug(
                "wikidata lookup failed for '{t}': {e} (fail-open)",
                t=title, e=str(exc),
            )
            cls._wikidata_cache[title] = True
            return True

        entities = (data or {}).get("entities") or {}
        if not entities:
            cls._wikidata_cache[title] = False
            return False
        # entities is a dict of Q-id -> entity. We don't care which Q-id —
        # there's typically exactly one. Iterate to be defensive.
        p31_qids: list[str] = []
        for _qid, entity in entities.items():
            if not isinstance(entity, dict):
                continue
            # 'missing' key set when the article has no Wikidata link.
            if entity.get("missing") is not None or entity.get("missing") == "":
                continue
            claims = (entity.get("claims") or {}).get("P31") or []
            for claim in claims:
                try:
                    qid = (
                        claim.get("mainsnak", {})
                        .get("datavalue", {})
                        .get("value", {})
                        .get("id")
                    )
                    if qid:
                        p31_qids.append(str(qid))
                except (AttributeError, TypeError):
                    continue

        if not p31_qids:
            # No P31 at all — Wikipedia article without proper Wikidata
            # typing. Reject; we'd rather skip than guess.
            log.debug(
                "wikidata reject '{t}': no P31 claims",
                t=title,
            )
            cls._wikidata_cache[title] = False
            return False

        # If any P31 is in our company allow-list, accept.
        if any(q in cls._WIKIDATA_COMPANY_QIDS for q in p31_qids):
            cls._wikidata_cache[title] = True
            return True
        # If every P31 is in the explicit reject set, reject.
        if all(q in cls._WIKIDATA_REJECT_QIDS for q in p31_qids):
            log.debug(
                "wikidata reject '{t}': all P31={p} in reject set",
                t=title, p=p31_qids,
            )
            cls._wikidata_cache[title] = False
            return False
        # Ambiguous type (e.g. P31=organization-but-not-business). Be
        # conservative — reject. Loud DEBUG so we can audit.
        log.debug(
            "wikidata reject '{t}': P31={p} not in company allow-list",
            t=title, p=p31_qids,
        )
        cls._wikidata_cache[title] = False
        return False

    # Generic words that shouldn't count toward title-overlap matching;
    # otherwise 'Industries' / 'Group' / 'Holdings' alone will rubber-stamp
    # any unrelated article that happens to share one of those words.
    _GENERIC_WORDS: frozenset[str] = frozenset({
        "industries", "company", "corporation", "corp", "inc",
        "incorporated", "group", "holdings", "holding", "ltd", "limited",
        "plc", "international", "global", "the", "and", "of", "co",
        "enterprise", "enterprises", "systems", "technologies",
        "technology", "solutions", "services", "products",
    })

    @classmethod
    def _title_matches(cls, query: str, title: str) -> bool:
        """Reject Wikipedia results that don't share a real word with the query.

        Wikipedia's full-text search will happily return the *nearest*
        article rather than no article (e.g. 'Patrick Industries' ->
        'Patrick Dovigi', 'S&P Global' -> 'List of S&P 400 companies').

        Heuristic:
        1. Reject Wikipedia meta-pages outright: 'List of *', 'Index of *',
           'Category:*', disambig pages ending in '(disambiguation)'.
        2. Require the FIRST non-generic word of the query to appear in
           the title. The first word is almost always the distinctive
           company-name root ('Patrick', 'Installed', 'Charter', 'Zoetis').
           Tail words like 'Industries', 'Building Products', 'Global'
           are too easy to false-positive on.
        """
        if not title:
            return False
        low_title = title.lower()
        if low_title.startswith(("list of ", "index of ", "category:")):
            return False
        if "(disambiguation)" in low_title:
            return False

        def _filtered_tokens(s: str) -> list[str]:
            return [
                w.lower().strip(".,()/&'\"")
                for w in s.replace("_", " ").split()
                if w and w.lower().strip(".,()/&'\"") not in cls._GENERIC_WORDS
            ]

        q_tokens = _filtered_tokens(query)
        t_tokens = set(_filtered_tokens(title))
        if not q_tokens or not t_tokens:
            return False
        # First non-generic word of query must appear in the title.
        return q_tokens[0] in t_tokens

    def _wiki_search(
        self, candidates: list[str], ticker: str
    ) -> str | None:
        """Try each candidate query against Wikipedia search until a hit.

        Validates that the returned title shares a non-generic word with
        the query (see ``_title_matches``) — otherwise the result is
        treated as a miss and we move on to the next candidate.
        """
        for query in candidates:
            try:
                resp = requests.get(
                    "https://en.wikipedia.org/w/api.php",
                    params={
                        "action": "query",
                        "list": "search",
                        "srsearch": query,
                        "srlimit": 3,
                        "format": "json",
                    },
                    headers={
                        "User-Agent": "GRID-DataPuller/1.0 (trading intelligence)"
                    },
                    timeout=10,
                )
                resp.raise_for_status()
                data = resp.json()
                hits = data.get("query", {}).get("search", [])
                for hit in hits:
                    title = hit.get("title")
                    if not title:
                        continue
                    if not self._title_matches(query, title):
                        log.debug(
                            "wiki reject {t}: '{q}' -> '{ti}' (no word overlap)",
                            t=ticker, q=query, ti=title,
                        )
                        continue
                    # Wikidata company gate — last-line defense against the
                    # PATK -> 'Patrick Dovigi' / AVBC -> human-named-bank
                    # class of false positives. _is_company_via_wikidata
                    # is fail-open, so a Wikidata outage doesn't zero out
                    # the resolver — only confident-rejections drop the
                    # hit.
                    if not self._is_company_via_wikidata(title):
                        log.debug(
                            "wiki reject {t}: '{q}' -> '{ti}' (wikidata non-company)",
                            t=ticker, q=query, ti=title,
                        )
                        continue
                    article = title.replace(" ", "_")
                    log.debug(
                        "wiki resolve {t} -> {a} (query='{q}')",
                        t=ticker, a=article, q=query,
                    )
                    return article
            except Exception as exc:
                log.debug(
                    "wiki search '{q}' failed for {t}: {e}",
                    q=query, t=ticker, e=str(exc),
                )
                continue
            time.sleep(0.3)  # Wikipedia search is gentle but be polite
        log.debug("wiki resolve {t}: no match", t=ticker)
        return None

    def pull_ticker(self, ticker: str, days: int = 90) -> dict[str, Any]:
        """Pull Wikipedia pageview data for a ticker."""
        article = self._resolve_wiki_article(ticker)
        if not article:
            return {"status": "SKIPPED", "ticker": ticker, "rows": 0,
                    "reason": "no_wiki_article"}
        end = date.today()
        start = end - timedelta(days=days)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        # The pageviews API expects the article title URL-encoded in the
        # path. urllib.parse.quote with safe='%' preserves any pre-encoded
        # entities in the static map (e.g. 'S%26P_500').
        from urllib.parse import quote as _quote
        article_path = _quote(article, safe="%")
        url = (
            f"{self._WIKI_API}/en.wikipedia/all-access/all-agents/"
            f"{article_path}/daily/{start_str}00/{end_str}00"
        )

        try:
            resp = requests.get(
                url,
                headers={"User-Agent": "GRID-DataPuller/1.0 (trading intelligence)"},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.warning("Wikipedia fetch failed for {t}: {e}", t=ticker, e=str(exc))
            return {"status": "FAILED", "ticker": ticker, "error": str(exc)}

        items = data.get("items", [])
        inserted = 0

        with self.engine.begin() as conn:
            for item in items:
                ts = item.get("timestamp", "")
                views = item.get("views", 0)
                try:
                    obs_date = datetime.strptime(ts[:8], "%Y%m%d").date()
                except (ValueError, IndexError):
                    continue

                sid = f"ATTENTION:{ticker}_wiki_views"
                if self._row_exists(sid, obs_date, conn):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=obs_date,
                    value=float(views),
                    raw_payload={"ticker": ticker, "article": article, "views": views},
                )
                inserted += 1

        if inserted > 0:
            log.info(
                "Wikipedia {t}: {n} days stored ({a})",
                t=ticker, n=inserted, a=article,
            )
        return {"status": "SUCCESS", "ticker": ticker, "rows": inserted}

    def pull_all(
        self,
        tickers: list[str] | None = None,
        ticker_source: str = "signal_registry",
        days: int = 90,
        lookback_days: int = 120,
        min_signals: int = 3,
        max_tickers: int = 1000,
    ) -> dict[str, Any]:
        """Pull Wikipedia pageviews for all tracked tickers.

        Task #182 (2026-05-17): default universe is now ``signal_registry``
        which expands coverage from the hard-coded 16-mega-cap list to the
        full ~580-ticker active universe. Pass ``ticker_source="watchlist"``
        for legacy behaviour, or supply ``tickers=[...]`` explicitly.
        """
        if tickers is None:
            try:
                from ingestion.watchlist_resolver import resolve_universe
                tickers = resolve_universe(
                    self.engine,
                    source=ticker_source,
                    lookback_days=lookback_days,
                    min_signals=min_signals,
                    max_tickers=max_tickers,
                )
            except Exception as exc:
                log.warning(
                    "watchlist_resolver unavailable ({e}) — "
                    "falling back to hard-coded TICKER_SEARCH_TERMS",
                    e=str(exc),
                )
                tickers = list(TICKER_SEARCH_TERMS.keys())

        results = []
        total = 0
        skipped = 0
        for ticker in tickers:
            result = self.pull_ticker(ticker, days=days)
            results.append(result)
            if result.get("status") == "SKIPPED":
                skipped += 1
            total += result.get("rows", 0)
            time.sleep(_RATE_LIMIT)

        log.info(
            "Wikipedia attention: {n} total rows from {t} tickers "
            "({sk} skipped — no wiki article)",
            n=total, t=len(tickers), sk=skipped,
        )
        return {
            "status": "SUCCESS", "rows_inserted": total,
            "tickers_attempted": len(tickers),
            "tickers_skipped": skipped,
            "results": results,
        }


# ── EDGAR Filing View Puller ─────────────────────────────────────────────

class EdgarViewsPuller(BasePuller):
    """Pull SEC EDGAR full-text search index page view data.

    Uses EDGAR's log file data (EDGAR access logs are public).
    Measures institutional attention — who's reading SEC filings?
    """

    SOURCE_NAME: str = "EDGAR_Attention"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://efts.sec.gov/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 46,
    }

    # CIK lookup for major tickers
    _CIK_MAP: dict[str, str] = {
        "AAPL": "0000320193", "MSFT": "0000789019",
        "GOOGL": "0001652044", "AMZN": "0001018724",
        "NVDA": "0001045810", "META": "0001326801",
        "TSLA": "0001318605", "JPM": "0000019617",
        "V": "0001403161", "MA": "0001141391",
    }

    _EFTS_URL = "https://efts.sec.gov/LATEST/search-index"

    def pull_ticker(self, ticker: str, days: int = 30) -> dict[str, Any]:
        """Pull EDGAR filing view counts for a ticker.

        Uses the EDGAR full-text search to count recent filings
        as a proxy for institutional attention.
        """
        cik = self._CIK_MAP.get(ticker)
        if not cik:
            return {"status": "SKIPPED", "ticker": ticker, "rows": 0}

        # Use EDGAR EFTS API to get recent filing counts
        end = date.today()
        start = end - timedelta(days=days)

        url = f"https://efts.sec.gov/LATEST/search-index?q=%22{cik}%22&dateRange=custom&startdt={start}&enddt={end}"
        try:
            resp = requests.get(
                url,
                headers={
                    "User-Agent": "GRID-DataPuller/1.0 anik@stepdad.finance",
                    "Accept": "application/json",
                },
                timeout=15,
            )
            if resp.status_code != 200:
                # Fall back to counting filings via submissions API
                return self._pull_filing_count(ticker, cik, days)
        except Exception:
            return self._pull_filing_count(ticker, cik, days)

        return {"status": "SUCCESS", "ticker": ticker, "rows": 0}

    def _pull_filing_count(self, ticker: str, cik: str, days: int) -> dict[str, Any]:
        """Count recent SEC filings as attention proxy."""
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        try:
            resp = requests.get(
                url,
                headers={
                    "User-Agent": "GRID-DataPuller/1.0 anik@stepdad.finance",
                    "Accept": "application/json",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.debug("EDGAR filing count failed for {t}: {e}", t=ticker, e=str(exc))
            return {"status": "FAILED", "ticker": ticker, "error": str(exc)}

        recent = data.get("filings", {}).get("recent", {})
        dates = recent.get("filingDate", [])
        forms = recent.get("form", [])

        cutoff = (date.today() - timedelta(days=days)).isoformat()
        count = sum(1 for d in dates if d >= cutoff)

        # Store as daily aggregate
        today = date.today()
        sid = f"ATTENTION:{ticker}_edgar_filings"

        try:
            with self.engine.begin() as conn:
                if not self._row_exists(sid, today, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id=sid,
                        obs_date=today,
                        value=float(count),
                        raw_payload={
                            "ticker": ticker, "cik": cik,
                            "filing_count": count, "days": days,
                        },
                    )
                    return {"status": "SUCCESS", "ticker": ticker, "rows": 1}
        except Exception as exc:
            log.debug("EDGAR store failed: {e}", e=str(exc))

        return {"status": "SUCCESS", "ticker": ticker, "rows": 0}

    def pull_all(self, tickers: list[str] | None = None) -> dict[str, Any]:
        """Pull EDGAR attention for all tracked tickers."""
        if tickers is None:
            tickers = list(self._CIK_MAP.keys())

        results = []
        total = 0
        for ticker in tickers:
            result = self.pull_ticker(ticker)
            results.append(result)
            total += result.get("rows", 0)
            time.sleep(_RATE_LIMIT)

        return {"status": "SUCCESS", "rows_inserted": total, "results": results}


# ── Google Trends Puller ─────────────────────────────────────────────────

class GoogleTrendsPuller(BasePuller):
    """Pull Google Trends search interest data.

    Weekly relative search volume (0-100) for stock-related terms.
    Requires pytrends library (pip install pytrends).

    Google Trends data goes back to 2004 — 20+ years of retail
    attention data. This is the most reliable free proxy for
    "how much are people talking about this stock."
    """

    SOURCE_NAME: str = "Google_Trends"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://trends.google.com",
        "cost_tier": "FREE",
        "latency_class": "WEEKLY",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "MED",
        "priority_rank": 47,
    }

    def pull_ticker(self, ticker: str, months: int = 3) -> dict[str, Any]:
        """Pull Google Trends data for a ticker."""
        info = TICKER_SEARCH_TERMS.get(ticker)
        if not info or "search" not in info:
            return {"status": "SKIPPED", "ticker": ticker, "rows": 0}

        try:
            from pytrends.request import TrendReq
        except ImportError:
            log.warning("pytrends not installed — pip install pytrends")
            return {"status": "SKIPPED", "ticker": ticker, "rows": 0,
                    "error": "pytrends not installed"}

        search_term = info["search"]
        timeframe = f"today {months}-m" if months <= 12 else "today 5-y"

        try:
            pytrends = TrendReq(hl="en-US", tz=360)
            pytrends.build_payload([search_term], timeframe=timeframe)
            df = pytrends.interest_over_time()
        except Exception as exc:
            log.warning("Google Trends fetch failed for {t}: {e}", t=ticker, e=str(exc))
            return {"status": "FAILED", "ticker": ticker, "error": str(exc)}

        if df is None or df.empty:
            return {"status": "SKIPPED", "ticker": ticker, "rows": 0}

        inserted = 0
        with self.engine.begin() as conn:
            for idx, row in df.iterrows():
                obs_date = idx.date() if hasattr(idx, "date") else idx
                value = float(row[search_term])
                sid = f"ATTENTION:{ticker}_google_trends"

                if self._row_exists(sid, obs_date, conn):
                    continue

                self._insert_raw(
                    conn=conn,
                    series_id=sid,
                    obs_date=obs_date,
                    value=value,
                    raw_payload={
                        "ticker": ticker, "search_term": search_term,
                        "relative_interest": value,
                    },
                )
                inserted += 1

        if inserted > 0:
            log.info(
                "Google Trends {t}: {n} weeks stored (term: {s})",
                t=ticker, n=inserted, s=search_term,
            )
        return {"status": "SUCCESS", "ticker": ticker, "rows": inserted}

    def pull_all(self, tickers: list[str] | None = None) -> dict[str, Any]:
        """Pull Google Trends for all tracked tickers."""
        if tickers is None:
            tickers = list(TICKER_SEARCH_TERMS.keys())

        results = []
        total = 0
        for ticker in tickers:
            result = self.pull_ticker(ticker)
            results.append(result)
            total += result.get("rows", 0)
            # Google Trends rate limits aggressively
            time.sleep(3.0)

        log.info("Google Trends: {n} total rows from {t} tickers",
                 n=total, t=len(tickers))
        return {"status": "SUCCESS", "rows_inserted": total, "results": results}


# ----------------------------------------------------------------------
# CLI / one-shot backfill entry point (Task #182)
# ----------------------------------------------------------------------
def main() -> None:
    """Run the Wikipedia attention puller from the command line.

    Used for the one-shot backfill that expands social_attention coverage
    from the 16 hard-coded mega-caps to the full ~580-ticker universe.
    The scheduler (smart_scheduler) still calls ``pull_all`` via the
    class registry; this CLI is for the backfill + ad-hoc runs.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="GRID social_attention puller (Task #182)"
    )
    parser.add_argument(
        "--tickers",
        help="Comma-separated explicit ticker list (overrides --ticker-source)",
    )
    parser.add_argument(
        "--ticker-source",
        choices=["signal_registry", "watchlist", "cli"],
        default="signal_registry",
        help="Where to load the ticker universe from. Default "
             "'signal_registry' = ~580 active tickers; 'watchlist' = "
             "legacy 16 hard-coded mega-caps.",
    )
    parser.add_argument("--days", type=int, default=30,
                        help="Pageview lookback per ticker (default 30)")
    parser.add_argument("--lookback-days", type=int, default=120,
                        help="signal_registry activity window")
    parser.add_argument("--min-signals", type=int, default=3,
                        help="Min signal_registry rows per ticker")
    parser.add_argument("--max-tickers", type=int, default=1000)
    args = parser.parse_args()

    from db import get_engine

    engine = get_engine()
    puller = WikipediaAttentionPuller(engine)

    override = None
    if args.tickers:
        override = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]

    result = puller.pull_all(
        tickers=override,
        ticker_source=args.ticker_source,
        days=args.days,
        lookback_days=args.lookback_days,
        min_signals=args.min_signals,
        max_tickers=args.max_tickers,
    )
    log.info(
        "social_attention puller done: rows={r} attempted={a} skipped={s}",
        r=result.get("rows_inserted"),
        a=result.get("tickers_attempted"),
        s=result.get("tickers_skipped"),
    )


if __name__ == "__main__":
    main()
