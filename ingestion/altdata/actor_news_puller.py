"""Actor news puller — pull news mentions for every actor in sector_map.

Hits five free sources per actor:
    1. Google News RSS       (unlimited, atom feed)
    2. GDELT DOC API         (~120 req/min, JSON)
    3. Wikipedia REST API    (bio extract, 500 req/s)
    4. SEC EDGAR full-text   (filing mentions)
    5. CrossRef              (scholarly mentions)

Non-ticker actors (activists, family offices, CEOs, trade groups,
private mega-caps, regulators) cannot be found via ticker-based news
ingestion. This puller queries by name, writing structured rows to
``actor_news`` with stance markers and loyalty signals extracted via
regex, plus Wikipedia-derived biographical context in ``actor_bio``.

Rate limits are respected between sources via ``_RATE_LIMIT`` dict.
The puller is idempotent via UNIQUE(actor_id, source, url).
"""
from __future__ import annotations

import html
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import retry_on_failure

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

USER_AGENT: str = "GRID-ActorNewsPuller/1.0 (research; contact=ops@stepdad.finance)"
_REQUEST_TIMEOUT: int = 20

# Per-source rate limits (seconds between requests)
_RATE_LIMIT: dict[str, float] = {
    "google_news": 0.3,   # RSS, unlimited but be polite
    "gdelt":       2.0,   # ~30/min — GDELT aggressively 429s above this
    "wikipedia":   0.1,   # REST API is generous
    "sec_edgar":   0.2,   # SEC asks for <10 req/s
    "crossref":    0.3,
}

# Regex patterns for stance / loyalty extraction. Applied to title+snippet.
_STANCE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("pro",         re.compile(r"\b(support(?:s|ed|ing)?|endors(?:e|ed|ing)|back(?:s|ed|ing)?|advocat(?:e|ed|ing)|champion(?:s|ed|ing)?)\b", re.I)),
    ("anti",        re.compile(r"\b(oppos(?:e|es|ed|ing)|criticiz(?:e|es|ed|ing)|blast(?:s|ed|ing)?|reject(?:s|ed|ing)?|condemn(?:s|ed|ing)?)\b", re.I)),
    ("call_for",    re.compile(r"\bcall(?:s|ed|ing)?\s+for\b", re.I)),
    ("warned",      re.compile(r"\bwarn(?:s|ed|ing)?\b", re.I)),
    ("sued",        re.compile(r"\b(sues|sued|lawsuit|litigation)\b", re.I)),
    ("activist",    re.compile(r"\b(activist|proxy\s+fight|hostile)\b", re.I)),
    ("voted",       re.compile(r"\bvot(?:e|ed|ing)\s+(?:for|against)\b", re.I)),
]

_LOYALTY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("former_employee", re.compile(r"\bformer\s+(?:CEO|CFO|COO|executive|director|president|chair(?:man|woman)?)\s+(?:of|at)\s+([A-Z][\w&\s\.]{2,40})", re.I)),
    ("board_member",    re.compile(r"\bboard\s+(?:member|director)\s+(?:of|at)\s+([A-Z][\w&\s\.]{2,40})", re.I)),
    ("founded",         re.compile(r"\b(?:founded|co-founded|founder\s+of)\s+([A-Z][\w&\s\.]{2,40})", re.I)),
    ("donor_to",        re.compile(r"\bdonat(?:e|ed|ing)\s+to\s+([A-Z][\w&\s\.]{2,40})", re.I)),
    ("allied_with",     re.compile(r"\ballied\s+(?:with|to)\s+([A-Z][\w&\s\.]{2,40})", re.I)),
    ("backed_by",       re.compile(r"\bback(?:ed|ing)?\s+by\s+([A-Z][\w&\s\.]{2,40})", re.I)),
]

# Simple sentiment lexicon (fallback when VADER missing)
_POS_WORDS: set[str] = {
    "good", "great", "strong", "growth", "beats", "beat", "exceed", "rally",
    "surge", "gain", "gains", "win", "wins", "positive", "profit", "profits",
    "approve", "approved", "boost", "boosts", "outperform", "upgrade",
    "successful", "success",
}
_NEG_WORDS: set[str] = {
    "bad", "weak", "loss", "losses", "miss", "missed", "decline", "drop",
    "drops", "fall", "falls", "fell", "plunge", "crash", "fraud", "scandal",
    "lawsuit", "sued", "investigation", "probe", "fine", "fined", "downgrade",
    "warn", "warning", "cut", "cuts", "negative", "risk", "bankrupt",
}


@dataclass
class NewsRow:
    """Structured news mention for insertion into actor_news."""

    actor_id: str
    source: str
    url: str
    title: str
    snippet: str = ""
    published_at: datetime | None = None
    sentiment: float | None = None
    stance_markers: list[str] = field(default_factory=list)
    loyalty_signals: list[str] = field(default_factory=list)
    raw_content: str = ""


@dataclass
class BioRow:
    """Biographical row for actor_bio."""

    actor_id: str
    wikipedia_url: str | None = None
    wikipedia_extract: str | None = None
    affiliations: list[str] = field(default_factory=list)
    positions: list[str] = field(default_factory=list)
    net_worth_usd: float | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def slugify(name: str) -> str:
    """Normalize an actor name into a stable actor_id slug."""
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")


def score_sentiment(text_blob: str) -> float:
    """Tiny hand-rolled lexicon sentiment in [-1, 1]."""
    if not text_blob:
        return 0.0
    tokens = re.findall(r"[a-zA-Z]+", text_blob.lower())
    if not tokens:
        return 0.0
    pos = sum(1 for t in tokens if t in _POS_WORDS)
    neg = sum(1 for t in tokens if t in _NEG_WORDS)
    if pos + neg == 0:
        return 0.0
    return round((pos - neg) / (pos + neg), 3)


def extract_stance(text_blob: str) -> list[str]:
    """Return list of stance markers found in text (label:snippet)."""
    out: list[str] = []
    for label, pat in _STANCE_PATTERNS:
        if pat.search(text_blob):
            out.append(label)
    return sorted(set(out))


def extract_loyalty(text_blob: str) -> list[str]:
    """Return list of loyalty signals: 'label:target'."""
    out: list[str] = []
    for label, pat in _LOYALTY_PATTERNS:
        for m in pat.finditer(text_blob):
            target = m.group(1).strip() if m.groups() else ""
            if target:
                # Clean trailing words that get greedily pulled in
                target = re.sub(r"\s+(said|told|announced|reported|filed|who|which|that|and|or).*$", "", target, flags=re.I)
                target = target.strip(" .,;:")
                if 2 <= len(target) <= 60:
                    out.append(f"{label}:{target}")
    return sorted(set(out))


def parse_rfc822(s: str) -> datetime | None:
    """Parse RFC822 date from Google News RSS."""
    if not s:
        return None
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S GMT",
    ):
        try:
            dt = datetime.strptime(s.strip(), fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# Source fetchers (one per source; all return list[NewsRow])
# ---------------------------------------------------------------------------


class ActorNewsPuller:
    """Fetch news mentions and bios for sector_map actors.

    Attributes:
        engine: SQLAlchemy engine for DB writes.
        session: requests.Session with UA header.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._last_call: dict[str, float] = {}

    # -----------------------------------------------------------------------
    # Rate limiting
    # -----------------------------------------------------------------------

    def _throttle(self, source: str) -> None:
        delay = _RATE_LIMIT.get(source, 0.5)
        now = time.time()
        last = self._last_call.get(source, 0.0)
        wait = delay - (now - last)
        if wait > 0:
            time.sleep(wait)
        self._last_call[source] = time.time()

    # -----------------------------------------------------------------------
    # 1. Google News RSS
    # -----------------------------------------------------------------------

    @retry_on_failure(max_attempts=2, backoff=1.5, retryable_exceptions=(requests.RequestException,))
    def _fetch_google_news(self, actor_id: str, query: str) -> list[NewsRow]:
        self._throttle("google_news")
        url = (
            "https://news.google.com/rss/search?"
            f"q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
        )
        try:
            resp = self.session.get(url, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
        except requests.RequestException as exc:
            log.warning("google_news fetch failed for {a}: {e}", a=actor_id, e=exc)
            return []

        rows: list[NewsRow] = []
        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as exc:
            log.warning("google_news XML parse failed for {a}: {e}", a=actor_id, e=exc)
            return []

        for item in root.iter("item"):
            title_el = item.find("title")
            link_el = item.find("link")
            desc_el = item.find("description")
            pub_el = item.find("pubDate")

            title = html.unescape((title_el.text or "").strip()) if title_el is not None else ""
            link = (link_el.text or "").strip() if link_el is not None else ""
            snippet = html.unescape(re.sub(r"<[^>]+>", " ", desc_el.text or "")) if desc_el is not None else ""
            snippet = re.sub(r"\s+", " ", snippet).strip()[:500]
            pub_at = parse_rfc822(pub_el.text or "") if pub_el is not None else None

            if not title or not link:
                continue

            blob = f"{title} {snippet}"
            rows.append(NewsRow(
                actor_id=actor_id,
                source="google_news",
                url=link,
                title=title[:500],
                snippet=snippet,
                published_at=pub_at,
                sentiment=score_sentiment(blob),
                stance_markers=extract_stance(blob),
                loyalty_signals=extract_loyalty(blob),
            ))
        return rows

    # -----------------------------------------------------------------------
    # 2. GDELT DOC API
    # -----------------------------------------------------------------------

    @retry_on_failure(max_attempts=2, backoff=1.5, retryable_exceptions=(requests.RequestException,))
    def _fetch_gdelt(self, actor_id: str, query: str) -> list[NewsRow]:
        self._throttle("gdelt")
        url = (
            "https://api.gdeltproject.org/api/v2/doc/doc"
            f"?query={quote_plus(query)}"
            "&format=json&mode=artlist&maxrecords=50&sort=datedesc"
        )
        try:
            resp = self.session.get(url, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json() if resp.text.strip() else {}
        except (requests.RequestException, ValueError) as exc:
            log.warning("gdelt fetch failed for {a}: {e}", a=actor_id, e=exc)
            return []

        rows: list[NewsRow] = []
        for art in payload.get("articles", []) or []:
            link = art.get("url", "")
            title = (art.get("title") or "").strip()
            if not title or not link:
                continue

            # GDELT date format: "20260411T123000Z"
            pub_at: datetime | None = None
            raw_date = art.get("seendate") or ""
            if raw_date:
                try:
                    pub_at = datetime.strptime(raw_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
                except ValueError:
                    pub_at = None

            tone_val: float | None = None
            try:
                tone_raw = art.get("tone")
                if tone_raw is not None and tone_raw != "":
                    tone_val = float(tone_raw) / 10.0  # GDELT tone ~[-10,10]
                    tone_val = max(-1.0, min(1.0, tone_val))
            except (TypeError, ValueError):
                tone_val = None

            blob = title
            rows.append(NewsRow(
                actor_id=actor_id,
                source="gdelt",
                url=link,
                title=title[:500],
                snippet=(art.get("domain") or "")[:500],
                published_at=pub_at,
                sentiment=tone_val if tone_val is not None else score_sentiment(blob),
                stance_markers=extract_stance(blob),
                loyalty_signals=extract_loyalty(blob),
            ))
        return rows

    # -----------------------------------------------------------------------
    # 3. Wikipedia REST (bio)
    # -----------------------------------------------------------------------

    @retry_on_failure(max_attempts=2, backoff=1.0, retryable_exceptions=(requests.RequestException,))
    def _fetch_wikipedia(self, actor_id: str, name: str) -> BioRow | None:
        self._throttle("wikipedia")
        # Use the page summary endpoint — resilient to disambiguation
        title = name.replace(" ", "_")
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{quote_plus(title)}"
        try:
            resp = self.session.get(url, timeout=_REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError) as exc:
            log.debug("wikipedia fetch failed for {a}: {e}", a=actor_id, e=exc)
            return None

        if data.get("type") == "disambiguation":
            return None

        extract = (data.get("extract") or "").strip()
        if not extract:
            return None

        page_url = data.get("content_urls", {}).get("desktop", {}).get("page") or f"https://en.wikipedia.org/wiki/{title}"
        loyalty = extract_loyalty(extract)
        affiliations: list[str] = []
        positions: list[str] = []
        for sig in loyalty:
            label, _, target = sig.partition(":")
            if label in ("former_employee", "board_member", "allied_with", "backed_by"):
                affiliations.append(target)
            if label in ("former_employee", "board_member"):
                positions.append(target)

        # Simple net worth extraction
        net_worth_usd: float | None = None
        m = re.search(r"net worth.*?\$?([\d\.]+)\s*(billion|million)", extract, re.I)
        if m:
            try:
                val = float(m.group(1))
                mult = 1e9 if m.group(2).lower() == "billion" else 1e6
                net_worth_usd = val * mult
            except ValueError:
                net_worth_usd = None

        return BioRow(
            actor_id=actor_id,
            wikipedia_url=page_url,
            wikipedia_extract=extract[:4000],
            affiliations=sorted(set(affiliations)),
            positions=sorted(set(positions)),
            net_worth_usd=net_worth_usd,
        )

    # -----------------------------------------------------------------------
    # 4. SEC EDGAR full-text
    # -----------------------------------------------------------------------

    @retry_on_failure(max_attempts=2, backoff=1.5, retryable_exceptions=(requests.RequestException,))
    def _fetch_sec_edgar(self, actor_id: str, query: str) -> list[NewsRow]:
        self._throttle("sec_edgar")
        quoted = '"' + query + '"'
        url = (
            "https://efts.sec.gov/LATEST/search-index?"
            f"q={quote_plus(quoted)}"
            "&dateRange=custom&startdt=2025-01-01&enddt=2026-12-31"
        )
        try:
            resp = self.session.get(url, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json() if resp.text.strip() else {}
        except (requests.RequestException, ValueError) as exc:
            log.debug("sec_edgar fetch failed for {a}: {e}", a=actor_id, e=exc)
            return []

        rows: list[NewsRow] = []
        hits = (payload.get("hits") or {}).get("hits") or []
        for hit in hits[:50]:
            src = hit.get("_source") or {}
            adsh = (src.get("adsh") or "").replace("-", "")
            cik = (src.get("ciks") or [""])[0] if src.get("ciks") else ""
            form = src.get("form", "")
            (src.get("file_type") or "").lower()
            # Reconstruct filing URL
            if cik and adsh:
                filing_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={form}"
            else:
                filing_url = f"https://efts.sec.gov/LATEST/search-index?q={quote_plus(query)}"

            title = f"{src.get('display_names', [''])[0] if src.get('display_names') else ''} {form}".strip()
            snippet = (src.get("file_description") or "")[:500]
            pub_at: datetime | None = None
            raw_date = src.get("file_date") or ""
            if raw_date:
                try:
                    pub_at = datetime.strptime(raw_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    pub_at = None

            if not title:
                continue

            blob = f"{title} {snippet}"
            rows.append(NewsRow(
                actor_id=actor_id,
                source="sec_edgar",
                url=f"{filing_url}#{adsh}",
                title=title[:500],
                snippet=snippet,
                published_at=pub_at,
                sentiment=score_sentiment(blob),
                stance_markers=extract_stance(blob),
                loyalty_signals=extract_loyalty(blob),
            ))
        return rows

    # -----------------------------------------------------------------------
    # 5. CrossRef (scholarly mentions)
    # -----------------------------------------------------------------------

    @retry_on_failure(max_attempts=2, backoff=1.5, retryable_exceptions=(requests.RequestException,))
    def _fetch_crossref(self, actor_id: str, query: str) -> list[NewsRow]:
        self._throttle("crossref")
        url = f"https://api.crossref.org/works?query={quote_plus(query)}&rows=20&sort=published&order=desc"
        try:
            resp = self.session.get(url, timeout=_REQUEST_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json() if resp.text.strip() else {}
        except (requests.RequestException, ValueError) as exc:
            log.debug("crossref fetch failed for {a}: {e}", a=actor_id, e=exc)
            return []

        rows: list[NewsRow] = []
        items = (payload.get("message") or {}).get("items") or []
        for item in items:
            title_parts = item.get("title") or []
            title = title_parts[0] if title_parts else ""
            link = item.get("URL", "")
            if not title or not link:
                continue

            pub_at: datetime | None = None
            pub = item.get("published", item.get("published-print") or item.get("published-online") or {})
            dparts = (pub or {}).get("date-parts") or [[]]
            if dparts and dparts[0]:
                try:
                    y = dparts[0][0]
                    m = dparts[0][1] if len(dparts[0]) > 1 else 1
                    d = dparts[0][2] if len(dparts[0]) > 2 else 1
                    pub_at = datetime(y, m, d, tzinfo=timezone.utc)
                except (TypeError, ValueError):
                    pub_at = None

            snippet = (item.get("container-title") or [""])[0][:500]
            blob = f"{title} {snippet}"
            rows.append(NewsRow(
                actor_id=actor_id,
                source="crossref",
                url=link,
                title=title[:500],
                snippet=snippet,
                published_at=pub_at,
                sentiment=score_sentiment(blob),
                stance_markers=extract_stance(blob),
                loyalty_signals=extract_loyalty(blob),
            ))
        return rows

    # -----------------------------------------------------------------------
    # Actor iteration + orchestration
    # -----------------------------------------------------------------------

    def _build_query(self, actor: dict[str, Any]) -> str:
        """Build a disambiguation query using name + sector context."""
        name = actor["name"]
        subsector = actor.get("subsector") or ""
        if subsector and actor.get("type") in ("person", "family_office", "activist", "regulator"):
            return f'"{name}" {subsector}'
        return f'"{name}"'

    def pull_one_actor(self, actor: dict[str, Any], sources: list[str]) -> dict[str, int]:
        """Pull all requested sources for a single actor.

        Returns per-source row counts.
        """
        actor_id = actor["actor_id"]
        query = self._build_query(actor)
        counts: dict[str, int] = {}

        all_rows: list[NewsRow] = []
        if "google_news" in sources:
            rs = self._fetch_google_news(actor_id, query)
            counts["google_news"] = len(rs)
            all_rows.extend(rs)
        if "gdelt" in sources:
            rs = self._fetch_gdelt(actor_id, query)
            counts["gdelt"] = len(rs)
            all_rows.extend(rs)
        if "sec_edgar" in sources:
            rs = self._fetch_sec_edgar(actor_id, actor["name"])
            counts["sec_edgar"] = len(rs)
            all_rows.extend(rs)
        if "crossref" in sources:
            rs = self._fetch_crossref(actor_id, actor["name"])
            counts["crossref"] = len(rs)
            all_rows.extend(rs)

        if all_rows:
            self._insert_news_rows(all_rows)

        if "wikipedia" in sources:
            bio = self._fetch_wikipedia(actor_id, actor["name"])
            if bio is not None:
                self._upsert_bio(bio)
                counts["wikipedia"] = 1
            else:
                counts["wikipedia"] = 0

        return counts

    # -----------------------------------------------------------------------
    # DB writes
    # -----------------------------------------------------------------------

    def _insert_news_rows(self, rows: list[NewsRow]) -> None:
        if not rows:
            return
        sql = text("""
            INSERT INTO actor_news (
                actor_id, source, url, title, snippet, published_at,
                sentiment, stance_markers, loyalty_signals, raw_content
            ) VALUES (
                :actor_id, :source, :url, :title, :snippet, :published_at,
                :sentiment, :stance_markers, :loyalty_signals, :raw_content
            )
            ON CONFLICT (actor_id, source, url) DO NOTHING
        """)
        with self.engine.begin() as conn:
            for r in rows:
                conn.execute(sql, {
                    "actor_id": r.actor_id,
                    "source": r.source,
                    "url": r.url,
                    "title": r.title,
                    "snippet": r.snippet,
                    "published_at": r.published_at,
                    "sentiment": r.sentiment,
                    "stance_markers": r.stance_markers or None,
                    "loyalty_signals": r.loyalty_signals or None,
                    "raw_content": r.raw_content or None,
                })

    def _upsert_bio(self, bio: BioRow) -> None:
        sql = text("""
            INSERT INTO actor_bio (
                actor_id, wikipedia_url, wikipedia_extract,
                affiliations, positions, net_worth_usd, updated_at
            ) VALUES (
                :actor_id, :wikipedia_url, :wikipedia_extract,
                :affiliations, :positions, :net_worth_usd, NOW()
            )
            ON CONFLICT (actor_id) DO UPDATE SET
                wikipedia_url = EXCLUDED.wikipedia_url,
                wikipedia_extract = EXCLUDED.wikipedia_extract,
                affiliations = EXCLUDED.affiliations,
                positions = EXCLUDED.positions,
                net_worth_usd = EXCLUDED.net_worth_usd,
                updated_at = NOW()
        """)
        with self.engine.begin() as conn:
            conn.execute(sql, {
                "actor_id": bio.actor_id,
                "wikipedia_url": bio.wikipedia_url,
                "wikipedia_extract": bio.wikipedia_extract,
                "affiliations": bio.affiliations or None,
                "positions": bio.positions or None,
                "net_worth_usd": bio.net_worth_usd,
            })


# ---------------------------------------------------------------------------
# Actor enumeration from sector_map
# ---------------------------------------------------------------------------


def enumerate_sector_map_actors(
    priority_only: bool = False,
) -> list[dict[str, Any]]:
    """Flatten sector_map into a list of actor dicts ready for pulling.

    Parameters:
        priority_only: If True, return only actors with weight >= 0.04.

    Returns:
        List of {actor_id, name, ticker, sector, subsector, type, weight}.
    """
    from analysis.sector_map import SECTOR_MAP

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for sector, sd in SECTOR_MAP.items():
        for subsector, sub in (sd.get("subsectors") or {}).items():
            for actor in sub.get("actors", []) or []:
                name = actor.get("name")
                if not name:
                    continue
                aid = slugify(name)
                if aid in seen:
                    continue
                seen.add(aid)
                weight = float(actor.get("weight", 0) or 0)
                if priority_only and weight < 0.04:
                    continue
                out.append({
                    "actor_id": aid,
                    "name": name,
                    "ticker": actor.get("ticker"),
                    "sector": sector,
                    "subsector": subsector,
                    "type": actor.get("type", "company"),
                    "weight": weight,
                    "description": actor.get("description", ""),
                })
    # Sort by weight desc so --limit gives the most important actors first
    out.sort(key=lambda a: a["weight"], reverse=True)
    return out
