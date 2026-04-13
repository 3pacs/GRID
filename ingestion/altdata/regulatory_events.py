"""
GRID regulatory enforcement events puller.

Polls public RSS/HTML feeds from US regulators for enforcement actions,
recalls, warning letters, press releases, and settlements. Each event is
stored in ``regulatory_events`` (staging table) AND projected into
``supply_chain_edges`` with ``relationship='regulatory_threat'``.

Sources (all free, public):

    fda         - FDA warning letters  (RSS, ATOM fallback)
    fda_recall  - FDA recalls / market withdrawals / safety alerts (RSS)
    ftc         - FTC press releases (RSS)
    doj         - DOJ antitrust press releases (HTML)
    sec         - SEC press releases (RSS)
    usda_fsis   - USDA FSIS meat/poultry recalls (RSS)
    cfpb        - CFPB press releases (RSS)
    epa         - EPA enforcement news (RSS)

Severity mapping (cf. spec):

    FDA warning letter   -> medium
    FDA class I recall   -> critical
    FDA class II recall  -> high
    FDA class III recall -> medium
    FTC settlement       -> medium
    DOJ indictment       -> high
    SEC enforcement      -> medium
    CFPB enforcement     -> medium
    EPA enforcement      -> medium
    USDA class I recall  -> critical

Each event writes a supply_chain_edges row for every affected ticker:

    upstream_id       = regulator slug ('fda', 'ftc', 'sec', ...)
    downstream_id     = ticker
    relationship      = 'regulatory_threat'
    chokepoint_score  = 0.9 critical / 0.7 high / 0.5 medium / 0.3 low
    confidence        = 'derived'
    source            = event URL
    as_of             = event_date

Scheduler: weekly (regulatory actions come in waves).
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Iterable

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.altdata.gov_contracts import CONTRACTOR_TICKER_MAP
from ingestion.base import BasePuller, retry_on_failure

# ── Config ────────────────────────────────────────────────────────────────────

_USER_AGENT: str = "GRID Intelligence ops@stepdad.finance"
_REQUEST_HEADERS: dict[str, str] = {
    "User-Agent": _USER_AGENT,
    "Accept": (
        "application/rss+xml, application/atom+xml, application/xml, "
        "text/xml, text/html;q=0.9, */*;q=0.8"
    ),
}
_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_SLEEP: float = 1.5  # be polite across .gov hosts

# Source registry. `parser` is one of: "rss" (feedparser) or "doj_html".
SOURCES: dict[str, dict[str, Any]] = {
    "fda": {
        "url": (
            "https://www.fda.gov/inspections-compliance-enforcement-and-"
            "criminal-investigations/compliance-actions-and-activities/"
            "warning-letters"
        ),
        "parser": "fda_warning_html",
        "action_type": "warning_letter",
        "default_severity": "medium",
    },
    "fda_recall": {
        "url": (
            "https://www.fda.gov/about-fda/contact-fda/"
            "stay-informed/rss-feeds/recalls/rss.xml"
        ),
        "parser": "rss",
        "action_type": "recall",
        "default_severity": "high",
    },
    "ftc": {
        "url": "https://www.ftc.gov/feeds/press-release.xml",
        "parser": "rss",
        "action_type": "press_release",
        "default_severity": "medium",
    },
    "sec": {
        "url": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=&company=&output=atom",
        "parser": "rss",
        "action_type": "enforcement",
        "default_severity": "medium",
    },
    "sec_press": {
        "url": "https://www.sec.gov/news/pressreleases.rss",
        "parser": "rss",
        "action_type": "press_release",
        "default_severity": "medium",
    },
    "usda_fsis": {
        "url": "https://www.fsis.usda.gov/fsis/api/recall/rss",
        "parser": "rss",
        "action_type": "recall",
        "default_severity": "high",
    },
    "cfpb": {
        "url": "https://www.consumerfinance.gov/about-us/newsroom/feed/",
        "parser": "rss",
        "action_type": "enforcement",
        "default_severity": "medium",
    },
    "epa": {
        "url": "https://www.epa.gov/newsreleases/search/rss",
        "parser": "rss",
        "action_type": "enforcement",
        "default_severity": "medium",
    },
    "doj": {
        "url": "https://www.justice.gov/atr/press-releases",
        "parser": "doj_html",
        "action_type": "press_release",
        "default_severity": "high",
    },
}

# Severity → chokepoint_score (0..1)
_SEVERITY_SCORE: dict[str, float] = {
    "low": 0.3,
    "medium": 0.5,
    "high": 0.7,
    "critical": 0.9,
}

# Regulator slug → pretty name for supply_chain_nodes insert
_REGULATOR_NAMES: dict[str, str] = {
    "fda": "U.S. Food and Drug Administration",
    "fda_recall": "U.S. Food and Drug Administration",
    "ftc": "U.S. Federal Trade Commission",
    "sec": "U.S. Securities and Exchange Commission",
    "sec_press": "U.S. Securities and Exchange Commission",
    "usda_fsis": "USDA Food Safety and Inspection Service",
    "cfpb": "Consumer Financial Protection Bureau",
    "epa": "U.S. Environmental Protection Agency",
    "doj": "U.S. Department of Justice (Antitrust Division)",
}

# Collapse duplicate slugs (fda and fda_recall both roll up to 'fda' regulator)
_REGULATOR_ROLLUP: dict[str, str] = {
    "fda_recall": "fda",
    "sec_press": "sec",
}


# ── Company name → ticker resolution ───────────────────────────────────────────
# Extends the shared CONTRACTOR_TICKER_MAP with consumer, food & financial firms
# that show up disproportionately in FDA, FTC, CFPB, USDA filings.

_EXTRA_TICKER_MAP: dict[str, str] = {
    # Food & consumer staples
    "kraft heinz": "KHC", "kraft": "KHC", "heinz": "KHC",
    "mondelez": "MDLZ", "mondelez international": "MDLZ",
    "general mills": "GIS",
    "kellogg": "K", "kellogg's": "K", "kellanova": "K",
    "tyson": "TSN", "tyson foods": "TSN",
    "hormel": "HRL", "hormel foods": "HRL",
    "conagra": "CAG", "conagra brands": "CAG",
    "campbell soup": "CPB", "campbell": "CPB",
    "hershey": "HSY", "the hershey company": "HSY",
    "nestle": "NSRGY", "nestle usa": "NSRGY",
    "coca-cola": "KO", "coca cola": "KO", "coke": "KO",
    "pepsi": "PEP", "pepsico": "PEP",
    "procter & gamble": "PG", "procter and gamble": "PG", "p&g": "PG",
    "unilever": "UL",
    "clorox": "CLX",
    "colgate": "CL", "colgate-palmolive": "CL",
    "kimberly-clark": "KMB", "kimberly clark": "KMB",
    "mcdonald's": "MCD", "mcdonalds": "MCD",
    "starbucks": "SBUX",
    "chipotle": "CMG",
    "yum brands": "YUM", "yum! brands": "YUM",
    "walmart": "WMT", "wal-mart": "WMT",
    "target corporation": "TGT", "target corp": "TGT",
    "costco": "COST",
    "kroger": "KR",
    "albertsons": "ACI",
    # Pharma / health (recalls, FDA enforcement)
    "abbott": "ABT", "abbott laboratories": "ABT",
    "bristol-myers squibb": "BMY", "bristol myers": "BMY",
    "novartis": "NVS",
    "sanofi": "SNY",
    "astrazeneca": "AZN",
    "gsk": "GSK", "glaxosmithkline": "GSK",
    "bayer": "BAYRY",
    "teva": "TEVA",
    "viatris": "VTRS",
    "mylan": "VTRS",
    "regeneron": "REGN",
    "vertex": "VRTX",
    "biogen": "BIIB",
    "cvs": "CVS", "cvs health": "CVS",
    "walgreens": "WBA", "walgreens boots alliance": "WBA",
    "rite aid": "RAD",
    "medtronic": "MDT",
    "boston scientific": "BSX",
    "stryker": "SYK",
    "intuitive surgical": "ISRG",
    "illumina": "ILMN",
    # Autos / EPA
    "volkswagen": "VWAGY",
    "toyota": "TM",
    "honda": "HMC",
    "stellantis": "STLA",
    "tesla": "TSLA",
    "ford motor company": "F",
    # Financials / SEC / CFPB
    "wells fargo": "WFC",
    "american express": "AXP",
    "capital one": "COF",
    "discover financial": "DFS", "discover": "DFS",
    "charles schwab": "SCHW", "schwab": "SCHW",
    "blackrock": "BLK",
    "blackstone": "BX",
    "kkr": "KKR",
    "apollo global": "APO", "apollo global management": "APO",
    "carlyle group": "CG",
    "robinhood": "HOOD",
    "coinbase": "COIN",
    "paypal": "PYPL",
    "block": "SQ", "square": "SQ",
    "affirm": "AFRM",
    "sofi": "SOFI",
    "upstart": "UPST",
    # Tech / FTC
    "amazon.com": "AMZN",
    "alphabet": "GOOGL", "google llc": "GOOGL",
    "meta platforms": "META", "facebook": "META", "instagram": "META",
    "microsoft corp": "MSFT",
    "apple inc": "AAPL",
    "nvidia corp": "NVDA",
    "intel corp": "INTC",
    "oracle corp": "ORCL",
    "adobe": "ADBE",
    "salesforce": "CRM",
    # Energy / EPA
    "exxonmobil": "XOM", "exxon": "XOM",
    "chevron corporation": "CVX", "chevron": "CVX",
    "bp p.l.c.": "BP", "bp plc": "BP",
    "shell plc": "SHEL", "royal dutch shell": "SHEL",
    "conocophillips": "COP",
    "marathon petroleum": "MPC",
    "valero": "VLO", "valero energy": "VLO",
    "phillips 66": "PSX",
    "occidental": "OXY", "occidental petroleum": "OXY",
    "duke energy": "DUK",
    "southern company": "SO",
    "dominion energy": "D",
    "exelon": "EXC",
    "pg&e": "PCG", "pacific gas and electric": "PCG",
    "next era": "NEE", "nextera energy": "NEE",
}

TICKER_MAP: dict[str, str] = {**CONTRACTOR_TICKER_MAP, **_EXTRA_TICKER_MAP}


def _match_ticker(text_blob: str) -> list[str]:
    """Extract all stock tickers mentioned in a blob of regulator prose.

    Strategy:
      1. Longest-first substring match against TICKER_MAP (case insensitive).
      2. De-duplicate while preserving order.

    Parameters:
        text_blob: Title + summary + raw content concatenated.

    Returns:
        List of ticker symbols (deduped, order preserved).
    """
    if not text_blob:
        return []
    blob = text_blob.lower()
    hits: list[str] = []
    seen: set[str] = set()
    for key in sorted(TICKER_MAP, key=len, reverse=True):
        if key in blob:
            tk = TICKER_MAP[key]
            if tk not in seen:
                seen.add(tk)
                hits.append(tk)
    return hits


def _strip_html(raw: str) -> str:
    """Remove tags and collapse whitespace — cheap, no BeautifulSoup needed."""
    if not raw:
        return ""
    no_tags = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", no_tags).strip()


def _infer_severity(
    title: str, summary: str, default: str,
) -> str:
    """Bump severity based on heuristic keywords in the title/summary."""
    blob = f"{title} {summary}".lower()
    if "class i recall" in blob or "life-threatening" in blob or "deaths" in blob:
        return "critical"
    if "class ii recall" in blob or "serious injury" in blob:
        return "high"
    if "class iii recall" in blob:
        return "medium"
    if "indictment" in blob or "criminal charges" in blob or "antitrust lawsuit" in blob:
        return "high"
    if "settlement" in blob or "consent order" in blob or "civil penalty" in blob:
        return "medium"
    return default


# ── Data classes ──────────────────────────────────────────────────────────────


@dataclass
class RegulatoryEvent:
    """One normalized regulatory action, source-agnostic."""

    regulator: str
    action_type: str
    event_date: date
    title: str
    summary: str
    url: str
    severity: str
    affected_tickers: list[str] = field(default_factory=list)
    raw_content: str = ""


@dataclass
class PullStats:
    """Per-source and aggregate counters for a single run."""

    events_by_source: dict[str, int] = field(default_factory=dict)
    tickers_resolved: int = 0
    tickers_skipped: int = 0
    edges_inserted: int = 0
    severity: dict[str, int] = field(default_factory=dict)

    def bump_source(self, src: str) -> None:
        self.events_by_source[src] = self.events_by_source.get(src, 0) + 1

    def bump_severity(self, sev: str) -> None:
        self.severity[sev] = self.severity.get(sev, 0) + 1

    def as_dict(self) -> dict[str, Any]:
        return {
            "events_by_source": dict(sorted(self.events_by_source.items())),
            "tickers_resolved": self.tickers_resolved,
            "tickers_skipped": self.tickers_skipped,
            "edges_inserted": self.edges_inserted,
            "severity": dict(sorted(self.severity.items())),
        }


# ── Puller ────────────────────────────────────────────────────────────────────


class RegulatoryEventsPuller(BasePuller):
    """Pulls enforcement actions from US regulator RSS/HTML feeds."""

    SOURCE_NAME: str = "REGULATORY_EVENTS"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://www.fda.gov/",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        log.info(
            "RegulatoryEventsPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ── HTTP ──────────────────────────────────────────────────────────────

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError,
                              requests.RequestException),
    )
    def _fetch(self, url: str) -> str:
        resp = requests.get(
            url, headers=_REQUEST_HEADERS, timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.text

    # ── Source parsers ────────────────────────────────────────────────────

    def _parse_rss(
        self, slug: str, cfg: dict[str, Any], limit: int,
    ) -> list[RegulatoryEvent]:
        """Parse an RSS / Atom feed into RegulatoryEvent objects."""
        try:
            import feedparser  # deferred: optional dep
        except ImportError:
            log.warning(
                "feedparser not installed — skipping RSS source {s}", s=slug,
            )
            return []

        try:
            raw = self._fetch(cfg["url"])
        except Exception as exc:
            log.warning("fetch failed for {s}: {e}", s=slug, e=str(exc))
            return []

        feed = feedparser.parse(raw)
        if getattr(feed, "bozo", 0) and not feed.entries:
            log.debug("RSS parse empty for {s}: {msg}", s=slug,
                      msg=str(getattr(feed, "bozo_exception", ""))[:120])
            return []

        out: list[RegulatoryEvent] = []
        reg = _REGULATOR_ROLLUP.get(slug, slug)
        for entry in feed.entries[:limit]:
            title = _strip_html(entry.get("title", ""))
            summary = _strip_html(
                entry.get("summary", "") or entry.get("description", "")
            )
            url = entry.get("link", "").strip()
            if not url or not title:
                continue

            # Date extraction — feedparser exposes published_parsed, else fall back
            event_date = _parse_date(entry)

            severity = _infer_severity(title, summary, cfg["default_severity"])
            blob = f"{title} . {summary}"
            tickers = _match_ticker(blob)

            out.append(RegulatoryEvent(
                regulator=reg,
                action_type=cfg["action_type"],
                event_date=event_date,
                title=title[:500],
                summary=summary[:2000],
                url=url,
                severity=severity,
                affected_tickers=tickers,
                raw_content=blob[:4000],
            ))
        return out

    def _parse_doj_html(
        self, slug: str, cfg: dict[str, Any], limit: int,
    ) -> list[RegulatoryEvent]:
        """Scrape DOJ antitrust press releases (no RSS available)."""
        try:
            raw = self._fetch(cfg["url"])
        except Exception as exc:
            log.warning("fetch failed for doj: {e}", e=str(exc))
            return []

        # DOJ press index: <a href="/opa/pr/..." rel="bookmark">
        #   <span class="field-formatter--string">Title</span>
        # </a>
        #
        # We pull anchor + any inner text (spans allowed) in a non-greedy match
        # and then strip tags before use.
        pattern = re.compile(
            r'<a[^>]+href="(?P<href>/opa/pr/[^"]+)"[^>]*>'
            r'(?P<inner>.{10,400}?)</a>',
            re.IGNORECASE | re.DOTALL,
        )
        out: list[RegulatoryEvent] = []
        seen: set[str] = set()
        for m in pattern.finditer(raw):
            if len(out) >= limit:
                break
            href = m.group("href").strip()
            if href in seen:
                continue
            seen.add(href)
            title = _strip_html(m.group("inner"))
            if not href or len(title) < 10:
                continue
            url = "https://www.justice.gov" + href
            severity = _infer_severity(title, "", cfg["default_severity"])
            tickers = _match_ticker(title)
            out.append(RegulatoryEvent(
                regulator=slug,
                action_type=cfg["action_type"],
                event_date=date.today(),  # DOJ index has no per-item date in HTML
                title=title[:500],
                summary="",
                url=url,
                severity=severity,
                affected_tickers=tickers,
                raw_content=title[:4000],
            ))
        return out

    def _parse_fda_warning_html(
        self, slug: str, cfg: dict[str, Any], limit: int,
    ) -> list[RegulatoryEvent]:
        """Scrape the FDA warning letters index page (no RSS available).

        The page structure uses anchor slugs that encode the letter date
        as MMDDYYYY at the end, e.g.::

            /warning-letters/company-name-721088-03312026

        We take the 6-8 digit "case number" as disambiguator and extract
        the trailing date token for event_date.
        """
        try:
            raw = self._fetch(cfg["url"])
        except Exception as exc:
            log.warning("fetch failed for fda warning letters: {e}", e=str(exc))
            return []

        # Anchor pattern: href=".../warning-letters/<slug>-<case#>-<MMDDYYYY>"
        pattern = re.compile(
            r'href="(?P<href>/inspections-compliance-enforcement-and-'
            r'criminal-investigations/warning-letters/'
            r'(?P<slugtail>[a-z0-9-]*?)(?P<date>\d{8}))"',
            re.IGNORECASE,
        )
        # Title pattern: text content inside <a> — fall back to company slug.
        title_pattern = re.compile(
            r'<a[^>]+href="(?P<href>/inspections-compliance-enforcement-and-'
            r'criminal-investigations/warning-letters/[a-z0-9-]+?\d{8})"[^>]*>'
            r'(?P<title>[^<]{3,400})</a>',
            re.IGNORECASE,
        )
        titles: dict[str, str] = {}
        for m in title_pattern.finditer(raw):
            titles[m.group("href")] = _strip_html(m.group("title"))

        seen_urls: set[str] = set()
        out: list[RegulatoryEvent] = []
        for m in pattern.finditer(raw):
            if len(out) >= limit:
                break
            href = m.group("href")
            if href in seen_urls:
                continue
            seen_urls.add(href)
            url = "https://www.fda.gov" + href
            # Parse date from trailing MMDDYYYY token.
            try:
                event_date = datetime.strptime(m.group("date"), "%m%d%Y").date()
            except ValueError:
                event_date = date.today()
            # Title: prefer <a> text; else humanise the slug tail.
            slug_tail = m.group("slugtail").rstrip("-")
            title = titles.get(href) or slug_tail.replace("-", " ").title()
            severity = _infer_severity(
                title, "", cfg["default_severity"],
            )
            tickers = _match_ticker(title)
            out.append(RegulatoryEvent(
                regulator=slug,
                action_type=cfg["action_type"],
                event_date=event_date,
                title=title[:500],
                summary="",
                url=url,
                severity=severity,
                affected_tickers=tickers,
                raw_content=title[:4000],
            ))
        return out

    def _parse_source(
        self, slug: str, cfg: dict[str, Any], limit: int,
    ) -> list[RegulatoryEvent]:
        parser = cfg["parser"]
        if parser == "rss":
            return self._parse_rss(slug, cfg, limit)
        if parser == "doj_html":
            return self._parse_doj_html(slug, cfg, limit)
        if parser == "fda_warning_html":
            return self._parse_fda_warning_html(slug, cfg, limit)
        log.warning("unknown parser {p} for {s}", p=parser, s=slug)
        return []

    # ── Persistence ───────────────────────────────────────────────────────

    def _upsert_regulator_node(self, conn: Any, regulator: str) -> None:
        """Ensure the regulator has a supply_chain_nodes entry."""
        name = _REGULATOR_NAMES.get(regulator, regulator.upper())
        conn.execute(
            text(
                """
                INSERT INTO supply_chain_nodes (id, name, type, country, notes)
                VALUES (:id, :name, 'regulator', 'US', :notes)
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {"id": regulator, "name": name,
             "notes": "auto-seeded by regulatory_events puller"},
        )

    def _insert_event(
        self, conn: Any, event: RegulatoryEvent,
    ) -> bool:
        """Insert into regulatory_events. Returns True on fresh insert."""
        res = conn.execute(
            text(
                """
                INSERT INTO regulatory_events (
                    regulator, action_type, event_date, title, summary,
                    url, severity, affected_tickers, raw_content
                ) VALUES (
                    :regulator, :action_type, :event_date, :title, :summary,
                    :url, :severity, :tickers, :raw
                )
                ON CONFLICT (url) DO NOTHING
                RETURNING id
                """
            ),
            {
                "regulator": event.regulator,
                "action_type": event.action_type,
                "event_date": event.event_date,
                "title": event.title,
                "summary": event.summary,
                "url": event.url,
                "severity": event.severity,
                "tickers": event.affected_tickers,
                "raw": event.raw_content,
            },
        )
        return res.fetchone() is not None

    def _insert_edge(
        self, conn: Any, event: RegulatoryEvent, ticker: str,
    ) -> bool:
        """Insert a regulatory_threat edge into supply_chain_edges."""
        score = _SEVERITY_SCORE.get(event.severity, 0.5)
        res = conn.execute(
            text(
                """
                INSERT INTO supply_chain_edges (
                    upstream_id, downstream_id, relationship, tier,
                    input_type, chokepoint_score, confidence, as_of, source
                ) VALUES (
                    :upstream, :downstream, 'regulatory_threat', 1,
                    :input_type, :score, 'derived', :as_of, :source
                )
                ON CONFLICT (upstream_id, downstream_id, relationship, as_of)
                DO UPDATE SET
                    chokepoint_score = GREATEST(
                        supply_chain_edges.chokepoint_score,
                        EXCLUDED.chokepoint_score
                    ),
                    source = EXCLUDED.source
                RETURNING id
                """
            ),
            {
                "upstream": event.regulator,
                "downstream": ticker,
                "input_type": event.action_type,
                "score": score,
                "as_of": event.event_date,
                "source": event.url,
            },
        )
        return res.fetchone() is not None

    def _ensure_ticker_node(self, conn: Any, ticker: str) -> None:
        conn.execute(
            text(
                """
                INSERT INTO supply_chain_nodes (id, name, type)
                VALUES (:id, :name, 'ticker')
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {"id": ticker, "name": ticker},
        )

    # ── Public entrypoint ─────────────────────────────────────────────────

    def pull(
        self,
        sources: Iterable[str] | None = None,
        limit_per_source: int = 50,
    ) -> dict[str, Any]:
        """Poll every configured source and persist events + edges.

        Parameters:
            sources: Optional subset of source slugs. Default: all.
            limit_per_source: Max events per feed to process this run.

        Returns:
            Summary stats dict (see PullStats.as_dict).
        """
        selected = list(sources) if sources else list(SOURCES.keys())
        stats = PullStats()

        for slug in selected:
            cfg = SOURCES.get(slug)
            if cfg is None:
                log.warning("unknown source slug {s}", s=slug)
                continue

            log.info("regulatory_events: polling {s}", s=slug)
            events = self._parse_source(slug, cfg, limit_per_source)
            log.info(
                "regulatory_events: {s} -> {n} events",
                s=slug, n=len(events),
            )

            with self.engine.begin() as conn:
                for event in events:
                    self._upsert_regulator_node(conn, event.regulator)
                    fresh = self._insert_event(conn, event)
                    if fresh:
                        stats.bump_source(event.regulator)
                        stats.bump_severity(event.severity)

                    if not event.affected_tickers:
                        stats.tickers_skipped += 1
                        continue

                    for ticker in event.affected_tickers:
                        self._ensure_ticker_node(conn, ticker)
                        inserted = self._insert_edge(conn, event, ticker)
                        if inserted:
                            stats.edges_inserted += 1
                            stats.tickers_resolved += 1

            time.sleep(_RATE_LIMIT_SLEEP)

        summary = stats.as_dict()
        log.info("regulatory_events run complete: {s}", s=summary)
        return {"status": "SUCCESS", **summary}


# ── Helpers ───────────────────────────────────────────────────────────────────


def _parse_date(entry: Any) -> date:
    """Extract a date from a feedparser entry, falling back to today."""
    for field_name in ("published_parsed", "updated_parsed", "created_parsed"):
        raw = entry.get(field_name) if hasattr(entry, "get") else None
        if raw is None:
            raw = getattr(entry, field_name, None)
        if raw:
            try:
                return datetime(*raw[:6], tzinfo=timezone.utc).date()
            except (TypeError, ValueError):
                continue
    for field_name in ("published", "updated", "created"):
        raw = entry.get(field_name) if hasattr(entry, "get") else None
        if not raw:
            continue
        for fmt in (
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d",
        ):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
    return date.today()


def run_weekly(db_engine: Engine | None = None) -> dict[str, Any]:
    """Hermes operator entrypoint — polls all sources once per week."""
    if db_engine is None:
        from db import get_engine

        db_engine = get_engine()
    puller = RegulatoryEventsPuller(db_engine=db_engine)
    return puller.pull(limit_per_source=50)


__all__ = [
    "RegulatoryEventsPuller",
    "RegulatoryEvent",
    "PullStats",
    "SOURCES",
    "TICKER_MAP",
    "run_weekly",
]
