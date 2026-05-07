"""
GRID Intelligence — SEC Filing Content Extractor.

Parses 8-K and other SEC filings for material facts, extracting
structured data from the raw text that the edgar_transcripts puller
stores in raw_series.

8-K Item Types (the SEC requires companies to report these):
    Item 1.01 — Entry into a material definitive agreement
    Item 1.02 — Termination of a material definitive agreement
    Item 1.03 — Bankruptcy or receivership
    Item 2.01 — Completion of acquisition or disposition of assets
    Item 2.02 — Results of operations and financial condition (earnings)
    Item 2.03 — Creation of a direct financial obligation
    Item 2.04 — Triggering events that accelerate obligations
    Item 2.05 — Costs associated with exit or disposal activities (layoffs)
    Item 2.06 — Material impairments
    Item 3.01 — Notice of delisting or failure to satisfy listing standard
    Item 4.01 — Changes in registrant's certifying accountant
    Item 4.02 — Non-reliance on previously issued financial statements
    Item 5.01 — Changes in control of registrant
    Item 5.02 — Departure/election of directors or officers
    Item 5.03 — Amendments to articles of incorporation
    Item 7.01 — Regulation FD disclosure
    Item 8.01 — Other events
    Item 9.01 — Financial statements and exhibits

Each extracted fact is classified by:
    - Item type (SEC 8-K item number)
    - Market impact direction (bullish/bearish/neutral)
    - Estimated magnitude (bps)
    - Dollar values mentioned
    - Named entities (companies, people)

Sources: raw_series where series_id LIKE 'edgar:8k:%'
Schedule: Runs after edgar_transcripts puller
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────

# 8-K item patterns and their significance
ITEM_TYPES: dict[str, dict[str, Any]] = {
    "1.01": {
        "name": "Material Definitive Agreement",
        "direction": "neutral",
        "typical_bps": 150,
        "keywords": ["agreement", "contract", "license", "partnership"],
    },
    "1.02": {
        "name": "Termination of Agreement",
        "direction": "bearish",
        "typical_bps": 100,
        "keywords": ["terminat", "cancel", "void"],
    },
    "1.03": {
        "name": "Bankruptcy or Receivership",
        "direction": "bearish",
        "typical_bps": 1000,
        "keywords": ["bankrupt", "chapter 11", "chapter 7", "receivership"],
    },
    "2.01": {
        "name": "Acquisition or Disposition",
        "direction": "neutral",
        "typical_bps": 500,
        "keywords": ["acqui", "dispos", "purchas", "sold", "divest"],
    },
    "2.02": {
        "name": "Results of Operations (Earnings)",
        "direction": "neutral",
        "typical_bps": 300,
        "keywords": ["revenue", "earnings", "net income", "EPS", "results of operations"],
    },
    "2.03": {
        "name": "Direct Financial Obligation",
        "direction": "neutral",
        "typical_bps": 80,
        "keywords": ["credit facilit", "loan", "debt", "borrow", "note"],
    },
    "2.05": {
        "name": "Exit/Disposal Activities (Restructuring)",
        "direction": "bearish",
        "typical_bps": 200,
        "keywords": ["restructur", "layoff", "workforce reduc", "exit", "close"],
    },
    "2.06": {
        "name": "Material Impairments",
        "direction": "bearish",
        "typical_bps": 250,
        "keywords": ["impair", "writedown", "write-down", "goodwill", "charge"],
    },
    "3.01": {
        "name": "Delisting Notice",
        "direction": "bearish",
        "typical_bps": 500,
        "keywords": ["delist", "listing standard", "non-compliance"],
    },
    "4.02": {
        "name": "Non-Reliance on Financial Statements",
        "direction": "bearish",
        "typical_bps": 400,
        "keywords": ["restate", "non-reliance", "material weakness", "error"],
    },
    "5.01": {
        "name": "Changes in Control",
        "direction": "neutral",
        "typical_bps": 600,
        "keywords": ["change of control", "change in control", "tender offer"],
    },
    "5.02": {
        "name": "Director/Officer Departure or Election",
        "direction": "neutral",
        "typical_bps": 80,
        "keywords": ["resign", "appoint", "elect", "depart", "retire", "CEO", "CFO", "director"],
    },
    "7.01": {
        "name": "Regulation FD Disclosure",
        "direction": "neutral",
        "typical_bps": 50,
        "keywords": ["regulation fd", "investor presentation", "conference"],
    },
    "8.01": {
        "name": "Other Events",
        "direction": "neutral",
        "typical_bps": 30,
        "keywords": [],
    },
}

# Item number extraction pattern
_ITEM_PATTERN = re.compile(
    r"Item\s+(\d+\.\d{2})", re.I,
)

# Dollar value extraction
_DOLLAR_RE = re.compile(
    r"\$\s*([\d,.]+)\s*(billion|million|B|M|bn|mn|trillion|T|thousand)?\b", re.I,
)

_MULTIPLIERS = {
    "trillion": 1e12, "t": 1e12,
    "billion": 1e9, "b": 1e9, "bn": 1e9,
    "million": 1e6, "m": 1e6, "mn": 1e6,
    "thousand": 1e3,
}

# Percentage extraction
_PCT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*%")

# Named entity patterns (company names)
_COMPANY_RE = re.compile(
    r"(?:(?:Inc|Corp|Corporation|LLC|Ltd|LP|Co|Group|Holdings|International|Technologies|Therapeutics|Pharmaceuticals|Biosciences)\.?)",
    re.I,
)


# ── Data Classes ─────────────────────────────────────────────────────────

@dataclass
class MaterialFact:
    """A material fact extracted from an SEC filing."""
    fact_id: str
    ticker: str
    filing_date: date
    item_number: str              # e.g. "2.01"
    item_name: str                # human-readable name
    description: str              # extracted text describing the fact
    direction: str                # bullish/bearish/neutral
    estimated_bps: int
    dollar_values: list[float]    # extracted dollar amounts
    percentages: list[float]      # extracted percentages
    key_entities: list[str]       # company/person names
    confidence: float
    raw_text: str                 # raw filing text for this item
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["filing_date"] = self.filing_date.isoformat()
        return d


# ── Main Extractor ───────────────────────────────────────────────────────

class SECFilingExtractor:
    """Extract material facts from SEC 8-K filings.

    Parses the raw filing text stored by edgar_transcripts in raw_series,
    identifies 8-K item types, and extracts structured data (dollar values,
    percentages, entities, direction).

    Attributes:
        engine: SQLAlchemy engine.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create sec_material_facts table if it doesn't exist."""
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS sec_material_facts (
                        id SERIAL PRIMARY KEY,
                        fact_id TEXT UNIQUE NOT NULL,
                        ticker TEXT NOT NULL,
                        filing_date DATE NOT NULL,
                        item_number TEXT NOT NULL,
                        item_name TEXT NOT NULL,
                        description TEXT DEFAULT '',
                        direction TEXT DEFAULT 'neutral',
                        estimated_bps INTEGER DEFAULT 0,
                        dollar_values JSONB DEFAULT '[]',
                        percentages JSONB DEFAULT '[]',
                        key_entities JSONB DEFAULT '[]',
                        confidence DOUBLE PRECISION DEFAULT 0.5,
                        raw_text TEXT DEFAULT '',
                        metadata JSONB DEFAULT '{}',
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_sec_facts_ticker
                    ON sec_material_facts (ticker)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_sec_facts_item
                    ON sec_material_facts (item_number)
                """))
                conn.execute(text("""
                    CREATE INDEX IF NOT EXISTS idx_sec_facts_date
                    ON sec_material_facts (filing_date DESC)
                """))
        except Exception as exc:
            log.warning("Failed to create sec_material_facts table: {e}", e=str(exc))

    def extract_from_text(
        self,
        ticker: str,
        filing_date: date,
        filing_text: str,
    ) -> list[MaterialFact]:
        """Extract material facts from a filing's text.

        Args:
            ticker: Ticker symbol.
            filing_date: Date of the filing.
            filing_text: Raw filing text.

        Returns:
            List of MaterialFact objects.
        """
        if len(filing_text) < 50:
            return []

        # Find all item numbers mentioned
        item_matches = list(_ITEM_PATTERN.finditer(filing_text))
        if not item_matches:
            # Try to classify from content keywords
            return self._classify_by_content(ticker, filing_date, filing_text)

        facts: list[MaterialFact] = []

        for i, match in enumerate(item_matches):
            item_num = match.group(1)
            if item_num not in ITEM_TYPES:
                continue

            item_info = ITEM_TYPES[item_num]

            # Extract text for this item (until next item or end)
            start = match.start()
            if i + 1 < len(item_matches):
                end = item_matches[i + 1].start()
            else:
                end = min(start + 5000, len(filing_text))

            item_text = filing_text[start:end]

            # Extract structured data
            dollar_values = self._extract_dollars(item_text)
            percentages = self._extract_percentages(item_text)
            entities = self._extract_entities(item_text)
            direction = self._infer_direction(item_num, item_text)

            # Build description from first ~300 chars after the item header
            desc_text = item_text[len(match.group(0)):].strip()[:300]

            confidence = self._compute_confidence(
                item_num, dollar_values, percentages, entities, desc_text,
            )

            fact_id = hashlib.sha256(
                f"sec:{ticker}:{filing_date}:{item_num}".encode()
            ).hexdigest()[:20]

            facts.append(MaterialFact(
                fact_id=fact_id,
                ticker=ticker.upper(),
                filing_date=filing_date,
                item_number=item_num,
                item_name=item_info["name"],
                description=desc_text,
                direction=direction,
                estimated_bps=item_info["typical_bps"],
                dollar_values=dollar_values,
                percentages=percentages,
                key_entities=entities,
                confidence=confidence,
                raw_text=item_text[:2000],
            ))

        return facts

    def _classify_by_content(
        self,
        ticker: str,
        filing_date: date,
        filing_text: str,
    ) -> list[MaterialFact]:
        """Classify filing content when no item numbers are found.

        Falls back to keyword matching against ITEM_TYPES definitions.

        Args:
            ticker: Ticker symbol.
            filing_date: Date of the filing.
            filing_text: Raw filing text.

        Returns:
            List of MaterialFact objects.
        """
        text_lower = filing_text.lower()
        facts: list[MaterialFact] = []

        for item_num, item_info in ITEM_TYPES.items():
            keywords = item_info.get("keywords", [])
            if not keywords:
                continue

            matched = any(kw in text_lower for kw in keywords)
            if not matched:
                continue

            dollar_values = self._extract_dollars(filing_text)
            percentages = self._extract_percentages(filing_text)
            entities = self._extract_entities(filing_text)
            direction = self._infer_direction(item_num, filing_text)

            fact_id = hashlib.sha256(
                f"sec:content:{ticker}:{filing_date}:{item_num}".encode()
            ).hexdigest()[:20]

            facts.append(MaterialFact(
                fact_id=fact_id,
                ticker=ticker.upper(),
                filing_date=filing_date,
                item_number=item_num,
                item_name=item_info["name"],
                description=filing_text[:300],
                direction=direction,
                estimated_bps=item_info["typical_bps"],
                dollar_values=dollar_values,
                percentages=percentages,
                key_entities=entities,
                confidence=0.35,  # Lower confidence for keyword-only match
                raw_text=filing_text[:2000],
                metadata={"classification": "keyword_only"},
            ))

        return facts

    def _extract_dollars(self, text: str) -> list[float]:
        """Extract dollar values from text."""
        values: list[float] = []
        for match in _DOLLAR_RE.finditer(text):
            try:
                raw = float(match.group(1).replace(",", ""))
                mult_text = (match.group(2) or "").lower()
                mult = _MULTIPLIERS.get(mult_text, 1.0)
                values.append(raw * mult)
            except (ValueError, TypeError):
                continue
        return values[:10]

    def _extract_percentages(self, text: str) -> list[float]:
        """Extract percentage values from text."""
        values: list[float] = []
        for match in _PCT_RE.finditer(text):
            try:
                val = float(match.group(1))
                if 0 < val < 10000:  # sanity check
                    values.append(val)
            except (ValueError, TypeError):
                continue
        return values[:10]

    def _extract_entities(self, text: str) -> list[str]:
        """Extract company/entity names from text."""
        entities: set[str] = set()

        # Find company name patterns
        # Look for capitalized words before Inc/Corp/LLC etc.
        pattern = re.compile(
            r"([A-Z][\w\s&.']{2,50})\s+(?:Inc|Corp|Corporation|LLC|Ltd|LP|Co|Group|Holdings)\.?",
        )
        for match in pattern.finditer(text):
            name = match.group(1).strip()
            if len(name) > 2 and len(name) < 80:
                entities.add(name)

        return list(entities)[:10]

    def _infer_direction(self, item_num: str, text: str) -> str:
        """Infer market direction from item type and text content."""
        item_info = ITEM_TYPES.get(item_num, {})
        default_direction = item_info.get("direction", "neutral")

        text_lower = text.lower()

        # For neutral-default items, use text analysis
        if default_direction == "neutral":
            bullish_words = sum(
                1 for w in ["growth", "increase", "beat", "exceed", "strong",
                            "record", "approv", "award"]
                if w in text_lower
            )
            bearish_words = sum(
                1 for w in ["decline", "loss", "miss", "weak", "impair",
                            "restructur", "layoff", "terminat", "default"]
                if w in text_lower
            )

            if bullish_words > bearish_words + 1:
                return "bullish"
            elif bearish_words > bullish_words + 1:
                return "bearish"

        return default_direction

    def _compute_confidence(
        self,
        item_num: str,
        dollar_values: list[float],
        percentages: list[float],
        entities: list[str],
        description: str,
    ) -> float:
        """Compute extraction confidence."""
        conf = 0.5  # Base for item-number match

        if dollar_values:
            conf += 0.15
        if percentages:
            conf += 0.05
        if entities:
            conf += 0.1
        if len(description) > 100:
            conf += 0.1

        # High-impact items get higher base confidence
        if item_num in ("1.03", "2.01", "4.02", "5.01"):
            conf += 0.1

        return min(1.0, round(conf, 3))

    def _store_facts(self, facts: list[MaterialFact]) -> int:
        """Persist facts to the database.

        Args:
            facts: List of MaterialFact objects.

        Returns:
            Number stored.
        """
        stored = 0
        with self.engine.begin() as conn:
            for fact in facts:
                try:
                    conn.execute(
                        text("""
                            INSERT INTO sec_material_facts
                            (fact_id, ticker, filing_date, item_number, item_name,
                             description, direction, estimated_bps, dollar_values,
                             percentages, key_entities, confidence, raw_text,
                             metadata)
                            VALUES
                            (:fid, :ticker, :fdate, :inum, :iname,
                             :desc, :dir, :bps, :dvals,
                             :pcts, :ents, :conf, :raw, :meta)
                            ON CONFLICT (fact_id) DO NOTHING
                        """),
                        {
                            "fid": fact.fact_id,
                            "ticker": fact.ticker,
                            "fdate": fact.filing_date,
                            "inum": fact.item_number,
                            "iname": fact.item_name,
                            "desc": fact.description,
                            "dir": fact.direction,
                            "bps": fact.estimated_bps,
                            "dvals": json.dumps(fact.dollar_values),
                            "pcts": json.dumps(fact.percentages),
                            "ents": json.dumps(fact.key_entities),
                            "conf": fact.confidence,
                            "raw": fact.raw_text,
                            "meta": json.dumps(fact.metadata, default=str),
                        },
                    )
                    stored += 1
                except Exception as exc:
                    log.debug("Failed to store fact: {e}", e=str(exc))

        return stored

    def run_extraction(
        self,
        tickers: list[str] | None = None,
        days_back: int = 90,
    ) -> dict[str, Any]:
        """Run extraction on all recent filings in raw_series.

        Args:
            tickers: Optional ticker filter.
            days_back: Days to look back.

        Returns:
            Summary dict.
        """
        cutoff = (date.today() - timedelta(days=days_back)).isoformat()

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT series_id, obs_date, raw_payload
                        FROM raw_series
                        WHERE series_id LIKE 'edgar:8k:%'
                          AND obs_date >= :cutoff
                        ORDER BY obs_date DESC
                    """),
                    {"cutoff": cutoff},
                ).fetchall()
        except Exception as exc:
            log.warning("SEC extraction query failed: {e}", e=str(exc))
            return {"error": str(exc)}

        all_facts: list[MaterialFact] = []

        for row in rows:
            payload = row[2] if isinstance(row[2], dict) else {}
            ticker = payload.get("ticker", "")
            if tickers and ticker not in tickers:
                continue

            filing_text = payload.get("text_preview", "")
            if len(filing_text) < 50:
                continue

            obs_date = row[1]
            facts = self.extract_from_text(ticker, obs_date, filing_text)
            all_facts.extend(facts)

        stored = self._store_facts(all_facts)

        # Count by item type
        by_item: dict[str, int] = {}
        by_direction: dict[str, int] = {}
        for f in all_facts:
            by_item[f.item_number] = by_item.get(f.item_number, 0) + 1
            by_direction[f.direction] = by_direction.get(f.direction, 0) + 1

        result = {
            "filings_scanned": len(rows),
            "facts_extracted": len(all_facts),
            "facts_stored": stored,
            "by_item": by_item,
            "by_direction": by_direction,
        }

        log.info(
            "SEC extraction: {f} filings → {n} facts, {s} stored",
            f=len(rows), n=len(all_facts), s=stored,
        )

        return result

    def get_recent_facts(
        self,
        ticker: str | None = None,
        item_number: str | None = None,
        days: int = 30,
    ) -> list[dict[str, Any]]:
        """Get recent material facts from the database.

        Args:
            ticker: Optional ticker filter.
            item_number: Optional item number filter.
            days: Days to look back.

        Returns:
            List of fact dicts.
        """
        cutoff = date.today() - timedelta(days=days)

        conditions = ["filing_date >= :cutoff"]
        params: dict[str, Any] = {"cutoff": cutoff}

        if ticker:
            conditions.append("ticker = :ticker")
            params["ticker"] = ticker.upper()
        if item_number:
            conditions.append("item_number = :inum")
            params["inum"] = item_number

        where_clause = " AND ".join(conditions)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(f"""
                        SELECT fact_id, ticker, filing_date, item_number,
                               item_name, description, direction, estimated_bps,
                               dollar_values, percentages, key_entities,
                               confidence, metadata
                        FROM sec_material_facts
                        WHERE {where_clause}
                        ORDER BY filing_date DESC, estimated_bps DESC
                        LIMIT 100
                    """),
                    params,
                ).fetchall()
        except Exception as exc:
            log.warning("Recent facts query failed: {e}", e=str(exc))
            return []

        return [
            {
                "fact_id": r[0],
                "ticker": r[1],
                "filing_date": r[2].isoformat() if r[2] else None,
                "item_number": r[3],
                "item_name": r[4],
                "description": r[5],
                "direction": r[6],
                "estimated_bps": r[7],
                "dollar_values": r[8],
                "percentages": r[9],
                "key_entities": r[10],
                "confidence": r[11],
                "metadata": r[12],
            }
            for r in rows
        ]

    def get_high_impact_facts(self, days: int = 7) -> list[dict[str, Any]]:
        """Get high-impact material facts (>= 200 bps).

        Args:
            days: Days to look back.

        Returns:
            List of high-impact fact dicts.
        """
        cutoff = date.today() - timedelta(days=days)

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT fact_id, ticker, filing_date, item_number,
                               item_name, description, direction, estimated_bps,
                               dollar_values, confidence
                        FROM sec_material_facts
                        WHERE filing_date >= :cutoff
                          AND estimated_bps >= 200
                        ORDER BY estimated_bps DESC, filing_date DESC
                        LIMIT 50
                    """),
                    {"cutoff": cutoff},
                ).fetchall()
        except Exception as exc:
            log.warning("High impact facts query failed: {e}", e=str(exc))
            return []

        return [
            {
                "fact_id": r[0],
                "ticker": r[1],
                "filing_date": r[2].isoformat() if r[2] else None,
                "item_number": r[3],
                "item_name": r[4],
                "description": r[5],
                "direction": r[6],
                "estimated_bps": r[7],
                "dollar_values": r[8],
                "confidence": r[9],
            }
            for r in rows
        ]
