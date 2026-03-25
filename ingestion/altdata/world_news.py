"""
GRID WorldNewsAPI ingestion module.

Pulls global news data from WorldNewsAPI (https://worldnewsapi.com) with
structured taxonomy-based parsing. Extracts article counts, sentiment
scores, and geographic distribution across macro-relevant categories.

Taxonomy:
    GEOPOLITICAL  — war, sanctions, diplomacy, treaties, territorial disputes
    MONETARY      — central banks, rate decisions, QE/QT, inflation targeting
    FISCAL        — government spending, taxation, debt, stimulus, austerity
    TRADE         — tariffs, trade agreements, supply chain, exports/imports
    ENERGY        — oil, gas, renewables, OPEC, energy policy, grid
    LABOR         — employment, strikes, wages, immigration, automation
    FINANCIAL     — banking, credit, markets, IPOs, defaults, regulation
    TECHNOLOGY    — AI, semiconductors, big tech, cybersecurity, crypto regulation
    CLIMATE       — extreme weather, emissions, carbon policy, disasters
    HEALTH        — pandemics, pharma, FDA, healthcare policy, drug pricing

Each category produces three features:
    {category}_article_count   — daily article volume (activity signal)
    {category}_sentiment_avg   — average sentiment (-1 to +1)
    {category}_global_spread   — number of distinct source countries (breadth)

Plus composite features:
    world_news_total_volume    — total articles across all categories
    world_news_fear_index      — negative-sentiment-weighted volume
    world_news_breadth         — total distinct source countries
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── Taxonomy ─────────────────────────────────────────────────────────────

NEWS_TAXONOMY: dict[str, dict[str, Any]] = {
    "geopolitical": {
        "query": "war OR sanctions OR diplomacy OR NATO OR military OR invasion OR ceasefire OR treaty",
        "family": "sentiment",
        "description": "Geopolitical conflict and diplomacy",
    },
    "monetary": {
        "query": "central bank OR interest rate OR Federal Reserve OR ECB OR inflation OR quantitative easing OR rate hike OR rate cut",
        "family": "sentiment",
        "description": "Central bank policy and monetary conditions",
    },
    "fiscal": {
        "query": "government spending OR stimulus OR budget deficit OR national debt OR austerity OR tax reform OR fiscal policy",
        "family": "sentiment",
        "description": "Government fiscal policy and spending",
    },
    "trade": {
        "query": "tariff OR trade war OR export ban OR supply chain OR trade agreement OR WTO OR sanctions",
        "family": "trade",
        "description": "International trade and supply chains",
    },
    "energy": {
        "query": "oil price OR OPEC OR natural gas OR energy crisis OR renewable energy OR nuclear power OR petroleum",
        "family": "commodity",
        "description": "Energy markets and policy",
    },
    "labor": {
        "query": "unemployment OR job market OR wages OR labor strike OR hiring OR layoffs OR nonfarm payroll",
        "family": "macro",
        "description": "Labor market conditions",
    },
    "financial": {
        "query": "bank failure OR credit crisis OR stock market OR bond market OR default OR financial regulation OR IPO",
        "family": "credit",
        "description": "Financial system and market stress",
    },
    "technology": {
        "query": "artificial intelligence OR semiconductor OR big tech OR cybersecurity OR crypto regulation OR chip shortage",
        "family": "sentiment",
        "description": "Technology sector and innovation",
    },
    "climate": {
        "query": "extreme weather OR hurricane OR wildfire OR carbon emissions OR climate policy OR drought OR flooding",
        "family": "alternative",
        "description": "Climate events and environmental policy",
    },
    "health": {
        "query": "pandemic OR vaccine OR FDA approval OR drug recall OR healthcare policy OR WHO OR epidemic",
        "family": "alternative",
        "description": "Public health and pharma",
    },
}

# Rate limiting
_RATE_LIMIT_DELAY: float = 1.1  # WorldNewsAPI free tier: ~1 req/sec
_REQUEST_TIMEOUT: int = 30
_API_BASE: str = "https://api.worldnewsapi.com"


class WorldNewsPuller(BasePuller):
    """Pulls structured global news data from WorldNewsAPI.

    Extracts article counts, sentiment, and geographic breadth across
    10 macro-relevant taxonomy categories. Produces 33 features total.

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for WorldNewsAPI.
        api_key: WorldNewsAPI key from environment.
    """

    SOURCE_NAME: str = "WorldNewsAPI"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": _API_BASE,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "NEVER",
        "trust_score": "MED",
        "priority_rank": 35,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self.api_key = os.getenv("WORLDNEWS_API_KEY", "")
        if not self.api_key:
            log.warning("WORLDNEWS_API_KEY not set — WorldNewsPuller will be inactive")
        log.info("WorldNewsPuller initialised — source_id={sid}", sid=self.source_id)

    @retry_on_failure(
        max_attempts=3,
        backoff=3.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError, requests.RequestException),
    )
    def _search_news(
        self,
        query: str,
        earliest_date: str,
        latest_date: str,
        number: int = 100,
    ) -> dict[str, Any]:
        """Search WorldNewsAPI for articles matching a query.

        Parameters:
            query: Boolean search query string.
            earliest_date: Start date (YYYY-MM-DD).
            latest_date: End date (YYYY-MM-DD).
            number: Max articles to return (1-100).

        Returns:
            API response dict with 'news' list and metadata.
        """
        resp = requests.get(
            f"{_API_BASE}/search-news",
            params={
                "api-key": self.api_key,
                "text": query,
                "earliest-publish-date": f"{earliest_date} 00:00:00",
                "latest-publish-date": f"{latest_date} 23:59:59",
                "language": "en",
                "number": number,
                "sort": "publish-time",
                "sort-direction": "DESC",
            },
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _parse_sentiment(article: dict[str, Any]) -> float:
        """Extract sentiment score from an article.

        WorldNewsAPI returns a sentiment field in [-1, 1].
        Falls back to a simple title-based heuristic if missing.

        Parameters:
            article: Article dict from API response.

        Returns:
            Sentiment score from -1.0 (negative) to +1.0 (positive).
        """
        # Primary: API-provided sentiment
        sentiment = article.get("sentiment")
        if sentiment is not None:
            try:
                return max(-1.0, min(1.0, float(sentiment)))
            except (ValueError, TypeError):
                pass

        # Fallback: simple keyword heuristic on title + text snippet
        text_content = (
            (article.get("title") or "") + " " + (article.get("text", "") or "")[:500]
        ).lower()

        negative_words = {
            "crash", "crisis", "collapse", "fear", "panic", "recession",
            "default", "war", "attack", "plunge", "slump", "disaster",
            "fail", "bankrupt", "layoff", "shutdown", "sanctions",
        }
        positive_words = {
            "surge", "rally", "boom", "growth", "recovery", "gain",
            "deal", "agreement", "peace", "record high", "expansion",
            "hiring", "breakthrough", "approval", "stimulus",
        }

        neg = sum(1 for w in negative_words if w in text_content)
        pos = sum(1 for w in positive_words if w in text_content)
        total = neg + pos
        if total == 0:
            return 0.0
        return max(-1.0, min(1.0, (pos - neg) / total))

    @staticmethod
    def _extract_source_country(article: dict[str, Any]) -> str | None:
        """Extract the source country from an article.

        Parameters:
            article: Article dict from API response.

        Returns:
            Two-letter country code or None.
        """
        # WorldNewsAPI provides source_country on articles
        country = article.get("source_country")
        if country and isinstance(country, str):
            return country[:2].upper()
        return None

    @staticmethod
    def _parse_article_date(article: dict[str, Any]) -> date | None:
        """Extract publication date from an article.

        Parameters:
            article: Article dict from API response.

        Returns:
            Publication date or None.
        """
        for key in ("publish_date", "publishDate", "published"):
            raw = article.get(key)
            if raw:
                try:
                    return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).date()
                except (ValueError, TypeError):
                    pass
                try:
                    # Handle "2026-03-25 14:30:00" format
                    return datetime.strptime(str(raw)[:10], "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    pass
        return None

    def _ensure_features(self) -> None:
        """Register all taxonomy features in feature_registry if missing."""
        features_to_register: list[tuple[str, str, str]] = []

        for category, config in NEWS_TAXONOMY.items():
            family = config["family"]
            desc_base = config["description"]
            features_to_register.extend([
                (f"wn_{category}_article_count", family, f"WorldNews: {desc_base} — daily article count"),
                (f"wn_{category}_sentiment_avg", "sentiment", f"WorldNews: {desc_base} — avg sentiment"),
                (f"wn_{category}_global_spread", "sentiment", f"WorldNews: {desc_base} — source country count"),
            ])

        # Composite features
        features_to_register.extend([
            ("wn_total_volume", "sentiment", "WorldNews: total articles across all categories"),
            ("wn_fear_index", "sentiment", "WorldNews: negative-sentiment-weighted volume"),
            ("wn_breadth", "sentiment", "WorldNews: total distinct source countries"),
        ])

        with self.engine.begin() as conn:
            for name, family, description in features_to_register:
                conn.execute(
                    text(
                        "INSERT INTO feature_registry "
                        "(name, family, description, transformation, transformation_version, "
                        "lag_days, normalization, missing_data_policy, eligible_from_date, model_eligible) "
                        "VALUES (:name, :family, :desc, 'RAW', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2024-01-01', TRUE) "
                        "ON CONFLICT (name) DO NOTHING"
                    ),
                    {"name": name, "family": family, "desc": description},
                )

    def pull_category(
        self,
        category: str,
        target_date: date,
    ) -> dict[str, Any]:
        """Pull news data for a single taxonomy category on a given date.

        Parameters:
            category: Taxonomy category key (e.g. 'geopolitical').
            target_date: Date to query.

        Returns:
            dict with article_count, sentiment_avg, countries, and raw articles.
        """
        config = NEWS_TAXONOMY[category]
        result: dict[str, Any] = {
            "category": category,
            "date": target_date.isoformat(),
            "article_count": 0,
            "sentiment_avg": 0.0,
            "countries": set(),
            "status": "SUCCESS",
        }

        try:
            data = self._search_news(
                query=config["query"],
                earliest_date=target_date.isoformat(),
                latest_date=target_date.isoformat(),
                number=100,
            )
        except Exception as exc:
            log.warning(
                "WorldNews pull failed for {cat} on {d}: {e}",
                cat=category, d=target_date, e=str(exc),
            )
            result["status"] = "FAILED"
            return result

        articles = data.get("news", [])
        if not articles:
            result["status"] = "NO_DATA"
            return result

        sentiments: list[float] = []
        countries: set[str] = set()

        for article in articles:
            sentiments.append(self._parse_sentiment(article))
            country = self._extract_source_country(article)
            if country:
                countries.add(country)

        result["article_count"] = len(articles)
        result["sentiment_avg"] = sum(sentiments) / len(sentiments) if sentiments else 0.0
        result["countries"] = countries

        return result

    def pull_day(self, target_date: date) -> dict[str, Any]:
        """Pull all taxonomy categories for a single day and store results.

        Parameters:
            target_date: Date to pull.

        Returns:
            Summary dict with per-category results.
        """
        if not self.api_key:
            return {"status": "SKIPPED", "reason": "WORLDNEWS_API_KEY not set"}

        log.info("WorldNews pulling all categories for {d}", d=target_date)

        all_countries: set[str] = set()
        total_articles = 0
        fear_score = 0.0
        rows_inserted = 0
        category_results: list[dict[str, Any]] = []

        with self.engine.begin() as conn:
            for category in NEWS_TAXONOMY:
                cat_result = self.pull_category(category, target_date)
                category_results.append(cat_result)

                count = cat_result["article_count"]
                sentiment = cat_result["sentiment_avg"]
                spread = len(cat_result.get("countries", set()))

                total_articles += count
                all_countries |= cat_result.get("countries", set())

                # Accumulate fear index: articles with negative sentiment weighted by magnitude
                if sentiment < 0:
                    fear_score += count * abs(sentiment)

                # Store category features
                for series_id, value in [
                    (f"wn_{category}_article_count", float(count)),
                    (f"wn_{category}_sentiment_avg", round(sentiment, 4)),
                    (f"wn_{category}_global_spread", float(spread)),
                ]:
                    if not self._row_exists(series_id, target_date, conn):
                        self._insert_raw(
                            conn=conn,
                            series_id=series_id,
                            obs_date=target_date,
                            value=value,
                        )
                        rows_inserted += 1

                time.sleep(_RATE_LIMIT_DELAY)

            # Store composite features
            for series_id, value in [
                ("wn_total_volume", float(total_articles)),
                ("wn_fear_index", round(fear_score, 4)),
                ("wn_breadth", float(len(all_countries))),
            ]:
                if not self._row_exists(series_id, target_date, conn):
                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=target_date,
                        value=value,
                        raw_payload={
                            "total_articles": total_articles,
                            "countries": sorted(all_countries),
                        },
                    )
                    rows_inserted += 1

        log.info(
            "WorldNews day complete — {d}: {n} articles, {c} countries, "
            "{r} rows inserted, fear={f:.2f}",
            d=target_date, n=total_articles, c=len(all_countries),
            r=rows_inserted, f=fear_score,
        )

        return {
            "date": target_date.isoformat(),
            "total_articles": total_articles,
            "countries": len(all_countries),
            "fear_index": round(fear_score, 4),
            "rows_inserted": rows_inserted,
            "categories": category_results,
        }

    def pull_all(
        self,
        days_back: int = 7,
        start_date: date | None = None,
    ) -> list[dict[str, Any]]:
        """Pull news data for the last N days across all categories.

        Parameters:
            days_back: Number of days to pull (default: 7).
            start_date: Explicit start date (overrides days_back).

        Returns:
            List of per-day result dicts.
        """
        if not self.api_key:
            log.warning("WorldNews pull_all skipped — WORLDNEWS_API_KEY not set")
            return [{"status": "SKIPPED", "reason": "WORLDNEWS_API_KEY not set"}]

        # Ensure features are registered
        self._ensure_features()

        end_date = date.today() - timedelta(days=1)  # Yesterday (complete day)
        if start_date is None:
            start_date = end_date - timedelta(days=days_back - 1)

        log.info(
            "WorldNews pull_all — {s} to {e} ({d} days)",
            s=start_date, e=end_date, d=(end_date - start_date).days + 1,
        )

        results: list[dict[str, Any]] = []
        current = start_date
        while current <= end_date:
            day_result = self.pull_day(current)
            results.append(day_result)
            current += timedelta(days=1)

        total_rows = sum(r.get("rows_inserted", 0) for r in results)
        total_articles = sum(r.get("total_articles", 0) for r in results)
        log.info(
            "WorldNews pull_all complete — {rows} rows, {arts} articles over {d} days",
            rows=total_rows, arts=total_articles, d=len(results),
        )
        return results
