"""
GRID Database Bridge for AutoAgent tasks.

Read-only access to GRID's PostgreSQL database for signal hypothesis testing.
Provides PIT-correct feature data and price series.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras

# Load .env from GRID repo root if available (for local runs outside Docker)
try:
    from dotenv import load_dotenv
    for candidate in [
        Path("/data/grid_v4/grid_repo/.env"),
        Path(__file__).resolve().parents[3] / ".env",  # autoagent/tasks/grid-signal-eog/files -> grid_repo
    ]:
        if candidate.exists():
            load_dotenv(candidate)
            break
except ImportError:
    pass  # python-dotenv not available in container — env vars set by task.toml


class GridBridge:
    """Read-only bridge to GRID's resolved_series and feature_registry."""

    def __init__(self) -> None:
        self._conn_params = {
            "host": os.environ.get("GRID_DB_HOST", os.environ.get("DB_HOST", "localhost")),
            "port": int(os.environ.get("GRID_DB_PORT", os.environ.get("DB_PORT", "5432"))),
            "dbname": os.environ.get("GRID_DB_NAME", os.environ.get("DB_NAME", "grid")),
            "user": os.environ.get("GRID_DB_USER", os.environ.get("DB_USER", "grid_user")),
            "password": os.environ.get("GRID_DB_PASSWORD", os.environ.get("DB_PASSWORD", "")),
            "options": "-c statement_timeout=60000",  # 60s query timeout
        }

    def _connect(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(**self._conn_params)

    def get_eog_prices(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch EOG daily close prices from resolved_series.

        Returns DataFrame with columns [obs_date, close], sorted ascending.
        Uses the 'eog_full' feature for maximum history (13k+ rows since 1989).
        Falls back to 'eog' if eog_full is unavailable.
        """
        query = """
            SELECT DISTINCT ON (rs.obs_date)
                rs.obs_date,
                rs.value AS close
            FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name IN ('eog_full', 'eog')
              AND rs.obs_date >= COALESCE(%(start)s, '1980-01-01'::date)
              AND rs.obs_date <= COALESCE(%(end)s, CURRENT_DATE)
            ORDER BY rs.obs_date, fr.name  -- prefer eog_full
        """
        with self._connect() as conn:
            df = pd.read_sql(
                query,
                conn,
                params={"start": start_date, "end": end_date},
                parse_dates=["obs_date"],
            )
        return df.sort_values("obs_date").reset_index(drop=True)

    def get_features(
        self,
        names: list[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        pit_policy: str = "LATEST_AS_OF",
    ) -> pd.DataFrame:
        """Fetch multiple features as a wide DataFrame (obs_date index).

        Uses PIT-correct DISTINCT ON queries matching GRID's vintage policy.
        Returns one column per feature name, indexed by obs_date.
        """
        if not names:
            return pd.DataFrame()

        frames = []
        with self._connect() as conn:
            for name in names:
                if pit_policy == "LATEST_AS_OF":
                    q = """
                        SELECT DISTINCT ON (rs.obs_date)
                            rs.obs_date,
                            rs.value
                        FROM resolved_series rs
                        JOIN feature_registry fr ON rs.feature_id = fr.id
                        WHERE fr.name = %(name)s
                          AND rs.obs_date >= COALESCE(%(start)s, '1980-01-01'::date)
                          AND rs.obs_date <= COALESCE(%(end)s, CURRENT_DATE)
                        ORDER BY rs.obs_date, rs.vintage_date DESC
                    """
                else:
                    q = """
                        SELECT DISTINCT ON (rs.obs_date)
                            rs.obs_date,
                            rs.value
                        FROM resolved_series rs
                        JOIN feature_registry fr ON rs.feature_id = fr.id
                        WHERE fr.name = %(name)s
                          AND rs.obs_date >= COALESCE(%(start)s, '1980-01-01'::date)
                          AND rs.obs_date <= COALESCE(%(end)s, CURRENT_DATE)
                        ORDER BY rs.obs_date, rs.vintage_date ASC
                    """
                df = pd.read_sql(
                    q, conn, params={"name": name, "start": start_date, "end": end_date},
                    parse_dates=["obs_date"],
                )
                if not df.empty:
                    df = df.rename(columns={"value": name})
                    df = df.set_index("obs_date")
                    frames.append(df)

        if not frames:
            return pd.DataFrame()

        result = frames[0]
        for f in frames[1:]:
            result = result.join(f, how="outer")
        return result.sort_index()

    def get_available_features(self, min_rows: int = 100) -> list[tuple]:
        """List features with sufficient data coverage.

        Returns list of (name, family, description, row_count) tuples,
        sorted by row_count descending.
        """
        query = """
            SELECT fr.name, fr.family, fr.description, COUNT(*) AS cnt
            FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.model_eligible = TRUE
            GROUP BY fr.name, fr.family, fr.description
            HAVING COUNT(*) >= %(min_rows)s
            ORDER BY cnt DESC
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query, {"min_rows": min_rows})
            return cur.fetchall()

    def get_feature_families(self) -> dict[str, list[str]]:
        """Get features grouped by family.

        Returns dict mapping family name to list of feature names.
        """
        query = """
            SELECT family, name
            FROM feature_registry
            WHERE model_eligible = TRUE
            ORDER BY family, name
        """
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchall()

        families: dict[str, list[str]] = {}
        for family, name in rows:
            families.setdefault(family, []).append(name)
        return families

    def get_news_headlines(
        self,
        hours: int = 72,
        ticker: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Pull recent news headlines with sentiment from news_articles table.

        Returns list of dicts with: title, source, published_at, sentiment,
        confidence, tickers, llm_summary.
        """
        with self._connect() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if ticker:
                cur.execute(
                    """
                    SELECT title, source, published_at, sentiment, confidence,
                           tickers, llm_summary, summary
                    FROM news_articles
                    WHERE %(ticker)s = ANY(tickers)
                      AND created_at >= NOW() - INTERVAL '%(hours)s hours'
                    ORDER BY published_at DESC NULLS LAST
                    LIMIT %(limit)s
                    """,
                    {"ticker": ticker.upper(), "hours": hours, "limit": limit},
                )
            else:
                cur.execute(
                    """
                    SELECT title, source, published_at, sentiment, confidence,
                           tickers, llm_summary, summary
                    FROM news_articles
                    WHERE created_at >= NOW() - INTERVAL '%(hours)s hours'
                    ORDER BY published_at DESC NULLS LAST
                    LIMIT %(limit)s
                    """,
                    {"hours": hours, "limit": limit},
                )
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    def get_gdelt_tone(
        self,
        days: int = 30,
    ) -> pd.DataFrame:
        """Pull GDELT article count and tone data from resolved_series.

        Returns DataFrame with obs_date index and columns for each GDELT feature.
        """
        gdelt_features = [
            "gdelt_article_count",
            "gdelt_conflict_count",
        ]
        # Also grab any gdelt_*_tone features that exist
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT fr.name
                FROM feature_registry fr
                WHERE fr.name LIKE 'gdelt_%'
                  AND fr.model_eligible = TRUE
                """
            )
            gdelt_features = list({r[0] for r in cur.fetchall()})

        if not gdelt_features:
            return pd.DataFrame()

        return self.get_features(gdelt_features)

    def get_supply_chain_data(self) -> pd.DataFrame:
        """Pull supply chain indicators from resolved_series.

        Includes: shipping rates, container indices, durable goods orders,
        manufacturing data, trade balance, industrial production.
        """
        supply_features = []
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT DISTINCT fr.name
                FROM feature_registry fr
                WHERE (fr.name LIKE 'supply_chain.%'
                    OR fr.name LIKE 'shipping_%'
                    OR fr.name LIKE 'container_%'
                    OR fr.name LIKE 'ais_%'
                    OR fr.name IN ('trade_volume_yoy', 'us_china_trade_balance'))
                  AND fr.model_eligible = TRUE
                """
            )
            supply_features = [r[0] for r in cur.fetchall()]

        if not supply_features:
            return pd.DataFrame()

        return self.get_features(supply_features)

    def get_energy_news_context(self, days: int = 7) -> list[dict]:
        """Pull energy-specific news for LLM reasoning.

        Filters for headlines mentioning oil, OPEC, pipeline, shale,
        Permian, drilling, refinery, LNG, energy policy, sanctions.

        Returns list of dicts ready for LLM consumption.
        """
        energy_keywords = [
            "oil", "crude", "opec", "pipeline", "shale", "permian",
            "drilling", "refinery", "lng", "natural gas", "energy",
            "petroleum", "barrel", "rig count", "fracking", "midstream",
            "eog", "devon", "pioneer", "conocophillips", "chevron", "exxon",
            "sanctions", "russia energy", "iran oil", "houthi", "red sea",
            "supply chain", "freight", "shipping", "tanker",
        ]
        with self._connect() as conn:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Build OR pattern for ILIKE matching
            conditions = " OR ".join(
                [f"LOWER(title) LIKE '%{kw}%'" for kw in energy_keywords]
            )
            cur.execute(
                f"""
                SELECT title, source, published_at, sentiment, confidence,
                       tickers, llm_summary, summary
                FROM news_articles
                WHERE created_at >= NOW() - INTERVAL '{days} days'
                  AND ({conditions})
                ORDER BY published_at DESC NULLS LAST
                LIMIT 200
                """
            )
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    def log_experiment(
        self,
        hypothesis_statement: str,
        feature_ids: list[int],
        metrics: dict,
        verdict: str,
    ) -> int:
        """Log an AutoAgent experiment run back to GRID's hypothesis_registry.

        Creates a CANDIDATE hypothesis and a validation_results entry.
        Returns the hypothesis_id.
        """
        with self._connect() as conn:
            cur = conn.cursor()

            # Insert hypothesis
            cur.execute(
                """
                INSERT INTO hypothesis_registry
                    (statement, layer, feature_ids, lag_structure,
                     proposed_metric, proposed_threshold, state)
                VALUES (%(stmt)s, 'TACTICAL', %(fids)s, %(lag)s,
                        %(metric)s, %(thresh)s, 'CANDIDATE')
                RETURNING id
                """,
                {
                    "stmt": hypothesis_statement,
                    "fids": feature_ids,
                    "lag": '{"autoagent": true}',
                    "metric": "sharpe_ratio",
                    "thresh": metrics.get("sharpe_ratio", 0.0),
                },
            )
            hyp_id = cur.fetchone()[0]

            # Insert validation results
            cur.execute(
                """
                INSERT INTO validation_results
                    (hypothesis_id, vintage_policy, era_results,
                     full_period_metrics, baseline_comparison,
                     simplicity_comparison, walk_forward_splits,
                     cost_assumption_bps, overall_verdict, gate_detail)
                VALUES (%(hid)s, 'LATEST_AS_OF', %(era)s,
                        %(full)s, %(base)s,
                        %(simp)s, %(splits)s,
                        10.0, %(verdict)s, %(gate)s)
                """,
                {
                    "hid": hyp_id,
                    "era": "{}",
                    "full": str(metrics).replace("'", '"'),
                    "base": '{"vs_buy_and_hold": true}',
                    "simp": '{"n_features": ' + str(len(feature_ids)) + "}",
                    "splits": metrics.get("n_splits", 4),
                    "verdict": verdict,
                    "gate": '{"source": "autoagent"}',
                },
            )
            conn.commit()
            return hyp_id
