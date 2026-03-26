"""
GRID Capital Flow Research Engine.

Triggered by LLM queries about capital flows, sector rotation, or fund
positioning. Performs a deep data pull across all available sources to
build a comprehensive sector-level capital flow picture:

1. Sector ETF price action + relative strength (yfinance)
2. Sector volume profiles and momentum
3. Cross-border flows (BIS)
4. FRED monetary aggregates (M2, reserves, bank credit)
5. SEC filing velocity by sector
6. Dark pool activity (FINRA ATS)
7. Credit spreads and bond flows
8. Options positioning (put/call by sector ETFs)

Results are cached, logged as an LLM insight, and returned as structured
data + LLM narrative synthesis.
"""

from __future__ import annotations

import json
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from loguru import logger as log

_CACHE_DIR = Path(__file__).parent.parent / "outputs" / "capital_flow_research"
_CACHE_TTL_HOURS = 4  # Reuse a research run within this window


# ---------------------------------------------------------------------------
# Sector ETF universe
# ---------------------------------------------------------------------------
SECTOR_ETFS = {
    "Technology": "XLK",
    "Financials": "XLF",
    "Energy": "XLE",
    "Healthcare": "XLV",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Industrials": "XLI",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Utilities": "XLU",
    "Communication Services": "XLC",
}

BENCHMARK = "SPY"

# Timeframes for comparison (years back from as_of)
COMPARISON_YEARS = [1, 2, 3, 5]


class CapitalFlowResearchEngine:
    """Performs deep capital flow analysis across sectors.

    Pulls from every data source available, computes relative
    strength / rotation metrics, and feeds results to the LLM for
    narrative synthesis.

    Attributes:
        db_engine: Optional SQLAlchemy engine.
        llm_client: Optional LLM client for narrative synthesis.
    """

    def __init__(
        self,
        db_engine: Any = None,
        llm_client: Any = None,
    ) -> None:
        self.engine = db_engine
        self.llm = llm_client
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)

        if self.llm is None:
            try:
                from ollama.client import get_client
                self.llm = get_client()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
    def run_research(
        self,
        as_of: date | None = None,
        sectors: list[str] | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """Execute a full capital flow research sweep.

        Parameters:
            as_of: Reference date (default: today).
            sectors: Subset of sectors to analyze (default: all).
            force: Skip cache and re-run.

        Returns:
            dict with keys: sectors, flows, relative_strength, monetary,
            credit, options_positioning, sec_velocity, narrative, metadata.
        """
        if as_of is None:
            as_of = date.today()

        cache_key = self._cache_key(as_of, sectors)
        if not force:
            cached = self._load_cache(cache_key)
            if cached:
                log.info("Capital flow research cache hit: {k}", k=cache_key[:12])
                return cached

        log.info("Starting deep capital flow research sweep as_of={d}", d=as_of)

        result: dict[str, Any] = {
            "as_of": as_of.isoformat(),
            "generated_at": datetime.now().isoformat(),
            "sectors": {},
            "monetary": {},
            "credit": {},
            "cross_border": {},
            "sec_velocity": {},
            "dark_pool": {},
            "options_positioning": {},
            "relative_strength": {},
            "yoy_comparison": {},
            "narrative": None,
            "metadata": {"sources_pulled": [], "errors": []},
        }

        target_sectors = sectors or list(SECTOR_ETFS.keys())

        # Pull from all available sources
        self._pull_sector_etf_data(result, as_of, target_sectors)
        self._pull_relative_strength(result, as_of, target_sectors)
        self._pull_yoy_comparison(result, as_of, target_sectors)
        self._pull_monetary_aggregates(result, as_of)
        self._pull_credit_flows(result, as_of)
        self._pull_cross_border(result, as_of)
        self._pull_sec_velocity(result, as_of)
        self._pull_dark_pool(result, as_of)
        self._pull_options_positioning(result, as_of, target_sectors)

        # LLM narrative synthesis
        result["narrative"] = self._synthesize_narrative(result, as_of)

        # Cache and log
        self._save_cache(cache_key, result)
        self._log_insight(result, as_of)

        # Persist to DB for historical analysis
        self._persist_snapshot(result, as_of)

        sources = len(result["metadata"]["sources_pulled"])
        errors = len(result["metadata"]["errors"])
        log.info(
            "Capital flow research complete: {s} sources, {e} errors",
            s=sources, e=errors,
        )

        return result

    def _persist_snapshot(self, result: dict, as_of: date) -> None:
        """Save capital flow snapshot to DB for trend analysis."""
        if not self.engine:
            return
        try:
            from sqlalchemy import text
            with self.engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO capital_flow_snapshots "
                    "(snapshot_date, sectors, relative_strength, monetary, "
                    "options_positioning, narrative, metadata) "
                    "VALUES (:d, :sectors, :rs, :mon, :opts, :narr, :meta) "
                    "ON CONFLICT (snapshot_date) DO UPDATE SET "
                    "generated_at = NOW(), sectors = EXCLUDED.sectors, "
                    "relative_strength = EXCLUDED.relative_strength, "
                    "monetary = EXCLUDED.monetary, "
                    "options_positioning = EXCLUDED.options_positioning, "
                    "narrative = EXCLUDED.narrative, metadata = EXCLUDED.metadata"
                ), {
                    "d": as_of,
                    "sectors": json.dumps(result.get("sectors", {})),
                    "rs": json.dumps(result.get("relative_strength", {})),
                    "mon": json.dumps(result.get("monetary", {})),
                    "opts": json.dumps(result.get("options_positioning", {})),
                    "narr": result.get("narrative"),
                    "meta": json.dumps(result.get("metadata", {})),
                })
            log.info("Capital flow snapshot persisted for {d}", d=as_of)
        except Exception as exc:
            log.warning("Failed to persist capital flow snapshot: {e}", e=str(exc))

    # ------------------------------------------------------------------
    # Data pull methods
    # ------------------------------------------------------------------
    def _pull_sector_etf_data(
        self, result: dict, as_of: date, sectors: list[str],
    ) -> None:
        """Pull sector ETF price, volume, and performance data."""
        if self.engine is None:
            result["metadata"]["errors"].append("No DB engine — skipping ETF data")
            return

        try:
            from sqlalchemy import text

            with self.engine.connect() as conn:
                for sector_name in sectors:
                    etf = SECTOR_ETFS.get(sector_name)
                    if not etf:
                        continue

                    # Latest price + 30d/90d/252d lookback
                    lookbacks = {"1m": 30, "3m": 90, "1y": 252}
                    sector_data: dict[str, Any] = {"etf": etf, "prices": {}, "volumes": {}}

                    for period_label, days_back in lookbacks.items():
                        start = as_of - timedelta(days=days_back)
                        rows = conn.execute(
                            text(
                                "SELECT obs_date, value FROM raw_series "
                                "WHERE series_id = :sid AND obs_date >= :start "
                                "AND obs_date <= :end ORDER BY obs_date"
                            ),
                            {"sid": f"YF:{etf}:close", "start": start, "end": as_of},
                        ).fetchall()

                        if rows:
                            prices = [float(r[1]) for r in rows]
                            dates = [str(r[0]) for r in rows]
                            sector_data["prices"][period_label] = {
                                "start": prices[0],
                                "end": prices[-1],
                                "return_pct": round((prices[-1] / prices[0] - 1) * 100, 2) if prices[0] else 0,
                                "high": round(max(prices), 2),
                                "low": round(min(prices), 2),
                                "n_obs": len(prices),
                                "first_date": dates[0],
                                "last_date": dates[-1],
                            }

                    # Volume data
                    vol_rows = conn.execute(
                        text(
                            "SELECT obs_date, value FROM raw_series "
                            "WHERE series_id = :sid AND obs_date >= :start "
                            "AND obs_date <= :end ORDER BY obs_date"
                        ),
                        {
                            "sid": f"YF:{etf}:volume",
                            "start": as_of - timedelta(days=30),
                            "end": as_of,
                        },
                    ).fetchall()

                    if vol_rows:
                        volumes = [float(r[1]) for r in vol_rows]
                        sector_data["volumes"] = {
                            "avg_30d": round(sum(volumes) / len(volumes), 0),
                            "latest": round(volumes[-1], 0) if volumes else 0,
                            "trend": "increasing" if len(volumes) > 5 and volumes[-1] > sum(volumes[:5]) / 5 else "decreasing",
                        }

                    result["sectors"][sector_name] = sector_data

            result["metadata"]["sources_pulled"].append("sector_etf_prices")
            result["metadata"]["sources_pulled"].append("sector_etf_volumes")

        except Exception as exc:
            log.warning("ETF data pull failed: {e}", e=str(exc))
            result["metadata"]["errors"].append(f"etf_data: {exc}")

    def _pull_relative_strength(
        self, result: dict, as_of: date, sectors: list[str],
    ) -> None:
        """Compute relative strength of each sector vs SPY."""
        if self.engine is None:
            return

        try:
            from sqlalchemy import text

            with self.engine.connect() as conn:
                # Get SPY returns for normalization
                spy_data = {}
                for period_label, days_back in [("1m", 30), ("3m", 90), ("1y", 252)]:
                    start = as_of - timedelta(days=days_back)
                    rows = conn.execute(
                        text(
                            "SELECT obs_date, value FROM raw_series "
                            "WHERE series_id = :sid AND obs_date >= :start "
                            "AND obs_date <= :end ORDER BY obs_date"
                        ),
                        {"sid": f"YF:{BENCHMARK}:close", "start": start, "end": as_of},
                    ).fetchall()
                    if rows:
                        prices = [float(r[1]) for r in rows]
                        spy_data[period_label] = round((prices[-1] / prices[0] - 1) * 100, 2) if prices[0] else 0

                # Compute relative strength for each sector
                for sector_name in sectors:
                    sector_info = result["sectors"].get(sector_name, {})
                    rs = {}
                    for period in ["1m", "3m", "1y"]:
                        sector_ret = sector_info.get("prices", {}).get(period, {}).get("return_pct", 0)
                        spy_ret = spy_data.get(period, 0)
                        rs[period] = round(sector_ret - spy_ret, 2)

                    # Classify rotation signal
                    rs_1m = rs.get("1m", 0)
                    rs_3m = rs.get("3m", 0)
                    if rs_1m > 1 and rs_3m > 2:
                        signal = "STRONG_INFLOW"
                    elif rs_1m > 0.5:
                        signal = "INFLOW"
                    elif rs_1m < -1 and rs_3m < -2:
                        signal = "STRONG_OUTFLOW"
                    elif rs_1m < -0.5:
                        signal = "OUTFLOW"
                    else:
                        signal = "NEUTRAL"

                    result["relative_strength"][sector_name] = {
                        "vs_spy": rs,
                        "signal": signal,
                    }

            result["metadata"]["sources_pulled"].append("relative_strength")

        except Exception as exc:
            log.warning("Relative strength calc failed: {e}", e=str(exc))
            result["metadata"]["errors"].append(f"relative_strength: {exc}")

    def _pull_yoy_comparison(
        self, result: dict, as_of: date, sectors: list[str],
    ) -> None:
        """Compare current sector returns to same periods in prior years."""
        if self.engine is None:
            return

        try:
            from sqlalchemy import text

            with self.engine.connect() as conn:
                for sector_name in sectors:
                    etf = SECTOR_ETFS.get(sector_name)
                    if not etf:
                        continue

                    yearly_returns = {}
                    for years_back in COMPARISON_YEARS:
                        ref_date = as_of.replace(year=as_of.year - years_back)
                        start = ref_date - timedelta(days=90)
                        end = ref_date

                        rows = conn.execute(
                            text(
                                "SELECT obs_date, value FROM raw_series "
                                "WHERE series_id = :sid AND obs_date >= :start "
                                "AND obs_date <= :end ORDER BY obs_date"
                            ),
                            {"sid": f"YF:{etf}:close", "start": start, "end": end},
                        ).fetchall()

                        if rows and len(rows) > 1:
                            prices = [float(r[1]) for r in rows]
                            ret = round((prices[-1] / prices[0] - 1) * 100, 2) if prices[0] else 0
                            yearly_returns[f"{years_back}y_ago_q"] = ret

                    # Current quarter return
                    current_start = as_of - timedelta(days=90)
                    rows = conn.execute(
                        text(
                            "SELECT obs_date, value FROM raw_series "
                            "WHERE series_id = :sid AND obs_date >= :start "
                            "AND obs_date <= :end ORDER BY obs_date"
                        ),
                        {"sid": f"YF:{etf}:close", "start": current_start, "end": as_of},
                    ).fetchall()
                    if rows and len(rows) > 1:
                        prices = [float(r[1]) for r in rows]
                        yearly_returns["current_q"] = round((prices[-1] / prices[0] - 1) * 100, 2) if prices[0] else 0

                    result["yoy_comparison"][sector_name] = yearly_returns

            result["metadata"]["sources_pulled"].append("yoy_comparison")

        except Exception as exc:
            log.warning("YoY comparison failed: {e}", e=str(exc))
            result["metadata"]["errors"].append(f"yoy_comparison: {exc}")

    def _pull_monetary_aggregates(self, result: dict, as_of: date) -> None:
        """Pull monetary data from resolved_series (feature_registry names)."""
        if self.engine is None:
            return

        try:
            from sqlalchemy import text

            # Map display labels to feature_registry names
            feature_map = {
                "fed_funds_rate": "fed_funds_rate",
                "treasury_10y": "treasury_10y",
                "treasury_2y": "treasury_2y",
                "yield_curve": "yield_curve_10y2y",
                "hy_spread": "hy_spread",
                "breakeven_10y": "breakeven_10y",
                "vix": "vix",
                "dollar_index": "dollar_index",
            }

            with self.engine.connect() as conn:
                for label, feat_name in feature_map.items():
                    latest = conn.execute(
                        text(
                            "SELECT rs.value, rs.obs_date FROM resolved_series rs "
                            "JOIN feature_registry fr ON fr.id = rs.feature_id "
                            "WHERE fr.name = :name AND rs.obs_date <= :end "
                            "ORDER BY rs.obs_date DESC LIMIT 1"
                        ),
                        {"name": feat_name, "end": as_of},
                    ).fetchone()

                    year_ago = conn.execute(
                        text(
                            "SELECT rs.value FROM resolved_series rs "
                            "JOIN feature_registry fr ON fr.id = rs.feature_id "
                            "WHERE fr.name = :name AND rs.obs_date <= :end "
                            "ORDER BY rs.obs_date DESC LIMIT 1"
                        ),
                        {"name": feat_name, "end": as_of - timedelta(days=365)},
                    ).fetchone()

                    if latest:
                        entry: dict[str, Any] = {
                            "value": round(float(latest[0]), 2),
                            "date": str(latest[1]),
                        }
                        if year_ago and float(year_ago[0]) != 0:
                            yoy_pct = round((float(latest[0]) - float(year_ago[0])) / abs(float(year_ago[0])) * 100, 2)
                            entry["yoy_pct"] = yoy_pct
                        result["monetary"][label] = entry

            result["metadata"]["sources_pulled"].append("fred_monetary")

        except Exception as exc:
            log.warning("Monetary aggregates pull failed: {e}", e=str(exc))
            result["metadata"]["errors"].append(f"monetary: {exc}")

    def _pull_credit_flows(self, result: dict, as_of: date) -> None:
        """Pull credit spread and bond flow data."""
        if self.engine is None:
            return

        try:
            from sqlalchemy import text

            credit_series = {
                "hyg_price": "YF:HYG:close",
                "lqd_price": "YF:LQD:close",
                "jnk_price": "YF:JNK:close",
                "emb_price": "YF:EMB:close",
                "ice_hy_spread": "FRED:BAMLH0A0HYM2",
                "ice_ig_spread": "FRED:BAMLC0A4CBBB",
            }

            with self.engine.connect() as conn:
                for label, sid in credit_series.items():
                    rows = conn.execute(
                        text(
                            "SELECT value, obs_date FROM raw_series "
                            "WHERE series_id = :sid AND obs_date <= :end "
                            "ORDER BY obs_date DESC LIMIT 2"
                        ),
                        {"sid": sid, "end": as_of},
                    ).fetchall()

                    if rows:
                        entry = {
                            "value": round(float(rows[0][0]), 4),
                            "date": str(rows[0][1]),
                        }
                        if len(rows) > 1:
                            entry["prev_value"] = round(float(rows[1][0]), 4)
                            entry["change"] = round(float(rows[0][0]) - float(rows[1][0]), 4)
                        result["credit"][label] = entry

            result["metadata"]["sources_pulled"].append("credit_flows")

        except Exception as exc:
            log.warning("Credit flow pull failed: {e}", e=str(exc))
            result["metadata"]["errors"].append(f"credit: {exc}")

    def _pull_cross_border(self, result: dict, as_of: date) -> None:
        """Pull BIS cross-border banking flow data."""
        if self.engine is None:
            return

        try:
            from sqlalchemy import text

            with self.engine.connect() as conn:
                # BIS cross-border claims
                row = conn.execute(
                    text(
                        "SELECT rs.value, rs.obs_date FROM resolved_series rs "
                        "JOIN feature_registry fr ON fr.id = rs.feature_id "
                        "WHERE fr.name = 'bis_global_cbflow' "
                        "AND rs.obs_date <= :end "
                        "ORDER BY rs.obs_date DESC LIMIT 1"
                    ),
                    {"end": as_of},
                ).fetchone()

                if row:
                    result["cross_border"]["bis_claims"] = {
                        "value": round(float(row[0]), 2),
                        "date": str(row[1]),
                    }

            result["metadata"]["sources_pulled"].append("bis_cross_border")

        except Exception as exc:
            log.warning("Cross-border pull failed: {e}", e=str(exc))
            result["metadata"]["errors"].append(f"cross_border: {exc}")

    def _pull_sec_velocity(self, result: dict, as_of: date) -> None:
        """Pull SEC filing velocity by sector."""
        if self.engine is None:
            return

        try:
            from sqlalchemy import text

            sectors_map = {
                "FIRE": "Financials & Real Estate",
                "MANUFACTURING": "Industrials & Materials",
                "RETAIL": "Consumer Discretionary",
                "SERVICES": "Business Services",
                "TECH": "Technology",
                "TRANSPORT": "Transportation",
                "ENERGY": "Energy & Mining",
                "HEALTH": "Healthcare",
            }

            with self.engine.connect() as conn:
                for sec_sector, display_name in sectors_map.items():
                    sid = f"SEC_VELOCITY:{sec_sector}"
                    rows = conn.execute(
                        text(
                            "SELECT value, obs_date FROM raw_series "
                            "WHERE series_id = :sid AND obs_date <= :end "
                            "ORDER BY obs_date DESC LIMIT 4"
                        ),
                        {"sid": sid, "end": as_of},
                    ).fetchall()

                    if rows:
                        values = [float(r[0]) for r in rows]
                        result["sec_velocity"][display_name] = {
                            "latest": round(values[0], 0),
                            "date": str(rows[0][1]),
                            "avg_4wk": round(sum(values) / len(values), 1),
                            "trend": "spiking" if values[0] > sum(values[1:]) / max(len(values) - 1, 1) * 1.5 else "normal",
                        }

            result["metadata"]["sources_pulled"].append("sec_velocity")

        except Exception as exc:
            log.warning("SEC velocity pull failed: {e}", e=str(exc))
            result["metadata"]["errors"].append(f"sec_velocity: {exc}")

    def _pull_dark_pool(self, result: dict, as_of: date) -> None:
        """Pull FINRA dark pool activity."""
        if self.engine is None:
            return

        try:
            from sqlalchemy import text

            dp_series = {
                "dark_pool_pct": "FINRA:DARK_POOL_PCT",
                "short_interest": "FINRA:SHORT_INTEREST_TOTAL",
            }

            with self.engine.connect() as conn:
                for label, sid in dp_series.items():
                    row = conn.execute(
                        text(
                            "SELECT value, obs_date FROM raw_series "
                            "WHERE series_id = :sid AND obs_date <= :end "
                            "ORDER BY obs_date DESC LIMIT 1"
                        ),
                        {"sid": sid, "end": as_of},
                    ).fetchone()

                    if row:
                        result["dark_pool"][label] = {
                            "value": round(float(row[0]), 4),
                            "date": str(row[1]),
                        }

            result["metadata"]["sources_pulled"].append("dark_pool")

        except Exception as exc:
            log.warning("Dark pool pull failed: {e}", e=str(exc))
            result["metadata"]["errors"].append(f"dark_pool: {exc}")

    def _pull_options_positioning(
        self, result: dict, as_of: date, sectors: list[str],
    ) -> None:
        """Pull options positioning from options_daily_signals table."""
        if self.engine is None:
            return

        try:
            from sqlalchemy import text

            with self.engine.connect() as conn:
                # Pull all options signals from latest date
                rows = conn.execute(
                    text(
                        "SELECT ticker, put_call_ratio, iv_atm, max_pain, "
                        "spot_price, total_oi, signal_date "
                        "FROM options_daily_signals "
                        "WHERE signal_date = ("
                        "  SELECT MAX(signal_date) FROM options_daily_signals"
                        ") ORDER BY ticker"
                    )
                ).fetchall()

                # Map tickers to sectors
                etf_to_sector = {v: k for k, v in SECTOR_ETFS.items()}

                for row in rows:
                    ticker = row[0]
                    # Check if it's a sector ETF
                    sector = etf_to_sector.get(ticker)
                    if sector and sector in sectors:
                        pcr = float(row[1]) if row[1] else None
                        result["options_positioning"][sector] = {
                            "ticker": ticker,
                            "put_call_ratio": round(pcr, 3) if pcr else None,
                            "iv_atm": round(float(row[2]) * 100, 1) if row[2] else None,
                            "max_pain": round(float(row[3])) if row[3] else None,
                            "spot_price": round(float(row[4]), 2) if row[4] else None,
                            "total_oi": int(row[5]) if row[5] else None,
                            "date": str(row[6]),
                            "sentiment": "bearish" if pcr and pcr > 1.2 else "bullish" if pcr and pcr < 0.7 else "neutral",
                        }

                # Also add key individual names
                for row in rows:
                    ticker = row[0]
                    if ticker not in etf_to_sector:
                        pcr = float(row[1]) if row[1] else None
                        result["options_positioning"][ticker] = {
                            "ticker": ticker,
                            "put_call_ratio": round(pcr, 3) if pcr else None,
                            "iv_atm": round(float(row[2]) * 100, 1) if row[2] else None,
                            "spot_price": round(float(row[4]), 2) if row[4] else None,
                            "date": str(row[6]),
                            "sentiment": "bearish" if pcr and pcr > 1.2 else "bullish" if pcr and pcr < 0.7 else "neutral",
                        }

            result["metadata"]["sources_pulled"].append("options_positioning")

        except Exception as exc:
            log.warning("Options positioning pull failed: {e}", e=str(exc))
            result["metadata"]["errors"].append(f"options: {exc}")

    # ------------------------------------------------------------------
    # LLM narrative synthesis
    # ------------------------------------------------------------------
    def _synthesize_narrative(self, result: dict, as_of: date) -> str | None:
        """Feed all collected data to the LLM for narrative synthesis."""
        if self.llm is None or not getattr(self.llm, "is_available", False):
            return self._fallback_narrative(result)

        # Build a massive context string from all pulled data
        ctx_parts = [
            f"## Capital Flow Research Data — as of {as_of.isoformat()}",
            "",
        ]

        # Sector performance
        if result["sectors"]:
            ctx_parts.append("### SECTOR ETF PERFORMANCE")
            for sector, data in result["sectors"].items():
                prices = data.get("prices", {})
                ctx_parts.append(f"**{sector} ({data.get('etf', '?')})**")
                for period, info in prices.items():
                    ctx_parts.append(f"  {period}: {info.get('return_pct', 'N/A')}%")
                vol = data.get("volumes", {})
                if vol:
                    ctx_parts.append(f"  Volume trend: {vol.get('trend', '?')}, avg 30d: {vol.get('avg_30d', '?')}")
            ctx_parts.append("")

        # Relative strength
        if result["relative_strength"]:
            ctx_parts.append("### RELATIVE STRENGTH vs SPY")
            for sector, rs in result["relative_strength"].items():
                ctx_parts.append(f"  {sector}: signal={rs.get('signal', '?')}, 1m={rs['vs_spy'].get('1m', 0)}, 3m={rs['vs_spy'].get('3m', 0)}")
            ctx_parts.append("")

        # YoY comparison
        if result["yoy_comparison"]:
            ctx_parts.append("### YEAR-OVER-YEAR COMPARISON (quarterly returns)")
            for sector, comp in result["yoy_comparison"].items():
                parts = [f"{k}={v}%" for k, v in comp.items()]
                ctx_parts.append(f"  {sector}: {', '.join(parts)}")
            ctx_parts.append("")

        # Monetary
        if result["monetary"]:
            ctx_parts.append("### MONETARY AGGREGATES")
            for label, info in result["monetary"].items():
                yoy = f" (YoY: {info.get('yoy_pct', '?')}%)" if "yoy_pct" in info else ""
                ctx_parts.append(f"  {label}: {info['value']}{yoy}")
            ctx_parts.append("")

        # Credit
        if result["credit"]:
            ctx_parts.append("### CREDIT FLOWS")
            for label, info in result["credit"].items():
                chg = f" (chg: {info.get('change', '?')})" if "change" in info else ""
                ctx_parts.append(f"  {label}: {info['value']}{chg}")
            ctx_parts.append("")

        # SEC velocity
        if result["sec_velocity"]:
            ctx_parts.append("### SEC FILING VELOCITY BY SECTOR")
            for sector, info in result["sec_velocity"].items():
                ctx_parts.append(f"  {sector}: {info.get('latest', '?')} filings/wk ({info.get('trend', '?')})")
            ctx_parts.append("")

        # Dark pool
        if result["dark_pool"]:
            ctx_parts.append("### DARK POOL ACTIVITY")
            for label, info in result["dark_pool"].items():
                ctx_parts.append(f"  {label}: {info['value']}")
            ctx_parts.append("")

        # Options positioning
        if result["options_positioning"]:
            ctx_parts.append("### OPTIONS POSITIONING BY SECTOR")
            for sector, info in result["options_positioning"].items():
                ctx_parts.append(f"  {sector}: P/C={info.get('put_call_ratio', '?')} ({info.get('sentiment', '?')})")
            ctx_parts.append("")

        data_context = "\n".join(ctx_parts)

        # Build compact summary for LLM (must fit in ~1200 tokens prompt)
        compact_parts = [f"Capital flows as of {as_of}:"]

        # Top 5 sectors by relative strength
        if result["relative_strength"]:
            sorted_rs = sorted(result["relative_strength"].items(), key=lambda x: x[1].get("vs_spy", {}).get("1m", 0), reverse=True)
            compact_parts.append("Sector 1m vs SPY: " + ", ".join(f"{s} {d['vs_spy'].get('1m',0):+.1f}%" for s, d in sorted_rs[:6]))

        # Monetary snapshot
        if result["monetary"]:
            m = result["monetary"]
            parts = []
            for k in ["fed_funds_rate", "treasury_10y", "hy_spread", "vix"]:
                if k in m:
                    yoy = f" ({m[k].get('yoy_pct',0):+.0f}% YoY)" if "yoy_pct" in m[k] else ""
                    parts.append(f"{k}={m[k]['value']}{yoy}")
            if parts:
                compact_parts.append("Macro: " + ", ".join(parts))

        # Options sentiment
        if result["options_positioning"]:
            bearish = [k for k, v in result["options_positioning"].items() if v.get("sentiment") == "bearish"]
            bullish = [k for k, v in result["options_positioning"].items() if v.get("sentiment") == "bullish"]
            if bearish:
                compact_parts.append(f"Options bearish: {', '.join(bearish[:5])}")
            if bullish:
                compact_parts.append(f"Options bullish: {', '.join(bullish[:5])}")

        data_compact = "\n".join(compact_parts)

        messages = [
            {"role": "user", "content": (
                f"{data_compact}\n\n"
                "In 4-6 sentences: Where is capital flowing, what's driving it, "
                "and what should I watch? Be specific, use the numbers above."
            )},
        ]

        narrative = self.llm.chat(
            messages=messages,
            temperature=0.3,
            num_predict=400,
        )

        return narrative

    def _fallback_narrative(self, result: dict) -> str:
        """Generate a data-only summary when LLM is unavailable."""
        lines = ["# Capital Flow Analysis (Data Only — LLM Unavailable)", ""]

        # Show top movers
        if result["relative_strength"]:
            sorted_rs = sorted(
                result["relative_strength"].items(),
                key=lambda x: x[1].get("vs_spy", {}).get("1m", 0),
                reverse=True,
            )
            lines.append("## Sector Rotation (1m relative strength vs SPY)")
            for sector, rs in sorted_rs:
                sig = rs.get("signal", "?")
                val = rs["vs_spy"].get("1m", 0)
                lines.append(f"- **{sector}**: {val:+.2f}% ({sig})")
            lines.append("")

        if result["monetary"]:
            lines.append("## Monetary Conditions")
            for label, info in result["monetary"].items():
                yoy = f" (YoY: {info.get('yoy_pct', '?')}%)" if "yoy_pct" in info else ""
                lines.append(f"- {label}: {info['value']}{yoy}")
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------
    def _cache_key(self, as_of: date, sectors: list[str] | None) -> str:
        key_str = f"{as_of.isoformat()}:{','.join(sorted(sectors)) if sectors else 'all'}"
        return hashlib.md5(key_str.encode()).hexdigest()

    def _load_cache(self, key: str) -> dict | None:
        path = _CACHE_DIR / f"{key}.json"
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            gen_at = datetime.fromisoformat(data.get("generated_at", "2000-01-01"))
            if (datetime.now() - gen_at).total_seconds() > _CACHE_TTL_HOURS * 3600:
                return None
            return data
        except Exception:
            return None

    def _save_cache(self, key: str, result: dict) -> None:
        try:
            path = _CACHE_DIR / f"{key}.json"
            path.write_text(json.dumps(result, indent=2, default=str))
        except Exception as exc:
            log.warning("Cache save failed: {e}", e=str(exc))

    def _log_insight(self, result: dict, as_of: date) -> None:
        """Log the research to the LLM insight archive."""
        try:
            from outputs.llm_logger import log_insight
            log_insight(
                title=f"Capital Flow Research — {as_of.isoformat()}",
                content=result.get("narrative") or "Data-only (LLM unavailable)",
                category="capital_flow_research",
                metadata={
                    "as_of": as_of.isoformat(),
                    "sources": result["metadata"]["sources_pulled"],
                    "sectors_analyzed": list(result["sectors"].keys()),
                },
                provider="llm",
            )
        except Exception as exc:
            log.debug("Insight logging skipped: {e}", e=str(exc))
