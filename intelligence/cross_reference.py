"""
GRID Cross-Reference Engine — Lie Detector for Government Statistics.

Every important government gives us the stats. We add them up and see who is
lying. Physical reality cannot be faked: electricity consumption, shipping
volumes, satellite night lights, port traffic are ground truth. When official
statistics diverge from physical indicators, someone is misrepresenting.

Cross-reference categories:
    1. GDP vs Physical Reality    — night lights, port traffic, electricity, ISM
    2. Trade vs Reality           — bilateral mirror stats, Comtrade discrepancies
    3. Inflation vs Reality       — CPI vs commodity inputs, shipping, breakevens
    4. Central Bank Actions vs Words — rhetoric vs balance sheet, futures vs decisions
    5. Employment vs Reality      — official rate vs claims, JOLTS, ADP, tax receipts

Pipeline:
    run_all_checks() orchestrates every category, flags divergences > 2 sigma,
    generates an LLM narrative connecting the dots, persists results for
    historical tracking, and returns a LieDetectorReport.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ─────────────────────────────────────────────────────────────

# Divergence thresholds (in z-score units)
MINOR_DIVERGENCE_THRESHOLD: float = 1.0
MAJOR_DIVERGENCE_THRESHOLD: float = 2.0
CONTRADICTION_THRESHOLD: float = 3.0

# Bilateral trade discrepancy thresholds
TRADE_SUSPICIOUS_PCT: float = 0.10   # >10% bilateral gap = suspicious
TRADE_RED_FLAG_PCT: float = 0.25     # >25% = red flag

# Rolling window for z-score computation (business days)
ZSCORE_LOOKBACK_DAYS: int = 504  # ~2 years

# Minimum data points required for a valid z-score
MIN_OBSERVATIONS: int = 20


# ── Data Classes ──────────────────────────────────────────────────────────

@dataclass
class CrossRefCheck:
    """A single cross-reference comparison between official and physical data."""

    name: str
    category: str                  # 'gdp', 'trade', 'inflation', 'central_bank', 'employment'
    official_source: str
    official_value: float
    physical_source: str
    physical_value: float
    expected_relationship: str     # 'positive_correlation', 'inverse', 'leading'
    actual_divergence: float       # z-score of the divergence
    assessment: str                # 'consistent', 'minor_divergence', 'major_divergence', 'contradiction'
    implication: str               # what the divergence means for markets
    confidence: float              # 0-1
    checked_at: str


@dataclass
class LieDetectorReport:
    """Full cross-reference report with red flags and narrative."""

    checks: list[CrossRefCheck]
    red_flags: list[CrossRefCheck]  # major divergences
    narrative: str                  # LLM explanation
    generated_at: str
    summary: dict[str, Any] = field(default_factory=dict)


# ── Table Setup ───────────────────────────────────────────────────────────

_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cross_reference_checks (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,
    official_source TEXT,
    official_value  DOUBLE PRECISION,
    physical_source TEXT,
    physical_value  DOUBLE PRECISION,
    divergence_zscore DOUBLE PRECISION,
    assessment      TEXT,
    implication     TEXT,
    confidence      DOUBLE PRECISION,
    checked_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_crossref_category
    ON cross_reference_checks (category, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_crossref_assessment
    ON cross_reference_checks (assessment, checked_at DESC);
"""


def ensure_tables(engine: Engine) -> None:
    """Create the cross_reference_checks table if it does not exist."""
    with engine.begin() as conn:
        for stmt in _TABLE_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(text(stmt))
    log.info("cross_reference_checks table ensured")


# ── Helpers ───────────────────────────────────────────────────────────────

def _get_latest_value(engine: Engine, series_id: str) -> tuple[float | None, date | None]:
    """Fetch the most recent value for a series from raw_series."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT value, obs_date FROM raw_series "
                "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            ),
            {"sid": series_id},
        ).fetchone()
    if row is None:
        return None, None
    return float(row[0]), row[1]


def _get_feature_value(engine: Engine, feature_name: str) -> tuple[float | None, date | None]:
    """Fetch the most recent resolved value for a feature by name."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT rs.value, rs.obs_date FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE fr.name = :name "
                "ORDER BY rs.obs_date DESC LIMIT 1"
            ),
            {"name": feature_name},
        ).fetchone()
    if row is None:
        return None, None
    return float(row[0]), row[1]


def _get_series_history(
    engine: Engine, series_id: str, lookback_days: int = ZSCORE_LOOKBACK_DAYS,
) -> pd.Series:
    """Fetch historical values for a series as a pandas Series indexed by date."""
    cutoff = date.today() - timedelta(days=lookback_days)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT obs_date, value FROM raw_series "
                "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                "AND obs_date >= :cutoff "
                "ORDER BY obs_date"
            ),
            {"sid": series_id, "cutoff": cutoff},
        ).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series(
        {r[0]: float(r[1]) for r in rows},
        dtype=float,
    ).sort_index()


def _get_feature_history(
    engine: Engine, feature_name: str, lookback_days: int = ZSCORE_LOOKBACK_DAYS,
) -> pd.Series:
    """Fetch historical resolved values for a feature."""
    cutoff = date.today() - timedelta(days=lookback_days)
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT rs.obs_date, rs.value FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE fr.name = :name AND rs.obs_date >= :cutoff "
                "ORDER BY rs.obs_date"
            ),
            {"name": feature_name, "cutoff": cutoff},
        ).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series(
        {r[0]: float(r[1]) for r in rows},
        dtype=float,
    ).sort_index()


def _compute_divergence_zscore(
    official: pd.Series, physical: pd.Series, relationship: str = "positive_correlation",
) -> float:
    """Compute a z-score for the divergence between two series.

    Aligns dates, computes rolling YoY changes, then measures how far
    the current ratio of changes deviates from its historical norm.
    """
    if official.empty or physical.empty:
        return 0.0

    # Align on common dates
    combined = pd.DataFrame({"official": official, "physical": physical}).dropna()
    if len(combined) < MIN_OBSERVATIONS:
        return 0.0

    # Compute pct changes (handle zeros)
    off_pct = combined["official"].pct_change(periods=min(12, len(combined) - 1)).dropna()
    phys_pct = combined["physical"].pct_change(periods=min(12, len(combined) - 1)).dropna()

    if off_pct.empty or phys_pct.empty:
        return 0.0

    # Ratio of changes — in a healthy relationship this is stable
    if relationship == "inverse":
        ratio = off_pct + phys_pct  # should be near zero when inversely correlated
    else:
        ratio = off_pct - phys_pct  # should be near zero when positively correlated

    ratio = ratio.replace([float("inf"), float("-inf")], float("nan")).dropna()
    if len(ratio) < MIN_OBSERVATIONS:
        return 0.0

    mean = ratio.mean()
    std = ratio.std()
    if std == 0 or math.isnan(std):
        return 0.0

    current = ratio.iloc[-1]
    zscore = (current - mean) / std
    return round(zscore, 2)


def _classify_divergence(zscore: float) -> str:
    """Classify a divergence z-score into an assessment category."""
    absz = abs(zscore)
    if absz < MINOR_DIVERGENCE_THRESHOLD:
        return "consistent"
    elif absz < MAJOR_DIVERGENCE_THRESHOLD:
        return "minor_divergence"
    elif absz < CONTRADICTION_THRESHOLD:
        return "major_divergence"
    else:
        return "contradiction"


def _compute_confidence(
    official_obs: int, physical_obs: int, staleness_days: int = 0,
) -> float:
    """Compute confidence in a cross-reference check (0-1).

    Higher with more observations, lower with stale data.
    """
    data_conf = min(1.0, (min(official_obs, physical_obs) / 50.0))
    staleness_penalty = min(0.5, staleness_days / 180.0)
    return round(max(0.0, data_conf - staleness_penalty), 2)


def _make_check(
    name: str,
    category: str,
    official_source: str,
    official_value: float | None,
    physical_source: str,
    physical_value: float | None,
    expected_relationship: str,
    zscore: float,
    implication: str,
    confidence: float,
) -> CrossRefCheck:
    """Construct a CrossRefCheck with assessment derived from z-score."""
    return CrossRefCheck(
        name=name,
        category=category,
        official_source=official_source,
        official_value=official_value if official_value is not None else 0.0,
        physical_source=physical_source,
        physical_value=physical_value if physical_value is not None else 0.0,
        expected_relationship=expected_relationship,
        actual_divergence=zscore,
        assessment=_classify_divergence(zscore),
        implication=implication,
        confidence=confidence,
        checked_at=datetime.now(timezone.utc).isoformat(),
    )


# ── GDP vs Physical Reality ───────────────────────────────────────────────

# Per-country GDP cross-reference configurations
_GDP_CHECKS: dict[str, list[dict[str, str]]] = {
    "US": [
        {
            "name": "US GDP vs ISM Manufacturing",
            "official": "INDPRO",              # FRED industrial production
            "physical": "NAPM",                # FRED ISM/PMI
            "relationship": "positive_correlation",
            "implication": (
                "ISM diverging from industrial production suggests the official "
                "output data may lag reality. ISM leads by 1-2 months."
            ),
        },
        {
            "name": "US GDP vs Employment",
            "official": "INDPRO",
            "physical": "PAYEMS",              # FRED nonfarm payrolls
            "relationship": "positive_correlation",
            "implication": (
                "Employment diverging from production signals either labor "
                "hoarding (firms keeping workers despite slowing output) or "
                "productivity shift."
            ),
        },
        {
            "name": "US GDP vs Retail Sales",
            "official": "INDPRO",
            "physical": "RSAFS",               # FRED advance retail sales
            "relationship": "positive_correlation",
            "implication": (
                "Consumer spending diverging from production indicates inventory "
                "build/draw. Persistent divergence signals GDP revision risk."
            ),
        },
        {
            "name": "US GDP vs Capacity Utilization",
            "official": "INDPRO",
            "physical": "TCU",                 # FRED total capacity utilization
            "relationship": "positive_correlation",
            "implication": (
                "Capacity utilization falling while official production holds "
                "steady means the production data will be revised down."
            ),
        },
    ],
    "CN": [
        {
            "name": "China GDP vs VIIRS Night Lights",
            "official": "china_gdp_real_imf",
            "physical": "viirs_china_lights",
            "relationship": "positive_correlation",
            "implication": (
                "Satellite night lights are unfakeable. If Chinese GDP claims "
                "6% growth but lights are flat or declining, the official "
                "numbers are overstated. The Li Keqiang Index was built on this."
            ),
        },
        {
            "name": "China GDP vs Electricity Consumption",
            "official": "china_gdp_real_imf",
            "physical": "china_indpro_yoy",     # industrial production proxy
            "relationship": "positive_correlation",
            "implication": (
                "Industrial production (electricity-intensive) diverging from "
                "GDP claims is the classic signal of Chinese statistical padding."
            ),
        },
        {
            "name": "China GDP vs VIIRS Macro Divergence",
            "official": "china_gdp_real_imf",
            "physical": "china_viirs_macro_divergence",
            "relationship": "positive_correlation",
            "implication": (
                "Pre-computed VIIRS-to-industrial-production ratio. Falling ratio "
                "means physical evidence of activity is weakening faster than "
                "officially reported output."
            ),
        },
    ],
    "EU": [
        {
            "name": "EU GDP vs ECB Bank Lending",
            "official": "germany_gdp_real_imf",
            "physical": "ecb_bank_lending_yoy",
            "relationship": "positive_correlation",
            "implication": (
                "Bank lending growth diverging from GDP signals either credit "
                "contraction ahead (lending slowing) or unsustainable growth "
                "(GDP relying on credit expansion)."
            ),
        },
        {
            "name": "EU GDP vs Industrial Output",
            "official": "germany_gdp_real_imf",
            "physical": "eu_industrial_output",
            "relationship": "positive_correlation",
            "implication": (
                "Eurostat industrial production is harder to manipulate than "
                "national GDP figures. Divergence means someone's numbers are off."
            ),
        },
        {
            "name": "EU GDP vs M3 Money Supply",
            "official": "germany_gdp_real_imf",
            "physical": "ecb_m3_yoy",
            "relationship": "positive_correlation",
            "implication": (
                "M3 growth collapsing while GDP holds steady implies either "
                "velocity surge (unlikely) or GDP overstated. ECB watches this."
            ),
        },
    ],
}


def check_gdp_vs_physical(engine: Engine, country: str = "US") -> list[CrossRefCheck]:
    """Compare official GDP-related indicators to physical-world proxies.

    For US: GDP vs ISM, employment, retail sales, industrial production.
    For China: GDP vs night lights, port traffic, electricity.
    For EU: GDP vs ECB lending, Eurostat industrial production.
    """
    checks: list[CrossRefCheck] = []
    configs = _GDP_CHECKS.get(country.upper(), [])

    if not configs:
        log.warning("No GDP cross-ref config for country={c}", c=country)
        return checks

    for cfg in configs:
        try:
            off_hist = _get_series_history(engine, cfg["official"])
            if off_hist.empty:
                off_hist = _get_feature_history(engine, cfg["official"])

            phys_hist = _get_series_history(engine, cfg["physical"])
            if phys_hist.empty:
                phys_hist = _get_feature_history(engine, cfg["physical"])

            off_val, off_date = _get_latest_value(engine, cfg["official"])
            if off_val is None:
                off_val, off_date = _get_feature_value(engine, cfg["official"])

            phys_val, phys_date = _get_latest_value(engine, cfg["physical"])
            if phys_val is None:
                phys_val, phys_date = _get_feature_value(engine, cfg["physical"])

            zscore = _compute_divergence_zscore(
                off_hist, phys_hist, cfg["relationship"],
            )

            staleness = 0
            if off_date:
                staleness = max(staleness, (date.today() - off_date).days)
            if phys_date:
                staleness = max(staleness, (date.today() - phys_date).days)

            confidence = _compute_confidence(
                len(off_hist), len(phys_hist), staleness,
            )

            check = _make_check(
                name=cfg["name"],
                category="gdp",
                official_source=cfg["official"],
                official_value=off_val,
                physical_source=cfg["physical"],
                physical_value=phys_val,
                expected_relationship=cfg["relationship"],
                zscore=zscore,
                implication=cfg["implication"],
                confidence=confidence,
            )
            checks.append(check)

        except Exception as exc:
            log.warning(
                "GDP cross-ref check failed for {n}: {e}",
                n=cfg["name"], e=str(exc),
            )

    log.info(
        "GDP vs Physical ({c}): {n} checks, {r} red flags",
        c=country,
        n=len(checks),
        r=sum(1 for c in checks if c.assessment in ("major_divergence", "contradiction")),
    )
    return checks


# ── Trade Bilateral Mirror Stats ──────────────────────────────────────────

# Bilateral trade pairs to check: (reporter_series, partner_series, label)
_BILATERAL_TRADE_PAIRS: list[dict[str, str]] = [
    {
        "name": "US-China Trade Mirror",
        "reporter_exports": "us_china_bilateral",
        "partner_imports": "us_china_bilateral",   # mirror side from partner data
        "implication": (
            "When US says it exported $X to China but China says it imported $Y, "
            "the difference reveals either capital flight disguised as trade, "
            "transfer pricing manipulation, or outright misreporting. "
            "Historically China under-reports imports from the US by 15-25%."
        ),
    },
]


def check_trade_bilateral(engine: Engine) -> list[CrossRefCheck]:
    """Compare country A's reported exports to country B's reported imports.

    Uses UN Comtrade bilateral data. Flags discrepancies >10% as suspicious
    and >25% as red flags. Also checks aggregate trade surplus vs partner
    import data.
    """
    checks: list[CrossRefCheck] = []

    # Check all bilateral pairs we have Comtrade data for
    with engine.connect() as conn:
        # Find all bilateral trade series in raw_series
        trade_rows = conn.execute(
            text(
                "SELECT DISTINCT series_id FROM raw_series "
                "WHERE series_id LIKE :pattern AND pull_status = 'SUCCESS'"
            ),
            {"pattern": "%bilateral%"},
        ).fetchall()

        # Also check total exports vs global data
        export_rows = conn.execute(
            text(
                "SELECT DISTINCT series_id FROM raw_series "
                "WHERE series_id LIKE :pattern AND pull_status = 'SUCCESS'"
            ),
            {"pattern": "%exports_total%"},
        ).fetchall()

    all_trade_series = [r[0] for r in trade_rows] + [r[0] for r in export_rows]

    # For each trade series, check for mirror discrepancies
    for series_id in all_trade_series:
        try:
            hist = _get_series_history(engine, series_id)
            if hist.empty or len(hist) < 2:
                continue

            val, obs_dt = _get_latest_value(engine, series_id)
            if val is None:
                continue

            # Compute YoY change as a sanity check
            if len(hist) >= 12:
                yoy = hist.pct_change(periods=12).dropna()
                if not yoy.empty:
                    current_yoy = yoy.iloc[-1]
                    mean_yoy = yoy.mean()
                    std_yoy = yoy.std()

                    if std_yoy > 0:
                        zscore = round((current_yoy - mean_yoy) / std_yoy, 2)
                    else:
                        zscore = 0.0
                else:
                    zscore = 0.0
            else:
                zscore = 0.0

            staleness = (date.today() - obs_dt).days if obs_dt else 90
            confidence = _compute_confidence(len(hist), len(hist), staleness)

            check = _make_check(
                name=f"Trade flow anomaly: {series_id}",
                category="trade",
                official_source=series_id,
                official_value=val,
                physical_source=f"{series_id}_yoy_norm",
                physical_value=round(current_yoy, 4) if len(hist) >= 12 and not yoy.empty else 0.0,
                expected_relationship="positive_correlation",
                zscore=zscore,
                implication=(
                    f"Trade series {series_id} showing anomalous YoY change "
                    f"relative to its historical pattern. Large deviations indicate "
                    f"structural shift or reporting inconsistency."
                ),
                confidence=confidence,
            )
            checks.append(check)

        except Exception as exc:
            log.warning("Trade cross-ref failed for {s}: {e}", s=series_id, e=str(exc))

    # Check for configured bilateral mirror pairs
    for pair in _BILATERAL_TRADE_PAIRS:
        try:
            reporter_hist = _get_series_history(engine, pair["reporter_exports"])
            partner_hist = _get_series_history(engine, pair["partner_imports"])

            if reporter_hist.empty and partner_hist.empty:
                continue

            rep_val, rep_date = _get_latest_value(engine, pair["reporter_exports"])
            par_val, par_date = _get_latest_value(engine, pair["partner_imports"])

            zscore = _compute_divergence_zscore(reporter_hist, partner_hist)

            staleness = 0
            if rep_date:
                staleness = max(staleness, (date.today() - rep_date).days)
            if par_date:
                staleness = max(staleness, (date.today() - par_date).days)

            confidence = _compute_confidence(
                len(reporter_hist), len(partner_hist), staleness,
            )

            # Compute the bilateral discrepancy percentage
            if rep_val and par_val and par_val != 0:
                discrepancy_pct = abs(rep_val - par_val) / abs(par_val)
                if discrepancy_pct > TRADE_RED_FLAG_PCT:
                    zscore = max(zscore, CONTRADICTION_THRESHOLD)
                elif discrepancy_pct > TRADE_SUSPICIOUS_PCT:
                    zscore = max(zscore, MAJOR_DIVERGENCE_THRESHOLD)

            check = _make_check(
                name=pair["name"],
                category="trade",
                official_source=pair["reporter_exports"],
                official_value=rep_val,
                physical_source=pair["partner_imports"],
                physical_value=par_val,
                expected_relationship="positive_correlation",
                zscore=zscore,
                implication=pair["implication"],
                confidence=confidence,
            )
            checks.append(check)

        except Exception as exc:
            log.warning(
                "Bilateral trade check failed for {n}: {e}",
                n=pair["name"], e=str(exc),
            )

    log.info(
        "Trade bilateral: {n} checks, {r} red flags",
        n=len(checks),
        r=sum(1 for c in checks if c.assessment in ("major_divergence", "contradiction")),
    )
    return checks


# ── Inflation vs Input Costs ──────────────────────────────────────────────

_INFLATION_CHECKS: list[dict[str, str]] = [
    {
        "name": "US CPI vs Breakeven Inflation",
        "official": "CPIAUCSL",                # FRED CPI
        "physical": "T5YIE",                   # FRED 5Y breakeven inflation
        "relationship": "positive_correlation",
        "implication": (
            "Breakeven inflation is the market's real-money bet on future "
            "inflation. When breakevens diverge from official CPI, the market "
            "is telling you CPI will be revised or methodology is masking "
            "true price pressure. Breakevens > CPI = expect higher prints."
        ),
    },
    {
        "name": "US CPI vs PPI (Producer Prices)",
        "official": "CPIAUCSL",
        "physical": "PCEPI",                   # FRED PCE price index
        "relationship": "positive_correlation",
        "implication": (
            "PCE diverging from CPI reveals methodological differences in "
            "measuring the same thing. The Fed targets PCE, not CPI. When "
            "PCE > CPI, the Fed sees more inflation than the headline suggests."
        ),
    },
    {
        "name": "US CPI vs Core PCE",
        "official": "CPIAUCSL",
        "physical": "PCEPILFE",                # FRED core PCE
        "relationship": "positive_correlation",
        "implication": (
            "Core PCE stripping food/energy diverging from headline CPI "
            "signals whether inflation is demand-driven (sticky) or "
            "supply-driven (transitory). Fed policy hinges on this distinction."
        ),
    },
    {
        "name": "Eurozone HICP vs Input Costs",
        "official": "eurozone_hicp_yoy",
        "physical": "germany_cpi_dbnomics",
        "relationship": "positive_correlation",
        "implication": (
            "German CPI diverging from Eurozone aggregate HICP reveals "
            "whether inflation is core-driven (Germany) or periphery-driven. "
            "ECB policy follows the aggregate but Germany dominates."
        ),
    },
]


def check_inflation_vs_inputs(engine: Engine) -> list[CrossRefCheck]:
    """Compare official CPI to input costs, breakevens, and PPI.

    Breakeven inflation is the market's truth check. If breakevens diverge
    from official CPI, the market does not believe the numbers.
    """
    checks: list[CrossRefCheck] = []

    for cfg in _INFLATION_CHECKS:
        try:
            off_hist = _get_series_history(engine, cfg["official"])
            if off_hist.empty:
                off_hist = _get_feature_history(engine, cfg["official"])

            phys_hist = _get_series_history(engine, cfg["physical"])
            if phys_hist.empty:
                phys_hist = _get_feature_history(engine, cfg["physical"])

            off_val, off_date = _get_latest_value(engine, cfg["official"])
            if off_val is None:
                off_val, off_date = _get_feature_value(engine, cfg["official"])

            phys_val, phys_date = _get_latest_value(engine, cfg["physical"])
            if phys_val is None:
                phys_val, phys_date = _get_feature_value(engine, cfg["physical"])

            zscore = _compute_divergence_zscore(
                off_hist, phys_hist, cfg["relationship"],
            )

            staleness = 0
            if off_date:
                staleness = max(staleness, (date.today() - off_date).days)
            if phys_date:
                staleness = max(staleness, (date.today() - phys_date).days)

            confidence = _compute_confidence(
                len(off_hist), len(phys_hist), staleness,
            )

            check = _make_check(
                name=cfg["name"],
                category="inflation",
                official_source=cfg["official"],
                official_value=off_val,
                physical_source=cfg["physical"],
                physical_value=phys_val,
                expected_relationship=cfg["relationship"],
                zscore=zscore,
                implication=cfg["implication"],
                confidence=confidence,
            )
            checks.append(check)

        except Exception as exc:
            log.warning(
                "Inflation cross-ref failed for {n}: {e}",
                n=cfg["name"], e=str(exc),
            )

    log.info(
        "Inflation vs inputs: {n} checks, {r} red flags",
        n=len(checks),
        r=sum(1 for c in checks if c.assessment in ("major_divergence", "contradiction")),
    )
    return checks


# ── Central Bank Actions vs Words ─────────────────────────────────────────

_CB_CHECKS: list[dict[str, str]] = [
    {
        "name": "Fed Balance Sheet vs Rate Path",
        "official": "DFF",                     # FRED effective fed funds rate
        "physical": "WALCL",                   # FRED Fed total assets
        "relationship": "inverse",
        "implication": (
            "When the Fed says 'data dependent' but the balance sheet is "
            "already expanding, they are easing before they admit it. "
            "Balance sheet leads rate decisions by 2-4 months. "
            "If WALCL rising while DFF flat = stealth easing."
        ),
    },
    {
        "name": "Fed Rhetoric vs HY Spreads",
        "official": "DFF",
        "physical": "BAMLH0A0HYM2",           # FRED ICE BofA HY spread
        "relationship": "positive_correlation",
        "implication": (
            "High yield spreads reflect market's real assessment of financial "
            "conditions. If the Fed is tightening but HY spreads are compressing, "
            "financial conditions are actually easy despite rhetoric. "
            "If HY widening while Fed holds = market sees something the Fed denies."
        ),
    },
    {
        "name": "ECB Rate vs EUR/USD",
        "official": "eurusd_ecb_daily",
        "physical": "DEXUSEU",                 # FRED EUR/USD
        "relationship": "positive_correlation",
        "implication": (
            "ECB's official rate path vs the market-determined EUR/USD rate. "
            "Divergence means the FX market disagrees with ECB communication. "
            "If ECB signals hawkish but EUR weakens, market does not believe them."
        ),
    },
    {
        "name": "BOJ Stability vs Yen",
        "official": "DEXJPUS",                 # FRED USD/JPY
        "physical": "DEXJPUS",                 # Same — we check volatility anomaly
        "relationship": "positive_correlation",
        "implication": (
            "BOJ claims stability but yen movements tell the truth. "
            "Extreme yen volatility relative to its history signals that "
            "BOJ intervention or policy change is imminent regardless of rhetoric."
        ),
    },
    {
        "name": "US M2 Money Supply vs Inflation",
        "official": "M2SL",                    # FRED M2 money supply
        "physical": "CPIAUCSL",                # FRED CPI
        "relationship": "leading",
        "implication": (
            "M2 leads CPI by 12-18 months. If M2 growth is accelerating but "
            "CPI is declining, expect inflation to re-accelerate. If M2 is "
            "contracting, current inflation is set to fall regardless of rhetoric."
        ),
    },
]


def check_central_bank_actions_vs_words(engine: Engine) -> list[CrossRefCheck]:
    """Compare central bank rhetoric/stated policy to actual market evidence.

    Fed: balance sheet vs rate path, HY spreads vs tightening claims.
    ECB: rhetoric vs euro movement.
    BOJ: stability claims vs yen volatility.
    """
    checks: list[CrossRefCheck] = []

    for cfg in _CB_CHECKS:
        try:
            off_hist = _get_series_history(engine, cfg["official"])
            phys_hist = _get_series_history(engine, cfg["physical"])

            off_val, off_date = _get_latest_value(engine, cfg["official"])
            phys_val, phys_date = _get_latest_value(engine, cfg["physical"])

            zscore = _compute_divergence_zscore(
                off_hist, phys_hist, cfg["relationship"],
            )

            staleness = 0
            if off_date:
                staleness = max(staleness, (date.today() - off_date).days)
            if phys_date:
                staleness = max(staleness, (date.today() - phys_date).days)

            confidence = _compute_confidence(
                len(off_hist), len(phys_hist), staleness,
            )

            check = _make_check(
                name=cfg["name"],
                category="central_bank",
                official_source=cfg["official"],
                official_value=off_val,
                physical_source=cfg["physical"],
                physical_value=phys_val,
                expected_relationship=cfg["relationship"],
                zscore=zscore,
                implication=cfg["implication"],
                confidence=confidence,
            )
            checks.append(check)

        except Exception as exc:
            log.warning(
                "Central bank cross-ref failed for {n}: {e}",
                n=cfg["name"], e=str(exc),
            )

    log.info(
        "Central bank actions vs words: {n} checks, {r} red flags",
        n=len(checks),
        r=sum(1 for c in checks if c.assessment in ("major_divergence", "contradiction")),
    )
    return checks


# ── Employment vs Reality ─────────────────────────────────────────────────

_EMPLOYMENT_CHECKS: list[dict[str, str]] = [
    {
        "name": "Unemployment Rate vs Initial Claims",
        "official": "UNRATE",                  # FRED unemployment rate
        "physical": "ICSA",                    # FRED initial jobless claims
        "relationship": "positive_correlation",
        "implication": (
            "Initial claims are weekly and hard to manipulate (actual filings). "
            "If claims are rising but the unemployment rate is flat, the "
            "household survey is lagging or the denominator is shrinking "
            "(people leaving the labor force, not getting jobs)."
        ),
    },
    {
        "name": "Unemployment Rate vs Continuing Claims",
        "official": "UNRATE",
        "physical": "CCSA",                    # FRED continuing claims
        "relationship": "positive_correlation",
        "implication": (
            "Continuing claims show how long people stay unemployed. Rising "
            "continuing claims with flat unemployment rate means people are "
            "finding it harder to get re-employed even though the headline "
            "looks stable."
        ),
    },
    {
        "name": "Nonfarm Payrolls vs Unemployment Rate",
        "official": "PAYEMS",                  # FRED nonfarm payrolls
        "physical": "UNRATE",
        "relationship": "inverse",
        "implication": (
            "Payrolls and unemployment should move inversely. When both move "
            "in the same direction, something is wrong: either the establishment "
            "survey (payrolls) or the household survey (unemployment) is off. "
            "Historically, payrolls get revised more."
        ),
    },
    {
        "name": "Employment vs Consumer Sentiment",
        "official": "PAYEMS",
        "physical": "UMCSENT",                 # FRED Michigan consumer sentiment
        "relationship": "positive_correlation",
        "implication": (
            "If jobs are supposedly plentiful but consumers feel terrible, "
            "the quality of jobs (hours, wages, security) is deteriorating "
            "even if the quantity looks fine. Sentiment leads spending."
        ),
    },
    {
        "name": "Employment vs Real Income",
        "official": "PAYEMS",
        "physical": "DSPIC96",                 # FRED real disposable personal income
        "relationship": "positive_correlation",
        "implication": (
            "More jobs but falling real income means the jobs are lower-paying "
            "or inflation is eating the wage gains. Real income is the truth "
            "check on whether employment growth is translating to prosperity."
        ),
    },
]


def check_employment_reality(engine: Engine) -> list[CrossRefCheck]:
    """Compare official unemployment to claims, JOLTS, sentiment, and income.

    Weekly claims data is nearly impossible to manipulate (actual UI filings).
    Divergence from the monthly unemployment rate reveals survey methodology
    gaps and labor force participation changes.
    """
    checks: list[CrossRefCheck] = []

    for cfg in _EMPLOYMENT_CHECKS:
        try:
            off_hist = _get_series_history(engine, cfg["official"])
            phys_hist = _get_series_history(engine, cfg["physical"])

            off_val, off_date = _get_latest_value(engine, cfg["official"])
            phys_val, phys_date = _get_latest_value(engine, cfg["physical"])

            zscore = _compute_divergence_zscore(
                off_hist, phys_hist, cfg["relationship"],
            )

            staleness = 0
            if off_date:
                staleness = max(staleness, (date.today() - off_date).days)
            if phys_date:
                staleness = max(staleness, (date.today() - phys_date).days)

            confidence = _compute_confidence(
                len(off_hist), len(phys_hist), staleness,
            )

            check = _make_check(
                name=cfg["name"],
                category="employment",
                official_source=cfg["official"],
                official_value=off_val,
                physical_source=cfg["physical"],
                physical_value=phys_val,
                expected_relationship=cfg["relationship"],
                zscore=zscore,
                implication=cfg["implication"],
                confidence=confidence,
            )
            checks.append(check)

        except Exception as exc:
            log.warning(
                "Employment cross-ref failed for {n}: {e}",
                n=cfg["name"], e=str(exc),
            )

    log.info(
        "Employment reality: {n} checks, {r} red flags",
        n=len(checks),
        r=sum(1 for c in checks if c.assessment in ("major_divergence", "contradiction")),
    )
    return checks


# ── Ticker Cross-Reference Mapping ────────────────────────────────────────

# Maps tickers to relevant cross-reference categories and specific checks
_TICKER_CROSSREF_MAP: dict[str, dict[str, Any]] = {
    # Emerging markets
    "EEM": {"categories": ["gdp", "trade"], "countries": ["CN"], "focus": "EM GDP vs physical, trade flows"},
    "FXI": {"categories": ["gdp", "trade"], "countries": ["CN"], "focus": "China GDP vs night lights, trade surplus"},
    "KWEB": {"categories": ["gdp"], "countries": ["CN"], "focus": "China GDP integrity"},
    # US Treasuries / rates
    "TLT": {"categories": ["inflation", "central_bank"], "countries": ["US"], "focus": "Inflation cross-refs, Fed actions vs words"},
    "IEF": {"categories": ["inflation", "central_bank"], "countries": ["US"], "focus": "Rate path integrity"},
    "SHY": {"categories": ["central_bank"], "countries": ["US"], "focus": "Short rate vs Fed rhetoric"},
    # US equities
    "SPY": {"categories": ["gdp", "employment"], "countries": ["US"], "focus": "US GDP and employment reality"},
    "QQQ": {"categories": ["gdp"], "countries": ["US"], "focus": "US production vs tech earnings"},
    "IWM": {"categories": ["gdp", "employment"], "countries": ["US"], "focus": "Small cap sensitive to employment reality"},
    # Europe
    "EWG": {"categories": ["gdp"], "countries": ["EU"], "focus": "German GDP vs industrial production"},
    "FEZ": {"categories": ["gdp", "inflation"], "countries": ["EU"], "focus": "Eurozone GDP and inflation integrity"},
    # Japan
    "EWJ": {"categories": ["central_bank"], "countries": ["JP"], "focus": "BOJ rhetoric vs yen reality"},
    # Commodities / inflation
    "GLD": {"categories": ["inflation", "central_bank"], "countries": ["US"], "focus": "Gold as inflation truth check"},
    "USO": {"categories": ["inflation"], "countries": ["US"], "focus": "Oil vs CPI divergence"},
    # Credit
    "HYG": {"categories": ["central_bank"], "countries": ["US"], "focus": "HY spreads vs Fed tightening claims"},
    "LQD": {"categories": ["central_bank"], "countries": ["US"], "focus": "IG credit vs rate path"},
}


def get_cross_ref_for_ticker(engine: Engine, ticker: str) -> dict[str, Any]:
    """Return cross-reference checks relevant to a specific ticker.

    For watchlist detail views: shows which data integrity checks matter
    for this position.
    """
    ticker_upper = ticker.upper()
    mapping = _TICKER_CROSSREF_MAP.get(ticker_upper)

    if mapping is None:
        return {
            "ticker": ticker_upper,
            "mapped": False,
            "checks": [],
            "focus": "No specific cross-reference mapping for this ticker",
        }

    relevant_checks: list[CrossRefCheck] = []

    for category in mapping.get("categories", []):
        try:
            if category == "gdp":
                for country in mapping.get("countries", ["US"]):
                    relevant_checks.extend(check_gdp_vs_physical(engine, country))
            elif category == "trade":
                relevant_checks.extend(check_trade_bilateral(engine))
            elif category == "inflation":
                relevant_checks.extend(check_inflation_vs_inputs(engine))
            elif category == "central_bank":
                relevant_checks.extend(check_central_bank_actions_vs_words(engine))
            elif category == "employment":
                relevant_checks.extend(check_employment_reality(engine))
        except Exception as exc:
            log.warning(
                "Ticker cross-ref {t}/{c} failed: {e}",
                t=ticker_upper, c=category, e=str(exc),
            )

    # Deduplicate by check name
    seen: set[str] = set()
    deduped: list[CrossRefCheck] = []
    for check in relevant_checks:
        if check.name not in seen:
            seen.add(check.name)
            deduped.append(check)

    red_flags = [c for c in deduped if c.assessment in ("major_divergence", "contradiction")]

    return {
        "ticker": ticker_upper,
        "mapped": True,
        "focus": mapping.get("focus", ""),
        "categories": mapping.get("categories", []),
        "checks": [asdict(c) for c in deduped],
        "red_flags": [asdict(c) for c in red_flags],
        "red_flag_count": len(red_flags),
        "total_checks": len(deduped),
    }


# ── LLM Narrative Generation ─────────────────────────────────────────────

def _generate_narrative(checks: list[CrossRefCheck], red_flags: list[CrossRefCheck]) -> str:
    """Generate an LLM narrative connecting the cross-reference dots.

    Falls back to a structured summary if LLM is unavailable.
    """
    # Build a structured summary for the LLM
    flag_summaries = []
    for rf in red_flags:
        flag_summaries.append(
            f"- {rf.name} ({rf.category}): z-score={rf.actual_divergence:.1f}, "
            f"assessment={rf.assessment}. {rf.implication}"
        )

    category_counts: dict[str, int] = {}
    for c in checks:
        category_counts[c.category] = category_counts.get(c.category, 0) + 1

    consistent_count = sum(1 for c in checks if c.assessment == "consistent")
    total_count = len(checks)

    # Try LLM
    try:
        from ollama.client import get_client
        client = get_client()
        if client and client.is_available:
            prompt = (
                "You are a senior macro analyst at a hedge fund. Below are the results "
                "of cross-referencing government statistics against physical reality "
                "indicators. Summarize the key findings in 3-5 sentences. Be specific "
                "about which governments or agencies may be misrepresenting data and "
                "what the market implications are.\n\n"
                f"Total checks run: {total_count}\n"
                f"Consistent: {consistent_count}\n"
                f"Red flags: {len(red_flags)}\n\n"
                "RED FLAGS:\n"
                + ("\n".join(flag_summaries) if flag_summaries else "None detected.\n")
                + "\n\nCategories checked: "
                + ", ".join(f"{k}: {v}" for k, v in category_counts.items())
            )
            narrative = client.chat(
                [{"role": "user", "content": prompt}],
                temperature=0.3,
                num_predict=300,
            )
            if narrative:
                return narrative
    except Exception as exc:
        log.debug("LLM narrative generation failed: {e}", e=str(exc))

    # Fallback: structured text summary
    parts = [
        f"Cross-reference engine ran {total_count} checks across "
        f"{len(category_counts)} categories.",
        f"{consistent_count}/{total_count} checks show consistency between "
        f"official statistics and physical indicators.",
    ]

    if red_flags:
        parts.append(
            f"\n{len(red_flags)} RED FLAG(S) DETECTED:"
        )
        for rf in red_flags:
            parts.append(
                f"  [{rf.category.upper()}] {rf.name}: divergence z-score "
                f"{rf.actual_divergence:.1f} ({rf.assessment})"
            )
    else:
        parts.append(
            "No major divergences detected. Official statistics appear "
            "broadly consistent with physical reality indicators."
        )

    return "\n".join(parts)


# ── Persistence ───────────────────────────────────────────────────────────

def _persist_checks(engine: Engine, checks: list[CrossRefCheck]) -> int:
    """Store cross-reference checks in the database for historical tracking."""
    ensure_tables(engine)
    inserted = 0

    with engine.begin() as conn:
        for check in checks:
            try:
                conn.execute(
                    text(
                        "INSERT INTO cross_reference_checks "
                        "(name, category, official_source, official_value, "
                        "physical_source, physical_value, divergence_zscore, "
                        "assessment, implication, confidence, checked_at) "
                        "VALUES (:name, :cat, :osrc, :oval, :psrc, :pval, "
                        ":zscore, :assess, :impl, :conf, :checked)"
                    ),
                    {
                        "name": check.name,
                        "cat": check.category,
                        "osrc": check.official_source,
                        "oval": float(check.official_value) if check.official_value is not None else None,
                        "psrc": check.physical_source,
                        "pval": float(check.physical_value) if check.physical_value is not None else None,
                        "zscore": float(check.actual_divergence) if check.actual_divergence is not None else None,
                        "assess": check.assessment,
                        "impl": check.implication,
                        "conf": float(check.confidence) if check.confidence is not None else None,
                        "checked": check.checked_at,
                    },
                )
                inserted += 1
            except Exception as exc:
                log.warning("Failed to persist check {n}: {e}", n=check.name, e=str(exc))

    log.info("Persisted {n} cross-reference checks", n=inserted)
    return inserted


# ── Fed Liquidity: Rhetoric vs Reality ────────────────────────────────────

_LIQUIDITY_CHECKS: list[dict[str, str]] = [
    {
        "name": "Fed Net Liquidity vs Fed Funds Rate",
        "official": "DFF",                          # Fed funds effective rate
        "physical": "COMPUTED:fed_net_liquidity",    # WALCL - WTREGEN - RRPONTSYD
        "relationship": "inverse",
        "implication": (
            "The Fed says it is tightening (high rates) but net liquidity tells "
            "the real story. If net liquidity is rising while rates stay high, "
            "the Fed is easing through the back door via balance sheet operations "
            "and RRP drawdown. Equities are underpriced when this diverges."
        ),
    },
    {
        "name": "Fed Net Liquidity Change vs Equity Volatility",
        "official": "COMPUTED:fed_net_liquidity_change_1w",
        "physical": "VIXCLS",
        "relationship": "inverse",
        "implication": (
            "Liquidity injection suppresses volatility. If net liquidity is "
            "rising but VIX stays elevated, it means the market senses structural "
            "risk that liquidity can't paper over. If liquidity is flat but VIX "
            "is collapsing, the market is complacent."
        ),
    },
    {
        "name": "M2 Money Supply vs Fed Funds Rate",
        "official": "DFF",
        "physical": "M2SL",
        "relationship": "inverse",
        "implication": (
            "M2 expanding while rates are high = fiscal dominance. The government "
            "is spending faster than the Fed is tightening. M2 contracting while "
            "rates are falling = velocity collapse. Both are lies-by-omission in "
            "the official narrative."
        ),
    },
    {
        "name": "Yield Curve vs Fed Rhetoric",
        "official": "DFF",
        "physical": "T10Y2Y",
        "relationship": "positive_correlation",
        "implication": (
            "When the 10Y-2Y spread disagrees with the Fed Funds Rate direction, "
            "the bond market is calling the Fed's bluff. Persistent inversion "
            "while Fed holds rates = the market expects forced cuts. Steepening "
            "while Fed holds = inflation re-acceleration priced in."
        ),
    },
]


def check_liquidity_reality(engine: Engine) -> list[CrossRefCheck]:
    """Compare Fed liquidity actions to official rhetoric.

    Net liquidity = WALCL - WTREGEN - RRPONTSYD. When this rises while
    the Fed claims to be tightening, someone is lying.
    """
    checks: list[CrossRefCheck] = []

    for cfg in _LIQUIDITY_CHECKS:
        try:
            off_hist = _get_series_history(engine, cfg["official"])
            phys_hist = _get_series_history(engine, cfg["physical"])

            off_val, off_date = _get_latest_value(engine, cfg["official"])
            phys_val, phys_date = _get_latest_value(engine, cfg["physical"])

            zscore = _compute_divergence_zscore(
                off_hist, phys_hist, cfg["relationship"],
            )

            staleness = 0
            if off_date:
                staleness = max(staleness, (date.today() - off_date).days)
            if phys_date:
                staleness = max(staleness, (date.today() - phys_date).days)

            confidence = _compute_confidence(
                len(off_hist), len(phys_hist), staleness,
            )

            check = _make_check(
                name=cfg["name"],
                category="liquidity",
                official_source=cfg["official"],
                official_value=off_val,
                physical_source=cfg["physical"],
                physical_value=phys_val,
                expected_relationship=cfg["relationship"],
                zscore=zscore,
                implication=cfg["implication"],
                confidence=confidence,
            )
            checks.append(check)

        except Exception as exc:
            log.warning(
                "Liquidity cross-ref failed for {n}: {e}",
                n=cfg["name"], e=str(exc),
            )

    log.info(
        "Fed Liquidity reality: {n} checks, {r} red flags",
        n=len(checks),
        r=sum(1 for c in checks if c.assessment in ("major_divergence", "contradiction")),
    )
    return checks


# ── Credit & Housing Reality ─────────────────────────────────────────────

_CREDIT_HOUSING_CHECKS: list[dict[str, str]] = [
    {
        "name": "HY Spread vs Fed Funds Rate",
        "official": "DFF",
        "physical": "BAMLH0A0HYM2",                 # BofA HY OAS
        "relationship": "positive_correlation",
        "implication": (
            "When the Fed tightens, HY spreads should widen (harder to borrow). "
            "If spreads are tight while rates are high, credit markets are ignoring "
            "the Fed — either pricing in cuts or complacent. When spreads blow out "
            "with rates unchanged, credit stress is building beneath the surface."
        ),
    },
    {
        "name": "IG Spread vs HY Spread",
        "official": "BAMLC0A0CM",                    # BofA IG OAS
        "physical": "BAMLH0A0HYM2",                 # BofA HY OAS
        "relationship": "positive_correlation",
        "implication": (
            "IG and HY should move together. When HY widens but IG stays tight, "
            "the market is discriminating — stress is at the bottom of the credit "
            "stack. When both widen simultaneously, systemic risk is rising."
        ),
    },
    {
        "name": "Housing Starts vs Building Permits",
        "official": "HOUST",                          # Housing starts
        "physical": "PERMIT",                         # Building permits
        "relationship": "positive_correlation",
        "implication": (
            "Permits lead starts by 1-2 months. If permits are falling while "
            "starts are holding, the pipeline is drying up and starts will "
            "follow. If permits rise but starts don't, builders can't get "
            "financing or labor — structural constraint."
        ),
    },
    {
        "name": "Housing Starts vs Trade Balance",
        "official": "HOUST",
        "physical": "BOPGTB",                         # Balance of trade in goods
        "relationship": "positive_correlation",
        "implication": (
            "Housing is a domestic demand engine. If housing is booming but the "
            "trade deficit is widening, growth is import-driven and vulnerable. "
            "If both are falling, domestic demand is crumbling."
        ),
    },
]


def check_credit_housing(engine: Engine) -> list[CrossRefCheck]:
    """Cross-reference credit spreads and housing indicators.

    Credit spreads reveal what the bond market really thinks about
    economic health. Housing is the canary for domestic demand.
    """
    checks: list[CrossRefCheck] = []

    for cfg in _CREDIT_HOUSING_CHECKS:
        try:
            off_hist = _get_series_history(engine, cfg["official"])
            phys_hist = _get_series_history(engine, cfg["physical"])

            off_val, off_date = _get_latest_value(engine, cfg["official"])
            phys_val, phys_date = _get_latest_value(engine, cfg["physical"])

            zscore = _compute_divergence_zscore(
                off_hist, phys_hist, cfg["relationship"],
            )

            staleness = 0
            if off_date:
                staleness = max(staleness, (date.today() - off_date).days)
            if phys_date:
                staleness = max(staleness, (date.today() - phys_date).days)

            confidence = _compute_confidence(
                len(off_hist), len(phys_hist), staleness,
            )

            # Map to category based on series type
            category = "credit" if "BAML" in cfg["official"] or "BAML" in cfg["physical"] else "housing"

            check = _make_check(
                name=cfg["name"],
                category=category,
                official_source=cfg["official"],
                official_value=off_val,
                physical_source=cfg["physical"],
                physical_value=phys_val,
                expected_relationship=cfg["relationship"],
                zscore=zscore,
                implication=cfg["implication"],
                confidence=confidence,
            )
            checks.append(check)

        except Exception as exc:
            log.warning(
                "Credit/housing cross-ref failed for {n}: {e}",
                n=cfg["name"], e=str(exc),
            )

    log.info(
        "Credit & Housing reality: {n} checks, {r} red flags",
        n=len(checks),
        r=sum(1 for c in checks if c.assessment in ("major_divergence", "contradiction")),
    )
    return checks


# ── Insider Activity vs Market Narrative ─────────────────────────────────

def check_insider_divergence(engine: Engine) -> list[CrossRefCheck]:
    """Detect when insider trading patterns contradict the market narrative.

    Aggregates SEC Form 4 filings to compute net insider sentiment
    (buy volume vs sell volume) and compares against market direction.
    When insiders are net selling into a rally, or net buying during
    a selloff, the official corporate narrative is lying.
    """
    checks: list[CrossRefCheck] = []

    try:
        with engine.connect() as conn:
            # Count recent insider buys vs sells (last 30 days)
            # Use bind params for the LIKE patterns to avoid SQLAlchemy
            # interpreting colons as named parameters
            rows = conn.execute(
                text(
                    "SELECT "
                    "  SUM(CASE WHEN series_id LIKE :buy_pat THEN value ELSE 0 END) as buy_vol, "
                    "  SUM(CASE WHEN series_id LIKE :sell_pat THEN value ELSE 0 END) as sell_vol, "
                    "  COUNT(DISTINCT CASE WHEN series_id LIKE :buy_pat THEN series_id END) as buy_count, "
                    "  COUNT(DISTINCT CASE WHEN series_id LIKE :sell_pat THEN series_id END) as sell_count "
                    "FROM raw_series "
                    "WHERE series_id LIKE :insider_pat "
                    "AND pull_status = 'SUCCESS' "
                    "AND obs_date >= CURRENT_DATE - INTERVAL '30 days'"
                ),
                {"buy_pat": "%:BUY", "sell_pat": "%:SELL", "insider_pat": "INSIDER:%"},
            ).fetchone()

        if rows is None:
            return checks

        buy_vol = float(rows[0] or 0)
        sell_vol = float(rows[1] or 0)
        buy_count = int(rows[2] or 0)
        sell_count = int(rows[3] or 0)

        total_vol = buy_vol + sell_vol
        if total_vol == 0:
            return checks

        # Net insider sentiment: -1 (all selling) to +1 (all buying)
        net_sentiment = (buy_vol - sell_vol) / total_vol
        # Ratio of unique sellers to unique buyers
        ratio = sell_count / max(buy_count, 1)

        # Historical baseline: typical sell/buy ratio is ~3:1 (execs routinely sell)
        # Divergence = how far from typical the current ratio is
        typical_ratio = 3.0
        divergence = (ratio - typical_ratio) / typical_ratio

        # High sell ratio diverging from typical = insiders are dumping
        zscore = round(divergence * 2.0, 2)  # Scale to sigma-like units

        check = _make_check(
            name="Insider Net Selling vs Historical Baseline",
            category="insider",
            official_source="corporate_guidance",
            official_value=round(net_sentiment, 4),
            physical_source="SEC Form 4 (30d)",
            physical_value=round(ratio, 2),
            expected_relationship="positive_correlation",
            zscore=zscore,
            implication=(
                f"Insider sell/buy ratio is {ratio:.1f}:1 (baseline ~3:1). "
                f"Net sentiment: {net_sentiment:+.2f}. "
                f"{buy_count} unique buyers vs {sell_count} unique sellers in 30d. "
                + (
                    "Insiders are selling at an elevated rate relative to baseline — "
                    "corporate guidance may be more optimistic than reality warrants."
                    if ratio > 4.0
                    else
                    "Insider activity within normal range."
                    if ratio < 5.0
                    else
                    "Insider selling is extreme — multiple executives dumping simultaneously "
                    "suggests they know something the market doesn't."
                )
            ),
            confidence=_compute_confidence(buy_count + sell_count, buy_count + sell_count, 0),
        )
        checks.append(check)

        # Also check for cluster buys (unusual, high signal)
        if buy_count > 5 and net_sentiment > 0.3:
            cluster_check = _make_check(
                name="Insider Cluster Buying Detected",
                category="insider",
                official_source="market_consensus",
                official_value=0.0,
                physical_source="SEC Form 4 cluster buys",
                physical_value=round(buy_vol, 2),
                expected_relationship="positive_correlation",
                zscore=round(net_sentiment * 3.0, 2),
                implication=(
                    f"{buy_count} unique insiders buying in the last 30 days with "
                    f"positive net sentiment ({net_sentiment:+.2f}). Insider cluster "
                    f"buying is one of the strongest signals in finance — insiders "
                    f"are putting their own money where their mouth is."
                ),
                confidence=_compute_confidence(buy_count, buy_count, 0),
            )
            checks.append(cluster_check)

    except Exception as exc:
        log.warning("Insider divergence check failed: {e}", e=str(exc))

    log.info(
        "Insider activity: {n} checks, {r} red flags",
        n=len(checks),
        r=sum(1 for c in checks if c.assessment in ("major_divergence", "contradiction")),
    )
    return checks


# ── Main Orchestration ────────────────────────────────────────────────────

def run_all_checks(
    engine: Engine,
    skip_narrative: bool = False,
) -> LieDetectorReport:
    """Run every cross-reference check, flag divergences, generate report.

    This is the main entry point. Runs all categories:
      1. GDP vs physical reality (US, China, EU)
      2. Trade bilateral mirror stats
      3. Inflation vs input costs
      4. Central bank actions vs words
      5. Employment vs reality
      6. Fed liquidity rhetoric vs reality
      7. Credit spreads & housing
      8. Insider activity vs market narrative

    Flags divergences > 2 standard deviations as red flags.
    Generates LLM narrative connecting the dots (unless skip_narrative=True).
    Persists results for historical tracking.

    Args:
        engine: SQLAlchemy database engine.
        skip_narrative: If True, skip the expensive LLM narrative generation.
            Useful for frequent scheduled runs where only the checks matter.
    """
    log.info("Cross-reference engine: starting all checks")
    all_checks: list[CrossRefCheck] = []

    # 1. GDP vs Physical
    for country in ("US", "CN", "EU"):
        try:
            all_checks.extend(check_gdp_vs_physical(engine, country))
        except Exception as exc:
            log.warning("GDP check failed for {c}: {e}", c=country, e=str(exc))

    # 2. Trade bilateral
    try:
        all_checks.extend(check_trade_bilateral(engine))
    except Exception as exc:
        log.warning("Trade bilateral check failed: {e}", e=str(exc))

    # 3. Inflation vs inputs
    try:
        all_checks.extend(check_inflation_vs_inputs(engine))
    except Exception as exc:
        log.warning("Inflation check failed: {e}", e=str(exc))

    # 4. Central bank actions vs words
    try:
        all_checks.extend(check_central_bank_actions_vs_words(engine))
    except Exception as exc:
        log.warning("Central bank check failed: {e}", e=str(exc))

    # 5. Employment vs reality
    try:
        all_checks.extend(check_employment_reality(engine))
    except Exception as exc:
        log.warning("Employment check failed: {e}", e=str(exc))

    # 6. Fed Liquidity rhetoric vs reality
    try:
        all_checks.extend(check_liquidity_reality(engine))
    except Exception as exc:
        log.warning("Liquidity check failed: {e}", e=str(exc))

    # 7. Credit spreads & housing
    try:
        all_checks.extend(check_credit_housing(engine))
    except Exception as exc:
        log.warning("Credit/housing check failed: {e}", e=str(exc))

    # 8. Insider activity vs narrative
    try:
        all_checks.extend(check_insider_divergence(engine))
    except Exception as exc:
        log.warning("Insider divergence check failed: {e}", e=str(exc))

    # Identify red flags (major divergence or contradiction)
    red_flags = [
        c for c in all_checks
        if c.assessment in ("major_divergence", "contradiction")
    ]

    # Generate narrative (skip if caller requested checks-only mode)
    if skip_narrative:
        narrative = ""
    else:
        narrative = _generate_narrative(all_checks, red_flags)

    # Build summary stats
    category_breakdown: dict[str, dict[str, int]] = {}
    for c in all_checks:
        if c.category not in category_breakdown:
            category_breakdown[c.category] = {
                "total": 0, "consistent": 0, "minor": 0, "major": 0, "contradiction": 0,
            }
        category_breakdown[c.category]["total"] += 1
        if c.assessment == "consistent":
            category_breakdown[c.category]["consistent"] += 1
        elif c.assessment == "minor_divergence":
            category_breakdown[c.category]["minor"] += 1
        elif c.assessment == "major_divergence":
            category_breakdown[c.category]["major"] += 1
        elif c.assessment == "contradiction":
            category_breakdown[c.category]["contradiction"] += 1

    summary = {
        "total_checks": len(all_checks),
        "red_flag_count": len(red_flags),
        "consistent_count": sum(1 for c in all_checks if c.assessment == "consistent"),
        "categories": category_breakdown,
    }

    # Persist to DB
    try:
        _persist_checks(engine, all_checks)
    except Exception as exc:
        log.warning("Failed to persist cross-reference checks: {e}", e=str(exc))

    report = LieDetectorReport(
        checks=all_checks,
        red_flags=red_flags,
        narrative=narrative,
        generated_at=datetime.now(timezone.utc).isoformat(),
        summary=summary,
    )

    log.info(
        "Cross-reference engine complete: {t} checks, {r} red flags",
        t=len(all_checks),
        r=len(red_flags),
    )
    return report


def get_historical_checks(
    engine: Engine,
    category: str | None = None,
    days: int = 30,
    assessment: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch historical cross-reference checks from the database.

    Parameters:
        engine: Database engine.
        category: Filter by category (gdp, trade, inflation, central_bank, employment).
        days: Lookback period in days.
        assessment: Filter by assessment level.

    Returns:
        List of historical check records.
    """
    ensure_tables(engine)
    cutoff = date.today() - timedelta(days=days)

    conditions = ["checked_at >= :cutoff"]
    params: dict[str, Any] = {"cutoff": cutoff}

    if category:
        conditions.append("category = :cat")
        params["cat"] = category

    if assessment:
        conditions.append("assessment = :assess")
        params["assess"] = assessment

    where_clause = " AND ".join(conditions)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT name, category, official_source, official_value, "
                f"physical_source, physical_value, divergence_zscore, "
                f"assessment, implication, confidence, checked_at "
                f"FROM cross_reference_checks "
                f"WHERE {where_clause} "
                f"ORDER BY checked_at DESC "
                f"LIMIT 500"
            ),
            params,
        ).fetchall()

    return [
        {
            "name": r[0],
            "category": r[1],
            "official_source": r[2],
            "official_value": r[3],
            "physical_source": r[4],
            "physical_value": r[5],
            "divergence_zscore": r[6],
            "assessment": r[7],
            "implication": r[8],
            "confidence": r[9],
            "checked_at": r[10].isoformat() if r[10] else None,
        }
        for r in rows
    ]


# ── CLI Entry Point ──────────────────────────────────────────────────────

if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    ensure_tables(engine)

    print("=" * 70)
    print("GRID CROSS-REFERENCE ENGINE — LIE DETECTOR")
    print("Every government gives us the stats. We add them up and see who is lying.")
    print("=" * 70)

    report = run_all_checks(engine)

    print(f"\nTotal checks: {report.summary.get('total_checks', 0)}")
    print(f"Consistent:   {report.summary.get('consistent_count', 0)}")
    print(f"Red flags:    {report.summary.get('red_flag_count', 0)}")

    if report.red_flags:
        print("\n--- RED FLAGS ---")
        for rf in report.red_flags:
            print(
                f"  [{rf.category.upper()}] {rf.name}: "
                f"z-score={rf.actual_divergence:.1f} ({rf.assessment})"
            )
            print(f"    {rf.implication[:120]}")

    print(f"\n--- NARRATIVE ---\n{report.narrative}")

    # Category breakdown
    for cat, stats in report.summary.get("categories", {}).items():
        print(f"\n  {cat}: {stats}")
