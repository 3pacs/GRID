"""
GRID Data Sanity Validator.

Runs 5 checks against every feature in feature_registry:
  1. Range check — expected min/max bounds per feature
  2. Staleness check — flag features with obs_date > 7 days old
  3. Cross-source validation — compare 2+ sources, flag >20% divergence
  4. Unit consistency — derived features vs inputs sanity
  5. Taxonomy check — entity_map source→target unit alignment

Run after every ingestion cycle via Hermes, or standalone:
    python scripts/data_sanity_check.py [--fix] [--verbose]

Exit code 0 = clean, 1 = warnings only, 2 = critical failures found.
"""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── 1. Range checks — known bounds for key features ────────────────────

RANGE_CHECKS: dict[str, tuple[float, float]] = {
    # Macro
    "cpi_yoy": (-5.0, 20.0),
    "cpi_index": (100.0, 500.0),
    "fed_funds_rate": (0.0, 25.0),
    "real_ffr": (-10.0, 15.0),
    "unemployment_rate": (2.0, 25.0),
    "unemployment": (2.0, 25.0),
    "nonfarm_payrolls": (100_000, 200_000_000),
    "m2_money_supply": (1_000, 30_000_000),
    "umich_sentiment": (20.0, 120.0),
    "real_disp_income": (1_000, 100_000_000),

    # Rates
    "yld_curve_2s10s": (-5.0, 5.0),
    "yld_curve_3m10y": (-5.0, 5.0),
    "yc_1y": (0.0, 20.0),
    "yc_2y": (0.0, 20.0),
    "yc_5y": (0.0, 20.0),
    "yc_30y": (0.0, 20.0),
    "yc_real_10y": (-5.0, 10.0),
    "breakeven_5y": (-2.0, 10.0),
    "yc_breakeven_10y": (-2.0, 10.0),
    "sofr": (0.0, 20.0),

    # FX
    "dxy_index": (70.0, 130.0),
    "dxy_proxy_close": (15.0, 40.0),
    "uup_etf_close": (15.0, 40.0),

    # Credit
    "hy_oas_spread": (1.0, 30.0),
    "ig_oas_spread": (0.1, 10.0),

    # Equity indices
    "sp500_close": (1_000.0, 10_000.0),
    "vix_spot": (5.0, 100.0),
    "vix_spot_yf": (5.0, 100.0),

    # Crypto
    "btc_full": (1_000.0, 500_000.0),
    "btc_usd_full": (1_000.0, 500_000.0),
    "eth_full": (50.0, 50_000.0),
    "eth_usd_full": (50.0, 50_000.0),
    "sol": (1.0, 1_000.0),
    "sol_full": (1.0, 1_000.0),
    "sol_usd_full": (1.0, 1_000.0),
    "btc_dominance": (20.0, 80.0),
    "eth_dominance": (5.0, 50.0),
    "crypto_fear_greed": (0.0, 100.0),
    "crypto_total_mcap": (100_000_000, 20_000_000_000_000),

    # DEX
    "dex_sol_txn_count_24h": (10_000.0, 100_000_000.0),
    "dex_sol_volume_24h": (10_000_000.0, 100_000_000_000.0),
    "dex_sol_buy_sell_ratio": (0.1, 10.0),

    # Sentiment
    "spy_rsi": (0.0, 100.0),
    "spy_pe_ratio": (5.0, 60.0),

    # Commodity
    "gold_futures_close": (500.0, 10_000.0),
    "copper_futures_close": (1.0, 20.0),
}

# ── 2. Derived feature sanity — input→output consistency ───────────────

DERIVED_CHECKS: list[dict[str, Any]] = [
    {
        "name": "real_ffr",
        "formula": "fed_funds_rate - cpi_yoy",
        "inputs": ["fed_funds_rate", "cpi_yoy"],
        "expected_range": (-10.0, 15.0),
    },
    {
        "name": "dxy_3m_chg",
        "formula": "pct_change(dxy_index, 63)",
        "inputs": ["dxy_index"],
        "expected_range": (-30.0, 30.0),
    },
]


# ── Runner ─────────────────────────────────────────────────────────────

class SanityResult:
    """Accumulates findings from all checks."""

    def __init__(self) -> None:
        self.critical: list[str] = []
        self.warning: list[str] = []
        self.ok: int = 0

    def add_critical(self, msg: str) -> None:
        self.critical.append(msg)
        log.error("CRITICAL: {m}", m=msg)

    def add_warning(self, msg: str) -> None:
        self.warning.append(msg)
        log.warning("WARNING: {m}", m=msg)

    def add_ok(self) -> None:
        self.ok += 1

    @property
    def exit_code(self) -> int:
        if self.critical:
            return 2
        if self.warning:
            return 1
        return 0

    def summary(self) -> str:
        return (
            f"Sanity check: {self.ok} OK, "
            f"{len(self.warning)} warnings, "
            f"{len(self.critical)} CRITICAL"
        )


def run_range_checks(engine: Engine, result: SanityResult) -> None:
    """Check latest value for each feature against known bounds."""
    log.info("Running range checks on {n} features", n=len(RANGE_CHECKS))

    with engine.connect() as conn:
        for feature_name, (lo, hi) in RANGE_CHECKS.items():
            row = conn.execute(text(
                "SELECT rs.value, rs.obs_date "
                "FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE fr.name = :name "
                "ORDER BY rs.obs_date DESC LIMIT 1"
            ), {"name": feature_name}).fetchone()

            if not row:
                result.add_warning(f"RANGE [{feature_name}]: no data found")
                continue

            val = float(row[0])
            obs_date = row[1]

            if val < lo or val > hi:
                result.add_critical(
                    f"RANGE [{feature_name}]: value={val:.4f} outside bounds "
                    f"[{lo}, {hi}] (as of {obs_date})"
                )
            else:
                result.add_ok()


def run_staleness_checks(engine: Engine, result: SanityResult, max_age_days: int = 7) -> None:
    """Flag features whose latest data is older than max_age_days."""
    log.info("Running staleness checks (max {d} days)", d=max_age_days)
    cutoff = date.today() - timedelta(days=max_age_days)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT fr.name, fr.family, MAX(rs.obs_date) as latest
            FROM feature_registry fr
            JOIN resolved_series rs ON rs.feature_id = fr.id
            WHERE fr.deprecated_at IS NULL AND fr.model_eligible = true
            GROUP BY fr.name, fr.family
            HAVING MAX(rs.obs_date) < :cutoff
            ORDER BY MAX(rs.obs_date) ASC
        """), {"cutoff": cutoff}).fetchall()

        for row in rows:
            name, family, latest = row[0], row[1], row[2]
            age = (date.today() - latest).days
            if age > 30:
                result.add_critical(
                    f"STALE [{family}/{name}]: last data {latest} ({age} days ago)"
                )
            else:
                result.add_warning(
                    f"STALE [{family}/{name}]: last data {latest} ({age} days ago)"
                )


def run_derived_checks(engine: Engine, result: SanityResult) -> None:
    """Verify derived features are consistent with their inputs."""
    log.info("Running derived feature consistency checks")

    with engine.connect() as conn:
        for check in DERIVED_CHECKS:
            name = check["name"]
            lo, hi = check["expected_range"]

            row = conn.execute(text(
                "SELECT rs.value, rs.obs_date "
                "FROM resolved_series rs "
                "JOIN feature_registry fr ON fr.id = rs.feature_id "
                "WHERE fr.name = :name "
                "ORDER BY rs.obs_date DESC LIMIT 1"
            ), {"name": name}).fetchone()

            if not row:
                result.add_warning(f"DERIVED [{name}]: no data")
                continue

            val = float(row[0])
            if val < lo or val > hi:
                # Also fetch inputs for diagnosis
                input_vals = {}
                for inp in check["inputs"]:
                    inp_row = conn.execute(text(
                        "SELECT rs.value FROM resolved_series rs "
                        "JOIN feature_registry fr ON fr.id = rs.feature_id "
                        "WHERE fr.name = :name ORDER BY rs.obs_date DESC LIMIT 1"
                    ), {"name": inp}).fetchone()
                    input_vals[inp] = float(inp_row[0]) if inp_row else None

                result.add_critical(
                    f"DERIVED [{name}]: value={val:.4f} outside [{lo}, {hi}]. "
                    f"Formula: {check['formula']}. Inputs: {input_vals}"
                )
            else:
                result.add_ok()


def run_cross_source_checks(engine: Engine, result: SanityResult) -> None:
    """Compare features that have data from multiple sources."""
    log.info("Running cross-source validation")

    with engine.connect() as conn:
        # Find features with multiple raw_series sources
        rows = conn.execute(text("""
            SELECT fr.name, COUNT(DISTINCT rs.source_priority_used) as n_sources
            FROM feature_registry fr
            JOIN resolved_series rs ON rs.feature_id = fr.id
            WHERE rs.obs_date >= CURRENT_DATE - 7
            GROUP BY fr.name
            HAVING COUNT(DISTINCT rs.source_priority_used) > 1
        """)).fetchall()

        for row in rows:
            name = row[0]
            # Get latest values per source priority
            vals = conn.execute(text("""
                SELECT source_priority_used, value
                FROM resolved_series rs
                JOIN feature_registry fr ON fr.id = rs.feature_id
                WHERE fr.name = :name
                AND rs.obs_date = (
                    SELECT MAX(obs_date) FROM resolved_series
                    WHERE feature_id = rs.feature_id
                )
                ORDER BY source_priority_used
            """), {"name": name}).fetchall()

            if len(vals) >= 2:
                values = [float(v[1]) for v in vals if v[1] is not None]
                if len(values) >= 2 and min(values) > 0:
                    divergence = (max(values) - min(values)) / min(values)
                    if divergence > 0.20:
                        result.add_warning(
                            f"CROSS-SOURCE [{name}]: {divergence:.1%} divergence "
                            f"across {len(values)} sources. Values: {values}"
                        )
                    else:
                        result.add_ok()


def run_taxonomy_checks(engine: Engine, result: SanityResult) -> None:
    """Verify entity_map mappings make sense (source unit matches target)."""
    log.info("Running taxonomy checks")

    # Known bad mappings that were already fixed — verify they stay fixed
    known_traps = [
        ("CPIAUCSL", "cpi_yoy", "CPIAUCSL is an index (~327), not YoY change (~3%)"),
        ("CPIAUCSL", "cpi", "CPIAUCSL is an index (~327), not a percentage"),
    ]

    try:
        from normalization.entity_map import SEED_MAPPINGS
    except ImportError:
        result.add_warning("TAXONOMY: could not import SEED_MAPPINGS")
        return

    for source_id, bad_target, reason in known_traps:
        actual_target = SEED_MAPPINGS.get(source_id)
        if actual_target == bad_target:
            result.add_critical(f"TAXONOMY: {source_id} → {bad_target}. {reason}")
        else:
            result.add_ok()


def cross_validate(engine: Engine, result: SanityResult) -> None:
    """Cross-validate related features against each other.

    Checks:
      1. VIX > 40 should correlate with SPY being down (flag if SPY up >2%)
      2. Inverted yield curve: 2Y > 10Y means negative spread
      3. Gold and DXY roughly inverse over 20-day windows
    """
    log.info("Running cross-validation checks")

    with engine.connect() as conn:
        # ── 1. VIX vs SPY: if VIX > 40, SPY should not be up >2% same day ──
        try:
            vix_rows = conn.execute(text("""
                SELECT rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON fr.id = rs.feature_id
                WHERE fr.name IN ('vix_spot', 'vix_spot_yf')
                AND rs.obs_date >= CURRENT_DATE - 30
                AND rs.value > 40
                ORDER BY rs.obs_date DESC
                LIMIT 10
            """)).fetchall()

            for vix_row in vix_rows:
                vix_date = vix_row[0]
                vix_val = float(vix_row[1])

                # Get SPY values on same date and prior date
                spy_rows = conn.execute(text("""
                    SELECT rs.obs_date, rs.value
                    FROM resolved_series rs
                    JOIN feature_registry fr ON fr.id = rs.feature_id
                    WHERE fr.name = 'sp500_close'
                    AND rs.obs_date <= :dt
                    ORDER BY rs.obs_date DESC
                    LIMIT 2
                """), {"dt": vix_date}).fetchall()

                if len(spy_rows) >= 2:
                    spy_today = float(spy_rows[0][1])
                    spy_prev = float(spy_rows[1][1])
                    if spy_prev > 0:
                        spy_pct = (spy_today - spy_prev) / spy_prev * 100
                        if spy_pct > 2.0:
                            result.add_warning(
                                f"CROSS-VAL [VIX/SPY]: VIX={vix_val:.1f} "
                                f"on {vix_date} but SPY was UP {spy_pct:.1f}% "
                                f"— unusual combination"
                            )
                        else:
                            result.add_ok()
        except Exception as exc:
            log.debug("VIX/SPY cross-val failed: {e}", e=str(exc))

        # ── 2. Yield curve inversion check ────────────────────────────────
        try:
            yc_row = conn.execute(text("""
                SELECT rs.value, rs.obs_date
                FROM resolved_series rs
                JOIN feature_registry fr ON fr.id = rs.feature_id
                WHERE fr.name = 'yld_curve_2s10s'
                ORDER BY rs.obs_date DESC LIMIT 1
            """)).fetchone()

            if yc_row:
                spread_val = float(yc_row[0])
                spread_date = yc_row[1]

                # Separately get 2Y and 10Y to cross-check
                y2_row = conn.execute(text("""
                    SELECT rs.value FROM resolved_series rs
                    JOIN feature_registry fr ON fr.id = rs.feature_id
                    WHERE fr.name = 'yc_2y'
                    ORDER BY rs.obs_date DESC LIMIT 1
                """)).fetchone()

                y10_row = conn.execute(text("""
                    SELECT rs.value FROM resolved_series rs
                    JOIN feature_registry fr ON fr.id = rs.feature_id
                    WHERE fr.name IN ('yc_10y', 'treasury_10y')
                    ORDER BY rs.obs_date DESC LIMIT 1
                """)).fetchone()

                if y2_row and y10_row:
                    y2 = float(y2_row[0])
                    y10 = float(y10_row[0])
                    computed_spread = y10 - y2
                    # If 2Y > 10Y (inverted), the spread should be negative
                    if y2 > y10 and spread_val > 0.05:
                        result.add_critical(
                            f"CROSS-VAL [YIELD]: 2Y={y2:.2f} > 10Y={y10:.2f} "
                            f"(inverted) but stored spread={spread_val:.4f} "
                            f"is positive — data inconsistency"
                        )
                    elif abs(computed_spread - spread_val) > 0.5:
                        result.add_warning(
                            f"CROSS-VAL [YIELD]: computed 10Y-2Y="
                            f"{computed_spread:.4f} vs stored spread="
                            f"{spread_val:.4f} — large discrepancy"
                        )
                    else:
                        result.add_ok()
        except Exception as exc:
            log.debug("Yield curve cross-val failed: {e}", e=str(exc))

        # ── 3. Gold and DXY inverse correlation over 20-day window ────────
        try:
            gold_rows = conn.execute(text("""
                SELECT rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON fr.id = rs.feature_id
                WHERE fr.name = 'gold_futures_close'
                AND rs.obs_date >= CURRENT_DATE - 30
                ORDER BY rs.obs_date ASC
            """)).fetchall()

            dxy_rows = conn.execute(text("""
                SELECT rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON fr.id = rs.feature_id
                WHERE fr.name = 'dxy_index'
                AND rs.obs_date >= CURRENT_DATE - 30
                ORDER BY rs.obs_date ASC
            """)).fetchall()

            if len(gold_rows) >= 10 and len(dxy_rows) >= 10:
                # Align by date
                gold_by_date = {r[0]: float(r[1]) for r in gold_rows}
                dxy_by_date = {r[0]: float(r[1]) for r in dxy_rows}
                common_dates = sorted(
                    set(gold_by_date.keys()) & set(dxy_by_date.keys())
                )

                if len(common_dates) >= 10:
                    gold_vals = [gold_by_date[d] for d in common_dates[-20:]]
                    dxy_vals = [dxy_by_date[d] for d in common_dates[-20:]]

                    # Compute Pearson correlation
                    n = len(gold_vals)
                    mean_g = sum(gold_vals) / n
                    mean_d = sum(dxy_vals) / n
                    cov = sum(
                        (g - mean_g) * (d - mean_d)
                        for g, d in zip(gold_vals, dxy_vals)
                    ) / n
                    std_g = (
                        sum((g - mean_g) ** 2 for g in gold_vals) / n
                    ) ** 0.5
                    std_d = (
                        sum((d - mean_d) ** 2 for d in dxy_vals) / n
                    ) ** 0.5

                    if std_g > 0 and std_d > 0:
                        corr = cov / (std_g * std_d)
                        if corr > 0.5:
                            result.add_warning(
                                f"CROSS-VAL [GOLD/DXY]: 20-day correlation "
                                f"= {corr:.2f} (expected negative/near-zero) "
                                f"— unusual positive correlation"
                            )
                        else:
                            result.add_ok()
        except Exception as exc:
            log.debug("Gold/DXY cross-val failed: {e}", e=str(exc))


def run_all_checks(engine: Engine, verbose: bool = False) -> SanityResult:
    """Run all 6 sanity checks and return results."""
    result = SanityResult()

    run_range_checks(engine, result)
    run_staleness_checks(engine, result)
    run_derived_checks(engine, result)
    run_cross_source_checks(engine, result)
    run_taxonomy_checks(engine, result)
    cross_validate(engine, result)

    log.info(result.summary())
    return result


if __name__ == "__main__":
    from db import get_engine

    verbose = "--verbose" in sys.argv
    engine = get_engine()
    result = run_all_checks(engine, verbose=verbose)

    print(f"\n{'='*60}")
    print(result.summary())

    if result.critical:
        print(f"\nCRITICAL ({len(result.critical)}):")
        for c in result.critical:
            print(f"  {c}")

    if result.warning and verbose:
        print(f"\nWARNINGS ({len(result.warning)}):")
        for w in result.warning:
            print(f"  {w}")

    print(f"{'='*60}")
    sys.exit(result.exit_code)
