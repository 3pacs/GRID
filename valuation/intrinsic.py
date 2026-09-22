"""
GRID — Intrinsic Value Engine.

Computes multiple intrinsic value estimates from balance sheet,
income statement, and cash flow data pulled via FMP and Tiingo.

Valuation methods:
  1. Book Value per Share (equity / shares)
  2. Tangible Book per Share (equity - intangibles - goodwill) / shares
  3. Net Current Asset Value (Graham's NCAV: current assets - total liabilities)
  4. Net Cash per Share (cash - total debt)
  5. Liquidation Value (conservative asset haircuts)
  6. Earnings Power Value (normalized earnings / cost of capital)
  7. Owner Earnings (Buffett: net income + D&A - maintenance capex)
  8. Simple DCF (10-year free cash flow projection)
  9. EV/EBITDA multiple

All methods operate on the most recent filing data available as of
the valuation date, respecting PIT correctness.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class FinancialInputs:
    """Snapshot of financial data needed for valuation.

    All dollar values in millions unless noted.
    """

    ticker: str
    filing_date: date
    period: str  # 'Q1', 'Q2', etc.

    # Balance sheet
    total_assets: float | None = None
    total_current_assets: float | None = None
    total_current_liabilities: float | None = None
    total_debt: float | None = None
    cash: float | None = None
    total_equity: float | None = None
    intangible_assets: float | None = None
    goodwill: float | None = None
    inventory: float | None = None
    receivables: float | None = None

    # Income statement (trailing 4Q or annual)
    revenue: float | None = None
    net_income: float | None = None
    ebitda: float | None = None
    depreciation: float | None = None
    operating_income: float | None = None

    # Cash flow
    operating_cf: float | None = None
    capex: float | None = None
    free_cf: float | None = None

    # Market data
    market_price: float | None = None
    shares_outstanding: float | None = None
    market_cap: float | None = None
    enterprise_value: float | None = None


@dataclass
class ValuationResult:
    """Multi-method intrinsic value estimates for a company."""

    ticker: str
    valuation_date: date
    market_price: float | None
    shares_outstanding: float | None
    market_cap: float | None

    # Per-share intrinsic values
    book_value_ps: float | None = None
    tangible_book_ps: float | None = None
    ncav_ps: float | None = None
    net_cash_ps: float | None = None
    liquidation_ps: float | None = None
    epv_ps: float | None = None
    owner_earnings_ps: float | None = None
    dcf_ps: float | None = None
    ev_ebitda: float | None = None

    # Relative
    pe_ratio: float | None = None
    pb_ratio: float | None = None
    ps_ratio: float | None = None
    peg_ratio: float | None = None

    # Composite
    intrinsic_low: float | None = None
    intrinsic_mid: float | None = None
    intrinsic_high: float | None = None
    margin_of_safety: float | None = None

    data_freshness: str = "CURRENT"
    input_payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v is not None}


# Default cost of capital for EPV (10% = long-run equity return)
_DEFAULT_COC = 0.10
# DCF assumptions
_DCF_YEARS = 10
_DCF_GROWTH_RATE = 0.03  # Conservative 3% terminal growth
_DCF_DISCOUNT_RATE = 0.10


class IntrinsicValueEngine:
    """Computes multiple intrinsic value estimates from financial data.

    Pulls data from raw_series (FMP-sourced balance sheet, income, cash flow)
    and computes per-share intrinsic values using several methods.
    """

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine

    def gather_inputs(self, ticker: str, as_of: date | None = None) -> FinancialInputs | None:
        """Gather the most recent financial data for a ticker.

        Pulls from FMP raw_series data stored by fmp_puller.py.
        Returns None if insufficient data available.
        """
        if as_of is None:
            as_of = date.today()

        # Map of series_suffix -> FinancialInputs field name
        bs_fields = {
            "total_assets": "total_assets",
            "total_debt": "total_debt",
            "cash": "cash",
            "equity": "total_equity",
            "current_assets": "total_current_assets",
            "current_liabilities": "total_current_liabilities",
        }
        income_fields = {
            "revenue": "revenue",
            "net_income": "net_income",
            "ebitda": "ebitda",
            "operating_income": "operating_income",
            "gross_profit": "gross_profit",
        }
        cf_fields = {
            "operating_cf": "operating_cf",
            "capex": "capex",
            "free_cf": "free_cf",
        }
        market_fields = {
            "market_cap": "market_cap",
            "price": "market_price",
            "pe_ratio": "pe_ratio",
        }

        values: dict[str, Any] = {"ticker": ticker, "filing_date": as_of, "period": "TTM"}

        with self.engine.connect() as conn:
            # Pull latest value for each FMP series
            all_fields = {
                **{f"fmp:{ticker}:{k}": v for k, v in {**bs_fields, **income_fields, **cf_fields}.items()},
                **{f"fmp:{ticker}:{k}": v for k, v in market_fields.items()},
            }

            for series_id, field_name in all_fields.items():
                row = conn.execute(
                    text("""
                        SELECT value, obs_date
                        FROM raw_series
                        WHERE series_id = :sid AND obs_date <= :as_of
                        ORDER BY obs_date DESC, pull_timestamp DESC
                        LIMIT 1
                    """),
                    {"sid": series_id, "as_of": as_of},
                ).fetchone()

                if row is not None:
                    values[field_name] = row[0]
                    # Track data staleness
                    if row[1] < as_of - timedelta(days=120):
                        values["data_freshness"] = "STALE"

            # Also try Tiingo fundamentals for market data
            for tiingo_suffix, field_name in [
                ("market_cap", "market_cap"),
                ("pe_ratio", "pe_ratio"),
                ("pb_ratio", "pb_ratio"),
                ("peg_ratio", "peg_ratio"),
                ("enterprise_value", "enterprise_value"),
            ]:
                if field_name in values and values[field_name] is not None:
                    continue
                series_id = f"TIINGO_FUND:{ticker}:{tiingo_suffix}"
                row = conn.execute(
                    text("""
                        SELECT value, obs_date
                        FROM raw_series
                        WHERE series_id = :sid AND obs_date <= :as_of
                        ORDER BY obs_date DESC, pull_timestamp DESC
                        LIMIT 1
                    """),
                    {"sid": series_id, "as_of": as_of},
                ).fetchone()
                if row is not None:
                    values[field_name] = row[0]

            # Derive shares outstanding from market cap / price
            if values.get("market_cap") and values.get("market_price") and values["market_price"] > 0:
                values["shares_outstanding"] = values["market_cap"] / values["market_price"]

        # Need at least some balance sheet data
        if values.get("total_equity") is None and values.get("total_assets") is None:
            log.warning("Insufficient financial data for {t}", t=ticker)
            return None

        # Filter to only FinancialInputs fields
        valid_fields = {f.name for f in FinancialInputs.__dataclass_fields__.values()}
        filtered = {k: v for k, v in values.items() if k in valid_fields}
        return FinancialInputs(**filtered)

    def compute(self, inputs: FinancialInputs) -> ValuationResult:
        """Compute all intrinsic value methods from financial inputs."""
        shares = inputs.shares_outstanding
        result = ValuationResult(
            ticker=inputs.ticker,
            valuation_date=inputs.filing_date,
            market_price=inputs.market_price,
            shares_outstanding=shares,
            market_cap=inputs.market_cap,
        )

        if not shares or shares <= 0:
            log.warning("No shares outstanding for {t}, limited valuation", t=inputs.ticker)
            result.data_freshness = "ESTIMATED"
            return result

        # 1. Book Value per Share
        if inputs.total_equity is not None:
            result.book_value_ps = inputs.total_equity / shares

        # 2. Tangible Book per Share
        if inputs.total_equity is not None:
            intangibles = (inputs.intangible_assets or 0) + (inputs.goodwill or 0)
            tangible_equity = inputs.total_equity - intangibles
            result.tangible_book_ps = tangible_equity / shares

        # 3. NCAV (Graham's Net Current Asset Value)
        if inputs.total_current_assets is not None:
            total_liab = (inputs.total_assets or 0) - (inputs.total_equity or 0)
            ncav = inputs.total_current_assets - total_liab
            result.ncav_ps = ncav / shares

        # 4. Net Cash per Share
        if inputs.cash is not None:
            net_cash = inputs.cash - (inputs.total_debt or 0)
            result.net_cash_ps = net_cash / shares

        # 5. Liquidation Value (conservative)
        if inputs.total_current_assets is not None:
            receivables_val = (inputs.receivables or 0) * 0.80
            inventory_val = (inputs.inventory or 0) * 0.50
            cash_val = inputs.cash or 0
            # Other current assets at 20% haircut
            other_current = max(0, inputs.total_current_assets
                                - (inputs.receivables or 0)
                                - (inputs.inventory or 0)
                                - (inputs.cash or 0))
            other_val = other_current * 0.20
            total_liab = (inputs.total_assets or 0) - (inputs.total_equity or 0)
            liquidation = receivables_val + inventory_val + cash_val + other_val - total_liab
            result.liquidation_ps = liquidation / shares

        # 6. Earnings Power Value (EPV)
        if inputs.operating_income is not None:
            # Normalize: use operating income * (1 - tax_rate) as sustainable earnings
            tax_rate = 0.21  # US corporate
            normalized_earnings = inputs.operating_income * (1 - tax_rate)
            if normalized_earnings > 0:
                epv = normalized_earnings / _DEFAULT_COC
                # Add excess cash, subtract debt
                epv += (inputs.cash or 0) - (inputs.total_debt or 0)
                result.epv_ps = epv / shares

        # 7. Owner Earnings (Buffett method)
        if inputs.net_income is not None:
            depreciation = inputs.depreciation or 0
            # Maintenance capex ~ 70% of total capex (heuristic)
            maintenance_capex = abs(inputs.capex or 0) * 0.70
            owner_earnings = inputs.net_income + depreciation - maintenance_capex
            if owner_earnings > 0:
                result.owner_earnings_ps = owner_earnings / shares

        # 8. Simple DCF (10-year free cash flow)
        if inputs.free_cf is not None and inputs.free_cf > 0:
            dcf_value = self._simple_dcf(
                base_fcf=inputs.free_cf,
                growth_rate=_DCF_GROWTH_RATE,
                discount_rate=_DCF_DISCOUNT_RATE,
                years=_DCF_YEARS,
            )
            # Add net cash
            dcf_value += (inputs.cash or 0) - (inputs.total_debt or 0)
            result.dcf_ps = dcf_value / shares

        # 9. EV/EBITDA
        if inputs.enterprise_value and inputs.ebitda and inputs.ebitda > 0:
            result.ev_ebitda = inputs.enterprise_value / inputs.ebitda

        # Relative ratios (pass through from data sources)
        if inputs.market_price and inputs.net_income and inputs.net_income > 0 and shares > 0:
            eps = inputs.net_income / shares
            result.pe_ratio = inputs.market_price / eps if eps > 0 else None

        if inputs.market_price and result.book_value_ps and result.book_value_ps > 0:
            result.pb_ratio = inputs.market_price / result.book_value_ps

        if inputs.market_price and inputs.revenue and inputs.revenue > 0 and shares > 0:
            rev_ps = inputs.revenue / shares
            result.ps_ratio = inputs.market_price / rev_ps

        # Composite: gather all per-share methods
        methods = [v for v in [
            result.book_value_ps,
            result.tangible_book_ps,
            result.epv_ps,
            result.owner_earnings_ps,
            result.dcf_ps,
        ] if v is not None and v > 0]

        if methods:
            result.intrinsic_low = min(methods)
            result.intrinsic_high = max(methods)
            result.intrinsic_mid = statistics.median(methods)

            if inputs.market_price and inputs.market_price > 0 and result.intrinsic_mid > 0:
                result.margin_of_safety = (
                    (result.intrinsic_mid - inputs.market_price) / result.intrinsic_mid
                )

        result.input_payload = {
            "total_equity": inputs.total_equity,
            "total_assets": inputs.total_assets,
            "total_debt": inputs.total_debt,
            "cash": inputs.cash,
            "net_income": inputs.net_income,
            "free_cf": inputs.free_cf,
            "operating_income": inputs.operating_income,
            "ebitda": inputs.ebitda,
            "capex": inputs.capex,
            "shares": shares,
        }

        return result

    def valuate(self, ticker: str, as_of: date | None = None) -> ValuationResult | None:
        """Full pipeline: gather inputs -> compute values -> return result."""
        inputs = self.gather_inputs(ticker, as_of)
        if inputs is None:
            return None
        return self.compute(inputs)

    def valuate_and_store(self, ticker: str, as_of: date | None = None) -> ValuationResult | None:
        """Compute valuation and persist to company_valuations table."""
        result = self.valuate(ticker, as_of)
        if result is None:
            return None
        self._store(result)
        return result

    def get_history(self, ticker: str, days: int = 365) -> list[dict[str, Any]]:
        """Retrieve valuation history for a ticker."""
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT valuation_date, market_price, intrinsic_mid, margin_of_safety,
                           book_value_ps, tangible_book_ps, epv_ps, dcf_ps,
                           net_cash_ps, ncav_ps, owner_earnings_ps
                    FROM company_valuations
                    WHERE ticker = :ticker
                      AND valuation_date >= CURRENT_DATE - :days
                    ORDER BY valuation_date
                """),
                {"ticker": ticker.upper(), "days": days},
            ).fetchall()

        return [
            {
                "date": str(r[0]), "market_price": r[1], "intrinsic_mid": r[2],
                "margin_of_safety": r[3], "book_value_ps": r[4],
                "tangible_book_ps": r[5], "epv_ps": r[6], "dcf_ps": r[7],
                "net_cash_ps": r[8], "ncav_ps": r[9], "owner_earnings_ps": r[10],
            }
            for r in rows
        ]

    def _store(self, result: ValuationResult) -> None:
        """Persist a ValuationResult to the database."""
        import json
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO company_valuations (
                        ticker, valuation_date, market_price, shares_outstanding, market_cap,
                        book_value_ps, tangible_book_ps, ncav_ps, net_cash_ps, liquidation_ps,
                        epv_ps, owner_earnings_ps, dcf_ps, ev_ebitda,
                        pe_ratio, pb_ratio, ps_ratio, peg_ratio,
                        intrinsic_low, intrinsic_mid, intrinsic_high, margin_of_safety,
                        data_freshness, input_payload
                    ) VALUES (
                        :ticker, :vdate, :mprice, :shares, :mcap,
                        :bv, :tbv, :ncav, :ncash, :liq,
                        :epv, :oe, :dcf, :eve,
                        :pe, :pb, :ps, :peg,
                        :ilow, :imid, :ihigh, :mos,
                        :fresh, :payload::jsonb
                    )
                    ON CONFLICT (ticker, valuation_date) DO UPDATE SET
                        market_price = EXCLUDED.market_price,
                        shares_outstanding = EXCLUDED.shares_outstanding,
                        market_cap = EXCLUDED.market_cap,
                        book_value_ps = EXCLUDED.book_value_ps,
                        tangible_book_ps = EXCLUDED.tangible_book_ps,
                        ncav_ps = EXCLUDED.ncav_ps,
                        net_cash_ps = EXCLUDED.net_cash_ps,
                        liquidation_ps = EXCLUDED.liquidation_ps,
                        epv_ps = EXCLUDED.epv_ps,
                        owner_earnings_ps = EXCLUDED.owner_earnings_ps,
                        dcf_ps = EXCLUDED.dcf_ps,
                        ev_ebitda = EXCLUDED.ev_ebitda,
                        pe_ratio = EXCLUDED.pe_ratio,
                        pb_ratio = EXCLUDED.pb_ratio,
                        ps_ratio = EXCLUDED.ps_ratio,
                        peg_ratio = EXCLUDED.peg_ratio,
                        intrinsic_low = EXCLUDED.intrinsic_low,
                        intrinsic_mid = EXCLUDED.intrinsic_mid,
                        intrinsic_high = EXCLUDED.intrinsic_high,
                        margin_of_safety = EXCLUDED.margin_of_safety,
                        data_freshness = EXCLUDED.data_freshness,
                        input_payload = EXCLUDED.input_payload
                """),
                {
                    "ticker": result.ticker,
                    "vdate": result.valuation_date,
                    "mprice": result.market_price,
                    "shares": result.shares_outstanding,
                    "mcap": result.market_cap,
                    "bv": result.book_value_ps,
                    "tbv": result.tangible_book_ps,
                    "ncav": result.ncav_ps,
                    "ncash": result.net_cash_ps,
                    "liq": result.liquidation_ps,
                    "epv": result.epv_ps,
                    "oe": result.owner_earnings_ps,
                    "dcf": result.dcf_ps,
                    "eve": result.ev_ebitda,
                    "pe": result.pe_ratio,
                    "pb": result.pb_ratio,
                    "ps": result.ps_ratio,
                    "peg": result.peg_ratio,
                    "ilow": result.intrinsic_low,
                    "imid": result.intrinsic_mid,
                    "ihigh": result.intrinsic_high,
                    "mos": result.margin_of_safety,
                    "fresh": result.data_freshness,
                    "payload": json.dumps(result.input_payload),
                },
            )
        log.info("Stored valuation for {t} @ {d}: mid=${m}", t=result.ticker,
                 d=result.valuation_date, m=result.intrinsic_mid)

    @staticmethod
    def _simple_dcf(
        base_fcf: float,
        growth_rate: float = 0.03,
        discount_rate: float = 0.10,
        years: int = 10,
    ) -> float:
        """Simple 10-year DCF with terminal value."""
        pv_sum = 0.0
        fcf = base_fcf
        for yr in range(1, years + 1):
            fcf *= (1 + growth_rate)
            pv_sum += fcf / (1 + discount_rate) ** yr

        # Terminal value (Gordon growth model)
        terminal_fcf = fcf * (1 + growth_rate)
        terminal_value = terminal_fcf / (discount_rate - growth_rate)
        pv_terminal = terminal_value / (1 + discount_rate) ** years

        return pv_sum + pv_terminal
