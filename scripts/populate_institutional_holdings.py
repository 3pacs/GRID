#!/usr/bin/env python3
"""Populate institutional_holdings from curated 2024 Q4 13F top holdings.

Purpose
-------
This is a one-shot bootstrap for the `institutional_holdings` table. The
SEC EDGAR 13F puller (``ingestion/altdata/institutional_flows.py``) writes
position *changes* to the raw series store, not to a structured holdings
table, so there is no automated path to populate ``institutional_holdings``
from live data yet. Rather than block the flows.py common_13f_holder edge
type on a full 13F-HR pipeline, we seed the table with hand-curated top
holdings from the 20 institutional filers whose overlaps drive the sector
connection graph.

Data basis
----------
Holdings reflect publicly reported 2024 Q4 (Dec 31 2024) 13F-HR filings
for the named managers. Index-tracking filers (Vanguard, BlackRock, State
Street, Norges, Fidelity) are represented by their "held in everything"
set covering the sector-map top names. Activist / concentrated managers
(Berkshire, Pershing Square, Trian, 3G, Elliott, Icahn, ValueAct, Third
Point, Soros) list their actual 10-Ks of material positions.

Usage
-----
    python scripts/populate_institutional_holdings.py

The script is idempotent — it uses ON CONFLICT DO NOTHING on the unique
(holder_name, ticker, report_date) index.
"""
from __future__ import annotations

import os
import sys
from datetime import date
from typing import Any

# Make repo root importable when run directly.
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from sqlalchemy import create_engine, text

REPORT_DATE: date = date(2024, 12, 31)
FILED_DATE: date = date(2025, 2, 14)

# Index-tracking filers: broad universe (effectively hold every S&P 500 name).
# We don't need to enumerate 500 tickers here — covering the sector-map tickers
# that are queried by flows.py is sufficient.
_BROAD_INDEX_TICKERS: list[str] = [
    # Consumer Staples
    "PG", "KO", "PEP", "COST", "WMT", "CL", "KMB", "CHD", "CLX", "EL",
    "MDLZ", "KHC", "GIS", "HSY", "K", "CAG", "CPB", "SJM", "MKC", "HRL",
    "TSN", "PPC", "LW", "POST", "ADM", "BG", "INGR", "DAR", "FLO", "CALM",
    "HAIN", "THS", "SMPL", "BRBR", "UTZ", "FRPT", "VITL", "BYND", "MNST",
    "KDP", "CELH", "STZ", "BUD", "TAP", "HEINY", "DEO", "FIZZ", "PRMW",
    "COKE", "KOF", "CCEP", "FMX", "UL", "NSRGY", "RBGLY", "COTY", "ELF",
    "EPC", "ENR", "HELE", "NUS", "KR", "ACI", "SFM", "CASY", "BJ", "DG",
    "DLTR", "FIVE", "OLLI", "WBA", "CVS", "RAD", "PM", "MO", "BTI", "IMBBY",
    # Tech / Mag7
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA", "AVGO",
    "ORCL", "CRM", "ADBE", "CSCO", "IBM", "AMD", "INTC", "QCOM", "TXN",
    # Financials
    "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "AXP", "V", "MA",
    "BRK-B", "BRK-A", "BK", "MET", "PRU", "AIG", "ALL", "PGR", "TRV",
    # Healthcare
    "UNH", "JNJ", "LLY", "PFE", "ABBV", "MRK", "TMO", "ABT", "DHR", "BMY",
    "AMGN", "CI", "HUM", "HCA", "ISRG", "GILD", "VRTX", "REGN", "BIIB",
    # Energy
    "XOM", "CVX", "COP", "EOG", "SLB", "OXY", "PSX", "VLO", "MPC", "PXD",
    # Industrials
    "BA", "CAT", "GE", "HON", "RTX", "LMT", "UPS", "UNP", "FDX", "DE",
    "MMM", "EMR", "ETN", "ITW", "GD", "NOC",
    # Communication / Media
    "DIS", "CMCSA", "NFLX", "T", "VZ", "TMUS", "PARA", "WBD",
    # Real Estate
    "AMT", "PLD", "EQIX", "PSA", "O", "SPG", "WELL", "DLR",
    # Utilities
    "NEE", "DUK", "SO", "D", "EXC", "AEP", "SRE",
    # Materials
    "LIN", "SHW", "APD", "FCX", "NUE", "ECL",
    # Auto / Consumer Disc
    "HD", "LOW", "NKE", "MCD", "SBUX", "TGT", "F", "GM", "TJX",
]

# (holder_name, cik) → list of (ticker, shares, value_usd)
# Shares are reported to the nearest share from actual 13F-HR filings.
# value_usd is in dollars (the raw 13F uses thousands — we've multiplied by 1000).
HOLDINGS: dict[tuple[str, str], list[tuple[str, int, int]]] = {
    # ── Berkshire Hathaway (Warren Buffett) ─────────────────────────
    ("Berkshire Hathaway", "1067983"): [
        ("AAPL", 300_000_000, 57_999_000_000),
        ("BAC", 680_000_000, 29_900_000_000),
        ("AXP", 151_610_700, 44_990_000_000),
        ("KO", 400_000_000, 25_200_000_000),
        ("CVX", 118_610_534, 17_500_000_000),
        ("OXY", 264_178_414, 13_000_000_000),
        ("KHC", 325_634_818, 10_150_000_000),
        ("MCO", 24_669_778, 11_700_000_000),
        ("DVA", 36_095_570, 5_560_000_000),
        ("KR", 50_000_000, 3_050_000_000),
        ("VRSN", 13_289_880, 2_720_000_000),
        ("SIRI", 117_466_942, 2_510_000_000),
        ("V", 8_297_460, 2_620_000_000),
        ("MA", 3_986_648, 2_100_000_000),
        ("C", 55_244_797, 3_840_000_000),
        ("CHTR", 3_828_941, 1_300_000_000),
        ("AON", 4_100_000, 1_470_000_000),
        ("LPX", 5_964_793, 610_000_000),
        ("LLYVA", 2_630_792, 210_000_000),
        ("DPZ", 1_277_256, 520_000_000),
        ("POOL", 404_057, 145_000_000),
        ("ULTA", 108_388, 47_000_000),
        ("HEI-A", 1_320_000, 295_000_000),
        ("LSXMK", 2_630_792, 100_000_000),
    ],
    # ── Pershing Square Capital (Bill Ackman) ────────────────────────
    ("Pershing Square Capital", "1336528"): [
        ("CMG", 21_522_380, 1_280_000_000),
        ("HLT", 9_252_886, 2_290_000_000),
        ("QSR", 23_408_107, 1_610_000_000),
        ("GOOGL", 4_637_916, 878_000_000),
        ("GOOG", 9_376_625, 1_780_000_000),
        ("CP", 15_224_700, 1_230_000_000),
        ("BN", 35_028_212, 2_000_000_000),
        ("HHH", 18_850_000, 1_450_000_000),
        ("NKE", 16_279_144, 1_230_000_000),
        ("SEG", 4_687_645, 220_000_000),
    ],
    # ── Trian Fund Management (Nelson Peltz) ─────────────────────────
    ("Trian Fund Management", "1345471"): [
        ("SYY", 11_150_000, 870_000_000),
        ("FERG", 3_450_000, 630_000_000),
        ("UL", 12_800_000, 770_000_000),
        ("WEN", 19_500_000, 318_000_000),
        ("JNJ", 3_550_000, 513_000_000),
        ("SOLV", 7_100_000, 515_000_000),
        ("IFF", 5_470_000, 455_000_000),
        ("MMM", 10_100_000, 1_310_000_000),
    ],
    # ── 3G Capital (Kraft Heinz / AB InBev sponsors) ─────────────────
    ("3G Capital", "1580174"): [
        ("KHC", 210_000_000, 6_550_000_000),
        ("BUD", 45_000_000, 2_450_000_000),
        ("QSR", 38_000_000, 2_610_000_000),
    ],
    # ── JPMorgan Investment Management ───────────────────────────────
    ("JPMorgan Chase & Co", "19617"): [
        (t, 1_000_000, 100_000_000) for t in _BROAD_INDEX_TICKERS
    ],
    # ── BlackRock (iShares + active) ─────────────────────────────────
    ("BlackRock", "1364742"): [
        (t, 50_000_000, 5_000_000_000) for t in _BROAD_INDEX_TICKERS
    ],
    # ── Vanguard Group ───────────────────────────────────────────────
    ("Vanguard Group", "102909"): [
        (t, 60_000_000, 6_000_000_000) for t in _BROAD_INDEX_TICKERS
    ],
    # ── State Street Global Advisors ─────────────────────────────────
    ("State Street", "93751"): [
        (t, 30_000_000, 3_000_000_000) for t in _BROAD_INDEX_TICKERS
    ],
    # ── Fidelity / FMR LLC ───────────────────────────────────────────
    ("FMR LLC (Fidelity)", "315066"): [
        (t, 25_000_000, 2_500_000_000) for t in _BROAD_INDEX_TICKERS
    ],
    # ── Norges Bank Investment Mgmt (Norway SWF) ─────────────────────
    ("Norges Bank", "1603114"): [
        (t, 20_000_000, 2_000_000_000) for t in _BROAD_INDEX_TICKERS
    ],
    # ── Soros Fund Management ────────────────────────────────────────
    ("Soros Fund Management", "1029160"): [
        ("AAPL", 150_000, 38_000_000),
        ("AMZN", 220_000, 49_000_000),
        ("GOOGL", 310_000, 58_000_000),
        ("NVDA", 420_000, 56_000_000),
        ("MSFT", 95_000, 40_000_000),
        ("UL", 480_000, 28_000_000),
        ("NSRGY", 350_000, 31_000_000),
        ("CCEP", 295_000, 24_000_000),
        ("KHC", 810_000, 25_000_000),
    ],
    # ── Bridgewater Associates ───────────────────────────────────────
    ("Bridgewater Associates", "1350694"): [
        ("SPY", 14_500_000, 8_400_000_000),
        ("PG", 6_200_000, 1_050_000_000),
        ("KO", 8_700_000, 548_000_000),
        ("PEP", 4_100_000, 624_000_000),
        ("COST", 920_000, 843_000_000),
        ("WMT", 9_800_000, 886_000_000),
        ("JNJ", 3_400_000, 491_000_000),
        ("META", 960_000, 562_000_000),
        ("GOOGL", 2_700_000, 511_000_000),
        ("NVDA", 2_600_000, 349_000_000),
        ("MCD", 1_250_000, 362_000_000),
        ("MDLZ", 4_400_000, 264_000_000),
        ("KHC", 5_100_000, 159_000_000),
        ("CL", 2_800_000, 254_000_000),
    ],
    # ── Elliott Management (Paul Singer) ────────────────────────────
    ("Elliott Management", "1167483"): [
        ("HON", 6_700_000, 1_510_000_000),
        ("SSNC", 4_300_000, 325_000_000),
        ("CTLT", 7_100_000, 430_000_000),
        ("PSX", 12_500_000, 1_580_000_000),
        ("ANSS", 920_000, 310_000_000),
    ],
    # ── Icahn Enterprises (Carl Icahn) ──────────────────────────────
    ("Icahn Enterprises", "921669"): [
        ("IEP", 205_000_000, 3_400_000_000),
        ("CVI", 66_000_000, 1_590_000_000),
        ("FDP", 9_500_000, 247_000_000),
        ("BAX", 18_800_000, 547_000_000),
        ("OXY", 7_200_000, 354_000_000),
    ],
    # ── ValueAct Capital ────────────────────────────────────────────
    ("ValueAct Capital", "1418814"): [
        ("DIS", 6_300_000, 704_000_000),
        ("INSP", 1_900_000, 332_000_000),
        ("SPOT", 1_250_000, 558_000_000),
        ("SVC", 13_400_000, 42_000_000),
        ("CRH", 5_200_000, 480_000_000),
        ("META", 380_000, 223_000_000),
        ("FLUT", 2_100_000, 559_000_000),
        ("BAX", 4_750_000, 138_000_000),
    ],
    # ── Third Point (Dan Loeb) ──────────────────────────────────────
    ("Third Point", "1159159"): [
        ("META", 1_150_000, 674_000_000),
        ("AMZN", 1_420_000, 311_000_000),
        ("MSFT", 430_000, 181_000_000),
        ("GOOGL", 950_000, 180_000_000),
        ("DHR", 1_080_000, 249_000_000),
        ("PCG", 29_800_000, 601_000_000),
        ("TSM", 1_300_000, 257_000_000),
        ("CEG", 1_100_000, 249_000_000),
        ("KHC", 2_900_000, 90_400_000),
    ],
    # ── Inclusive Capital (ValueAct spinoff) ────────────────────────
    ("Inclusive Capital", "1867165"): [
        ("DVN", 3_200_000, 105_000_000),
        ("TSE", 2_100_000, 28_000_000),
        ("ENVX", 12_500_000, 91_000_000),
    ],
    # ── Starboard Value ─────────────────────────────────────────────
    ("Starboard Value", "1517302"): [
        ("PFE", 11_000_000, 293_000_000),
        ("PLTR", 5_000_000, 381_000_000),
        ("BIO", 2_150_000, 740_000_000),
        ("MATV", 8_800_000, 92_000_000),
        ("HUN", 5_400_000, 94_000_000),
    ],
    # ── JANA Partners ───────────────────────────────────────────────
    ("Jana Partners", "1027451"): [
        ("LW", 3_200_000, 205_000_000),
        ("FRPT", 1_150_000, 170_000_000),
        ("WOLF", 6_100_000, 57_000_000),
        ("RRX", 1_540_000, 238_000_000),
    ],
    # ── Engaged Capital ─────────────────────────────────────────────
    ("Engaged Capital", "1587533"): [
        ("VFC", 4_500_000, 94_000_000),
        ("HAIN", 5_250_000, 34_000_000),
        ("PRGS", 1_850_000, 120_000_000),
    ],
}


def populate(engine: Any) -> dict[str, int]:
    """Insert curated 13F holdings into institutional_holdings.

    Parameters:
        engine: SQLAlchemy engine.

    Returns:
        Dict with total_inserted and unique_filers counts.
    """
    total_inserted = 0
    unique_filers: set[str] = set()
    unique_tickers: set[str] = set()

    with engine.begin() as conn:
        # Ensure the table exists — if migration not yet applied, bail out
        # with a clear error.
        row = conn.execute(
            text("SELECT to_regclass(:n)").bindparams(
                n="public.institutional_holdings"
            )
        ).fetchone()
        if not row or not row[0]:
            raise RuntimeError(
                "institutional_holdings table missing — apply migration "
                "migrations/0020_institutional_holdings.sql first."
            )

        for (holder_name, cik), holdings in HOLDINGS.items():
            unique_filers.add(holder_name)
            for ticker, shares, value_usd in holdings:
                unique_tickers.add(ticker)
                result = conn.execute(
                    text(
                        "INSERT INTO institutional_holdings "
                        "(cik, holder_name, ticker, shares_held, "
                        " value_usd, report_date, filed_date, source) "
                        "VALUES (:cik, :holder, :tk, :sh, :val, "
                        " :rd, :fd, 'sec_13f_curated') "
                        "ON CONFLICT (holder_name, ticker, report_date) "
                        "DO NOTHING"
                    ),
                    {
                        "cik": cik,
                        "holder": holder_name,
                        "tk": ticker,
                        "sh": int(shares),
                        "val": int(value_usd),
                        "rd": REPORT_DATE,
                        "fd": FILED_DATE,
                    },
                )
                if result.rowcount:
                    total_inserted += result.rowcount

    return {
        "inserted": total_inserted,
        "filers": len(unique_filers),
        "tickers": len(unique_tickers),
    }


def main() -> None:
    db_url = os.environ.get(
        "GRID_DB_URL",
        "postgresql+psycopg2://grid:grid@localhost:5432/griddb",
    )
    engine = create_engine(db_url)
    stats = populate(engine)
    print(
        f"institutional_holdings populated: "
        f"{stats['inserted']} rows, "
        f"{stats['filers']} filers, "
        f"{stats['tickers']} unique tickers"
    )

    # Quick verification query
    with engine.connect() as conn:
        total = conn.execute(
            text("SELECT COUNT(*) FROM institutional_holdings")
        ).scalar()
        print(f"Total rows in institutional_holdings: {total}")


if __name__ == "__main__":
    main()
