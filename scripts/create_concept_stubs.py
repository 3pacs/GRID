#!/usr/bin/env python3
"""
Create Obsidian concept stub pages for GRID wiki entities.

These are short pages that serve as nodes in Obsidian's graph view,
linking back to the documents that reference them.
"""

import re
from pathlib import Path
from collections import defaultdict

GRID_ROOT = Path(__file__).resolve().parent.parent
WIKI_DIR = GRID_ROOT / "docs" / "wiki"

# Concept definitions: target → (category, short description, source_file_or_module)
CONCEPTS = {
    # ─── Core Engine ───
    "PIT Store": (
        "Core Engine",
        "Point-in-time query engine ensuring no lookahead bias in data access. "
        "Uses PostgreSQL `DISTINCT ON` for efficient vintage-aware queries.",
        "store/pit.py",
    ),
    "Conflict Resolution": (
        "Core Engine",
        "Multi-source conflict resolver that picks the highest-priority source "
        "when the same indicator diverges across providers. Per-family thresholds.",
        "normalization/resolver.py",
    ),
    "Entity Map": (
        "Core Engine",
        "Entity disambiguation layer that maps the same economic concept "
        "across different naming conventions from different data sources.",
        "normalization/entity_map.py",
    ),
    "Decision Journal": (
        "Core Engine",
        "Append-only audit log of every model decision. Immutability enforced "
        "via PostgreSQL triggers. Outcomes recorded separately.",
        "journal/log.py",
    ),
    "Model Governance": (
        "Core Engine",
        "Model lifecycle state machine: CANDIDATE -> SHADOW -> STAGING -> PRODUCTION -> FLAGGED -> RETIRED. "
        "Enforces one production model per layer.",
        "governance/registry.py",
    ),
    "Feature Engineering": (
        "Core Engine",
        "Feature transformation lab: zscore, rolling_slope, pct_change, ratio, spread. "
        "All transforms use PIT-correct inputs only.",
        "features/lab.py",
    ),
    "Walk-Forward Backtesting": (
        "Core Engine",
        "Promotion gate enforcement via walk-forward backtesting with strict "
        "temporal boundaries. Validates model fitness before state transitions.",
        "validation/gates.py",
    ),
    "Live Inference": (
        "Core Engine",
        "Live model scoring engine. Loads production model, gets latest PIT data, "
        "computes features, generates BUY/SELL/HOLD recommendations.",
        "inference/live.py",
    ),
    "Base Puller": (
        "Core Engine",
        "Base class for all data ingestion modules. Provides shared `_resolve_source_id()`, "
        "`_row_exists()`, `_insert_raw()`, and retry logic.",
        "ingestion/base.py",
    ),
    "Hermes Scheduler": (
        "Core Engine",
        "Central scheduler orchestrating all 48 data pullers, agent loops, "
        "and background tasks on configurable intervals.",
        "scripts/hermes_operator.py",
    ),
    "Regime Discovery": (
        "Core Engine",
        "Unsupervised regime clustering using GMM, KMeans, and Agglomerative methods. "
        "Tests k=2..6, computes transition matrices and persistence metrics.",
        "discovery/clustering.py",
    ),
    "Orthogonality Audit": (
        "Core Engine",
        "Feature independence audit using PCA, correlation heatmaps, and scree plots. "
        "Ensures features contribute unique information.",
        "discovery/orthogonality.py",
    ),

    # ─── Intelligence Layer ───
    "Trust Scorer": (
        "Intelligence",
        "Bayesian trust scoring with 90-day recency half-life for all signal sources. "
        "Evaluation windows vary by source type (congressional 30d, darkpool 5d, etc.).",
        "intelligence/trust_scorer.py",
    ),
    "Lever Pullers": (
        "Intelligence",
        "Identifies and tracks market-moving actors across 5 categories. "
        "Maps who pulls which liquidity valves.",
        "intelligence/lever_pullers.py",
    ),
    "Actor Network": (
        "Intelligence",
        "495 named actors with wealth flow tracking. US deep map covering "
        "pensions, lobbying, donors, defense, Fed, REITs, media.",
        "intelligence/actor_network.py",
    ),
    "Cross Reference": (
        "Intelligence",
        "Government statistics vs physical reality 'lie detector'. "
        "Flags divergence between official data and ground-truth signals.",
        "intelligence/cross_reference.py",
    ),
    "Source Audit": (
        "Intelligence",
        "Source accuracy comparison and redundancy mapping via pairwise comparison. "
        "Auto-updates resolver priorities based on track record.",
        "intelligence/source_audit.py",
    ),
    "Postmortem": (
        "Intelligence",
        "Automated failure analysis for every bad trade. Mandatory post-mortems "
        "that feed back into trust scores and model calibration.",
        "intelligence/postmortem.py",
    ),
    "Sleuth": (
        "Intelligence",
        "Investigative leads and signal pattern discovery engine. "
        "Surfaces non-obvious connections between market events.",
        "intelligence/sleuth.py",
    ),
    "Thesis Tracker": (
        "Intelligence",
        "Thesis versioning and scoring engine. Tracks investment theses "
        "through their lifecycle with confidence updates.",
        "intelligence/thesis_tracker.py",
    ),
    "Dollar Flows": (
        "Intelligence",
        "USD normalization and capital flow quantification. "
        "Tracks money movement across asset classes and geographies.",
        "intelligence/dollar_flows.py",
    ),
    "Event Sequence": (
        "Intelligence",
        "Chronological timeline reconstruction of market-moving events. "
        "Orders signals by timestamp for causal analysis.",
        "intelligence/event_sequence.py",
    ),
    "Forensics": (
        "Intelligence",
        "Price move reconstruction from actor signals. "
        "Reverse-engineers what caused a specific market move.",
        "intelligence/forensics.py",
    ),
    "Causation": (
        "Intelligence",
        "Traces market actions back to root actor causes. "
        "Separates levers (causes) from conditions (amplifiers).",
        "intelligence/causation.py",
    ),
    "Flow Thesis": (
        "Intelligence",
        "10+ capital flow theses and rotation patterns. "
        "Tracks macro-level money movement themes.",
        "intelligence/flow_thesis.py",
    ),
    "Flow Aggregator": (
        "Intelligence",
        "Sector and time-slice aggregation engine for capital flows. "
        "Rolls up flow signals into actionable views.",
        "intelligence/flow_aggregator.py",
    ),

    # ─── Trading ───
    "Options Scanner": (
        "Trading",
        "7-signal mispricing detector with LLM sanity check. "
        "Identifies options trading opportunities including 100x plays.",
        "discovery/options_scanner.py",
    ),
    "Options Recommender": (
        "Trading",
        "Generates specific trade recommendations: strike, expiry, entry, "
        "target, stop, and Kelly-optimal position sizing.",
        "trading/options_recommender.py",
    ),
    "Options Tracker": (
        "Trading",
        "Outcome tracking and self-improving scanner weight adjustment. "
        "Closes the loop on options recommendations.",
        "trading/options_tracker.py",
    ),
    "Dealer Gamma": (
        "Trading",
        "Options market microstructure: GEX (gamma exposure), vanna, charm, "
        "and gamma walls. Tracks dealer positioning effects on price.",
        "physics/dealer_gamma.py",
    ),
    "Oracle Engine": (
        "Trading",
        "5 competing prediction models with signal/anti-signal weighting "
        "and dynamic weight evolution. Runs every 6 hours via Hermes.",
        "oracle/engine.py",
    ),
    "Oracle Calibration": (
        "Trading",
        "Prediction calibration metrics: Brier score, expected calibration error (ECE), "
        "and reliability diagrams. 615 predictions locked for scoring.",
        "oracle/calibration.py",
    ),

    # ─── Data Sources ───
    "FRED": ("Data Source", "Federal Reserve Economic Data — macro indicators (rates, GDP, employment).", "ingestion/fred.py"),
    "BLS": ("Data Source", "Bureau of Labor Statistics — employment, CPI, PPI.", "ingestion/bls.py"),
    "ECB": ("Data Source", "European Central Bank — eurozone rates, monetary aggregates.", "ingestion/international/ecb.py"),
    "EDGAR": ("Data Source", "SEC EDGAR — 10-K/10-Q filings, corporate financial data.", "ingestion/edgar.py"),
    "NOAA": ("Data Source", "National Oceanic and Atmospheric Administration — weather data for commodity signals.", "ingestion/physical/noaa.py"),
    "USDA": ("Data Source", "US Department of Agriculture — crop reports, agricultural commodities.", "ingestion/physical/usda.py"),
    "EIA": ("Data Source", "Energy Information Administration — oil, gas, energy data.", "ingestion/physical/eia.py"),
    "GDELT": ("Data Source", "Global Database of Events, Language, and Tone — geopolitical event signals.", "ingestion/altdata/gdelt.py"),
    "FARA": ("Data Source", "DOJ Foreign Agent Registration Act — foreign lobbying disclosures.", "ingestion/altdata/fara.py"),
    "FOIA": ("Data Source", "Freedom of Information Act — declassified State Dept and NSA diplomatic cables.", "ingestion/altdata/foia_cables.py"),
    "CoinGecko": ("Data Source", "CoinGecko API — crypto prices, market cap, volume (free tier).", "ingestion/coingecko.py"),
    "Polymarket": ("Data Source", "Polymarket prediction market — rapid probability shift detection.", "ingestion/altdata/prediction_odds.py"),
    "Dark Pool": ("Data Source", "FINRA dark pool weekly transparency data — institutional hidden order flow.", "ingestion/altdata/dark_pool.py"),
    "Congressional Trading": ("Data Source", "Congressional insider trading disclosures — political edge signals.", "ingestion/altdata/congressional.py"),
    "Insider Filings": ("Data Source", "SEC Form 4 insider filings with cluster buy detection.", "ingestion/altdata/insider_filings.py"),
    "Institutional Flows": ("Data Source", "ETF flows + SEC 13F quarterly holdings — institutional positioning.", "ingestion/altdata/institutional_flows.py"),
    "Fed Liquidity": ("Data Source", "Federal Reserve net liquidity equation — proprietary liquidity measure.", "ingestion/altdata/fed_liquidity.py"),
    "Campaign Finance": ("Data Source", "FEC campaign contributions and PAC spending mapped to policy outcomes.", "ingestion/altdata/campaign_finance.py"),
    "CFTC COT": ("Data Source", "Commitments of Traders — weekly futures positioning data.", "ingestion/altdata/cftc_cot.py"),
    "Yield Curve": ("Data Source", "Full US Treasury yield curve daily — term structure signals.", "ingestion/altdata/yield_curve_full.py"),
    "Baltic Dry Index": ("Data Source", "Baltic Dry Index + shipping costs — global trade activity proxy.", "ingestion/altdata/baltic_dry.py"),
    "Supply Chain": ("Data Source", "Shipping rates, container index, ISM — supply chain health indicators.", "ingestion/altdata/supply_chain.py"),

    # ─── Infrastructure ───
    "PostgreSQL": ("Infrastructure", "Primary database: PostgreSQL 15 with TimescaleDB extension.", None),
    "TimescaleDB": ("Infrastructure", "Time-series extension for PostgreSQL — hypertables for time-series data.", None),
    "FastAPI": ("Infrastructure", "Python async web framework powering the REST API + WebSocket endpoints.", None),
    "SQLAlchemy": ("Infrastructure", "SQL toolkit and ORM (Core mode) — all queries use `text()` with parameterized binds.", None),
    "Zustand": ("Infrastructure", "Lightweight React state management used in the PWA frontend.", None),
    "Alembic": ("Infrastructure", "Database migration framework for incremental schema changes.", None),
    "Ollama": ("Infrastructure", "Local LLM inference server — market briefings and analysis.", "ollama/"),
    "Hyperspace": ("Infrastructure", "P2P local LLM inference layer for distributed reasoning.", "hyperspace/"),
    "llama.cpp": ("Infrastructure", "Direct llama.cpp inference for GPU-accelerated local LLM.", "llamacpp/"),
    "TradingAgents": ("Infrastructure", "Multi-agent trading framework with specialized analyst agents.", "agents/"),

    # ─── Database Tables ───
    "Raw Series Table": ("Database", "Stores every raw observation with source_id, obs_date, release_date, vintage_date, and pull_timestamp.", None),
    "Resolved Series Table": ("Database", "Canonical, deduplicated time series after conflict resolution. PIT queries read from here.", None),
    "Source Catalog Table": ("Database", "Registry of all external data sources with trust scores, cost tiers, and priority rankings.", None),
    "Feature Registry Table": ("Database", "Canonical feature definitions with family, transformation, normalization, and model eligibility.", None),

    # ─── Frontend Views ───
    "MoneyFlow View": ("Frontend", "Global money flow D3 visualization: Central Banks -> Markets -> Sectors.", "pwa/src/views/MoneyFlow.jsx"),
    "Actor Network View": ("Frontend", "D3 force graph of financial power structure — actors, connections, influence.", "pwa/src/views/ActorNetwork.jsx"),
    "Cross Reference View": ("Frontend", "Government stats vs physical reality lie detector dashboard.", "pwa/src/views/CrossReference.jsx"),
    "TrendTracker View": ("Frontend", "Momentum, regime, rotation, volatility, and liquidity trend dashboard.", "pwa/src/views/TrendTracker.jsx"),
    "Intel Dashboard View": ("Frontend", "Unified intelligence command center — all intel modules in one view.", "pwa/src/views/IntelDashboard.jsx"),

    # ─── Domains ───
    "AstroGrid": ("Domain", "Celestial mechanics and astronomical cycle overlay for market timing.", "astrogrid/"),
    "Trial Gem Hunter": ("Domain", "Clinical trial signal domain: ClinicalTrials.gov Phase 2/3 -> biotech equity prediction.", "signals/trial_signal.py"),
    "Kelly Criterion": ("Trading", "Optimal position sizing formula based on edge and odds. Used in options recommender.", None),
}


def find_backlinks(target: str, docs_dir: Path) -> list[tuple[str, str]]:
    """Find all files that link to this target via [[target]] or [[target|text]]."""
    backlinks = []
    pattern = re.compile(r"\[\[" + re.escape(target) + r"(?:\|[^\]]+)?\]\]")

    for md_file in docs_dir.rglob("*.md"):
        # Skip the wiki directory itself
        if "wiki" in md_file.relative_to(docs_dir).parts[:1]:
            continue
        try:
            content = md_file.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        if pattern.search(content):
            rel = md_file.relative_to(docs_dir.parent)
            backlinks.append((str(rel), md_file.stem))

    # Also check root-level md files
    for md_file in docs_dir.parent.glob("*.md"):
        try:
            content = md_file.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        if pattern.search(content):
            backlinks.append((md_file.name, md_file.stem))

    return backlinks


def create_stub(target: str, category: str, description: str,
                source_path: str | None, backlinks: list[tuple[str, str]]) -> str:
    """Generate a concept stub page."""
    lines = [
        f"---",
        f"title: {target}",
        f"category: {category}",
        f"type: concept",
        f"auto_generated: true",
        f"---",
        f"",
        f"# {target}",
        f"",
        f"**Category:** {category}",
        f"",
        f"{description}",
        f"",
    ]

    if source_path:
        lines.extend([
            f"## Source",
            f"",
            f"`{source_path}`",
            f"",
        ])

    if backlinks:
        lines.extend([
            f"## Referenced By",
            f"",
        ])
        for filepath, stem in sorted(set(backlinks)):
            lines.append(f"- [[{stem}]]")
        lines.append("")

    return "\n".join(lines)


def main():
    WIKI_DIR.mkdir(parents=True, exist_ok=True)

    created = 0
    skipped = 0

    for target, (category, description, source_path) in sorted(CONCEPTS.items()):
        # Check if a page already exists for this target
        slug = target.replace(" ", "-")
        target_file = WIKI_DIR / f"{target}.md"

        # Find backlinks from across the docs
        backlinks = find_backlinks(target, GRID_ROOT / "docs")

        if not backlinks:
            skipped += 1
            continue

        content = create_stub(target, category, description, source_path, backlinks)
        target_file.write_text(content, encoding="utf-8")
        created += 1
        print(f"  + {target} ({len(backlinks)} backlinks)")

    print(f"\nCreated {created} concept pages in docs/wiki/")
    print(f"Skipped {skipped} concepts with no backlinks")


if __name__ == "__main__":
    main()
