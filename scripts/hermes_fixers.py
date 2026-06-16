"""
GRID Hermes Operator — pull fixers, pipeline runner, diagnostics.

Contains:
  - _resolve_puller, _retry_source — source resolution and retry logic
  - diagnose_and_fix_pulls — smart pull diagnosis and repair
  - maybe_run_pipeline — pipeline scheduling
  - fill_data_gaps — historical data gap filler
  - run_self_diagnostics — Hermes self-healing diagnostics
  - maybe_run_autoresearch — autoresearch trigger
  - save_cycle_snapshot — cycle result persistence
  - _run_intel_task, _hours_since — intelligence task helpers
"""

from __future__ import annotations

import json
import re
import time
import traceback
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log

from scripts.hermes_health import (
    OperatorState,
    log_issue,
    export_issues,
    resolve_source_issues,
    _ensure_issues_table,
)

# These constants are duplicated here to avoid circular imports
PIPELINE_INTERVAL_HOURS = 6
MAX_PULL_RETRIES = 3
AUTORESEARCH_MAX_ITER = 5
HERMES_TEMPERATURE = 0.3

_PULL_ACTIONS: dict[str, str] = {
    "RETRY": "transient error; retry with normal strategy",
    "SKIP": "known outage or maintenance; do not spend a retry",
    "BACKFILL": "data gap; retry with extended lookback",
    "CHECK_KEY": "API key, quota, or auth configuration needs attention",
    "ESCALATE": "human action is needed before Hermes can safely continue",
}

HERMES_REPAIR_SKILLS: tuple[tuple[str, str], ...] = (
    ("RUN_REGIME", "Re-run regime detection."),
    ("RUN_FEATURES", "Recompute feature importance."),
    ("REPULL:<source_name>", "Re-pull a specific source if it is not in cooldown."),
    ("RUN_PIPELINE", "Trigger the standard full pipeline."),
    ("VACUUM_DB", "Run VACUUM ANALYZE on hot data tables."),
    ("FIX_DATA_QUALITY[:family]", "Scan recent resolved_series quality and remove exact duplicates."),
    ("FIX_OUTPUT_DIRS", "Repair or create common output directories used by reports and widgets."),
    ("ENSURE_OPERATOR_TABLES", "Create Hermes operator issue tables and indexes if missing."),
    ("CHECK_SCHEMA:<table>", "Verify a required public table exists and log a critical issue if not."),
    ("SYNC_EARNINGS_CALENDAR", "Run the earnings_events to earnings_calendar compatibility sync."),
    ("CHECK_HEALTH", "Record the current health payload as an action result."),
    ("CHECK_OUTPUT_DIRS", "Inspect common output directory status without mutating anything."),
    ("CHECK_LLM_PROVIDERS", "Run the configured Hermes LLM health check."),
    ("CHECK_TASK_FAILURES", "Summarize failed Hermes task_status entries."),
    ("INSPECT_SOURCE:<source_name>", "Inspect source_catalog freshness, recent failures, and last error for one source."),
    ("COOLDOWN_SOURCE:<source_name>", "Temporarily pause retrying a noisy source in the current Hermes state."),
    ("RUN_WIRING_AUDIT", "Run the read-only GRID wiring audit and return dark router/puller/module counts."),
    ("SCOUT_FREE_DATA:<source_name>", "Find public/free fallback candidates for a failing or low-value source."),
    ("CHECK_SOURCE_QUALITY[:days]", "Run paid-vs-free source quality ablation and write a bounded report."),
    ("CHECK_STORAGE[:target_id]", "Run bounded grid-svr storage maintenance scan and write a cleanup/ingest plan."),
    ("LOG_FOLLOWUP:<category>:<severity>:<title>", "Record a pending operator issue for human or agent follow-up."),
    ("LIST_SUBAGENTS", "List Hermes' dedicated subagent roles and queue contracts."),
    ("DISPATCH_SUBAGENT:<role>:<target_id>[:priority]", "Queue a bounded goal for a dedicated Hermes subagent role."),
    ("CHECK_SUBAGENTS", "Inspect goal_queue state for Hermes-managed subagent work."),
    ("NONE", "No repair needed."),
)

HERMES_SUBAGENTS: dict[str, dict[str, Any]] = {
    "source_doctor": {
        "goal_type": "hermes_diagnose_source",
        "hardware_tier": "cpu",
        "priority": 160,
        "allow_cloud": False,
        "description": "Inspect one source's catalog state, recent failures, and retry posture.",
    },
    "free_data_scout": {
        "goal_type": "hermes_scout_free_data",
        "hardware_tier": "cpu",
        "priority": 155,
        "allow_cloud": False,
        "description": "Find public/free fallback candidates for one failing or low-value data source.",
    },
    "wiring_auditor": {
        "goal_type": "hermes_wiring_audit",
        "hardware_tier": "cpu",
        "priority": 120,
        "allow_cloud": False,
        "description": "Run the read-only wiring audit and report dark routers/pullers/modules.",
    },
    "storage_maintainer": {
        "goal_type": "hermes_storage_maintenance",
        "hardware_tier": "cpu",
        "priority": 130,
        "allow_cloud": False,
        "description": "Inventory GRID data roots, flag un-ingested archives, and plan cold-storage cleanup.",
    },
    "hypothesis_scorer": {
        "goal_type": "score_active_hypothesis",
        "hardware_tier": "cpu",
        "priority": 140,
        "allow_cloud": False,
        "description": "Use the existing idle-fleet scorer for one active hypothesis id.",
    },
}

_COMMON_OUTPUT_DIRS: tuple[Path, ...] = (
    Path("outputs/backtest"),
    Path("outputs/market_briefings"),
    Path("outputs/paper_trades"),
    Path("outputs/llm_insights"),
)


def _normalize_source_key(source_name: str) -> str:
    return source_name.strip().lower().replace(" ", "_").replace("-", "_")


def _parse_pull_diagnosis_actions(diagnosis_text: str) -> dict[str, str]:
    """Parse Hermes pull-fix recommendations into source/action pairs."""
    actions: dict[str, str] = {}
    for raw_line in diagnosis_text.splitlines():
        line = raw_line.strip()
        if ":" not in line:
            continue
        source, remainder = line.split(":", 1)
        match = re.match(r"\s*([A-Za-z_]+)\b", remainder)
        if not match:
            continue
        action = match.group(1).upper()
        if action in _PULL_ACTIONS:
            actions[_normalize_source_key(source)] = action
    return actions


def _format_pull_action_catalog() -> str:
    return "\n".join(f"- {action} - {desc}" for action, desc in _PULL_ACTIONS.items())


def _format_repair_skill_catalog() -> str:
    return "\n".join(f"- {cmd} - {desc}" for cmd, desc in HERMES_REPAIR_SKILLS)


def _truncate(value: Any, max_chars: int) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3] + "..."


def _summarize_recent_issue(issue: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": issue.get("id"),
        "created_at": issue.get("created_at"),
        "category": issue.get("category"),
        "severity": issue.get("severity"),
        "source": issue.get("source"),
        "title": issue.get("title"),
        "detail": _truncate(issue.get("detail"), 500),
        "stack_trace": _truncate(issue.get("stack_trace"), 700),
        "hermes_diagnosis": _truncate(issue.get("hermes_diagnosis"), 500),
        "fix_applied": issue.get("fix_applied"),
        "fix_result": issue.get("fix_result"),
    }


def _inspect_output_dirs() -> list[dict[str, Any]]:
    status: list[dict[str, Any]] = []
    for path in _COMMON_OUTPUT_DIRS:
        status.append({
            "path": str(path),
            "exists": path.exists(),
            "is_dir": path.is_dir(),
            "is_symlink": path.is_symlink(),
            "resolved": str(path.resolve(strict=False)),
        })
    return status


def _ensure_common_output_dirs() -> dict[str, str]:
    from outputs.path_utils import ensure_output_dir

    repaired: dict[str, str] = {}
    for path in _COMMON_OUTPUT_DIRS:
        repaired[str(path)] = str(ensure_output_dir(path).resolve(strict=False))
    return repaired


def _require_engine(engine: Any, cmd: str) -> Any:
    if engine is None:
        raise ValueError(f"{cmd} requires a database engine")
    return engine


def _run_data_quality_fix(engine: Any, target: str | None, state: OperatorState) -> dict[str, Any]:
    from sqlalchemy import text as sa_text

    engine = _require_engine(engine, "FIX_DATA_QUALITY")
    dq_issues: list[dict[str, Any]] = []
    fixes = 0
    with engine.begin() as conn:
        # Check for NaN/null values in recent resolved_series.
        q_nulls = (
            "SELECT fr.name, COUNT(*) AS null_count "
            "FROM resolved_series rs "
            "JOIN feature_registry fr ON fr.id = rs.feature_id "
            "WHERE rs.obs_date >= CURRENT_DATE - INTERVAL '7 days' "
            "AND rs.value IS NULL "
        )
        if target:
            q_nulls += "AND fr.family = :target "
        q_nulls += "GROUP BY fr.name HAVING COUNT(*) > 0 ORDER BY null_count DESC LIMIT 20"
        params = {"target": target} if target else {}
        null_rows = conn.execute(sa_text(q_nulls), params).fetchall()
        for r in null_rows:
            dq_issues.append({"type": "null_values", "feature": r[0], "count": r[1]})

        # Check for duplicate timestamps.
        q_dupes = (
            "SELECT fr.name, rs.obs_date, COUNT(*) AS n "
            "FROM resolved_series rs "
            "JOIN feature_registry fr ON fr.id = rs.feature_id "
            "WHERE rs.obs_date >= CURRENT_DATE - INTERVAL '7 days' "
        )
        if target:
            q_dupes += "AND fr.family = :target "
        q_dupes += "GROUP BY fr.name, rs.obs_date HAVING COUNT(*) > 1 LIMIT 20"
        dupe_rows = conn.execute(sa_text(q_dupes), params).fetchall()
        for r in dupe_rows:
            dq_issues.append({"type": "duplicate", "feature": r[0], "date": str(r[1]), "count": r[2]})

        # Check for extreme outliers.
        q_outliers = (
            "SELECT fr.name, rs.value, rs.obs_date "
            "FROM resolved_series rs "
            "JOIN feature_registry fr ON fr.id = rs.feature_id "
            "WHERE rs.obs_date >= CURRENT_DATE - INTERVAL '7 days' "
            "AND rs.value IS NOT NULL "
            "AND ABS(rs.value) > 1e15 "
        )
        if target:
            q_outliers += "AND fr.family = :target "
        q_outliers += "LIMIT 20"
        outlier_rows = conn.execute(sa_text(q_outliers), params).fetchall()
        for r in outlier_rows:
            dq_issues.append({"type": "outlier", "feature": r[0], "value": float(r[1]), "date": str(r[2])})

        # Auto-fix exact duplicates, keeping one row per feature/date.
        for r in dupe_rows:
            try:
                conn.execute(sa_text(
                    "DELETE FROM resolved_series WHERE ctid NOT IN ("
                    "  SELECT MIN(ctid) FROM resolved_series rs "
                    "  JOIN feature_registry fr ON fr.id = rs.feature_id "
                    "  WHERE fr.name = :fname AND rs.obs_date = :odate "
                    "  GROUP BY rs.feature_id, rs.obs_date"
                    ") AND feature_id = (SELECT id FROM feature_registry WHERE name = :fname) "
                    "AND obs_date = :odate"
                ), {"fname": r[0], "odate": r[1]})
                fixes += 1
            except Exception as exc:
                log.debug("Hermes: duplicate resolved_series delete failed for {f}: {e}", f=r[0], e=str(exc))

    severity = "WARNING" if len(dq_issues) < 5 else "ERROR" if len(dq_issues) < 20 else "CRITICAL"
    if dq_issues:
        log_issue(
            engine,
            category="system", severity=severity,
            source=target or "all",
            title=f"Data quality: {len(dq_issues)} issues found",
            detail=str(dq_issues[:10]),
            fix_applied="dedup" if fixes > 0 else None,
            fix_result="SUCCESS" if fixes > 0 else None,
            cycle_number=getattr(state, "cycle_count", None),
        )

    return {
        "cmd": f"FIX_DATA_QUALITY:{target}" if target else "FIX_DATA_QUALITY",
        "status": "ok",
        "issues_found": len(dq_issues),
        "duplicates_fixed": fixes,
    }


def _check_schema_table(engine: Any, table: str, state: OperatorState) -> dict[str, Any]:
    from sqlalchemy import text as sa_text

    engine = _require_engine(engine, "CHECK_SCHEMA")
    table = table.strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError(f"Unsafe table name for CHECK_SCHEMA: {table!r}")

    with engine.connect() as conn:
        exists = bool(conn.execute(
            sa_text("SELECT to_regclass(:table_name) IS NOT NULL"),
            {"table_name": f"public.{table}"},
        ).scalar())

    if not exists:
        log_issue(
            engine,
            category="schema", severity="CRITICAL",
            source=table,
            title=f"Missing required table — {table}",
            detail="Hermes CHECK_SCHEMA did not find this table in public schema.",
            fix_result="PENDING",
            cycle_number=getattr(state, "cycle_count", None),
        )

    return {
        "cmd": f"CHECK_SCHEMA:{table}",
        "status": "ok" if exists else "missing",
        "table": table,
        "exists": exists,
    }


def _summarize_task_failures(state: OperatorState, limit: int = 10) -> dict[str, Any]:
    failures: list[dict[str, Any]] = []
    for task, info in getattr(state, "task_status", {}).items():
        if info.get("success") is not False:
            continue
        failures.append({
            "task": task,
            "last_run": info.get("last_run"),
            "duration_s": info.get("duration_s"),
            "error": _truncate(info.get("error"), 700),
        })
    failures.sort(key=lambda item: str(item.get("last_run") or ""), reverse=True)
    return {
        "failed_count": len(failures),
        "failures": failures[:limit],
    }


def _inspect_source(engine: Any, source_name: str) -> dict[str, Any]:
    from sqlalchemy import text as sa_text

    engine = _require_engine(engine, "INSPECT_SOURCE")
    source_name = source_name.strip()
    if not source_name:
        raise ValueError("INSPECT_SOURCE requires a source name")

    with engine.connect() as conn:
        frequency_expr = (
            "frequency"
            if _source_catalog_column_exists(conn, "frequency")
            else "NULL AS frequency"
        )
        source = conn.execute(
            sa_text(
                f"SELECT id, name, active, last_pull_at, {frequency_expr} "
                "FROM source_catalog "
                "WHERE LOWER(name) = LOWER(:source) "
                "LIMIT 1"
            ),
            {"source": source_name},
        ).fetchone()
        failures = conn.execute(
            sa_text(
                "SELECT COUNT(*) AS fail_count, "
                "       MAX(rs.pull_timestamp) AS last_fail, "
                "       MAX(rs.raw_payload::text) AS last_error "
                "FROM raw_series rs "
                "JOIN source_catalog sc ON sc.id = rs.source_id "
                "WHERE LOWER(sc.name) = LOWER(:source) "
                "AND rs.pull_status = 'FAILED' "
                "AND rs.pull_timestamp > NOW() - INTERVAL '24 hours'"
            ),
            {"source": source_name},
        ).fetchone()
        latest_success = conn.execute(
            sa_text(
                "SELECT MAX(rs.pull_timestamp) "
                "FROM raw_series rs "
                "JOIN source_catalog sc ON sc.id = rs.source_id "
                "WHERE LOWER(sc.name) = LOWER(:source) "
                "AND rs.pull_status = 'SUCCESS'"
            ),
            {"source": source_name},
        ).fetchone()

    return {
        "cmd": f"INSPECT_SOURCE:{source_name}",
        "status": "ok" if source else "missing",
        "source": {
            "id": source[0] if source else None,
            "name": source[1] if source else source_name,
            "active": source[2] if source else None,
            "last_pull_at": source[3].isoformat() if source and source[3] else None,
            "frequency": source[4] if source else None,
            "latest_success": latest_success[0].isoformat() if latest_success and latest_success[0] else None,
        },
        "recent_failures": {
            "count_24h": int(failures[0] or 0) if failures else 0,
            "last_fail": failures[1].isoformat() if failures and failures[1] else None,
            "last_error": _truncate(failures[2], 700) if failures else None,
        },
    }


def _source_catalog_column_exists(conn: Any, column_name: str) -> bool:
    from sqlalchemy import text as sa_text

    row = conn.execute(
        sa_text(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.columns "
            "  WHERE table_schema = 'public' "
            "    AND table_name = 'source_catalog' "
            "    AND column_name = :column_name"
            ")"
        ),
        {"column_name": column_name},
    ).scalar()
    return bool(row)


_FREE_FALLBACK_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "family": "equity_price",
        "match": (
            "yfinance", "yahoo", "tiingo", "polygon", "fmp",
            "alphavantage", "alpha_vantage", "twelvedata", "twelve_data",
        ),
        "candidates": [
            {
                "provider": "stooq_csv",
                "url_shape": "https://stooq.com/q/l/?s={ticker}.US&f=sd2t2ohlcv&h&e=csv",
                "repo_path": "ingestion/price_fallback.py",
                "caveat": "Good free EOD proxy for many tickers; symbol coverage and anti-bot behavior vary.",
            },
            {
                "provider": "sec_companyfacts",
                "url_shape": "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json",
                "repo_path": "ingestion/earnings_events_puller.py",
                "caveat": "Fundamental filings, not intraday prices; requires compliant User-Agent and CIK mapping.",
            },
        ],
    },
    {
        "family": "news",
        "match": (
            "worldnews", "world_news", "tiingo_news", "newsapi",
            "alphavantage_news", "alpha_vantage_news", "rss", "news",
        ),
        "candidates": [
            {
                "provider": "news_scraper_rss",
                "url_shape": "CNBC/MarketWatch/Yahoo/Bloomberg/Fed/SEC RSS registry",
                "repo_path": "ingestion/altdata/news_scraper.py",
                "caveat": "Free and resilient, but ticker extraction/sentiment is noisier than curated feeds.",
            },
            {
                "provider": "gdelt_doc",
                "url_shape": "https://api.gdeltproject.org/api/v2/doc/doc?query={query}&mode=timelineTone&timespan=60m",
                "repo_path": "ingestion/altdata/gdelt.py",
                "caveat": "Broad public event firehose; needs tight query taxonomy to avoid false positives.",
            },
            {
                "provider": "sec_current_8k_atom",
                "url_shape": "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom",
                "repo_path": "ingestion/altdata/news_scraper.py",
                "caveat": "Hard corporate events only; not a replacement for general market news.",
            },
        ],
    },
    {
        "family": "shipping_trade",
        "match": ("baltic", "freight", "shipping", "container", "scfi", "drewry"),
        "candidates": [
            {
                "provider": "balticdryindex_github_latest",
                "url_shape": "https://raw.githubusercontent.com/balticdryindex/balticdryindex/gh-pages/data/latest.json",
                "repo_path": "ingestion/altdata/baltic_dry.py",
                "caveat": "Current daily snapshot only; do not use the site's synthetic chart history for backtests.",
            },
            {
                "provider": "drewry_wci_public_page",
                "url_shape": "https://www.drewry.co.uk/supply-chain-advisors/supply-chain-expertise/world-container-index-assessed-by-drewry",
                "repo_path": "ingestion/altdata/container_freight.py",
                "caveat": "HTML scraping can break; weekly container rates are not a Baltic dry-bulk substitute.",
            },
        ],
    },
    {
        "family": "crypto_onchain",
        "match": ("coingecko", "crypto", "defi", "dex", "binance", "etherscan", "solana"),
        "candidates": [
            {
                "provider": "defi_llama",
                "url_shape": "https://api.llama.fi/protocols and stablecoins/chain endpoints",
                "repo_path": "ingestion/altdata/defi_llama_puller.py",
                "caveat": "Free/no-key but endpoint churn happens; store endpoint version in payload.",
            },
            {
                "provider": "binance_public_market_data",
                "url_shape": "https://api.binance.us/api/v3/klines?symbol={symbol}&interval=1d",
                "repo_path": "ingestion/realtime/feeds/binance.py",
                "caveat": "Binance global may geoblock US IPs; Binance.US has narrower symbols.",
            },
            {
                "provider": "dexscreener_public",
                "url_shape": "https://api.dexscreener.com/latest/dex/search?q={query}",
                "repo_path": "ingestion/dexscreener.py",
                "caveat": "Great for liquidity/trending pairs; noisy for investable-market benchmarks.",
            },
        ],
    },
    {
        "family": "macro_gov",
        "match": ("fred", "bls", "eia", "world_bank", "oecd", "macro", "treasury", "cftc"),
        "candidates": [
            {
                "provider": "dbnomics_public",
                "url_shape": "https://api.db.nomics.world/v22/series/{provider}/{dataset}/{series}",
                "repo_path": "ingestion/international/rbi.py",
                "caveat": "Good mirror/fallback for official macro series; provider/dataset IDs need mapping.",
            },
            {
                "provider": "treasury_fiscaldata",
                "url_shape": "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/{dataset}",
                "repo_path": "scripts/sources_expanded.py",
                "caveat": "Existing sketch writes raw_ingest; needs canonical raw_series puller before production.",
            },
            {
                "provider": "cftc_socrata",
                "url_shape": "https://publicreporting.cftc.gov/resource/{dataset}.json",
                "repo_path": "scripts/sources_expanded.py",
                "caveat": "Public but schema differs across reports; map report type explicitly.",
            },
        ],
    },
)


def _recommend_free_data_fallbacks(source_name: str) -> list[dict[str, Any]]:
    key = _normalize_source_key(source_name)
    matches: list[dict[str, Any]] = []
    for group in _FREE_FALLBACK_CATALOG:
        if any(token in key for token in group["match"]):
            for candidate in group["candidates"]:
                matches.append({"family": group["family"], **candidate})

    if matches:
        return matches

    return [
        {
            "family": "generic",
            "provider": "source_audit_redundancy_map",
            "url_shape": "Compare existing raw_series sources that map to the same feature.",
            "repo_path": "intelligence/source_audit.py",
            "caveat": "Best first step when no obvious public API maps to this source name.",
        },
        {
            "family": "generic",
            "provider": "public_rss_or_official_api_search",
            "url_shape": "Search official agency/RSS/API docs before adding another paid dependency.",
            "repo_path": "scripts/hermes_fixers.py",
            "caveat": "Recommendation only; a human/agent still needs to validate PIT behavior and coverage.",
        },
    ]


def _scout_free_data_sources(engine: Any, source_name: str, state: OperatorState) -> dict[str, Any]:
    source_name = source_name.strip()
    if not source_name:
        raise ValueError("SCOUT_FREE_DATA requires a source name")

    inspection = _inspect_source(_require_engine(engine, "SCOUT_FREE_DATA"), source_name)
    candidates = _recommend_free_data_fallbacks(source_name)
    issue_id = log_issue(
        engine,
        category="ingestion",
        severity="INFO",
        source=source_name,
        title=f"Free data fallback candidates — {source_name}",
        detail=json.dumps(
            {
                "inspection": inspection,
                "candidates": candidates,
                "policy": (
                    "Read-only scout: validate coverage, PIT semantics, and model lift "
                    "before replacing or downweighting paid feeds."
                ),
            },
            default=str,
        )[:4000],
        fix_applied="free_data_scout",
        fix_result="PENDING",
        cycle_number=getattr(state, "cycle_count", None),
    )
    return {
        "cmd": f"SCOUT_FREE_DATA:{source_name}",
        "status": "ok",
        "source": source_name,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "inspection": inspection,
        "issue_id": issue_id,
    }


def _run_wiring_audit_summary() -> dict[str, Any]:
    from scripts import audit_wiring

    mounted, missing_routers = audit_wiring._audit_api_routers()
    registered, unregistered = audit_wiring._audit_puller_scheduling()
    graph = audit_wiring._audit_import_graph()
    return {
        "cmd": "RUN_WIRING_AUDIT",
        "status": "ok",
        "api": {
            "mounted": len(mounted),
            "dark": len(missing_routers),
            "missing_routers": missing_routers[:25],
        },
        "pullers": {
            "registered": len(registered),
            "dark": len(unregistered),
            "unregistered": unregistered[:25],
        },
        "import_graph": {
            "total_modules": graph.get("total_modules"),
            "reachable_count": graph.get("reachable_count"),
            "orphans": len(graph.get("orphans", [])),
            "unreachable": len(graph.get("unreachable", [])),
            "top_orphans": graph.get("orphans", [])[:25],
        },
    }


def _inspect_storage_maintenance(
    engine: Any,
    target_id: str = "grid-svr-data",
    state: OperatorState | None = None,
) -> dict[str, Any]:
    """Run bounded storage maintenance in report-first mode."""
    from scripts import storage_curator

    target_id = (target_id or "grid-svr-data").strip()
    if not target_id:
        target_id = "grid-svr-data"
    result = storage_curator.run_storage_maintenance(
        _require_engine(engine, "CHECK_STORAGE"),
        target_id=target_id,
    )
    if result.get("status") != "ok":
        log_issue(
            engine,
            category="storage",
            severity="WARNING" if result.get("status") == "ingest_gap" else "ERROR",
            source=target_id,
            title=f"Storage maintenance status — {result.get('status')}",
            detail=json.dumps(result.get("summary", {}), default=str)[:4000],
            fix_applied="storage_maintainer",
            fix_result="PENDING",
            cycle_number=getattr(state, "cycle_count", None) if state else None,
        )
    return {"cmd": f"CHECK_STORAGE:{target_id}", **result}


def _log_followup_issue(raw_cmd: str, engine: Any, state: OperatorState) -> dict[str, Any]:
    parts = raw_cmd.split(":", 3)
    if len(parts) != 4:
        raise ValueError("LOG_FOLLOWUP format is LOG_FOLLOWUP:<category>:<severity>:<title>")
    _cmd, category, severity, title = parts
    category = category.strip().lower()
    severity = severity.strip().upper()
    title = title.strip()
    if not re.fullmatch(r"[a-z_][a-z0-9_]{1,40}", category):
        raise ValueError(f"Unsafe LOG_FOLLOWUP category: {category!r}")
    if severity not in {"INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError(f"Invalid LOG_FOLLOWUP severity: {severity!r}")
    if not title or len(title) > 240:
        raise ValueError("LOG_FOLLOWUP title must be 1-240 characters")
    issue_id = log_issue(
        _require_engine(engine, raw_cmd),
        category=category,
        severity=severity,
        title=title,
        detail="Hermes logged this as a deferred follow-up from self-diagnostics.",
        fix_result="PENDING",
        cycle_number=getattr(state, "cycle_count", None),
    )
    return {"cmd": raw_cmd, "status": "ok", "issue_id": issue_id}


def _list_subagents() -> dict[str, Any]:
    return {
        "cmd": "LIST_SUBAGENTS",
        "status": "ok",
        "subagents": [
            {"role": role, **spec}
            for role, spec in HERMES_SUBAGENTS.items()
        ],
    }


def _dispatch_subagent(raw_cmd: str, engine: Any, state: OperatorState) -> dict[str, Any]:
    parts = raw_cmd.split(":")
    if len(parts) not in (3, 4):
        raise ValueError("DISPATCH_SUBAGENT format is DISPATCH_SUBAGENT:<role>:<target_id>[:priority]")
    _cmd, role, target_id = parts[:3]
    role = role.strip().lower()
    target_id = target_id.strip()
    if not re.fullmatch(r"[a-z_][a-z0-9_]{1,40}", role):
        raise ValueError(f"Unsafe subagent role: {role!r}")
    if role not in HERMES_SUBAGENTS:
        raise ValueError(f"Unknown Hermes subagent role: {role!r}")
    if not target_id or len(target_id) > 180:
        raise ValueError("DISPATCH_SUBAGENT target_id must be 1-180 characters")
    spec = HERMES_SUBAGENTS[role]
    priority = int(parts[3]) if len(parts) == 4 and parts[3].strip() else int(spec["priority"])

    from intelligence.goal_queue import enqueue_goal

    goal_id = enqueue_goal(
        _require_engine(engine, raw_cmd),
        goal_type=spec["goal_type"],
        target_id=target_id,
        payload={
            "role": role,
            "requested_by": "hermes",
            "requested_cycle": getattr(state, "cycle_count", None),
            "description": spec["description"],
        },
        priority=priority,
        hardware_tier=spec["hardware_tier"],
        allow_cloud=bool(spec.get("allow_cloud", False)),
        dedupe_window=f"hermes:{role}",
        depth=1,
    )
    return {
        "cmd": raw_cmd,
        "status": "queued" if goal_id is not None else "deduped",
        "role": role,
        "goal_type": spec["goal_type"],
        "target_id": target_id,
        "goal_id": goal_id,
    }


def _check_subagent_queue(engine: Any) -> dict[str, Any]:
    from sqlalchemy import text as sa_text

    goal_types = [spec["goal_type"] for spec in HERMES_SUBAGENTS.values()]
    with _require_engine(engine, "CHECK_SUBAGENTS").connect() as conn:
        rows = conn.execute(
            sa_text(
                "SELECT goal_type, state, COUNT(*) "
                "FROM goal_queue "
                "WHERE goal_type = ANY(:goal_types) "
                "GROUP BY goal_type, state "
                "ORDER BY goal_type, state"
            ),
            {"goal_types": goal_types},
        ).fetchall()
        recent = conn.execute(
            sa_text(
                "SELECT goal_type, target_id, state, updated_at, last_error "
                "FROM goal_queue "
                "WHERE goal_type = ANY(:goal_types) "
                "ORDER BY updated_at DESC "
                "LIMIT 20"
            ),
            {"goal_types": goal_types},
        ).fetchall()

    counts: dict[str, dict[str, int]] = {}
    for goal_type, state, count in rows:
        counts.setdefault(goal_type, {})[state] = int(count)
    return {
        "cmd": "CHECK_SUBAGENTS",
        "status": "ok",
        "counts": counts,
        "recent": [
            {
                "goal_type": r[0],
                "target_id": r[1],
                "state": r[2],
                "updated_at": r[3].isoformat() if r[3] else None,
                "last_error": _truncate(r[4], 500),
            }
            for r in recent
        ],
    }


def _execute_hermes_repair_command(
    cmd: str,
    engine: Any,
    health: dict,
    state: OperatorState,
) -> dict[str, Any]:
    """Execute one bounded Hermes repair command."""
    raw_cmd = cmd.strip()
    upper_cmd = raw_cmd.upper()

    if upper_cmd == "NONE":
        return {"cmd": raw_cmd, "status": "skipped"}

    if upper_cmd == "RUN_REGIME":
        log.info("Hermes action: running regime detection")
        from scripts.auto_regime import run
        regime_result = run()
        return {"cmd": raw_cmd, "status": "ok", "regime": regime_result.get("regime")}

    if upper_cmd == "RUN_FEATURES":
        log.info("Hermes action: recomputing features")
        from features.lab import recompute_importance
        recompute_importance(_require_engine(engine, raw_cmd))
        return {"cmd": raw_cmd, "status": "ok"}

    if upper_cmd.startswith("REPULL:"):
        source = raw_cmd.split(":", 1)[1].strip()
        if not source:
            raise ValueError("REPULL requires a source name")
        if state.cooldowns.can_retry(source):
            log.info("Hermes action: re-pulling {s}", s=source)
            _retry_source(source, _require_engine(engine, raw_cmd), attempt=1)
            state.cooldowns.record_attempt(source, success=True)
            return {"cmd": raw_cmd, "status": "ok"}
        log.info("Hermes wants REPULL:{s} but source in cooldown", s=source)
        return {"cmd": raw_cmd, "status": "cooldown"}

    if upper_cmd == "RUN_PIPELINE":
        log.info("Hermes action: triggering pipeline")
        from scripts.run_full_pipeline import run_pipeline
        run_pipeline(historical=False)
        state.last_pipeline_run = datetime.now(timezone.utc)
        return {"cmd": raw_cmd, "status": "ok"}

    if upper_cmd == "VACUUM_DB":
        log.info("Hermes action: vacuuming database")
        from sqlalchemy import text as sa_text
        with _require_engine(engine, raw_cmd).connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(sa_text("VACUUM ANALYZE raw_series"))
            conn.execute(sa_text("VACUUM ANALYZE resolved_series"))
        return {"cmd": raw_cmd, "status": "ok"}

    if upper_cmd.startswith("FIX_DATA_QUALITY"):
        target = raw_cmd.split(":", 1)[1].strip() if ":" in raw_cmd else None
        log.info("Hermes action: data quality check{t}", t=f" for {target}" if target else "")
        return _run_data_quality_fix(engine, target, state)

    if upper_cmd == "FIX_OUTPUT_DIRS":
        log.info("Hermes action: ensuring output directories")
        return {"cmd": raw_cmd, "status": "ok", "paths": _ensure_common_output_dirs()}

    if upper_cmd == "ENSURE_OPERATOR_TABLES":
        log.info("Hermes action: ensuring operator issue tables")
        _ensure_issues_table(_require_engine(engine, raw_cmd))
        return {"cmd": raw_cmd, "status": "ok"}

    if upper_cmd.startswith("CHECK_SCHEMA:"):
        table = raw_cmd.split(":", 1)[1].strip()
        return _check_schema_table(engine, table, state)

    if upper_cmd == "SYNC_EARNINGS_CALENDAR":
        log.info("Hermes action: syncing earnings calendar compatibility table")
        from sqlalchemy import text as sa_text
        with _require_engine(engine, raw_cmd).begin() as conn:
            row = conn.execute(
                sa_text("SELECT inserted_count, total_events FROM sync_earnings_events_to_calendar()")
            ).fetchone()
        inserted = int(row[0]) if row and row[0] is not None else 0
        total = int(row[1]) if row and row[1] is not None else 0
        return {"cmd": raw_cmd, "status": "ok", "inserted": inserted, "total_events": total}

    if upper_cmd == "CHECK_HEALTH":
        return {"cmd": raw_cmd, "status": "ok", "health": health}

    if upper_cmd == "CHECK_OUTPUT_DIRS":
        return {"cmd": raw_cmd, "status": "ok", "output_dirs": _inspect_output_dirs()}

    if upper_cmd == "CHECK_LLM_PROVIDERS":
        from scripts.hermes_health import check_hermes_health
        return {"cmd": raw_cmd, "status": "ok", "hermes": check_hermes_health()}

    if upper_cmd == "CHECK_TASK_FAILURES":
        return {"cmd": raw_cmd, "status": "ok", **_summarize_task_failures(state)}

    if upper_cmd.startswith("INSPECT_SOURCE:"):
        source = raw_cmd.split(":", 1)[1].strip()
        return _inspect_source(engine, source)

    if upper_cmd.startswith("COOLDOWN_SOURCE:"):
        source = raw_cmd.split(":", 1)[1].strip()
        if not source:
            raise ValueError("COOLDOWN_SOURCE requires a source name")
        state.cooldowns.blacklist_for_timeout(source)
        return {
            "cmd": raw_cmd,
            "status": "ok",
            "source": source,
            "cooldown": "timeout_blacklist",
        }

    if upper_cmd == "RUN_WIRING_AUDIT":
        return _run_wiring_audit_summary()

    if upper_cmd.startswith("SCOUT_FREE_DATA:"):
        source = raw_cmd.split(":", 1)[1].strip()
        return _scout_free_data_sources(engine, source, state)

    if upper_cmd == "CHECK_SOURCE_QUALITY" or upper_cmd.startswith("CHECK_SOURCE_QUALITY:"):
        days = 30
        if ":" in raw_cmd:
            raw_days = raw_cmd.split(":", 1)[1].strip()
            if raw_days:
                days = max(1, min(365, int(raw_days)))
        from intelligence import source_quality_ablation

        return {
            "cmd": raw_cmd,
            **source_quality_ablation.run_source_quality_ablation(
                _require_engine(engine, raw_cmd),
                days=days,
                prediction_days=max(90, min(365, days * 6)),
            ),
        }

    if upper_cmd == "CHECK_STORAGE" or upper_cmd.startswith("CHECK_STORAGE:"):
        target_id = raw_cmd.split(":", 1)[1].strip() if ":" in raw_cmd else "grid-svr-data"
        return _inspect_storage_maintenance(engine, target_id, state)

    if upper_cmd.startswith("LOG_FOLLOWUP:"):
        return _log_followup_issue(raw_cmd, engine, state)

    if upper_cmd == "LIST_SUBAGENTS":
        return _list_subagents()

    if upper_cmd.startswith("DISPATCH_SUBAGENT:"):
        return _dispatch_subagent(raw_cmd, engine, state)

    if upper_cmd == "CHECK_SUBAGENTS":
        return _check_subagent_queue(engine)

    raise ValueError(f"Unknown Hermes command: {raw_cmd}")


def _parse_hermes_action_commands(response: str, limit: int = 5) -> list[str]:
    commands: list[str] = []
    for raw_line in response.splitlines():
        line = raw_line.strip()
        if not line.upper().startswith("ACTION:"):
            continue
        cmd = line.split(":", 1)[1].strip()
        if cmd:
            commands.append(cmd)
        if len(commands) >= limit:
            break
    return commands


# Map source_catalog names to hermes registry keys
# Covers naming mismatches between DB catalog and puller registry
_CATALOG_TO_REGISTRY: dict[str, str] = {
    "coingecko": "yfinance",  # CoinGecko data pulled via yfinance crypto
    "DexScreener": "yfinance",
    "DeFi_Llama": "defillama",
    "polygon": "yfinance",
    "nasa_firms": "noaa_swpc",
    "VIIRS": "noaa_swpc",
    "etherscan": "yfinance",
    "fmp": "earnings_calendar",
    "wikipedia_pageviews": "social_attention",
    "FINRA_MARGIN": "margin_debt",
    "TIINGO_FUNDAMENTALS": "tiingo",
    "TIINGO": "tiingo",
    "TIINGO_NEWS": "tiingo",
    "opensecrets": "lobbying",
    "Cloudflare_Radar": "gdelt_news",
    "cryptoquant": "yfinance",
    "OFR": "fed_liquidity",
    "LUNAR_EPHEMERIS": "lunar_ephemeris",
    "PLANETARY_EPHEMERIS": "planetary_ephemeris",
    "VEDIC_JYOTISH": "vedic_jyotish",
    "CHINESE_CALENDAR": "chinese_calendar",
    "NOAA_SWPC": "noaa_swpc",
    "GoogleTrends": "googletrends",
    "FedSpeeches": "fedspeeches",
    "WorldNewsAPI": "world_news",
    "Crucix": "crucix",
    "TradingView": "yfinance",
    "EPHEMERIS_ENGINE": "planetary_ephemeris",
    "open_meteo": "noaa_swpc",
    "binance": "yfinance",
    "yfinance_options": "yfinance_options",
    "alphavantage_news_sentiment": "alphavantage_sentiment",
    "hf_financial_news": "hf_financial_news",
    "NYFED_GSCPI": "nyfed_gscpi",
    "GDELT_NEWS": "gdelt_news",
    "Social_Smart_Money": "smart_money",
    "Discord_Solana_Scanner": "yfinance",
    "BCB_BR": "fred",
    "EDINET": "fred",
    "KAGGLE_BULK": "fred",
    "NY_Fed": "ny_fed",
    "Telegram_Solana_Scanner": "yfinance",
    "SEC_INSIDER": "insider_filings",
    "INSTITUTIONAL_FLOWS": "institutional_flows",
    "Supply_Chain": "supply_chain",
    "NewsScraperRSS": "news_scraper",
    "USASPENDING_GOV": "gov_contracts",
    "BIS_EXPORT_CONTROLS": "export_controls",
    "STOCKTWITS": "stocktwits",
    "OppInsights": "fred",
    "yfinance_earnings": "earnings_calendar",
    "Analyst_Ratings": "earnings_calendar",
    "Fear_Greed": "fear_greed",
    "Kalshi": "kalshi",
    "POLYMARKET": "polymarket",
    "FOIA_CABLES": "foia_cables",
    "AKShare": "yfinance",
    "Eurostat": "fred",
    "CFTC_COT": "cftc_cot",
    "OpenCorporates": "offshore_leaks",
    "GDELT": "gdelt",
    "AAII_Sentiment": "aaii_sentiment",
    "USPTO_PV": "fred",
    "USDA_NASS": "fred",
    "nowcast": "fred",
    "world_bank": "fred",
    "EIA": "fred",
    "computed": "fred",
    "FRED": "fred",
    "BLS": "bls",
    "CBOE": "cboe",
}


class _FunctionPuller:
    """Adapter so fn-based registry entries can use the standard retry path."""

    def __init__(self, fn: Any, engine: Any) -> None:
        self._fn = fn
        self._engine = engine

    def run(self, **kwargs: Any) -> Any:
        import inspect

        call_kwargs = dict(kwargs)
        try:
            params = inspect.signature(self._fn).parameters
        except (TypeError, ValueError):
            params = {}
        if "db_engine" in params:
            call_kwargs.setdefault("db_engine", self._engine)
        elif "engine" in params:
            call_kwargs.setdefault("engine", self._engine)
        return self._fn(**call_kwargs)


def _resolve_puller(source_name: str, engine: Any) -> tuple[Any, str, dict[str, Any]]:
    """Resolve a source name to a puller instance using the registry.

    Returns:
        (puller_instance, pull_method_name, pull_kwargs)

    Raises:
        ValueError: If no handler found for the source.
    """
    import importlib

    source_lower = source_name.lower().replace(" ", "_").replace("-", "_")

    from scripts.hermes_operator import _SOURCE_REGISTRY

    # 1. Direct registry match
    entry = _SOURCE_REGISTRY.get(source_lower)

    # 2. Catalog name mapping
    if entry is None:
        mapped = _CATALOG_TO_REGISTRY.get(source_name) or _CATALOG_TO_REGISTRY.get(source_lower)
        if mapped:
            entry = _SOURCE_REGISTRY.get(mapped)

    # 3. Fuzzy prefix match
    if entry is None:
        for key, val in _SOURCE_REGISTRY.items():
            if source_lower.startswith(key) or key.startswith(source_lower):
                entry = val
                break

    if entry is None:
        raise ValueError(f"No puller registered for source: {source_name}")

    # Honor registry-level skip flag — entries that are catalogued for the
    # wiring audit but not yet wrapped for the standard _resolve_puller calling
    # convention (e.g. ctors with positional `engine` instead of `db_engine`).
    if entry.get("skip_runtime"):
        raise ValueError(
            f"Source {source_name} registered but skipped at runtime: {entry['skip_runtime']}"
        )

    mod = importlib.import_module(entry["mod"])

    if "fn" in entry and "cls" not in entry:
        fn = getattr(mod, entry["fn"])
        return (
            _FunctionPuller(fn, engine),
            "run",
            dict(entry.get("pull_kwargs", {})),
        )

    cls = getattr(mod, entry["cls"])

    # Build constructor kwargs
    ctor_kwargs: dict[str, Any] = {"db_engine": engine}
    if "api_key" in entry:
        from config import settings
        ctor_kwargs["api_key"] = getattr(settings, entry["api_key"])

    puller = cls(**ctor_kwargs)
    method = entry.get("pull_method", "pull_all")
    kwargs = entry.get("pull_kwargs", {})

    return puller, method, kwargs


def _retry_source(source_name: str, engine: Any, attempt: int = 1) -> dict[str, Any]:
    """Retry a single source pull with strategy variation per attempt.

    Attempt 1: standard pull (recent data only)
    Attempt 2: pull with extended lookback
    Attempt 3: full historical backfill for last 7 days

    Returns:
        dict with pull result info.
    """
    log.info("Retrying {s} (attempt {a}/{m})", s=source_name, a=attempt, m=MAX_PULL_RETRIES)

    puller, method, kwargs = _resolve_puller(source_name, engine)

    # Vary strategy per attempt
    if attempt >= 2:
        # Extend lookback on retry — pull more historical data
        if "days_back" in kwargs:
            kwargs["days_back"] = kwargs["days_back"] * (attempt + 1)
        elif hasattr(puller, "pull_all"):
            # For pullers with start_date, go further back on retry
            from datetime import timedelta
            kwargs["start_date"] = (date.today() - timedelta(days=7 * attempt)).isoformat()

    pull_fn = getattr(puller, method)
    result = pull_fn(**kwargs)

    # Update last_pull_at in source_catalog on success
    try:
        from sqlalchemy import text
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE source_catalog SET last_pull_at = NOW() "
                "WHERE LOWER(name) = LOWER(:name)"
            ), {"name": source_name})
    except Exception:
        pass  # Non-critical — pull succeeded even if catalog update fails

    return result if isinstance(result, dict) else {"status": "ok"}


def diagnose_and_fix_pulls(
    engine: Any,
    hermes_available: bool,
    state: OperatorState,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Find failed/stale pulls, diagnose with Hermes, and actively fix them.

    Improvements over naive retry:
    - Per-source cooldown prevents retry spam
    - Hermes diagnosis is parsed into actionable fix categories
    - Retry strategy varies per attempt (standard → extended → backfill)
    - Failures are tracked with full context for pattern analysis
    """
    from sqlalchemy import text
    result: dict[str, Any] = {
        "retried": 0, "fixed": 0, "diagnosed": 0,
        "skipped_cooldown": 0, "skipped_no_handler": 0,
        "skipped_key_check": 0, "escalated": 0,
    }

    # Find sources with recent failures + their error details
    with engine.connect() as conn:
        failed = conn.execute(
            text(
                "SELECT sc.name, COUNT(*) AS fail_count, "
                "       MAX(rs.pull_timestamp) AS last_fail, "
                "       MAX(rs.raw_payload::text) AS last_error "
                "FROM raw_series rs "
                "JOIN source_catalog sc ON rs.source_id = sc.id "
                "WHERE rs.pull_status = 'FAILED' "
                "AND rs.pull_timestamp > NOW() - INTERVAL '24 hours' "
                "GROUP BY sc.name "
                "ORDER BY fail_count DESC"
            )
        ).fetchall()

    if not failed:
        log.info("No failed pulls in last 24h")
        return result

    failed_info = [
        {"source": r[0], "fail_count": r[1],
         "last_fail": r[2].isoformat() if r[2] else None,
         "last_error": (r[3] or "")[:200]}
        for r in failed
    ]
    failed_sources = [f["source"] for f in failed_info]
    # Surface the actual upstream error alongside the source name so operators
    # can diagnose without DB access. Previously this line only listed the
    # source names, forcing a psql query to figure out *why* FRED (or any
    # other source) was failing.
    summary_lines = []
    for f in failed_info:
        err = (f.get("last_error") or "").strip().replace("\n", " ")[:160]
        last_iso = f.get("last_fail") or "?"
        last_short = last_iso.split("T")[1][:8] if "T" in last_iso else last_iso
        summary_lines.append(
            f"{f['source']} (n={f['fail_count']}, last={last_short}, err={err!r})"
        )
    log.warning(
        "Failed sources in last 24h: {sources}\n  {detail}",
        sources=failed_sources,
        detail="\n  ".join(summary_lines),
    )

    # If Hermes is available, get structured diagnosis with fix actions
    diagnosis_text: str | None = None
    fix_actions: dict[str, str] = {}  # source → recommended action

    if hermes_available:
        try:
            from llm.router import get_llm, Tier
            client = get_llm(Tier.REASON)
            diagnosis_text = client.chat(
                messages=[
                    {"role": "system", "content": (
                        "You are GRID's operations agent. Diagnose data pull failures and "
                        "recommend specific actions. For EACH source, output one line:\n"
                        "SOURCE_NAME: ACTION - reason\n\n"
                        "ACTION must be one of:\n"
                        f"{_format_pull_action_catalog()}\n\n"
                        "Be specific and concise. No preamble."
                    )},
                    {"role": "user", "content": (
                        "Failed sources with details:\n"
                        + json.dumps(failed_info, default=str, indent=2)
                        + f"\nCurrent date: {date.today()}"
                    )},
                ],
                temperature=HERMES_TEMPERATURE,
            )
            if diagnosis_text:
                log.info("Hermes diagnosis:\n{d}", d=diagnosis_text[:800])
                result["diagnosis"] = diagnosis_text
                result["diagnosed"] = len(failed_sources)
                fix_actions = _parse_pull_diagnosis_actions(diagnosis_text)

        except Exception as exc:
            log.warning("Hermes diagnosis failed: {e}", e=str(exc))

    if dry_run:
        log.info("Dry run — not retrying pulls")
        result["fix_actions"] = fix_actions
        return result

    # Retry each failed source with cooldown awareness and strategy variation
    for source_name in failed_sources:
        source_key = _normalize_source_key(source_name)

        # Check Hermes recommendation
        action = fix_actions.get(source_key, "RETRY")
        if action == "SKIP":
            log.info("Hermes says SKIP {s} — known outage", s=source_name)
            result["skipped_cooldown"] += 1
            continue
        if action == "ESCALATE":
            log.warning("Hermes says ESCALATE {s} — needs human attention", s=source_name)
            result["escalated"] += 1
            log_issue(
                engine, category="ingestion", severity="CRITICAL",
                source=source_name,
                title=f"Hermes escalation — {source_name} needs human attention",
                hermes_diagnosis=diagnosis_text,
                fix_result="SKIPPED",
                cycle_number=state.cycle_count,
            )
            continue
        if action == "CHECK_KEY":
            log.warning("Hermes says CHECK_KEY {s} — not retrying until credentials/quota are checked", s=source_name)
            result["skipped_key_check"] += 1
            log_issue(
                engine, category="config", severity="CRITICAL",
                source=source_name,
                title=f"API key or quota check required — {source_name}",
                detail="Hermes classified the pull failure as an auth, quota, or key-health issue.",
                hermes_diagnosis=diagnosis_text,
                fix_result="SKIPPED",
                cycle_number=state.cycle_count,
            )
            continue

        # Check cooldown
        if not state.cooldowns.can_retry(source_name):
            cooldown_info = state.cooldowns.get_status(source_name)
            fails = cooldown_info.get("consecutive_fails", 0) if cooldown_info else 0
            log.info(
                "Skipping {s} — in cooldown ({f} consecutive fails)",
                s=source_name, f=fails,
            )
            result["skipped_cooldown"] += 1
            continue

        # Determine attempt number from cooldown state
        cooldown_info = state.cooldowns.get_status(source_name)
        attempt = (cooldown_info.get("consecutive_fails", 0) + 1) if cooldown_info else 1
        attempt = min(attempt, MAX_PULL_RETRIES)

        try:
            # Use BACKFILL strategy if Hermes recommends it
            if action == "BACKFILL":
                attempt = MAX_PULL_RETRIES  # force extended lookback

            _retry_source(source_name, engine, attempt=attempt)
            result["retried"] += 1
            result["fixed"] += 1
            state.cooldowns.record_attempt(source_name, success=True)
            resolved_count = resolve_source_issues(
                engine,
                source_name,
                cycle_number=state.cycle_count,
            )
            log_issue(
                engine, category="ingestion", severity="INFO",
                source=source_name,
                title=f"Pull recovered — {source_name}",
                detail=(
                    f"Fixed on attempt {attempt} (strategy: {action}); "
                    f"resolved_prior_issues={resolved_count}"
                ),
                hermes_diagnosis=diagnosis_text,
                fix_applied=f"Retried with strategy={action}, attempt={attempt}",
                fix_result="SUCCESS",
                cycle_number=state.cycle_count,
            )
        except ValueError as exc:
            # No handler registered for this source
            log.info("No handler for {s}: {e}", s=source_name, e=str(exc))
            result["skipped_no_handler"] += 1
            log_issue(
                engine, category="ingestion", severity="WARNING",
                source=source_name,
                title=f"No Hermes pull handler — {source_name}",
                detail=str(exc),
                hermes_diagnosis=diagnosis_text,
                fix_result="SKIPPED",
                cycle_number=state.cycle_count,
            )
        except Exception as exc:
            log.warning("Retry for {s} failed: {e}", s=source_name, e=str(exc))
            state.cooldowns.record_attempt(source_name, success=False, error=str(exc))
            result["retried"] += 1
            log_issue(
                engine, category="ingestion", severity="ERROR",
                source=source_name,
                title=f"Pull retry failed — {source_name} (attempt {attempt})",
                detail=str(exc),
                stack_trace=traceback.format_exc(),
                hermes_diagnosis=diagnosis_text,
                fix_applied=f"Retried with strategy={action}, attempt={attempt}",
                fix_result="FAILED",
                cycle_number=state.cycle_count,
            )

    return result


# ─── Pipeline runner ─────────────────────────────────────────────────

def maybe_run_pipeline(state: OperatorState, dry_run: bool = False) -> dict[str, Any] | None:
    """Run the full pipeline if enough time has passed."""
    now = datetime.now(timezone.utc)
    if state.last_pipeline_run is not None:
        hours_since = (now - state.last_pipeline_run).total_seconds() / 3600
        if hours_since < PIPELINE_INTERVAL_HOURS:
            log.info(
                "Pipeline ran {h:.1f}h ago (threshold={t}h) — skipping",
                h=hours_since, t=PIPELINE_INTERVAL_HOURS,
            )
            return None

    if dry_run:
        log.info("Dry run — not running pipeline")
        return {"skipped": "dry_run"}

    log.info("Running full pipeline")
    try:
        from scripts.run_full_pipeline import run_pipeline
        summary = run_pipeline(historical=False)
        state.last_pipeline_run = now
        return summary
    except Exception as exc:
        log.error("Pipeline failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ─── Data gap filler ─────────────────────────────────────────────────

def fill_data_gaps(engine: Any, state: OperatorState, dry_run: bool = False) -> dict[str, Any]:
    """Find gaps in historical data and actively fill them by re-pulling sources.

    Strategy:
    1. Find features with sparse data or stale last observations
    2. Map each feature back to its source via source_catalog
    3. Re-pull the source with extended lookback to fill the gap
    """
    from sqlalchemy import text
    result: dict[str, Any] = {"gaps_found": 0, "gaps_filled": 0, "sources_repulled": []}

    try:
        with engine.connect() as conn:
            # Find features with gaps: sparse data OR stale (>7 days old)
            rows = conn.execute(
                text(
                    "SELECT fr.name, COUNT(rs.id) AS obs_count, "
                    "       MIN(rs.obs_date) AS first_obs, MAX(rs.obs_date) AS last_obs, "
                    "       sc.name AS source_name "
                    "FROM feature_registry fr "
                    "LEFT JOIN resolved_series rs ON rs.feature_id = fr.id "
                    "LEFT JOIN raw_series raw ON raw.series_id = fr.name "
                    "LEFT JOIN source_catalog sc ON raw.source_id = sc.id "
                    "WHERE fr.model_eligible = TRUE "
                    "GROUP BY fr.name, sc.name "
                    "HAVING COUNT(rs.id) < 100 OR MAX(rs.obs_date) < CURRENT_DATE - 7 "
                    "ORDER BY COUNT(rs.id) ASC "
                    "LIMIT 15"
                )
            ).fetchall()

        if not rows:
            log.info("No data gaps found")
            return result

        result["gaps_found"] = len(rows)
        sources_to_repull: dict[str, dict[str, Any]] = {}

        for r in rows:
            feature_name, obs_count, first_obs, last_obs, source_name = r
            log.info(
                "Data gap: {name} — {count} obs, range {first} to {last} (source: {src})",
                name=feature_name, count=obs_count,
                first=first_obs if first_obs else "none",
                last=last_obs if last_obs else "none",
                src=source_name or "unknown",
            )
            if source_name and source_name not in sources_to_repull:
                # Calculate how far back to pull based on the gap
                days_back = 90  # default
                if last_obs:
                    gap_days = (date.today() - last_obs).days
                    days_back = max(gap_days + 7, 30)  # at least 30 days
                sources_to_repull[source_name] = {"days_back": days_back, "features": []}
            if source_name:
                sources_to_repull[source_name]["features"].append(feature_name)

        if dry_run:
            log.info("Dry run — identified {n} sources to re-pull: {s}",
                     n=len(sources_to_repull), s=list(sources_to_repull.keys()))
            return result

        # Actually re-pull each source
        for source_name, info in sources_to_repull.items():
            # Respect cooldowns
            if not state.cooldowns.can_retry(source_name):
                log.info("Skipping gap-fill for {s} — in cooldown", s=source_name)
                continue

            try:
                log.info(
                    "Gap-filling {s} — {n} features, {d} days back",
                    s=source_name, n=len(info["features"]), d=info["days_back"],
                )
                _retry_source(source_name, engine, attempt=2)  # use extended strategy
                result["gaps_filled"] += len(info["features"])
                result["sources_repulled"].append(source_name)
                state.cooldowns.record_attempt(source_name, success=True)
                log.info("Gap-fill for {s} succeeded", s=source_name)
            except ValueError:
                log.info("No handler for gap-fill source: {s}", s=source_name)
            except Exception as exc:
                log.warning("Gap-fill for {s} failed: {e}", s=source_name, e=str(exc))
                state.cooldowns.record_attempt(source_name, success=False, error=str(exc))

    except Exception as exc:
        log.warning("Gap analysis failed: {e}", e=str(exc))

    return result


# ─── Self-diagnostics ────────────────────────────────────────────────

def run_self_diagnostics(
    engine: Any,
    hermes_available: bool,
    health: dict,
    state: OperatorState,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Have Hermes analyze the system state and actively execute fixes.

    Hermes outputs structured commands that get executed:
    - commands listed in HERMES_REPAIR_SKILLS
    """
    if not hermes_available:
        return {"skipped": "hermes_unavailable"}

    result: dict[str, Any] = {"actions_taken": []}

    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.REASON)

        # Include recent issues in the report so Hermes has memory
        recent_issues: list[dict[str, Any]] = []
        try:
            recent_issues = export_issues(engine, days_back=1)[:10]
        except Exception as exc:
            log.debug("Hermes: recent issues export failed: {e}", e=str(exc))

        status_report = json.dumps({
            "date": date.today().isoformat(),
            "cycle": state.cycle_count,
            "db_healthy": health["db"]["healthy"],
            "stale_sources": health["db"].get("stale_sources", []),
            "failed_pulls_24h": health["db"].get("failed_pulls_24h", 0),
            "raw_series_count": health["db"].get("raw_series_count", 0),
            "latest_pull": health["db"].get("latest_pull"),
            "sources_in_cooldown": state.cooldowns.skipped_sources(),
            "task_failures": _summarize_task_failures(state),
            "subagent_roles": _list_subagents()["subagents"],
            "recent_issues": [
                _summarize_recent_issue(i)
                for i in recent_issues
            ],
            "output_dirs": _inspect_output_dirs(),
            "repair_skills": [cmd for cmd, _desc in HERMES_REPAIR_SKILLS],
            "operator_stats": {
                "fixes_applied": state.fixes_applied,
                "pulls_retried": state.pulls_retried,
                "consecutive_failures": state.consecutive_failures,
            },
        }, default=str, indent=2)

        response = client.chat(
            messages=[
                {"role": "system", "content": (
                    "You are GRID's self-diagnostics agent. Analyze the system and output "
                    "STRUCTURED COMMANDS to fix issues. Think in this order: observe the evidence, "
                    "name the likely root cause, choose only bounded repair commands, then verify "
                    "from the next cycle's telemetry. Output format - one command per line:\n\n"
                    "SEVERITY: OK|WARNING|CRITICAL\n"
                    "ACTION: <command>\n"
                    "ACTION: <command>\n"
                    "SUMMARY: <one line summary>\n\n"
                    "Available repair skills:\n"
                    f"{_format_repair_skill_catalog()}\n\n"
                    "Use observation skills first when evidence is thin. "
                    "Use DISPATCH_SUBAGENT for work that needs a bounded background lane. "
                    "Use LOG_FOLLOWUP for non-urgent human/agent follow-ups. "
                    "Max 7 actions. Use NONE if there is no clear repair. "
                    "Be specific. No prose except in SUMMARY."
                )},
                {"role": "user", "content": f"System status:\n{status_report}"},
            ],
            temperature=0.2,
            system_knowledge=[
                "01_architecture", "02_data_sources", "03_pit_store",
                "04_conflict_resolution", "05_features", "06_clustering",
                "07_regime", "08_options", "09_journal", "10_governance",
                "11_autoresearch",
            ],
        )

        if not response:
            return {"skipped": "empty_response"}

        log.info("Hermes self-diagnostic:\n{r}", r=response[:600])
        result["assessment"] = response

        if dry_run:
            return result

        # Parse and execute structured commands
        for cmd in _parse_hermes_action_commands(response, limit=7):
            if cmd.upper() == "NONE":
                continue
            try:
                action_result = _execute_hermes_repair_command(cmd, engine, health, state)
                if action_result.get("status") != "skipped":
                    result["actions_taken"].append(action_result)
            except Exception as exc:
                log.warning("Hermes action {c} failed: {e}", c=cmd, e=str(exc))
                result["actions_taken"].append({
                    "cmd": cmd, "status": "failed", "error": str(exc),
                })

    except Exception as exc:
        log.warning("Self-diagnostics failed: {e}", e=str(exc))
        return {"skipped": "error", "error": str(exc)}

    return result


# ─── Autoresearch trigger ────────────────────────────────────────────

def maybe_run_autoresearch(state: OperatorState, dry_run: bool = False) -> dict[str, Any] | None:
    """Run autoresearch if system is healthy and enough time has passed."""
    now = datetime.now(timezone.utc)

    # Only run autoresearch every 12 hours
    if state.last_autoresearch is not None:
        hours_since = (now - state.last_autoresearch).total_seconds() / 3600
        if hours_since < 12:
            return None

    if dry_run:
        return {"skipped": "dry_run"}

    log.info("Running autoresearch cycle")
    try:
        from scripts.autoresearch import run_autoresearch
        result = run_autoresearch(max_iterations=AUTORESEARCH_MAX_ITER)
        state.last_autoresearch = now
        state.hypotheses_tested += result.get("iterations", 0)
        return result
    except Exception as exc:
        log.warning("Autoresearch failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ─── Snapshot persistence ────────────────────────────────────────────

def save_cycle_snapshot(engine: Any, cycle_result: dict[str, Any]) -> None:
    """Save the operator cycle result as an analytical snapshot."""
    try:
        from store.snapshots import AnalyticalSnapshotStore
        snap = AnalyticalSnapshotStore(db_engine=engine)
        snap.save_snapshot(
            category="pipeline_summary",
            subcategory="hermes_operator",
            payload=cycle_result,
            metrics={
                "overall_healthy": cycle_result.get("health", {}).get("overall_healthy"),
                "pulls_retried": cycle_result.get("pull_fixer", {}).get("retried", 0),
                "pipeline_ran": cycle_result.get("pipeline") is not None,
                "cycle": cycle_result.get("cycle"),
            },
        )
    except Exception as exc:
        log.warning("Failed to save cycle snapshot: {e}", e=str(exc))



# ─── Intelligence task runner ────────────────────────────────────────

def _run_intel_task(
    name: str,
    fn: Any,
    state: OperatorState,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Run an intelligence task with timing, logging, and error isolation.

    Every task is wrapped so that a single failure never kills the loop.
    Timing and success/failure are recorded in state.task_status for the
    hermes-status API endpoint.

    Returns:
        The task result, or None on failure.
    """
    t0 = time.monotonic()
    try:
        result = fn(*args, **kwargs)
        elapsed = time.monotonic() - t0
        state.record_task(name, success=True, duration_s=elapsed)
        log.info(
            "Intel task '{n}' completed in {t:.1f}s",
            n=name, t=elapsed,
        )
        return result
    except Exception as exc:
        elapsed = time.monotonic() - t0
        state.record_task(name, success=False, duration_s=elapsed, error=str(exc))
        log.warning(
            "Intel task '{n}' failed after {t:.1f}s: {e}",
            n=name, t=elapsed, e=str(exc),
        )
        return None


def _hours_since(ts: datetime | None) -> float:
    """Return hours elapsed since *ts*, or 999 if ts is None."""
    if ts is None:
        return 999.0
    return (datetime.now(timezone.utc) - ts).total_seconds() / 3600


def _refresh_signal_registry(engine: Any) -> None:
    """Refresh all signal adapters and prune expired signals."""
    try:
        from intelligence.adapters import ALL_ADAPTERS
        from intelligence.adapters.base import AdapterRegistry
        from intelligence.signal_registry import SignalRegistry

        registry = AdapterRegistry([cls() for cls in ALL_ADAPTERS])
        results = registry.refresh_all(engine)
        SignalRegistry.prune_expired(engine, days_old=7)
        log.info("Signal registry refreshed: {r}", r=results)
    except Exception as exc:
        log.error("Signal registry refresh failed: {e}", e=str(exc))
