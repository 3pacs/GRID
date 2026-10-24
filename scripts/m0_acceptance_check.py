#!/usr/bin/env python3
"""Read-only bounded acceptance check for the M0 slice (HELD: not run anywhere).

M0 is the end-to-end slice  ingestion -> materialization -> reasoning with
provenance -> publication.  This script produces ONE dated JSON receipt with
four hops, each a handful of bounded SELECTs against griddb:

  H1 ingestion        pull_log YFinance_Daily rows for the date; raw_series
                      YF:^GSPC:close vintages for the date (pull_timestamp,
                      capture_state); source mix for the tickers.
  H2 materialization  max date / max created_at / row count in a lookback
                      window for insider_trades, congressional_trades,
                      dark_pool_weekly, etf_flows, junction_point_readings;
                      resolved_series max vintage_date for spy_full/xlk_full.
  H3 reasoning        market_diary row for the date: narrative_model,
                      price_basis/source present, which indices report
                      "no close for date".
  H4 publication      market_briefings rows for the date; presence of the
                      capture folder path passed by argument.

Safety contract (enforced by tests/test_m0_acceptance_check.py):
  * refuses to execute (exit 2) unless --i-am-authorized is given;
  * --dry-run prints the SQL and bounds and touches nothing (no DB import);
  * every query runs in its own BEGIN ISOLATION LEVEL REPEATABLE READ READ
    ONLY transaction with SET LOCAL statement_timeout <= 20 s, then ROLLBACK;
  * only SELECT / WITH statements are accepted (see assert_read_only_sql);
  * no provider call, no write, no file write other than the receipt itself,
    which is created with mode 'x' (never overwrites).

Execution on grid-svr requires Anik's explicit authorization; see
docs/runbooks/M0-ACCEPTANCE.md.  The Q1/Q2/Q7 statements are the ones specified
in CLAUDE-FRESHNESS-EVIDENCE-REQUEST-20261019.md, with psql variables replaced
by bind parameters.

Usage:
    python3 scripts/m0_acceptance_check.py --date 2026-10-19 --dry-run
    python3 scripts/m0_acceptance_check.py --date 2026-10-19 --i-am-authorized \\
        --capture-dir /path/to/capture --out /tmp/m0-receipt.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

SCHEMA = "m0-acceptance-receipt-v1"
MAX_STATEMENT_TIMEOUT_S = 20
DEFAULT_TICKERS = "^GSPC,SPY"
DEFAULT_SERIES = "YF:^GSPC:close"
DEFAULT_FEATURE = "sp500_full"  # matrix: ^GSPC -> sp500_full (close basis)
DEFAULT_SNAPSHOT_KEY = "^GSPC"  # key under market_briefings.snapshot_data->'equities'
DEFAULT_DIARY_LABEL = "S&P 500"  # key under market_diary.market_moves->'indices'
DEFAULT_LOOKBACK_DAYS = 60
DEFAULT_MAX_AGE_DAYS = 5
RESOLVED_FEATURES = ("spy_full", "xlk_full")
DIARY_INDEX_LABELS = ("S&P 500", "Dow Jones", "Nasdaq", "Russell 2000", "VIX")

# raw_series is ~1.9e9 rows: the saved audit found unhinted plans timed out, so
# raw_series statements carry the same planner hints as the freshness request.
RAW_SERIES_HINTS: tuple[tuple[str, str], ...] = (
    ("enable_hashjoin", "off"),
    ("enable_mergejoin", "off"),
    ("enable_seqscan", "off"),
    ("enable_bitmapscan", "off"),
)

_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|call|do|"
    r"vacuum|analyze|refresh|lock|listen|notify|set\s+role|reindex|cluster)\b",
    re.IGNORECASE,
)


def assert_read_only_sql(sql: str) -> None:
    """Raise ValueError unless ``sql`` is a single SELECT/WITH statement."""
    body = re.sub(r"--[^\n]*", "", sql).strip().rstrip(";").strip()
    if ";" in body:
        raise ValueError("multiple statements are not allowed")
    if not re.match(r"^(select|with)\b", body, re.IGNORECASE):
        raise ValueError("only SELECT/WITH statements are allowed")
    hit = _FORBIDDEN.search(body)
    if hit:
        raise ValueError(f"forbidden keyword in read-only statement: {hit.group(1)}")


@dataclass(frozen=True)
class QuerySpec:
    qid: str
    hop: str
    purpose: str
    sql: str
    params: dict[str, Any]
    timeout_s: int = MAX_STATEMENT_TIMEOUT_S
    raw_series: bool = False
    timestamp_kind: str = ""

    def __post_init__(self) -> None:
        if not (1 <= self.timeout_s <= MAX_STATEMENT_TIMEOUT_S):
            raise ValueError(f"{self.qid}: timeout must be 1..{MAX_STATEMENT_TIMEOUT_S}s")
        assert_read_only_sql(self.sql)


# --------------------------------------------------------------------------
# SQL (Q1 / Q2 / Q7 are the freshness-request statements; psql vars -> binds)
# --------------------------------------------------------------------------

SQL_Q1_CLOCK = """
SELECT now() AT TIME ZONE 'UTC' AS db_now_utc,
       current_setting('transaction_read_only') AS ro,
       current_setting('statement_timeout') AS st
""".strip()

SQL_Q1_SOURCES = """
SELECT id, name, priority_rank, active, last_pull_at
FROM source_catalog
WHERE id IN (1, 2, 182, 184, 524, 1995) OR name IN ('TWELVEDATA', 'yfinance')
ORDER BY id LIMIT 20
""".strip()

SQL_H1_PULL_LOG = """
SELECT puller_name, status, started_at, completed_at, rows_inserted, node_name
FROM pull_log
WHERE puller_name = 'YFinance_Daily'
  AND started_at >= CAST(:od AS date)
  AND started_at <  CAST(:od AS date) + 1
ORDER BY started_at LIMIT 20
""".strip()

SQL_H1_INPUT_VINTAGES = """
SELECT r.id, r.source_id, r.obs_date, r.pull_timestamp, r.value,
       r.raw_payload->>'capture_state'  AS capture_state,
       r.raw_payload->>'capture_policy' AS capture_policy
FROM raw_series r
WHERE r.series_id = :series AND r.source_id = 2
  AND r.pull_status = 'SUCCESS' AND r.obs_date = CAST(:od AS date)
ORDER BY r.pull_timestamp LIMIT 20
""".strip()

SQL_L2_RESOLVED_ROWS = """
SELECT fr.id AS feature_id, fr.name, rs.obs_date, rs.vintage_date, rs.release_date, rs.value, rs.source_priority_used
FROM feature_registry fr
JOIN resolved_series rs ON rs.feature_id = fr.id
WHERE fr.name = :feature AND rs.obs_date = CAST(:od AS date)
ORDER BY rs.vintage_date, rs.id LIMIT 20
""".strip()

SQL_L3_BRIEFINGS = """
SELECT id, briefing_type, briefing_date, created_at,
       snapshot_data->'equities'->CAST(:snapkey AS text) AS snapshot_value,
       snapshot_data->'publication_status'               AS publication_status,
       snapshot_data->>'timestamp'                       AS snapshot_timestamp
FROM market_briefings
WHERE briefing_date >= CAST(:od AS date) AND briefing_date <= CAST(:od AS date) + 3
  AND (CAST(:rid AS bigint) IS NULL OR id = CAST(:rid AS bigint))
ORDER BY created_at LIMIT 60
""".strip()

SQL_Q2_CENSUS = """
WITH t AS (SELECT unnest(string_to_array(:tickers, ',')) AS ticker)
SELECT t.ticker, v.n_vintages, v.first_pull, v.last_pull, v.last_state, v.last_policy, v.n_policy_eligible
FROM t LEFT JOIN LATERAL (
  SELECT count(*) AS n_vintages, min(pull_timestamp) AS first_pull, max(pull_timestamp) AS last_pull,
         (array_agg(raw_payload->>'capture_state'  ORDER BY pull_timestamp DESC))[1] AS last_state,
         (array_agg(raw_payload->>'capture_policy' ORDER BY pull_timestamp DESC))[1] AS last_policy,
         count(*) FILTER (WHERE raw_payload->>'capture_state' = 'POLICY_ELIGIBLE')    AS n_policy_eligible
  FROM raw_series r
  WHERE r.series_id = 'YF:' || t.ticker || ':' || 'close' AND r.source_id = 2
    AND r.pull_status = 'SUCCESS' AND r.obs_date = CAST(:od AS date)) v ON TRUE
ORDER BY t.ticker LIMIT 80
""".strip()

SQL_Q7_SOURCE_MIX = """
WITH t AS (SELECT unnest(string_to_array(:tickers, ',')) AS ticker), s(sid) AS (VALUES (2), (524), (1995))
SELECT t.ticker, s.sid AS source_id, x.obs_date, x.pull_timestamp, x.value
FROM t CROSS JOIN s
LEFT JOIN LATERAL (
  SELECT r.obs_date, r.pull_timestamp, r.value FROM raw_series r
  WHERE r.series_id = 'YF:' || t.ticker || ':' || 'close' AND r.source_id = s.sid
    AND r.pull_status = 'SUCCESS' AND r.obs_date >= CAST(:since AS date)
  ORDER BY r.obs_date DESC, r.pull_timestamp DESC LIMIT 1) x ON TRUE
ORDER BY t.ticker, s.sid LIMIT 220
""".strip()

# H2: (table, date column). Bound = date column >= :floor so the planner can use
# the (ticker, date DESC) / (date DESC) indexes where they exist; a NULL max means
# "no row inside the window", which the check reports as stale.
FLOW_TABLES: tuple[tuple[str, str], ...] = (
    ("insider_trades", "trade_date"),
    ("congressional_trades", "disclosure_date"),
    ("dark_pool_weekly", "report_date"),
    ("etf_flows", "flow_date"),
    ("junction_point_readings", "obs_date"),
)

# Literal statements (no string formatting of SQL): one per table.
FLOW_SQL: dict[str, str] = {
    "insider_trades": """
SELECT max(trade_date) AS max_date, max(created_at) AS materialized_at, count(*) AS row_count
FROM insider_trades
WHERE trade_date >= CAST(:floor AS date)
""".strip(),
    "congressional_trades": """
SELECT max(disclosure_date) AS max_date, max(created_at) AS materialized_at, count(*) AS row_count
FROM congressional_trades
WHERE disclosure_date >= CAST(:floor AS date)
""".strip(),
    "dark_pool_weekly": """
SELECT max(report_date) AS max_date, max(created_at) AS materialized_at, count(*) AS row_count
FROM dark_pool_weekly
WHERE report_date >= CAST(:floor AS date)
""".strip(),
    "etf_flows": """
SELECT max(flow_date) AS max_date, max(created_at) AS materialized_at, count(*) AS row_count
FROM etf_flows
WHERE flow_date >= CAST(:floor AS date)
""".strip(),
    "junction_point_readings": """
SELECT max(obs_date) AS max_date, max(created_at) AS materialized_at, count(*) AS row_count
FROM junction_point_readings
WHERE obs_date >= CAST(:floor AS date)
""".strip(),
}

SQL_H2_RESOLVED = """
SELECT fr.name, max(rs.vintage_date) AS max_vintage_date, max(rs.obs_date) AS max_obs_date, count(*) AS rows_in_window
FROM feature_registry fr
JOIN resolved_series rs ON rs.feature_id = fr.id AND rs.obs_date >= CAST(:floor AS date)
WHERE fr.name = ANY(string_to_array(:features, ','))
GROUP BY fr.name
ORDER BY fr.name LIMIT 10
""".strip()

SQL_H3_DIARY = """
SELECT id, date, generated_at, narrative_model, narrative_fallback,
       market_moves->>'prices_enabled' AS prices_enabled,
       market_moves->>'price_basis'    AS price_basis,
       market_moves->>'price_source'   AS price_source,
       market_moves->'indices'         AS indices,
       market_moves->'indices'->CAST(:label AS text) AS index_entry
FROM market_diary
WHERE date = CAST(:od AS date)
LIMIT 2
""".strip()

SQL_H4_BRIEFINGS = """
SELECT briefing_type, count(*) AS n, max(created_at) AS latest_created_at
FROM market_briefings
WHERE briefing_date = CAST(:od AS date)
GROUP BY briefing_type
ORDER BY briefing_type LIMIT 10
""".strip()


def build_queries(
    target: date,
    *,
    tickers: str = DEFAULT_TICKERS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    series: str = DEFAULT_SERIES,
    feature: str = DEFAULT_FEATURE,
    snapshot_key: str = DEFAULT_SNAPSHOT_KEY,
    diary_label: str = DEFAULT_DIARY_LABEL,
    reasoning_id: int | None = None,
) -> list[QuerySpec]:
    """All queries for one run, in execution order. Pure: no I/O."""
    od = target.isoformat()
    floor = (target - timedelta(days=lookback_days)).isoformat()
    since = (target - timedelta(days=14)).isoformat()
    ts_ing, ts_obs, ts_rdr = "ingestion", "provider observation", "reader"
    qs = [
        QuerySpec("Q1a", "H1", "DB clock and read-only proof", SQL_Q1_CLOCK, {}, 5, False, "db clock"),
        QuerySpec("Q1b", "H1", "source_catalog priority / last_pull_at", SQL_Q1_SOURCES, {}, 5, False, ts_ing),
        QuerySpec("H1a", "H1", "pull_log YFinance_Daily rows for the date", SQL_H1_PULL_LOG, {"od": od}, 10, False, ts_ing),
        QuerySpec("H1b", "H1", f"raw_series input vintages for the date ({series})", SQL_H1_INPUT_VINTAGES, {"od": od, "series": series}, 15, True, f"{ts_obs} (obs_date) + {ts_ing} (pull_timestamp)"),
        QuerySpec("Q2", "H1", "capture-state census for the tickers", SQL_Q2_CENSUS, {"od": od, "tickers": tickers}, 20, True, f"{ts_obs} + {ts_ing}"),
        QuerySpec("Q7", "H1", "source mix (yfinance / TIINGO / 1995) for the tickers", SQL_Q7_SOURCE_MIX, {"since": since, "tickers": tickers}, 20, True, f"{ts_obs} + {ts_ing}"),
    ]
    for table, col in FLOW_TABLES:
        qs.append(QuerySpec(
            f"H2:{table}", "H2", f"{table}: max {col}, max created_at, rows in window",
            FLOW_SQL[table], {"floor": floor}, 20, False,
            f"{ts_obs} ({col}) + materialization (created_at)",
        ))
    qs.append(QuerySpec(
        "H2:resolved", "H2", "resolved_series max vintage_date for spy_full/xlk_full",
        SQL_H2_RESOLVED, {"floor": floor, "features": ",".join(RESOLVED_FEATURES)}, 20, False,
        f"{ts_obs} (obs_date) + {ts_rdr} (vintage_date, date granularity)",
    ))
    qs.append(QuerySpec("L2", "L2", f"resolved_series rows for {feature} at the date", SQL_L2_RESOLVED_ROWS, {"od": od, "feature": feature}, 15, False, f"{ts_obs} (obs_date) + {ts_rdr} (vintage_date, date granularity)"))
    qs.append(QuerySpec("L3:briefings", "L3", "market_briefings rows (date .. date+3) with the snapshot value", SQL_L3_BRIEFINGS, {"od": od, "snapkey": snapshot_key, "rid": reasoning_id}, 15, False, f"{ts_rdr} (created_at)"))
    qs.append(QuerySpec("H3:diary", "H3", "market_diary row for the date", SQL_H3_DIARY, {"od": od, "label": diary_label}, 10, False, f"{ts_rdr} (generated_at)"))
    qs.append(QuerySpec("H4:briefings", "H4", "market_briefings rows for the date", SQL_H4_BRIEFINGS, {"od": od}, 10, False, f"{ts_rdr} (created_at)"))
    return qs


def session_settings(spec: QuerySpec) -> list[tuple[str, str]]:
    """Transaction-local GUCs for one query (applied with set_config(..., true))."""
    settings = [
        ("statement_timeout", f"{int(spec.timeout_s)}s"),
        ("lock_timeout", "2s"),
        ("idle_in_transaction_session_timeout", "30s"),
    ]
    if spec.raw_series:
        settings.extend(RAW_SERIES_HINTS)
    return settings


def transaction_preamble(spec: QuerySpec) -> list[str]:
    """Human-readable equivalent of what run_query does (used by --dry-run)."""
    stmts = ["BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"]
    stmts.extend(f"SET LOCAL {name} = '{value}'" for name, value in session_settings(spec))
    return stmts


def render_dry_run(queries: list[QuerySpec]) -> str:
    out: list[str] = [
        "-- M0 acceptance check: DRY RUN (nothing is executed, no database is contacted)",
        f"-- statement_timeout cap: {MAX_STATEMENT_TIMEOUT_S}s; every query is its own REPEATABLE READ READ ONLY transaction ending in ROLLBACK",
        "",
    ]
    for q in queries:
        out.append(f"-- [{q.hop}] {q.qid}: {q.purpose}  (timestamp: {q.timestamp_kind})")
        out.append(f"-- params: {json.dumps(q.params, sort_keys=True)}")
        for s in transaction_preamble(q):
            out.append(s + ";")
        out.append(q.sql + ";")
        out.append("ROLLBACK;")
        out.append("")
    out += [
        "-- Link 4 (local files only, no SQL): capture.json sha256 == directory name == receipt export sha256;",
        "-- row sha256 (sort_keys, compact, ensure_ascii=False) == receipt row sha256; packet.json entry text sha256",
        "-- and source_record match; DB id/created_at/snapshot value equal the retained record.",
        "-- accepted = link1 AND link2 AND link3 AND link4 for ONE linked example; counts/existence never set it.",
    ]
    return "\n".join(out)


# --------------------------------------------------------------------------
# Execution (only reached with --i-am-authorized and without --dry-run)
# --------------------------------------------------------------------------

def _jsonable(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, (dict, list, str, int, float, bool)) or v is None:
        return v
    try:
        return float(v)
    except (TypeError, ValueError):
        return str(v)


def _open_engine() -> Any:
    """The repo's engine (reads .env via config.settings). Imported lazily."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    from db import get_engine

    return get_engine()


def run_query(engine: Any, spec: QuerySpec) -> dict[str, Any]:
    """Run one spec in its own READ ONLY / REPEATABLE READ transaction."""
    from sqlalchemy import text

    result: dict[str, Any] = {"qid": spec.qid, "purpose": spec.purpose, "timestamp_kind": spec.timestamp_kind}
    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="REPEATABLE READ")
            conn.execute(text("SET TRANSACTION READ ONLY"))
            for name, value in session_settings(spec):  # is_local=true: scoped to this transaction
                conn.execute(text("SELECT set_config(:name, :value, true)"), {"name": name, "value": value})
            try:
                cur = conn.execute(text(spec.sql), spec.params)
                cols = list(cur.keys())
                rows = [{c: _jsonable(v) for c, v in zip(cols, r)} for r in cur.fetchall()]
            finally:
                conn.rollback()
        result["rows"] = rows
        result["error"] = None
    except Exception as exc:  # noqa: BLE001 - a failed query is a receipt entry, not a crash
        result["rows"] = None
        result["error"] = f"{type(exc).__name__}: {str(exc)[:300]}"
    return result


def _hop_status(checks: dict[str, bool | None]) -> str:
    vals = list(checks.values())
    if any(v is False for v in vals):
        return "FAIL"
    if not vals or any(v is None for v in vals):
        return "UNKNOWN"
    return "PASS"


def _rows(results: dict[str, dict[str, Any]], qid: str) -> list[dict[str, Any]] | None:
    r = results.get(qid)
    return None if r is None or r.get("error") else r.get("rows")


def evaluate(
    results: dict[str, dict[str, Any]],
    target: date,
    *,
    max_age_days: int,
    capture_dir: str | None,
) -> dict[str, dict[str, Any]]:
    """Turn raw query results into per-hop checks. Pure: unit-testable offline."""
    hops: dict[str, dict[str, Any]] = {}

    # ---- H1 ingestion
    clock = _rows(results, "Q1a")
    pl = _rows(results, "H1a")
    vint = _rows(results, "H1b")
    h1: dict[str, bool | None] = {
        "db_read_only": None if not clock else clock[0].get("ro") == "on",
        "pull_log_has_run": None if pl is None else len(pl) > 0,
        "rows_inserted_positive": None if pl is None else sum(int(r.get("rows_inserted") or 0) for r in pl) > 0,
        "input_vintage_present": None if vint is None else len(vint) > 0,
        "input_vintages_have_capture_state": None if vint is None else (len(vint) > 0 and all(v.get("capture_state") for v in vint)),
    }
    hops["H1"] = {
        "name": "ingestion",
        "checks": h1,
        "status": _hop_status(h1),
        "evidence": {
            "pull_log": pl,
            "input_vintages": vint,
            "capture_state_census": _rows(results, "Q2"),
            "source_mix": _rows(results, "Q7"),
            "source_catalog": _rows(results, "Q1b"),
            "db_clock": clock,
        },
        "proves": "rows were written for the date by the scheduler and the stored vintages carry a capture_state",
        "does_not_prove": "that the stored close equals the provider's final close, or that readers display it",
    }

    # ---- H2 materialization
    h2: dict[str, bool | None] = {}
    tables: dict[str, Any] = {}
    for table, col in FLOW_TABLES:
        rows = _rows(results, f"H2:{table}")
        if rows is None:
            h2[f"{table}_fresh"] = None
            tables[table] = None
            continue
        r = rows[0] if rows else {}
        max_date = r.get("max_date")
        age = None
        if max_date:
            age = (target - date.fromisoformat(str(max_date)[:10])).days
        tables[table] = {**r, "date_column": col, "age_days_vs_target": age}
        # No row inside the lookback window (max_date NULL) reads as stale/empty.
        h2[f"{table}_fresh"] = age is not None and age <= max_age_days
    resolved = _rows(results, "H2:resolved")
    resolved_ev: dict[str, Any] = {}
    for feat in RESOLVED_FEATURES:
        hit = next((r for r in (resolved or []) if r.get("name") == feat), None)
        resolved_ev[feat] = hit
        if resolved is None:
            h2[f"{feat}_vintage_recent"] = None
        elif hit is None or not hit.get("max_vintage_date"):
            h2[f"{feat}_vintage_recent"] = False
        else:
            vage = (target - date.fromisoformat(str(hit["max_vintage_date"])[:10])).days
            hit["vintage_age_days_vs_target"] = vage
            h2[f"{feat}_vintage_recent"] = vage <= max_age_days
    hops["H2"] = {
        "name": "materialization",
        "checks": h2,
        "status": _hop_status(h2),
        "evidence": {"flow_tables": tables, "resolved_series": resolved_ev, "max_age_days": max_age_days},
        "proves": "the flow tables and the PIT features have rows whose newest date/created_at is within max_age_days of the target date",
        "does_not_prove": "that the writer ran today (grid-hermes), nor that every ticker/feature is fresh, nor value correctness",
    }

    # ---- H3 reasoning
    diary = _rows(results, "H3:diary")
    row = diary[0] if diary else None
    indices = None
    if row:
        raw_idx = row.get("indices")
        indices = json.loads(raw_idx) if isinstance(raw_idx, str) else raw_idx
    no_close = sorted(k for k, v in (indices or {}).items() if isinstance(v, dict) and v.get("status") == "no close for date")
    priced = {k: v for k, v in (indices or {}).items() if isinstance(v, dict) and v.get("status") != "no close for date"}
    h3: dict[str, bool | None] = {
        "diary_row_present": None if diary is None else row is not None,
        "narrative_model_present": None if row is None else bool(row.get("narrative_model")),
        "price_basis_present": None if row is None else bool(row.get("price_basis")),
        "priced_indices_carry_basis_and_source": (
            None if row is None or not priced
            else all(v.get("price_basis") and v.get("source") for v in priced.values())
        ),
    }
    hops["H3"] = {
        "name": "reasoning",
        "checks": h3,
        "status": _hop_status(h3),
        "evidence": {
            "diary_date": None if row is None else row.get("date"),
            "generated_at": None if row is None else row.get("generated_at"),
            "narrative_model": None if row is None else row.get("narrative_model"),
            "narrative_fallback": None if row is None else row.get("narrative_fallback"),
            "prices_enabled": None if row is None else row.get("prices_enabled"),
            "indices_no_close_for_date": no_close,
            "indices_priced": sorted(priced),
        },
        "proves": "a diary row exists for the date and its price fields name their basis/source; lists which indices were refused for lack of a qualified close",
        "does_not_prove": "that the narrative is grounded or correct, or that 'no close for date' is a failure (it is the designed refusal before qualification)",
    }

    # ---- H4 publication retained
    br = _rows(results, "H4:briefings")
    total = None if br is None else sum(int(r.get("n") or 0) for r in br)
    cap: dict[str, Any] = {"provided": capture_dir is not None}
    cap_ok: bool | None = None
    if capture_dir is not None:
        cap["path"] = capture_dir
        cap["exists"] = os.path.exists(capture_dir)
        cap["is_dir"] = os.path.isdir(capture_dir)
        cap["entry_count"] = len(os.listdir(capture_dir)) if cap["is_dir"] else None
        cap_ok = bool(cap["is_dir"] and cap["entry_count"])
    h4: dict[str, bool | None] = {
        "market_briefings_rows_for_date": None if total is None else total > 0,
        "capture_folder_present_and_nonempty": cap_ok,
    }
    hops["H4"] = {
        "name": "publication_retained",
        "checks": h4,
        "status": _hop_status(h4),
        "evidence": {"market_briefings": br, "market_briefings_total": total, "capture_folder": cap},
        "proves": "a briefing row was retained for the date and (if given) the capture folder exists and is non-empty",
        "does_not_prove": "that anything was published externally or that the capture content matches the stored rows",
    }
    return hops


# --------------------------------------------------------------------------
# Linked chain (decides `accepted`): ONE input observation -> the resolved row it
# produced -> the reasoning output that used it (or its explicit unavailable
# reason) -> the retained publication bytes whose sha256 matches the retained
# capture receipt.  Counts / existence / folder presence (H1..H4 above) are
# context only and can never set accepted=true.
# --------------------------------------------------------------------------

MAX_CAPTURE_FILE_BYTES = 256 * 1024 * 1024
_SHA_RE = r"[0-9a-f]{64}"


def canonical_sha256(obj: Any) -> str:
    """sha256 of the record JSON as the retained capture computes it
    (sort_keys, compact separators, ensure_ascii=False)."""
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _as_obj(v: Any) -> Any:
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return v
    return v


def _dict(v: Any) -> dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _ts(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    else:
        try:
            dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _near(a: Any, b: Any, abs_tol: float) -> bool:
    try:
        return abs(float(a) - float(b)) <= max(abs_tol, 1e-9 * abs(float(b)))
    except (TypeError, ValueError):
        return False


def parse_capture_receipt(path: str) -> dict[str, Any]:
    """Read the retained capture receipt.

    JSON: {"export_sha256": "...", "rows": [{"channel": "market_briefings"|"market_diary", "id": 4534, "sha256": "..."}]}.
    Markdown (the executor receipts under grid-diary-*-handoff-*/): the `capture.json` line of the
    files table gives the export sha256; the `| id | type | created_at | row sha256 |` table gives
    market_briefings row hashes. A markdown receipt carries no diary row hashes.
    """
    if os.path.getsize(path) > MAX_CAPTURE_FILE_BYTES:
        raise ValueError("receipt too large")
    with open(path, encoding="utf-8") as fh:
        text_ = fh.read()
    rows: dict[tuple[str, int], str] = {}
    export: str | None = None
    if path.lower().endswith(".json"):
        obj = json.loads(text_)
        export = obj.get("export_sha256")
        for r in obj.get("rows", []):
            rows[(str(r["channel"]), int(r["id"]))] = str(r["sha256"])
        return {"export_sha256": export, "rows": rows, "format": "json"}
    for line in text_.splitlines():
        m = re.match(rf"^\|\s*capture\.json\s*\|\s*\d+\s*\|\s*({_SHA_RE})\s*\|", line)
        if m:
            export = m.group(1)
            continue
        m = re.match(rf"^\|\s*(\d+)\s*\|\s*\w+\s*\|\s*[^|]*\|\s*({_SHA_RE})\s*\|", line)
        if m:
            rows[("market_briefings", int(m.group(1)))] = m.group(2)
    return {"export_sha256": export, "rows": rows, "format": "markdown"}


def _read_bytes(path: str) -> bytes:
    if os.path.getsize(path) > MAX_CAPTURE_FILE_BYTES:
        raise ValueError(f"{path}: larger than {MAX_CAPTURE_FILE_BYTES} bytes")
    with open(path, "rb") as fh:
        return fh.read()


def verify_capture(
    kind: str,
    row: dict[str, Any] | None,
    capture_dir: str | None,
    receipt_path: str | None,
    *,
    snapshot_key: str,
    diary_label: str,
) -> dict[str, Any]:
    """Link 4: the retained bytes of the reasoning row match the retained capture receipt. Local files only."""
    out: dict[str, Any] = {"ok": None, "checks": {}, "reason": None}
    if row is None:
        out.update(ok=False, reason="no reasoning row selected (link 3 failed)")
        return out
    if not capture_dir or not receipt_path:
        out.update(reason="--capture-dir and --capture-receipt are both required for link 4")
        return out
    try:
        cap_bytes = _read_bytes(os.path.join(capture_dir, "capture.json"))
        packet = json.loads(_read_bytes(os.path.join(capture_dir, "packet.json")))
        capture = json.loads(cap_bytes)
        receipt = parse_capture_receipt(receipt_path)
    except (OSError, ValueError, KeyError) as exc:
        out.update(ok=False, reason=f"cannot read retained capture: {type(exc).__name__}: {str(exc)[:200]}")
        return out

    export = hashlib.sha256(cap_bytes).hexdigest()
    ck: dict[str, bool | None] = {
        "capture_json_sha_equals_directory_name": export == os.path.basename(os.path.normpath(capture_dir)),
        "capture_json_sha_equals_receipt_export": export == receipt.get("export_sha256"),
        "packet_export_sha_equals_capture": (packet.get("capture_receipt") or {}).get("export_sha256") == export,
    }
    rid = int(row["id"])
    if kind == "briefing":
        rec = next((r["record"] for r in capture.get("records", [])
                    if r.get("channel") == "market_briefings" and int(r["record"]["id"]) == rid), None)
        pub = next((p for p in packet.get("publications", []) if p.get("id") == f"market_briefings:{rid}"), None)
        ck["record_present_in_capture"] = rec is not None
        ck["packet_entry_present"] = pub is not None
        if rec is not None:
            row_sha = canonical_sha256(rec)
            out["row_sha256"] = row_sha
            ck["row_sha_equals_receipt"] = receipt["rows"].get(("market_briefings", rid)) == row_sha
            ck["db_id_and_created_at_equal_retained"] = (
                int(rec["id"]) == rid and _ts(rec.get("created_at")) == _ts(row.get("created_at")))
            retained_snap = ((rec.get("snapshot_data") or {}).get("equities") or {}).get(snapshot_key)
            ck["db_snapshot_value_equals_retained"] = _as_obj(row.get("snapshot_value")) == retained_snap
        if pub is not None:
            ck["packet_text_sha_matches"] = hashlib.sha256(str(pub.get("text", "")).encode("utf-8")).hexdigest() == pub.get("sha256")
            ck["packet_source_record_equals_capture"] = (rec is not None and canonical_sha256(pub.get("source_record")) == canonical_sha256(rec))
    else:
        rec = next((d for d in capture.get("existing_diary_context", []) if int(d["id"]) == rid), None)
        ev = next((e for e in packet.get("evidence", []) if e.get("id") == f"market_diary:{rid}:context"), None)
        ck["record_present_in_capture"] = rec is not None
        ck["packet_entry_present"] = ev is not None
        if rec is not None:
            row_sha = canonical_sha256(rec)
            out["row_sha256"] = row_sha
            ck["packet_evidence_sha_equals_row_sha"] = ev is not None and ev.get("sha256") == row_sha
            # A markdown receipt has no diary rows, so this stays False (not accepted) unless a JSON receipt lists it.
            ck["row_sha_equals_receipt"] = receipt["rows"].get(("market_diary", rid)) == row_sha
            ck["db_generated_at_equal_retained"] = _ts(rec.get("generated_at")) == _ts(row.get("generated_at"))
            ck["db_index_entry_equals_retained"] = _as_obj(row.get("index_entry")) == ((rec.get("market_moves") or {}).get("indices") or {}).get(diary_label)
    out["checks"] = ck
    out["receipt_format"] = receipt.get("format")
    out["ok"] = all(v is True for v in ck.values())
    return out


def evaluate_chain(
    results: dict[str, dict[str, Any]],
    target: date,
    *,
    series: str,
    feature: str,
    reasoning: str,
    snapshot_key: str,
    diary_label: str,
    capture_dir: str | None,
    capture_receipt: str | None,
) -> dict[str, Any]:
    """Build the linked L1..L4 chain for ONE session. Pure apart from reading the capture files."""
    od = target.isoformat()
    vint = [dict(v, _ts=_ts(v.get("pull_timestamp"))) for v in (_rows(results, "H1b") or [])]
    chain: dict[str, dict[str, Any]] = {}

    # ---- L3 first (it decides which input vintage was actually used)
    used: dict[str, Any] | None = None
    row: dict[str, Any] | None = None
    l3: dict[str, Any] = {"kind": reasoning, "ok": None}
    if reasoning == "briefing":
        rows = _rows(results, "L3:briefings")
        if rows is None:
            l3.update(reason="query failed")
        else:
            row = next((r for r in rows
                        if _dict(_as_obj(r.get("snapshot_value"))).get("date") == od), None)
            if row is None:
                l3.update(ok=False, reason=f"no market_briefings row in [{od}, {od}+3d] whose snapshot_data.equities[{snapshot_key}] is dated {od}",
                          rows_examined=len(rows))
            else:
                snap = _dict(_as_obj(row["snapshot_value"]))
                created = _ts(row.get("created_at"))
                cands = [v for v in vint if v["_ts"] and created and v["_ts"] <= created and _near(v.get("value"), snap.get("value"), 5e-5)]
                used = max(cands, key=lambda v: v["_ts"]) if cands else None
                l3.update(
                    ok=used is not None, row_id=row["id"], briefing_type=row.get("briefing_type"),
                    knowledge_timestamp=row.get("created_at"), snapshot_value=snap,
                    snapshot_timestamp=row.get("snapshot_timestamp"), publication_status=_as_obj(row.get("publication_status")),
                    reason=None if used else "snapshot value equals no input vintage pulled before the briefing was created",
                )
    else:
        rows = _rows(results, "H3:diary")
        if rows is None:
            l3.update(reason="query failed")
        elif not rows:
            l3.update(ok=False, reason=f"no market_diary row for {od}")
        else:
            row = rows[0]
            entry = _dict(_as_obj(row.get("index_entry")))
            gen = _ts(row.get("generated_at"))
            eligible = [v for v in vint if v["_ts"] and gen and v["_ts"] <= gen and v.get("capture_state") == "POLICY_ELIGIBLE"]
            l3.update(row_id=row["id"], knowledge_timestamp=row.get("generated_at"), index_entry=entry)
            if "close" in entry:
                used = next((v for v in sorted(eligible, key=lambda v: v["_ts"], reverse=True)
                             if _near(round(float(v["value"]), 2), entry["close"], 0.005)), None)
                ok = used is not None and entry.get("obs_date") == od
                l3.update(ok=ok, reason=None if ok else "diary close/obs_date equals no POLICY_ELIGIBLE vintage pulled before generation")
            elif entry.get("status") == "no close for date":
                ok = not eligible
                l3.update(ok=ok, explicit_unavailable_reason=entry["status"],
                          reason=None if ok else "diary says 'no close for date' although a POLICY_ELIGIBLE vintage existed before generation")
            else:
                l3.update(ok=False, reason="diary index entry neither carries a close nor the explicit 'no close for date' reason")
    chain["L3_reasoning_output"] = l3

    # ---- L1 input observation (the vintage that was used; else the newest vintage)
    inp = used or (max((v for v in vint if v["_ts"]), key=lambda v: v["_ts"], default=None))
    chain["L1_input_observation"] = {
        "ok": None if _rows(results, "H1b") is None else bool(inp and inp.get("capture_state")),
        "series_id": series, "obs_date": od,
        "pull_timestamp": inp.get("pull_timestamp") if inp else None,
        "capture_state": inp.get("capture_state") if inp else None,
        "value": inp.get("value") if inp else None,
        "raw_series_id": inp.get("id") if inp else None,
        "source_id": inp.get("source_id") if inp else None,
        "selected_as": "vintage used by the reasoning output" if used else "newest vintage (reasoning output did not use one)",
        "reason": (
            "no SUCCESS vintage for the series/date" if not inp
            else None if inp.get("capture_state")
            else "the vintage carries no raw_payload capture_state (unmarked: written before the #853 writer or by another path)"
        ),
        "vintages_examined": [
            {k: v.get(k) for k in ("id", "source_id", "pull_timestamp", "value", "capture_state", "capture_policy")} for v in vint],
    }

    # ---- L2 materialization row produced from that input
    rr = _rows(results, "L2")
    l2: dict[str, Any] = {"table": "resolved_series", "ok": None, "materialized_at_basis": "vintage_date (date granularity; resolved_series has no timestamp)"}
    if rr is None:
        l2["reason"] = "query failed"
    elif not inp:
        l2.update(ok=False, reason="no input vintage to link")
    else:
        pdate = (inp["_ts"].date() if inp.get("_ts") else None)
        hit = next((r for r in rr
                    if _near(r.get("value"), inp.get("value"), 1e-9)
                    and r.get("source_priority_used") == inp.get("source_id")
                    and pdate is not None and str(r.get("vintage_date"))[:10] >= pdate.isoformat()), None)
        l2.update(ok=hit is not None, feature=feature, resolved_rows_examined=len(rr),
                  resolved_rows=[{k: str(r.get(k)) if k.endswith("date") else r.get(k)
                                  for k in ("feature_id", "obs_date", "vintage_date", "release_date", "value", "source_priority_used")}
                                 for r in rr],
                  row_key=None if not hit else {"feature_id": hit.get("feature_id"), "obs_date": str(hit.get("obs_date"))[:10], "vintage_date": str(hit.get("vintage_date"))[:10]},
                  materialized_at=None if not hit else str(hit.get("vintage_date"))[:10],
                  reason=None if hit else "no resolved_series row equals the input vintage (value, source, vintage_date >= pull date)")
    chain["L2_materialization_row"] = l2

    # ---- L4 retained bytes
    chain["L4_retained_publication_bytes"] = verify_capture(
        reasoning, row, capture_dir, capture_receipt, snapshot_key=snapshot_key, diary_label=diary_label)
    order = ["L1_input_observation", "L2_materialization_row", "L3_reasoning_output", "L4_retained_publication_bytes"]
    chain = {k: chain[k] for k in order}
    blocking = [k for k, v in chain.items() if v.get("ok") is not True]
    return {
        "accepted": not blocking,
        "blocking_links": blocking,
        "links": chain,
        "rule": "accepted only if all four links are true for ONE linked example; counts, row existence and folder presence never set it",
    }



def build_receipt(
    target: date,
    hops: dict[str, dict[str, Any]],
    results: dict[str, dict[str, Any]],
    *,
    args_echo: dict[str, Any],
    chain: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "target_date": target.isoformat(),
        "host": socket.gethostname(),
        "read_only": True,
        "authorized_flag": True,
        "arguments": args_echo,
        "accepted": chain["accepted"],
        "chain": chain,
        "context_overall": "PASS" if all(h["status"] == "PASS" for h in hops.values()) else (
            "FAIL" if any(h["status"] == "FAIL" for h in hops.values()) else "UNKNOWN"
        ),
        "context_hops": hops,
        "queries": [
            {"qid": r["qid"], "purpose": r["purpose"], "timestamp_kind": r["timestamp_kind"], "error": r["error"],
             "row_count": None if r["rows"] is None else len(r["rows"])}
            for r in results.values()
        ],
        "notes": [
            "accepted is decided ONLY by the linked chain (one input -> resolved row -> reasoning output -> retained bytes); context_hops/context_overall are counts and existence checks printed for context.",
            "Receipt is an observation, not a certification: see proves/does_not_prove on each context hop.",
            "Provider observation (obs_date), ingestion (pull_timestamp/created_at) and reader (vintage_date/generated_at) are separate timestamps.",
        ],
    }


def default_out_path(target: date) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"m0-acceptance-receipt-{target.isoformat()}-{stamp}.json"


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Read-only M0 acceptance check (held; see docs/runbooks/M0-ACCEPTANCE.md).")
    p.add_argument("--date", required=True, help="target date YYYY-MM-DD")
    p.add_argument("--i-am-authorized", action="store_true", dest="authorized",
                   help="required to execute against the database (operator authorization)")
    p.add_argument("--dry-run", action="store_true", help="print the SQL and bounds only; touch nothing")
    p.add_argument("--tickers", default=DEFAULT_TICKERS, help="comma-separated tickers for H1 census/source mix")
    p.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="H2 window below the target date")
    p.add_argument("--max-age-days", type=int, default=DEFAULT_MAX_AGE_DAYS, help="H2 freshness tolerance vs target date")
    p.add_argument("--capture-dir", default=None,
                   help="retained capture directory capture/<export_sha256>/ (capture.json, packet.json); link 4 and context H4")
    p.add_argument("--capture-receipt", default=None,
                   help="retained capture receipt (executor .md or .json) holding the export and row sha256s; link 4")
    p.add_argument("--series", default=DEFAULT_SERIES, help="link 1: raw_series series_id of the input observation")
    p.add_argument("--feature", default=DEFAULT_FEATURE, help="link 2: resolved_series feature the series materializes into")
    p.add_argument("--reasoning", choices=("briefing", "diary"), default="briefing",
                   help="link 3: which reasoning output to link (market_briefings row or market_diary row)")
    p.add_argument("--reasoning-id", type=int, default=None, help="link 3: pin the market_briefings.id instead of auto-selecting")
    p.add_argument("--snapshot-key", default=DEFAULT_SNAPSHOT_KEY, help="briefing snapshot_data.equities key for the series")
    p.add_argument("--diary-label", default=DEFAULT_DIARY_LABEL, help="diary market_moves.indices label for the series")
    p.add_argument("--out", default=None, help="receipt path (created exclusively; never overwritten)")
    p.add_argument("--strict", action="store_true", help="exit 1 unless accepted is true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None, *, engine_factory: Callable[[], Any] | None = None) -> int:
    args = parse_args(argv)
    try:
        target = date.fromisoformat(args.date)
    except ValueError:
        print(f"error: --date must be YYYY-MM-DD, got {args.date!r}", file=sys.stderr)
        return 2
    if not (1 <= args.lookback_days <= 400) or not (0 <= args.max_age_days <= 60):
        print("error: --lookback-days must be 1..400 and --max-age-days 0..60", file=sys.stderr)
        return 2
    if not re.fullmatch(r"[\^A-Za-z0-9.=\-,]{1,200}", args.tickers):
        print("error: --tickers has unexpected characters", file=sys.stderr)
        return 2

    for name, val in (("--series", args.series), ("--feature", args.feature),
                      ("--snapshot-key", args.snapshot_key), ("--diary-label", args.diary_label)):
        if not re.fullmatch(r"[\^A-Za-z0-9.=:&\-_ ]{1,80}", val):
            print(f"error: {name} has unexpected characters", file=sys.stderr)
            return 2

    queries = build_queries(
        target, tickers=args.tickers, lookback_days=args.lookback_days, series=args.series,
        feature=args.feature, snapshot_key=args.snapshot_key, diary_label=args.diary_label,
        reasoning_id=args.reasoning_id,
    )

    if args.dry_run:
        print(render_dry_run(queries))
        return 0
    if not args.authorized:
        print(
            "REFUSED: this script reads the production database and is held. Re-run with "
            "--i-am-authorized only with the operator's explicit authorization, or use --dry-run "
            "to print the SQL. See docs/runbooks/M0-ACCEPTANCE.md.",
            file=sys.stderr,
        )
        return 2

    engine = (engine_factory or _open_engine)()
    results = {q.qid: run_query(engine, q) for q in queries}
    hops = evaluate(results, target, max_age_days=args.max_age_days, capture_dir=args.capture_dir)
    chain = evaluate_chain(
        results, target, series=args.series, feature=args.feature, reasoning=args.reasoning,
        snapshot_key=args.snapshot_key, diary_label=args.diary_label,
        capture_dir=args.capture_dir, capture_receipt=args.capture_receipt,
    )
    receipt = build_receipt(
        target, hops, results,
        args_echo={k: v for k, v in vars(args).items() if k not in ("authorized",)},
        chain=chain,
    )
    out = args.out or default_out_path(target)
    try:
        with open(out, "x", encoding="utf-8") as fh:  # 'x': refuse to overwrite an existing receipt
            json.dump(receipt, fh, indent=2, sort_keys=True, default=str)
            fh.write("\n")
    except FileExistsError:
        print(f"error: receipt path already exists, refusing to overwrite: {out}", file=sys.stderr)
        return 2
    links = " ".join(f"{k.split('_')[0]}={v.get('ok')}" for k, v in chain["links"].items())
    print(f"receipt written: {out} accepted={chain['accepted']} {links} | context: "
          + " ".join(f"{k}={v['status']}" for k, v in hops.items()))
    return 1 if (args.strict and not chain["accepted"]) else 0


if __name__ == "__main__":
    sys.exit(main())
