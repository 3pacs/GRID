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

SQL_H1_GSPC_VINTAGES = """
SELECT r.id, r.source_id, r.obs_date, r.pull_timestamp, r.value,
       r.raw_payload->>'capture_state' AS capture_state
FROM raw_series r
WHERE r.series_id = 'YF:^GSPC:close' AND r.source_id = 2
  AND r.pull_status = 'SUCCESS' AND r.obs_date = CAST(:od AS date)
ORDER BY r.pull_timestamp LIMIT 20
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
  WHERE r.series_id = 'YF:' || t.ticker || ':close' AND r.source_id = 2
    AND r.pull_status = 'SUCCESS' AND r.obs_date = CAST(:od AS date)) v ON TRUE
ORDER BY t.ticker LIMIT 80
""".strip()

SQL_Q7_SOURCE_MIX = """
WITH t AS (SELECT unnest(string_to_array(:tickers, ',')) AS ticker), s(sid) AS (VALUES (2), (524), (1995))
SELECT t.ticker, s.sid AS source_id, x.obs_date, x.pull_timestamp, x.value
FROM t CROSS JOIN s
LEFT JOIN LATERAL (
  SELECT r.obs_date, r.pull_timestamp, r.value FROM raw_series r
  WHERE r.series_id = 'YF:' || t.ticker || ':close' AND r.source_id = s.sid
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
SELECT date, generated_at, narrative_model, narrative_fallback,
       market_moves->>'prices_enabled' AS prices_enabled,
       market_moves->>'price_basis'    AS price_basis,
       market_moves->>'price_source'   AS price_source,
       market_moves->'indices'         AS indices
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
        QuerySpec("H1b", "H1", "raw_series YF:^GSPC:close vintages for the date", SQL_H1_GSPC_VINTAGES, {"od": od}, 15, True, f"{ts_obs} (obs_date) + {ts_ing} (pull_timestamp)"),
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
    qs.append(QuerySpec("H3:diary", "H3", "market_diary row for the date", SQL_H3_DIARY, {"od": od}, 10, False, f"{ts_rdr} (generated_at)"))
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
        "gspc_vintage_present": None if vint is None else len(vint) > 0,
        "gspc_vintages_have_capture_state": None if vint is None else (len(vint) > 0 and all(v.get("capture_state") for v in vint)),
    }
    hops["H1"] = {
        "name": "ingestion",
        "checks": h1,
        "status": _hop_status(h1),
        "evidence": {
            "pull_log": pl,
            "gspc_vintages": vint,
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


def build_receipt(
    target: date,
    hops: dict[str, dict[str, Any]],
    results: dict[str, dict[str, Any]],
    *,
    args_echo: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema": SCHEMA,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "target_date": target.isoformat(),
        "host": socket.gethostname(),
        "read_only": True,
        "authorized_flag": True,
        "arguments": args_echo,
        "overall": "PASS" if all(h["status"] == "PASS" for h in hops.values()) else (
            "FAIL" if any(h["status"] == "FAIL" for h in hops.values()) else "UNKNOWN"
        ),
        "hops": hops,
        "queries": [
            {"qid": r["qid"], "purpose": r["purpose"], "timestamp_kind": r["timestamp_kind"], "error": r["error"],
             "row_count": None if r["rows"] is None else len(r["rows"])}
            for r in results.values()
        ],
        "notes": [
            "Receipt is an observation, not a certification: see proves/does_not_prove on each hop.",
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
    p.add_argument("--capture-dir", default=None, help="H4: capture folder path to check for presence")
    p.add_argument("--out", default=None, help="receipt path (created exclusively; never overwritten)")
    p.add_argument("--strict", action="store_true", help="exit 1 if the overall result is not PASS")
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

    queries = build_queries(target, tickers=args.tickers, lookback_days=args.lookback_days)

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
    receipt = build_receipt(
        target, hops, results,
        args_echo={k: v for k, v in vars(args).items() if k not in ("authorized",)},
    )
    out = args.out or default_out_path(target)
    try:
        with open(out, "x", encoding="utf-8") as fh:  # 'x': refuse to overwrite an existing receipt
            json.dump(receipt, fh, indent=2, sort_keys=True, default=str)
            fh.write("\n")
    except FileExistsError:
        print(f"error: receipt path already exists, refusing to overwrite: {out}", file=sys.stderr)
        return 2
    print(f"receipt written: {out} overall={receipt['overall']} " + " ".join(f"{k}={v['status']}" for k, v in hops.items()))
    return 1 if (args.strict and receipt["overall"] != "PASS") else 0


if __name__ == "__main__":
    sys.exit(main())
