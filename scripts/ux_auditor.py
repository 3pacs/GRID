"""
GRID UX Auditor — Hermes-driven autonomous UX testing and improvement.

Hermes acts as a synthetic user, hitting every API endpoint and evaluating:
  1. AVAILABILITY — does the endpoint return 2xx?
  2. LATENCY — is response time acceptable?
  3. DATA QUALITY — are responses well-formed, non-empty, consistent?
  4. COHERENCE — do related endpoints tell a consistent story?
  5. UX FRICTION — LLM evaluates the data from a user's perspective

Each audit cycle:
  - Crawls all API endpoints as a contributor user
  - Measures response times and status codes
  - Feeds results to Hermes for UX analysis
  - Generates improvement tickets (stored in DB)
  - Optionally generates PWA component suggestions

Runs as step 7 in the Hermes operator cycle, or standalone:
    python scripts/ux_auditor.py               # single audit
    python scripts/ux_auditor.py --verbose      # detailed output
"""

from __future__ import annotations

import json
import time
import traceback
from datetime import datetime, timezone
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

UX_AUDIT_INTERVAL_HOURS = 6       # run UX audit every 6 hours
LATENCY_WARN_MS = 2000            # flag endpoints slower than 2s
LATENCY_CRITICAL_MS = 5000        # flag endpoints slower than 5s
BASE_URL = "http://localhost:8000"

# Endpoints to test, grouped by user journey
# (method, path, description, expected_keys, requires_auth)
_ENDPOINT_REGISTRY: list[dict[str, Any]] = [
    # ── Health (no auth) ──
    {"method": "GET", "path": "/api/v1/system/health", "desc": "System health", "auth": False,
     "expect_keys": ["status", "checks"]},

    # ── Regime Journey ──
    {"method": "GET", "path": "/api/v1/regime/current", "desc": "Current regime", "auth": True,
     "expect_keys": ["macro"]},
    {"method": "GET", "path": "/api/v1/regime/all", "desc": "All regimes", "auth": True,
     "expect_keys": ["macro", "strategy"]},
    {"method": "GET", "path": "/api/v1/regime/history?days=30", "desc": "Regime history", "auth": True,
     "expect_keys": ["history"]},
    {"method": "GET", "path": "/api/v1/regime/synthesis", "desc": "LLM synthesis", "auth": True},

    # ── Strategy ──
    {"method": "GET", "path": "/api/v1/strategy/active", "desc": "Active strategies", "auth": True},

    # ── Journal ──
    {"method": "GET", "path": "/api/v1/journal/", "desc": "Decision journal", "auth": True},

    # ── Discovery ──
    {"method": "GET", "path": "/api/v1/discovery/clusters", "desc": "Cluster state", "auth": True},

    # ── Associations ──
    {"method": "GET", "path": "/api/v1/associations/correlations", "desc": "Correlations", "auth": True},

    # ── Physics ──
    {"method": "GET", "path": "/api/v1/physics/dashboard", "desc": "Physics dashboard", "auth": True},
    {"method": "GET", "path": "/api/v1/physics/news-energy", "desc": "News energy", "auth": True},

    # ── Knowledge ──
    {"method": "GET", "path": "/api/v1/knowledge/recent?limit=5", "desc": "Recent knowledge", "auth": True},

    # ── Signals ──
    {"method": "GET", "path": "/api/v1/signals/live", "desc": "Live signals", "auth": True},

    # ── Models ──
    {"method": "GET", "path": "/api/v1/models/", "desc": "Model registry", "auth": True},

    # ── Watchlist ──
    {"method": "GET", "path": "/api/v1/watchlist/", "desc": "Watchlist", "auth": True},

    # ── Backtest ──
    {"method": "GET", "path": "/api/v1/backtest/results", "desc": "Backtest results", "auth": True},

    # ── System ──
    {"method": "GET", "path": "/api/v1/system/status", "desc": "System status", "auth": True,
     "expect_keys": ["database", "server"]},

    # ── Snapshots ──
    {"method": "GET", "path": "/api/v1/snapshots/?limit=5", "desc": "Recent snapshots", "auth": True},

    # ── Options ──
    {"method": "GET", "path": "/api/v1/options/scanner", "desc": "Options scanner", "auth": True},

    # ── LLM ──
    {"method": "GET", "path": "/api/v1/ollama/health", "desc": "LLM health", "auth": True},
]

# User journeys — sequences of endpoints a real user would hit
_USER_JOURNEYS: list[dict[str, Any]] = [
    {
        "name": "New User Onboarding",
        "desc": "User registers, sees regime, reads synthesis",
        "steps": [
            "/api/v1/system/health",
            "/api/v1/regime/current",
            "/api/v1/regime/all",
            "/api/v1/regime/synthesis",
        ],
    },
    {
        "name": "Daily Check-in",
        "desc": "Returning user checks regime, signals, journal",
        "steps": [
            "/api/v1/regime/current",
            "/api/v1/strategy/active",
            "/api/v1/signals/live",
            "/api/v1/journal/",
        ],
    },
    {
        "name": "Deep Analysis",
        "desc": "Power user exploring physics, associations, knowledge",
        "steps": [
            "/api/v1/physics/dashboard",
            "/api/v1/associations/correlations",
            "/api/v1/discovery/clusters",
            "/api/v1/knowledge/recent?limit=5",
        ],
    },
    {
        "name": "Strategy Review",
        "desc": "User reviews strategy assignments and backtest",
        "steps": [
            "/api/v1/strategy/active",
            "/api/v1/regime/all",
            "/api/v1/backtest/results",
            "/api/v1/models/",
        ],
    },
]


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------

def _get_auth_token(base_url: str = BASE_URL) -> str | None:
    """Login as master password and return JWT token."""
    try:
        import os
        password = os.getenv("GRID_MASTER_PASSWORD", "")
        if not password:
            from config import settings
            password = settings.GRID_MASTER_PASSWORD
        resp = requests.post(
            f"{base_url}/api/v1/auth/login",
            json={"password": password},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("token")
        log.warning("UX Auditor auth failed: {s}", s=resp.status_code)
        return None
    except Exception as exc:
        log.warning("UX Auditor auth error: {e}", e=str(exc))
        return None


# ---------------------------------------------------------------------------
# Endpoint crawler
# ---------------------------------------------------------------------------

def _test_endpoint(
    endpoint: dict[str, Any],
    token: str | None,
    base_url: str = BASE_URL,
) -> dict[str, Any]:
    """Hit a single endpoint and return test results."""
    url = f"{base_url}{endpoint['path']}"
    headers = {}
    if endpoint.get("auth") and token:
        headers["Authorization"] = f"Bearer {token}"

    result: dict[str, Any] = {
        "path": endpoint["path"],
        "method": endpoint["method"],
        "desc": endpoint["desc"],
    }

    start = time.monotonic()
    try:
        resp = requests.request(
            endpoint["method"],
            url,
            headers=headers,
            timeout=15,
        )
        latency_ms = (time.monotonic() - start) * 1000

        result["status_code"] = resp.status_code
        result["latency_ms"] = round(latency_ms, 1)
        result["ok"] = 200 <= resp.status_code < 300

        # Check response body
        try:
            body = resp.json()
            result["has_data"] = bool(body)
            result["response_type"] = type(body).__name__

            # Check expected keys
            if endpoint.get("expect_keys") and isinstance(body, dict):
                missing = [k for k in endpoint["expect_keys"] if k not in body]
                result["missing_keys"] = missing
                if missing:
                    result["data_quality"] = "DEGRADED"
                else:
                    result["data_quality"] = "OK"
            else:
                result["data_quality"] = "OK" if body else "EMPTY"

            # Sample the data for LLM analysis (truncated)
            result["sample"] = _truncate_sample(body)

        except ValueError:
            result["has_data"] = False
            result["data_quality"] = "NOT_JSON"

        # Latency classification
        if latency_ms > LATENCY_CRITICAL_MS:
            result["latency_grade"] = "CRITICAL"
        elif latency_ms > LATENCY_WARN_MS:
            result["latency_grade"] = "SLOW"
        else:
            result["latency_grade"] = "OK"

    except requests.Timeout:
        result["status_code"] = None
        result["latency_ms"] = 15000
        result["ok"] = False
        result["error"] = "TIMEOUT"
        result["latency_grade"] = "CRITICAL"
        result["data_quality"] = "UNAVAILABLE"
    except requests.ConnectionError:
        result["status_code"] = None
        result["latency_ms"] = None
        result["ok"] = False
        result["error"] = "CONNECTION_REFUSED"
        result["latency_grade"] = "CRITICAL"
        result["data_quality"] = "UNAVAILABLE"
    except Exception as exc:
        result["status_code"] = None
        result["ok"] = False
        result["error"] = str(exc)
        result["data_quality"] = "ERROR"

    return result


def _truncate_sample(data: Any, max_chars: int = 500) -> str:
    """Truncate response data for LLM consumption."""
    s = json.dumps(data, default=str)
    if len(s) > max_chars:
        return s[:max_chars] + "... [truncated]"
    return s


# ---------------------------------------------------------------------------
# Journey tester
# ---------------------------------------------------------------------------

def _test_journey(
    journey: dict[str, Any],
    token: str | None,
    endpoint_results: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Evaluate a user journey using already-collected endpoint results."""
    steps_ok = 0
    total_latency = 0.0
    blockers: list[str] = []

    for path in journey["steps"]:
        result = endpoint_results.get(path)
        if result is None:
            blockers.append(f"{path}: not tested")
            continue
        if result["ok"]:
            steps_ok += 1
        else:
            blockers.append(f"{path}: {result.get('error', result.get('status_code', 'failed'))}")
        total_latency += result.get("latency_ms", 0) or 0

    completion_rate = steps_ok / len(journey["steps"]) if journey["steps"] else 0

    return {
        "name": journey["name"],
        "desc": journey["desc"],
        "completion_rate": round(completion_rate, 2),
        "total_latency_ms": round(total_latency, 1),
        "steps": len(journey["steps"]),
        "steps_ok": steps_ok,
        "blockers": blockers,
        "grade": "PASS" if completion_rate == 1.0 else ("DEGRADED" if completion_rate > 0.5 else "FAIL"),
    }


# ---------------------------------------------------------------------------
# LLM Analysis
# ---------------------------------------------------------------------------

_UX_ANALYSIS_PROMPT = """You are GRID's UX auditor. Analyze these API test results from a USER EXPERIENCE perspective.

Think like a first-time user visiting grid.stepdad.finance. They see a dark trading dashboard.
What works? What's confusing? What's broken? What would make them leave?

## Endpoint Results
{endpoint_summary}

## User Journey Results
{journey_summary}

## Response Samples
{samples}

Respond with this exact structure:

SCORE: <1-10 overall UX score>

WORKING_WELL:
- <thing that works well>
- <thing that works well>

FRICTION_POINTS:
- <issue>: <impact on user> | SEVERITY: LOW|MEDIUM|HIGH|CRITICAL
- <issue>: <impact on user> | SEVERITY: LOW|MEDIUM|HIGH|CRITICAL

IMPROVEMENTS:
- <specific actionable improvement> | EFFORT: LOW|MEDIUM|HIGH | IMPACT: LOW|MEDIUM|HIGH
- <specific actionable improvement> | EFFORT: LOW|MEDIUM|HIGH | IMPACT: LOW|MEDIUM|HIGH

DATA_GAPS:
- <missing data or endpoint that would improve UX>

PRIORITY_FIX: <the single most impactful thing to fix right now>
"""


def _run_llm_analysis(
    endpoint_results: list[dict[str, Any]],
    journey_results: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Have Hermes analyze the UX test results."""
    try:
        from llamacpp.client import get_client
        client = get_client()
        if not client.is_available:
            return None

        # Build summaries
        endpoint_summary = "\n".join(
            f"  {r['desc']:25s} | {r.get('status_code', '---'):>3} | "
            f"{r.get('latency_ms', '---'):>7}ms | "
            f"data={r.get('data_quality', '?'):10s} | "
            f"latency={r.get('latency_grade', '?')}"
            for r in endpoint_results
        )

        journey_summary = "\n".join(
            f"  {j['name']:25s} | {j['grade']:8s} | "
            f"{j['steps_ok']}/{j['steps']} steps | "
            f"{j['total_latency_ms']:.0f}ms total"
            + (f" | blockers: {', '.join(j['blockers'])}" if j['blockers'] else "")
            for j in journey_results
        )

        # Include samples from problematic endpoints
        problem_samples = []
        for r in endpoint_results:
            if r.get("data_quality") not in ("OK", None) or not r.get("ok"):
                sample = r.get("sample", "no data")
                problem_samples.append(f"  {r['path']}: {sample}")
        # Also include a few good samples for context
        good_samples = [
            f"  {r['path']}: {r.get('sample', 'no data')}"
            for r in endpoint_results
            if r.get("ok") and r.get("sample")
        ][:3]

        samples_text = "\n".join(problem_samples + good_samples) or "  No samples available"

        prompt = _UX_ANALYSIS_PROMPT.format(
            endpoint_summary=endpoint_summary,
            journey_summary=journey_summary,
            samples=samples_text,
        )

        response = client.chat(
            messages=[
                {"role": "system", "content": (
                    "You are a UX expert auditing a trading intelligence PWA. "
                    "Be specific, actionable, and honest. Focus on what real users "
                    "would experience. No vague suggestions."
                )},
                {"role": "user", "content": prompt},
            ],
            temperature=0.4,
            num_predict=1500,
        )

        if response:
            return _parse_ux_analysis(response)
        return None

    except Exception as exc:
        log.warning("UX LLM analysis failed: {e}", e=str(exc))
        return None


def _parse_ux_analysis(response: str) -> dict[str, Any]:
    """Parse the structured LLM response into a dict."""
    result: dict[str, Any] = {"raw": response}

    # Extract score
    for line in response.split("\n"):
        line = line.strip()
        if line.startswith("SCORE:"):
            try:
                result["score"] = int(line.split(":")[1].strip().split("/")[0].strip())
            except (ValueError, IndexError):
                pass
        elif line.startswith("PRIORITY_FIX:"):
            result["priority_fix"] = line.split(":", 1)[1].strip()

    # Extract sections
    current_section = None
    sections: dict[str, list[str]] = {
        "WORKING_WELL": [],
        "FRICTION_POINTS": [],
        "IMPROVEMENTS": [],
        "DATA_GAPS": [],
    }

    for line in response.split("\n"):
        line = line.strip()
        if line.rstrip(":") in sections:
            current_section = line.rstrip(":")
        elif current_section and line.startswith("- "):
            sections[current_section].append(line[2:])

    result.update(sections)
    return result


# ---------------------------------------------------------------------------
# Improvement ticket storage
# ---------------------------------------------------------------------------

_ENSURE_TABLE_SQL = text("""
    CREATE TABLE IF NOT EXISTS ux_audit_results (
        id              SERIAL PRIMARY KEY,
        audit_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        score           INTEGER,
        total_endpoints INTEGER,
        endpoints_ok    INTEGER,
        avg_latency_ms  REAL,
        journey_pass    INTEGER,
        journey_total   INTEGER,
        priority_fix    TEXT,
        friction_points JSONB,
        improvements    JSONB,
        full_report     JSONB,
        acted_on        BOOLEAN DEFAULT FALSE
    )
""")

_INSERT_AUDIT_SQL = text("""
    INSERT INTO ux_audit_results
        (score, total_endpoints, endpoints_ok, avg_latency_ms,
         journey_pass, journey_total, priority_fix,
         friction_points, improvements, full_report)
    VALUES
        (:score, :total_endpoints, :endpoints_ok, :avg_latency_ms,
         :journey_pass, :journey_total, :priority_fix,
         :friction_points, :improvements, :full_report)
    RETURNING id
""")


def _save_audit(engine: Any, report: dict[str, Any]) -> int | None:
    """Persist audit results to the database."""
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_TABLE_SQL)

            analysis = report.get("analysis", {})
            endpoint_results = report.get("endpoints", [])
            journey_results = report.get("journeys", [])

            ok_count = sum(1 for e in endpoint_results if e.get("ok"))
            latencies = [e["latency_ms"] for e in endpoint_results if e.get("latency_ms")]
            avg_lat = sum(latencies) / len(latencies) if latencies else 0
            j_pass = sum(1 for j in journey_results if j["grade"] == "PASS")

            row = conn.execute(
                _INSERT_AUDIT_SQL.bindparams(
                    score=analysis.get("score"),
                    total_endpoints=len(endpoint_results),
                    endpoints_ok=ok_count,
                    avg_latency_ms=round(avg_lat, 1),
                    journey_pass=j_pass,
                    journey_total=len(journey_results),
                    priority_fix=analysis.get("priority_fix"),
                    friction_points=json.dumps(analysis.get("FRICTION_POINTS", [])),
                    improvements=json.dumps(analysis.get("IMPROVEMENTS", [])),
                    full_report=json.dumps(report, default=str),
                )
            ).fetchone()
            return row[0] if row else None
    except Exception as exc:
        log.warning("Failed to save UX audit: {e}", e=str(exc))
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_ux_audit(
    engine: Any | None = None,
    base_url: str = BASE_URL,
    verbose: bool = False,
) -> dict[str, Any]:
    """Execute a full UX audit cycle.

    Parameters:
        engine: SQLAlchemy engine (for persisting results).
        base_url: API base URL to test against.
        verbose: Log detailed results.

    Returns:
        dict: Full audit report with endpoints, journeys, and LLM analysis.
    """
    log.info("═══ UX Audit starting ═══")
    audit_start = time.monotonic()

    report: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # 1. Authenticate
    token = _get_auth_token(base_url)
    if not token:
        log.warning("UX Audit: could not authenticate — testing unauthenticated endpoints only")

    # 2. Crawl all endpoints
    endpoint_results: list[dict[str, Any]] = []
    endpoint_lookup: dict[str, dict[str, Any]] = {}

    for ep in _ENDPOINT_REGISTRY:
        if ep.get("auth") and not token:
            endpoint_results.append({
                "path": ep["path"], "desc": ep["desc"],
                "ok": False, "error": "NO_AUTH", "data_quality": "SKIPPED",
            })
            continue

        result = _test_endpoint(ep, token, base_url)
        endpoint_results.append(result)
        endpoint_lookup[ep["path"]] = result

        if verbose:
            status = "OK" if result["ok"] else "FAIL"
            log.info(
                "  {s} {d:25s} | {code:>3} | {lat:>7}ms",
                s=status, d=ep["desc"],
                code=result.get("status_code", "---"),
                lat=result.get("latency_ms", "---"),
            )

    report["endpoints"] = endpoint_results

    # 3. Evaluate user journeys
    journey_results: list[dict[str, Any]] = []
    for journey in _USER_JOURNEYS:
        jr = _test_journey(journey, token, endpoint_lookup)
        journey_results.append(jr)

        if verbose:
            log.info(
                "  Journey: {n} — {g} ({ok}/{total})",
                n=journey["name"], g=jr["grade"],
                ok=jr["steps_ok"], total=jr["steps"],
            )

    report["journeys"] = journey_results

    # 4. LLM analysis
    analysis = _run_llm_analysis(endpoint_results, journey_results)
    if analysis:
        report["analysis"] = analysis
        log.info(
            "UX Score: {s}/10 | Priority: {p}",
            s=analysis.get("score", "?"),
            p=analysis.get("priority_fix", "none"),
        )
    else:
        report["analysis"] = {"score": None, "note": "LLM unavailable"}
        log.warning("UX Audit: LLM analysis unavailable")

    # 5. Summary stats
    ok_count = sum(1 for e in endpoint_results if e.get("ok"))
    fail_count = len(endpoint_results) - ok_count
    slow_count = sum(1 for e in endpoint_results if e.get("latency_grade") in ("SLOW", "CRITICAL"))
    j_pass = sum(1 for j in journey_results if j["grade"] == "PASS")

    report["summary"] = {
        "endpoints_total": len(endpoint_results),
        "endpoints_ok": ok_count,
        "endpoints_failed": fail_count,
        "endpoints_slow": slow_count,
        "journeys_total": len(journey_results),
        "journeys_pass": j_pass,
        "score": analysis.get("score") if analysis else None,
        "elapsed_seconds": round(time.monotonic() - audit_start, 1),
    }

    # 6. Persist
    if engine is not None:
        audit_id = _save_audit(engine, report)
        report["audit_id"] = audit_id
        log.info("UX Audit saved — id={id}", id=audit_id)

    log.info(
        "═══ UX Audit complete — {ok}/{total} endpoints OK, "
        "{jp}/{jt} journeys pass, {t:.1f}s ═══",
        ok=ok_count, total=len(endpoint_results),
        jp=j_pass, jt=len(journey_results),
        t=report["summary"]["elapsed_seconds"],
    )

    return report


def maybe_run_ux_audit(
    state: Any,
    engine: Any,
    dry_run: bool = False,
) -> dict[str, Any] | None:
    """Run UX audit if enough time has passed.

    Designed to be called from hermes_operator.run_cycle().

    Parameters:
        state: OperatorState with last_ux_audit timestamp.
        engine: SQLAlchemy engine.
        dry_run: If True, skip actual audit.

    Returns:
        dict: Audit report, or None if skipped.
    """
    now = datetime.now(timezone.utc)

    last_audit = getattr(state, "last_ux_audit", None)
    if last_audit is not None:
        hours_since = (now - last_audit).total_seconds() / 3600
        if hours_since < UX_AUDIT_INTERVAL_HOURS:
            return None

    if dry_run:
        return {"skipped": "dry_run"}

    log.info("Running UX audit cycle")
    try:
        report = run_ux_audit(engine=engine)
        state.last_ux_audit = now  # type: ignore[attr-defined]
        return report
    except Exception as exc:
        log.warning("UX audit failed: {e}", e=str(exc))
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="GRID UX Auditor")
    parser.add_argument("--verbose", "-v", action="store_true", help="Detailed output")
    parser.add_argument("--no-db", action="store_true", help="Skip DB persistence")
    parser.add_argument("--base-url", default=BASE_URL, help="API base URL")
    args = parser.parse_args()

    import sys
    _GRID_DIR = str(__import__("pathlib").Path(__file__).resolve().parent.parent)
    if _GRID_DIR not in sys.path:
        sys.path.insert(0, _GRID_DIR)

    engine = None
    if not args.no_db:
        try:
            from db import get_engine
            engine = get_engine()
        except Exception:
            log.warning("Could not connect to DB — running without persistence")

    report = run_ux_audit(engine=engine, base_url=args.base_url, verbose=args.verbose)

    # Print summary
    s = report["summary"]
    print(f"\n{'='*50}")
    print(f"  UX AUDIT RESULTS")
    print(f"{'='*50}")
    print(f"  Endpoints:  {s['endpoints_ok']}/{s['endpoints_total']} OK, {s['endpoints_slow']} slow")
    print(f"  Journeys:   {s['journeys_pass']}/{s['journeys_total']} pass")
    print(f"  UX Score:   {s.get('score', '?')}/10")
    print(f"  Time:       {s['elapsed_seconds']}s")

    analysis = report.get("analysis", {})
    if analysis.get("priority_fix"):
        print(f"\n  PRIORITY FIX: {analysis['priority_fix']}")
    if analysis.get("FRICTION_POINTS"):
        print(f"\n  FRICTION POINTS:")
        for fp in analysis["FRICTION_POINTS"]:
            print(f"    - {fp}")
    if analysis.get("IMPROVEMENTS"):
        print(f"\n  IMPROVEMENTS:")
        for imp in analysis["IMPROVEMENTS"]:
            print(f"    - {imp}")
    print()
