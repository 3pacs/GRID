#!/usr/bin/env python3
"""Live GRID fleet health probe (HTTP, stdlib-only).

Polls the running API's system endpoints and prints a data-health-style
dashboard. Unlike ``scripts/hermes_health.py`` (which runs *on* the server and
queries the DB directly), this probe reaches the API over HTTP, so it works
from anywhere with network egress to the host — a laptop, a Claude Code web
session, or grid-svr itself.

Endpoints used (see ``api/routers/system.py``):
  - GET /api/v1/system/health      (public)   DB, freshness, pool, threads, disk, LLM, API keys
  - GET /api/v1/system/freshness   (auth)     per-family data freshness
  - GET /api/v1/system/services    (auth)     systemd service state
  - GET /api/v1/system/hermes-status (auth)   scheduler / Hermes cycle health

Auth: the richer endpoints require a JWT. Pass one via ``--token`` or the
``GRID_API_TOKEN`` env var. Without a token the probe still reports the public
``/health`` payload, which already covers most of the fleet signal.

Usage:
    python scripts/live_health_probe.py
    python scripts/live_health_probe.py --base-url https://grid.stepdad.finance
    GRID_API_TOKEN=$JWT python scripts/live_health_probe.py --json

Exit codes (monitoring-friendly): 0 = ok, 1 = degraded, 2 = unreachable.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any

DEFAULT_BASE_URL = os.environ.get("GRID_API_BASE_URL", "https://grid.stepdad.finance")
API_PREFIX = "/api/v1/system"

# Exit codes
EXIT_OK = 0
EXIT_DEGRADED = 1
EXIT_UNREACHABLE = 2


def _fetch(base_url: str, path: str, token: str | None, timeout: float) -> tuple[int, Any]:
    """GET a JSON endpoint. Returns (http_status, parsed_body_or_error_string).

    Network/parse failures surface as status 0 with the reason as the body so
    callers can render them without exceptions bubbling up.
    """
    url = f"{base_url.rstrip('/')}{path}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Accept", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (trusted host)
            raw = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(raw)
            except json.JSONDecodeError:
                return resp.status, raw
    except urllib.error.HTTPError as exc:
        body: Any
        try:
            body = json.loads(exc.read().decode("utf-8"))
        except Exception:
            body = str(exc)
        return exc.code, body
    except urllib.error.URLError as exc:
        return 0, f"{exc.reason}"
    except Exception as exc:  # pragma: no cover - defensive
        return 0, str(exc)


def _icon(value: object) -> str:
    """Render a checks-dict value as a status glyph."""
    if value is True:
        return "✓"
    if value is False:
        return "✗"
    return str(value)


def probe(base_url: str, token: str | None, timeout: float) -> tuple[int, dict[str, Any]]:
    """Run the probe. Returns (exit_code, structured_result)."""
    result: dict[str, Any] = {"base_url": base_url, "endpoints": {}}

    status_code, health = _fetch(base_url, f"{API_PREFIX}/health", token, timeout)
    result["endpoints"]["health"] = {"http_status": status_code, "body": health}

    if status_code == 0:
        result["overall"] = "unreachable"
        result["reason"] = health
        return EXIT_UNREACHABLE, result
    if status_code != 200 or not isinstance(health, dict):
        result["overall"] = "unreachable"
        result["reason"] = f"unexpected /health response (HTTP {status_code})"
        return EXIT_UNREACHABLE, result

    overall = health.get("status", "unknown")
    result["overall"] = overall

    # Authenticated endpoints are best-effort; a 401 just means no token.
    if token:
        for name, path in (
            ("freshness", f"{API_PREFIX}/freshness"),
            ("services", f"{API_PREFIX}/services"),
            ("hermes_status", f"{API_PREFIX}/hermes-status"),
        ):
            sc, body = _fetch(base_url, path, token, timeout)
            result["endpoints"][name] = {"http_status": sc, "body": body}

    exit_code = EXIT_OK if overall == "ok" else EXIT_DEGRADED
    return exit_code, result


def _print_dashboard(result: dict[str, Any]) -> None:
    base = result["base_url"]
    overall = result.get("overall", "unknown")
    banner = {"ok": "OK", "degraded": "DEGRADED", "unreachable": "UNREACHABLE"}.get(
        overall, overall.upper()
    )
    print("=== GRID LIVE HEALTH PROBE ===")
    print(f"Target : {base}")
    print(f"Status : {banner}")

    if overall == "unreachable":
        print(f"Reason : {result.get('reason')}")
        print("\nThe API did not answer. Check egress to the host, the tunnel, "
              "and that grid-api is running.")
        return

    health = result["endpoints"]["health"]["body"]
    checks = health.get("checks", {}) if isinstance(health, dict) else {}
    reasons = health.get("degraded_reasons", []) if isinstance(health, dict) else []

    if reasons:
        print("\nDEGRADED REASONS")
        for r in reasons:
            print(f"  ⚠ {r}")

    if checks:
        print("\nCHECKS")
        for key in sorted(checks):
            print(f"  {key:24} {_icon(checks[key])}")

    fresh = result["endpoints"].get("freshness")
    if fresh and fresh.get("http_status") == 200 and isinstance(fresh["body"], dict):
        families = fresh["body"].get("families", [])
        stale = [f for f in families if isinstance(f, dict) and f.get("is_stale")]
        print(f"\nDATA FRESHNESS  ({len(families)} families, {len(stale)} stale)")
        for f in stale[:20]:
            name = f.get("family") or f.get("name") or "?"
            age = f.get("staleness_hours") or f.get("age_hours") or "?"
            print(f"  ⚠ {name:28} {age}h")
    elif fresh and fresh.get("http_status") in (401, 403):
        print("\nDATA FRESHNESS  (skipped — needs a valid --token / GRID_API_TOKEN)")

    print()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Probe a running GRID API for fleet health.")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL,
                        help=f"API base URL (default: {DEFAULT_BASE_URL})")
    parser.add_argument("--token", default=os.environ.get("GRID_API_TOKEN"),
                        help="JWT for authenticated endpoints (or set GRID_API_TOKEN)")
    parser.add_argument("--timeout", type=float, default=15.0, help="Per-request timeout (s)")
    parser.add_argument("--json", action="store_true", help="Emit raw JSON instead of a dashboard")
    args = parser.parse_args(argv)

    exit_code, result = probe(args.base_url, args.token, args.timeout)

    if args.json:
        print(json.dumps(result, indent=2, default=str))
    else:
        _print_dashboard(result)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
