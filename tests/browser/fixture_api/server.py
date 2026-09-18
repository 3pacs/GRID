#!/usr/bin/env python3
"""Standalone fixture API server for the GRID browser-acceptance harness.

Serves sanitized, synthetic JSON at the exact API paths the five
first-release journeys call (see tests/browser/README.md), so the real PWA
can be pointed at it via Vite's dev proxy with NO database, NO Docker, and
NO production/staging access.

Deliberately stdlib-only (``http.server``): the real ``api/main.py`` cannot
be imported here because it requires a live Postgres connection at import
time (``api/auth.py``'s ``_get_db_conn`` and friends). Importing it against
no database would either hang or crash, and running the real app is exactly
what this harness exists to avoid needing.

Usage:
    python tests/browser/fixture_api/server.py --port 8000 --scenario healthy
    FIXTURE_SCENARIO=partial python tests/browser/fixture_api/server.py

Scenarios: healthy | partial | empty (default: healthy). See fixtures.py for
what each one changes and why.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import fixtures as fx  # noqa: E402

VALID_SCENARIOS = ("healthy", "partial", "empty")

# path-parameter routes: (compiled regex, handler(match, query, scenario) -> dict | None)
_TICKER_RE = r"[A-Za-z0-9_.\-]+"

ROUTES: list[tuple[str, re.Pattern, callable]] = [
    ("POST", re.compile(r"^/api/v1/auth/login$"), lambda m, q, s: fx.login_response()),
    ("GET", re.compile(r"^/api/v1/auth/verify$"), lambda m, q, s: fx.verify_response()),

    # (a) home / market overview
    ("GET", re.compile(r"^/api/v1/regime/current$"), lambda m, q, s: fx.regime_current(s)),
    ("GET", re.compile(r"^/api/v1/watchlist/?$"), lambda m, q, s: fx.watchlist_list(s)),
    ("GET", re.compile(r"^/api/v1/physics/momentum$"), lambda m, q, s: fx.news_momentum(s)),
    ("GET", re.compile(r"^/api/v1/flows/sectors$"), lambda m, q, s: fx.sector_flows(s)),

    # (b) ticker investigation
    ("GET", re.compile(rf"^/api/v1/dad/ticker/(?P<ticker>{_TICKER_RE})/gold$"),
     lambda m, q, s: fx.dad_ticker_gold(m.group("ticker"), s)),
    ("GET", re.compile(rf"^/api/v1/dad/ticker/(?P<ticker>{_TICKER_RE})/evidence$"),
     lambda m, q, s: fx.dad_ticker_evidence(m.group("ticker"), s)),
    ("GET", re.compile(rf"^/api/v1/dad/ticker/(?P<ticker>{_TICKER_RE})/chart$"),
     lambda m, q, s: fx.dad_ticker_chart(m.group("ticker"), s)),
    ("GET", re.compile(rf"^/api/v1/dad/ticker/(?P<ticker>{_TICKER_RE})/finviz$"),
     lambda m, q, s: fx.dad_ticker_finviz(m.group("ticker"), s)),
    ("GET", re.compile(rf"^/api/v1/dad/ticker/(?P<ticker>{_TICKER_RE})/options$"),
     lambda m, q, s: fx.dad_ticker_options(m.group("ticker"), s)),
    ("GET", re.compile(rf"^/api/v1/valuation/catalyst-timeline/(?P<ticker>{_TICKER_RE})$"),
     lambda m, q, s: fx.catalyst_timeline(m.group("ticker"), s)),

    # (c) watchlist / portfolio, /edge, trust+convergence
    ("GET", re.compile(r"^/api/v1/watchlist/portfolio$"), lambda m, q, s: fx.watchlist_portfolio(s)),
    ("GET", re.compile(rf"^/api/v1/watchlist/(?P<ticker>{_TICKER_RE})/edge$"),
     lambda m, q, s: fx.ticker_edge(m.group("ticker"), s)),
    ("GET", re.compile(r"^/api/v1/intelligence/dashboard$"), lambda m, q, s: fx.intelligence_dashboard(s)),

    # (d) research status — no dedicated PWA page exists (see README);
    # this stubs the closest analog actually wired up, Pipeline Health.
    ("GET", re.compile(r"^/api/v1/system/pipeline-health$"), lambda m, q, s: fx.pipeline_health(s)),

    # (e) data health / source drill-down
    ("GET", re.compile(r"^/api/v1/system/health$"), lambda m, q, s: fx.system_health(s)),
    ("GET", re.compile(r"^/api/v1/system/hermes-status$"), lambda m, q, s: fx.hermes_status(s)),
    ("GET", re.compile(rf"^/api/v1/sectors/(?P<sector>[A-Za-z0-9_.\- ]+)/health$"),
     lambda m, q, s: fx.sector_health(m.group("sector"), s)),
]


class FixtureHandler(BaseHTTPRequestHandler):
    scenario = "healthy"

    def _send_json(self, payload, status=200):
        body = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        # Permissive CORS: this only ever serves synthetic fixtures on
        # localhost during development, never real data.
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def _dispatch(self, method):
        parts = urlsplit(self.path)
        path = parts.path
        query = parse_qs(parts.query)
        for route_method, pattern, handler in ROUTES:
            if route_method != method:
                continue
            match = pattern.match(path)
            if match:
                try:
                    result = handler(match, query, self.scenario)
                except Exception as exc:  # pragma: no cover - fixture bug guard
                    self._send_json({"error": True, "message": f"fixture handler failed: {exc}"}, status=500)
                    return
                self._send_json(result)
                return
        self._send_json(
            {"error": True, "status": 404, "message": f"no fixture route for {method} {path}"},
            status=404,
        )

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def log_message(self, fmt, *args):
        sys.stderr.write("[fixture-api] " + (fmt % args) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=int(os.environ.get("FIXTURE_PORT", "8000")))
    parser.add_argument(
        "--scenario",
        choices=VALID_SCENARIOS,
        default=os.environ.get("FIXTURE_SCENARIO", "healthy"),
    )
    args = parser.parse_args()

    FixtureHandler.scenario = args.scenario
    server = ThreadingHTTPServer(("127.0.0.1", args.port), FixtureHandler)
    print(
        f"[fixture-api] serving scenario={args.scenario!r} on http://127.0.0.1:{args.port} "
        f"(ticker={fx.TICKER}) — synthetic data only, no DB, no production access",
        file=sys.stderr,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
