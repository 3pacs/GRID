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
    ("POST", re.compile(r"^/api/v1/chat/compose$"), lambda m, q, s: fx.chat_compose(s)),
    ("GET", re.compile(rf"^/api/v1/watchlist/(?P<ticker>{_TICKER_RE})/quote$"),
     lambda m, q, s: fx.ticker_quote(m.group("ticker"), s)),
    ("GET", re.compile(r"^/api/v1/alerts$"), lambda m, q, s: fx.alerts_list(s)),
    ("GET", re.compile(r"^/api/v1/options/recommendations$"),
     lambda m, q, s: fx.options_recommendations(s, (q.get("ticker") or [None])[0])),
    ("GET", re.compile(r"^/api/v1/ten-year-portfolio/weekly$"),
     lambda m, q, s: fx.ten_year_portfolio_weekly(s)),

    # (b) ticker investigation — note GET .../gold/stream (SSE) has NO route
    # here on purpose: TickerLookup.jsx's EventSource falls back to the four
    # GETs below on a stream error, and a 404 is exactly that trigger. See
    # README "SSE fallback" note.
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
    ("GET", re.compile(rf"^/api/v1/watchlist/(?P<ticker>{_TICKER_RE})/analysis$"),
     lambda m, q, s: fx.watchlist_ticker_analysis(m.group("ticker"), s)),
    ("GET", re.compile(rf"^/api/v1/watchlist/(?P<ticker>{_TICKER_RE})/overview$"),
     lambda m, q, s: fx.watchlist_ticker_overview(m.group("ticker"), s)),
    ("GET", re.compile(rf"^/api/v1/derivatives/gex/(?P<ticker>{_TICKER_RE})$"),
     lambda m, q, s: fx.derivatives_gex(m.group("ticker"), s)),
    ("GET", re.compile(rf"^/api/v1/derivatives/vanna-charm/(?P<ticker>{_TICKER_RE})$"),
     lambda m, q, s: fx.derivatives_vanna_charm(m.group("ticker"), s)),
    ("GET", re.compile(rf"^/api/v1/derivatives/flow-timeline/(?P<ticker>{_TICKER_RE})$"),
     lambda m, q, s: fx.derivatives_flow_timeline(m.group("ticker"), s)),
    ("GET", re.compile(r"^/api/v1/intelligence/dashboard$"), lambda m, q, s: fx.intelligence_dashboard(s)),

    # (d) research status — no dedicated PWA page exists (see README).
    # Pipeline Health is the closest analog wired up; Discovery.jsx is the
    # real operator-only "research status" surface the lead's browser run
    # identified.
    ("GET", re.compile(r"^/api/v1/system/pipeline-health$"), lambda m, q, s: fx.pipeline_health(s)),
    ("GET", re.compile(r"^/api/v1/discovery/jobs$"), lambda m, q, s: fx.discovery_jobs(s)),
    ("GET", re.compile(r"^/api/v1/discovery/results/(?P<type>orthogonality|clustering)$"),
     lambda m, q, s: fx.discovery_results(m.group("type"), s)),
    ("GET", re.compile(r"^/api/v1/discovery/hypotheses/results$"),
     lambda m, q, s: fx.discovery_hypotheses_results(s)),
    ("GET", re.compile(r"^/api/v1/discovery/hypotheses$"), lambda m, q, s: fx.discovery_hypotheses(s)),
    # /research/latest: --research-unavailable (default off) forces the
    # envelope-level "unavailable" shape regardless of --scenario, for
    # checking that specific rendering without needing a real DB outage.
    ("GET", re.compile(r"^/api/v1/snapshots/research/latest$"),
     lambda m, q, s: (
         {"status": "unavailable", "reason": "fixture: category unavailable"}
         if FixtureHandler.research_unavailable else fx.research_status(s)
     )),

    # God View pillars (W6) — 'cftc' is the one built pillar, registered
    # BEFORE the catch-all (mirrors api/routers/godview_pillars.py's own
    # FastAPI registration order: the concrete /pillars/cftc route must be
    # added first so it, not the catch-all, matches "cftc").
    ("GET", re.compile(r"^/api/v1/godview/pillars/cftc$"), lambda m, q, s: fx.godview_pillar_cftc(s)),
    ("GET", re.compile(r"^/api/v1/godview/pillars/(?P<pillar>[A-Za-z0-9_]+)$"),
     lambda m, q, s: fx.godview_pillar_unbuilt(m.group("pillar"))),

    # (e) data health / source drill-down — Operator.jsx's actual six calls.
    ("GET", re.compile(r"^/api/v1/system/status$"), lambda m, q, s: fx.system_status(s)),
    ("GET", re.compile(r"^/api/v1/system/health$"), lambda m, q, s: fx.system_health(s)),
    ("GET", re.compile(r"^/api/v1/system/freshness$"), lambda m, q, s: fx.system_freshness(s)),
    ("GET", re.compile(r"^/api/v1/system/hermes-status$"), lambda m, q, s: fx.hermes_status(s)),
    ("GET", re.compile(r"^/api/v1/snapshots/issues$"), lambda m, q, s: fx.snapshots_issues(s)),
    ("GET", re.compile(r"^/api/v1/snapshots/latest/(?P<category>[A-Za-z0-9_]+)$"),
     lambda m, q, s: fx.snapshots_latest(m.group("category"), s)),
    ("GET", re.compile(rf"^/api/v1/sectors/(?P<sector>[A-Za-z0-9_.\- ]+)/health$"),
     lambda m, q, s: fx.sector_health(m.group("sector"), s)),
]

# POST /api/v1/chat/ask/stream is handled separately (SSE, not JSON) — see
# FixtureHandler._handle_ask_stream.
_ASK_STREAM_PATH = re.compile(r"^/api/v1/chat/ask/stream$")


class FixtureHandler(BaseHTTPRequestHandler):
    scenario = "healthy"
    research_unavailable = False

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

    def _handle_ask_stream(self):
        """SSE response for POST /api/v1/chat/ask/stream, matching what
        pwa/src/api.js's askStream() parses: `data: {"delta": "..."}\\n\\n`
        chunks over a chunked/streamed connection, closed when done — no
        explicit terminator event is required (api.js:945-976).
        """
        deltas = fx.chat_ask_stream_deltas(self.scenario)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Connection", "close")
        self.end_headers()
        full = ""
        for delta in deltas:
            full += delta
            chunk = f"data: {json.dumps({'delta': delta})}\n\n".encode("utf-8")
            try:
                self.wfile.write(chunk)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        parts = urlsplit(self.path)
        if _ASK_STREAM_PATH.match(parts.path):
            self._handle_ask_stream()
            return
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
    parser.add_argument(
        "--research-unavailable",
        action="store_true",
        default=False,
        help=(
            "Force GET /api/v1/snapshots/research/latest to the envelope-level "
            "{'status': 'unavailable', ...} shape regardless of --scenario "
            "(default off — /research/latest follows --scenario like everything else)."
        ),
    )
    args = parser.parse_args()

    FixtureHandler.scenario = args.scenario
    FixtureHandler.research_unavailable = args.research_unavailable
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
