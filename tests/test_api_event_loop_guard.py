"""grid-api must never run blocking work on its event loop.

grid-api is a single uvicorn process with one asyncio loop. On 2026-11-06
it stopped answering even /health for minutes: ~390 ``async def`` route
handlers contained no ``await`` at all, so all their synchronous DB / LLM /
graph work ran on the loop (db.py: "DB connection held 114.0s —
acquired_by=MainThread"). They are now plain ``def`` (FastAPI runs those in
its threadpool). This file is the guard that keeps it that way, plus the
diagnostics that make the next blocker name itself.
"""

from __future__ import annotations

import ast
import asyncio
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any


os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("DB_PASSWORD", "testpass")

REPO = Path(__file__).resolve().parents[1]
API_FILES = sorted((REPO / "api").rglob("*.py"))

_ROUTE_METHODS = {"get", "post", "put", "delete", "patch", "api_route", "websocket"}
_OFFLOAD_CALLS = {"to_thread", "run_in_threadpool", "run_in_executor", "run_sync"}

# Synchronous calls that must never run directly inside an async route.
_BLOCKING_ATTRS = {
    "connect", "begin", "execute", "fetchall", "read_sql", "read_sql_query",
    "build_actor_graph", "score_thesis", "run_all_checks", "sleep",
}
_BLOCKING_NAMES = {"build_actor_graph", "score_thesis", "run_all_checks", "get_engine"}
_BLOCKING_PREFIXES = ("_build_", "build_")
_BLOCKING_MODULE_CALLS = {("requests", "get"), ("requests", "post"), ("httpx", "get"),
                          ("httpx", "post"), ("time", "sleep"), ("pd", "read_sql")}

# Async routes that legitimately contain no await: they only schedule work
# on an executor and return a job id.
_NO_AWAIT_ALLOWLIST = {
    "api/routers/discovery.py::trigger_orthogonality",
    "api/routers/discovery.py::trigger_clustering",
}


def _is_route(node: ast.AsyncFunctionDef) -> bool:
    return any(
        isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute)
        and d.func.attr in _ROUTE_METHODS
        for d in node.decorator_list
    )


def _async_routes():
    for path in API_FILES:
        rel = path.relative_to(REPO).as_posix()
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.AsyncFunctionDef) and _is_route(node):
                yield rel, node


def _direct_calls(node: ast.AST):
    """Calls in ``node``'s own body: not in nested defs/lambdas, not in offload args."""
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
            continue
        if isinstance(child, ast.Call):
            func = child.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            if name in _OFFLOAD_CALLS:
                continue  # whatever it is handed runs in a worker thread
            yield child
        yield from _direct_calls(child)


def _blocking_reason(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Attribute):
        if isinstance(func.value, ast.Name) and (func.value.id, func.attr) in _BLOCKING_MODULE_CALLS:
            return f"{func.value.id}.{func.attr}"
        if func.attr in _BLOCKING_ATTRS:
            return f".{func.attr}()"
        if func.attr.startswith(_BLOCKING_PREFIXES) and func.attr.endswith("_cache"):
            return f".{func.attr}()"
    elif isinstance(func, ast.Name):
        if func.id in _BLOCKING_NAMES:
            return f"{func.id}()"
        if func.id.startswith(_BLOCKING_PREFIXES) and func.id.endswith("_cache"):
            return f"{func.id}()"
    return None


def test_every_async_route_awaits_something():
    offenders = []
    for rel, node in _async_routes():
        if f"{rel}::{node.name}" in _NO_AWAIT_ALLOWLIST:
            continue
        if not any(isinstance(n, (ast.Await, ast.AsyncFor, ast.AsyncWith)) for n in ast.walk(node)):
            offenders.append(f"{rel}:{node.lineno} {node.name}")
    assert not offenders, (
        "These `async def` routes never await, so all of their work runs on "
        "grid-api's single event loop and freezes every request. Make them "
        f"plain `def` (FastAPI runs those in its threadpool): {offenders}"
    )


def test_no_blocking_call_directly_in_an_async_route():
    offenders = []
    for rel, node in _async_routes():
        for call in _direct_calls(node):
            if isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name) \
                    and call.func.value.id in {"websocket", "request", "file", "queue"}:
                continue  # Starlette's own async I/O objects
            reason = _blocking_reason(call)
            if reason:
                offenders.append(f"{rel}:{call.lineno} {node.name} -> {reason}")
    assert not offenders, (
        "Blocking calls inside `async def` routes: wrap them in "
        "`await asyncio.to_thread(...)` / `run_in_threadpool(...)`, or make "
        f"the route plain `def`: {offenders}"
    )


def test_guard_catches_the_original_bug_shape():
    src = (
        "@router.get('/x')\n"
        "async def h():\n"
        "    with engine.connect() as c:\n"
        "        return _build_full_actor_cache()\n"
    )
    node = ast.parse(src).body[0]
    reasons = {_blocking_reason(c) for c in _direct_calls(node)}
    assert ".connect()" in reasons and "_build_full_actor_cache()" in reasons
    offloaded = ast.parse(
        "async def h():\n    return await asyncio.to_thread(_build_full_actor_cache)\n"
    ).body[0]
    assert not [c for c in _direct_calls(offloaded) if _blocking_reason(c)]


# ── Event-loop lag watchdog ─────────────────────────────────────────────────

def _blocking_coroutine_for_test(seconds: float) -> None:
    time.sleep(seconds)  # deliberately blocks the loop


def test_watchdog_names_the_blocking_function():
    from api.loop_watchdog import EventLoopWatchdog

    async def main() -> EventLoopWatchdog:
        wd = EventLoopWatchdog(
            asyncio.get_running_loop(), threading.get_ident(),
            threshold=0.2, interval=0.05, cooldown=60.0,
        )
        wd.start()
        await asyncio.sleep(0.15)  # healthy loop: no stall
        assert wd.stalls_detected == 0
        _blocking_coroutine_for_test(0.8)
        await asyncio.sleep(0.3)
        wd.stop()
        return wd

    wd = asyncio.run(main())
    assert wd.stalls_detected >= 1
    assert wd.last_stack and "_blocking_coroutine_for_test" in wd.last_stack


def test_watchdog_rate_limits_stack_logs():
    from api.loop_watchdog import EventLoopWatchdog

    async def main() -> tuple[int, str | None]:
        wd = EventLoopWatchdog(
            asyncio.get_running_loop(), threading.get_ident(),
            threshold=0.1, interval=0.02, cooldown=60.0,
        )
        wd.start()
        stacks = []
        for _ in range(3):
            _blocking_coroutine_for_test(0.3)
            await asyncio.sleep(0.15)
            stacks.append(wd.last_stack)
        wd.stop()
        return wd.stalls_detected, stacks

    stalls, stacks = asyncio.run(main())
    assert stalls >= 2
    assert stacks[0] is not None and stacks[0] is stacks[-1], "one stack log per cooldown window"


# ── db.py: long holds acquired on the event loop carry their call site ──────

def test_checkout_on_event_loop_records_call_site():
    import db

    rec = object()

    async def acquire_on_loop() -> None:
        db._record_checkout_open(rec)

    asyncio.run(acquire_on_loop())
    try:
        entry = db._open_checkouts[id(rec)]
        assert entry[3] and "acquire_on_loop" in entry[3]
        assert "test_api_event_loop_guard.py" in entry[3]
    finally:
        db._open_checkouts.pop(id(rec), None)


def test_checkout_off_loop_skips_call_site():
    import db

    rec = object()
    t = threading.Thread(target=db._record_checkout_open, args=(rec,), name="worker-1")
    t.start()
    t.join()
    try:
        assert db._open_checkouts[id(rec)][3] is None
    finally:
        db._open_checkouts.pop(id(rec), None)


def test_long_hold_on_loop_always_warns_with_call_site(monkeypatch):
    import db

    seen: list[str] = []
    monkeypatch.setattr(db.log, "warning", lambda msg, **kw: seen.append(msg.format(**kw)))
    db._report_long_hold(6.0, "MainThread", "MainThread", 0.0,
                         "api/routers/x.py:58 get_actor_network > intelligence/actors/graph.py:223 build")
    assert seen and "ON THE EVENT LOOP" in seen[0] and "get_actor_network" in seen[0]


# ── Startup: the dashboard re-warm loop no longer starves later hooks ───────

def test_deferred_startup_returns_and_starts_every_warmer(monkeypatch):
    import api.main as main
    import db
    import oracle.model_factory as mf
    from api.routers import flows, intelligence_actors, intelligence_spider

    started: list[str] = []
    monkeypatch.setattr(db, "health_check", lambda: True)
    monkeypatch.setattr(db, "get_engine", lambda: object())
    monkeypatch.setattr(mf, "migrate_default_models", lambda engine: None)
    monkeypatch.setattr(intelligence_spider, "warm_graph_async", lambda: started.append("spider"))
    monkeypatch.setattr(intelligence_actors, "warm_actor_graph_async", lambda: started.append("actors"))
    monkeypatch.setattr(flows, "start_sector_flow_warm_thread", lambda: started.append("sector_flows"))
    monkeypatch.setattr(main, "_dashboard_warm_loop", lambda: started.append("dashboard"))

    t = threading.Thread(target=main._sync_deferred_startup, args=(None,))
    t.start()
    t.join(10)
    assert not t.is_alive(), "deferred startup must return (the dashboard loop has its own thread)"
    time.sleep(0.1)
    assert {"spider", "actors", "sector_flows", "dashboard"} <= set(started)


# ── Converted handlers that used to break ────────────────────────────────────

def test_sector_dive_returns_sector_detail(monkeypatch):
    """It awaited a sync function (TypeError on every call) before."""
    from api.routers import flows

    monkeypatch.setattr(flows, "get_sector_detail", lambda name, tok: {"sector": name})
    assert flows.get_sector_dive("Tech", "t") == {"sector": "Tech"}


def test_tradingview_webhook_writes_off_the_event_loop(monkeypatch):
    from api.routers import tradingview

    threads: dict[str, int] = {}

    class _Conn:
        def __enter__(self):
            threads["db"] = threading.get_ident()
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, *a, **k):
            return None

    monkeypatch.setattr(tradingview, "get_db_engine", lambda: SimpleNamespace(begin=_Conn))
    monkeypatch.setattr(tradingview, "_get_source_id", lambda engine: 1)

    class _Req:
        async def json(self) -> Any:
            return {"ticker": "AAPL", "action": "buy", "price": "185.5"}

    async def run() -> dict:
        threads["loop"] = threading.get_ident()
        return await tradingview.receive_webhook(_Req(), _key="k")

    out = asyncio.run(run())
    assert out["ticker"] == "AAPL" and out["signal_value"] == 1.0
    assert threads["db"] != threads["loop"]


# ── /tps/today: no dependency on the never-created SQL function ─────────────

class _RecordingConn:
    def __init__(self, rows, total):
        self.rows, self.total, self.sql = rows, total, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, stmt, params=None):
        self.sql.append((str(stmt), dict(params or {})))
        if "COUNT(*) FROM tps_snapshots" in str(stmt):
            return SimpleNamespace(scalar=lambda: self.total)
        return SimpleNamespace(fetchall=lambda: self.rows)


def test_tps_today_has_no_coverage_layer_count_function(monkeypatch):
    from api.routers import tps

    row = SimpleNamespace(_mapping={
        "ticker": "LMT", "as_of_date": "2026-11-05", "score": 7.5,
        "coverage": {"actor_hops": True, "fara_admin": False, "direct_contracts": True},
        "evidence": {}, "layer_scores": {}, "generated_at": "2026-11-05T11:00:00Z",
    })
    conn = _RecordingConn([row], total=1)
    monkeypatch.setattr(tps, "get_db_engine", lambda: SimpleNamespace(connect=lambda: conn))

    out = tps.get_today(limit=25, offset=0, min_coverage_layers=2, _token="t")

    assert [e["ticker"] for e in out["entries"]] == ["LMT"]
    assert out["entries"][0]["coverage_layer_count"] == 2
    assert out["total"] == 1 and out["has_more"] is False
    assert len(conn.sql) == 2
    for sql, params in conn.sql:
        assert "coverage_layer_count(" not in sql
        assert "jsonb_each(coverage)" in sql
        assert params["min_layers"] == 2  # the count query filters too


# ── Faster builders keep their results ──────────────────────────────────────

def test_flow_fetch_caches_each_window_and_skips_evidence(monkeypatch):
    import analysis.flow_aggregator as fa

    queries: list[tuple[str, Any]] = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, stmt, params):
            queries.append((str(stmt), params["cutoff"]))
            return SimpleNamespace(fetchall=lambda: [
                ("insider", "Jane", "AAPL", 1000.0, "inflow", None, "2026-11-01"),
            ])

    engine = SimpleNamespace(connect=_Conn)
    fa._flow_cache.clear()
    try:
        a = fa._fetch_flows(engine, 30)
        b = fa._fetch_flows(engine, 60)
        assert fa._fetch_flows(engine, 30) is a and fa._fetch_flows(engine, 60) is b
        assert len(queries) == 2, "each window fetched once within the TTL"
        assert all("evidence" not in sql for sql, _ in queries)
        assert a[0]["confidence"] == "estimated" and a[0]["evidence"] == {}
    finally:
        fa._flow_cache.clear()


def test_actor_tier_classification_unchanged():
    import analysis.flow_aggregator as fa

    assert fa._classify_actor_tier("insider", "  federal reserve bank of ny") == "sovereign"
    assert fa._classify_actor_tier("insider", "Norway GPFG") == "sovereign"
    assert fa._classify_actor_tier("13f", "calpers") == "regional"
    assert fa._classify_actor_tier("congressional", None) == fa._SOURCE_TIER_MAP.get("congressional", "individual")


def test_canvas_wealth_flows_single_query_same_selection(monkeypatch):
    from api.routers import canvas

    rows = [
        {"aid": "a", "id": 1, "from_actor": "a", "to_entity": "x", "amount_estimate": 5, "confidence": "c", "flow_date": "2026-11-02"},
        {"aid": "a", "id": 2, "from_actor": "y", "to_entity": "a", "amount_estimate": 1, "confidence": None, "flow_date": "2026-11-04"},
        {"aid": "b", "id": 1, "from_actor": "a", "to_entity": "x", "amount_estimate": 5, "confidence": "c", "flow_date": "2026-11-02"},
        {"aid": "b", "id": 3, "from_actor": "b", "to_entity": "z", "amount_estimate": 2, "confidence": "c", "flow_date": None},
    ]
    executes: list[dict] = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, stmt, params):
            executes.append(params)
            return SimpleNamespace(mappings=lambda: SimpleNamespace(fetchall=lambda: rows))

    edges = canvas._load_wealth_flows(SimpleNamespace(connect=_Conn), ["a", "b", "a"])
    assert len(executes) == 1 and executes[0]["ids"] == ["a", "b"]
    assert [(e["source"], e["target"]) for e in edges] == [
        ("a:y", "a:a"),   # a's newest first
        ("a:a", "a:x"),
        ("a:b", "a:z"),   # (a, x) already seen via actor a
    ]
