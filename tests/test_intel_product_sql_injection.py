"""SQL-injection regression guard for the paid Intelligence product router
(``api/routers/intel.py``, prefix ``/api/v1/intel``).

Historical context: the contracts-phase-1 handoff (2026-04-11) raised
pre-existing SQL-injection flags against this router. They were closed in
commits ``81968d2b`` ("resolve critical SQL injection ...") and ``4e6c0932``
("security hardening ...") by converting every dynamic query to SQLAlchemy
``text()`` with named bind parameters. This test is the *regression guard*
that keeps them closed: it fails the moment any handler re-introduces an
f-string / ``.format()`` / ``%`` / concatenated user value into the SQL
string itself.

The whole suite is fully offline — there is NO live DB. We monkeypatch
``api.routers.intel.get_db_engine`` with a recording fake engine that captures
the *compiled* SQL text and the params dict handed to every ``execute()``
call, then assert, for adversarial user input, that:

  1. the malicious string is delivered as a **bound parameter value**
     (present in the params dict), and
  2. the malicious string never appears in the rendered SQL text
     (i.e. it was *not* interpolated).

If a future edit splices ``q`` straight into the SQL, assertion (2) fails.
"""
from __future__ import annotations

import sys
from types import ModuleType

import pytest


# ---------------------------------------------------------------------------
# Import-time guard (mirrors tests/test_intelligence_search.py): prefer the
# real api.auth / api.dependencies, but stub them if their heavy transitive
# deps (psycopg2, jose, passlib) are unavailable in a lightweight env. The
# product router calls ``Depends(require_auth)`` but we invoke the handler
# functions directly, bypassing the dependency, so a stub is harmless.
# ---------------------------------------------------------------------------
try:
    import api.auth  # noqa: F401
except Exception:  # pragma: no cover - env-dependent
    _auth_stub = ModuleType("api.auth")
    _auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.auth"] = _auth_stub

try:
    import api.dependencies  # noqa: F401
except Exception:  # pragma: no cover - env-dependent
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub


from api.routers import intel as intel_router  # noqa: E402


# A payload that, if interpolated, would terminate a string literal and
# tack on a destructive statement. If it ever shows up verbatim in the
# compiled SQL, the query is injectable.
INJECTION = "'; DROP TABLE oracle_predictions; --"
INJECTION_TICKER = "AAPL'; DELETE FROM actors WHERE '1'='1"


def _render(clause) -> str:
    """Render a SQLAlchemy ``text()`` / string into raw SQL text for inspection.

    ``text()`` clauses expose their template via ``.text``; plain strings pass
    through unchanged. We deliberately read the *template* (with ``:name``
    placeholders intact) — a parameterised query keeps user values out of it.
    """
    return getattr(clause, "text", clause)


class _RecordingConnection:
    """A stand-in DB connection that records every execute() call instead of
    touching a database. Returns an empty result set so handlers run their
    full code path (and exercise *every* query) without a backend."""

    def __init__(self, log: list[tuple[str, dict]]):
        self._log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, clause, params=None):
        self._log.append((_render(clause), dict(params or {})))
        return _EmptyResult()

    # Some handlers use engine.begin(); alias to the same recorder.
    def begin(self):  # pragma: no cover - not used by GET handlers
        return self


class _EmptyResult:
    def fetchall(self):
        return []

    def fetchone(self):
        return None


class _RecordingEngine:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def connect(self):
        return _RecordingConnection(self.calls)

    def begin(self):  # pragma: no cover - GET handlers use connect()
        return _RecordingConnection(self.calls)


@pytest.fixture
def recording_engine(monkeypatch) -> _RecordingEngine:
    """Patch the router's ``get_db_engine`` to hand back a recorder."""
    eng = _RecordingEngine()
    monkeypatch.setattr(intel_router, "get_db_engine", lambda: eng)
    return eng


def _assert_bound_not_interpolated(
    engine: _RecordingEngine, needle: str, *, min_calls: int = 1
) -> None:
    """Core assertion: ``needle`` must appear only inside bound params, never
    inside any rendered SQL text."""
    assert len(engine.calls) >= min_calls, (
        f"expected >= {min_calls} execute() call(s), got {len(engine.calls)}"
    )

    in_params = False
    for sql, params in engine.calls:
        # (2) the dangerous string must never be interpolated into SQL text
        assert needle not in sql, (
            "user input was interpolated into the SQL string (injectable!):\n"
            f"  SQL    : {sql}\n"
            f"  needle : {needle!r}"
        )
        # every rendered query must rely on :named placeholders, not literals
        if params:
            assert ":" in sql, f"params supplied but no bind placeholder in SQL: {sql}"
        # (1) track whether the dangerous string was carried as a bound value
        if any(needle in str(v) for v in params.values()):
            in_params = True

    assert in_params, (
        f"adversarial input {needle!r} never reached a bound parameter — "
        "the handler may be dropping or, worse, interpolating it"
    )


# ── /search — the broadest user-facing query surface ─────────────────────

def test_search_passes_query_as_bound_param(recording_engine):
    intel_router.intel_search(
        q=INJECTION, type="all", limit=25, offset=0, _token="t"
    )
    # /search fires up to 3 queries (actors, entities, tickers); the LIKE
    # branches wrap the needle as ``%...%`` and the ticker branch upper-cases
    # it, so check the substring is bound rather than equality.
    _assert_bound_not_interpolated(recording_engine, INJECTION, min_calls=3)


def test_search_ticker_only_binds_uppercased_value(recording_engine):
    # ticker branch uses ``q.upper()`` as a bound equality param.
    intel_router.intel_search(
        q=INJECTION_TICKER, type="ticker", limit=10, offset=0, _token="t"
    )
    assert recording_engine.calls, "ticker search issued no query"
    sql, params = recording_engine.calls[-1]
    assert INJECTION_TICKER.upper() not in sql
    assert params.get("q") == INJECTION_TICKER.upper()


# ── /predictions/active — where_clauses builder (classic SQLi vector) ────

def test_predictions_active_binds_ticker_and_model(recording_engine):
    intel_router.intel_predictions_active(
        ticker=INJECTION, model=INJECTION, limit=50, offset=0, _token="t"
    )
    # COUNT(*) + main SELECT = 2 calls. ticker is upper-cased before binding.
    _assert_bound_not_interpolated(recording_engine, INJECTION.upper(), min_calls=2)
    # model is bound verbatim — confirm it is present as a param, not in SQL.
    for sql, params in recording_engine.calls:
        assert INJECTION not in sql
    assert any(p.get("model") == INJECTION for _, p in recording_engine.calls)


def test_predictions_active_where_clause_uses_placeholders(recording_engine):
    """The dynamically-assembled WHERE must contain only :placeholders for the
    user-controlled predicates, never the literal values."""
    intel_router.intel_predictions_active(
        ticker="NVDA", model="oracle_v3", limit=5, offset=0, _token="t"
    )
    main_sql = recording_engine.calls[-1][0]
    assert "ticker = :ticker" in main_sql
    assert "model_name = :model" in main_sql
    # literal values must NOT be baked into the SQL
    assert "NVDA" not in main_sql
    assert "oracle_v3" not in main_sql


# ── /predictions/track-record — six assembled queries share where_sql ────

def test_track_record_binds_all_filters(recording_engine):
    intel_router.intel_predictions_track_record(
        model=INJECTION, ticker=INJECTION, timeframe="week", _token="t"
    )
    # 6 queries (overall/by_model/by_ticker/by_direction/recent/calibration).
    _assert_bound_not_interpolated(recording_engine, INJECTION.upper(), min_calls=6)
    for sql, _ in recording_engine.calls:
        assert INJECTION not in sql  # raw (non-upper) model value not spliced


def test_track_record_timeframe_is_not_interpolated(recording_engine):
    """``timeframe`` selects a Python ``timedelta`` cutoff; the cutoff date is
    bound as ``:cutoff`` and the raw timeframe token never enters the SQL."""
    intel_router.intel_predictions_track_record(
        model=None, ticker=None, timeframe="month", _token="t"
    )
    for sql, params in recording_engine.calls:
        assert "month" not in sql
        if "scored_at >= :cutoff" in sql:
            assert "cutoff" in params


# ── /entity/{name} — path param flows into LIKE binds ────────────────────

def test_entity_profile_binds_name(recording_engine):
    intel_router.intel_entity_profile(name=INJECTION, _token="t")
    _assert_bound_not_interpolated(recording_engine, INJECTION, min_calls=1)


# ── /ticker/{symbol} — lookback-window integer must not interpolate ──────

def test_ticker_intelligence_binds_symbol_and_cutoff(recording_engine):
    intel_router.intel_ticker(symbol=INJECTION_TICKER, days=30, _token="t")
    assert recording_engine.calls, "ticker dossier issued no query"
    upper = INJECTION_TICKER.upper()
    for sql, params in recording_engine.calls:
        assert INJECTION_TICKER not in sql
        assert upper not in sql
    # symbol is bound (upper-cased) as :t on at least one query.
    assert any(p.get("t") == upper for _, p in recording_engine.calls)


# ── Whole-router static guard — no f-string SQL reaches execute() ────────

def test_router_source_has_no_interpolated_sql():
    """AST guard over the whole router: no f-string, ``.format()`` or
    ``%``-format string is ever passed (directly or via a variable/concat)
    to an ``execute()`` call. This is a belt-and-suspenders check that does
    not depend on which endpoints the dynamic tests happen to cover."""
    import ast
    import inspect

    src = inspect.getsource(intel_router)
    tree = ast.parse(src)

    def taint_reasons(node) -> list[str]:
        reasons: list[str] = []
        for n in ast.walk(node):
            if isinstance(n, ast.JoinedStr):
                reasons.append("f-string")
            if (
                isinstance(n, ast.BinOp)
                and isinstance(n.op, ast.Mod)
                and isinstance(n.left, ast.Constant)
                and isinstance(n.left.value, str)
            ):
                reasons.append("%-format")
            if (
                isinstance(n, ast.Call)
                and isinstance(n.func, ast.Attribute)
                and n.func.attr == "format"
            ):
                reasons.append(".format")
        return reasons

    # Map: variable name -> taint reasons, and concat-with-name graph.
    var_taint: dict[str, set[str]] = {}
    concat_names: dict[str, set[str]] = {}

    def concat_member_names(node) -> list[str]:
        out: list[str] = []

        def walk(n):
            if isinstance(n, ast.BinOp) and isinstance(n.op, ast.Add):
                walk(n.left)
                walk(n.right)
            elif isinstance(n, ast.Name):
                out.append(n.id)

        walk(node)
        return out

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    r = taint_reasons(node.value)
                    if r:
                        var_taint.setdefault(tgt.id, set()).update(r)
                    cn = concat_member_names(node.value)
                    if cn:
                        concat_names.setdefault(tgt.id, set()).update(cn)

    findings: list[str] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "execute"
            and node.args
        ):
            target = node.args[0]
            if (
                isinstance(target, ast.Call)
                and getattr(target.func, "id", None) == "text"
                and target.args
            ):
                target = target.args[0]

            reasons = list(taint_reasons(target))
            if isinstance(target, ast.Name):
                nm = target.id
                reasons += [f"via_var:{nm}:{r}" for r in var_taint.get(nm, ())]
                seen: set[str] = set()
                stack = list(concat_names.get(nm, ()))
                while stack:
                    x = stack.pop()
                    if x in seen:
                        continue
                    seen.add(x)
                    reasons += [
                        f"via_concat:{nm}<-{x}:{r}" for r in var_taint.get(x, ())
                    ]
                    stack.extend(concat_names.get(x, ()))

            if reasons:
                findings.append(f"execute() near line {node.lineno}: {reasons}")

    assert not findings, (
        "interpolated SQL reaches execute() in api/routers/intel.py:\n  "
        + "\n  ".join(findings)
    )
