"""GRID W4e — isolated research vertical slice: prove the autoresearch loop
can complete meaningfully and fail visibly, end to end, in one process.

Scope (per the W4e task boundary): no production, no live LLM, no
activation. Everything here runs against fakes — a pure-Python fake PIT
store standing in for ``store/pit.py``'s real Postgres-backed one, a fake
psycopg2 cursor/connection standing in for the raw autoresearch DB writes,
and a deterministic fake Ollama client. The ONE place a real PostgreSQL
service is used is ``TestRealPostgresPITBoundary``, which is skipped locally
with an explicit reason whenever a real Postgres isn't reachable (see
``tests/conftest.py::pg_engine`` — export ``GRID_TEST_DB_URL`` to point it at
one). Nothing here ever calls a live LLM or sends a real notification.

Run everything in this file:
    DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/integration/test_research_vertical_slice.py -v

What this file drives real, unmodified code through (not reimplemented):
  - ``scripts/autoresearch.py::run_autoresearch`` — the full closed loop.
  - ``validation/backtest.py::WalkForwardBacktest.run_validation`` — the
    real walk-forward engine, fed through a real ``get_feature_matrix``-shaped
    fake PIT store (see ``FakePITStore`` below). Version note: this module
    has no ``VALIDATION_VERSION``-style constant to cite; the behavior
    documented here is validation/backtest.py exactly as it exists on this
    branch (fable/research-slice-20260918 @ HEAD, base 549d7fb0 = draft #566).
  - ``evaluation/signal_outcomes.py`` (cherry-picked from draft #562,
    commit 5889f5be, sig-eval-1) — used directly, standalone, to demonstrate
    the honest-outcome vocabulary (INELIGIBLE with an explicit reason, never
    a fabricated pass). NOT wired into ``run_autoresearch`` anywhere in this
    codebase today — this file does not claim otherwise; see the docstring
    on ``TestHonestNoDataOutcomes``.

Two load-bearing, exact findings this file's tests encode (see the handoff
doc for the full writeup — repeated here because a future edit to
validation/backtest.py should make these tests fail loudly if it changes
this):
  1. ``WalkForwardBacktest.run_validation``'s ``predict_fn`` parameter is
     accepted but never read by ``_compute_era_metrics`` — the "strategy"
     return series is always ``matrix.iloc[:, 0].pct_change()``, the exact
     same series ``_compute_baseline_metrics`` uses for "baseline". The only
     difference between them is the constant ``cost_bps`` drag subtracted
     from every day of the strategy series. Since a constant per-day drag
     cannot raise the Sharpe ratio (it lowers cumulative return while
     leaving volatility unchanged), ``_determine_verdict``'s
     ``full_metrics.sharpe <= baseline.sharpe -> FAIL`` gate is *always*
     true for any real feature matrix at any cost_bps >= 0. **A "PASS"
     verdict is structurally unreachable through this function as it
     exists on this branch, for ANY hypothesis, cheating or honest.**
     Verified empirically (see this file's ``TestTimeCorrectEvaluation``)
     and independently by hand: cost_bps=0.0 gives sharpe == baseline_sharpe
     (still FAILs on ``<=``); cost_bps=10.0 gives sharpe < baseline_sharpe.
  2. A SQL failure DURING the loop body (hypothesis_registry INSERT, the
     backtest call, model_registry INSERT) is caught locally per-iteration
     inside ``run_autoresearch``'s ``for iteration in ...`` loop and turned
     into an ``attempts[i]["error"]`` entry — it does NOT flip the run's
     TOP-LEVEL ``status`` to "failed". Only a failure while loading the
     research context (``_load_research_context`` — feature_list /
     feature_name_map / market_snapshot) produces a top-level
     ``status="failed"`` with a ``phase``/``error_category``. See
     ``TestSQLFailureVisibility`` for both cases side by side.
"""

from __future__ import annotations

import json
import math
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import scripts.autoresearch as autoresearch  # noqa: E402
from validation.backtest import WalkForwardBacktest  # noqa: E402
from evaluation import signal_outcomes as sig_out  # noqa: E402

_FIXTURE_PATH = (
    Path(__file__).resolve().parents[1] / "fixtures" / "research_slice" / "feature_window.json"
)


def _load_fixture() -> dict[str, Any]:
    with open(_FIXTURE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


FIXTURE = _load_fixture()

# Deterministic feature_id assignment, shared by every test in this file so
# the fake LLM's hypothesis JSON, the fake cursor's feature_name_map, and the
# fake PIT store's rows all agree on the same IDs.
FEATURE_NAME_TO_ID: dict[str, int] = {
    f["name"]: i + 1 for i, f in enumerate(FIXTURE["features"])
}
FEATURE_ID_TO_NAME: dict[int, str] = {v: k for k, v in FEATURE_NAME_TO_ID.items()}


def _build_series_rows() -> list[dict[str, Any]]:
    """Build a real-shaped resolved_series row set from the fixture.

    Each feature gets ``n_days`` daily observations on a smooth deterministic
    series (no randomness — reproducible across runs and across the fake and
    real-Postgres paths). One extra row is added for the mid-window
    revision: a SECOND vintage of the SAME (feature_id, obs_date) released
    ``revised_release_offset_days`` later than the original, carrying a very
    different value — exactly the shape store/pit.py's DISTINCT ON
    (feature_id, obs_date) ... ORDER BY vintage_date query is built to
    resolve (see tests/test_pit.py's own two-vintage fixture for the same
    pattern against a real Postgres).
    """
    window = FIXTURE["series_window"]
    start = date.fromisoformat(window["start_date"])
    n = window["n_days"]
    base = window["base_value"]
    drift = window["daily_drift"]
    amp = window["oscillation_amplitude"]
    period = window["oscillation_period_days"]

    rows: list[dict[str, Any]] = []
    for feat_index, feature in enumerate(FIXTURE["features"]):
        fid = FEATURE_NAME_TO_ID[feature["name"]]
        series_offset = feat_index * 5.0  # keep the two series distinguishable
        for i in range(n):
            d = start + timedelta(days=i)
            value = base + series_offset + drift * i + amp * math.sin(2 * math.pi * i / period)
            rows.append(
                {
                    "feature_id": fid,
                    "obs_date": d,
                    "release_date": d,
                    "vintage_date": d,
                    "value": round(value, 6),
                }
            )

    rev = FIXTURE["mid_window_revision"]
    rev_fid = FEATURE_NAME_TO_ID[rev["feature"]]
    obs_d = start + timedelta(days=rev["obs_date_offset_days"])
    revised_release = obs_d + timedelta(days=rev["revised_release_offset_days"])
    rows.append(
        {
            "feature_id": rev_fid,
            "obs_date": obs_d,
            "release_date": revised_release,
            "vintage_date": revised_release,
            "value": rev["revised_value"],
        }
    )
    return rows


SERIES_ROWS = _build_series_rows()
SERIES_START = date.fromisoformat(FIXTURE["series_window"]["start_date"])
SERIES_END = SERIES_START + timedelta(days=FIXTURE["series_window"]["n_days"] - 1)
REVISION_OBS_DATE = SERIES_START + timedelta(days=FIXTURE["mid_window_revision"]["obs_date_offset_days"])
REVISION_RELEASE_DATE = REVISION_OBS_DATE + timedelta(
    days=FIXTURE["mid_window_revision"]["revised_release_offset_days"]
)


# ---------------------------------------------------------------------------
# Fake PIT store(s) — pure Python, no database (item 1a)
# ---------------------------------------------------------------------------


class FakePITStore:
    """Pure-Python stand-in for store/pit.py's PITStore, real PIT semantics.

    Implements the exact contract ``WalkForwardBacktest.run_validation``
    depends on: for each (feature_id, obs_date), pick the FIRST_RELEASE
    (min vintage_date) or LATEST_AS_OF (max vintage_date) row among those
    with ``release_date <= as_of_date``, then pivot into a wide matrix. A
    row with ``release_date > as_of_date`` is never visible, no matter how
    old its ``obs_date`` is — this is the property that makes the mid-window
    revision here meaningful test data rather than decoration.
    """

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def _pick(self, feature_ids: list[int], as_of_date: date, vintage_policy: str) -> dict[tuple, dict]:
        picked: dict[tuple, dict] = {}
        for r in self.rows:
            if r["feature_id"] not in feature_ids:
                continue
            if r["obs_date"] > as_of_date or r["release_date"] > as_of_date:
                continue
            key = (r["feature_id"], r["obs_date"])
            if key not in picked:
                picked[key] = r
                continue
            if vintage_policy == "FIRST_RELEASE":
                if r["vintage_date"] < picked[key]["vintage_date"]:
                    picked[key] = r
            else:  # LATEST_AS_OF
                if r["vintage_date"] > picked[key]["vintage_date"]:
                    picked[key] = r
        return picked

    def get_feature_matrix(
        self,
        feature_ids: list[int],
        start_date: date,
        end_date: date,
        as_of_date: date,
        vintage_policy: str = "FIRST_RELEASE",
    ) -> pd.DataFrame:
        picked = self._pick(feature_ids, as_of_date, vintage_policy)
        selected = [r for r in picked.values() if start_date <= r["obs_date"] <= end_date]
        if not selected:
            return pd.DataFrame(index=pd.DatetimeIndex([], name="obs_date"))
        df = pd.DataFrame(selected)
        matrix = df.pivot_table(index="obs_date", columns="feature_id", values="value", aggfunc="first")
        matrix.index = pd.DatetimeIndex(matrix.index, name="obs_date")
        return matrix.sort_index()


class LeakyFakePITStore(FakePITStore):
    """Deliberately-broken PIT store used ONLY to demonstrate, by contrast,
    what a lookahead bug would look like: it ignores ``release_date``
    entirely and always takes the latest vintage. Never used to feed a
    passing test expectation — only to show its output DIFFERS from the
    honest store's for the same query, proving the honest store's filtering
    is actually doing something.
    """

    def _pick(self, feature_ids: list[int], as_of_date: date, vintage_policy: str) -> dict[tuple, dict]:
        picked: dict[tuple, dict] = {}
        for r in self.rows:
            if r["feature_id"] not in feature_ids or r["obs_date"] > as_of_date:
                continue
            key = (r["feature_id"], r["obs_date"])
            if key not in picked or r["vintage_date"] > picked[key]["vintage_date"]:
                picked[key] = r
        return picked


# ---------------------------------------------------------------------------
# Fake psycopg2 cursor/connection — the raw autoresearch DB writes
# ---------------------------------------------------------------------------


class _FakeAutoresearchDB:
    """In-memory stand-in for the slice of Postgres run_autoresearch() and
    WalkForwardBacktest touch directly via raw SQL: hypothesis_registry and
    model_registry (validation_results is read-only here, always empty).
    Shared across multiple run_autoresearch() calls with the same run_id to
    prove idempotent retry behaviour.
    """

    def __init__(self) -> None:
        self.hypotheses: dict[int, dict[str, Any]] = {}
        self.models: dict[int, int] = {}
        self.next_hyp_id = 1
        self.next_model_id = 1
        self.hyp_insert_count = 0
        self.model_insert_count = 0
        self.update_calls: list[tuple] = []


class _FakeCursor:
    def __init__(self, db: _FakeAutoresearchDB, raise_map: dict[str, Exception] | None = None):
        self.db = db
        self.raise_map = raise_map or {}
        self._result: Any = None

    def execute(self, sql: str, params: Any = None) -> None:
        s = " ".join(sql.split())
        for prefix, exc in self.raise_map.items():
            if s.startswith(prefix):
                raise exc

        if s.startswith("SELECT id, state FROM hypothesis_registry"):
            statement, layer = params
            match = None
            for hyp_id, row in self.db.hypotheses.items():
                if row["statement"] == statement and row["layer"] == layer:
                    match = (hyp_id, row["state"])
            self._result = match
        elif s.startswith("INSERT INTO hypothesis_registry"):
            self.db.hyp_insert_count += 1
            statement, layer = params[0], params[1]
            hyp_id = self.db.next_hyp_id
            self.db.next_hyp_id += 1
            self.db.hypotheses[hyp_id] = {"statement": statement, "layer": layer, "state": "TESTING"}
            self._result = (hyp_id,)
        elif s.startswith("UPDATE hypothesis_registry SET state='FAILED', kill_reason=%s WHERE id=%s"):
            kill_reason, hyp_id = params
            self.db.hypotheses[hyp_id]["state"] = "FAILED"
            self.db.update_calls.append(("backtest_exception", hyp_id, kill_reason))
            self._result = None
        elif s.startswith("UPDATE hypothesis_registry SET state=%s"):
            state, kill_reason, hyp_id = params
            self.db.hypotheses[hyp_id]["state"] = state
            self.db.update_calls.append(("normal", hyp_id, state, kill_reason))
            self._result = None
        elif s.startswith("SELECT id FROM model_registry WHERE hypothesis_id"):
            (hyp_id,) = params
            self._result = (self.db.models[hyp_id],) if hyp_id in self.db.models else None
        elif s.startswith("SELECT id FROM validation_results"):
            self._result = None
        elif s.startswith("INSERT INTO model_registry"):
            self.db.model_insert_count += 1
            hyp_id = params[3]
            model_id = self.db.next_model_id
            self.db.next_model_id += 1
            self.db.models[hyp_id] = model_id
            self._result = (model_id,)
        else:
            self._result = None

    def fetchone(self):
        return self._result

    def fetchall(self):
        return []

    def close(self):
        pass


class _FakeConnection:
    autocommit = False

    def __init__(self, db: _FakeAutoresearchDB, raise_map: dict[str, Exception] | None = None):
        self.db = db
        self.raise_map = raise_map
        self.closed = False

    def cursor(self):
        return _FakeCursor(self.db, self.raise_map)

    def close(self):
        self.closed = True


# ---------------------------------------------------------------------------
# Fake Ollama client — deterministic, well-formed OR malformed (item 2)
# ---------------------------------------------------------------------------

_WELL_FORMED_HYPOTHESIS_TEXT = f"""\
```json
{{
  "statement": "When {FEATURE_ID_TO_NAME[1]} diverges from {FEATURE_ID_TO_NAME[2]}, mean reversion follows within 5 days",
  "feature_ids": [1, 2],
  "lag_structure": {{"1": 0, "2": 5}},
  "layer": "REGIME",
  "proposed_metric": "sharpe",
  "proposed_threshold": 0.5
}}
```
"""

_MALFORMED_HYPOTHESIS_TEXT = "I could not compute a well-formed hypothesis this cycle, sorry."


class _DeterministicOllama:
    """Fake LLM client — is_available toggle + a fixed chat() response."""

    def __init__(self, response_text: str | None, is_available: bool = True):
        self.is_available = is_available
        self._response_text = response_text
        self.chat_calls = 0

    def chat(self, *args: Any, **kwargs: Any):
        self.chat_calls += 1
        return self._response_text


# ---------------------------------------------------------------------------
# Recording AnalyticalSnapshotStore stand-in (mirrors test_autoresearch_runstate.py)
# ---------------------------------------------------------------------------


class _RecordingSnapshotStore:
    events: list[dict[str, Any]] = []

    def __init__(self, db_engine: Any = None) -> None:
        self.db_engine = db_engine

    def save_snapshot(self, category, payload, as_of_date=None, subcategory=None, metrics=None, actor_name=None):
        _RecordingSnapshotStore.events.append(
            {"category": category, "subcategory": subcategory, "payload": dict(payload)}
        )
        return len(_RecordingSnapshotStore.events)


# ---------------------------------------------------------------------------
# Shared wiring fixture
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _wire_common(monkeypatch):
    """Wire every test in this file to fakes only — no real DB, no real LLM.

    Feature-list / market-snapshot / name-map helpers are stubbed (as the
    existing runstate tests do) but their content is DERIVED from the same
    fixture the fake PIT store and fake LLM hypothesis use, so the run's
    ``inputs`` manifest and the hypothesis's feature_ids are mutually
    consistent — a real-shaped feature set, not an empty stand-in.
    """
    _RecordingSnapshotStore.events = []
    import store.snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "AnalyticalSnapshotStore", _RecordingSnapshotStore)
    monkeypatch.setattr(autoresearch, "_ortho_cache", None)
    monkeypatch.setattr(autoresearch, "get_engine", lambda: object())
    monkeypatch.setattr(autoresearch, "OllamaReasoner", lambda ollama: object())
    monkeypatch.setattr(autoresearch, "_select_orthogonal_features", lambda cur, **kw: [1, 2])
    monkeypatch.setattr(autoresearch, "get_feature_name_map", lambda cur: dict(FEATURE_ID_TO_NAME))
    monkeypatch.setattr(
        autoresearch,
        "get_feature_list",
        lambda cur: "\n".join(f"  ID={fid}  {name}" for fid, name in FEATURE_ID_TO_NAME.items()),
    )
    monkeypatch.setattr(
        autoresearch,
        "get_market_snapshot",
        lambda cur: "\n".join(f"  {name}: latest" for name in FEATURE_ID_TO_NAME.values()),
    )
    yield


def _wire_backtester(monkeypatch, backtester) -> None:
    monkeypatch.setattr(autoresearch, "PITStore", lambda engine: object())
    monkeypatch.setattr(autoresearch, "WalkForwardBacktest", lambda engine, pit: backtester)


def _wire_db(monkeypatch, db: _FakeAutoresearchDB, raise_map: dict[str, Exception] | None = None) -> None:
    import psycopg2

    monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: _FakeConnection(db, raise_map))


def _wire_ollama(monkeypatch, client) -> None:
    monkeypatch.setattr(autoresearch, "get_ollama", lambda: client)


# ===========================================================================
# Item 3 (main path) + item 1a + item 2 (well-formed case)
# ===========================================================================


class TestHonestLoopEndToEnd:
    """The full closed loop, real WalkForwardBacktest, real fake-PIT data,
    real hypothesis dedup — no stubbed verdict. Per this file's module
    docstring finding #1, ``run_validation`` structurally cannot return PASS
    for ANY hypothesis on this branch, so this test's honest expectation is
    verdict=FAIL — status="ok" still means "the run completed without being
    aborted or erroring," not "a hypothesis passed."
    """

    def test_started_running_ok_sequence_iterations_and_hypothesis_dedup(self, monkeypatch):
        db = _FakeAutoresearchDB()
        _wire_db(monkeypatch, db)
        _wire_backtester(monkeypatch, WalkForwardBacktest(None, FakePITStore(SERIES_ROWS)))
        _wire_ollama(monkeypatch, _DeterministicOllama(_WELL_FORMED_HYPOTHESIS_TEXT))
        notify_calls: list[Any] = []
        import scripts.notify as notify_module

        monkeypatch.setattr(notify_module, "notify_on_pass", lambda attempt: notify_calls.append(attempt))

        result = autoresearch.run_autoresearch(
            max_iterations=1,
            run_id="slice-happy-1",
            backtest_start=SERIES_START,
            backtest_end=SERIES_END,
            n_splits=1,
            cost_bps=0.0,
        )

        # ── Run-record sequence (started -> running(context) -> running(iter) -> ok) ──
        statuses = [e["payload"]["status"] for e in _RecordingSnapshotStore.events]
        phases = [e["payload"]["phase"] for e in _RecordingSnapshotStore.events]
        assert statuses == ["started", "running", "running", "ok"]
        assert phases == ["init", "context_loaded", "iteration", "complete"]

        ctx_event = _RecordingSnapshotStore.events[1]
        assert ctx_event["payload"]["inputs"] == {
            "feature_ids_count": 2,
            "market_snapshot_keys": ["rs_slice_macro_a", "rs_slice_macro_b"],
            "evaluation_version": None,
        }

        # ── Loop outcome ──
        assert result["status"] == "ok"
        assert result["iterations"] == 1 == result["iterations_run"]
        attempt = result["all_attempts"][0]
        assert attempt["verdict"] == "FAIL"  # see class docstring — never PASS on this branch
        assert attempt["statement"].startswith("When rs_slice_macro_a")

        # ── Hypothesis written exactly once ──
        assert db.hyp_insert_count == 1
        assert notify_calls == []  # no notify on FAIL

        # ── Idempotent retry: same run_id, same statement (deterministic
        # fake LLM) -> reused, no duplicate INSERT, no duplicate backtest ──
        result_2 = autoresearch.run_autoresearch(
            max_iterations=1,
            run_id="slice-happy-1",
            backtest_start=SERIES_START,
            backtest_end=SERIES_END,
            n_splits=1,
            cost_bps=0.0,
        )
        assert result_2["status"] == "ok"
        assert db.hyp_insert_count == 1  # unchanged
        assert result_2["all_attempts"][0]["reused"] is True
        assert result_2["all_attempts"][0]["verdict"] == "FAIL"
        assert notify_calls == []


# ===========================================================================
# Item 2 (malformed case)
# ===========================================================================


class TestMalformedLLMOutput:
    def test_malformed_hypothesis_json_recorded_as_parse_failure_not_crash(self, monkeypatch):
        db = _FakeAutoresearchDB()
        _wire_db(monkeypatch, db)
        _wire_backtester(monkeypatch, WalkForwardBacktest(None, FakePITStore(SERIES_ROWS)))
        _wire_ollama(monkeypatch, _DeterministicOllama(_MALFORMED_HYPOTHESIS_TEXT))

        # Must not raise.
        result = autoresearch.run_autoresearch(
            max_iterations=1,
            run_id="slice-malformed-1",
            backtest_start=SERIES_START,
            backtest_end=SERIES_END,
            n_splits=1,
        )

        assert result["status"] == "ok"  # the RUN did not crash or fail top-level
        assert result["iterations"] == 1  # one attempt was made
        assert result["passed"] is False
        attempt = result["all_attempts"][0]
        assert attempt["error"] == "JSON parse failed"
        assert db.hyp_insert_count == 0  # never reached hypothesis_registry at all

        # EXACT FINDING: run_autoresearch has no per-attempt "skip_reasons"
        # concept — that field only ever holds fence events (see
        # _generation_current usage). A parse failure is visible ONLY as
        # all_attempts[i]["error"]; the terminal run-record's own
        # skip_reasons list stays empty even though this run made zero
        # usable hypotheses. Documented here, not silently assumed.
        end_event = _RecordingSnapshotStore.events[-1]
        assert end_event["payload"]["skip_reasons"] == []
        assert end_event["payload"]["status"] == "ok"


# ===========================================================================
# Item 3 (time-correct evaluation) + documented structural finding
# ===========================================================================


class TestTimeCorrectEvaluation:
    def test_honest_pit_store_excludes_future_revision_leaky_one_does_not(self):
        """Direct proof that the honest fake (and by extension store/pit.py's
        real DISTINCT ON query it mirrors) never lets the mid-window
        revision leak into a matrix built with an as_of_date before the
        revision was released — while the deliberately-broken leaky store
        does. This is the actual lookahead-safety property
        WalkForwardBacktest depends on via PITStore.get_feature_matrix.
        """
        honest = FakePITStore(SERIES_ROWS)
        leaky = LeakyFakePITStore(SERIES_ROWS)

        as_of_before_release = REVISION_RELEASE_DATE - timedelta(days=1)

        honest_matrix = honest.get_feature_matrix(
            [1], SERIES_START, SERIES_END, as_of_date=as_of_before_release, vintage_policy="FIRST_RELEASE"
        )
        leaky_matrix = leaky.get_feature_matrix(
            [1], SERIES_START, SERIES_END, as_of_date=as_of_before_release, vintage_policy="FIRST_RELEASE"
        )

        honest_value = honest_matrix.loc[pd.Timestamp(REVISION_OBS_DATE), 1]
        leaky_value = leaky_matrix.loc[pd.Timestamp(REVISION_OBS_DATE), 1]

        revised_value = FIXTURE["mid_window_revision"]["revised_value"]
        assert honest_value != revised_value  # honest: revision not yet released, invisible
        assert leaky_value == revised_value  # leaky: ignores release_date, "sees the future"

    def test_real_walk_forward_backtest_never_returns_pass_on_this_branch(self):
        """EXACT FINDING #1 (see module docstring): ``predict_fn`` is dead —
        ``_compute_era_metrics`` always scores the same column
        ``_compute_baseline_metrics`` uses, so a per-day constant cost drag
        is the ONLY difference between "strategy" and "baseline", and that
        can never raise the Sharpe ratio. This is why item 3's "a cheating
        hypothesis must NOT pass" is trivially, uninterestingly true here:
        NOTHING passes, cheating or honest. Verified for both cost_bps=0
        (equal Sharpe, still FAILs on the `<=` gate) and cost_bps=10 (this
        branch's real default, strictly lower Sharpe).
        """
        bt = WalkForwardBacktest(None, FakePITStore(SERIES_ROWS))

        for cost_bps, expect_equal in ((0.0, True), (10.0, False)):
            result = bt.run_validation(
                hypothesis_id=1,
                feature_ids=[1],
                start_date=SERIES_START,
                end_date=SERIES_END,
                n_splits=1,
                cost_bps=cost_bps,
            )
            assert result["overall_verdict"] == "FAIL"
            sharpe = result["full_period_metrics"]["sharpe"]
            baseline_sharpe = result["baseline_comparison"]["sharpe"]
            if expect_equal:
                assert sharpe == pytest.approx(baseline_sharpe)
            else:
                assert sharpe < baseline_sharpe


# ===========================================================================
# Item 4 (honest results: no-data + SQL failure)
# ===========================================================================


class TestHonestNoDataOutcomes:
    def test_empty_feature_window_yields_insufficient_data_not_fabricated_sharpe(self, monkeypatch):
        """A backtest window entirely outside the fixture's data range must
        come back as an explicit "no data" result — never a fabricated
        positive Sharpe, and never reported as PASS.
        """
        no_data = FIXTURE["no_data_window"]
        start = date.fromisoformat(no_data["start_date"])
        end = date.fromisoformat(no_data["end_date"])

        bt = WalkForwardBacktest(None, FakePITStore(SERIES_ROWS))
        result = bt.run_validation(
            hypothesis_id=1, feature_ids=[1], start_date=start, end_date=end, n_splits=1, cost_bps=0.0
        )

        assert result["era_results"][0]["status"] == "INSUFFICIENT_DATA"
        assert result["full_period_metrics"] == {"return": 0.0, "sharpe": 0.0, "max_drawdown": 0.0}
        assert result["overall_verdict"] == "FAIL"  # no valid eras -> FAIL, never PASS

        # Same "no data, never a crash" property through the full loop:
        db = _FakeAutoresearchDB()
        _wire_db(monkeypatch, db)
        _wire_backtester(monkeypatch, bt)
        _wire_ollama(monkeypatch, _DeterministicOllama(_WELL_FORMED_HYPOTHESIS_TEXT))

        loop_result = autoresearch.run_autoresearch(
            max_iterations=1, run_id="slice-nodata-1", backtest_start=start, backtest_end=end, n_splits=1,
        )
        assert loop_result["status"] == "ok"
        assert loop_result["all_attempts"][0]["verdict"] == "FAIL"
        assert loop_result["all_attempts"][0]["sharpe"] == 0.0

    def test_signal_outcomes_cohort_summary_reports_ineligible_not_fabricated(self):
        """evaluation/signal_outcomes.py (cherry-picked, commit 5889f5be) is
        NOT wired into run_autoresearch anywhere in this codebase — this
        test exercises it directly, standalone, to demonstrate the "honest
        no-data" outcome vocabulary this vertical slice makes available:
        a signal with no entry price is INELIGIBLE with an explicit reason,
        never silently dropped and never scored as a fabricated CORRECT/pass.
        """
        no_price_accessor: sig_out.PriceAccessor = lambda instrument, as_of: None

        def _priced_accessor(instrument: str, as_of: date) -> sig_out.PricePoint | None:
            # A trivial deterministic price so the other two records resolve.
            return sig_out.PricePoint(price=100.0 + as_of.day, bar_date=as_of, basis="close")

        records = [
            sig_out.SignalRecord(
                source_type="test", instrument="NO_PRICE_TICKER", signal_date=date(2024, 1, 5),
                direction="BUY", horizon_days=5, origin_tag="synthetic",
            ),
            sig_out.SignalRecord(
                source_type="test", instrument="PRICED_A", signal_date=date(2024, 1, 5),
                direction="BUY", horizon_days=5, origin_tag="synthetic",
            ),
            sig_out.SignalRecord(
                source_type="test", instrument="PRICED_B", signal_date=date(2024, 1, 5),
                direction="SELL", horizon_days=5, origin_tag="synthetic",
            ),
        ]

        outcomes = []
        for i, rec in enumerate(records):
            accessor = no_price_accessor if i == 0 else _priced_accessor
            outcomes.append(
                sig_out.evaluate_signal(rec, accessor, today=date(2024, 1, 20))
            )

        assert outcomes[0].outcome == "INELIGIBLE"
        assert outcomes[0].eligibility_reason == sig_out.REASON_MISSING_ENTRY
        assert outcomes[0].raw_return is None  # no fabricated return

        summary = sig_out.summarize_outcomes(outcomes)
        assert summary.n_total == 3
        assert summary.n_ineligible == 1
        assert summary.n_ineligible_by_reason == {sig_out.REASON_MISSING_ENTRY: 1}
        assert summary.n_eligible == 2


class TestSQLFailureVisibility:
    def test_context_load_sql_failure_yields_top_level_failed_status(self, monkeypatch):
        """Failure in _load_research_context -> top-level status=failed,
        with phase + error_category, and iterations/iterations_run both 0
        (never silently reported as a zero-hypothesis "success")."""
        original = RuntimeError('column "f.does_not_exist" does not exist (simulated)')

        def _raise(cur):
            raise original

        monkeypatch.setattr(autoresearch, "get_market_snapshot", _raise)
        db = _FakeAutoresearchDB()
        _wire_db(monkeypatch, db)
        _wire_backtester(monkeypatch, WalkForwardBacktest(None, FakePITStore(SERIES_ROWS)))
        _wire_ollama(monkeypatch, _DeterministicOllama(_WELL_FORMED_HYPOTHESIS_TEXT))

        result = autoresearch.run_autoresearch(max_iterations=1, run_id="slice-sqlfail-context")

        assert result["status"] == "failed"
        assert result["phase"] == "market_snapshot"
        assert "does_not_exist" in result["error"]
        assert result["iterations"] == 0
        assert result["iterations_run"] == 0
        assert result["passed"] is False

        end_event = _RecordingSnapshotStore.events[-1]
        assert end_event["payload"]["status"] == "failed"
        assert end_event["payload"]["error_category"] == "db_load_failure"
        assert end_event["payload"]["iterations"] == 0

    def test_mid_loop_sql_failure_does_not_flip_top_level_status(self, monkeypatch):
        """EXACT FINDING #2 (see module docstring): a SQL failure INSIDE the
        loop body (here: the hypothesis_registry INSERT) is caught locally
        and recorded as attempts[i]["error"] — the run's top-level status
        stays "ok", not "failed". This is the precise mechanism the task's
        item 4 wording ("a case where the SQL fails ... yields status
        failed") does NOT hold for uniformly — it is true only for the
        context-loading phase (see the test above), not for a failure
        anywhere in the loop body. Documented here rather than papered over.
        """
        db = _FakeAutoresearchDB()
        insert_failure = RuntimeError("simulated: duplicate key value violates unique constraint")
        _wire_db(monkeypatch, db, raise_map={"INSERT INTO hypothesis_registry": insert_failure})
        _wire_backtester(monkeypatch, WalkForwardBacktest(None, FakePITStore(SERIES_ROWS)))
        _wire_ollama(monkeypatch, _DeterministicOllama(_WELL_FORMED_HYPOTHESIS_TEXT))

        result = autoresearch.run_autoresearch(max_iterations=1, run_id="slice-sqlfail-midloop")

        # Not "failed" at the top level -- exactly the documented boundary.
        assert result["status"] == "ok"
        assert "phase" not in result  # _failed_result's shape never appears
        assert result["iterations"] == 1  # one attempt was made (and it errored)
        assert result["passed"] is False
        attempt = result["all_attempts"][0]
        assert "DB insert failed" in attempt["error"]
        assert db.hyp_insert_count == 0  # the raise happened before any row landed

        end_event = _RecordingSnapshotStore.events[-1]
        assert end_event["payload"]["status"] == "ok"  # confirms the boundary at the run-record layer too


# ===========================================================================
# Item 5 (generation fencing mid-run)
# ===========================================================================


class TestGenerationFencingMidRun:
    def test_generation_advanced_mid_run_fences_before_further_writes(self, monkeypatch):
        """Simulates the operator moving on (e.g. after _run_with_timeout
        gives up on a slow worker) partway through a multi-iteration run:
        iteration 1 completes normally (generation still current at both of
        its checks), then the generation is superseded before iteration 2 —
        which must be fenced before any further hypothesis/model write, and
        the run must end "abandoned", not "ok".
        """
        db = _FakeAutoresearchDB()
        # First hypothesis fails distinctly from a possible second one so
        # a second INSERT would be visibly a new row, not a dedup hit.
        ollama = _DeterministicOllama(_WELL_FORMED_HYPOTHESIS_TEXT)
        _wire_db(monkeypatch, db)
        _wire_backtester(monkeypatch, WalkForwardBacktest(None, FakePITStore(SERIES_ROWS)))
        _wire_ollama(monkeypatch, ollama)

        # is_current_generation is checked at: top of iter 1, before iter 1's
        # backtest. Both must still be True for iteration 1 to complete. It
        # is checked again at the top of iteration 2 -- that check (the 3rd
        # call) is where the operator has moved on.
        call_count = {"n": 0}

        def _is_current(_gen: int) -> bool:
            call_count["n"] += 1
            return call_count["n"] <= 2

        result = autoresearch.run_autoresearch(
            max_iterations=3,
            run_id="slice-fence-midrun",
            generation=5,
            is_current_generation=_is_current,
        )

        assert result["status"] == "abandoned"
        assert result["fenced"] is True
        assert db.hyp_insert_count == 1  # iteration 1's write happened; iteration 2's never did

        attempts = result["all_attempts"]
        assert attempts[0]["verdict"] == "FAIL"  # iteration 1 completed for real
        assert attempts[1]["fenced"] is True
        assert "generation superseded" in attempts[1]["error"]

        end_event = _RecordingSnapshotStore.events[-1]
        assert end_event["payload"]["status"] == "abandoned"
        assert end_event["payload"]["skip_reasons"] == ["fenced_before_iteration_2"]


# ===========================================================================
# Item 6 (mock notification)
# ===========================================================================


class _AlwaysPassBacktester:
    """Deliberate stub, not the real WalkForwardBacktest.

    Per this file's documented structural finding, the real
    ``run_validation`` cannot return PASS for any input on this branch. To
    still test the NOTIFICATION mechanism's exactly-once/zero-duplicate
    contract (item 6), this stub returns a canned PASS result the same
    shape ``run_validation`` produces. It is used ONLY in this class.
    """

    def run_validation(self, **kwargs):
        return {
            "overall_verdict": "PASS",
            "full_period_metrics": {"sharpe": 1.2, "return": 0.1, "max_drawdown": 0.05},
            "baseline_comparison": {"sharpe": 0.1},
            "era_results": [],
        }


class TestNotificationHook:
    def test_notify_called_once_on_genuine_pass_zero_on_retry_zero_on_fail(self, monkeypatch):
        notify_calls: list[Any] = []
        import scripts.notify as notify_module

        monkeypatch.setattr(notify_module, "notify_on_pass", lambda attempt: notify_calls.append(attempt))

        # ── FAIL case first: real backtester, zero notifications ──
        db_fail = _FakeAutoresearchDB()
        _wire_db(monkeypatch, db_fail)
        _wire_backtester(monkeypatch, WalkForwardBacktest(None, FakePITStore(SERIES_ROWS)))
        _wire_ollama(monkeypatch, _DeterministicOllama(_WELL_FORMED_HYPOTHESIS_TEXT))
        fail_result = autoresearch.run_autoresearch(max_iterations=1, run_id="slice-notify-fail")
        assert fail_result["passed"] is False
        assert notify_calls == []

        # ── PASS case: stub backtester (see _AlwaysPassBacktester docstring) ──
        db_pass = _FakeAutoresearchDB()
        _wire_db(monkeypatch, db_pass)
        _wire_backtester(monkeypatch, _AlwaysPassBacktester())
        _wire_ollama(monkeypatch, _DeterministicOllama(_WELL_FORMED_HYPOTHESIS_TEXT))

        pass_result = autoresearch.run_autoresearch(max_iterations=1, run_id="slice-notify-pass")
        assert pass_result["passed"] is True
        assert len(notify_calls) == 1
        assert db_pass.hyp_insert_count == 1
        assert db_pass.model_insert_count == 1

        # ── Retry with the SAME run_id: hypothesis already PASSED ->
        # reused, no duplicate model, NO second notification ──
        pass_result_2 = autoresearch.run_autoresearch(max_iterations=1, run_id="slice-notify-pass")
        assert pass_result_2["status"] == "ok"
        assert db_pass.hyp_insert_count == 1
        assert db_pass.model_insert_count == 1
        assert len(notify_calls) == 1  # still just the one


# ===========================================================================
# Item 7 (provider routing)
# ===========================================================================


class TestProviderRouting:
    def test_provider_identity_follows_effective_config_ollama_fallback(self, monkeypatch):
        """With no OpenAI key and llama.cpp disabled, get_client() must fall
        through to OllamaClient constructed from the config values it
        actually reads (settings.OLLAMA_*) -- asserted from the fake
        client's captured constructor kwargs, not from source-reading.
        """
        import ollama.client as ollama_client_module
        from config import settings

        monkeypatch.setattr(settings, "OPENAI_API_KEY", "")
        monkeypatch.setattr(settings, "AGENTS_OPENAI_API_KEY", "")
        monkeypatch.setattr(ollama_client_module.os, "environ", {**ollama_client_module.os.environ, "OPENAI_API_KEY": ""})
        monkeypatch.setattr(settings, "LLAMACPP_ENABLED", False)
        monkeypatch.setattr(ollama_client_module, "_client_instance", None)

        captured: dict[str, Any] = {}

        class _FakeOllamaClient:
            def __init__(self, base_url, model, embed_model, timeout):
                captured.update(base_url=base_url, model=model, embed_model=embed_model, timeout=timeout)
                self.is_available = True

        monkeypatch.setattr(ollama_client_module, "OllamaClient", _FakeOllamaClient)

        client = ollama_client_module.get_client()

        assert isinstance(client, _FakeOllamaClient)
        assert captured == {
            "base_url": settings.OLLAMA_BASE_URL,
            "model": settings.OLLAMA_CHAT_MODEL,
            "embed_model": settings.OLLAMA_EMBED_MODEL,
            "timeout": settings.OLLAMA_TIMEOUT_SECONDS,
        }

        monkeypatch.setattr(ollama_client_module, "_client_instance", None)

    def test_disabled_ollama_produces_visible_failed_status_not_silent_skip(self, monkeypatch):
        db = _FakeAutoresearchDB()
        _wire_db(monkeypatch, db)
        _wire_backtester(monkeypatch, WalkForwardBacktest(None, FakePITStore(SERIES_ROWS)))
        _wire_ollama(monkeypatch, _DeterministicOllama(None, is_available=False))

        result = autoresearch.run_autoresearch(max_iterations=1, run_id="slice-ollama-down")

        # A visible, structured failure -- not a quiet zero-iteration "ok".
        assert result["status"] == "failed"
        assert result["phase"] == "ollama_availability"
        assert result["error"] == "Ollama not available"
        assert result["iterations"] == 0
        assert result["iterations_run"] == 0
        assert db.hyp_insert_count == 0

        end_event = _RecordingSnapshotStore.events[-1]
        assert end_event["payload"]["status"] == "failed"
        assert end_event["payload"]["error_category"] == "ollama_unavailable"


# ===========================================================================
# Item 1b — the CI PostgreSQL service (skipped locally with an explicit reason)
# ===========================================================================


def _required_tables_present(conn) -> list[str]:
    from sqlalchemy import text

    required = ("feature_registry", "resolved_series", "source_catalog")
    missing = []
    for t in required:
        row = conn.execute(
            text("SELECT to_regclass(:name) IS NOT NULL"), {"name": t}
        ).fetchone()
        if not row or not row[0]:
            missing.append(t)
    return missing


@pytest.mark.integration
class TestRealPostgresPITBoundary:
    """The ONE test in this file that touches a real database. Gated on the
    shared ``pg_engine`` fixture (tests/conftest.py) exactly like every
    other DB-gated test in this suite: it SKIPS (never fakes a pass) when
    PostgreSQL is unreachable, and also skips with an explicit reason if the
    schema hasn't been bootstrapped (schema.sql) — that boundary is checked
    here rather than assumed, unlike the older test_pit.py pattern this
    mirrors. Set ``GRID_TEST_DB_URL`` to point at a real, disposable
    Postgres to exercise it; see this file's module docstring.
    """

    def test_pit_correct_vintage_filtering_against_real_postgres(self, pg_engine):
        from sqlalchemy import text
        from store.pit import PITStore

        with pg_engine.connect() as conn:
            missing = _required_tables_present(conn)
        if missing:
            pytest.skip(
                "GRID_TEST_DB_URL points at a Postgres without the GRID schema "
                f"applied (missing tables: {missing}). Apply schema.sql to this "
                "database first -- see docs/handoffs/2026-09-18/"
                "fable-w4e-vertical-slice.md for the CI command this test runs "
                "under. Not a fake pass: skipping explicitly."
            )

        source_name = "rs_slice_vertical_test_source"
        feature_name = "rs_slice_vertical_test_feature"

        with pg_engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO source_catalog "
                    "(name, base_url, cost_tier, latency_class, revision_behavior, trust_score, priority_rank) "
                    "VALUES (:name, 'http://example.invalid', 'FREE', 'EOD', 'RARE', 'HIGH', 999) "
                    "ON CONFLICT (name) DO NOTHING"
                ),
                {"name": source_name},
            )
            source_id = conn.execute(
                text("SELECT id FROM source_catalog WHERE name = :name"), {"name": source_name}
            ).fetchone()[0]

            conn.execute(
                text(
                    "INSERT INTO feature_registry "
                    "(name, family, description, transformation, normalization, "
                    " missing_data_policy, eligible_from_date, model_eligible) "
                    "VALUES (:name, 'macro', 'W4e vertical slice fixture', 'raw', 'RAW', "
                    " 'FORWARD_FILL', '1990-01-01', TRUE) "
                    "ON CONFLICT (name) DO NOTHING"
                ),
                {"name": feature_name},
            )
            fid = conn.execute(
                text("SELECT id FROM feature_registry WHERE name = :name"), {"name": feature_name}
            ).fetchone()[0]

            conn.execute(text("DELETE FROM resolved_series WHERE feature_id = :fid"), {"fid": fid})

            obs_d = date(2024, 3, 10)
            original_release = obs_d
            revised_release = obs_d + timedelta(days=10)

            conn.execute(
                text(
                    "INSERT INTO resolved_series "
                    "(feature_id, obs_date, release_date, vintage_date, value, source_priority_used) "
                    "VALUES (:fid, :obs, :orig_rel, :orig_rel, 100.0, :sid)"
                ),
                {"fid": fid, "obs": obs_d, "orig_rel": original_release, "sid": source_id},
            )
            conn.execute(
                text(
                    "INSERT INTO resolved_series "
                    "(feature_id, obs_date, release_date, vintage_date, value, source_priority_used) "
                    "VALUES (:fid, :obs, :rev_rel, :rev_rel, 999.0, :sid)"
                ),
                {"fid": fid, "obs": obs_d, "rev_rel": revised_release, "sid": source_id},
            )

        try:
            pit = PITStore(pg_engine)

            before_release = pit.get_pit([fid], as_of_date=revised_release - timedelta(days=1))
            assert len(before_release) == 1
            assert float(before_release.iloc[0]["value"]) == 100.0

            after_release = pit.get_pit([fid], as_of_date=revised_release)
            assert len(after_release) == 1
            assert float(after_release.iloc[0]["value"]) == 999.0
        finally:
            with pg_engine.begin() as conn:
                conn.execute(text("DELETE FROM resolved_series WHERE feature_id = :fid"), {"fid": fid})
                conn.execute(text("DELETE FROM feature_registry WHERE id = :fid"), {"fid": fid})
                conn.execute(text("DELETE FROM source_catalog WHERE id = :sid"), {"sid": source_id})
