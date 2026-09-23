"""Guard for the `confidence` policy (docs/reference/CONFIDENCE_POLICY.md).

A field named ``confidence`` or ``probability`` is ``null`` unless it comes
from a scored track record. Any surviving heuristic is renamed
(``heuristic_confidence``, ``heuristic_score``, ``source_class``, ...) and
ships its inputs.

Two kinds of test live here:

1. A **source scan** over the production tree that fails on a literal
   assignment to ``confidence``/``probability``, with an explicit allowlist.
   Every allowlist entry carries a reason; entries are either a genuinely
   scored number, an honest explicit zero, prose, or a site another in-flight
   remediation batch owns.
2. **Behavioural** tests for the surfaces Batch 6 changed: the value is null
   when nothing was measured, it moves when the measured input moves, and
   nothing downstream treats null as 0 or 0.5.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# The production tree. `tests/` and `ingestion/` are deliberately excluded:
# test fixtures legitimately pin a confidence, and ingestion is a separate
# batch. `astrogrid_api/` is included because it mirrors `api/routers/`.
SCAN_ROOTS = (
    "api",
    "intelligence",
    "analysis",
    "oracle",
    "knowledge",
    "store",
    "trading",
    "astrogrid_api",
)

LITERAL_PATTERNS = (
    re.compile(r"confidence\s*=\s*0\.\d+"),
    re.compile(r'"confidence"\s*:\s*0\.\d+'),
    re.compile(r"probability\s*=\s*0\.\d+"),
    re.compile(r'"probability"\s*:\s*0\.\d+'),
)

# ---------------------------------------------------------------------------
# Allowlist: "<path>::<stripped source line>" -> reason.
#
# Keyed by the line's text, not its number, so an unrelated edit above the
# line (another batch adding code to the same file) cannot silently turn a
# reviewed exemption into a false failure or a blanket one.
#
# A line may be allowlisted only because it is one of:
#   (scored)   the number comes from a scored track record;
#   (zero)     an explicit, honest "no confidence" / uncalibrated sentinel --
#              0.0 is the absence of confidence, not a midpoint stand-in;
#   (prose)    a docstring, comment or usage example, not a live assignment;
#   (batch)    a C4/C1 site another in-flight remediation batch owns. These
#              are defects; they are not Batch 6's files and are listed here
#              so this guard can land without silently blessing them.
# ---------------------------------------------------------------------------
ALLOWLIST: dict[str, str] = {
    # -- (zero) explicit "we have not measured this" sentinels ---------------
    "api/routers/regime.py::confidence=0.0,": (
        "zero: the UNCALIBRATED branch, returned when decision_journal has no "
        "row at all. 0.0 with state='UNCALIBRATED' is an explicit absence, "
        "not a midpoint."
    ),
    "api/routers/regime.py::transition_probability=0.0,": (
        "zero: transition_probability on the same UNCALIBRATED branch."
    ),
    "intelligence/historical_scenario_library.py::mean_confidence=0.0,": (
        "zero: mean_confidence of an empty match set."
    ),
    "intelligence/historical_scenario_library.py::mean_confidence=0.0,": (
        "zero: mean_confidence of an empty match set."
    ),
    "intelligence/prediction_market_arbitrage.py::safe_confidence = 0.0": (
        "zero: unparseable oracle_confidence is floored to 0.0 and the "
        "multiplier is then held at 1.00 with an advisory explaining why."
    ),
    "intelligence/prediction_market_arbitrage.py::safe_confidence = 0.0": (
        "zero: the NaN branch of the same guard."
    ),
    "oracle/engine.py::confidence=0.0,": (
        "zero: no-signal branch emits confidence 0.0, not a midpoint."
    ),
    "oracle/engine.py::confidence=0.0,": (
        "zero: no-signal branch emits confidence 0.0, not a midpoint."
    ),
    "oracle/hallucination_guard.py::original_confidence=0.0,": (
        "zero: original_confidence on the nothing-to-adjust branch."
    ),
    "oracle/hallucination_guard.py::adjusted_confidence=0.0,": (
        "zero: adjusted_confidence on the nothing-to-adjust branch."
    ),
    "oracle/signal_aggregator.py::direction=\"neutral\", strength=0.0, confidence=0.0, coherence=0.0,": (
        "zero: the empty-aggregate result (direction='neutral', strength=0.0, "
        "confidence=0.0, coherence=0.0) for an empty signal list."
    ),
    # -- (prose) not a live assignment ---------------------------------------
    "intelligence/confidence_calibration.py::trades pinned at exactly ``confidence = 0.950`` (and another 65 at": (
        "prose: module docstring describing the defect this module exists to "
        "detect (trades pinned at exactly 0.950)."
    ),
    "intelligence/self_learning_loop.py::output={\"classification\": \"bullish\", \"probability\": 0.72},": (
        "prose: module docstring usage example."
    ),
    "oracle/risk.py::confidence=0.82,": (
        "prose: docstring usage example for check_recommendation()."
    ),
    "intelligence/adapters/earnings_adapter.py::z_score=None, confidence=0.6,": (
        "batch: registered-signal adapters emit a per-adapter prior; the "
        "signal-registry batch owns the adapter confidence contract."
    ),
    "intelligence/causation_graph.py::confidence = 0.1": (
        "batch: causal-chain batch. The geometric-mean branch above it is "
        "computed; only the empty-`probs` fallback is a literal."
    ),
    "intelligence/causation_graph.py::probability=0.95,": (
        "batch: causal-chain batch. A hand-set link probability."
    ),
    "intelligence/causation_graph.py::probability=0.9,": (
        "batch: causal-chain batch. A hand-set link probability."
    ),
    "intelligence/news_impact.py::confidence=0.6,": (
        "batch: catalyst/attribution batch owns per-catalyst confidence."
    ),
    "intelligence/obsidian_agent.py::\"confidence\": 0.5,": (
        "batch: writes an oracle_predictions row with confidence 0.5 for a "
        "vault_alpha placeholder; the oracle-writers batch owns it. Note "
        "oracle/calibration.py now excludes null-confidence rows, so this "
        "row still scores until that batch lands."
    ),
    "intelligence/postmortem.py::\"confidence\": 0.5,": (
        "batch: post-mortem LLM fallback defaults; the post-mortem batch "
        "owns them."
    ),
    "intelligence/sec_filing_extractor.py::confidence=0.35,  # Lower confidence for keyword-only match": (
        "batch: filing-extraction batch. A keyword-only match prior."
    ),
    "intelligence/trend_tracker.py::confidence=0.4,": (
        "batch: trend-tracker batch owns the per-trend confidence priors."
    ),
    "intelligence/trend_tracker.py::confidence=0.6,": (
        "batch: trend-tracker batch owns the per-trend confidence priors."
    ),
    "intelligence/trend_tracker.py::confidence=0.55,": (
        "batch: trend-tracker batch owns the per-trend confidence priors."
    ),
    "intelligence/trend_tracker.py::confidence=0.5,": (
        "batch: trend-tracker batch owns the per-trend confidence priors."
    ),
    "oracle/claim_extractor.py::confidence=0.9,": (
        "batch: publishing-firewall internals. A per-pattern extraction "
        "prior, never emitted as an API `confidence`. /chat/ask's "
        "verified_claim_ratio is built from claim_count and flagged_count "
        "(verdict-based), not from these numbers."
    ),
    "oracle/claim_extractor.py::confidence=0.85,": "batch: publishing-firewall internals.",
    "oracle/claim_extractor.py::confidence=0.7,": "batch: publishing-firewall internals.",
    "oracle/claim_extractor.py::confidence=0.8,": "batch: publishing-firewall internals.",
    "oracle/claim_verifier.py::claim=claim, verdict=\"supported\", confidence=0.9,": "batch: publishing-firewall internals.",
    "oracle/claim_verifier.py::claim=claim, verdict=\"contradicted\", confidence=0.9,": "batch: publishing-firewall internals.",
    "oracle/claim_verifier.py::claim=claim, verdict=\"supported\", confidence=0.85,": "batch: publishing-firewall internals.",
    "oracle/claim_verifier.py::claim=claim, verdict=\"contradicted\", confidence=0.85,": "batch: publishing-firewall internals.",
    "oracle/claim_verifier.py::claim=claim, verdict=\"supported\", confidence=0.8,": "batch: publishing-firewall internals.",
    "oracle/claim_verifier.py::claim=claim, verdict=\"contradicted\", confidence=0.8,": "batch: publishing-firewall internals.",
    "oracle/claim_verifier.py::claim=claim, verdict=\"ambiguous\", confidence=0.3,": "batch: publishing-firewall internals.",
}

_ALLOWED_PREFIXES = ("scored:", "zero:", "prose:", "batch:")


def _iter_python_files():
    for root in SCAN_ROOTS:
        base = REPO_ROOT / root
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*.py")):
            parts = set(path.parts)
            if "__pycache__" in parts or "node_modules" in parts:
                continue
            yield path


def _scan() -> list[tuple[str, int, str]]:
    hits: list[tuple[str, int, str]] = []
    for path in _iter_python_files():
        rel = path.relative_to(REPO_ROOT).as_posix()
        text = path.read_text(encoding="utf-8", errors="replace")
        for lineno, line in enumerate(text.splitlines(), start=1):
            if any(p.search(line) for p in LITERAL_PATTERNS):
                hits.append((rel, lineno, line.strip()))
    return hits


class TestSourceScan:
    def test_no_unallowlisted_confidence_literal(self) -> None:
        """No new hand-tuned confidence/probability literal in production."""
        offenders = [
            f"{rel}:{lineno}  {line}"
            for rel, lineno, line in _scan()
            if f"{rel}::{line}" not in ALLOWLIST
        ]
        assert not offenders, (
            "Literal confidence/probability assignment outside the allowlist.\n"
            "See docs/reference/CONFIDENCE_POLICY.md: a field named "
            "`confidence` or `probability` is null unless it comes from a "
            "scored track record; a surviving heuristic is renamed and ships "
            "its inputs. Do not replace the literal with another literal.\n\n"
            + "\n".join(offenders)
        )

    def test_allowlist_has_no_stale_entries(self) -> None:
        """An allowlisted line that no longer matches must be removed.

        Without this the allowlist rots into a blanket exemption whose line
        numbers point at unrelated code.
        """
        live = {f"{rel}::{line}" for rel, _, line in _scan()}
        stale = sorted(set(ALLOWLIST) - live)
        assert not stale, (
            "Allowlist entries that no longer match a literal (fixed, moved, "
            "or the line numbers drifted) — remove or re-point them:\n"
            + "\n".join(stale)
        )

    def test_every_allowlist_entry_states_a_reason(self) -> None:
        bad = sorted(
            key
            for key, reason in ALLOWLIST.items()
            if not reason.strip().startswith(_ALLOWED_PREFIXES)
            or len(reason.strip()) < 30
        )
        assert not bad, (
            "Each allowlist entry must start with one of "
            f"{_ALLOWED_PREFIXES} and say why in a full sentence: "
            + ", ".join(bad)
        )

    def test_scan_actually_covers_the_tree(self) -> None:
        """Guard the guard: a broken path glob must not pass vacuously."""
        files = list(_iter_python_files())
        assert len(files) > 200, f"scan only saw {len(files)} files"
        roots_seen = {f.relative_to(REPO_ROOT).parts[0] for f in files}
        assert set(SCAN_ROOTS) <= roots_seen

    def test_the_pattern_would_catch_the_retired_literals(self) -> None:
        """The regexes must match the exact forms this batch removed."""
        for sample in (
            "                confidence = 0.75 if context_text else 0.5",
            '                        "confidence": 0.7,',
            "        confidence = 0.72",
            "        confidence = 0.45",
            "                probability=0.95,",
            '    "probability": 0.72',
        ):
            assert any(p.search(sample) for p in LITERAL_PATTERNS), sample


# ---------------------------------------------------------------------------
# Behavioural tests for the surfaces this batch changed.
# ---------------------------------------------------------------------------


class TestChatAsk:
    """POST /api/v1/chat/ask no longer carries a `confidence` key."""

    def test_response_model_has_no_confidence_field(self) -> None:
        from api.routers.chat import ChatAskResponse

        assert "confidence" not in ChatAskResponse.model_fields
        for field in (
            "verified_claim_ratio",
            "claim_count",
            "flagged_count",
            "firewall_decision",
        ):
            assert field in ChatAskResponse.model_fields

    def test_measurement_fields_default_to_null(self) -> None:
        from api.routers.chat import ChatAskResponse

        resp = ChatAskResponse(
            answer="x", sources_used=[], generated_at="2026-09-17T00:00:00Z"
        )
        assert resp.verified_claim_ratio is None
        assert resp.claim_count is None
        assert resp.flagged_count is None

    def test_ratio_is_null_when_nothing_was_measured(self) -> None:
        from api.routers.chat import _verified_claim_ratio

        # Firewall did not run.
        assert _verified_claim_ratio(None, None) is None
        # Firewall ran but extracted no checkable claim: nothing measured.
        assert _verified_claim_ratio(0, 0) is None
        # Half a measurement is not a measurement.
        assert _verified_claim_ratio(4, None) is None

    def test_ratio_moves_with_the_measured_input(self) -> None:
        """Context held constant, the value tracks the firewall's counts.

        This is the property the old `0.75 if context_text else 0.5` lacked:
        it reported the same number for an answer whose every claim was
        flagged as for one whose claims all verified.
        """
        from api.routers.chat import _verified_claim_ratio

        all_good = _verified_claim_ratio(8, 0)
        half_bad = _verified_claim_ratio(8, 4)
        all_bad = _verified_claim_ratio(8, 8)
        assert all_good == 1.0
        assert half_bad == 0.5
        assert all_bad == 0.0
        assert all_good > half_bad > all_bad

    def test_a_rejected_answer_cannot_report_a_clean_ratio(self) -> None:
        from api.routers.chat import _verified_claim_ratio

        # D-H6: the old field reported 0.75 even when the firewall replaced
        # the answer. Flagged claims now necessarily lower the ratio.
        assert _verified_claim_ratio(3, 3) == 0.0


class TestCanvasScores:
    def test_measured_or_none_keeps_zero_and_drops_null(self) -> None:
        from api.routers.canvas import _measured_or_none

        assert _measured_or_none(None) is None
        assert _measured_or_none(0) == 0.0  # the falsy test used to lose this
        assert _measured_or_none(0.8) == 0.8  # 0-1 column, not rescaled
        assert _measured_or_none("nope") is None
        assert _measured_or_none(float("nan")) is None

    def test_unmapped_signal_label_is_unknown_not_a_midpoint(self) -> None:
        from api.routers.canvas import _signal_confidence

        assert _signal_confidence("confirmed") == (1.0, "confirmed")
        assert _signal_confidence("DERIVED") == (0.8, "derived")
        # D-M13: the old code returned 0.5 for an unmapped label and
        # relabelled a NULL as "estimated", weaker than the schema default.
        assert _signal_confidence("something_new") == (None, "unknown")
        assert _signal_confidence(None) == (None, "unknown")

    def test_a_scored_actor_outranks_an_unscored_one_under_limit(self) -> None:
        """D-H1: the placeholder used to decide which nodes survived."""
        from api.routers.canvas import _limit_canvas_nodes

        nodes = [
            {"id": "a:unscored", "type": "actor", "influence": None},
            {"id": "a:scored", "type": "actor", "influence": 0.2},
        ]
        kept = _limit_canvas_nodes(nodes, 1)
        assert [n["id"] for n in kept] == ["a:scored"]

    def test_a_low_but_real_score_still_beats_unscored(self) -> None:
        from api.routers.canvas import _limit_canvas_nodes

        nodes = [
            {"id": "a:unscored1", "type": "actor", "influence": None},
            {"id": "a:unscored2", "type": "actor", "influence": None},
            {"id": "a:barely", "type": "actor", "influence": 0.01},
        ]
        kept = _limit_canvas_nodes(nodes, 1)
        assert [n["id"] for n in kept] == ["a:barely"]

    def test_center_nodes_still_win_over_everything(self) -> None:
        from api.routers.canvas import _limit_canvas_nodes

        nodes = [
            {"id": "a:scored", "type": "actor", "influence": 0.9},
            {"id": "a:center", "type": "actor", "influence": None,
             "is_center": True},
        ]
        kept = _limit_canvas_nodes(nodes, 1)
        assert [n["id"] for n in kept] == ["a:center"]


class TestCanvasExpandScores:
    def test_unrated_lever_puller_passes_null_through(self) -> None:
        from api.routers.canvas_expand import _measured_or_none

        assert _measured_or_none(None) is None
        assert _measured_or_none(0) == 0.0
        assert _measured_or_none(0.42) == 0.42


class TestKnowledgeHeuristic:
    def test_renamed_and_ships_its_inputs(self) -> None:
        from knowledge.tree import answer_heuristic_score

        result = answer_heuristic_score("The CPI print was 3.1% in 2024-05.")
        assert set(result) == {"score", "basis", "inputs"}
        assert set(result["inputs"]) == {
            "word_count",
            "hedge_phrase_count",
            "specific_token_count",
        }
        assert result["basis"]

    def test_empty_answer_has_no_score(self) -> None:
        from knowledge.tree import answer_heuristic_score

        assert answer_heuristic_score("")["score"] is None

    def test_inputs_reproduce_the_score(self) -> None:
        """The published inputs must be the ones the number came from."""
        from knowledge.tree import answer_heuristic_score

        answer = " ".join(["word"] * 250) + " $10,000 and 4.2% in 2024-05"
        result = answer_heuristic_score(answer)
        i = result["inputs"]
        expected = 0.5
        if i["word_count"] > 200:
            expected += 0.15
        expected -= i["hedge_phrase_count"] * 0.1
        expected += min(i["specific_token_count"] * 0.05, 0.2)
        assert result["score"] == pytest.approx(
            max(0.0, min(1.0, round(expected, 2)))
        )

    def test_read_path_strips_the_legacy_column(self) -> None:
        from knowledge.tree import _with_answer_heuristic

        row = {"id": 1, "answer": "A 4.2% print.", "confidence": 0.85}
        out = _with_answer_heuristic(row)
        assert "confidence" not in out
        assert out["answer_heuristic_score"] is not None
        assert out["answer_heuristic_inputs"]["specific_token_count"] == 1

    def test_read_path_handles_a_row_with_no_answer(self) -> None:
        from knowledge.tree import _with_answer_heuristic

        out = _with_answer_heuristic({"id": 2, "answer": None})
        assert out["answer_heuristic_score"] is None


class TestOraclePublish:
    def test_missing_metrics_become_null_not_a_midpoint(self) -> None:
        from oracle.publish import _measured_or_none

        assert _measured_or_none(None) is None
        # D-H11: `or 0.5` rewrote a legitimate zero.
        assert _measured_or_none(0.0) == 0.0
        assert _measured_or_none(0.61) == 0.61

    def test_the_three_metrics_read_three_different_keys(self) -> None:
        """They were `payload.get("confidence") or 0.5` three times over."""
        import inspect

        from oracle import publish

        src = inspect.getsource(publish.publish_astrogrid_prediction)
        assert '"signal_strength": _measured_or_none(payload.get("signal_strength"))' in src
        assert '"coherence": _measured_or_none(payload.get("coherence"))' in src
        assert 'payload.get("confidence") or 0.5' not in src

    def test_calibration_excludes_null_confidence_rows(self) -> None:
        """calibration.n must count only predictions that stated one."""
        import inspect

        from oracle import calibration

        src = inspect.getsource(calibration.compute_calibration)
        assert "confidence IS NOT NULL" in src

    def test_calibration_counts_only_non_null(self) -> None:
        """total_predictions comes from the rows the query returned, and the
        query cannot return a null-confidence row."""
        import sqlite3

        from oracle.calibration import compute_calibration

        class _Conn:
            def __init__(self, raw): self._raw = raw
            def execute(self, stmt, params):
                sql = str(stmt).replace(":model", "?").replace(":ticker", "?")
                # sqlite has no `id NOT LIKE 'astrogrid:%'` quirk to worry about
                return self._raw.execute(sql, list(params.values()))
            def __enter__(self): return self
            def __exit__(self, *a): return False

        class _Engine:
            def __init__(self, raw): self._raw = raw
            def connect(self): return _Conn(self._raw)

        raw = sqlite3.connect(":memory:")
        raw.execute(
            "CREATE TABLE oracle_predictions ("
            "id TEXT, confidence REAL, verdict TEXT, dedup_keep INTEGER, "
            "model_name TEXT, ticker TEXT)"
        )
        raw.executemany(
            "INSERT INTO oracle_predictions VALUES (?,?,?,?,?,?)",
            [
                ("p1", 0.8, "hit", 1, "m", "SPY"),
                ("p2", 0.4, "miss", 1, "m", "SPY"),
                ("p3", None, "hit", 1, "m", "SPY"),
                ("p4", None, "miss", 1, "m", "SPY"),
            ],
        )
        report = compute_calibration(_Engine(raw), n_bins=4)
        assert report.total_predictions == 2, (
            "the two null-confidence rows must not be scored for reliability"
        )


class TestAstrogridSeerAndScorecard:
    def test_seer_emits_no_confidence_literal(self) -> None:
        import inspect

        from api.routers import astrogrid_helpers as h

        src = inspect.getsource(h)
        for literal in ("confidence = 0.72", "confidence = 0.69",
                        "confidence = 0.6\n"):
            assert literal not in src, literal

    def test_seer_confidence_is_null_with_its_inputs(self) -> None:
        from api.routers.astrogrid_helpers import _build_snapshot_seer

        out = _build_snapshot_seer(
            signals={
                "planetaryStress": 9,
                "softAspectCount": 0,
                "retrogradeCount": 2,
                "solarGeomagneticKp": 6,
                "voidOfCourse": True,
                "marketRegimeBias": None,
            },
            lunar={"illumination": 10, "phase_name": "waxing"},
            nakshatra={"nakshatra_name": "Ashwini"},
            events=[],
        )
        assert out["confidence"] is None
        assert out["confidence_band"] is None
        assert out["confidence_basis"]
        # The counts the reading was actually built from.
        assert out["bucket"] == "pressure_dominant"
        assert out["pressure_score"] == 13
        assert out["release_score"] == 0

    def test_a_different_sky_changes_the_bucket_not_a_constant(self) -> None:
        from api.routers.astrogrid_helpers import _build_snapshot_seer

        calm = _build_snapshot_seer(
            signals={
                "planetaryStress": 0,
                "softAspectCount": 5,
                "retrogradeCount": 0,
                "solarGeomagneticKp": None,
                "voidOfCourse": False,
                "marketRegimeBias": None,
            },
            lunar={"illumination": 80, "phase_name": "full"},
            nakshatra={},
            events=[],
        )
        assert calm["bucket"] == "release_dominant"
        assert calm["release_score"] == 6
        assert calm["confidence"] is None

    def test_scorecard_score_is_renamed_and_carries_its_basis(self) -> None:
        import inspect

        from api.routers import astrogrid_helpers as h

        assert not hasattr(h, "_scorecard_confidence")
        assert hasattr(h, "_coverage_score")
        src = inspect.getsource(h._build_scorecard_item) if hasattr(
            h, "_build_scorecard_item"
        ) else inspect.getsource(h)
        assert '"coverage_score": _coverage_score(' in src
        assert '"confidence": _scorecard_confidence(' not in src

    def test_coverage_score_reacts_to_each_of_its_three_inputs(self) -> None:
        from datetime import date, timedelta

        from api.routers.astrogrid_helpers import _coverage_score

        today = date.today()
        base = _coverage_score(6, today, False)
        # history_points (below the 21-point cap where the term saturates)
        assert _coverage_score(18, today, False) > base
        assert _coverage_score(6, today, True) > base        # has_live_price
        assert _coverage_score(                              # latest_date
            6, today - timedelta(days=10), False
        ) < base

    def test_astrogrid_api_mirror_matches(self) -> None:
        import inspect

        from astrogrid_api import astrogrid_helpers as mirror

        src = inspect.getsource(mirror)
        assert "confidence = 0.72" not in src
        assert "_coverage_score" in src
        assert "_scorecard_confidence" not in src

    def test_unstated_prediction_confidence_is_null(self) -> None:
        """D-M2: a request with no seer block was a 50% prediction of record."""
        from api.routers.astrogrid_helpers import _prediction_confidence

        class _Req:
            def __init__(self, seer): self.seer = seer

        assert _prediction_confidence(_Req(None)) is None
        assert _prediction_confidence(_Req({})) is None
        assert _prediction_confidence(_Req({"confidence": "junk"})) is None
        assert _prediction_confidence(_Req({"confidence": 0.0})) == 0.0
        assert _prediction_confidence(_Req({"confidence": 0.9})) == 0.9
        assert _prediction_confidence(_Req({"confidence": 5})) == 1.0


class TestSourceClassLabels:
    @pytest.mark.xfail(
        reason=(
            "packet2a extraction: commit b1d3dd8b (intel.py: confidence "
            "labels become source_class naming the branch) was NOT "
            "cherry-picked. It imports `actor_source`/`source_as_of` from "
            "`intelligence.actors.provenance`, a module added by an earlier "
            "commit (e37bdf18) that lives in the #544 lower stack "
            "(#539-#542), which is out of scope for this extraction. See "
            "docs/handoffs/2026-09-21/fable-packet2a-extraction.md."
        ),
        strict=True,
    )
    def test_intel_has_no_per_branch_confidence_literal(self) -> None:
        path = REPO_ROOT / "api" / "routers" / "intel.py"
        src = path.read_text(encoding="utf-8")
        assert src.count('"confidence": "confirmed"') == 0
        assert not re.search(r'"confidence(_label)?"\s*:\s*"', src), (
            "every confidence label in intel.py must be read from the row's "
            "provenance column or renamed source_class"
        )

    @pytest.mark.xfail(
        reason=(
            "packet2a extraction: commit b1d3dd8b was NOT cherry-picked "
            "(depends on the #544 lower stack's intelligence.actors."
            "provenance module). See "
            "docs/handoffs/2026-09-21/fable-packet2a-extraction.md."
        ),
        strict=True,
    )
    def test_intel_label_is_never_a_numeric_threshold(self) -> None:
        path = REPO_ROOT / "api" / "routers" / "intel.py"
        src = path.read_text(encoding="utf-8")
        assert 'confidence", 0) > 0.7' not in src
        assert not re.search(
            r'"confidence_label"\s*:\s*"[^"]*"\s+if\b', src
        )

    def test_canvas_edges_carry_no_confidence(self) -> None:
        path = REPO_ROOT / "api" / "routers" / "canvas.py"
        src = path.read_text(encoding="utf-8")
        assert '"confidence": 0.7' not in src
        assert not re.search(r'"(confidence|probability)"\s*:', src), (
            "no canvas response field may be named confidence/probability"
        )

    def test_canvas_dots_ships_counts_not_an_affine_score(self) -> None:
        path = REPO_ROOT / "api" / "routers" / "canvas.py"
        src = path.read_text(encoding="utf-8")
        assert not re.search(r"min\(0\.9, 0\.3 \+ cnt", src)
        assert not re.search(r"min\(0\.9[5]?, 0\.4 \+ ", src)
        assert '"inputs": {"insider_transaction_count"' in src

    def test_canvas_does_not_rescale_the_0_1_score_columns(self) -> None:
        path = REPO_ROOT / "api" / "routers" / "canvas.py"
        src = path.read_text(encoding="utf-8")
        assert "or 50) / 100" not in src

    def test_knowledge_heuristic_formula_only_under_the_new_name(self) -> None:
        import inspect

        from knowledge import tree

        assert not hasattr(tree, "_estimate_confidence")
        assert "specifics * 0.05" in inspect.getsource(
            tree.answer_heuristic_score
        )
        # ... and nowhere else in the module.
        src = (REPO_ROOT / "knowledge" / "tree.py").read_text(encoding="utf-8")
        assert src.count("specifics * 0.05") == 1


class TestChatPanelRenderer:
    """The PWA must not render a percentage for an absent confidence."""

    def test_chat_panel_no_longer_formats_a_confidence_percentage(self) -> None:
        path = REPO_ROOT / "pwa" / "src" / "components" / "ChatPanel.jsx"
        src = path.read_text(encoding="utf-8")
        assert "msg.confidence" not in src
        assert "confidence: {" not in src
        assert "claimCheck" in src

    def test_gotham_canvas_no_longer_renders_percent_conf(self) -> None:
        path = REPO_ROOT / "pwa" / "src" / "canvas" / "GothamCanvas.jsx"
        src = path.read_text(encoding="utf-8")
        assert "% conf" not in src
        assert "dot.confidence" not in src

    def test_canvas_store_does_not_substitute_a_midpoint(self) -> None:
        path = REPO_ROOT / "pwa" / "src" / "canvas" / "CanvasStore.js"
        src = path.read_text(encoding="utf-8")
        assert "node.confidence || 0.5" not in src
        assert "data.influence_score || 0.3" not in src
