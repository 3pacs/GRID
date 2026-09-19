"""company_milestones.probability: NULL means UNSCORED, never a 0.5 prior.

``valuation/milestones.py`` declared ``Milestone.probability: float = 0.5``
and ``probability_weighted_impact`` substituted ``or 0.5`` for a missing
value. Under ``docs/reference/CONFIDENCE_POLICY.md`` (on the
confidence-policy branch, #544) a field named ``probability`` is null unless
it comes from a scored track record, and ``x or 0.5`` is forbidden outright —
it also rewrites a stated 0. ``valuation/`` sat outside that policy's scanner.

These pin:

* the dataclass defaults to ``None`` and a stated value needs a
  ``confidence_source`` basis; a stated 0 is accepted;
* ``MilestoneTracker.add`` writes NULL for an unscored milestone;
* ``probability_weighted_impact`` skips unscored milestones, counts them,
  treats a stated 0 as scored, and publishes ``scored_n`` beside the total;
* the scorecard's 0.0 execution score is a measurement, not "no data";
* the composite pipeline carries the counts and labels a stated probability
  as a stated prior with its basis — never as "confidence";
* the API create model no longer defaults to 0.5;
* the populate script keeps ``None`` as ``None`` and fails closed while the
  column is still NOT NULL;
* a literal scan over ``valuation/`` and the populate script, with an
  allowlist keyed by ``path::source-text`` and a reason per entry. Fold the
  directories into ``tests/test_confidence_policy.py``'s scan when #544 lands.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from valuation.milestones import Milestone, MilestoneTracker

REPO = Path(__file__).resolve().parents[1]


def _tracker_with_rows(rows):
    """A MilestoneTracker over a mocked engine whose SELECT returns ``rows``."""
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = rows
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    return MilestoneTracker(engine), conn


def _row(mid, probability, value_impact_ps, *, source="ANALYST", status="PENDING"):
    # id, ticker, milestone_type, announced_date, target_date, actual_date,
    # description, target_value, target_unit, actual_value, achievement_pct,
    # probability, confidence_source, value_impact_ps, value_impact_pct,
    # status, source_url, notes, created_at, updated_at
    return (
        mid, "ACME", "EARNINGS_GUIDANCE", date(2026, 1, 1), date(2026, 6, 1), None,
        f"milestone {mid}", 1.0, "EPS", None, None,
        probability, source, value_impact_ps, None,
        status, None, None, "2026-01-01", "2026-01-01",
    )


# ── dataclass ──────────────────────────────────────────────────────────


class TestMilestoneDataclass:
    def _base(self, **kw):
        return Milestone(
            ticker="ACME", milestone_type="RUMOR",
            announced_date=date(2026, 1, 1), description="x", **kw,
        )

    def test_default_is_unscored_not_half(self):
        m = self._base()
        assert m.probability is None
        assert m.confidence_source is None

    def test_stated_probability_requires_a_basis(self):
        with pytest.raises(ValueError, match="confidence_source"):
            self._base(probability=0.7)

    def test_stated_probability_with_basis_is_kept(self):
        m = self._base(probability=0.7, confidence_source="MARKET")
        assert m.probability == 0.7
        assert m.confidence_source == "MARKET"

    def test_stated_zero_is_a_measurement(self):
        m = self._base(probability=0.0, confidence_source="MANAGEMENT")
        assert m.probability == 0.0

    def test_out_of_range_still_rejected(self):
        with pytest.raises(ValueError, match="Probability must be 0-1"):
            self._base(probability=1.5, confidence_source="ANALYST")

    def test_unknown_basis_rejected(self):
        with pytest.raises(ValueError, match="Invalid confidence_source"):
            self._base(probability=0.5, confidence_source="GUESS")

    def test_basis_without_a_number_is_allowed(self):
        # A source can be known (e.g. RUMOR) while the odds are unscored.
        m = self._base(confidence_source="RUMOR")
        assert m.probability is None


# ── writer ─────────────────────────────────────────────────────────────


class TestTrackerAddWritesNull:
    def test_unscored_milestone_is_written_as_null_not_half(self):
        engine = MagicMock()
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (42,)
        engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
        engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        new_id = MilestoneTracker(engine).add(Milestone(
            ticker="acme", milestone_type="RUMOR",
            announced_date=date(2026, 1, 1), description="x",
        ))
        assert new_id == 42
        params = conn.execute.call_args.args[1]
        assert params["prob"] is None
        assert params["conf_src"] is None


# ── weighted impact ────────────────────────────────────────────────────


class TestProbabilityWeightedImpact:
    def test_unscored_milestones_are_skipped_and_counted(self):
        tracker, _ = _tracker_with_rows([
            _row(1, 0.5, 2.0),          # scored: +1.00
            _row(2, None, 10.0),        # unscored: contributes nothing
            _row(3, None, -4.0),        # unscored: contributes nothing
        ])
        out = tracker.probability_weighted_impact("ACME")
        assert out["total_impact"] == pytest.approx(1.0)
        assert out["scored_n"] == 1
        assert out["unscored_n"] == 2
        assert out["milestones_n"] == 3

    def test_no_midpoint_is_substituted(self):
        # Under the old `or 0.5`, this would have been 10 * 0.5 = 5.0.
        tracker, _ = _tracker_with_rows([_row(1, None, 10.0)])
        out = tracker.probability_weighted_impact("ACME")
        assert out["total_impact"] == 0.0
        assert out["scored_n"] == 0
        assert out["unscored_n"] == 1

    def test_stated_zero_is_scored_and_contributes_zero(self):
        # Under the old `or 0.5`, a stated 0 became 0.5.
        tracker, _ = _tracker_with_rows([_row(1, 0.0, 10.0)])
        out = tracker.probability_weighted_impact("ACME")
        assert out["total_impact"] == 0.0
        assert out["scored_n"] == 1
        assert out["unscored_n"] == 0

    def test_scored_milestone_without_impact_is_reported_separately(self):
        tracker, _ = _tracker_with_rows([_row(1, 0.8, None)])
        out = tracker.probability_weighted_impact("ACME")
        assert out["total_impact"] == 0.0
        assert out["scored_n"] == 0
        assert out["no_impact_n"] == 1

    def test_empty_pipeline(self):
        tracker, _ = _tracker_with_rows([])
        out = tracker.probability_weighted_impact("ACME")
        assert out == {
            "total_impact": 0.0, "scored_n": 0, "unscored_n": 0,
            "no_impact_n": 0, "milestones_n": 0,
        }


# ── scorecard zero casts ───────────────────────────────────────────────


class TestScorecardZeroIsAMeasurement:
    def _scorecard(self, row):
        engine = MagicMock()
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = row
        engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
        engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        return MilestoneTracker(engine).get_scorecard("ACME")

    def test_zero_execution_score_is_poor_execution_not_no_data(self):
        # total, achieved, ahead, missed_or_behind, pending, avg_pct, score
        sc = self._scorecard((4, 0, 0, 4, 0, 0.0, 0.0))
        assert sc["execution_score"] == 0.0
        assert sc["assessment"] == "POOR_EXECUTION"
        assert sc["avg_achievement_pct"] == 0.0

    def test_null_execution_score_is_still_no_completed(self):
        sc = self._scorecard((2, 0, 0, 0, 2, None, None))
        assert sc["execution_score"] is None
        assert sc["assessment"] == "NO_COMPLETED_MILESTONES"


# ── composite consumer ─────────────────────────────────────────────────


class TestCompositeCarriesCoverage:
    def _engine(self):
        from valuation.composite import CompositeValuationEngine

        eng = CompositeValuationEngine(MagicMock())
        eng.intrinsic.valuate = MagicMock(return_value=None)
        eng.derivatives.analyze = MagicMock(return_value=None)
        eng.milestones.get_scorecard = MagicMock(return_value={"total_milestones": 0})
        return eng

    def test_counts_reach_to_dict(self):
        eng = self._engine()
        eng.milestones.get_timeline = MagicMock(return_value=[])
        eng.milestones.probability_weighted_impact = MagicMock(return_value={
            "total_impact": 1.5, "scored_n": 2, "unscored_n": 3,
            "no_impact_n": 0, "milestones_n": 5,
        })
        d = eng.analyze("ACME", as_of=date(2026, 9, 18)).to_dict()
        assert d["milestone_value_adjustment"] == 1.5
        assert d["milestone_scored_n"] == 2
        assert d["milestone_unscored_n"] == 3

    def test_prompt_labels_stated_prior_and_unscored(self):
        eng = self._engine()
        eng.milestones.get_timeline = MagicMock(return_value=[
            {"date": "2026-06-01", "type": "M_AND_A", "description": "deal",
             "target_value": None, "target_unit": None, "actual_value": None,
             "achievement_pct": None, "probability": 0.65,
             "confidence_source": "MARKET", "status": "PENDING",
             "value_impact_ps": 1.0},
            {"date": "2026-07-01", "type": "RUMOR", "description": "whisper",
             "target_value": None, "target_unit": None, "actual_value": None,
             "achievement_pct": None, "probability": None,
             "confidence_source": None, "status": "PENDING",
             "value_impact_ps": None},
            {"date": "2026-08-01", "type": "BUYBACK", "description": "zero",
             "target_value": None, "target_unit": None, "actual_value": None,
             "achievement_pct": None, "probability": 0.0,
             "confidence_source": "MANAGEMENT", "status": "PENDING",
             "value_impact_ps": None},
        ])
        eng.milestones.probability_weighted_impact = MagicMock(return_value={
            "total_impact": 0.65, "scored_n": 1, "unscored_n": 1,
            "no_impact_n": 1, "milestones_n": 3,
        })
        c = eng.analyze("ACME", as_of=date(2026, 9, 18))
        prompt = eng._build_prompt(c, "analysis-id")
        assert "Stated probability: 65% (MARKET)" in prompt
        assert "[Probability: unscored]" in prompt
        # A stated 0 is printed, not hidden (the old `if m.get("probability")`).
        assert "Stated probability: 0% (MANAGEMENT)" in prompt
        assert "Confidence:" not in prompt
        assert "(scored 1, unscored 1)" in prompt


# ── API create model ───────────────────────────────────────────────────


class TestApiCreateModelDefault:
    def test_probability_defaults_to_none(self):
        import os

        os.environ.setdefault("DB_PASSWORD", "test-password")
        os.environ.setdefault("ENVIRONMENT", "development")
        os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
        from api.routers.valuation import MilestoneCreate

        body = MilestoneCreate(
            ticker="ACME", milestone_type="RUMOR",
            announced_date="2026-01-01", description="x",
        )
        assert body.probability is None
        assert body.confidence_source is None


# ── populate script ────────────────────────────────────────────────────


class TestPopulateScriptKeepsNone:
    def test_clamp_keeps_none(self):
        from scripts.populate_milestones import _clamp_probability

        assert _clamp_probability(None) is None
        assert _clamp_probability(0.0) == 0.0
        assert _clamp_probability(1.7) == 1.0

    def test_unscored_row_is_skipped_while_column_is_not_null(self, monkeypatch):
        import scripts.populate_milestones as pm

        monkeypatch.setattr(pm, "_PROBABILITY_NULLABLE", None)
        monkeypatch.setattr(pm, "_SKIPPED_UNSCORED", 0)
        conn = MagicMock()
        conn.execute.return_value.scalar.return_value = "NO"  # is_nullable
        inserted = pm._insert_milestone(
            conn, ticker="ACME", milestone_type="RUMOR",
            announced_date=date(2026, 1, 1), description="x",
        )
        assert inserted is False
        assert pm._SKIPPED_UNSCORED == 1
        # No INSERT went out.
        sql_sent = " ".join(
            str(getattr(c.args[0], "text", c.args[0])) for c in conn.execute.call_args_list
        )
        assert "INSERT INTO company_milestones" not in sql_sent

    def test_unscored_row_is_written_as_null_once_nullable(self, monkeypatch):
        import scripts.populate_milestones as pm

        monkeypatch.setattr(pm, "_PROBABILITY_NULLABLE", True)
        monkeypatch.setattr(pm, "_milestone_exists", lambda *_a, **_k: False)
        conn = MagicMock()
        inserted = pm._insert_milestone(
            conn, ticker="ACME", milestone_type="RUMOR",
            announced_date=date(2026, 1, 1), description="x",
        )
        assert inserted is True
        params = conn.execute.call_args.args[1]
        assert params["prob"] is None


# ── literal scan over valuation/ and the populate script ──────────────

_SCAN_FILES = sorted((REPO / "valuation").glob("*.py")) + [
    REPO / "scripts" / "populate_milestones.py",
]

_LITERAL = re.compile(
    r"""(?x)
      (?:^|\s)prob(?:ability)?\s*(?::\s*[\w\s|\[\],]+)?=\s*0\.\d+   # prob = 0.x
    | ["']probability["']\s*:\s*0\.\d+                            # "probability": 0.x
    | \bor\s+0\.\d+\b                                              # x or 0.x
    | ^\s*["'][a-z_]+["']\s*:\s*0\.\d+\s*,?\s*$                    # "label": 0.x  (a label->prior map)
    | \.get\([^()]*,\s*0\.\d+\)                                     # map.get(label, 0.x) midpoint default
    """
)

# path::stripped-source-line -> reason. Adding an entry is a reviewable act.
_ALLOWLIST: dict[str, str] = {
    'scripts/populate_milestones.py::"bullish": 0.65,':
        "_DIR_PROB direction prior; written with confidence_source=CALCULATED as its basis. Follow-up: rename to prior_weight.",
    'scripts/populate_milestones.py::"bearish": 0.35,':
        "_DIR_PROB direction prior; basis CALCULATED. Follow-up: rename to prior_weight.",
    'scripts/populate_milestones.py::"neutral": 0.50,':
        "_DIR_PROB direction prior; basis CALCULATED. Follow-up: rename to prior_weight.",
    'scripts/populate_milestones.py::"confirmed": 0.9,':
        "conf_map label->prior for intel signals; basis is the mapped confidence_source. Follow-up: rename to prior_weight.",
    'scripts/populate_milestones.py::"derived": 0.6,':
        "conf_map label->prior; see above.",
    'scripts/populate_milestones.py::"estimated": 0.5,':
        "conf_map label->prior; see above.",
    'scripts/populate_milestones.py::"rumored": 0.3,':
        "conf_map label->prior; see above.",
    'scripts/populate_milestones.py::"inferred": 0.4,':
        "conf_map label->prior; see above.",
    'scripts/populate_milestones.py::prob = conf_map.get(confidence_label, 0.5)':
        "label->prior map for intel signals; basis is the mapped confidence_source. Follow-up: unknown label should be unscored, not 0.5.",
    'scripts/populate_milestones.py::prob = 0.75':
        "trial-window prior (<=14d); basis CALCULATED. Follow-up: rename to prior_weight.",
    'scripts/populate_milestones.py::prob = 0.60':
        "trial-window prior (<=30d); basis CALCULATED.",
    'scripts/populate_milestones.py::prob = 0.45':
        "trial-window prior (>30d); basis CALCULATED.",
    'scripts/populate_milestones.py::overall_tone = r[3] or 0.0':
        "tone, not a probability; a NULL tone as 0.0 is a separate (C-M) finding, out of scope here.",
    'scripts/populate_milestones.py::magnitude = r[5] or 0.0':
        "magnitude, not a probability; NULL as 0.0 is a separate finding, out of scope here.",
}


def _scan():
    hits = []
    for f in _SCAN_FILES:
        rel = f.relative_to(REPO).as_posix()
        for n, line in enumerate(f.read_text(encoding="utf-8").splitlines(), 1):
            if _LITERAL.search(line):
                hits.append((rel, n, line.strip()))
    return hits


class TestValuationProbabilityLiterals:
    def test_no_unallowed_probability_literal(self):
        bad = [
            f"{rel}:{n}: {line}" for rel, n, line in _scan()
            if f"{rel}::{line}" not in _ALLOWLIST
        ]
        assert not bad, "probability literal outside the allowlist:\n" + "\n".join(bad)

    def test_allowlist_entries_are_live_and_reasoned(self):
        live = {f"{rel}::{line}" for rel, _n, line in _scan()}
        stale = sorted(k for k in _ALLOWLIST if k not in live)
        assert not stale, "stale allowlist entries (remove them):\n" + "\n".join(stale)
        unreasoned = [k for k, v in _ALLOWLIST.items() if len(v.strip()) < 20]
        assert not unreasoned

    def test_the_retired_literals_are_gone(self):
        src = (REPO / "valuation" / "milestones.py").read_text(encoding="utf-8")
        assert "probability: float = 0.5" not in src
        assert 'm.get("probability") or 0.5' not in src
        ddl = (REPO / "grid" / "scripts" / "migrations" / "add_valuation_tables.sql").read_text(encoding="utf-8")
        assert "NOT NULL DEFAULT 0.5" not in ddl
        script = (REPO / "scripts" / "populate_milestones.py").read_text(encoding="utf-8")
        assert "DOUBLE PRECISION NOT NULL DEFAULT 0.5" not in script
        # Code, not the docstring that explains what it used to do.
        assert not re.search(r"^\s*return 0\.5\b", script, re.MULTILINE)
