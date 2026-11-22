"""normalization/eligibility_audit.py (dry-run only) and the retired fix_model_eligible script (#1301).

The SQL semantics (point-in-time fencing, retractions, the separate contamination
section) are proven on real PostgreSQL in tests/test_eligibility_audit_pg.py.
"""

from __future__ import annotations

import dataclasses
import inspect
import math
from datetime import date
from unittest.mock import MagicMock

import pytest

from normalization import eligibility_audit as EA
from normalization.eligibility_audit import (
    FeatureHealthStats,
    evaluate_feature_eligibility,
    infer_cadence,
)
from scripts.fix_model_eligible import fix_model_eligible

AS_OF = date(2026, 11, 21)


def _healthy(**overrides) -> FeatureHealthStats:
    base = {
        "feature_id": 11, "name": "vix_spot", "family": "vol",
        "current_eligible": False, "deprecated": False,
        "n_obs": 500, "last_obs": date(2026, 11, 20),
        "n_distinct": 450, "value_std": 4.2, "cadence": "daily",
    }
    return FeatureHealthStats(**(base | overrides))


def test_eligibility_audit_rules():
    # Healthy daily VIX passes
    ok = evaluate_feature_eligibility(_healthy(), as_of=AS_OF)
    assert ok.eligible is True
    assert ok.changed is True

    # Ephemeris rejected even with ample history
    astro = evaluate_feature_eligibility(
        _healthy(feature_id=27734, name="ephemeris_lunar_age_days", family="alternative",
                 current_eligible=True, n_obs=1000, last_obs=AS_OF, n_distinct=999, value_std=8.5),
        as_of=AS_OF,
    )
    assert astro.eligible is False
    assert astro.reasons == ("excluded_non_economic_feature",)

    # Constant placeholder feed (like ais_ground_truth) rejected
    const = evaluate_feature_eligibility(
        _healthy(feature_id=999, name="ais_port_la_util", family="alternative",
                 current_eligible=True, n_obs=300, last_obs=AS_OF, n_distinct=1, value_std=0.0),
        as_of=AS_OF,
    )
    assert const.eligible is False
    assert const.reasons == ("zero_variance_placeholder",)

    stale = evaluate_feature_eligibility(_healthy(last_obs=date(2026, 10, 1)), as_of=AS_OF)
    assert stale.reasons == ("stale(51d>15d)",)

    assert infer_cadence("macro", "cpi_yoy") == "monthly"
    assert infer_cadence("macro", "initial_claims") == "weekly"


def test_health_decision_has_no_future_date_input():
    """Contamination is its own report: health stats carry no max_obs, the rule takes only stats + as_of."""
    assert "max_obs" not in {f.name for f in dataclasses.fields(FeatureHealthStats)}
    assert list(inspect.signature(evaluate_feature_eligibility).parameters) == ["stats", "as_of"]


@pytest.mark.parametrize(
    ("value_std", "n_distinct", "n_non_finite", "reason_prefix"),
    [
        (math.nan, 450, 0, "non_finite_variance"),
        (math.inf, 450, 0, "non_finite_variance"),
        (-math.inf, 450, 0, "non_finite_variance"),
        (None, 450, 0, "non_finite_variance"),
        (4.2, 450, 1, "non_finite_variance"),     # one NaN/inf/out-of-range value at as_of
        (None, 449, 1, "non_finite_variance"),
        (0.0, 450, 0, "zero_variance_placeholder"),
        (-1.0, 450, 0, "zero_variance_placeholder"),
        (4.2, 1, 0, "zero_variance_placeholder"),
    ],
)
def test_variance_must_be_finite_and_positive(value_std, n_distinct, n_non_finite, reason_prefix):
    stats = _healthy(value_std=value_std, n_distinct=n_distinct, n_non_finite=n_non_finite)
    assert EA.has_finite_positive_variance(stats) is False
    decision = evaluate_feature_eligibility(stats, as_of=AS_OF)
    assert decision.eligible is False
    assert len(decision.reasons) == 1 and decision.reasons[0].startswith(reason_prefix), decision.reasons


def test_eligible_implies_finite_positive_variance():
    for std in (1e-12, 4.2, 1e90):
        stats = _healthy(value_std=std)
        assert evaluate_feature_eligibility(stats, as_of=AS_OF).eligible is True
        assert EA.has_finite_positive_variance(stats) is True


def test_retired_fix_model_eligible_refuses_to_run():
    with pytest.raises(RuntimeError, match="retired"):
        fix_model_eligible()


def _mock_engine(rows):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.dialect.name = "postgresql"
    conn.execute.return_value.fetchall.return_value = rows
    return engine, conn


def test_the_audit_never_writes(monkeypatch):
    """Dry-run only: no apply parameter, no write statement, a read-only transaction."""
    for fn in (EA.audit_feature_eligibility, EA.audit_future_dated_rows, EA.read_feature_health):
        assert "apply" not in inspect.signature(fn).parameters
    source = inspect.getsource(EA).upper()
    for verb in ("UPDATE ", "INSERT ", "DELETE ", "TRUNCATE ", "ALTER "):
        assert verb not in source, verb
    monkeypatch.setattr(EA, "retractions_table_exists", lambda engine: True)

    engine, conn = _mock_engine([
        (11, "vix_spot", "vol", False, False, 500, date(2026, 11, 20), 450, 0, 4.2),
    ])
    report = EA.audit_feature_eligibility(engine, as_of=AS_OF)
    statements = [str(c[0][0]) for c in conn.execute.call_args_list]
    assert statements[0] == "SET TRANSACTION READ ONLY" and len(statements) == 2
    params = conn.execute.call_args_list[1][0][1]
    assert params["as_of_date"] == AS_OF
    assert params["retraction_cutoff"] == EA.retraction_cutoff(AS_OF)
    engine.begin.assert_not_called()
    assert report["applied"] is False and report["to_enable"] == [{"id": 11, "name": "vix_spot"}]
    assert report["section"] == "point_in_time_health"
    assert "features" not in report

    engine, conn = _mock_engine([
        (7, "weather_tmax_fcst", True, 3, 3, 3, 3, 0, date(2026, 11, 22), date(2026, 11, 24)),
    ])
    contamination = EA.audit_future_dated_rows(engine, as_of=AS_OF)
    assert str(conn.execute.call_args_list[0][0][0]) == "SET TRANSACTION READ ONLY"
    engine.begin.assert_not_called()
    assert contamination["applied"] is False
    assert contamination["section"] == "current_store_contamination"
    assert contamination["features"][0]["max_obs_date"] == "2026-11-24"
    assert "to_disable" not in contamination and "to_enable" not in contamination


def test_the_audit_refuses_without_retraction_knowledge(monkeypatch):
    monkeypatch.setattr(EA, "retractions_table_exists", lambda engine: False)
    engine, _ = _mock_engine([])
    for fn in (EA.audit_feature_eligibility, EA.audit_future_dated_rows):
        with pytest.raises(RuntimeError, match="retraction"):
            fn(engine, as_of=AS_OF)
    engine.connect.assert_not_called()
