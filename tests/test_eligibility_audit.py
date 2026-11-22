"""normalization/eligibility_audit.py (dry-run only) and the retired fix_model_eligible script (#1301)."""

from __future__ import annotations

import inspect
from datetime import date
from unittest.mock import MagicMock

import pytest

from normalization import eligibility_audit as EA
from normalization.eligibility_audit import FeatureHealthStats, evaluate_feature_eligibility, infer_cadence
from scripts.fix_model_eligible import fix_model_eligible


def test_eligibility_audit_rules():
    as_of = date(2026, 11, 21)

    # Healthy daily VIX passes
    ok = evaluate_feature_eligibility(
        FeatureHealthStats(
            feature_id=11, name="vix_spot", family="vol",
            current_eligible=False, deprecated=False,
            n_obs=500, last_obs=date(2026, 11, 20), max_obs=date(2026, 11, 20),
            n_distinct=450, value_std=4.2, cadence="daily",
        ),
        as_of=as_of,
    )
    assert ok.eligible is True
    assert ok.changed is True

    # Ephemeris rejected even with ample history
    astro = evaluate_feature_eligibility(
        FeatureHealthStats(
            feature_id=27734, name="ephemeris_lunar_age_days", family="alternative",
            current_eligible=True, deprecated=False,
            n_obs=1000, last_obs=date(2026, 11, 21), max_obs=date(2100, 12, 31),
            n_distinct=999, value_std=8.5, cadence="daily",
        ),
        as_of=as_of,
    )
    assert astro.eligible is False
    assert "excluded_non_economic_feature" in astro.reasons
    assert any(r.startswith("future_dated") for r in astro.reasons)

    # Constant placeholder feed (like ais_ground_truth) rejected
    const = evaluate_feature_eligibility(
        FeatureHealthStats(
            feature_id=999, name="ais_port_la_util", family="alternative",
            current_eligible=True, deprecated=False,
            n_obs=300, last_obs=date(2026, 11, 21), max_obs=date(2026, 11, 21),
            n_distinct=1, value_std=0.0, cadence="daily",
        ),
        as_of=as_of,
    )
    assert const.eligible is False
    assert "zero_variance_placeholder" in const.reasons

    assert infer_cadence("macro", "cpi_yoy") == "monthly"
    assert infer_cadence("macro", "initial_claims") == "weekly"


def test_retired_fix_model_eligible_refuses_to_run():
    with pytest.raises(RuntimeError, match="retired"):
        fix_model_eligible()


def test_the_audit_never_writes():
    """Dry-run only: no apply parameter, no write statement, a read-only transaction."""
    assert "apply" not in inspect.signature(EA.audit_feature_eligibility).parameters
    source = inspect.getsource(EA).upper()
    for verb in ("UPDATE ", "INSERT ", "DELETE ", "TRUNCATE ", "ALTER "):
        assert verb not in source, verb
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.dialect.name = "postgresql"
    conn.execute.return_value.fetchall.return_value = [
        (11, "vix_spot", "vol", False, False, 500, date(2026, 11, 20), date(2026, 11, 20), 450, 4.2),
    ]
    report = EA.audit_feature_eligibility(engine, as_of=date(2026, 11, 21))
    assert str(conn.execute.call_args_list[0][0][0]) == "SET TRANSACTION READ ONLY"
    engine.begin.assert_not_called()
    assert report["applied"] is False and report["to_enable"] == [{"id": 11, "name": "vix_spot"}]
