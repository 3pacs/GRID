"""Turn core market and flow features model-eligible, and astrological ones off, keeping a pre-image (#1301).

Revision ID: eligibility_core_20261122
Revises: agent_report_ask_checks_20261122
Create Date: 2026-11-22 00:00:00.000000

DO NOT MERGE before the order agreed for #1301:
1. the Oracle polarity fix (claude/fp-oracle-polarity-20261122) is merged and deployed;
2. auto_regime binds its own declared sources and has hysteresis (#1307).

``feature_registry.model_eligible`` is a global switch read by the Oracle,
inference, discovery, feature selection and the auto_regime BUY gate, and every
deploy runs ``alembic upgrade head``. This migration therefore changes all of
those consumers at once.

Why: the retired ``scripts/fix_model_eligible.py`` set ids 1..25 FALSE. That
switched off vix_spot (id 11) and yld_curve_2s10s (id 1) although both are
current, and on griddb the auto_regime stress index runs on 6 inputs with no
VIX or curve. Meanwhile ``scripts/backfill_celestial_ephemeris.py`` registered
ephemeris rows (dated to 2100) model-eligible.

What it does:
* ``feature_eligibility_changes`` (append-only) records the pre-image of every
  row it changes: id, name, old and new flag, which migration, when.
* It turns the listed core features on and the astrological ones off. Both lists
  touch only rows that are not deprecated and whose flag actually differs.
* Downgrade restores each changed row's old flag from that pre-image, so a
  rollback never guesses, and it records the restore as well.
"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
revision: str = "eligibility_core_20261122"
down_revision: Union[str, Sequence[str], None] = "agent_report_ask_checks_20261122"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

CORE = (
    "vix_spot", "vix9d_spot", "vix3m_spot", "vvix", "implied_correlation_3m",
    "yld_curve_2s10s", "yld_curve_3m10y", "fed_funds_rate",
    "finra_dix_sp500", "finra_short_volume_ratio_market",
    "finra_short_volume_ratio_spy", "finra_short_volume_ratio_qqq",
    "finra_short_volume_ratio_iwm",
)
ASTRO = (
    "days_to_full_moon", "days_to_new_moon", "jupiter_saturn_angle",
    "lunar_eclipse_proximity", "lunar_illumination", "lunar_phase",
    "mars_volatility_index", "mercury_retrograde", "planetary_stress_index",
    "solar_eclipse_proximity", "venus_cycle_phase", "chinese_element",
    "chinese_lunar_month", "chinese_yin_yang", "chinese_zodiac_year",
    "dasha_cycle_phase", "feng_shui_flying_star", "iching_hexagram_of_day",
    "nakshatra_index", "nakshatra_quality", "rahu_ketu_axis", "tithi",
)

CHANGES_TABLE = """
CREATE TABLE IF NOT EXISTS feature_eligibility_changes (
    id               BIGSERIAL PRIMARY KEY,
    feature_id       INTEGER NOT NULL,
    name             TEXT NOT NULL,
    old_eligible     BOOLEAN NOT NULL,
    new_eligible     BOOLEAN NOT NULL,
    migration        TEXT NOT NULL,
    direction        TEXT NOT NULL CHECK (direction IN ('upgrade', 'downgrade')),
    changed_at       TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""

APPEND_ONLY = """
CREATE OR REPLACE FUNCTION feature_eligibility_changes_append_only()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'feature_eligibility_changes is append-only: % refused', TG_OP;
END;
$$ LANGUAGE plpgsql
"""

TRIGGERS = (
    "DROP TRIGGER IF EXISTS feature_eligibility_changes_no_mutation ON feature_eligibility_changes",
    """CREATE TRIGGER feature_eligibility_changes_no_mutation
    BEFORE UPDATE OR DELETE ON feature_eligibility_changes
    FOR EACH ROW EXECUTE FUNCTION feature_eligibility_changes_append_only()""",
    "DROP TRIGGER IF EXISTS feature_eligibility_changes_no_truncate ON feature_eligibility_changes",
    """CREATE TRIGGER feature_eligibility_changes_no_truncate
    BEFORE TRUNCATE ON feature_eligibility_changes
    FOR EACH STATEMENT EXECUTE FUNCTION feature_eligibility_changes_append_only()""",
)

GRANTS = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
        EXECUTE 'GRANT SELECT, INSERT ON feature_eligibility_changes TO grid';
        EXECUTE 'GRANT USAGE, SELECT ON SEQUENCE feature_eligibility_changes_id_seq TO grid';
    END IF;
END
$$
"""

# Whole literal statements with bound values (.claude/rules/security.md): each records the pre-image and
# flips the flag in one statement. Only non-deprecated rows whose flag differs are touched, so a re-run
# changes and records nothing.
ENABLE_CORE = text("""
WITH targets AS (
    SELECT id, name, model_eligible FROM feature_registry
    WHERE deprecated_at IS NULL AND model_eligible IS DISTINCT FROM TRUE AND name = ANY(:names)
    FOR UPDATE
), recorded AS (
    INSERT INTO feature_eligibility_changes (feature_id, name, old_eligible, new_eligible, migration, direction)
    SELECT id, name, model_eligible, TRUE, :revision, 'upgrade' FROM targets
)
UPDATE feature_registry f SET model_eligible = TRUE FROM targets t WHERE f.id = t.id
""")

DISABLE_ASTRO = text("""
WITH targets AS (
    SELECT id, name, model_eligible FROM feature_registry
    WHERE deprecated_at IS NULL AND model_eligible IS DISTINCT FROM FALSE
      AND (name LIKE 'ephemeris%' OR name = ANY(:names))
    FOR UPDATE
), recorded AS (
    INSERT INTO feature_eligibility_changes (feature_id, name, old_eligible, new_eligible, migration, direction)
    SELECT id, name, model_eligible, FALSE, :revision, 'upgrade' FROM targets
)
UPDATE feature_registry f SET model_eligible = FALSE FROM targets t WHERE f.id = t.id
""")

RESTORE = text("""
WITH changed AS (
    SELECT DISTINCT ON (feature_id) feature_id, name, old_eligible
    FROM feature_eligibility_changes
    WHERE migration = :revision AND direction = 'upgrade'
    ORDER BY feature_id, id DESC
), restore AS (
    SELECT c.feature_id, c.name, c.old_eligible, f.model_eligible
    FROM changed c JOIN feature_registry f ON f.id = c.feature_id
    WHERE f.model_eligible IS DISTINCT FROM c.old_eligible
    FOR UPDATE OF f
), recorded AS (
    INSERT INTO feature_eligibility_changes (feature_id, name, old_eligible, new_eligible, migration, direction)
    SELECT feature_id, name, model_eligible, old_eligible, :revision, 'downgrade' FROM restore
)
UPDATE feature_registry f SET model_eligible = r.old_eligible FROM restore r WHERE f.id = r.feature_id
""")


def upgrade() -> None:
    op.execute(CHANGES_TABLE)
    op.execute(APPEND_ONLY)
    for statement in TRIGGERS:
        op.execute(statement)
    op.execute(GRANTS)
    op.execute(ENABLE_CORE.bindparams(names=list(CORE), revision=revision))
    op.execute(DISABLE_ASTRO.bindparams(names=list(ASTRO), revision=revision))


def downgrade() -> None:
    # Restore exactly what this migration changed; the change log stays (it is the audit trail).
    op.execute(RESTORE.bindparams(revision=revision))
