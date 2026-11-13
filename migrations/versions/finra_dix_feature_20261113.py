"""Register the DIX feature (finra:dix_sp500).

Revision ID: finra_dix_feature_20261113
Revises: options_spot_clock_20261110
Create Date: 2026-11-13 12:00:00.000000

``ingestion/altdata/finra_dix.py`` writes ``finra:dix_sp500``: the dollar-weighted short share of FINRA
off-exchange volume across S&P 500 members (SqueezeMetrics' DIX definition), one row per trade date, from the same
Reg SHO CNMS file as ``finra_short_volume_agg``. ``entity_map`` sends it to ``finra_dix_sp500``.

It is a share of volume, so RAW normalization; family ``alternative`` and ``model_eligible`` FALSE, like the FINRA
ratios. The series does not exist when this revision runs, so ``eligible_from_date`` is dated, conservatively, from
the first SUCCESS row of ``finra:short_volume:SPY`` (every CNMS file carries SPY); skipped if that has no data.

Idempotent: ``ON CONFLICT (name) DO NOTHING``. Downgrade refuses, like ``finra_short_volume_agg_20261105``.
"""

import logging
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "finra_dix_feature_20261113"
down_revision: str | Sequence[str] | None = "options_spot_clock_20261110"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

log = logging.getLogger("alembic.runtime.migration")

DIX_FEATURE = """
WITH candidate (name, family, description, transformation, transformation_version,
                lag_days, normalization, missing_data_policy, dating_series_id) AS (
  VALUES
  ('finra_dix_sp500', 'alternative', 'DIX: dollar-weighted FINRA dark-pool short-volume share across S&P 500 members',
   'FINRA Reg SHO CNMS sum(ShortVolume*close)/sum(TotalVolume*close), S&P 500 members', 1, 1, 'RAW', 'FORWARD_FILL',
   'finra:short_volume:SPY')
)
INSERT INTO feature_registry
  (name, family, description, transformation,
   transformation_version, lag_days, normalization, missing_data_policy,
   eligible_from_date, model_eligible)
SELECT c.name, c.family, c.description, c.transformation,
       c.transformation_version, c.lag_days, c.normalization, c.missing_data_policy,
       first_obs.obs_date, FALSE
FROM candidate c
CROSS JOIN LATERAL (
  SELECT r.obs_date
  FROM raw_series r
  WHERE r.series_id = c.dating_series_id
    AND r.pull_status = 'SUCCESS'
  ORDER BY r.obs_date ASC
  LIMIT 1
) AS first_obs
ON CONFLICT (name) DO NOTHING
"""

REGISTERED_FEATURES: dict[str, str] = {"finra_dix_sp500": "finra:dix_sp500"}


def upgrade() -> None:
    conn = op.get_bind()
    op.execute("SET LOCAL statement_timeout = '120s'")
    op.execute(DIX_FEATURE)
    op.execute("SET LOCAL statement_timeout TO DEFAULT")
    n = conn.execute(text("SELECT count(*) FROM feature_registry WHERE name = ANY(:n)"),
                     {"n": list(REGISTERED_FEATURES)}).scalar()
    log.warning("feature_registry DIX: %d of %d present after upgrade.", n, len(REGISTERED_FEATURES))


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through finra_dix_feature_20261113 is refused: it cannot tell a row it inserted from one that "
        "already existed, and resolved_series references it. Use a separately reviewed forward migration."
    )
