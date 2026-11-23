"""Register the Cboe equity put/call ratio (equity_put_call_ratio).

Revision ID: cboe_equity_pcr_20261123
Revises: market_state_prov_20261122
Create Date: 2026-11-23 12:00:00.000000

``ingestion/altdata/cboe_indices.py`` now stores the "EQUITY PUT/CALL RATIO"
of Cboe's daily options page (``<date>_daily_options``, the page it already
reads for the total ratio) as ``equity_put_call_ratio``, one row per trading
day. ``entity_map`` sends it to the feature of the same name. Without this
row the resolver would drop every observation ("Mapping exists ... but feature
not in registry", the ghost-mapping case of ``finra_ats_feature_20261020``).

It sits beside ``put_call_ratio`` (schema.sql: family ``vol``, RAW, lag 0,
FORWARD_FILL), but ``model_eligible`` is FALSE: the series starts in November
2026, so no model has seen it. The series does not exist when this revision
runs, so ``eligible_from_date`` is dated from the first SUCCESS row of
``put_call_ratio``, which comes from the same page; skipped if that has no
data (an empty database).

Idempotent: ``ON CONFLICT (name) DO NOTHING``. Downgrade refuses, like
``finra_dix_feature_20261113``.
"""

import logging
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "cboe_equity_pcr_20261123"
down_revision: str | Sequence[str] | None = "market_state_prov_20261122"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

log = logging.getLogger("alembic.runtime.migration")

EQUITY_PCR_FEATURE = """
WITH candidate (name, family, description, transformation, transformation_version,
                lag_days, normalization, missing_data_policy, dating_series_id) AS (
  VALUES
  ('equity_put_call_ratio', 'vol', 'CBOE equity PUT/CALL volume ratio',
   'Cboe daily options page, ratios[] EQUITY PUT/CALL RATIO', 1, 0, 'RAW', 'FORWARD_FILL',
   'put_call_ratio')
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

REGISTERED_FEATURES: dict[str, str] = {"equity_put_call_ratio": "equity_put_call_ratio"}


def upgrade() -> None:
    conn = op.get_bind()
    op.execute("SET LOCAL statement_timeout = '120s'")
    op.execute(EQUITY_PCR_FEATURE)
    op.execute("SET LOCAL statement_timeout TO DEFAULT")
    n = conn.execute(text("SELECT count(*) FROM feature_registry WHERE name = ANY(:n)"),
                     {"n": list(REGISTERED_FEATURES)}).scalar()
    log.warning("feature_registry equity put/call: %d of %d present after upgrade.", n, len(REGISTERED_FEATURES))


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through cboe_equity_pcr_20261123 is refused: it cannot tell a row it inserted from one that "
        "already existed, and resolved_series references it. Use a separately reviewed forward migration."
    )
