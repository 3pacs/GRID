"""Register the FINRA daily short-volume ratio features.

Revision ID: finra_short_volume_agg_20261105
Revises: ghost_features_20261105
Create Date: 2026-11-05 12:00:00.000000

``ingestion/altdata/finra_short_volume.py`` has written about 12k
``finra:short_volume:<SYMBOL>`` series a day since 2026-09-27, and nothing
outside ingestion read them: the resolver counts them as "no entity_map
entry" on every run. ``ingestion/altdata/finra_short_volume_agg.py`` (the
scheduler's ``finra_short_volume`` entry now runs its subclass) writes four
daily aggregates from the same file, in their own namespace and mapped in
``entity_map``:

==========================================  =================================
raw series                                  feature
==========================================  =================================
``finra:short_volume_agg:market_ratio``     finra_short_volume_ratio_market
``finra:short_volume_agg:spy_ratio``        finra_short_volume_ratio_spy
``finra:short_volume_agg:qqq_ratio``        finra_short_volume_ratio_qqq
``finra:short_volume_agg:iwm_ratio``        finra_short_volume_ratio_iwm
==========================================  =================================

Each is sum(ShortVolume) / sum(TotalVolume) over every facility row in
scope. That is a share of volume, so it uses RAW normalization. The family
is ``alternative``, like ``finra_ats_total_volume``. ``model_eligible`` is
FALSE.

The aggregate series do not exist when this revision runs; they start with
the next pull or with ``backfill_aggregates``. Each row is therefore dated
from the first SUCCESS row of the per-symbol series computed from the same
daily files (SPY for the market-wide ratio, since every CNMS file carries
it), and skipped if that series has no data. A backfill can reach further
back than this date, so the date is conservative, never early.

Idempotent: ``ON CONFLICT (name) DO NOTHING``. Downgrade refuses, like
``ghost_features_20261105``.
"""

import logging
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "finra_short_volume_agg_20261105"
down_revision: str | Sequence[str] | None = "ghost_features_20261105"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

log = logging.getLogger("alembic.runtime.migration")

FINRA_RATIO_FEATURES = """
WITH candidate (name, family, description, transformation, transformation_version,
                lag_days, normalization, missing_data_policy, dating_series_id) AS (
  VALUES
  ('finra_short_volume_ratio_market', 'alternative', 'FINRA daily short-sale volume as a share of total volume, all CNMS symbols',
   'FINRA Reg SHO CNMS sum(ShortVolume)/sum(TotalVolume), all symbols', 1, 0, 'RAW', 'FORWARD_FILL', 'finra:short_volume:SPY'),
  ('finra_short_volume_ratio_spy', 'alternative', 'FINRA daily short-sale volume ratio for SPY',
   'FINRA Reg SHO CNMS ShortVolume/TotalVolume, SPY', 1, 0, 'RAW', 'FORWARD_FILL', 'finra:short_volume:SPY'),
  ('finra_short_volume_ratio_qqq', 'alternative', 'FINRA daily short-sale volume ratio for QQQ',
   'FINRA Reg SHO CNMS ShortVolume/TotalVolume, QQQ', 1, 0, 'RAW', 'FORWARD_FILL', 'finra:short_volume:QQQ'),
  ('finra_short_volume_ratio_iwm', 'alternative', 'FINRA daily short-sale volume ratio for IWM',
   'FINRA Reg SHO CNMS ShortVolume/TotalVolume, IWM', 1, 0, 'RAW', 'FORWARD_FILL', 'finra:short_volume:IWM')
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

# feature name -> the aggregate raw series entity_map sends to it.
REGISTERED_FEATURES: dict[str, str] = {
    "finra_short_volume_ratio_market": "finra:short_volume_agg:market_ratio",
    "finra_short_volume_ratio_spy": "finra:short_volume_agg:spy_ratio",
    "finra_short_volume_ratio_qqq": "finra:short_volume_agg:qqq_ratio",
    "finra_short_volume_ratio_iwm": "finra:short_volume_agg:iwm_ratio",
}


def _registered(conn) -> int:
    return conn.execute(
        text("SELECT count(*) FROM feature_registry WHERE name = ANY(:names)"),
        {"names": list(REGISTERED_FEATURES)},
    ).scalar()


def upgrade() -> None:
    conn = op.get_bind()
    before = _registered(conn)

    op.execute("SET LOCAL statement_timeout = '120s'")
    op.execute(FINRA_RATIO_FEATURES)
    op.execute("SET LOCAL statement_timeout TO DEFAULT")

    after = _registered(conn)
    log.warning(
        "feature_registry FINRA short-volume ratios: %d of %d present before, "
        "%d after (%d inserted).",
        before, len(REGISTERED_FEATURES), after, after - before,
    )


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through finra_short_volume_agg_20261105 is refused: it cannot "
        "tell rows it inserted from rows that already existed, and resolved_series "
        "references them. Use a separately reviewed forward migration."
    )
