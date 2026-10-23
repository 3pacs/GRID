"""Register finra_ats_total_volume, the FINRA ATS feature the resolver drops.

Revision ID: finra_ats_feature_20261020
Revises: people_events_v2_20261001
Create Date: 2026-10-20 04:10:00.000000

After the 2026-10-20 FINRA ATS repair (pull_log 9724, ``smart:finra_ats``
SUCCESS 50, the first rows ``finra.ats_total_volume`` has ever had on
griddb) grid-hermes logged, at 2026-10-20 03:25:36Z::

    normalization.entity_map:get_feature_id:1308 -- Mapping exists
    (finra.ats_total_volume -> finra_ats_total_volume) but feature not in
    registry -- further misses for this series_id are counted, not logged

That is the ghost-mapping failure migrations 0062 / 0063 (ported in
``raw_sql_port_20260914``) fixed for GDELT and nine ETF closes:
``normalization/entity_map.py`` has mapped ``finra.ats_total_volume`` onto
``finra_ats_total_volume`` in ``SEED_MAPPINGS`` all along, but
``EntityMap.get_feature_id()`` resolves that name against
``feature_registry`` and returns ``None`` when there is no row, so
``normalization/resolver.py`` drops every one of the 50 observations and
``resolved_series`` never sees them.

Confirmed absent, not assumed
-----------------------------
Read-only against griddb on 2026-10-20 (statement_timeout 30 s): no
``feature_registry`` row has a name starting with ``finra`` or containing
``dark_pool``, ``short_volume`` or ``short_interest``; ``count(*) WHERE
name = 'finra_ats_total_volume'`` is 0. ``source_catalog`` 2027 FINRA_ATS exists
(latency_class WEEKLY, pit_available, revision_behavior NEVER).

Deliberately NOT included
-------------------------
The puller declares two siblings, and ``SEED_MAPPINGS`` maps both, but
neither has a writer today, and 0063 set the rule that a registry row with
no producer only adds freshness noise:

* ``finra_ats_dark_pct`` -- ``pull_ats_volume`` inserts it only when the
  week has a consolidated total volume, and FINRA's ``weeklySummary``
  carries no such field; the repair receipt records 0 rows "by design".
* ``finra_short_interest_total`` -- ``pull_short_interest`` still gets
  HTTP 400 from ``consolidatedShortInterest`` (sort without an EQUAL
  partition-key filter); 0 rows ever. Its request fix is a separate code
  change, and the row belongs with it.

Column values
-------------
``feature_registry`` CHECKs them (``family`` in a fixed set,
``normalization`` in ZSCORE/MINMAX/RAW/RANK, ``missing_data_policy`` in
FORWARD_FILL/INTERPOLATE/NAN, ``transformation_version >= 1``) and
``tests/test_migration_feature_registry_values.py`` reads this file.

* family ``alternative``: the puller lives in ``ingestion/altdata/`` and
  that is the family the registry's other altdata features carry (47 rows
  on griddb; ``flows`` is allowed by the CHECK but holds none).
* ``lag_days`` 0: in this registry ``lag_days`` is the differencing period
  ``features/lab.py`` applies (``series.diff(lag_days)``), not FINRA's
  two-week publication delay. Publication timing is carried by
  ``raw_series.pull_timestamp`` and the PIT readers.
* ``normalization`` ZSCORE: a weekly share count, like every volume-style
  feature here; RAW is for prices and rates stored as published (0063).
* ``eligible_from_date`` 2025-10-20: the earliest week in the rows now on
  griddb (``weeklySummary`` holds a rolling 12 months).

Idempotent by construction
--------------------------
``ON CONFLICT (name) DO NOTHING``, as ``raw_sql_port_20260914`` and
``ingestion/seed_v2.py`` write it. No table, no sequence, so no GRANT
footer: ``grid`` already holds privileges on ``feature_registry``. The
``before -> after`` count in the deploy log turns a silent no-op into a
stated one, exactly as the port revision does.

Applied by ``python3 -m alembic upgrade head`` in ``deploy.yml``; nothing
else runs it, and it was not applied by hand anywhere.
"""

import logging
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
# 26 characters: alembic_version.version_num is VARCHAR(32);
# tests/test_alembic_single_head.py guards the ceiling.
revision: str = "finra_ats_feature_20261020"
down_revision: str | Sequence[str] | None = "people_events_v2_20261001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

log = logging.getLogger("alembic.runtime.migration")

# The name is SEED_MAPPINGS["finra.ats_total_volume"] in
# normalization/entity_map.py; the description is the puller's own
# FINRA_ATS_FEATURES["ats_total_volume"] in ingestion/altdata/finra_ats.py.
FINRA_ATS_FEATURES = """
INSERT INTO feature_registry
  (name, family, description, transformation,
   transformation_version, lag_days, normalization, missing_data_policy,
   eligible_from_date, model_eligible)
VALUES
  -- Weekly ATS (dark pool) share volume, FINRA otcMarket/weeklySummary,
  -- ATS_W_SMBL totalWeeklyShareQuantity summed over the tracked tickers.
  ('finra_ats_total_volume', 'alternative',
   'Total ATS (dark pool) weekly share volume across major tickers (SPY, QQQ, IWM, AAPL, MSFT, NVDA, TSLA), FINRA weeklySummary',
   'FINRA weeklySummary ATS_W_SMBL totalWeeklyShareQuantity, summed over tracked tickers',
   1, 0, 'ZSCORE', 'FORWARD_FILL', '2025-10-20', TRUE)
ON CONFLICT (name) DO NOTHING
"""

# Every name the statement above inserts. Used to report what the revision
# actually changed, and by tests/test_finra_ats_feature_registration.py.
REGISTERED_FEATURES = ("finra_ats_total_volume",)


def _registered(conn) -> int:
    """How many of REGISTERED_FEATURES feature_registry currently holds."""
    return conn.execute(
        text("SELECT count(*) FROM feature_registry WHERE name = ANY(:names)"),
        {"names": list(REGISTERED_FEATURES)},
    ).scalar()


def upgrade() -> None:
    """Insert the row, then say whether this database actually needed it."""
    conn = op.get_bind()
    before = _registered(conn)

    op.execute(FINRA_ATS_FEATURES)

    after = _registered(conn)
    log.warning(
        "feature_registry finra_ats_total_volume: %d of %d present before, "
        "%d after (%d inserted). 0 inserted means this database already had "
        "it; a read of griddb on 2026-10-20 found it absent.",
        before, len(REGISTERED_FEATURES), after, after - before,
    )


def downgrade() -> None:
    """Delete exactly the row this revision inserts.

    Safe only because ``feature_registry.name`` is unique and the name is
    the migration's own. Any ``resolved_series`` rows the resolver wrote
    against it are left alone: a downgrade that reached into the data would
    be destroying observations to undo a registration.
    """
    op.get_bind().execute(
        text("DELETE FROM feature_registry WHERE name = ANY(:names)"),
        {"names": list(REGISTERED_FEATURES)},
    )
