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
2026, so no model has seen it.

The row is a literal, so every database gets it whatever raw_series holds
(Codex review 6026828927 finding 2). The first version dated it from
``put_call_ratio``'s first SUCCESS row through a CROSS JOIN, which inserts
nothing on a fresh database; Alembic never replays a revision, so the
equity series would have been a ghost mapping there for good.
``eligible_from_date`` is 2026-11-20, the first session whose page was read
and checked for this ratio (0.77, the fixture
``tests/fixtures/cboe/daily_options_2026-11-20.json``). Nothing reads the
date to admit rows (the resolver keys on the name); a backfill may store
earlier sessions. griddb was at ``market_state_prov_20261122`` with no
``equity_put_call_ratio`` row on 2026-11-23, so no database ran the first
version.

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
INSERT INTO feature_registry
  (name, family, description, transformation,
   transformation_version, lag_days, normalization, missing_data_policy,
   eligible_from_date, model_eligible)
VALUES
  ('equity_put_call_ratio', 'vol', 'CBOE equity PUT/CALL volume ratio',
   'Cboe daily options page, ratios[] EQUITY PUT/CALL RATIO', 1, 0, 'RAW', 'FORWARD_FILL',
   '2026-11-20', FALSE)
ON CONFLICT (name) DO NOTHING
"""

REGISTERED_FEATURES: dict[str, str] = {"equity_put_call_ratio": "equity_put_call_ratio"}


def upgrade() -> None:
    conn = op.get_bind()
    op.execute(EQUITY_PCR_FEATURE)
    n = conn.execute(text("SELECT count(*) FROM feature_registry WHERE name = ANY(:n)"),
                     {"n": list(REGISTERED_FEATURES)}).scalar()
    log.warning("feature_registry equity put/call: %d of %d present after upgrade.", n, len(REGISTERED_FEATURES))


def downgrade() -> None:
    raise RuntimeError(
        "Downgrade through cboe_equity_pcr_20261123 is refused: it cannot tell a row it inserted from one that "
        "already existed, and resolved_series references it. Use a separately reviewed forward migration."
    )
