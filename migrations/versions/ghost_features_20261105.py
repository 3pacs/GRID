"""Register the 17 mapped features the resolver still drops after #941.

Revision ID: ghost_features_20261105
Revises: yf_latest_vintage_scan_20261026
Create Date: 2026-11-05 11:00:00.000000

``ghost_features_20261104`` registered the 51 names in that day's hermes
journal. A bounded re-resolve dry run on 2026-11-05
(``python -m normalization.resolver --since 2026-10-06 --chunk-days 1
--dry-run``) still printed these in its daily summaries as "feature name(s)
missing from feature_registry (rows dropped until registered)". Every one is
in ``normalization/entity_map.SEED_MAPPINGS`` and has a producer in
``ingestion/``:

* CNN Fear & Greed (``altdata/fear_greed.py``): ``feargreed.cnn_value``,
  ``feargreed.cnn_previous_close``
* AAII sentiment survey (``altdata/aaii_sentiment.py``): ``aaii.*``
* GDELT topic tone and volume (``altdata/gdelt.py``)
* Freight container index (``altdata/supply_chain.py``)
* UN Comtrade (``trade/comtrade.py``): ``us_exports_total``,
  ``us_china_bilateral_yoy``
* Fed H.4.1 lines (``altdata/fed_liquidity.py`` / ``fred.py``): ``WSHOSHO``,
  ``SWPT``. ``ingestion/seed_v2.py`` lists both, but that statement never
  inserted anything (see ``ghost_features_20261104``).
* NY Fed SOMA (``altdata/nyfed.py``), FINRA short interest
  (``altdata/finra_short_volume.py``), and the ECB BTP-Bund spread
  (``international/ecb.py``, also listed in seed_v2).

Data-driven, not hand-dated
---------------------------
#941's dates came from a read of griddb. These rows are inserted with
``INSERT ... SELECT``, joined LATERAL to the earliest SUCCESS ``raw_series``
row of the mapped series. So:

* ``eligible_from_date`` is that series' first SUCCESS ``obs_date`` on this
  database, which is #941's convention, computed instead of copied.
* A name whose series has no SUCCESS row is skipped (0063's rule: no registry
  row without a producer). The upgrade logs how many were inserted.

Each lookup is a range scan of one ``series_id``
(``uq_raw_series_composite`` leads with it), never a scan of the whole
table. These are low-frequency macro and sentiment series, and the statement
is bounded by a local statement timeout.

Column values follow ``ghost_features_20261104``: family from the nearest
registered sibling (GDELT tone rows are ``sentiment``, FINRA ``alternative``,
Comtrade ``trade``), RAW for indices and percentages published as-is,
ZSCORE for quantities, ``lag_days`` 0, ``FORWARD_FILL``, and
``model_eligible`` FALSE: registering stops the drop, and promotion stays a
separate decision. ``tests/test_migration_feature_registry_values.py`` parses
the VALUES rows below.

PIT: registration writes no observations. Rows the resolver writes later
carry ``release_date = vintage_date = pull date``. Re-resolving older pulls
is an explicit, bounded ``python -m normalization.resolver --since`` run.

Idempotent: ``ON CONFLICT (name) DO NOTHING``.
"""

import logging
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "ghost_features_20261105"
down_revision: str | Sequence[str] | None = "yf_latest_vintage_scan_20261026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

log = logging.getLogger("alembic.runtime.migration")

GHOST_FEATURES = """
WITH candidate (name, family, description, transformation, transformation_version,
                lag_days, normalization, missing_data_policy, raw_series_id) AS (
  VALUES
  -- CNN Fear & Greed (altdata/fear_greed.py)
  ('feargreed_cnn_value', 'sentiment', 'CNN Fear & Greed index, current value (0-100)',
   'CNN Fear and Greed index value', 1, 0, 'RAW', 'FORWARD_FILL', 'feargreed.cnn_value'),
  ('feargreed_cnn_previous_close', 'sentiment', 'CNN Fear & Greed index, previous close (0-100)',
   'CNN Fear and Greed index previous close', 1, 0, 'RAW', 'FORWARD_FILL', 'feargreed.cnn_previous_close'),

  -- AAII weekly sentiment survey (altdata/aaii_sentiment.py)
  ('aaii_bullish_pct', 'sentiment', 'AAII sentiment survey, percent bullish',
   'AAII survey bullish percent', 1, 0, 'RAW', 'FORWARD_FILL', 'aaii.bullish_pct'),
  ('aaii_bearish_pct', 'sentiment', 'AAII sentiment survey, percent bearish',
   'AAII survey bearish percent', 1, 0, 'RAW', 'FORWARD_FILL', 'aaii.bearish_pct'),
  ('aaii_neutral_pct', 'sentiment', 'AAII sentiment survey, percent neutral',
   'AAII survey neutral percent', 1, 0, 'RAW', 'FORWARD_FILL', 'aaii.neutral_pct'),
  ('aaii_bull_bear_spread', 'sentiment', 'AAII sentiment survey, bullish minus bearish percent',
   'AAII survey bullish minus bearish', 1, 0, 'RAW', 'FORWARD_FILL', 'aaii.bull_bear_spread'),

  -- GDELT topic tone / volume (altdata/gdelt.py)
  ('gdelt_recession_tone', 'sentiment', 'GDELT average media tone of recession coverage',
   'GDELT DOC tone, recession topic query', 1, 0, 'ZSCORE', 'FORWARD_FILL', 'gdelt_recession_tone'),
  ('gdelt_fed_tone', 'sentiment', 'GDELT average media tone of Federal Reserve coverage',
   'GDELT DOC tone, Federal Reserve topic query', 1, 0, 'ZSCORE', 'FORWARD_FILL', 'gdelt_fed_tone'),
  ('gdelt_trade_conflict_volume', 'sentiment', 'GDELT article volume of trade-conflict coverage',
   'GDELT DOC volume, trade conflict topic query', 1, 0, 'ZSCORE', 'FORWARD_FILL', 'gdelt_trade_conflict_volume'),

  -- Supply chain (altdata/supply_chain.py)
  ('container_index', 'trade', 'Global container freight rate index',
   'supply_chain container freight index', 1, 0, 'ZSCORE', 'FORWARD_FILL', 'container_index'),

  -- UN Comtrade (trade/comtrade.py)
  ('us_exports_total', 'trade', 'US total goods exports to the world (UN Comtrade, reporter 842)',
   'Comtrade reporter 842 partner 0 TOTAL exports', 1, 0, 'ZSCORE', 'FORWARD_FILL', 'us_exports_total'),
  ('us_china_bilateral_yoy', 'trade', 'US-China bilateral goods trade, year-over-year change (UN Comtrade)',
   'Comtrade reporter 842 partner 156 TOTAL, YoY change', 1, 0, 'ZSCORE', 'FORWARD_FILL', 'us_china_bilateral_yoy'),

  -- Fed H.4.1 (altdata/fed_liquidity.py, fred.py)
  ('fed_treasury_holdings', 'macro', 'Fed holdings of Treasury securities, weekly (FRED WSHOSHO)',
   'WSHOSHO weekly level from FRED', 1, 0, 'ZSCORE', 'FORWARD_FILL', 'WSHOSHO'),
  ('central_bank_liq_swaps', 'macro', 'Central bank liquidity swaps outstanding, weekly (FRED SWPT)',
   'SWPT weekly level from FRED', 1, 0, 'ZSCORE', 'FORWARD_FILL', 'SWPT'),

  -- NY Fed SOMA (altdata/nyfed.py)
  ('nyfed_soma_total', 'macro', 'NY Fed SOMA holdings, total par value (USD bn)',
   'NY Fed SOMA total par, billions', 1, 0, 'ZSCORE', 'FORWARD_FILL', 'nyfed.soma_total_par_bn'),

  -- FINRA (altdata/finra_short_volume.py)
  ('finra_short_interest_total', 'alternative', 'FINRA aggregate short interest across reported equities',
   'FINRA short interest aggregate total', 1, 0, 'ZSCORE', 'FORWARD_FILL', 'finra.short_interest_total'),

  -- ECB (international/ecb.py)
  ('euro_btp_bund_spread', 'credit', 'Italy BTP minus German Bund 10Y yield spread (euro-area stress proxy)',
   'ECB SDW BTP 10Y minus Bund 10Y', 1, 0, 'RAW', 'FORWARD_FILL', 'euro_btp_bund_spread')
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
  WHERE r.series_id = c.raw_series_id
    AND r.pull_status = 'SUCCESS'
  ORDER BY r.obs_date ASC
  LIMIT 1
) AS first_obs
ON CONFLICT (name) DO NOTHING
"""

# feature name -> the raw series_id the statement above dates it from.
# Mirrors the VALUES rows; tests/test_ghost_features_20261105.py checks both
# against normalization/entity_map.SEED_MAPPINGS.
REGISTERED_FEATURES: dict[str, str] = {
    "feargreed_cnn_value": "feargreed.cnn_value",
    "feargreed_cnn_previous_close": "feargreed.cnn_previous_close",
    "aaii_bullish_pct": "aaii.bullish_pct",
    "aaii_bearish_pct": "aaii.bearish_pct",
    "aaii_neutral_pct": "aaii.neutral_pct",
    "aaii_bull_bear_spread": "aaii.bull_bear_spread",
    "gdelt_recession_tone": "gdelt_recession_tone",
    "gdelt_fed_tone": "gdelt_fed_tone",
    "gdelt_trade_conflict_volume": "gdelt_trade_conflict_volume",
    "container_index": "container_index",
    "us_exports_total": "us_exports_total",
    "us_china_bilateral_yoy": "us_china_bilateral_yoy",
    "fed_treasury_holdings": "WSHOSHO",
    "central_bank_liq_swaps": "SWPT",
    "nyfed_soma_total": "nyfed.soma_total_par_bn",
    "finra_short_interest_total": "finra.short_interest_total",
    "euro_btp_bund_spread": "euro_btp_bund_spread",
}


def _registered(conn) -> int:
    """How many of REGISTERED_FEATURES feature_registry currently holds."""
    return conn.execute(
        text("SELECT count(*) FROM feature_registry WHERE name = ANY(:names)"),
        {"names": list(REGISTERED_FEATURES)},
    ).scalar()


def upgrade() -> None:
    """Insert the rows that have data, then say how many this database needed."""
    conn = op.get_bind()
    before = _registered(conn)

    op.execute("SET LOCAL statement_timeout = '120s'")
    op.execute(GHOST_FEATURES)
    op.execute("SET LOCAL statement_timeout TO DEFAULT")

    after = _registered(conn)
    log.warning(
        "feature_registry ghost features (2026-11-05): %d of %d present before, "
        "%d after (%d inserted; names whose series has no SUCCESS raw row are "
        "skipped).",
        before, len(REGISTERED_FEATURES), after, after - before,
    )


def downgrade() -> None:
    """Refuse, for the same reason as ``ghost_features_20261104``.

    ``ON CONFLICT DO NOTHING`` means a name may have existed before the
    upgrade, and once the resolver writes ``resolved_series`` rows against
    these names the ``feature_id`` foreign key makes a delete destroy
    observations.
    """
    raise RuntimeError(
        "Downgrade through ghost_features_20261105 is refused: it cannot tell "
        "rows it inserted from rows that already existed, and resolved_series "
        "references them. Use a separately reviewed forward migration."
    )
