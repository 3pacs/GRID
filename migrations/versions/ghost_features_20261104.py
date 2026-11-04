"""Register the 47 mapped features the resolver drops (or is about to).

Revision ID: ghost_features_20261104
Revises: merge_yf_finra_20261023
Create Date: 2026-11-04 06:00:00.000000

In the 24 h to 2026-11-04 05:28Z grid-hermes logged 498 lines of::

    normalization.entity_map:get_feature_id -- Mapping exists (X -> Y) but
    feature not in registry -- further misses for this series_id are
    counted, not logged

and 55 resolver summaries naming the same names as "missing from
feature_registry (rows dropped until registered)". That is the ghost-mapping
failure 0062 / 0063 / ``finra_ats_feature_20261020`` fixed for other names:
``normalization/entity_map.py`` maps a live raw series onto a feature name,
``EntityMap.get_feature_id()`` finds no ``feature_registry`` row for it, and
``normalization/resolver.py`` drops every observation.

Why these were never registered
-------------------------------
Most of them (the Fed liquidity block, the 33 ETF flow proxies,
``ecb_bank_lending_yoy``, ``oi_k_shape_ratio``) *are* listed in
``ingestion/seed_v2.py``'s ``FEATURE_REGISTRY_SQL``. That statement is one
multi-row INSERT, and ten of its rows use ``family = 'liquidity'``, which
``feature_registry_family_check`` rejects -- so the whole statement fails and
inserts nothing. Nothing calls ``run_seed_v2()`` anyway (it is a ``__main__``
script); alembic is the mechanism deploy runs (``migrations/RAW_SQL_LEDGER.md``).
``vix3m_spot`` / ``vix9d_spot`` / ``yc_2y`` were added to the mappings by
later pullers (``cboe_indices.py``, ``yield_curve_full.py``) whose siblings
were registered by hand without them.

Confirmed absent, and confirmed fed, not assumed
-------------------------------------------------
Read-only against griddb on 2026-11-04 (statement_timeout 30 s):

* none of the 47 names (45 logged, plus bls_jolts_openings and the COR3M successor) below has a ``feature_registry`` row;
  ``count(*) WHERE name LIKE 'etf_flow%'`` is 0.
* every mapped series id has live SUCCESS rows in ``raw_series``:
  ``ETF_FLOW:<T>:{5d,20d,accel}`` 22 rows in the last 30 obs-days, obs from
  2020-01-02, last pull 2026-11-03 15:51Z; ``COMPUTED:fed_net_liquidity``
  (and ``_change_1w`` / ``_change_1m`` / ``reverse_repo_pct_of_peak``) 365
  rows / 30 d, last pull 2026-11-04 00:54Z; ``COMPUTED:tga_drawdown`` 58;
  ``RRPONTSYD`` 21 SUCCESS / 30 d; ``CBOE:VIX3M`` / ``CBOE:VIX9D`` 22 each,
  obs through 2026-11-03; ``DGS2`` 20 SUCCESS / 30 d; ``eurozone_hicp_yoy``
  last pull 2026-11-04 02:17Z; ``ecb_bank_lending_yoy`` 1,092 rows pulled in
  the last 3 days, obs through 2026-09-01; ``oi_k_shape_ratio`` last pull
  2026-11-04 00:54Z (its upstream, Opportunity Insights, stops at
  2024-06-16 -- the same as its registered siblings ``oi_*``).

Plus two names that are not in the log because their writer is new:
``bls_jolts_openings`` -- ``ingestion/bls.py`` requested ``JTS00000000JOL``,
which BLS answers "Series does not exist", so JOLTS was never stored; the bls
puller now requests ``JTS000000000000000JOL`` (total nonfarm job openings,
SA level, 2026 M09 = 7523), mapped in entity_map alongside the old key -- and
``implied_correlation_3m``: ``ingestion/altdata/
cboe_indices.py`` replaces Cboe's discontinued ICJ index (``correlation_index``,
last observation 2021-11-19, CSV now 403) with its successor COR3M, written as
``CBOE:COR3M``; COR3M_History.csv was probed from grid-svr on 2026-11-04
(HTTP 200, DATE/OPEN/HIGH/LOW/CLOSE, 2006-01-03 .. 2026-11-03).

Deliberately left as they are
-----------------------------
* ``overnight_reverse_repo`` duplicates two stale rows, ``reverse_repo``
  (119) and ``reverse_repo_usage`` (18104), and ``reverse_repo_pct_of_peak``
  duplicates ``rrp_as_pct_of_peak`` (18105) -- all three last resolved
  2026-04-02 from a yield-curve puller that no longer writes. ``yc_2y``
  likewise shadows the stale ``treasury_2y`` (107, last 2026-04-02) while
  matching its live siblings ``yc_1y`` / ``yc_5y`` / ``yc_30y``. The live
  writers already target the names registered here; collapsing the stale
  duplicates is a separate consumer-facing change.
* ``yc_10y`` is mapped from ``DGS10`` and absent too, but ``DGS10`` has no
  observation after 2026-04-02 on griddb, so it is not a ghost today
  (0063's rule: no registry row without a producer).

Column values
-------------
``feature_registry`` CHECKs (family / normalization / missing_data_policy /
transformation_version >= 1) are pinned by
``tests/test_migration_feature_registry_values.py``, which reads this file.
The ``trg_feature_registry_transformation_version`` trigger rejects a row
whose ``transformation`` text already exists at another version; every
``transformation`` here is either new on griddb or, where it is shared
(``CBOE CSV download``, ``RRPONTSYD from FRED``, ``RRPONTSYD / max(RRPONTSYD)``),
already at version 1 (checked 2026-11-04).

Each row copies its nearest registered neighbour:

* family -- ``rates`` for the ON RRP / TGA rows (``reverse_repo_usage``,
  ``rrp_as_pct_of_peak``, ``treasury_tga_balance``); ``macro`` for net
  liquidity (``fed_balance_sheet``) and the euro-area series
  (``ecb_m3_yoy``); ``vol`` for the CBOE indices (``vix_spot``,
  ``skew_index``); ``alternative`` for the OI series (seed_v2); ``flows`` for
  the ETF flow proxies (seed_v2's design, and the family ``oracle/engine.py``
  already weights).
* normalization -- RAW for levels published as-is (yields, VIX closes,
  percent-of-peak), ZSCORE for quantities and deltas (seed_v2, finra).
* ``lag_days`` 0: in this registry it is the differencing period
  ``features/lab.py`` applies, not a publication delay.
* ``eligible_from_date`` -- the earliest obs_date on griddb for the series.
* ``model_eligible`` -- the neighbour's value; FALSE where no registered
  neighbour exists (the ETF flows, the new COR3M row), so adding the row
  only stops the drop and promotion stays a separate decision.

PIT
---
Registration writes no observations. ``resolved_series`` rows the resolver
writes later carry ``release_date = vintage_date = the raw row's
pull_timestamp`` date (``Resolver.resolve_pending``), so history pulled in
March 2026 is visible to a point-in-time read from its March pull date
onwards, never earlier. Hermes resolves a 2-day pull window behind a
watermark, so only rows pulled after this revision runs are picked up
automatically; re-resolving the earlier pulls is an explicit, bounded
``python -m normalization.resolver --since <date> --until <date>`` run.

Idempotent: ``ON CONFLICT (name) DO NOTHING``. No table or sequence, so no
GRANT footer. Applied by ``python3 -m alembic upgrade head`` in deploy.yml.
"""

import logging
from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = "ghost_features_20261104"
down_revision: str | Sequence[str] | None = "merge_yf_finra_20261023"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

log = logging.getLogger("alembic.runtime.migration")

GHOST_FEATURES = """
INSERT INTO feature_registry
  (name, family, description, transformation,
   transformation_version, lag_days, normalization, missing_data_policy,
   eligible_from_date, model_eligible)
VALUES
  -- Euro area (international/eurostat.py, physical/dbnomics.py, international/ecb.py)
  ('eurozone_hicp_yoy', 'macro', 'Euro area HICP inflation, annual rate of change (Eurostat prc_hicp_manr)',
   'Eurostat prc_hicp_manr EA CP00 monthly annual rate of change', 1, 0, 'ZSCORE', 'FORWARD_FILL', '1997-01-01', FALSE),
  ('ecb_bank_lending_yoy', 'macro', 'Euro area bank lending to the private sector, YoY growth',
   'ECB SDW BSI.M.U2.Y.U.A20.A.I.U2.2240.Z01.A', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2004-01-01', FALSE),

  -- Opportunity Insights (altdata/opportunity.py)
  ('oi_k_shape_ratio', 'alternative', 'K-shape divergence: high-income minus low-income consumer spend index',
   'oi_spend_high minus oi_spend_low', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-13', FALSE),

  -- Fed liquidity equation (altdata/fed_liquidity.py, FRED RRPONTSYD)
  ('overnight_reverse_repo', 'rates', 'Fed ON RRP facility daily usage (billions)',
   'RRPONTSYD from FRED', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2003-02-07', TRUE),
  ('fed_net_liquidity', 'macro', 'Fed net liquidity = Fed total assets - TGA - ON RRP (millions USD)',
   'COMPUTED: WALCL - WTREGEN - RRPONTSYD', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-03-20', TRUE),
  ('fed_net_liquidity_change_1w', 'macro', 'Week-over-week change in Fed net liquidity',
   'COMPUTED: 7-day delta of fed_net_liquidity', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-03-20', TRUE),
  ('fed_net_liquidity_change_1m', 'macro', 'Month-over-month change in Fed net liquidity',
   'COMPUTED: 30-day delta of fed_net_liquidity', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-03-20', TRUE),
  ('reverse_repo_pct_of_peak', 'rates', 'ON RRP usage as a percentage of its historical peak (drain gauge)',
   'RRPONTSYD / max(RRPONTSYD)', 1, 0, 'RAW', 'FORWARD_FILL', '2026-03-20', TRUE),
  ('tga_drawdown', 'rates', '30-day change in the Treasury General Account (negative = spending = liquidity injection)',
   'COMPUTED: 30-day delta of WTREGEN', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-03-25', TRUE),

  -- CBOE indices (altdata/cboe_indices.py)
  ('vix3m_spot', 'vol', 'CBOE 3-month VIX (VIX3M) daily close',
   'CBOE CSV download', 1, 0, 'RAW', 'FORWARD_FILL', '2009-09-18', FALSE),
  ('vix9d_spot', 'vol', 'CBOE 9-day VIX (VIX9D) daily close',
   'CBOE CSV download', 1, 0, 'RAW', 'FORWARD_FILL', '2011-01-04', FALSE),
  ('implied_correlation_3m', 'vol', 'CBOE 3-month implied correlation index (COR3M), successor to the discontinued ICJ',
   'CBOE CSV download', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2006-01-03', FALSE),

  -- BLS JOLTS (ingestion/bls.py, JTS000000000000000JOL)
  ('bls_jolts_openings', 'macro', 'JOLTS total nonfarm job openings, seasonally adjusted level (thousands)',
   'BLS JTS000000000000000JOL', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2000-12-01', FALSE),

  -- Treasury curve (FRED DGS2; siblings yc_1y / yc_5y / yc_30y)
  ('yc_2y', 'rates', '2-year Treasury constant maturity yield',
   'DGS2 from FRED', 1, 0, 'RAW', 'FORWARD_FILL', '1990-01-01', TRUE),

  -- ETF flow proxies (altdata/institutional_flows.py)
  ('etf_flow_spy_5d', 'flows', 'SPY 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_spy_20d', 'flows', 'SPY 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_spy_accel', 'flows', 'SPY flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_qqq_5d', 'flows', 'QQQ 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_qqq_20d', 'flows', 'QQQ 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_qqq_accel', 'flows', 'QQQ flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_iwm_5d', 'flows', 'IWM 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_iwm_20d', 'flows', 'IWM 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_iwm_accel', 'flows', 'IWM flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_tlt_5d', 'flows', 'TLT 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_tlt_20d', 'flows', 'TLT 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_tlt_accel', 'flows', 'TLT flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_hyg_5d', 'flows', 'HYG 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_hyg_20d', 'flows', 'HYG 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_hyg_accel', 'flows', 'HYG flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_gld_5d', 'flows', 'GLD 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_gld_20d', 'flows', 'GLD 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_gld_accel', 'flows', 'GLD flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_eem_5d', 'flows', 'EEM 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_eem_20d', 'flows', 'EEM 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_eem_accel', 'flows', 'EEM flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xlk_5d', 'flows', 'XLK 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xlk_20d', 'flows', 'XLK 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xlk_accel', 'flows', 'XLK flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xlf_5d', 'flows', 'XLF 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xlf_20d', 'flows', 'XLF 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xlf_accel', 'flows', 'XLF flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xle_5d', 'flows', 'XLE 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xle_20d', 'flows', 'XLE 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xle_accel', 'flows', 'XLE flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xlv_5d', 'flows', 'XLV 5-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 5d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xlv_20d', 'flows', 'XLV 20-day rolling dollar volume flow proxy',
   'ETF daily close*volume rolling 20d sum', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE),
  ('etf_flow_xlv_accel', 'flows', 'XLV flow acceleration (5d change of 5d flow)',
   'diff(5d) of ETF 5d rolling flow', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2020-01-02', FALSE)
ON CONFLICT (name) DO NOTHING
"""

_ETF_TICKERS = ("spy", "qqq", "iwm", "tlt", "hyg", "gld", "eem", "xlk", "xlf", "xle", "xlv")

# Every name the statement above inserts, in statement order. Used to report
# what the revision changed and by tests/test_ghost_features_registration.py.
REGISTERED_FEATURES: tuple[str, ...] = (
    "eurozone_hicp_yoy",
    "ecb_bank_lending_yoy",
    "oi_k_shape_ratio",
    "overnight_reverse_repo",
    "fed_net_liquidity",
    "fed_net_liquidity_change_1w",
    "fed_net_liquidity_change_1m",
    "reverse_repo_pct_of_peak",
    "tga_drawdown",
    "vix3m_spot",
    "vix9d_spot",
    "implied_correlation_3m",
    "bls_jolts_openings",
    "yc_2y",
    *(f"etf_flow_{t}_{s}" for t in _ETF_TICKERS for s in ("5d", "20d", "accel")),
)


def _registered(conn) -> int:
    """How many of REGISTERED_FEATURES feature_registry currently holds."""
    return conn.execute(
        text("SELECT count(*) FROM feature_registry WHERE name = ANY(:names)"),
        {"names": list(REGISTERED_FEATURES)},
    ).scalar()


def upgrade() -> None:
    """Insert the rows, then say how many this database actually needed."""
    conn = op.get_bind()
    before = _registered(conn)

    op.execute(GHOST_FEATURES)

    after = _registered(conn)
    log.warning(
        "feature_registry ghost features: %d of %d present before, %d after "
        "(%d inserted). A read of griddb on 2026-11-04 found all %d absent.",
        before, len(REGISTERED_FEATURES), after, after - before,
        len(REGISTERED_FEATURES),
    )


def downgrade() -> None:
    """Refuse: a downgrade cannot know which rows this revision created.

    ``ON CONFLICT DO NOTHING`` means any of these names may have been
    registered before the upgrade ran (by hand, or by
    ``EntityMap.ensure_canonical_feature``), and once the resolver writes
    ``resolved_series`` rows against them the ``feature_id`` foreign key makes
    a delete either fail or require destroying observations.
    ``merge_yf_finra_20261023`` refuses for the same reason.
    """
    raise RuntimeError(
        "Downgrade through ghost_features_20261104 is refused: it cannot tell "
        "rows it inserted from rows that already existed, and resolved_series "
        "references them. Use a separately reviewed forward migration."
    )
