"""God view G6: replace the market_god_view_daily matview with a point-in-time plain view.

Revision ID: godview_view_v2_20260927
Revises: resolved_retractions_20260927

Slice G6 of the god-view materialization plan
(GRID-GODVIEW-MATERIALIZATION-PLAN-20260926.md, section 4 and slice G6).
Owner approval is required to merge and apply (it is a migration).

Why a plain view, not a refreshed matview
-----------------------------------------
The legacy matview has no refresher and no consumer (live OpenAPI has no
god-view route), no unique index (so ``REFRESH ... CONCURRENTLY`` fails), and
no provenance filter: it serves the incident rows (fabricated WALCL
constants, CFTC z-scores computed over series that switch instrument week to
week, GEX flips at 0.5 and 300). It also joins on the *observation* date, so
Tuesday's COT report appears on Tuesday, three days before it was published.
The inputs are three small indexed tables, so a 366-day spine with six
LATERAL index lookups per day costs milliseconds. A plain view cannot go
stale, which is the failure being fixed; a matview would need a refresher
that is itself a new thing to keep alive.

What the view is
----------------
* One row per America/New_York calendar day, today and the 365 days before.
* ``known_before``: the end of that ET day (midnight ET of the next day),
  capped at ``now()``. A pillar row joins a day only when
  ``provenance IS NOT NULL`` and both ``release_at`` and ``available_at`` are
  before ``known_before``. So a row appears on the first day it was actually
  acquired, never on its observation date.
* Legacy rows (``provenance IS NULL``) never appear.
* For each pillar: the values, plus ``<p>_obs_date`` / ``<p>_report_date``,
  ``<p>_release_at``, ``<p>_available_at``, ``<p>_basis``,
  ``<p>_provenance`` and ``<p>_stale``. ``<p>_stale`` is NULL when the pillar
  has no row, so "no data" is never read as "fresh". Consumers that want
  only observed acquisitions filter ``<p>_basis = 'observed_acquisition'``.
* Pillars in v1:
  - Fed: net liquidity, 1-week and 4-week deltas, RRP, RRP % of peak, regime.
    Stale when more than 9 days older than the day.
  - CFTC ES, ZN, GC, CL: 3-year z-score, 3-year percentile, crowding regime.
    Stale when released more than 10 days before ``known_before``.
  - GEX SPY (``provenance = 'modeled'``): aggregate, normalized, gamma flip,
    regime, model basis. Stale when older than the previous weekday. That
    rule ignores NYSE holidays, so it can flag stale a day early after a
    holiday; the API (``godview/read_model.py``) uses the NYSE calendar.
* Dropped from the legacy definition: sp500/nasdaq/vix from briefing JSON
  (unknown price basis; the price routes serve prices), Cushing, copper and
  buyback columns (fabricated or unsourced pillars), insider buy/sell counts
  (need a filing-date PIT basis). ``market_briefings`` is no longer read.

Consumers
---------
Nothing reads this relation today. The G8 API reads the pillar tables
directly and does not depend on this migration.

Locks
-----
``DROP MATERIALIZED VIEW`` takes ACCESS EXCLUSIVE on the matview only.
``CREATE VIEW`` records dependencies on the three pillar tables without
rewriting them. ``SET LOCAL lock_timeout = '5s'`` fails the upgrade fast (and
rolls it back whole) instead of queueing behind a long reader. The drop is
not CASCADE: if anything depends on the matview, the upgrade fails.

Downgrade
---------
Drops the view and recreates the legacy matview exactly as
``god_view_market_tables_20260918`` defined it (populated, with
``idx_god_view_as_of``). That needs ``market_briefings``, ``insider_trades``,
``commodity_warehouse_inventories`` and ``corporate_buyback_blackouts``,
which the earlier revision created.
"""

from alembic import op

revision = "godview_view_v2_20260927"
down_revision = "godview_writers_20260926"
branch_labels = None
depends_on = None

_LOCK_TIMEOUT = "5s"
_STATEMENT_TIMEOUT = "30s"

VIEW_NAME = "market_god_view_daily"
FED_STALE_AFTER_DAYS = 9
CFTC_STALE_AFTER_DAYS = 10
CFTC_VIEW_ROOTS = ("ES", "ZN", "GC", "CL")


def _set_timeouts() -> None:
    # Literals, not f-strings (repo SQL rule); the tests assert they match
    # _LOCK_TIMEOUT / _STATEMENT_TIMEOUT.
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute("SET LOCAL statement_timeout = '30s'")


_CREATE_VIEW_SQL = """
CREATE VIEW market_god_view_daily AS
WITH spine AS (
    SELECT
        d::date AS as_of_date,
        LEAST(((d::date + 1)::timestamp AT TIME ZONE 'America/New_York'), now()) AS known_before
    FROM generate_series(
        ((now() AT TIME ZONE 'America/New_York')::date - 365)::timestamp,
        (now() AT TIME ZONE 'America/New_York')::date::timestamp,
        interval '1 day'
    ) AS d
)
SELECT
    s.as_of_date,
    s.known_before,

    fed.obs_date                 AS fed_obs_date,
    fed.net_liquidity_usd_m      AS fed_net_liquidity_usd_m,
    fed.delta_1w_m               AS fed_delta_1w_m,
    fed.delta_4w_m               AS fed_delta_4w_m,
    fed.reverse_repo_rrp         AS fed_rrp_usd_m,
    fed.rrp_as_pct_of_peak       AS fed_rrp_as_pct_of_peak,
    fed.liquidity_regime         AS fed_liquidity_regime,
    fed.release_at               AS fed_release_at,
    fed.available_at             AS fed_available_at,
    fed.availability_basis       AS fed_basis,
    fed.provenance               AS fed_provenance,
    CASE WHEN fed.obs_date IS NULL THEN NULL
         ELSE (s.as_of_date - fed.obs_date) > 9 END AS fed_stale,

    es.report_date               AS es_report_date,
    es.z_score_3y                AS es_z_score_3y,
    es.percentile_3y             AS es_percentile_3y,
    es.crowding_regime           AS es_crowding_regime,
    es.release_at                AS es_release_at,
    es.available_at              AS es_available_at,
    es.availability_basis        AS es_basis,
    es.provenance                AS es_provenance,
    CASE WHEN es.report_date IS NULL THEN NULL
         ELSE (s.known_before - es.release_at) > interval '10 days' END AS es_stale,

    zn.report_date               AS zn_report_date,
    zn.z_score_3y                AS zn_z_score_3y,
    zn.percentile_3y             AS zn_percentile_3y,
    zn.crowding_regime           AS zn_crowding_regime,
    zn.release_at                AS zn_release_at,
    zn.available_at              AS zn_available_at,
    zn.availability_basis        AS zn_basis,
    zn.provenance                AS zn_provenance,
    CASE WHEN zn.report_date IS NULL THEN NULL
         ELSE (s.known_before - zn.release_at) > interval '10 days' END AS zn_stale,

    gc.report_date               AS gc_report_date,
    gc.z_score_3y                AS gc_z_score_3y,
    gc.percentile_3y             AS gc_percentile_3y,
    gc.crowding_regime           AS gc_crowding_regime,
    gc.release_at                AS gc_release_at,
    gc.available_at              AS gc_available_at,
    gc.availability_basis        AS gc_basis,
    gc.provenance                AS gc_provenance,
    CASE WHEN gc.report_date IS NULL THEN NULL
         ELSE (s.known_before - gc.release_at) > interval '10 days' END AS gc_stale,

    cl.report_date               AS cl_report_date,
    cl.z_score_3y                AS cl_z_score_3y,
    cl.percentile_3y             AS cl_percentile_3y,
    cl.crowding_regime           AS cl_crowding_regime,
    cl.release_at                AS cl_release_at,
    cl.available_at              AS cl_available_at,
    cl.availability_basis        AS cl_basis,
    cl.provenance                AS cl_provenance,
    CASE WHEN cl.report_date IS NULL THEN NULL
         ELSE (s.known_before - cl.release_at) > interval '10 days' END AS cl_stale,

    gex.obs_date                 AS spy_gex_obs_date,
    gex.gex_aggregate            AS spy_gex_aggregate,
    gex.gex_normalized           AS spy_gex_normalized,
    gex.gamma_flip               AS spy_gamma_flip,
    gex.regime                   AS spy_gex_regime,
    gex.model_basis              AS spy_gex_model_basis,
    gex.release_at               AS spy_gex_release_at,
    gex.available_at             AS spy_gex_available_at,
    gex.availability_basis       AS spy_gex_basis,
    gex.provenance               AS spy_gex_provenance,
    CASE WHEN gex.obs_date IS NULL THEN NULL
         ELSE gex.obs_date < s.as_of_date - (CASE EXTRACT(ISODOW FROM s.as_of_date)
                                                  WHEN 1 THEN 3 WHEN 7 THEN 2 ELSE 1 END)::int
    END AS spy_gex_stale
FROM spine s
LEFT JOIN LATERAL (
    SELECT f.obs_date, f.net_liquidity_usd_m, f.delta_1w_m, f.delta_4w_m, f.reverse_repo_rrp,
           f.rrp_as_pct_of_peak, f.liquidity_regime, f.release_at, f.available_at,
           f.availability_basis, f.provenance
    FROM fed_net_liquidity_daily f
    WHERE f.provenance IS NOT NULL
      AND f.release_at < s.known_before AND f.available_at < s.known_before
    ORDER BY f.obs_date DESC
    LIMIT 1
) fed ON TRUE
LEFT JOIN LATERAL (
    SELECT c.report_date, c.z_score_3y, c.percentile_3y, c.crowding_regime, c.release_at,
           c.available_at, c.availability_basis, c.provenance
    FROM cftc_positioning_daily c
    WHERE c.contract_code = 'ES' AND c.provenance IS NOT NULL
      AND c.release_at < s.known_before AND c.available_at < s.known_before
    ORDER BY c.report_date DESC
    LIMIT 1
) es ON TRUE
LEFT JOIN LATERAL (
    SELECT c.report_date, c.z_score_3y, c.percentile_3y, c.crowding_regime, c.release_at,
           c.available_at, c.availability_basis, c.provenance
    FROM cftc_positioning_daily c
    WHERE c.contract_code = 'ZN' AND c.provenance IS NOT NULL
      AND c.release_at < s.known_before AND c.available_at < s.known_before
    ORDER BY c.report_date DESC
    LIMIT 1
) zn ON TRUE
LEFT JOIN LATERAL (
    SELECT c.report_date, c.z_score_3y, c.percentile_3y, c.crowding_regime, c.release_at,
           c.available_at, c.availability_basis, c.provenance
    FROM cftc_positioning_daily c
    WHERE c.contract_code = 'GC' AND c.provenance IS NOT NULL
      AND c.release_at < s.known_before AND c.available_at < s.known_before
    ORDER BY c.report_date DESC
    LIMIT 1
) gc ON TRUE
LEFT JOIN LATERAL (
    SELECT c.report_date, c.z_score_3y, c.percentile_3y, c.crowding_regime, c.release_at,
           c.available_at, c.availability_basis, c.provenance
    FROM cftc_positioning_daily c
    WHERE c.contract_code = 'CL' AND c.provenance IS NOT NULL
      AND c.release_at < s.known_before AND c.available_at < s.known_before
    ORDER BY c.report_date DESC
    LIMIT 1
) cl ON TRUE
LEFT JOIN LATERAL (
    SELECT g.obs_date, g.gex_aggregate, g.gex_normalized, g.gamma_flip, g.regime, g.model_basis,
           g.release_at, g.available_at, g.availability_basis, g.provenance
    FROM dealer_gex_daily g
    WHERE g.ticker = 'SPY' AND g.provenance IS NOT NULL
      AND g.release_at < s.known_before AND g.available_at < s.known_before
    ORDER BY g.obs_date DESC
    LIMIT 1
) gex ON TRUE
"""

# The legacy definition, verbatim from god_view_market_tables_20260918, for
# the downgrade. The doubled %% match how that revision passed the ILIKE
# patterns through op.execute (they reach PostgreSQL as single %).
_LEGACY_MATVIEW_SQL = """
CREATE MATERIALIZED VIEW IF NOT EXISTS market_god_view_daily AS
SELECT DISTINCT ON (m.briefing_date)
    m.briefing_date AS as_of_date,
    (m.snapshot_data->'equities'->'^GSPC'->>'value')::DOUBLE PRECISION AS sp500_close,
    (m.snapshot_data->'equities'->'^IXIC'->>'value')::DOUBLE PRECISION AS nasdaq_close,
    (m.snapshot_data->'volatility'->'^VIX'->>'value')::DOUBLE PRECISION AS vix_spot,
    (m.snapshot_data->'volatility'->'^VIX3M'->>'value')::DOUBLE PRECISION AS vix_3m,
    liq.net_liquidity_usd_m,
    liq.delta_30d_m AS net_liq_30d_delta,
    liq.rrp_as_pct_of_peak,
    cot_es.z_score_3y AS es_spec_zscore,
    cot_zn.z_score_3y AS zn_spec_zscore,
    cot_gc.z_score_3y AS gold_spec_zscore,
    cot_cl.z_score_3y AS oil_spec_zscore,
    oil_hub.total_inventory AS cushing_crude_m_bbl,
    oil_hub.floor_buffer_pct AS cushing_floor_buffer,
    cu_wh.canceled_ratio AS copper_canceled_warrant_pct,
    bb.sp500_cap_blackout_pct,
    bb.active_corporate_bid_m,
    ins.buy_count AS insider_buys,
    ins.sell_count AS insider_sells,
    gex_spy.net_gex_usd_m AS spy_net_gex,
    gex_spy.gamma_flip_strike AS spy_gamma_flip
FROM market_briefings m
LEFT JOIN LATERAL (
    SELECT net_liquidity_usd_m, delta_30d_m, rrp_as_pct_of_peak FROM fed_net_liquidity_daily
    WHERE obs_date <= m.briefing_date ORDER BY obs_date DESC LIMIT 1
) liq ON TRUE
LEFT JOIN LATERAL (
    SELECT z_score_3y FROM cftc_positioning_daily
    WHERE contract_code = 'ES' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
) cot_es ON TRUE
LEFT JOIN LATERAL (
    SELECT z_score_3y FROM cftc_positioning_daily
    WHERE contract_code = 'ZN' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
) cot_zn ON TRUE
LEFT JOIN LATERAL (
    SELECT z_score_3y FROM cftc_positioning_daily
    WHERE contract_code = 'GC' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
) cot_gc ON TRUE
LEFT JOIN LATERAL (
    SELECT z_score_3y FROM cftc_positioning_daily
    WHERE contract_code = 'CL' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
) cot_cl ON TRUE
LEFT JOIN LATERAL (
    SELECT total_inventory, floor_buffer_pct FROM commodity_warehouse_inventories
    WHERE location_hub = 'CUSHING_OK' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
) oil_hub ON TRUE
LEFT JOIN LATERAL (
    SELECT canceled_ratio FROM commodity_warehouse_inventories
    WHERE commodity = 'COPPER' AND report_date <= m.briefing_date ORDER BY report_date DESC LIMIT 1
) cu_wh ON TRUE
LEFT JOIN LATERAL (
    SELECT sp500_cap_blackout_pct, active_corporate_bid_m FROM corporate_buyback_blackouts
    WHERE calendar_date <= m.briefing_date ORDER BY calendar_date DESC LIMIT 1
) bb ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE trade_type ILIKE '%%BUY%%') AS buy_count,
        COUNT(*) FILTER (WHERE trade_type ILIKE '%%SELL%%') AS sell_count
    FROM insider_trades
    WHERE trade_date <= m.briefing_date AND trade_date >= m.briefing_date - INTERVAL '7 days'
) ins ON TRUE
LEFT JOIN LATERAL (
    SELECT net_gex_usd_m, gamma_flip_strike FROM dealer_gex_daily
    WHERE ticker = 'SPY' AND obs_date <= m.briefing_date ORDER BY obs_date DESC LIMIT 1
) gex_spy ON TRUE
WHERE m.briefing_type = 'hourly'
ORDER BY m.briefing_date DESC, m.id DESC;

CREATE INDEX IF NOT EXISTS idx_god_view_as_of ON market_god_view_daily (as_of_date DESC);
"""


def _grant_select() -> None:
    # GRANT footer (migrations/_TEMPLATE.sql). Production runs alembic as
    # `grid`, which will own the view; this covers a deploy under another role.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grid') THEN
                EXECUTE 'GRANT SELECT ON market_god_view_daily TO grid';
            END IF;
        END
        $$
    """)


def upgrade() -> None:
    _set_timeouts()
    # Drop the legacy matview (not CASCADE: fail if anything depends on it).
    # If a plain view of the same name already exists (a re-run), replace it.
    op.execute("""
        DO $$
        DECLARE
            kind "char";
        BEGIN
            SELECT c.relkind INTO kind
            FROM pg_class c
            WHERE c.oid = to_regclass('market_god_view_daily');
            IF kind = 'm' THEN
                EXECUTE 'DROP MATERIALIZED VIEW market_god_view_daily';
            ELSIF kind = 'v' THEN
                EXECUTE 'DROP VIEW market_god_view_daily';
            ELSIF kind IS NOT NULL THEN
                RAISE EXCEPTION USING MESSAGE =
                    'market_god_view_daily exists with unexpected relkind ' || kind;
            END IF;
        END
        $$
    """)
    op.execute(_CREATE_VIEW_SQL)
    op.execute("""
        COMMENT ON VIEW market_god_view_daily IS
        'God view G6: point-in-time daily spine over the provenance rows of fed_net_liquidity_daily, cftc_positioning_daily (ES/ZN/GC/CL) and dealer_gex_daily (SPY, modeled). A row joins a day only once release_at and available_at are before known_before. Legacy NULL-provenance rows never appear.'
    """)
    _grant_select()


def downgrade() -> None:
    _set_timeouts()
    op.execute("DROP VIEW IF EXISTS market_god_view_daily")
    op.execute(_LEGACY_MATVIEW_SQL)
    _grant_select()
