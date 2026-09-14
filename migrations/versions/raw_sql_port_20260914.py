"""Register the feature_registry rows two raw .sql migrations never delivered.

Revision ID: raw_sql_port_20260914
Revises: snapshot_actor_index_20260912
Create Date: 2026-09-14 05:50:00.000000

``migrations/0062_register_gdelt_scheduled_features.sql`` and
``migrations/0063_register_ghost_mapped_features.sql`` were written, reviewed
and merged, and then never ran. Nothing runs them. ``deploy.yml`` executes
``python3 -m alembic upgrade head`` and nothing else, while every
``migrations/*.sql`` file in this tree is applied -- when it is applied at
all -- by a person typing ``sudo -u postgres psql griddb -f …``. The
2026-09-11 TODO for 0062 said exactly that: "Anik: apply it by hand after
#452 merges". A migration mechanism whose apply step is a human remembering
is the defect; the empty news card on grid.stepdad.finance was one symptom
of it.

Confirmed outstanding, not assumed
----------------------------------
Read-only against griddb (ops-exec runs 308/309, 2026-09-14). Every one of
the 14 names 0062 registers and the 9 names 0063 registers is absent from
``feature_registry``; the only ``gdelt_*`` rows present are the 13 canonical
ones from ``scripts/parse_gdelt.py``'s bulk CSV loader
(``gdelt_avg_tone``, ``gdelt_conflict_count``, ``gdelt_events_*``, …), a
different and unscheduled pipeline. ``migrations/RAW_SQL_LEDGER.md`` carries
the full audit of all 52 raw files; only what it proved outstanding is
ported, because porting an already-applied migration that is not idempotent
fails a deploy.

Why this is the whole fix for the news card
-------------------------------------------
``ingestion/altdata/gdelt.py::GDELTPuller.pull_recent`` is scheduled in
``ingestion/scheduler.py`` and writes these 14 series to ``raw_series``
every cycle, and ``normalization/entity_map.py`` self-maps each series_id
onto the identically-named feature. But ``EntityMap.get_feature_id()``
resolves that name against ``feature_registry`` and returns ``None`` when
there is no row, so ``normalization/resolver.py`` has dropped every one of
those observations. With the rows in place the resolver can write them and
``physics/momentum.py::NewsMomentumAnalyzer`` stops answering
``available: false``.

Idempotent by construction
--------------------------
``ON CONFLICT (name) DO NOTHING``, exactly as the two .sql files and
``ingestion/seed_v2.py`` write it. On any host where someone did run the raw
file by hand this inserts nothing and still records as applied. That
property is what makes porting safe at all.

Column values are not free text: ``feature_registry`` CHECKs them
(``normalization`` in ZSCORE/MINMAX/RAW/RANK, ``missing_data_policy`` in
FORWARD_FILL/INTERPOLATE/NAN, ``family`` in a fixed set,
``transformation_version >= 1``). 0063 originally shipped
``normalization='NONE'``, which psql rejected, so that migration registered
0 of 9 even on the one occasion it was run. The literals below are carried
over verbatim from the corrected files and
``tests/test_migration_feature_registry_values.py`` -- extended in this
change to read ``migrations/versions/*.py`` as well as ``migrations/*.sql``
-- checks them.

No GRANT footer
---------------
``migrations/_TEMPLATE.sql``'s footer exists because a table created by the
``postgres`` superuser is unreadable to the unprivileged ``grid`` role. This
revision creates no table and no sequence; it inserts into
``feature_registry``, on which ``grid`` already holds privileges. 0062 and
0063 say the same in their own headers.

No size gate
------------
``migrations/versions/snapshot_actor_index_20260912.py`` gates on
``pg_total_relation_size`` because a build on a 1155 MB table cannot finish
under ``db.py``'s 120 s ``statement_timeout``. Nothing here needs that:
``feature_registry`` is a few hundred rows and this writes at most 23 of
them, with no index build and no scan of anything. The raw migration that
*would* need the gate -- ``0047_system_health_indexes.sql``, two indexes on
the 511 GB / 1.93e9-row ``raw_series`` -- is already applied on griddb and
is not ported. The ledger records that.
"""

import logging
from typing import Sequence, Union

from alembic import op
from sqlalchemy import text


# revision identifiers, used by Alembic.
# 21 characters: alembic_version.version_num is VARCHAR(32) and a 37-char id
# failed deploy 640 on the UPDATE *after* its upgrade() had already run.
# tests/test_alembic_single_head.py guards the ceiling.
revision: str = "raw_sql_port_20260914"
down_revision: Union[str, Sequence[str], None] = "snapshot_actor_index_20260912"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

log = logging.getLogger("alembic.runtime.migration")

# Verbatim from migrations/0062_register_gdelt_scheduled_features.sql. The 14
# names match ingestion/altdata/gdelt.py's GDELT_ACTOR_QUERIES and
# GDELT_TENSION_PAIRS exactly -- the puller's own "feature" keys -- and the
# same 14 are the lists physics/momentum.py reads.
GDELT_SCHEDULED_FEATURES = """
INSERT INTO feature_registry
  (name, family, description, transformation,
   transformation_version, lag_days, normalization, missing_data_policy,
   eligible_from_date, model_eligible)
VALUES
  -- Named-actor media tone (GDELT DOC API, timelinetone, 30d window)
  ('gdelt_actor_powell_tone',  'sentiment', 'GDELT media tone around Jerome Powell / Federal Reserve coverage',
   'GDELT DOC API timelinetone', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_actor_lagarde_tone', 'sentiment', 'GDELT media tone around Christine Lagarde / ECB coverage',
   'GDELT DOC API timelinetone', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_actor_xi_tone',      'sentiment', 'GDELT media tone around Xi Jinping / China coverage',
   'GDELT DOC API timelinetone', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_actor_putin_tone',   'sentiment', 'GDELT media tone around Vladimir Putin / Russia coverage',
   'GDELT DOC API timelinetone', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_actor_mbs_tone',     'sentiment', 'GDELT media tone around Mohammed bin Salman / Saudi Arabia coverage',
   'GDELT DOC API timelinetone', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_actor_yellen_tone',  'sentiment', 'GDELT media tone around Janet Yellen / Treasury coverage',
   'GDELT DOC API timelinetone', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_actor_ueda_tone',    'sentiment', 'GDELT media tone around Kazuo Ueda / BOJ coverage',
   'GDELT DOC API timelinetone', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),

  -- Bilateral country-pair tension (negative tone volume, inverted sign)
  ('gdelt_tension_us_china',       'sentiment', 'GDELT bilateral tension score, United States-China (trade war coverage)',
   'GDELT DOC API timelinetone, negated', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_tension_us_russia',      'sentiment', 'GDELT bilateral tension score, United States-Russia (sanctions coverage)',
   'GDELT DOC API timelinetone, negated', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_tension_us_iran',        'sentiment', 'GDELT bilateral tension score, United States-Iran (oil sanctions coverage)',
   'GDELT DOC API timelinetone, negated', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_tension_china_taiwan',   'sentiment', 'GDELT bilateral tension score, China-Taiwan (strait crisis coverage)',
   'GDELT DOC API timelinetone, negated', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_tension_russia_ukraine', 'sentiment', 'GDELT bilateral tension score, Russia-Ukraine (war coverage)',
   'GDELT DOC API timelinetone, negated', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_tension_israel_iran',    'sentiment', 'GDELT bilateral tension score, Israel-Iran (mideast coverage)',
   'GDELT DOC API timelinetone, negated', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('gdelt_tension_india_china',    'sentiment', 'GDELT bilateral tension score, India-China (border coverage)',
   'GDELT DOC API timelinetone, negated', 1, 0, 'ZSCORE', 'FORWARD_FILL', '2026-01-01', TRUE)
ON CONFLICT (name) DO NOTHING
"""

# Verbatim from migrations/0063_register_ghost_mapped_features.sql. "Ghost
# mappings": entity_map.py already maps raw series onto these names, so they
# never appear in any count of UNMAPPED series -- they look wired and produce
# nothing, because get_feature_id() finds no registry row behind the mapping.
GHOST_MAPPED_FEATURES = """
INSERT INTO feature_registry
  (name, family, description, transformation,
   transformation_version, lag_days, normalization, missing_data_policy,
   eligible_from_date, model_eligible)
VALUES
  -- FX: the largest ghost by row volume (339 rows / 30 d on 2026-09-11)
  ('eurusd_ecb_daily', 'fx', 'EUR/USD daily reference rate published by the ECB',
   'RAW', 1, 0, 'RAW', 'FORWARD_FILL', '2026-01-01', TRUE),

  -- Bond ETF closes: mapped by the NEW_MAPPINGS_V2 comprehension over
  -- close and adj_close, target *_full, never registered.
  ('shy_full', 'credit', 'iShares 1-3 Year Treasury Bond ETF (SHY) close',
   'RAW', 1, 0, 'RAW', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('ief_full', 'credit', 'iShares 7-10 Year Treasury Bond ETF (IEF) close',
   'RAW', 1, 0, 'RAW', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('emb_full', 'credit', 'iShares JP Morgan USD Emerging Markets Bond ETF (EMB) close',
   'RAW', 1, 0, 'RAW', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('jnk_full', 'credit', 'SPDR Bloomberg High Yield Bond ETF (JNK) close',
   'RAW', 1, 0, 'RAW', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('mub_full', 'credit', 'iShares National Muni Bond ETF (MUB) close',
   'RAW', 1, 0, 'RAW', 'FORWARD_FILL', '2026-01-01', TRUE),

  -- Sector/theme ETF closes: mapped in SEED_MAPPINGS, never registered.
  ('smh_close', 'equity', 'VanEck Semiconductor ETF (SMH) close',
   'RAW', 1, 0, 'RAW', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('icln_close', 'equity', 'iShares Global Clean Energy ETF (ICLN) close',
   'RAW', 1, 0, 'RAW', 'FORWARD_FILL', '2026-01-01', TRUE),
  ('lit_close', 'commodity', 'Global X Lithium & Battery Tech ETF (LIT) close',
   'RAW', 1, 0, 'RAW', 'FORWARD_FILL', '2026-01-01', TRUE)
ON CONFLICT (name) DO NOTHING
"""

# Every name the two statements above insert, in order. Used only to report
# what the revision actually changed.
PORTED_FEATURES = (
    "gdelt_actor_powell_tone", "gdelt_actor_lagarde_tone", "gdelt_actor_xi_tone",
    "gdelt_actor_putin_tone", "gdelt_actor_mbs_tone", "gdelt_actor_yellen_tone",
    "gdelt_actor_ueda_tone", "gdelt_tension_us_china", "gdelt_tension_us_russia",
    "gdelt_tension_us_iran", "gdelt_tension_china_taiwan",
    "gdelt_tension_russia_ukraine", "gdelt_tension_israel_iran",
    "gdelt_tension_india_china",
    "eurusd_ecb_daily", "shy_full", "ief_full", "emb_full", "jnk_full",
    "mub_full", "smh_close", "icln_close", "lit_close",
)


def _registered(conn) -> int:
    """How many of PORTED_FEATURES feature_registry currently holds."""
    return conn.execute(
        text("SELECT count(*) FROM feature_registry WHERE name = ANY(:names)"),
        {"names": list(PORTED_FEATURES)},
    ).scalar()


def upgrade() -> None:
    """Insert both sets, then say how many rows this actually added.

    The count is the point. ``ON CONFLICT DO NOTHING`` makes "ran fine" and
    "did nothing" look identical, and that ambiguity is the whole reason this
    revision exists -- 0062 and 0063 both merged looking applied. The
    ``before -> after`` line in the deploy log turns an invisible no-op into a
    stated one.

    ``migrations/logging_setup.py`` is what makes that line visible at all:
    ``import alembic`` installs a NullHandler that otherwise discards every
    record a migration logs.
    """
    conn = op.get_bind()
    before = _registered(conn)

    op.execute(GDELT_SCHEDULED_FEATURES)
    op.execute(GHOST_MAPPED_FEATURES)

    after = _registered(conn)
    log.warning(
        "feature_registry rows from 0062+0063: %d of %d present before, %d "
        "after (%d inserted). 0 inserted means this database already had "
        "them -- someone applied the raw .sql by hand -- which is the "
        "expected outcome everywhere except griddb, where a read on "
        "2026-09-14 found all %d absent.",
        before, len(PORTED_FEATURES), after, after - before, len(PORTED_FEATURES),
    )


def downgrade() -> None:
    """Delete exactly the rows this revision inserts.

    Safe only because ``feature_registry.name`` is unique and these names are
    the migration's own. Any ``resolved_series`` rows the resolver wrote
    against them are left alone: a downgrade that reached into the data would
    be destroying observations to undo a registration.
    """
    op.get_bind().execute(
        text("DELETE FROM feature_registry WHERE name = ANY(:names)"),
        {"names": list(PORTED_FEATURES)},
    )
