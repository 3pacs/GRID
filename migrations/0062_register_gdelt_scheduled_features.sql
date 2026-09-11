-- Migration: 0062_register_gdelt_scheduled_features.sql
-- Author: slice 17 (stepdad news card / GDELT)
-- Applies via: sudo -u postgres psql griddb -f migrations/0062_register_gdelt_scheduled_features.sql
--
-- Purpose: register the 14 GDELT features that
--          ingestion/altdata/gdelt.py::GDELTPuller.pull_recent actually
--          writes every scheduled cycle (it is registered in
--          ingestion/scheduler.py and confirmed live on griddb with rows
--          through 2026-09-09/11) but that were never inserted into
--          feature_registry. Without a feature_registry row the resolver
--          (normalization/resolver.py::Resolver.resolve_pending) silently
--          drops every raw_series observation for them — entity_map.py
--          already had self-mappings for these series_ids (or gets them in
--          this same change), but EntityMap.get_feature_id() returns None
--          when the mapped feature name has no feature_registry row, so
--          resolved_series never received a single one of these rows.
--
--          Live read of griddb (2026-09-11) showed the physics/momentum.py
--          endpoint behind the stepdad.finance "news" home-page card was
--          instead querying two feature names (gdelt_tone_usa,
--          gdelt_conflict_global) that exist in neither feature_registry
--          nor any writer's output on this database — orphaned from an
--          earlier design. physics/momentum.py now reads the features this
--          migration registers.
--
--          A separate, already-registered set of 12-13 canonical GDELT
--          features (gdelt_avg_tone, gdelt_conflict_count, ...) exists from
--          scripts/parse_gdelt.py, a bulk CSV loader that writes directly to
--          resolved_series and is not wired into ingestion/scheduler.py —
--          its resolved_series rows stopped in 2026-03/04. That pipeline is
--          out of scope here (see the PR's "Left out" section); this
--          migration only registers the features the *scheduled* puller is
--          producing today.
--
-- Populated by: ingestion/altdata/gdelt.py::GDELTPuller.pull_recent
--               (via normalization/resolver.py, once entity_map.py maps
--               these series_ids — see the accompanying entity_map.py change)
-- Consumed by:  physics/momentum.py::NewsMomentumAnalyzer
--
-- Idempotent: ON CONFLICT (name) DO NOTHING, matching ingestion/seed_v2.py.
-- No new table/sequence, so no GRANT footer is required (grid already has
-- privileges on feature_registry).

-- ====== DATA ======

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
ON CONFLICT (name) DO NOTHING;
