# Fresh-install claims, reconciled (pass 5, 2026-09-18)

**Headline: no run in passes 3–4 proves a tracked-migrations-only fresh install. Every "applied from an empty database" result was a PREPARED-database proof. Problem B stays open.**

## 1. Exactly how each "empty database" was bootstrapped before `alembic upgrade head`

Order used in run 1 (composition 6e8df878), run 4 (v6 0f0451aa) and the combined-graph proof (b534b32b), identical each time:

| Step | Command / helper | What it does | Tracked by Alembic? |
|---|---|---|---|
| 0 | `CREATE DATABASE griddb_fable_<stamp> OWNER fable_test_<stamp>` | truly empty database (0 tables in `public`) | — |
| 1 | `python -c "import db; db.apply_schema()"` | executes `schema.sql` (the tree's full snapshot DDL, ~107 KB) via psycopg2 using `DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD` | no — it is the pre-Alembic baseline snapshot |
| 2 | `python -m alembic stamp 7e4dfecce247` | marks revision `7e4dfecce247` ("baseline schema from schema.sql", the chain's first revision after `<base>`) as applied without running anything | stamp only |
| 3a | `intelligence.hypothesis_engine.ensure_tables(engine)` | creates `discovered_hypotheses` (+ related) | **no** (runtime lazy creator) |
| 3b | `store.snapshots.ensure_analytical_snapshots_table(engine)` | creates `analytical_snapshots` | **no** |
| 3c | `intelligence.actors.db._ensure_tables(engine)` | creates `actors` / `wealth_flows` if absent — `actors` is in fact already created by `schema.sql`, so this call was redundant for `actors`; it may still create `wealth_flows` | `actors`: yes via schema.sql; the rest: no |
| 3d | `NewsScraperPuller._ensure_news_table()` (on a `__new__` instance to skip a `source_catalog` seed bug) | creates `news_articles` | **no** |
| 3e | verbatim `CREATE TABLE IF NOT EXISTS market_briefings …` + two indexes, lines 598–612 of `ollama/market_briefing.py` | creates `market_briefings` | **no** |
| 4 | `python -m alembic upgrade head` | walks `7e4dfecce247 → … → research_leases_0918` (32 revisions on v6; 38 on the combined tree) | yes |

Without steps 3a–3e, step 4 fails at `phase4_fts_001` (`phase4_fts_intelligence_search.py`, verbatim exception recorded in run 1: `relation "discovered_hypotheses" does not exist`). Static confirmation on b534b32b: the tracked revisions that reference runtime-only tables are `phase4_fts_001` (discovered_hypotheses, analytical_snapshots), `phase4_fts_002` (news_articles), `reapply_snapshot_fts_20260911`, `restore_news_search_arm_20260912`, `snapshot_actor_index_20260912`, `snapshot_actor_col_20260914` (analytical_snapshots), `idle_fleet_goal_queue_day1` (discovered_hypotheses), `god_view_market_tables_20260918` (market_briefings). `schema.sql` creates none of `discovered_hypotheses`, `analytical_snapshots`, `news_articles`, `market_briefings`.

## 2. Why CI does not see this

#560's CI bootstrap is `schema.sql → alembic stamp snapshot_actor_col_20260914 → upgrade head`: it stamps PAST every revision listed above except `god_view_market_tables_20260918`, whose `market_briefings` dependency #558/#560 covered by adding that table's DDL to `schema.sql`. So CI exercises only the tail of the chain and never the early revisions.

## 3. What each proof does and does not establish

- **Prepared-database proof (runs 1, 4, combined):** with the four runtime creators applied, the tracked chain applies, round-trips and passes the DB-gated suites. Establishes: the new revisions are correct and ordered; the combined packet-2 + Fable graph is linear and applicable. Does NOT establish: a fresh install from `schema.sql` + tracked migrations alone.
- **Tracked-migrations-only fresh install:** FAILS (run 1, reproduced unchanged in run 4 by design — the creators were applied deliberately because the failure is known). This is problem B; acceptance is `schema.sql → alembic upgrade head` from empty with zero manual creators, and a no-op on a production-stamped database.

## 4. Ownership (unchanged)

Problem B is scoped (`PROBLEM-B-SCOPE.md`) and unassigned; `schema.sql` is claimed by the data-integrity lane. The four missing creators are the candidate DDL either for `schema.sql` (merged precedent) or for an idempotent prerequisites revision placed at the current tail; that choice is the owner's. Fable will not implement B without an assignment.
