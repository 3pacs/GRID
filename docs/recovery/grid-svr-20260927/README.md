# grid-svr recovery — 2026-09-27

Read-only inventory of production code that exists only on `grid-svr`
(`/data/grid_v4/grid_repo`, a symlink to `/home/grid/grid_v4/grid_repo`), taken
without modifying, restarting, or querying anything on that host. Owner
default for 2026-09-27: recover by PR, no production change.

`grid_repo` was at local HEAD `5facbdf0` (a merge of `origin/main` from
2026-09-11, i.e. through PR #391) with ~20 modified tracked files and ~370
untracked files. `origin/main` has since moved to `1bb2f61b` (PR #671,
2026-09-26) — a ~2-week/~280-PR gap, which is why most of the "modified"
files below turned out to be stale relative to current `main` rather than
real unmerged work.

## What was added to the repo (this PR)

Four scripts/modules that four live systemd units run in production, but
that were **never committed** to `origin/main`:

| systemd unit | file (as it runs on grid-svr) | status before this PR |
|---|---|---|
| `grid-gem-outcomes.timer/.service` | `scripts/evaluate_gem_outcomes.py` | untracked — deliberately hidden via `.git/info/exclude` |
| `grid-gem-watchlist-coverage.timer/.service` | `scripts/backfill_smallcap_coverage.py` | untracked — deliberately hidden via `.git/info/exclude` |
| `grid-td-backfill.timer/.service` | `scripts/td_backfill_gem_tickers.py` | untracked — deliberately hidden via `.git/info/exclude` |
| `grid-fleet-heartbeat.timer/.service` | `intelligence/fleet_heartbeat.py` | **absent from `grid_repo` entirely** — the unit's `path-fix.conf` drop-in overrides `WorkingDirectory`/`PYTHONPATH` to `/data/grid_v4/astrogrid_dedup`, a separate, ancient standalone checkout (its own git history, HEAD `3ee27b1b`), and runs the module from there instead |

`grid_repo/.git/info/exclude` has a comment dated 2026-09-03 explaining the
first three were "untracked production scripts restored from
stash@{0}^3 (never committed to 3pacs/GRID; identical copies in
`/data/grid_v4/astrogrid_dedup/scripts/`)" and excluded so `git status` and
a local catchup script leave them alone. That comment also names two more
excluded scripts not in scope for this PR: `scripts/pull_options_gem_tickers.py`
and `scripts/td_backfill_universe.py` (the latter is **already on `main`** —
see credential note below).

`intelligence/fleet_heartbeat.py` has never existed anywhere in this repo's
git history and nothing on `main` references `fleet_heartbeat` or its
`FLEET`/`ENDPOINTS` registries. Its docstring says it was "Created
2026-05-25" — it has been running unreviewed from the legacy
`astrogrid_dedup` tree for four months.

**`alpha_research/heartbeat.py` (already on `main`) is *not* a successor.**
It's an unrelated alpha-research alert job (VIX regime transitions, PIT
freshness, puller health) — nothing to do with fleet/GPU/systemd-unit
monitoring. The two modules only share a name. `intelligence/fleet_heartbeat.py`
has no equivalent anywhere on `main`; this PR is the first time this
capability enters the reviewed codebase.

### Credential fixes applied while porting

Two of the four scripts hardcoded a live plaintext Postgres password. Both
are fixed here using the exact pattern already established and merged for
their sibling script (`scripts/td_backfill_universe.py`, commit `b978ee91`,
"fix(scripts): stop hardcoding a live DB password, commit the script"):
read `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USER`/`DB_PASSWORD` from the process
env as `psycopg2.connect(**kwargs)`, `sys.exit` if `DB_PASSWORD` is unset.

- **`scripts/evaluate_gem_outcomes.py`** — `DB_DSN` built a connection
  string with `f"password={os.getenv('DB_PASSWORD', 'gridmaster2026')}"` —
  a literal password as the *default* if the env var was ever unset.
  Replaced with `_connect_params_from_env()` / `DB_CONNECT_PARAMS`.
- **`scripts/td_backfill_gem_tickers.py`** — `DSN` was a literal constant
  string containing the live password outright, and the Twelve Data API
  key was read by manually parsing a hardcoded, host-specific absolute
  path (`/data/grid_v4/astrogrid_dedup/.env`) instead of the environment.
  Both replaced with env-var reads (`_connect_params_from_env()` /
  `CONNECT_PARAMS`, `os.environ.get("TWELVEDATA_API_KEY", "")`), plus the
  same URL-in-exception redaction helper (`_redact_api_key`) `td_backfill_universe.py`
  uses, since `requests`' exception text can embed the failed URL
  (including the API key query param).
- **`scripts/backfill_smallcap_coverage.py`** and
  **`intelligence/fleet_heartbeat.py`** already read all credentials
  through `config.settings` / `os.environ` with no hardcoded fallback —
  no fix needed.

The literal password value itself is not reproduced anywhere in this repo,
this PR description, or any commit message.

Each of the four ships with a minimal pure-logic smoke/import test
(`tests/test_evaluate_gem_outcomes_smoke.py`,
`tests/test_backfill_smallcap_coverage_smoke.py`,
`tests/test_td_backfill_gem_tickers_smoke.py`,
`tests/test_fleet_heartbeat_smoke.py`) — no live DB, no network, no
subprocess/ssh, no email.

## Incident files inventoried but deliberately NOT added

Per the owner's explicit instruction, the following untracked files are
the fabricated-fallback incident code and are **not** part of this PR —
listed here only so the inventory is complete:

- `api/routers/god_view.py`
- `ingestion/god_view_materializer.py`
- `derivatives/dealer_gex_engine.py`
- `ingestion/altdata/cftc_materializer.py`
- `ingestion/altdata/commodity_warehouse_materializer.py`
- `ingestion/altdata/corporate_buyback_engine.py`
- `ingestion/altdata/fed_liquidity_materializer.py`
- `ingestion/altdata/short_volume_ftd_materializer.py`
- `migrations/versions/god_view_market_tables_20260918.py`

Two of the 20 *tracked* modifications below (`api/main.py`, `db.py`) exist
**only** to wire up this incident code and are classified accordingly —
see the table.

Also present untracked on grid-svr, not part of the incident set but out of
scope for this PR (not one of the four unit-mapped files, no owner
instruction to recover): `migrations/versions/raw_sql_port_20260914.py`,
`migrations/versions/reapply_snapshot_fts_20260911.py`,
`migrations/versions/restore_news_search_arm_20260912.py`,
`migrations/versions/snapshot_actor_col_20260914.py`,
`migrations/versions/snapshot_actor_index_20260912.py`,
`docs/handoffs/2026-09-18/`, `scripts/td_backfill_universe.py.prev-20260917-dryrun-bug`,
`tmp_transfer/`, and ~350 dated `outputs/autoresearch_*.json` /
`outputs/storage_maintenance/storage_maintenance_*.{json,md}` run artifacts.

## The 20 modified tracked files — patches + classification

Every file below has a full `git diff HEAD` patch in `patches/`, taken
read-only from grid-svr. **None have been applied.** They are not proposed
as this PR's changes — they're handed to the owner to decide.
`patches/_real-changes-only-whitespace-ignored.patch` isolates just the
three hunks below that survive `--ignore-space-at-eol --ignore-all-space`
(api/main.py, db.py, phase4_fts_intelligence_search.py) for quick review,
without wading through the CRLF noise in the full per-file patches.

The first pass of this audit diffed each file against grid-svr's own
`HEAD` (`5facbdf0`, 2026-09-11). Re-running each diff with
`--ignore-space-at-eol --ignore-all-space` showed that 14 of the 15
touched migration files, plus most of `api/main.py`/`db.py`'s line count,
are **pure CRLF line-ending churn** — very likely from something on
grid-svr (or a prior Windows-side edit) rewriting line endings on files
that were never otherwise touched. `file(1)` confirms: the working-tree
copy is CRLF, `HEAD`'s blob is LF, byte content is identical once
line-endings are normalized.

| file | patch | real change? | classification |
|---|---|---|---|
| `migrations/versions/phase4_fts_intelligence_search.py` | `migrations_phase4_fts_intelligence_search.py.patch` | **Yes** | **Genuine unmerged fix — recommend porting**, but as a *new* forward-fixing migration, not by editing this already-applied file's history in place (alembic won't re-run a migration that's already stamped, and rewriting an applied migration's source makes fresh-environment history diverge silently from what production actually ran). See detail below. |
| `api/main.py` | `api_main.py.patch` | Yes (1 hunk, 1 line) | **Local hack tied to the incident code — do not port.** Registers `("god_view", "api.routers.god_view", False)` in the router load list. `god_view.py` is explicitly out of scope (fabricated-fallback incident code). |
| `db.py` | `db.py.patch` | Yes (1 hunk, 3 lines) | **Local hack tied to the incident code — do not port.** Adds `get_db_engine = get_engine` purely so `ingestion/god_view_materializer.py` and `derivatives/dealer_gex_engine.py` (both incident code, both `from db import get_db_engine`) would import. `main`'s real `get_db_engine` lives in `api/dependencies.py` and is what every tracked caller (`api/routers/actor_detail.py`, `associations.py`, `astrogrid_celestial.py`, …) actually uses; this alias has zero tracked callers. |
| `docs/PUNCH-LIST-2026-05-13.md` | `docs_PUNCH-LIST-2026-05-13.md.patch` | Yes (cosmetic) | **Local tooling artifact — do not port.** Every hunk either reorders an unchanged checklist line or inserts Obsidian `[[wikilink]]` markup (`[[SQLAlchemy]]`, `[[PIT Store\|PIT-correct]]`, `[[Hermes Scheduler\|Hermes]]`, …) into prose that was plain text on `HEAD`. Looks like a local vault-linking pass ran over this doc; no factual/content change. |
| `outputs/storage_maintenance/storage_maintenance_latest.{json,md}` | `outputs_storage_maintenance_latest.{json,md}.patch` | Yes | **N/A — generated runtime artifact, not source.** Newer run output than what `HEAD`'s committed snapshot happens to hold. Not something to "merge"; flagging only that these output files are tracked in git at all is a separate, pre-existing question for the owner. |
| 14 other `migrations/versions/*.py` (see list below) | `migrations_*.patch` | **No** | **Line-ending noise only, no functional change.** `git diff --ignore-space-at-eol --ignore-all-space` is empty for every one of these. Safe to ignore / not real drift from `HEAD`. |

The 14 line-ending-only migration files: `7e4dfecce247_baseline_schema_from_schema_sql.py`,
`a1b2c3d4e5f6_canvas_tables.py`, `b6bba10f0fdb_add_astrogrid_schema_boundary.py`,
`c91b4a2e7d33_add_astrogrid_learning_loop_scaffold.py`,
`e2f6a9d3c4b1_add_astrogrid_scoring_class.py`, `f1a2b3c4d5e6_capital_flow_tables.py`,
`idle_fleet_goal_queue_day1.py`, `merge_heads_20260910.py`, `phase4_actor_analytics.py`,
`phase4_fts_news.py`, `phase4_investigation_evidence.py`, `regime_history_data_as_of.py`,
`regime_history_writer.py`, `tps_phase0_snapshots.py`.

### Detail: the one genuine fix (`phase4_fts_intelligence_search.py`)

The migration's original `upgrade()` backfills/triggers a `search_vector`
tsvector column on `analytical_snapshots` from
`COALESCE(title, '') || ' ' || COALESCE(summary, '')`. **`analytical_snapshots`
has no `title` or `summary` columns.** Its actual text columns are
`category`/`subcategory` — confirmed against the table's real writer,
`ingestion/openbb_pipeline.py`'s `INSERT INTO analytical_snapshots
(snapshot_date, category, subcategory, as_of_date, payload, metrics)`.
(A stale `.claude/CODEBASE_INDEX.md` doc entry lists `title, summary` as
columns of this table — that doc is wrong; the live INSERT statement is
authoritative.) As originally written, the backfill UPDATE would fail
outright and the row-level trigger would reject every write to the table.
grid-svr's working copy fixes both call sites plus the intelligence_search
union view to use `category`/`subcategory`. The in-place patch is included
here for reference; **the recommended path is a new migration** that
applies the same corrected SQL going forward, since this file is already
applied on production and alembic has no mechanism to "re-diff" a stamped
revision.

## Not investigated further (out of scope for this PR)

- The ~350 dated `outputs/autoresearch_*.json` files and
  `outputs/storage_maintenance_*.{json,md}` snapshots — untracked run
  history, not code.
- `docs/handoffs/2026-09-18/`, `tmp_transfer/` — untracked, not evaluated.
- `scripts/pull_options_gem_tickers.py` (named in the `.git/info/exclude`
  comment but not one of the four unit-mapped scripts) — not evaluated in
  detail. It does have a live unit, `grid-options-puller.service`, but that
  unit is currently quarantined: a `99-grid652-quarantine.conf` drop-in sets
  `RefuseManualStart=yes` gated on
  `/data/grid_v4/deploy-forensics/GRID-652-legacy-quarantine-20260925T1900Z/activation-held`,
  and a `50-grid652-immutable-pin.conf` drop-in pins `ExecStart` to a frozen
  copy under `/data/grid_v4/grid-options-puller-pins/<hash>/`, not
  `grid_repo` — this looks like a separate, already-handled incident
  (GRID-652) and out of scope for this recovery pass.
