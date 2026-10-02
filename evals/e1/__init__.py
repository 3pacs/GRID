"""E1 integrity gates (GRID-EVALS-SELF-IMPROVING-ENGINE-PLAN-20260930, milestone M1).

Pass/fail gates that block any change making GRID's research or ingestion
less honest. They are cheap (seconds, synthetic fixtures, no network, no
production access) so they run on every PR:

* ``test_lookahead_canary.py`` / ``test_gates_pg.py`` (PostgreSQL) -- a small
  synthetic world with vintages, pull timestamps, revisions and data
  published after ``as_of``. The main point-in-time consumers
  (``store.observations.read_window_known_at`` / ``read_window`` /
  ``read_latest``, ``store.pit.PITStore``, the regime
  ``compute_state_vector`` path and the VS1 panel feature builders) must
  give byte-identical output at a past ``as_of`` after future rows are
  appended, and must not correlate with a planted "future-leak" series whose
  values equal the next-period outcome but which only becomes available
  after that outcome is realised. Every leak canary has a self-test that
  proves it trips on a deliberately leaky reader.
* ``test_honest_success.py`` -- every ``SmartScheduler`` registry entry,
  through its own wiring, reports SUCCESS (and advances source freshness)
  only when rows > 0; and every registered puller resolves to a
  ``source_catalog`` name and a callable pull method.
* ``test_provenance.py`` -- every ``INSERT INTO raw_series`` writer sets
  ``source_id`` and ``pull_status`` (``pull_timestamp`` explicitly or via the
  schema's ``DEFAULT NOW()``), never rewrites a stored vintage in place, and
  one ``source_catalog`` name keeps one price-adjustment meaning.
* ``test_reproducibility.py`` -- the VS1 discovery ledger and the real-panel
  scan, run twice on a frozen fixture, give byte-identical JSON.

Known violations on main are not weakened away: each is a strict, named
``xfail`` registered in :mod:`evals.e1.known_violations` (strict, so fixing
the code turns the xfail into a failure until the entry is removed).

The suite is hash-pinned: ``MANIFEST.sha256`` lists the sha256 of every file
in this directory (LF-normalised) and ``test_manifest_guard.py`` fails when
any file changes without the manifest. Regenerate deliberately with
``python -m evals.e1.manifest --write`` -- an eval change is a reviewed,
versioned event, never a side effect.

The PostgreSQL gates use only ``GRID_TEST_DB_URL`` (no default) and refuse
production database names (``pg_safety.py``, ``test_pg_safety.py``): scratch
schemas never touch a production cluster database.

CI: ``.github/workflows/test.yml`` step "E1 integrity gates" runs
``python -m pytest evals/e1`` against the job's PostgreSQL with
``E1_REQUIRE_PG=1``; a skipped PostgreSQL gate fails the step.
"""

SUITE_VERSION = "e1-v1.3"  # v1.1: E1-V1, V2, V5 fixed; v1.2: E1-V3, V4, V6 fixed; their xfails removed; v1.3: E1-V7 resolved_series vintage gate (DFa)
