"""Guards against the alembic history splitting into multiple heads again.

Loads the script directory offline (no database connection) and asserts
there is exactly one head. This is what caught the three-way split fixed
by migrations/versions/merge_heads_20260910.py, and what enforces the
re-parenting called out there whenever a new branch merges concurrently.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.mark.unit
def test_single_alembic_head():
    config = Config(os.path.join(REPO_ROOT, "alembic.ini"))
    config.set_main_option(
        "script_location", os.path.join(REPO_ROOT, "migrations")
    )
    script = ScriptDirectory.from_config(config)

    heads = script.get_heads()

    assert len(heads) == 1, (
        f"Expected exactly one alembic head, found {len(heads)}: {heads}. "
        "A new revision was probably branched off an existing head instead "
        "of the current tip, or two PRs added parallel revisions off the "
        "same parent. Add a merge revision (down_revision = tuple of the "
        "heads) to reconcile them."
    )


# alembic's default version table is `version_num VARCHAR(32)`, and griddb's
# was created with that default. A longer identifier does not fail at import,
# at planning, or while the revision runs -- it fails on the very last
# statement, the UPDATE that records the revision as applied.
VERSION_NUM_MAX = 32


@pytest.mark.unit
def test_revision_ids_fit_the_version_column():
    """A revision id over 32 chars fails *after* its upgrade() has run.

    Deploy run 640 (2026-09-13) is the case: `snapshot_payload_actor_index_
    20260912` is 37 characters, so alembic raised

        psycopg2.errors.StringDataRightTruncation:
        value too long for type character varying(32)

    on `UPDATE alembic_version SET version_num=...`, with the migration's own
    work already done. It had been wrong since the revision was written, and
    stayed invisible for four deploys because an earlier failure inside
    upgrade() meant execution never reached the UPDATE. Its parent,
    `restore_news_search_arm_20260912`, is exactly 32 -- the ceiling had never
    been crossed before.

    Checked across every revision, not just the head, because `alembic
    upgrade head` records each one it passes through.
    """
    config = Config(os.path.join(REPO_ROOT, "alembic.ini"))
    config.set_main_option(
        "script_location", os.path.join(REPO_ROOT, "migrations")
    )
    script = ScriptDirectory.from_config(config)

    oversized = {
        rev.revision: len(rev.revision)
        for rev in script.walk_revisions()
        if len(rev.revision) > VERSION_NUM_MAX
    }

    assert not oversized, (
        f"revision id(s) longer than alembic_version.version_num "
        f"VARCHAR({VERSION_NUM_MAX}): {oversized}. The revision will run and "
        "then fail on the UPDATE that records it, leaving the database "
        "changed but the version unrecorded. Shorten the id (and any "
        "down_revision referencing it)."
    )


@pytest.mark.unit
def test_migration_warnings_are_not_swallowed():
    """A migration's log.warning must actually reach a handler.

    ``import alembic`` installs a ``NullHandler`` on the ``alembic`` logger.
    That handler counts as "found" in ``Logger.callHandlers``, so Python's
    last-resort stderr handler never fires and every record from a migration
    is discarded in silence. Nothing in the config says so; it is purely an
    artefact of the import.

    ``migrations/env.py`` has to call ``fileConfig`` to undo it. It did not,
    which made ``alembic.ini``'s ``[loggers]``/``[handlers]`` sections dead
    text. Deploy 641 proved the cost: ``snapshot_actor_index_20260912`` took
    its deferral branch and warned, naming two INVALID indexes and the SQL to
    fix them, and none of it reached the deploy log.

    That revision is *designed* to skip work and report it. The warning is the
    whole difference between a documented deferral and the silent degradation
    #477/#479 existed to remove.

    Checked by logging through the real chain, not by reading env.py's source:
    the failure mode here is a handler lookup, and only exercising it proves
    anything.
    """
    import subprocess
    import textwrap

    # A subprocess, because fileConfig mutates process-global logging state:
    # once any test in this process has applied it, the "before" half stops
    # being observable. A clean interpreter shows both directions.
    probe = textwrap.dedent(
        """
        import importlib.util, logging, sys
        import alembic  # installs the NullHandler under test

        logging.getLogger("alembic.runtime.migration").warning("BEFORE-CANARY")

        spec = importlib.util.spec_from_file_location(
            "grid_alembic_logging_setup", sys.argv[1])
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod.configure_logging(sys.argv[2]), "alembic.ini not found"

        logging.getLogger("alembic.runtime.migration").warning("AFTER-CANARY")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", probe,
         os.path.join(REPO_ROOT, "migrations", "logging_setup.py"),
         os.path.join(REPO_ROOT, "alembic.ini")],
        capture_output=True, text=True, cwd=REPO_ROOT,
    )
    assert result.returncode == 0, result.stderr
    emitted = result.stdout + result.stderr

    # The bug, demonstrated: without the fix the record goes nowhere. If this
    # ever starts appearing, alembic stopped installing its NullHandler and
    # this whole guard can be reconsidered.
    assert "BEFORE-CANARY" not in emitted, (
        "alembic no longer swallows migration warnings by default; "
        "re-evaluate whether configure_logging() is still needed"
    )

    # The fix, demonstrated.
    assert "AFTER-CANARY" in emitted, (
        "a WARNING from alembic.runtime.migration reached no handler even "
        "after configure_logging(). Every deferral, skip and fallback a "
        "migration reports is invisible in production.\n"
        f"stdout: {result.stdout!r}\nstderr: {result.stderr!r}"
    )

    # ...and env.py must actually call it, or production stays silent.
    env_source = Path(REPO_ROOT, "migrations", "env.py").read_text()
    assert "configure_logging(" in env_source, (
        "migrations/env.py does not call configure_logging(), so alembic.ini's "
        "logging config stays dead text in production"
    )
