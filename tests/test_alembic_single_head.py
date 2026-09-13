"""Guards against the alembic history splitting into multiple heads again.

Loads the script directory offline (no database connection) and asserts
there is exactly one head. This is what caught the three-way split fixed
by migrations/versions/merge_heads_20260910.py, and what enforces the
re-parenting called out there whenever a new branch merges concurrently.
"""

from __future__ import annotations

import os

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
