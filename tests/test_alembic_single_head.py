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
