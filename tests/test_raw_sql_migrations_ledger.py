"""No `migrations/*.sql` file may exist without a recorded disposition.

This repository has carried two migration mechanisms side by side, and the
deploy only ever ran one of them. `.github/workflows/deploy.yml` runs
``python3 -m alembic upgrade head``; the 52 raw ``migrations/*.sql`` files
were applied, when they were applied at all, by a human typing
``sudo -u postgres psql griddb -f …``. Nothing in CI or deploy applied them
and nothing reported that they had not been.

The cost was not hypothetical. ``0062_register_gdelt_scheduled_features.sql``
registered the GDELT features the news card on grid.stepdad.finance reads,
merged, and was never applied, so ``GET /physics/momentum`` answered
``available: false`` for months with the only record of the outstanding work
being a line in a TODO that said a person would remember to run it.

So every raw ``.sql`` file under ``migrations/`` must appear in
``migrations/RAW_SQL_LEDGER.md`` with an explicit status. A file that is
merely present is indistinguishable from one that is live; a file in the
ledger has been checked against the database and says what it is.

The ledger is the freeze, in both directions: a new raw ``.sql`` file that
nobody listed fails here, and its failure message says to write an alembic
revision instead. Adding one and calling it ``legacy-applied`` is still
possible — but it is an edit to a reviewed file that claims, in writing, that
the effect is already on griddb. That is the difference this guard is for:
not preventing every bad commit, but making the parallel mechanism impossible
to grow *silently*.

Walks the real ledger and the real alembic script directory rather than
pattern-matching source, following ``tests/test_alembic_single_head.py``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = REPO_ROOT / "migrations"
LEDGER = MIGRATIONS / "RAW_SQL_LEDGER.md"

# `_TEMPLATE.sql` is documentation, not a migration: it creates
# `your_table_here`. scripts/lint_migrations.py skips it for the same reason.
NOT_A_MIGRATION = {"_TEMPLATE.sql"}

VALID_STATUSES = {
    # Verified present on griddb; retained for history, never runs again.
    "legacy-applied",
    # Was outstanding; its effect is now carried by the named alembic revision.
    "ported",
    # A one-off data repair against specific primary keys. Not reproducible
    # on another database and not portable to alembic; kept as the record of
    # what was done.
    "legacy-data-fix",
    # Outstanding -- the effect is NOT on griddb -- and deliberately not
    # ported, because what it would create is already produced another way or
    # has no consumer left. The evidence column has to say which.
    "legacy-superseded",
}

# | `file.sql` | status | revision-or-dash | evidence |
_ROW = re.compile(
    r"^\|\s*`(?P<file>[^`]+\.sql)`\s*\|\s*(?P<status>[a-z-]+)\s*\|"
    r"\s*(?P<revision>[^|]*?)\s*\|\s*(?P<evidence>[^|]*?)\s*\|\s*$",
    re.MULTILINE,
)


def _ledger_rows() -> dict[str, tuple[str, str, str]]:
    """file name -> (status, revision, evidence), from the ledger table."""
    if not LEDGER.exists():
        pytest.fail(
            f"{LEDGER} is missing. It is the record of which raw SQL "
            "migrations are already on griddb and which were ported to "
            "alembic; without it nothing here can tell the two apart."
        )
    text = LEDGER.read_text(errors="replace")
    rows: dict[str, tuple[str, str, str]] = {}
    for m in _ROW.finditer(text):
        name = m.group("file")
        assert name not in rows, f"{name} is listed twice in {LEDGER.name}"
        rows[name] = (m.group("status"), m.group("revision"), m.group("evidence"))
    return rows


def _raw_sql_files() -> set[str]:
    return {
        p.name for p in MIGRATIONS.glob("*.sql") if p.name not in NOT_A_MIGRATION
    }


def _alembic_revisions() -> set[str]:
    config = Config(str(REPO_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(MIGRATIONS))
    script = ScriptDirectory.from_config(config)
    return {rev.revision for rev in script.walk_revisions()}


@pytest.mark.unit
def test_ledger_parses() -> None:
    """If the table shape drifts, every other assertion here goes quiet."""
    assert LEDGER.exists(), f"{LEDGER} is missing"
    rows = _ledger_rows()
    assert len(rows) > 40, (
        f"only {len(rows)} ledger rows parsed from {LEDGER.name}; the table "
        "format changed and this guard stopped seeing most of it"
    )


@pytest.mark.unit
def test_every_raw_sql_file_is_in_the_ledger() -> None:
    missing = sorted(_raw_sql_files() - set(_ledger_rows()))
    assert not missing, (
        f"raw SQL migration(s) with no entry in migrations/{LEDGER.name}: "
        f"{missing}. Nothing in CI or deploy runs migrations/*.sql -- deploy "
        "runs `alembic upgrade head` and only that -- so a new .sql file here "
        "will never be applied to griddb by any automation. Write an alembic "
        "revision under migrations/versions/ instead. If this file genuinely "
        "records work already applied by hand, add it to the ledger with the "
        "evidence that says so."
    )


@pytest.mark.unit
def test_ledger_has_no_rows_for_files_that_are_gone() -> None:
    stale = sorted(set(_ledger_rows()) - _raw_sql_files())
    assert not stale, (
        f"migrations/{LEDGER.name} lists file(s) that no longer exist: "
        f"{stale}. Remove the row, or restore the file if it was deleted by "
        "accident -- a ledger that describes files that are not there stops "
        "being evidence of anything."
    )


@pytest.mark.unit
def test_every_ledger_status_is_known() -> None:
    bad = {
        name: status
        for name, (status, _, _) in _ledger_rows().items()
        if status not in VALID_STATUSES
    }
    assert not bad, (
        f"unknown status value(s) in migrations/{LEDGER.name}: {bad}. "
        f"Allowed: {sorted(VALID_STATUSES)}."
    )


@pytest.mark.unit
def test_ported_rows_name_a_real_alembic_revision() -> None:
    """A `ported` claim is only worth anything if the revision exists.

    Checked against the script directory alembic itself walks, so a revision
    that was renamed, re-parented off the history, or never committed fails
    here rather than at `alembic upgrade head` on the deploy runner.
    """
    revisions = _alembic_revisions()
    broken = {
        name: revision
        for name, (status, revision, _) in _ledger_rows().items()
        if status == "ported" and revision not in revisions
    }
    assert not broken, (
        f"ledger row(s) claim a ported alembic revision that is not in the "
        f"script directory: {broken}. Known revisions: {sorted(revisions)}"
    )


@pytest.mark.unit
def test_legacy_rows_carry_evidence() -> None:
    """A claim about the database -- applied, or deliberately skipped -- needs a citation."""
    empty = sorted(
        name
        for name, (status, _, evidence) in _ledger_rows().items()
        if status in {"legacy-applied", "legacy-data-fix", "legacy-superseded"}
        and len(evidence.replace("-", "").strip()) < 10
    )
    assert not empty, (
        f"ledger row(s) make a claim about griddb and cite nothing: "
        f"{empty}. Name what was observed, and for a skip why it was skipped "
        "-- the whole point of the ledger is that the next person does not "
        "have to re-derive it."
    )
