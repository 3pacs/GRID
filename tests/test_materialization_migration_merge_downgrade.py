"""Real Alembic offline traversal on an isolated four-revision fixture.

Standalone: python3 -I -B tests/test_materialization_migration_merge_downgrade.py -v
Uses installed Alembic/SQLAlchemy; never loads GRID's env.py or parent bodies.
The negative control removes the guard only in a temporary copy.
"""

from __future__ import annotations

import ast
import io
from pathlib import Path
import tempfile
import unittest


MERGE = "merge_yf_finra_20261023"
COMMON = "people_events_v2_20261001"
MERGE_PATH = Path(__file__).resolve().parents[1] / "migrations" / "versions" / f"{MERGE}.py"
REFUSAL = (
    "Downgrade through merge_yf_finra_20261023 is refused: "
    "the historical FINRA downgrade can delete a pre-existing registration. "
    "Retain registry rows and dependent observations; use a separately "
    "reviewed forward recovery plan."
)
PARENTS = {
    "yf_latest_daily_vintage_20261019": "guard_fixture_yf_parent",
    "finra_ats_feature_20261020": "guard_fixture_finra_parent",
}
OFFLINE_ENV = '''from alembic import context

if not context.is_offline_mode():
    raise RuntimeError("This test environment is offline only")
context.configure(dialect_name="postgresql", literal_binds=True)
with context.begin_transaction():
    context.run_migrations()
'''


class MaterializationMergeDowngradeTests(unittest.TestCase):
    def run_offline_downgrade(self, merge_source: str) -> tuple[RuntimeError | None, str]:
        # Public Alembic command and runtime; no GRID imports, engine or DB URL.
        from alembic import command
        from alembic.config import Config

        output = io.StringIO()
        failure = None
        with tempfile.TemporaryDirectory(prefix="grid-merge-downgrade-") as folder:
            root = Path(folder)
            versions = root / "versions"
            versions.mkdir()
            (root / "env.py").write_text(OFFLINE_ENV, encoding="utf-8")
            (versions / f"{MERGE}.py").write_text(merge_source, encoding="utf-8")
            (versions / f"{COMMON}.py").write_text(
                f"revision = {COMMON!r}\ndown_revision = None\n"
                "branch_labels = None\ndepends_on = None\n"
                "def upgrade():\n    pass\ndef downgrade():\n    pass\n",
                encoding="utf-8",
            )
            for revision, marker in PARENTS.items():
                # Synthetic parent bodies expose traversal without original SQL.
                (versions / f"{revision}.py").write_text(
                    "from alembic import op\n"
                    f"revision = {revision!r}\ndown_revision = {COMMON!r}\n"
                    "branch_labels = None\ndepends_on = None\n"
                    "def upgrade():\n    pass\n"
                    "def downgrade():\n"
                    f"    op.execute({('SELECT ' + repr(marker))!r})\n",
                    encoding="utf-8",
                )
            config = Config(output_buffer=output)
            config.set_main_option("script_location", str(root))
            try:
                command.downgrade(config, f"{MERGE}:{COMMON}", sql=True)
            except RuntimeError as error:
                failure = error
        return failure, output.getvalue()

    def assert_guard_blocks(self, failure: RuntimeError | None, sql: str) -> None:
        self.assertIsInstance(failure, RuntimeError)
        self.assertEqual(str(failure), REFUSAL)
        for marker in PARENTS.values():
            self.assertNotIn(marker, sql)
        for statement in (
            "UPDATE alembic_version", "INSERT INTO alembic_version",
            "DELETE FROM alembic_version",
        ):
            self.assertNotIn(statement, sql)

    def test_merge_refusal_precedes_parent_traversal_and_version_rewrite(self) -> None:
        failure, sql = self.run_offline_downgrade(MERGE_PATH.read_text(encoding="utf-8"))
        self.assert_guard_blocks(failure, sql)

    def test_guard_removal_negative_reaches_both_parents(self) -> None:
        tree = ast.parse(MERGE_PATH.read_text(encoding="utf-8"))
        downgrade = next(
            node for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "downgrade"
        )
        downgrade.body = [ast.Pass()]
        mutant = ast.unparse(ast.fix_missing_locations(tree)) + "\n"
        failure, sql = self.run_offline_downgrade(mutant)
        self.assertIsNone(failure)
        for marker in PARENTS.values():
            self.assertIn(marker, sql)
        with self.assertRaises(AssertionError):
            self.assert_guard_blocks(failure, sql)


if __name__ == "__main__":
    unittest.main()
