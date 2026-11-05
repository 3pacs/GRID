"""Read migration literals without importing Alembic or application modules.

Standalone: python3 tests/test_materialization_migration_merge_graph.py -v
"""

from __future__ import annotations

import ast
from pathlib import Path
import unittest


VERSIONS = Path(__file__).resolve().parents[1] / "migrations" / "versions"
MERGE = "merge_yf_finra_20261023"
PARENTS = ("yf_latest_daily_vintage_20261019", "finra_ats_feature_20261020")
COMMON_PARENT = "people_events_v2_20261001"
METADATA = ("revision", "down_revision", "branch_labels", "depends_on")
REFUSAL = 'Downgrade through merge_yf_finra_20261023 is refused: the historical FINRA downgrade can delete a pre-existing registration. Retain registry rows and dependent observations; use a separately reviewed forward recovery plan.'


def read_metadata(path: Path) -> tuple[ast.Module, dict[str, object]]:
    tree = ast.parse(path.read_bytes(), filename=str(path))
    values = {}
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else (
            [node.target] if isinstance(node, ast.AnnAssign) else []
        )
        for target in targets:
            if isinstance(target, ast.Name) and target.id in METADATA:
                if target.id in values:
                    raise AssertionError(f"duplicate {target.id}: {path}")
                values[target.id] = ast.literal_eval(node.value)
    return tree, values


class MaterializationMigrationMergeGraphTests(unittest.TestCase):
    def test_merge_is_literal_join_with_refusing_downgrade(self) -> None:
        tree, values = read_metadata(VERSIONS / f"{MERGE}.py")
        self.assertEqual(values, dict(zip(METADATA, (MERGE, PARENTS, None, None))))
        self.assertLessEqual(len(MERGE), 32)
        functions = {}
        for node in tree.body:
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                self.assertIsInstance(node.value.value, str)
            elif isinstance(node, ast.Assign):
                self.assertEqual(len(node.targets), 1)
                self.assertIsInstance(node.targets[0], ast.Name)
                self.assertIn(node.targets[0].id, METADATA)
            elif isinstance(node, ast.FunctionDef):
                self.assertNotIn(node.name, functions)
                functions[node.name] = node
                self.assertFalse(node.decorator_list)
                self.assertFalse(
                    node.args.posonlyargs or node.args.args or node.args.kwonlyargs
                    or node.args.defaults or node.args.kw_defaults
                    or node.args.vararg or node.args.kwarg
                )
                self.assertEqual(len(node.body), 1)
                expected = ast.Pass() if node.name == "upgrade" else ast.Raise(
                    exc=ast.Call(
                        func=ast.Name(id="RuntimeError", ctx=ast.Load()),
                        args=[ast.Constant(REFUSAL)], keywords=[],
                    ),
                    cause=None,
                )
                self.assertEqual(ast.dump(node.body[0]), ast.dump(expected))
                self.assertEqual(ast.dump(node.returns), ast.dump(ast.Constant(None)))
            else:
                self.fail(f"unexpected executable merge statement: {ast.dump(node)}")
        self.assertEqual(set(functions), {"upgrade", "downgrade"})

    def test_historical_siblings_keep_their_parent(self) -> None:
        for revision in PARENTS:
            with self.subTest(revision=revision):
                _, values = read_metadata(VERSIONS / f"{revision}.py")
                self.assertEqual(values["revision"], revision)
                self.assertEqual(values["down_revision"], COMMON_PARENT)

    def test_graph_is_closed_acyclic_and_has_one_joined_head(self) -> None:
        parents = {}
        for path in sorted(VERSIONS.glob("*.py")):
            _, values = read_metadata(path)
            if "revision" not in values:
                continue
            revision = values["revision"]
            self.assertIsInstance(revision, str)
            self.assertTrue(revision)
            self.assertLessEqual(len(revision), 32)
            self.assertNotIn(revision, parents, f"duplicate revision: {path}")
            down = values.get("down_revision")
            self.assertIsInstance(down, (str, tuple, type(None)))
            parents[revision] = (
                () if down is None else ((down,) if isinstance(down, str) else down)
            )
            self.assertEqual(len(parents[revision]), len(set(parents[revision])))
            for parent in parents[revision]:
                self.assertIsInstance(parent, str)
            self.assertIsNone(
                values.get("depends_on"), f"dependency needs graph support: {path}"
            )
        referenced = {parent for group in parents.values() for parent in group}
        self.assertFalse(referenced - parents.keys(), "missing declared parent")
        active, complete = set(), set()

        def visit(revision: str) -> None:
            self.assertNotIn(revision, active, f"cycle at {revision}")
            if revision in complete:
                return
            active.add(revision)
            for parent in parents[revision]:
                visit(parent)
            active.remove(revision)
            complete.add(revision)

        for revision in parents:
            visit(revision)
        heads = set(parents) - referenced
        self.assertEqual(len(heads), 1, f"migration heads: {sorted(heads)}")
        ancestors, pending = set(), list(heads)
        while pending:
            revision = pending.pop()
            if revision not in ancestors:
                ancestors.add(revision)
                pending.extend(parents[revision])
        self.assertIn(MERGE, ancestors)
        self.assertTrue(set(PARENTS) <= ancestors)


if __name__ == "__main__":
    unittest.main()
