"""Offline integration contract for the AV/publication-review migration join."""
import ast
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
VERSIONS = ROOT / "migrations" / "versions"


def metadata(path):
    tree = ast.parse(path.read_bytes())
    values = {}
    for node in tree.body:
        targets = (node.targets if isinstance(node, ast.Assign) else
                   [node.target] if isinstance(node, ast.AnnAssign) else [])
        for target in targets:
            if (isinstance(target, ast.Name)
                    and target.id in {"revision", "down_revision"}):
                values[target.id] = ast.literal_eval(node.value)
    return tree, values


def test_join_preserves_both_independent_parent_histories():
    tree, values = metadata(VERSIONS / "merge_av_review_20261109.py")
    assert values["down_revision"] == (
        "av_event_dates_20261108", "pub_review_ingest_20261108")
    for parent in values["down_revision"]:
        _, previous = metadata(VERSIONS / f"{parent}.py")
        assert previous["down_revision"] == "sleuth_intersections_20261107"
    upgrade = next(n for n in tree.body
                   if isinstance(n, ast.FunctionDef) and n.name == "upgrade")
    assert len(upgrade.body) == 1 and isinstance(upgrade.body[0], ast.Pass)
    scope = {}
    exec(compile(tree, "merge_av_review_20261109.py", "exec"), scope)
    assert scope["upgrade"]() is None
    with pytest.raises(RuntimeError, match="forward recovery plan"):
        scope["downgrade"]()


def test_single_head_includes_both_av_and_review():
    parents = {}
    for path in VERSIONS.glob("*.py"):
        _, values = metadata(path)
        if "revision" not in values:
            continue
        down = values.get("down_revision")
        parents[values["revision"]] = (() if down is None else
                                       (down,) if isinstance(down, str) else down)
    referenced = {p for group in parents.values() for p in group}
    heads = set(parents) - referenced
    assert len(heads) == 1
    seen, pending = set(), list(heads)
    while pending:
        current = pending.pop()
        if current not in seen:
            seen.add(current)
            pending.extend(parents[current])
    assert {"merge_av_review_20261109", "av_event_dates_20261108",
            "pub_review_ingest_20261108"} <= seen
