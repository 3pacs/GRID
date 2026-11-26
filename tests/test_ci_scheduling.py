"""tests/ci_scheduling.py and the conftest hooks that use it: CI shards and xdist units.

The property that matters most: the shards partition the suite. Every
collected test runs in exactly one shard, so splitting the run can never drop
or repeat a test. The rest pins the xdist units (database tests serialised,
spread files split, everything else by file), the dispatch order and the
GRID Tests workflow wiring that relies on them.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from tests import ci_scheduling as ci
from tests import conftest

REPO = Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github" / "workflows" / "test.yml"


# --------------------------------------------------------------------------
# parse_shard
# --------------------------------------------------------------------------


@pytest.mark.parametrize("value,expected", [("1/2", (1, 2)), ("2/2", (2, 2)), (" 3/5 ", (3, 5)), ("1/1", (1, 1))])
def test_parse_shard_accepts_index_over_total(value: str, expected: tuple[int, int]) -> None:
    assert ci.parse_shard(value) == expected


@pytest.mark.parametrize("value", ["", "1", "0/2", "3/2", "-1/2", "a/b", "1/2/3", "1/0", "1 / 2x"])
def test_parse_shard_refuses_anything_else(value: str) -> None:
    with pytest.raises(ValueError):
        ci.parse_shard(value)


# --------------------------------------------------------------------------
# assign_shards: a deterministic, balanced partition
# --------------------------------------------------------------------------


def _weights(n: int) -> dict[str, float]:
    # Uneven, repeatable weights: a few heavy files and a long light tail.
    return {f"tests/test_f{i:03d}.py": float((i * 37) % 101) + (500.0 if i % 50 == 0 else 0.0) for i in range(n)}


@pytest.mark.parametrize("total", [1, 2, 3, 5])
def test_every_file_gets_exactly_one_shard(total: int) -> None:
    weights = _weights(300)
    db = {m for i, m in enumerate(sorted(weights)) if i % 7 == 0}
    owner = ci.assign_shards(weights, db, total)
    assert set(owner) == set(weights)
    assert set(owner.values()) <= set(range(1, total + 1))
    # each shard's selection, put back together, is the whole suite once
    selections = [{m for m, s in owner.items() if s == k} for k in range(1, total + 1)]
    assert sum(len(s) for s in selections) == len(weights)
    assert set().union(*selections) == set(weights)


def test_assignment_ignores_input_order() -> None:
    weights = _weights(120)
    db = set(list(weights)[::9])
    reversed_weights = dict(reversed(list(weights.items())))
    assert ci.assign_shards(weights, db, 3) == ci.assign_shards(reversed_weights, sorted(db, reverse=True), 3)


@pytest.mark.parametrize("total", [2, 3])
def test_shards_are_balanced_within_one_file(total: int) -> None:
    weights = _weights(400)
    owner = ci.assign_shards(weights, set(), total)
    loads = [sum(w for m, w in weights.items() if owner[m] == k) for k in range(1, total + 1)]
    # greedy longest-first: the spread is at most the heaviest single file
    assert max(loads) - min(loads) <= max(weights.values())


def test_database_files_are_balanced_among_themselves() -> None:
    weights = _weights(200)
    db = {m for i, m in enumerate(sorted(weights)) if i % 4 == 0}
    owner = ci.assign_shards(weights, db, 2)
    chains = [sum(weights[m] for m in db if owner[m] == k) for k in (1, 2)]
    assert abs(chains[0] - chains[1]) <= max(weights[m] for m in db)


def test_unknown_db_files_are_ignored_and_total_must_be_positive() -> None:
    owner = ci.assign_shards({"tests/test_a.py": 1.0}, {"tests/test_not_collected.py"}, 2)
    assert owner == {"tests/test_a.py": 1}
    with pytest.raises(ValueError):
        ci.assign_shards({"tests/test_a.py": 1.0}, set(), 0)


def test_file_weights_use_measured_seconds_then_test_count() -> None:
    heavy = next(iter(ci.HEAVY_SECONDS))
    weights = ci.file_weights({heavy: 3, "tests/test_light.py": 20})
    assert weights[heavy] == ci.HEAVY_SECONDS[heavy]
    assert weights["tests/test_light.py"] == pytest.approx(20 * ci.DEFAULT_SECONDS_PER_TEST)


# --------------------------------------------------------------------------
# scope_for: the xdist units
# --------------------------------------------------------------------------


def _write(root: Path, rel: str, body: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


@pytest.mark.parametrize("token", ci.DB_TOKENS)
def test_a_database_file_joins_the_postgres_unit(tmp_path: Path, token: str) -> None:
    safe = re.sub(r"\W", "_", token)
    _write(tmp_path, f"tests/test_db_{safe}.py", f"def test_x():\n    {token}\n")
    assert ci.scope_for(f"tests/test_db_{safe}.py::test_x", tmp_path) == ci.POSTGRES_GROUP


def test_other_files_are_one_unit_each_and_spread_files_one_per_test(tmp_path: Path) -> None:
    _write(tmp_path, "tests/sub/test_plain.py", "def test_x():\n    pass\n")
    assert ci.scope_for("tests/sub/test_plain.py::TestC::test_x[a-b]", tmp_path) == "tests/sub/test_plain.py"
    spread = sorted(ci.SPREAD_FILES)[0]
    _write(tmp_path, spread, "def test_x():\n    pass\n")
    assert ci.scope_for(f"{spread}::test_x[1]", tmp_path) == f"{spread}::test_x[1]"


def test_an_unreadable_file_is_treated_as_a_database_file(tmp_path: Path) -> None:
    assert ci.scope_for("tests/test_missing.py::test_x", tmp_path) == ci.POSTGRES_GROUP


def test_a_group_suffix_is_stripped_but_an_at_inside_a_parameter_is_not(tmp_path: Path) -> None:
    spread = sorted(ci.SPREAD_FILES)[-1]
    _write(tmp_path, spread, "def test_x():\n    pass\n")
    assert ci.scope_for(f"{spread}::test_x[1]@postgres", tmp_path) == f"{spread}::test_x[1]"
    assert ci.scope_for(f"{spread}::test_x[user@host]", tmp_path) == f"{spread}::test_x[user@host]"


def test_dispatch_rank_puts_postgres_then_heavy_slowest_first_then_the_rest() -> None:
    heavy = sorted(ci.HEAVY_SECONDS, key=lambda m: -ci.HEAVY_SECONDS[m])
    modules = ["tests/test_z_plain.py", heavy[-1], "tests/test_a_plain.py", "tests/test_pg.py", heavy[0]]
    is_db = {"tests/test_pg.py"}
    ordered = sorted(modules, key=lambda m: ci.dispatch_rank(m, m in is_db))
    assert ordered == ["tests/test_pg.py", heavy[0], heavy[-1], "tests/test_z_plain.py", "tests/test_a_plain.py"]


def test_make_scheduler_uses_grid_units_and_keeps_the_collection_order() -> None:
    pytest.importorskip("xdist")
    config = SimpleNamespace(
        getvalue={"tx": ["2*popen"], "dist": "loadgroup"}.__getitem__,
        option=SimpleNamespace(loadscopereorder=True),
        rootpath=REPO,
    )
    sched = ci.make_scheduler(config, None)
    assert config.option.loadscopereorder is False
    assert sched._split_scope("tests/test_true_breadth.py::test_x") == "tests/test_true_breadth.py"
    assert sched._split_scope("tests/test_future_dated_reads_guard.py::test_x") == ci.POSTGRES_GROUP
    spread = sorted(ci.SPREAD_FILES)[0]
    assert sched._split_scope(f"{spread}::test_x") == f"{spread}::test_x"


# --------------------------------------------------------------------------
# The real conftest hook, on a fake collection
# --------------------------------------------------------------------------


class _Hook:
    def __init__(self) -> None:
        self.deselected: list[Any] = []

    def pytest_deselected(self, items: list[Any]) -> None:
        self.deselected.extend(items)


def _config(*, worker: bool = False, loadgroup: bool = False) -> SimpleNamespace:
    cfg = SimpleNamespace(rootpath=REPO, hook=_Hook(), option=SimpleNamespace(loadgroup=loadgroup))
    if worker:
        cfg.workerinput = {}
    return cfg


def _collection() -> list[SimpleNamespace]:
    files = sorted(p.relative_to(REPO).as_posix() for p in (REPO / "tests").glob("test_*.py"))[:80]
    return [SimpleNamespace(nodeid=f"{f}::test_{k}") for f in files for k in range(3)]


def test_conftest_shards_partition_a_real_file_list(monkeypatch: pytest.MonkeyPatch) -> None:
    every = [item.nodeid for item in _collection()]
    seen: list[str] = []
    for index in (1, 2, 3):
        monkeypatch.setenv(ci.SHARD_ENV, f"{index}/3")
        items, cfg = _collection(), _config()
        conftest.pytest_collection_modifyitems(cfg, items)
        kept = [item.nodeid for item in items]
        assert kept, f"shard {index}/3 kept nothing"
        assert len(kept) + len(cfg.hook.deselected) == len(every)
        # a file is never split across shards
        assert {ci.module_of(n) for n in kept}.isdisjoint({ci.module_of(i.nodeid) for i in cfg.hook.deselected})
        seen.extend(kept)
    assert sorted(seen) == sorted(every)


def test_conftest_refuses_a_malformed_shard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(ci.SHARD_ENV, "3/2")
    with pytest.raises(pytest.UsageError):
        conftest.pytest_collection_modifyitems(_config(), _collection())


def test_conftest_without_a_shard_keeps_everything_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ci.SHARD_ENV, raising=False)
    items, cfg = _collection(), _config()
    before = [item.nodeid for item in items]
    conftest.pytest_collection_modifyitems(cfg, items)
    assert [item.nodeid for item in items] == before and not cfg.hook.deselected


def test_conftest_sorts_for_dispatch_only_on_a_loadgroup_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(ci.SHARD_ENV, raising=False)
    heavy = max(ci.HEAVY_SECONDS, key=ci.HEAVY_SECONDS.get)
    # real files: a missing one would count as a database file (unreadable -> postgres)
    plain = "tests/test_true_breadth.py"
    names = [f"{plain}::test_1", f"{plain}::test_0",
             f"{heavy}::test_b", "tests/test_future_dated_reads_guard.py::test_x", f"{heavy}::test_a"]
    for worker, loadgroup, expect_sorted in ((True, True, True), (True, False, False), (False, True, False)):
        items = [SimpleNamespace(nodeid=n) for n in names]
        conftest.pytest_collection_modifyitems(_config(worker=worker, loadgroup=loadgroup), items)
        got = [item.nodeid for item in items]
        if expect_sorted:
            # postgres first, heavy next with its own order kept, then the rest in collection order
            assert got == [names[3], names[2], names[4], names[0], names[1]]
        else:
            assert got == names


def test_scheduler_hook_only_replaces_loadgroup() -> None:
    assert conftest.pytest_xdist_make_scheduler(SimpleNamespace(getvalue={"dist": "load"}.__getitem__), None) is None


# --------------------------------------------------------------------------
# Guards on the tables and markers
# --------------------------------------------------------------------------


def test_heavy_and_spread_entries_name_existing_files() -> None:
    for rel in set(ci.HEAVY_SECONDS) | ci.SPREAD_FILES:
        assert (REPO / rel).is_file(), f"stale entry {rel}"


def _shared_fixture_users(path: Path) -> dict[str, int]:
    """Module/class/package-scoped fixtures in ``path`` -> how many tests request them."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    scoped: set[str] = set()
    autouse: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for deco in node.decorator_list:
                if not isinstance(deco, ast.Call):
                    continue
                kws = {kw.arg: kw.value for kw in deco.keywords}
                scope = kws.get("scope")
                if isinstance(scope, ast.Constant) and scope.value in {"module", "class", "package", "session"}:
                    scoped.add(node.name)
                    use = kws.get("autouse")
                    if isinstance(use, ast.Constant) and use.value is True:
                        autouse.add(node.name)
    users = dict.fromkeys(scoped, 0)
    # an autouse fixture serves every test in its scope without being named
    users.update(dict.fromkeys(autouse, 2))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test"):
            for arg in node.args.args:
                if arg.arg in users:
                    users[arg.arg] += 1
    return users


@pytest.mark.parametrize("rel", sorted(ci.SPREAD_FILES))
def test_spread_files_are_safe_to_spread(rel: str) -> None:
    """No database access, and no wider-than-function fixture that two tests would share."""
    path = REPO / rel
    assert not ci.module_touches_db(str(path)), f"{rel} reaches a database: it must stay in the postgres unit"
    shared = {name: n for name, n in _shared_fixture_users(path).items() if n > 1}
    assert not shared, f"{rel} has fixtures shared by several tests {shared}: spreading would rebuild them per worker"


def test_explicit_xdist_groups_are_only_postgres() -> None:
    """The source scan understands the postgres group marker only; any other group name would be ignored."""
    pattern = re.compile(r"xdist_group\(\s*(?:name\s*=\s*)?(['\"])(?P<name>[^'\"]+)\1")
    offenders = []
    for path in (REPO / "tests").rglob("*.py"):
        if path.name in {"ci_scheduling.py", "test_ci_scheduling.py"}:
            continue
        for match in pattern.finditer(path.read_text(encoding="utf-8")):
            if match.group("name") != ci.POSTGRES_GROUP:
                offenders.append(f"{path.relative_to(REPO)}: {match.group(0)}")
    assert not offenders, offenders


# --------------------------------------------------------------------------
# GRID Tests workflow wiring
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_suite_matrix_covers_every_shard_once(workflow: dict[str, Any]) -> None:
    job = workflow["jobs"]["backend-tests-suite"]
    shards = job["strategy"]["matrix"]["shard"]
    assert shards == list(range(1, len(shards) + 1)) and len(shards) >= 2
    assert job["strategy"]["fail-fast"] is False
    run = next(step for step in job["steps"] if step.get("name") == "Run tests")
    assert run["env"][ci.SHARD_ENV] == f"${{{{ matrix.shard }}}}/{len(shards)}"
    # the shard runs the whole tests/ tree under GRID's xdist units, unfiltered
    assert "python -m pytest tests/ -v --tb=short --maxfail=50 --timeout=180" in run["run"]
    assert "-n auto --dist loadgroup" in run["run"]
    options = run["run"].replace("python -m pytest", "python_m_pytest")
    narrowing = re.findall(r"\s(-k|-m|--deselect|--ignore(?:-glob)?|--lf|--last-failed|--sw|--stepwise)\b", options)
    assert not narrowing, narrowing


def test_backend_tests_fails_unless_every_suite_shard_passed(workflow: dict[str, Any]) -> None:
    job = workflow["jobs"]["backend-tests"]
    assert job["name"] == "Backend Tests"
    assert job["needs"] == "backend-tests-suite"
    # runs (and fails) when a shard failed, but not when the run was cancelled
    assert job["if"] == "${{ !cancelled() }}"
    gate = job["steps"][0]
    assert gate["name"] == "Main suite shards passed"
    assert gate["env"]["SUITE_RESULT"] == "${{ needs.backend-tests-suite.result }}"
    assert '[ "$SUITE_RESULT" != "success" ]' in gate["run"] and "exit 1" in gate["run"]
    # the main suite no longer runs here; the #1352 witness and the contracts still do
    names = [step.get("name") for step in job["steps"]]
    assert "Run tests" not in names
    assert "Option B release witness (#1352)" in names
