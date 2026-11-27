"""tests/ci_scheduling.py and the conftest hooks that use it: CI shards and xdist units.

The property that matters most: the shards partition the suite. Every
collected test runs in exactly one shard, so splitting the run can never drop
or repeat a test. The rest pins the xdist units (database tests serialised,
spread files split, everything else by file), the dispatch order and the
GRID Tests and Python 3.14 workflow wiring that relies on them.
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
PY314_WORKFLOW = REPO / ".github" / "workflows" / "python314.yml"


@pytest.fixture(autouse=True)
def _no_real_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    """These tests call the conftest hooks directly. In a CI shard GRID_TEST_MANIFEST_DIR
    points at that shard's real manifest, which they must never write; and the
    recorded node ids of the real run must not pick up the fake ones."""
    monkeypatch.delenv(ci.MANIFEST_ENV, raising=False)
    monkeypatch.setattr(conftest, "_EXECUTED", set())


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


def _items(counts: dict[str, int]) -> list[SimpleNamespace]:
    return [SimpleNamespace(nodeid=f"{module}::test_{k}") for module, k_max in counts.items() for k in range(k_max)]


def test_runners_that_collect_differently_still_agree_on_every_file(monkeypatch: pytest.MonkeyPatch) -> None:
    """Codex review 6070328480's control: shard 1 collects A=1/B=1/C=1, shard 2 collects A=1/B=2/C=1.

    Weighing the partition by each runner's own counts let A and C run twice
    and B never. Ownership now comes from the tree, so each file still has
    exactly one owner. A file outside the tree is placed by a hash of its path.
    """
    files = sorted(p.relative_to(REPO).as_posix() for p in (REPO / "tests").glob("test_*.py"))
    a, b, c = files[0], files[len(files) // 2], files[-1]
    outside = "tests/not_on_disk/test_virtual.py"
    collections = {1: {a: 1, b: 1, c: 1, outside: 1}, 2: {a: 1, b: 2, c: 1, outside: 3}}
    for total in (2, 3):
        owners: dict[str, list[int]] = {}
        for index in range(1, total + 1):
            monkeypatch.setenv(ci.SHARD_ENV, f"{index}/{total}")
            items = _items(collections[1 if index == 1 else 2])
            conftest.pytest_collection_modifyitems(_config(), items)
            for module in {ci.module_of(i.nodeid) for i in items}:
                owners.setdefault(module, []).append(index)
        assert owners.keys() == {a, b, c, outside}
        assert all(len(shards) == 1 for shards in owners.values()), owners


def test_conftest_writes_the_collected_and_executed_manifests(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(ci.SHARD_ENV, "2/2")
    monkeypatch.setenv(ci.MANIFEST_ENV, str(tmp_path))
    items = _collection()
    every = sorted(i.nodeid for i in items)
    conftest.pytest_collection_modifyitems(_config(), items)
    assert (tmp_path / ci.MANIFEST_SHARD).read_text() == "2/2\n"
    # collected = the whole suite this runner saw, before the shard filter
    assert (tmp_path / ci.MANIFEST_COLLECTED).read_text().splitlines() == every

    for item in items:
        for when in ("setup", "call", "teardown"):
            conftest.pytest_runtest_logreport(SimpleNamespace(nodeid=item.nodeid, when=when))
    conftest.pytest_runtest_logreport(SimpleNamespace(nodeid=f"{items[0].nodeid}@postgres", when="setup"))
    session = SimpleNamespace(config=SimpleNamespace(option=SimpleNamespace(collectonly=False)))
    conftest.pytest_sessionfinish(session, 0)
    assert (tmp_path / ci.MANIFEST_EXECUTED).read_text().splitlines() == sorted(i.nodeid for i in items)


@pytest.mark.parametrize("worker,collectonly", [(True, False), (False, True)])
def test_workers_and_the_collect_only_pass_write_no_executed_manifest(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, worker: bool, collectonly: bool,
) -> None:
    monkeypatch.setenv(ci.SHARD_ENV, "1/2")
    monkeypatch.setenv(ci.MANIFEST_ENV, str(tmp_path))
    config = SimpleNamespace(option=SimpleNamespace(collectonly=collectonly))
    if worker:
        config.workerinput = {"workerid": "gw0"}
    conftest.pytest_sessionfinish(SimpleNamespace(config=config), 0)
    assert not (tmp_path / ci.MANIFEST_EXECUTED).exists()


def test_only_worker_gw0_records_the_collection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(ci.SHARD_ENV, "1/2")
    monkeypatch.setenv(ci.MANIFEST_ENV, str(tmp_path))
    gw1 = _config(worker=True)
    gw1.workerinput = {"workerid": "gw1"}
    conftest.pytest_collection_modifyitems(gw1, _collection())
    assert not (tmp_path / ci.MANIFEST_COLLECTED).exists()
    gw0 = _config(worker=True)
    gw0.workerinput = {"workerid": "gw0"}
    conftest.pytest_collection_modifyitems(gw0, _collection())
    assert (tmp_path / ci.MANIFEST_COLLECTED).exists()


# --------------------------------------------------------------------------
# tree_assignment: ownership from the checked-out tree
# --------------------------------------------------------------------------


@pytest.mark.parametrize("total", [2, 3])
def test_tree_assignment_owns_every_test_file_on_disk_once(total: int) -> None:
    files = ci.tree_test_files(REPO)
    assert "tests/test_ci_scheduling.py" in files and all(f.startswith("tests/") for f in files)
    owner = ci.tree_assignment(REPO, total)
    assert set(owner) == set(files)
    assert set(owner.values()) == set(range(1, total + 1))


def test_tree_assignment_reads_only_the_tree(tmp_path: Path) -> None:
    for rel, body in {
        "tests/test_a.py": "def test_1():\n    pass\ndef test_2():\n    pass\n",
        "tests/sub/test_b.py": "async def test_x():\n    pass\n",
        "tests/sub/c_test.py": "class T:\n    def test_y(self):\n        pass\n",
        "tests/helpers.py": "def test_not_a_test_file():\n    pass\n",
    }.items():
        _write(tmp_path, rel, body)
    assert ci.tree_test_files(tmp_path) == ["tests/sub/c_test.py", "tests/sub/test_b.py", "tests/test_a.py"]
    assert ci.source_test_count(tmp_path / "tests/test_a.py") == 2
    assert ci.source_test_count(tmp_path / "tests/helpers_missing.py") == 1
    first = ci.tree_assignment(tmp_path, 2)
    ci.tree_assignment.cache_clear()
    assert ci.tree_assignment(tmp_path, 2) == first


def test_shard_of_falls_back_to_a_stable_hash_off_the_tree() -> None:
    owner = {"tests/test_a.py": 2}
    assert ci.shard_of("tests/test_a.py", owner, 2) == 2
    off_tree = [ci.shard_of(f"tests/gen/test_{k}.py", owner, 3) for k in range(30)]
    assert off_tree == [ci.shard_of(f"tests/gen/test_{k}.py", {}, 3) for k in range(30)]
    assert set(off_tree) <= {1, 2, 3} and len(set(off_tree)) > 1


# --------------------------------------------------------------------------
# verify_manifests: the gate's coverage proof
# --------------------------------------------------------------------------


def _manifest(root: Path, name: str, shard: str, collected: list[str], executed: list[str]) -> Path:
    directory = root / name
    ci.write_manifest_lines(directory, ci.MANIFEST_SHARD, [shard])
    ci.write_manifest_lines(directory, ci.MANIFEST_COLLECTED, collected)
    ci.write_manifest_lines(directory, ci.MANIFEST_EXECUTED, executed)
    return directory


SUITE = ["tests/test_a.py::t1", "tests/test_a.py::t2", "tests/test_b.py::t1", "tests/test_c.py::t1"]


def test_verify_accepts_an_exhaustive_disjoint_split(tmp_path: Path) -> None:
    dirs = [
        _manifest(tmp_path, "m1", "1/2", SUITE, SUITE[:2]),
        _manifest(tmp_path, "m2", "2/2", SUITE, SUITE[2:]),
    ]
    problems, summary = ci.verify_manifests(dirs, 2)
    assert problems == []
    assert "4 distinct ran, 4 collected" in summary
    assert ci.main(["verify", "--shards", "2", *map(str, dirs)]) == 0


@pytest.mark.parametrize("case,expected", [
    ("missed", "ran in no shard"),
    ("twice", "ran in shard 1 and shard 2"),
    ("different suite", "collected a different suite"),
    ("extra", "ran that no shard collected"),
    ("same shard twice", "appears twice"),
    ("wrong total", "expected one of 2"),
    ("one shard only", "no manifest for shard(s) [2]"),
    ("unreadable", "unreadable manifest"),
])
def test_verify_rejects_every_broken_split(tmp_path: Path, case: str, expected: str) -> None:
    one = (SUITE, SUITE[:2])
    two = (SUITE, SUITE[2:])
    shards = {"1/2": one, "2/2": two}
    if case == "missed":
        shards["2/2"] = (SUITE, SUITE[2:3])
    elif case == "twice":
        shards["2/2"] = (SUITE, SUITE[1:])
    elif case == "different suite":
        shards["2/2"] = (SUITE + ["tests/test_d.py::t1"], SUITE[2:] + ["tests/test_d.py::t1"])
    elif case == "extra":
        shards["2/2"] = (SUITE, SUITE[2:] + ["tests/test_z.py::t9"])
    dirs = [_manifest(tmp_path, f"m{i}", shard, *lists) for i, (shard, lists) in enumerate(shards.items())]
    if case == "same shard twice":
        dirs[1] = _manifest(tmp_path, "dup", "1/2", *two)
    elif case == "wrong total":
        dirs[1] = _manifest(tmp_path, "w", "2/3", *two)
    elif case == "one shard only":
        dirs = dirs[:1]
    elif case == "unreadable":
        dirs.append(tmp_path / "missing")
    problems, _ = ci.verify_manifests(dirs, 2)
    assert any(expected in p for p in problems), problems
    assert ci.main(["verify", "--shards", "2", *map(str, dirs)]) == 1


def test_the_gate_catches_runners_that_collected_differently(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """End to end through the conftest hooks: two runners, different collections -> the gate fails."""
    base = _collection()
    variants = {1: base, 2: base + [SimpleNamespace(nodeid=f"{base[0].nodeid}_extra")]}
    dirs = []
    for index, collection in variants.items():
        out = tmp_path / f"s{index}"
        monkeypatch.setenv(ci.SHARD_ENV, f"{index}/2")
        monkeypatch.setenv(ci.MANIFEST_ENV, str(out))
        monkeypatch.setattr(conftest, "_EXECUTED", set())
        items = list(collection)
        conftest.pytest_collection_modifyitems(_config(), items)
        for item in items:
            conftest.pytest_runtest_logreport(SimpleNamespace(nodeid=item.nodeid, when="setup"))
        conftest.pytest_sessionfinish(SimpleNamespace(config=SimpleNamespace(option=SimpleNamespace(collectonly=False))), 0)
        dirs.append(out)
    problems, _ = ci.verify_manifests(dirs, 2)
    assert any("collected a different suite" in p for p in problems), problems
    # and with identical collections the same path proves the split
    ci.write_manifest_lines(dirs[1], ci.MANIFEST_COLLECTED,
                            (tmp_path / "s1" / ci.MANIFEST_COLLECTED).read_text().splitlines())
    executed_2 = set((dirs[1] / ci.MANIFEST_EXECUTED).read_text().splitlines()) - {f"{base[0].nodeid}_extra"}
    ci.write_manifest_lines(dirs[1], ci.MANIFEST_EXECUTED, executed_2)
    assert ci.verify_manifests(dirs, 2)[0] == []


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


def _assert_sharded_suite(workflow: dict[str, Any], suite: str, gate_job: str, artifact: str) -> None:
    """The suite job's matrix, its GRID_TEST_SHARD and the gate job's `verify --shards` agree,
    every shard uploads its manifest and the gate verifies all of them, and each shard runs
    the whole tests/ tree under GRID's xdist units, unfiltered."""
    job = workflow["jobs"][suite]
    shards = job["strategy"]["matrix"]["shard"]
    assert shards == list(range(1, len(shards) + 1)) and len(shards) >= 2
    assert job["strategy"]["fail-fast"] is False
    run = next(step for step in job["steps"] if step.get("name") == "Run tests")
    assert run["env"][ci.SHARD_ENV] == f"${{{{ matrix.shard }}}}/{len(shards)}"
    manifest_dir = run["env"][ci.MANIFEST_ENV]
    upload = next(step for step in job["steps"] if step.get("name") == "Upload shard manifest")
    assert upload["uses"].startswith("actions/upload-artifact@")
    assert upload["if"] == "always()"
    assert upload["with"] == {
        "name": f"{artifact}-${{{{ matrix.shard }}}}", "path": manifest_dir,
        "if-no-files-found": "error", "retention-days": 7,
    }
    gate = workflow["jobs"][gate_job]["steps"]
    download = next(step for step in gate if step.get("name") == "Download suite shard manifests")
    assert download["uses"].startswith("actions/download-artifact@")
    assert download["with"]["pattern"] == f"{artifact}-*"
    verify = next(step for step in gate if step.get("name") == "Suite shards ran every collected test exactly once")
    assert verify["run"] == (
        f"python -m tests.ci_scheduling verify --shards {len(shards)} "
        f"{download['with']['path']}/{artifact}-*"
    )
    names = [step.get("name") or step.get("uses") for step in gate]
    assert names.index("Download suite shard manifests") < names.index("Suite shards ran every collected test exactly once")
    # the shard runs the whole tests/ tree under GRID's xdist units, unfiltered
    assert "python -m pytest tests/ -v --tb=short --maxfail=50 --timeout=180" in run["run"]
    assert "-n auto --dist loadgroup" in run["run"]
    options = run["run"].replace("python -m pytest", "python_m_pytest")
    narrowing = re.findall(r"\s(-k|-m|--deselect|--ignore(?:-glob)?|--lf|--last-failed|--sw|--stepwise)\b", options)
    assert not narrowing, narrowing


def test_suite_matrix_covers_every_shard_once(workflow: dict[str, Any]) -> None:
    _assert_sharded_suite(workflow, "backend-tests-suite", "backend-tests", "backend-suite-manifest")


def test_backend_tests_fails_unless_every_suite_shard_passed(workflow: dict[str, Any]) -> None:
    job = workflow["jobs"]["backend-tests"]
    assert job["name"] == "Backend Tests"
    assert job["needs"] == ["backend-tests-suite", "backend-tests-contracts", "backend-tests-pit-postgres"]
    # runs (and fails) when a shard or the contracts failed, but not when the run was cancelled
    assert job["if"] == "${{ !cancelled() }}"
    gate = job["steps"][1]
    assert gate["name"] == "Suite shards and contracts passed"
    assert gate["env"]["SUITE_RESULT"] == "${{ needs.backend-tests-suite.result }}"
    assert '[ "$SUITE_RESULT" != "success" ]' in gate["run"] and "exit 1" in gate["run"]
    # neither the main suite nor the contracts run here; the #1352 witness does
    names = [step.get("name") for step in job["steps"]]
    assert "Run tests" not in names
    assert "Option B release witness (#1352)" in names


# --------------------------------------------------------------------------
# Python 3.14 workflow wiring
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def py314_workflow() -> dict[str, Any]:
    return yaml.safe_load(PY314_WORKFLOW.read_text(encoding="utf-8"))


def test_py314_suite_matrix_covers_every_shard_once(py314_workflow: dict[str, Any]) -> None:
    _assert_sharded_suite(py314_workflow, "backend-tests-py314-suite", "backend-tests-py314", "py314-suite-manifest")
    job = py314_workflow["jobs"]["backend-tests-py314-suite"]
    # always hosted: each shard needs its own VM and its own ephemeral Postgres
    assert job["runs-on"] == "ubuntu-latest"
    setup = next(step for step in job["steps"] if step.get("name") == "Set up Python")
    assert setup["with"]["python-version"] == "3.14"
    # the same suite command as GRID Tests' shards: a drift between the two would make one of them prove less
    def pytest_command(steps: list[dict[str, Any]]) -> str:
        script = next(step for step in steps if step.get("name") == "Run tests")["run"]
        lines = script.splitlines()
        first = next(i for i, line in enumerate(lines) if "python -m pytest tests/ -v --tb=short --maxfail" in line)
        command = []
        for line in lines[first:]:
            command.append(line.strip().rstrip("\\").strip())
            if not line.rstrip().endswith("\\"):
                break
        return " ".join(part for part in command if not part.startswith(">"))

    grid_tests = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]["backend-tests-suite"]["steps"]
    assert pytest_command(job["steps"]) == pytest_command(grid_tests)


def test_backend_tests_py314_fails_unless_every_suite_shard_passed(py314_workflow: dict[str, Any]) -> None:
    jobs = py314_workflow["jobs"]
    job = jobs["backend-tests-py314"]
    # the #1352 release check pins this exact check name to python314.yml / backend-tests-py314
    assert job["name"] == "Backend Tests (Python 3.14)"
    assert "strategy" not in job   # one check run under that name, not a matrix of them
    assert job["needs"] == "backend-tests-py314-suite"
    # runs (and fails) when a shard failed, but not when the run was cancelled
    assert job["if"] == "${{ !cancelled() }}"
    assert job["runs-on"] == "ubuntu-latest"
    steps = job["steps"]
    # the tested checkout first (that check's workflow guard binds the job to it), then the gate
    assert steps[0]["uses"].startswith("actions/checkout@") and "with" not in steps[0] and "if" not in steps[0]
    gate = steps[1]
    assert gate["name"] == "Python 3.14 suite shards passed"
    assert "if" not in gate
    assert gate["env"]["SUITE_RESULT"] == "${{ needs.backend-tests-py314-suite.result }}"
    assert '[ "$SUITE_RESULT" != "success" ]' in gate["run"] and "exit 1" in gate["run"]
    # the manifests land in a fresh directory of this job, never a fixed path a runner could keep
    download = next(step for step in steps if step.get("name") == "Download suite shard manifests")
    assert download["with"]["path"].startswith("${{ runner.temp }}/")
    # the suite no longer runs here, and nothing else in the workflow runs it unsharded
    assert "Run tests" not in [step.get("name") for step in steps]
    assert set(jobs) == {"backend-tests-py314-suite", "backend-tests-py314"}
