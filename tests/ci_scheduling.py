"""How the GRID test suite is split across CI jobs and pytest-xdist workers.

``tests/conftest.py`` wires these helpers into pytest. Two levels of
parallelism:

* **Shards (CI jobs).** ``GRID_TEST_SHARD=i/n`` keeps only the test files that
  shard ``i`` of ``n`` owns and deselects the rest. Which shard owns a file is
  decided from the checked-out tree alone (:func:`tree_assignment`: the test
  files on disk and their source), never from what a runner happened to
  collect, so two runners that collect differently still agree on every
  file's owner. A file never straddles two shards, so its module fixtures and
  test order are what they would be in a single run. Each shard job runs on
  its own hosted VM with its own ephemeral PostgreSQL.

  Each shard also writes a manifest (``GRID_TEST_MANIFEST_DIR``): its shard
  id, every node id it collected before the shard filter, and every node id
  it actually ran. The required "Backend Tests" job checks the manifests with
  ``python -m tests.ci_scheduling verify`` (:func:`verify_manifests`): every
  shard ran, all collected the same suite, no test ran twice and none was
  missed. So a green gate proves the partition, it does not assume it.

* **Workers (pytest-xdist, inside one job).** ``-n auto --dist loadgroup``
  uses :func:`scope_for` to decide which tests may run at the same time. Tests
  that can reach a live PostgreSQL share one ``postgres`` unit and run one
  after another on one worker: they share a single database, write to the
  public schema and do not roll back per test. Files in :data:`SPREAD_FILES`
  are split into one unit per test. Every other file is one unit, which keeps
  its module fixtures and intra-file order (``--dist loadfile`` semantics).

The worker grouping used to be applied with ``xdist_group`` markers. That
stopped working with pytest 9.2.0: xdist 3.8.0 (the latest release) appends
the group to ``item._nodeid``, but pytest 9.2 computes ``nodeid`` from
``item._id``, so the suffix is never seen and ``loadgroup`` silently ran as
plain ``load`` (CI logs: every run on or after 2026-11-05 shows no ``@group``
suffix, while runs on pytest 9.1.1 do). The grouping now lives in a scheduler
on the xdist controller (``pytest_xdist_make_scheduler`` in conftest.py),
which reads only node ids and the test files' source.

Units are dispatched heaviest first (:func:`dispatch_rank`): the ``postgres``
unit, then the files in :data:`HEAVY_SECONDS` from the slowest, then
everything else in collection order. Without that, xdist's default ordering
(units by test count, largest first) sends a slow file with few tests near
the end of the run, where it becomes the tail of the whole job.
"""

from __future__ import annotations

import argparse
import re
import sys
import zlib
from collections.abc import Iterable, Mapping, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any

SHARD_ENV = "GRID_TEST_SHARD"
MANIFEST_ENV = "GRID_TEST_MANIFEST_DIR"
POSTGRES_GROUP = "postgres"

# Files in a shard's manifest directory.
MANIFEST_SHARD = "shard.txt"          # "i/n"
MANIFEST_COLLECTED = "collected.txt"  # every node id collected, before the shard filter
MANIFEST_EXECUTED = "executed.txt"    # every node id that ran (has a setup report)

# A module containing any of these can reach a live database. Membership is
# detected by scanning the module source, so a new database test file is
# picked up on its own; a false positive only costs a little parallelism,
# which is the right direction for this to fail in. The last two catch the
# files that declare the group themselves with an explicit marker.
DB_TOKENS = (
    "pg_engine",
    "live_engine",
    "get_engine(",
    "get_connection(",
    'xdist_group("postgres")',
    "xdist_group('postgres')",
)

# Seconds of test time per file (junit: setup + call + teardown, summed over
# a shard's four xdist workers) from GRID Tests run 45777773901 on 2026-11-26,
# whose two shards ran on 4-vCPU AMD EPYC 9V74 and 7763 runners; every file
# at 30 s or more. Used for two things only: balancing the shards and
# dispatching heavy units first. A stale, missing or wrong entry makes the
# shards less even or the run's tail longer. It never drops or repeats a test
# (shard ownership is a partition whatever the weights, and the Backend Tests
# job verifies the manifests). Refresh from the "test time by file" group in
# the shard logs.
HEAVY_SECONDS: dict[str, float] = {
    "tests/test_e0_calibration.py": 552.0,
    "tests/test_fp_validation_gates.py": 404.0,
    "tests/test_factory_live_parity.py": 268.0,
    "tests/test_alpha_signals.py": 228.0,
    "tests/test_gauntlet_determinism.py": 150.0,
    "tests/test_ledger_steered_exploration.py": 144.0,
    "tests/test_d0_admission.py": 121.0,
    "tests/test_e0v2_benchmark.py": 92.0,
    "tests/test_options_raw_read_guard.py": 88.0,
    "tests/test_e3_gates.py": 67.0,
    "tests/test_high_sharpe_ensemble.py": 62.0,
    "tests/test_oracle_predictions_read_guard.py": 51.0,
    "tests/test_generalization_gate.py": 50.0,
    "tests/test_cross_section_factory.py": 45.0,
    "tests/test_source_eia.py": 43.0,
    "tests/test_taiwan_strait_osint.py": 42.0,
    "tests/test_offline_research_stats_core.py": 41.0,
    "tests/test_e3_funnel.py": 41.0,
    "tests/test_module_factory_options.py": 37.0,
    "tests/test_panel_insider_density.py": 35.0,
    "tests/test_edge_stream.py": 34.0,
    "tests/test_null_farm.py": 31.0,
    "tests/test_panel_insider_density_v9.py": 31.0,
}

# Files whose individual tests are slow enough that keeping the file together
# would make it the critical path. Their tests are spread one at a time like
# plain ``--dist load``. Every entry has no database access and no module- or
# class-scoped fixture shared by more than one test (test_e0_calibration's
# module fixture ``recovery`` serves test_parameter_recovery alone;
# tests/test_ci_scheduling.py checks both). Spreading is also what every file
# had from 2026-11-05 to 2026-11-26, while loadgroup was a no-op, and the suite
# stayed green. test_alpha_signals and test_gauntlet_determinism were the
# first two (run 34554188271: 49.4 s + 42.9 s of gauntlet work in one file,
# and ~91 s); the rest are the heaviest files of run 45777773901, where
# test_parameter_recovery alone was 384 s of fixture setup. test_e3_gates and
# test_high_sharpe_ensemble are heavy but stay together: their module
# fixtures are shared by several tests.
SPREAD_FILES = frozenset({
    "tests/test_alpha_signals.py",
    "tests/test_gauntlet_determinism.py",
    "tests/test_e0_calibration.py",
    "tests/test_fp_validation_gates.py",
    "tests/test_factory_live_parity.py",
    "tests/test_ledger_steered_exploration.py",
    "tests/test_d0_admission.py",
    "tests/test_e0v2_benchmark.py",
    "tests/test_options_raw_read_guard.py",
    "tests/test_oracle_predictions_read_guard.py",
    "tests/test_generalization_gate.py",
    "tests/test_e3_funnel.py",
    "tests/test_e2_manifest_guard.py",
})

# Seconds per test function (as written in the source, before parametrize)
# outside HEAVY_SECONDS: run 45777773901 spent 3,791 s of test time, 2,657 s
# of it in the HEAVY_SECONDS files, and the other files define 20,060 test
# functions.
DEFAULT_SECONDS_PER_TEST = 0.057


def module_of(nodeid: str) -> str:
    """The test file part of a node id (``tests/test_x.py::T::t[p]`` -> ``tests/test_x.py``)."""
    return nodeid.split("::", 1)[0]


def _strip_group_suffix(nodeid: str) -> str:
    # xdist (on pytest < 9.2) appends "@<group>" to the node id of a test with
    # an xdist_group marker. A parametrize id can itself contain "@", so, like
    # xdist's own LoadGroupScheduling, only an "@" after the last "]" counts.
    at = nodeid.rfind("@")
    if at > nodeid.rfind("]"):
        return nodeid[:at]
    return nodeid


@lru_cache(maxsize=None)
def module_touches_db(path: str) -> bool:
    """True if the test module at ``path`` can reach a live database."""
    if not path:
        return False
    try:
        source = Path(path).read_text(encoding="utf-8")
    except OSError:
        # Unreadable module: assume it might touch the database and let it
        # serialise rather than risk a parallel write collision.
        return True
    return any(token in source for token in DB_TOKENS)


def scope_for(nodeid: str, rootpath: Path) -> str:
    """The xdist work unit a test belongs to (tests in one unit run in order on one worker)."""
    nodeid = _strip_group_suffix(nodeid)
    module = module_of(nodeid)
    if module_touches_db(str(rootpath / module)):
        return POSTGRES_GROUP
    if module in SPREAD_FILES:
        return nodeid
    return module


def parse_shard(value: str) -> tuple[int, int]:
    """Parse ``"i/n"`` (1 <= i <= n). Anything else raises ValueError, so a typo fails the run."""
    try:
        index_text, total_text = value.strip().split("/")
        index, total = int(index_text), int(total_text)
    except ValueError as exc:
        raise ValueError(f"{SHARD_ENV} must look like '1/2', got {value!r}") from exc
    if not 1 <= index <= total:
        raise ValueError(f"{SHARD_ENV}={value!r}: need 1 <= shard <= total")
    return index, total


def file_weights(test_counts: Mapping[str, int]) -> dict[str, float]:
    """Estimated seconds per file: the measured figure if known, else test count x average."""
    return {
        module: HEAVY_SECONDS.get(module, count * DEFAULT_SECONDS_PER_TEST)
        for module, count in test_counts.items()
    }


def assign_shards(weights: Mapping[str, float], db_files: Iterable[str], total: int) -> dict[str, int]:
    """Give every file in ``weights`` exactly one shard (1-based), balancing estimated time.

    Greedy, heaviest first, ties broken by path, so every shard computes the
    same answer from the same collection. Database files are placed first and
    balanced among themselves: each shard runs its database files one after
    another on one worker, so that chain is what has to be even.
    """
    if total < 1:
        raise ValueError("total must be >= 1")
    db = set(db_files) & set(weights)
    load = [0.0] * total
    db_load = [0.0] * total
    owner: dict[str, int] = {}
    for module in sorted(db, key=lambda m: (-weights[m], m)):
        shard = min(range(total), key=lambda k: (db_load[k], load[k], k))
        owner[module] = shard + 1
        db_load[shard] += weights[module]
        load[shard] += weights[module]
    for module in sorted(set(weights) - db, key=lambda m: (-weights[m], m)):
        shard = min(range(total), key=lambda k: (load[k], k))
        owner[module] = shard + 1
        load[shard] += weights[module]
    return owner


_TEST_DEF = re.compile(r"^[ \t]*(?:async[ \t]+)?def[ \t]+test", re.MULTILINE)


def source_test_count(path: Path) -> int:
    """Test functions defined in a file's source (at least 1): a weight that every runner reads alike."""
    try:
        return max(1, len(_TEST_DEF.findall(path.read_text(encoding="utf-8"))))
    except OSError:
        return 1


def tree_test_files(rootpath: Path) -> list[str]:
    """Every test file under ``tests/`` (pytest's default ``test_*.py`` / ``*_test.py``), sorted."""
    tests = rootpath / "tests"
    found = {p for pattern in ("test_*.py", "*_test.py") for p in tests.rglob(pattern) if p.is_file()}
    return sorted(p.relative_to(rootpath).as_posix() for p in found)


@lru_cache(maxsize=None)
def tree_assignment(rootpath: Path, total: int) -> dict[str, int]:
    """The shard of every test file on disk, from the tree alone (paths, source, HEAVY_SECONDS).

    The same checkout gives the same answer on every runner, whatever each
    one collects. Weights: HEAVY_SECONDS if measured, else the number of test
    functions in the source times DEFAULT_SECONDS_PER_TEST.
    """
    files = tree_test_files(rootpath)
    counts = {f: source_test_count(rootpath / f) for f in files}
    db_files = {f for f in files if module_touches_db(str(rootpath / f))}
    return assign_shards(file_weights(counts), db_files, total)


def shard_of(module: str, assignment: Mapping[str, int], total: int) -> int:
    """The shard that owns ``module``: its tree assignment, else a stable hash of its path.

    The hash covers a collected module that is not a ``tests/`` test file on
    disk (none today); it depends on the path alone, so runners still agree.
    """
    owner = assignment.get(module)
    if owner is not None:
        return owner
    return zlib.crc32(module.encode("utf-8")) % total + 1


def dispatch_rank(module: str, is_db: bool) -> tuple[int, float]:
    """Sort key for the collection: the postgres unit, then heavy files slowest first, then the rest.

    Sorting is stable, so tests keep their order inside a file and files of
    equal rank keep their collection order.
    """
    if is_db:
        return (0, 0.0)
    if module in HEAVY_SECONDS:
        return (1, -HEAVY_SECONDS[module])
    return (2, 0.0)


def make_scheduler(config: Any, log: Any) -> Any:
    """The xdist scheduler for ``--dist loadgroup``: units from :func:`scope_for`, in collection order."""
    from xdist.scheduler import LoadScopeScheduling

    rootpath = Path(config.rootpath)

    class GridLoadGroupScheduling(LoadScopeScheduling):
        def _split_scope(self, nodeid: str) -> str:
            return scope_for(nodeid, rootpath)

    # The collection is already in dispatch order (dispatch_rank, applied on
    # every worker by conftest.py); xdist's default reorder by test count
    # would undo it.
    config.option.loadscopereorder = False
    return GridLoadGroupScheduling(config, log)


# --------------------------------------------------------------------------
# Shard manifests: proof that the shards ran the whole suite exactly once
# --------------------------------------------------------------------------


def node_key(nodeid: str) -> str:
    """A node id as the manifests record it (without an xdist ``@group`` suffix)."""
    return _strip_group_suffix(nodeid)


def write_manifest_lines(directory: Path, name: str, lines: Iterable[str]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text("".join(f"{line}\n" for line in sorted(set(lines))), encoding="utf-8")


def _read_lines(path: Path) -> set[str]:
    return {line for line in path.read_text(encoding="utf-8").splitlines() if line}


def verify_manifests(directories: Sequence[Path], total: int) -> tuple[list[str], str]:
    """Check shard manifests: (problems, summary). No problems means exhaustive, disjoint coverage.

    Every shard 1..total must be present exactly once; every shard must have
    collected the same node ids (before its shard filter); the node ids the
    shards ran must not overlap; together they must be exactly that suite.
    """
    problems: list[str] = []
    shards: dict[int, tuple[set[str], set[str]]] = {}
    for directory in directories:
        try:
            shard_text = (directory / MANIFEST_SHARD).read_text(encoding="utf-8").strip()
            index, of = parse_shard(shard_text)
            collected = _read_lines(directory / MANIFEST_COLLECTED)
            executed = _read_lines(directory / MANIFEST_EXECUTED)
        except (OSError, ValueError) as exc:
            problems.append(f"{directory}: unreadable manifest ({exc})")
            continue
        if of != total:
            problems.append(f"{directory}: shard {shard_text}, expected one of {total}")
        if index in shards:
            problems.append(f"{directory}: shard {index}/{of} appears twice")
        shards[index] = (collected, executed)
    missing_shards = sorted(set(range(1, total + 1)) - set(shards))
    if missing_shards:
        problems.append(f"no manifest for shard(s) {missing_shards} of {total}")
    if not shards:
        return problems, "no manifests"

    suites = {index: collected for index, (collected, _) in shards.items()}
    reference_index = min(suites)
    reference = suites[reference_index]
    for index, collected in sorted(suites.items()):
        if collected != reference:
            only_here = sorted(collected - reference)
            only_there = sorted(reference - collected)
            problems.append(
                f"shard {index} collected a different suite from shard {reference_index}: "
                f"{len(only_here)} only in {index} (e.g. {only_here[:3]}), "
                f"{len(only_there)} only in {reference_index} (e.g. {only_there[:3]})"
            )
    suite = set().union(*suites.values())

    seen: dict[str, int] = {}
    for index, (_, executed) in sorted(shards.items()):
        twice = sorted(n for n in executed if n in seen)
        if twice:
            problems.append(f"{len(twice)} test(s) ran in shard {seen[twice[0]]} and shard {index} (e.g. {twice[:3]})")
        for nodeid in executed:
            seen.setdefault(nodeid, index)
    ran = set(seen)
    missed = sorted(suite - ran)
    extra = sorted(ran - suite)
    if missed:
        problems.append(f"{len(missed)} collected test(s) ran in no shard (e.g. {missed[:3]})")
    if extra:
        problems.append(f"{len(extra)} test(s) ran that no shard collected (e.g. {extra[:3]})")
    per_shard = ", ".join(f"{i}/{total}: {len(executed)} ran" for i, (_, executed) in sorted(shards.items()))
    summary = f"{per_shard}; {len(ran)} distinct ran, {len(suite)} collected"
    return problems, summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tests.ci_scheduling")
    sub = parser.add_subparsers(dest="command", required=True)
    verify = sub.add_parser("verify", help="check shard manifests for exhaustive, disjoint coverage")
    verify.add_argument("--shards", type=int, required=True, help="how many shards the matrix runs")
    verify.add_argument("directories", nargs="+", type=Path)
    args = parser.parse_args(argv)
    problems, summary = verify_manifests(args.directories, args.shards)
    print(f"shard manifests: {summary}")
    for problem in problems:
        print(f"::error::shard coverage: {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
