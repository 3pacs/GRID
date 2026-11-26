"""How the GRID test suite is split across CI jobs and pytest-xdist workers.

``tests/conftest.py`` wires these helpers into pytest. Two levels of
parallelism:

* **Shards (CI jobs).** ``GRID_TEST_SHARD=i/n`` keeps only the test files that
  shard ``i`` of ``n`` owns and deselects the rest. Every collected file goes
  to exactly one shard (:func:`assign_shards` is a partition), so the shards
  together run the whole suite once. A file never straddles two shards, so its
  module fixtures and its test order are what they would be in a single run.
  Each hosted shard job gets its own VM and its own ephemeral PostgreSQL.

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

from collections.abc import Iterable, Mapping
from functools import lru_cache
from pathlib import Path
from typing import Any

SHARD_ENV = "GRID_TEST_SHARD"
POSTGRES_GROUP = "postgres"

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

# Seconds of test time (setup + call) per file on a 4-vCPU ubuntu-latest
# runner running four xdist workers, read from the ``--durations`` output of
# GRID Tests and Python 3.14 runs on 2026-11-26 (e.g. runs 45768313133,
# 45764592902, 45769489164). Lower bounds: only tests that reached a
# slowest-25 list are counted. Used for two things only: balancing the shards
# and dispatching heavy units first. A stale, missing or wrong entry makes the
# shards less even or the run's tail longer. It never drops or repeats a test.
HEAVY_SECONDS: dict[str, float] = {
    "tests/test_e0_calibration.py": 615.0,
    "tests/test_fp_validation_gates.py": 405.0,
    "tests/test_gauntlet_determinism.py": 187.0,
    "tests/test_e3_gates.py": 138.0,
    "tests/test_alpha_signals.py": 100.0,
    "tests/test_d0_admission.py": 73.0,
    "tests/test_options_raw_read_guard.py": 55.0,
    "tests/test_e0v2_benchmark.py": 47.0,
    "tests/test_e3_funnel.py": 46.0,
    "tests/test_e0_ic01_recovery.py": 41.0,
    "tests/test_e2_manifest_guard.py": 37.0,
    "tests/test_ledger_steered_exploration.py": 37.0,
    "tests/test_no_literal_secrets.py": 32.0,
}

# Files whose individual tests are slow enough that keeping the file together
# would make it the critical path. Their tests are spread one at a time like
# plain ``--dist load``. Every entry has no database access and no module- or
# class-scoped fixture shared by more than one test (test_e0_calibration's
# module fixture ``recovery`` serves test_parameter_recovery alone), so
# spreading them is safe. test_alpha_signals and test_gauntlet_determinism
# were measured first on run 34554188271 (49.4 s + 42.9 s of gauntlet work in
# one file, and ~91 s); the rest come from the 2026-11-26 runs above, where
# test_parameter_recovery alone was ~410 s and test_no_db ~205 s of one file.
# test_e3_gates is heavy but stays together: its two module fixtures are
# shared by several tests.
SPREAD_FILES = frozenset({
    "tests/test_alpha_signals.py",
    "tests/test_gauntlet_determinism.py",
    "tests/test_e0_calibration.py",
    "tests/test_fp_validation_gates.py",
    "tests/test_d0_admission.py",
    "tests/test_e0v2_benchmark.py",
    "tests/test_e3_funnel.py",
    "tests/test_e2_manifest_guard.py",
})

# Average test time outside HEAVY_SECONDS, used to weigh files by their test
# count: run 45768313133 spent ~4,340 s of test time (1,084 s on 4 workers),
# ~1,810 s of it in HEAVY_SECONDS files, over ~27,500 other tests.
DEFAULT_SECONDS_PER_TEST = 0.09


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
