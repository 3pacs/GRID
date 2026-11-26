"""Deterministic split of the backend test files across CI jobs.

``.github/workflows/test.yml`` (and ``python314.yml``) run the suite as N
parallel jobs, each with ``GRID_TEST_SHARD=<index>/<N>``. ``tests/conftest.py``
then skips collecting every test file that belongs to another shard, so a
shard neither imports nor runs them. The unit is the file: a module's tests
stay together on one runner, which keeps module-scoped fixtures and the
``xdist_group`` rules in conftest.py exactly as they are inside one job.

A file's shard depends only on its path relative to the repository root, so
the split is the same on every runner, needs no list to maintain, and a new
test file lands in exactly one shard on its own.
"""

from __future__ import annotations

import fnmatch
import zlib

SHARD_ENV = "GRID_TEST_SHARD"

# pytest's default ``python_files``; pytest.ini does not override it.
TEST_FILE_PATTERNS = ("test_*.py", "*_test.py")


def parse_shard(spec: str) -> tuple[int, int]:
    """``"2/3"`` -> ``(2, 3)``; raises ValueError for anything else."""
    try:
        index_text, total_text = spec.split("/")
        index, total = int(index_text), int(total_text)
    except ValueError:
        raise ValueError(f"{SHARD_ENV} must look like '2/3', got {spec!r}") from None
    if not 1 <= index <= total:
        raise ValueError(f"{SHARD_ENV} index must be within 1..total, got {spec!r}")
    return index, total


def is_test_file(name: str) -> bool:
    return any(fnmatch.fnmatch(name, pattern) for pattern in TEST_FILE_PATTERNS)


# The few files that dominate the suite's CPU time, placed by hand so one shard
# does not draw them all (a path hash alone put ~25 of ~47 CPU-minutes in one
# shard). Measured on ubuntu-latest, mean CPU-minutes per run, from the
# `--durations` lists of ten green runs on 2026-11-26. Valid for the 3-shard split CI
# uses; any other shard count falls back to the hash. This table is a balance
# hint only: a stale or missing entry changes how long a shard takes, never which
# tests run, because every file is still owned by exactly one shard.
_PINNED_FOR_3 = {
    "tests/test_e0_calibration.py": 1,       # 9.4: a 6-7 min fixture + a 3 min test
    "tests/test_fp_validation_gates.py": 2,  # 5.0
    "tests/test_e3_gates.py": 3,             # 2.2
    "tests/test_alpha_signals.py": 3,        # 1.0
    "tests/test_d0_admission.py": 3,         # 0.9
}


def shard_of(rel_path: str, total: int) -> int:
    """The 1-based shard that owns the test file at ``rel_path`` (posix, repo-relative)."""
    if total == 3 and rel_path in _PINNED_FOR_3:
        return _PINNED_FOR_3[rel_path]
    return zlib.crc32(rel_path.encode("utf-8")) % total + 1
