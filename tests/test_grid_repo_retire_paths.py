"""Release-tree code must not depend on the old grid_repo checkout.

grid-svr's /data/grid_v4/grid_repo (== /home/grid/grid_v4/grid_repo) is main
from 2026-09-11 and is being moved aside. Code that put it on sys.path, read
FinBERT weights or wrote images inside it, or defaulted to its .env would break
or quietly run stale code once it moves. The shared env file is
/etc/grid/grid.env since 2026-11-17 (grid_repo/.env is a symlink to it).
"""

from __future__ import annotations

import ast
import importlib
import importlib.util
import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
NEW_ENV = "/etc/grid/grid.env"
LEGACY_ENV = "/home/grid/grid_v4/grid_repo/.env"

needs_posix_bash = pytest.mark.skipif(
    os.name == "nt" or shutil.which("bash") is None, reason="needs a POSIX bash"
)

# Files whose sys.path handling used to name the old checkout.
SYS_PATH_FILES = (
    "scripts/run_forensics_batch.py",
    "scripts/run_intelligence_cycles.py",
    "scripts/paper_trading_review.py",
    "scripts/score_chokepoints.py",
    "scripts/activate_v2_mappings.py",
    "scripts/score_oracle_trades.py",
    "ingestion/openbb_pipeline.py",
)

# Files whose operator hints / usage text used to cd into the old checkout.
HINT_FILES = (
    "scripts/run_full_pipeline.py",
    "scripts/backfill_celestial_ephemeris.py",
    "ingestion/scheduler.py",
)


def _string_constants(node: ast.AST) -> list[str]:
    return [
        n.value
        for n in ast.walk(node)
        if isinstance(n, ast.Constant) and isinstance(n.value, str)
    ]


def _is_sys_path_insert(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "insert"
        and isinstance(node.func.value, ast.Attribute)
        and node.func.value.attr == "path"
        and isinstance(node.func.value.value, ast.Name)
        and node.func.value.value.id == "sys"
    )


@pytest.mark.parametrize("rel", SYS_PATH_FILES)
def test_sys_path_never_names_the_old_checkout(rel: str) -> None:
    path = REPO / rel
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    offenders = []
    for node in ast.walk(tree):
        # sys.path.insert(...) calls, and the candidate tuples looped over
        # just before them (score_chokepoints.py).
        if _is_sys_path_insert(node) or isinstance(node, ast.For):
            offenders += [s for s in _string_constants(node) if "grid_repo" in s]
    assert offenders == [], f"{rel} still puts the old checkout on sys.path: {offenders}"


@pytest.mark.parametrize("rel", HINT_FILES)
def test_operator_hints_point_at_the_release_tree(rel: str) -> None:
    text = (REPO / rel).read_text(encoding="utf-8")
    assert "cd /data/grid_v4/grid_repo" not in text


@pytest.mark.parametrize(
    "rel", ("scripts/run_intelligence_cycles.py", "scripts/score_oracle_trades.py")
)
def test_guarded_fallback_is_this_repos_root(rel: str) -> None:
    """The __main__-only fallback now inserts the file's own repo root."""
    path = REPO / rel
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    inserts = [n for n in ast.walk(tree) if _is_sys_path_insert(n)]
    assert inserts, f"{rel}: expected the guarded sys.path fallback"
    for call in inserts:
        expr = ast.unparse(call.args[1])
        assert expr == "os.path.dirname(os.path.dirname(os.path.abspath(__file__)))"
    # ...which, evaluated for this file, is the checkout the test runs in.
    assert os.path.dirname(os.path.dirname(os.path.abspath(path))) == str(REPO)


# ── env file defaults ────────────────────────────────────────────────


def _cron_env_pick(tmp_path: Path, *, default: Path, legacy: Path, override: str | None):
    script = (
        f'source "{REPO / "scripts" / "grid_cron_env.sh"}"\n'
        f'GRID_ENV_FILE_DEFAULT="{default}"\n'
        f'GRID_ENV_FILE_LEGACY="{legacy}"\n'
        "grid_cron_env_file\n"
    )
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path)}
    if override is not None:
        env["GRID_ENV_FILE"] = override
    proc = subprocess.run(
        ["bash", "-c", script], env=env, capture_output=True, text=True, check=True
    )
    return proc.stdout.strip()


def test_grid_cron_env_defaults_to_etc_grid() -> None:
    text = (REPO / "scripts" / "grid_cron_env.sh").read_text(encoding="utf-8")
    assert f'GRID_ENV_FILE_DEFAULT="{NEW_ENV}"' in text
    assert f'GRID_ENV_FILE_LEGACY="{LEGACY_ENV}"' in text


@needs_posix_bash
def test_grid_cron_env_picks_default_then_legacy_then_override(tmp_path: Path) -> None:
    default = tmp_path / "etc-grid.env"
    legacy = tmp_path / "grid_repo.env"
    legacy.write_text("X=1\n", encoding="utf-8")

    # Default missing: the legacy checkout path is used.
    assert _cron_env_pick(tmp_path, default=default, legacy=legacy, override=None) == str(legacy)
    # Default present: it wins over legacy.
    default.write_text("X=2\n", encoding="utf-8")
    assert _cron_env_pick(tmp_path, default=default, legacy=legacy, override=None) == str(default)
    # GRID_ENV_FILE always wins; an empty value means "not set".
    other = str(tmp_path / "other.env")
    assert _cron_env_pick(tmp_path, default=default, legacy=legacy, override=other) == other
    assert _cron_env_pick(tmp_path, default=default, legacy=legacy, override="") == str(default)


@needs_posix_bash
def test_grid_cron_load_env_uses_the_resolved_default(tmp_path: Path) -> None:
    default = tmp_path / "etc-grid.env"
    default.write_text("RETIRE_PROBE=from-default\n", encoding="utf-8")
    script = (
        f'source "{REPO / "scripts" / "grid_cron_env.sh"}"\n'
        f'GRID_ENV_FILE_DEFAULT="{default}"\n'
        f'GRID_ENV_FILE_LEGACY="{tmp_path / "missing.env"}"\n'
        f'GRID_ROOT="{tmp_path}"\n'
        'grid_cron_load_env && echo "$RETIRE_PROBE|$GRID_ENV_FILE"\n'
    )
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path)}
    proc = subprocess.run(
        ["bash", "-c", script], env=env, capture_output=True, text=True, check=False
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == f"from-default|{default}"


def test_deploy_build_hook_tries_etc_grid_before_the_old_checkout() -> None:
    text = (REPO / "scripts" / "deploy_build_hook.sh").read_text(encoding="utf-8")
    assert NEW_ENV in text
    assert text.index(NEW_ENV) < text.index(LEGACY_ENV)


def test_regression_eval_weekly_runs_from_its_own_tree() -> None:
    """Static on purpose: executing the script (or an older copy of it) would
    start a real regression eval against the live env."""
    text = (REPO / "scripts" / "run_regression_eval_weekly.sh").read_text(encoding="utf-8")
    assert "REPO=/data/grid_v4/grid_repo" not in text
    assert 'REPO="${REPO:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"' in text
    assert "dotenv_values('.env')" not in text
    assert text.index(NEW_ENV) < text.index(LEGACY_ENV)
    # The fail-closed env check comes before anything runs.
    assert text.index("GRID env file missing or unreadable") < text.index("-m scripts.run_regression_eval")


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(
        f"_retire_{name}", REPO / "scripts" / f"{name}.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_langfuse_scripts_default_to_etc_grid(tmp_path: Path, monkeypatch) -> None:
    pytest.importorskip("requests")
    env_file = tmp_path / "grid.env"
    env_file.write_text(
        "LANGFUSE_HOST=http://example.invalid\n"
        "LANGFUSE_PUBLIC_KEY=pk-test\n"
        "LANGFUSE_SECRET_KEY=sk-test\n",
        encoding="utf-8",
    )
    # bootstrap_test_evaluators setdefault()s every key into os.environ at
    # import; pre-set them through monkeypatch so nothing leaks past this test.
    monkeypatch.setenv("LANGFUSE_HOST", "http://example.invalid")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-test")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk-test")
    monkeypatch.setenv("GRID_ENV_FILE", str(env_file))
    for name in ("setup_langfuse_evaluators", "bootstrap_test_evaluators"):
        module = _load_script(name)
        assert module.ENV_PATH == env_file
        # Without GRID_ENV_FILE: /etc/grid/grid.env first, the old checkout
        # path only as a fallback. (Not imported unset: on grid-svr that would
        # read the real env file.)
        assert module._ENV_CANDIDATES == (Path(NEW_ENV), Path("/data/grid_v4/grid_repo/.env"))


# ── runtime data outside the checkout ────────────────────────────────


def test_finbert_cache_dir_moves_off_the_old_checkout(tmp_path: Path, monkeypatch) -> None:
    pytest.importorskip("torch")
    pytest.importorskip("transformers")
    from ingestion.ml import finbert_scorer as fs

    assert "grid_repo" not in fs.FINBERT_CACHE_DIR_DEFAULT
    default = tmp_path / "models" / "finbert"
    legacy = tmp_path / "grid_repo" / "grid" / "models" / "finbert"
    monkeypatch.delenv("GRID_FINBERT_CACHE_DIR", raising=False)

    # Neither exists: the new default (from_pretrained downloads there).
    assert fs.finbert_cache_dir(default=str(default), legacy=str(legacy)) == str(default)
    # Only the old checkout holds the cache: keep using it until it is copied.
    legacy.mkdir(parents=True)
    assert fs.finbert_cache_dir(default=str(default), legacy=str(legacy)) == str(legacy)
    # Once copied to the new home, the new home wins.
    default.mkdir(parents=True)
    assert fs.finbert_cache_dir(default=str(default), legacy=str(legacy)) == str(default)
    # The override wins over both.
    monkeypatch.setenv("GRID_FINBERT_CACHE_DIR", str(tmp_path / "x"))
    assert fs.finbert_cache_dir(default=str(default), legacy=str(legacy)) == str(tmp_path / "x")


def test_generated_images_dir_moves_off_the_old_checkout(tmp_path: Path, monkeypatch) -> None:
    image_gen = importlib.import_module("intelligence.image_gen")
    monkeypatch.delenv("GRID_GENERATED_IMAGES_DIR", raising=False)

    absent = tmp_path / "no-shared-outputs"
    assert image_gen.generated_images_dir(shared_root=absent) == (
        REPO / "outputs" / "generated_images"
    )
    shared = tmp_path / "outputs"
    shared.mkdir()
    assert image_gen.generated_images_dir(shared_root=shared) == shared / "generated_images"
    monkeypatch.setenv("GRID_GENERATED_IMAGES_DIR", str(tmp_path / "imgs"))
    assert image_gen.generated_images_dir(shared_root=shared) == tmp_path / "imgs"
    assert "grid_repo" not in str(image_gen._OUTPUT_DIR)
