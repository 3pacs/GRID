"""OPS-W1: crontab cutover transform + release-path cron entry points.

The transform tests feed a sanitized copy of grid-svr's ``grid`` crontab
(2026-10-01; recipient value replaced) through
``scripts/crontab_release_cutover.py`` and check that exactly the GRID job
lines change, nothing else does, drift is refused, and a second run is a
no-op. The shell tests run the release-path entry points against a temporary
tree to check that the env file is loaded fail-closed, PYTHONPATH is pinned
to the tree, symlinked roots resolve to one physical tree, and ``--check``
never runs the target.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"

_spec = importlib.util.spec_from_file_location(
    "crontab_release_cutover", SCRIPTS / "crontab_release_cutover.py"
)
assert _spec and _spec.loader
cutover = importlib.util.module_from_spec(_spec)
sys.modules["crontab_release_cutover"] = cutover
_spec.loader.exec_module(cutover)

FAKE_DAD = "+10000000000"

# Sanitized excerpt of the live crontab: every GRID line the cutover touches,
# plus neighbours it must leave alone (legacy grid_v4 jobs, paused/disabled
# comments that mention grid_repo, the paper-log env readers, unrelated jobs).
LIVE = textwrap.dedent(
    f"""\
    */15 * * * * /home/grid/bin/pipeline_status_wrap.sh run_pipeline /tmp/grid_duckdb.lock /bin/bash -c 'cd /home/grid/grid_v4 && /home/grid/grid_v4/venv/bin/python scripts/run_pipeline.py' >> /data/grid_v4/logs/cron.log 2>&1
    # DISABLED 2026-05-20 hermes-noise-fix: stale missing path caused daily cron failure
    # 0 22 * * 1-5 cd /home/grid/grid_v4/grid_repo/grid && /usr/bin/python3 scripts/run_full_pipeline.py >> /tmp/grid_daily.log 2>&1
    0 6 * * * cd ~/grid_v4/grid_repo && set -a && source .env && set +a && python3 -m grid.ingestors.trial_ingestor >> /var/log/grid/trial_ingestor.log 2>&1
    0 0,6,12,18 * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/python3 scripts/warm_dashboard_cache.py >> /data/grid/logs/cache-warm.log 2>&1
    # --- GRID Hourly Analysis Catch-up ---
    0 * * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/flock -n /tmp/grid-hourly-catchup.lock /bin/bash /home/grid/grid_v4/grid_repo/scripts/grid_hourly_catchup.sh >> /data/grid/logs/hourly-catchup.log 2>&1 # GRID-CRON-hourly-catchup
    5 * * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/flock -n /tmp/astrogrid-hourly-catchup.lock /bin/bash /home/grid/grid_v4/grid_repo/scripts/astrogrid_hourly_catchup.sh >> /data/grid/logs/astrogrid-hourly-catchup.log 2>&1 # GRID-CRON-astrogrid-hourly-catchup
    # PAUSED 20260926T054208Z by claude@ANIK (owner-approved) # 0 2 * * 1-5 /home/grid/grid_v4/grid_repo/scripts/grid_cron.sh autoresearch # GRID-CRON-autoresearch
    0 6 * * 1-5 /home/grid/grid_v4/grid_repo/scripts/grid_cron.sh briefing daily # GRID-CRON-briefing-daily
    30 6 * * 1-5 /home/grid/grid_v4/grid_repo/scripts/grid_cron.sh analyst # GRID-CRON-analyst
    0 7 * * 1 /home/grid/grid_v4/grid_repo/scripts/grid_cron.sh briefing weekly # GRID-CRON-briefing-weekly
    0 5 * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/python3 -m scripts.auto_improve_from_postmortems >> /home/grid/logs/auto-improve.log 2>&1 # GRID-CRON-auto-improve
    CRON_TZ=America/Los_Angeles
    */30 * * * * /usr/bin/flock -n /tmp/grid-git-catchup.lock /bin/bash /home/grid/bin/grid-git-catchup.sh # GRID-CRON-git-catchup
    */15 * * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/flock -n /tmp/sd-price-alerts.lock /usr/bin/env SD_IMESSAGE_DAD={FAKE_DAD} PYTHONPATH=/home/grid/grid_v4/grid_repo /usr/bin/python3 -m scripts.check_price_alerts >> /data/grid/logs/price-alerts.log 2>&1 # GRID-CRON-price-alerts
    */30 * * * * /home/grid/ocmri/bin/ocmri-cc-refresh.sh >/dev/null 2>&1
    45 12,13 * * 1-5 [ "$(TZ=America/New_York date +\\%H)" = "08" ] && /usr/bin/flock -n /tmp/paper-log-gex-levels-preopen.lock /bin/bash -c 'cd /data/grid/paper_log/code/x && set -a && source /home/grid/grid_v4/grid_repo/.env && set +a && /data/grid_v4/venv/bin/python -m paper_log.gex_levels preopen' >> /data/grid/paper_log/gex_levels_v1/job.log 2>&1
    """
)


def _run(text: str = LIVE, stamp: str = "20261001") -> tuple[str, list[str]]:
    return cutover.transform(text, stamp)


def _active(text: str) -> list[str]:
    return [
        ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")
    ]


def test_rewrites_every_grid_job_to_release_tree() -> None:
    new, summary = _run()
    assert sorted(s.split()[1] for s in summary if s.startswith("rewrite")) == sorted(
        r.key for r in cutover.RULES if r.action == "rewrite"
    )
    for line in _active(new):
        if "GRID-CRON-" in line:
            assert "/data/grid_v4/grid_release/scripts/" in line, line
    # Only the shared env-file path may still name grid_repo on an active line.
    for line in _active(new):
        assert "grid_repo" not in line.replace(
            "/home/grid/grid_v4/grid_repo/.env", ""
        ), line


def test_schedules_locks_logs_and_markers_are_unchanged() -> None:
    new, _ = _run()
    old_lines, new_lines = LIVE.splitlines(), new.splitlines()
    assert len(old_lines) == len(new_lines)
    for old, cur in zip(old_lines, new_lines):
        if old == cur or cur.startswith("# OPS-W1-DISABLED"):
            continue
        assert old.split()[:5] == cur.split()[:5]  # schedule fields
        for token in old.split():
            if token.startswith(
                ("/tmp/", "/data/grid/logs/", "/home/grid/logs/", "GRID-CRON-")
            ):
                assert token in cur.split(), (token, cur)


def test_recipient_value_carried_over_and_old_pythonpath_dropped() -> None:
    new, _ = _run()
    (line,) = [ln for ln in new.splitlines() if "GRID-CRON-price-alerts" in ln]
    assert f"SD_IMESSAGE_DAD={FAKE_DAD}" in line
    assert "PYTHONPATH" not in line  # grid_cron_env.sh pins it to the release tree
    assert (
        "/data/grid_v4/grid_release/scripts/grid_cron_run.sh -m scripts.check_price_alerts"
        in line
    )


def test_dead_or_redundant_jobs_are_commented_not_deleted() -> None:
    new, _ = _run()
    disabled = [
        ln for ln in new.splitlines() if ln.startswith("# OPS-W1-DISABLED 20261001 ")
    ]
    assert len(disabled) == 3
    keys = {r.key for r in cutover.RULES if r.action == "disable"}
    assert {d.split()[3] for d in disabled} == keys
    for original in (
        "python3 -m grid.ingestors.trial_ingestor",
        "scripts/warm_dashboard_cache.py",
        "/home/grid/bin/grid-git-catchup.sh",
    ):
        assert any(original in d for d in disabled)
        assert not any(original in a for a in _active(new))


def test_unrelated_lines_untouched() -> None:
    new, _ = _run()
    old_lines = LIVE.splitlines()
    new_lines = new.splitlines()
    touched = {i for i, (a, b) in enumerate(zip(old_lines, new_lines)) if a != b}
    assert len(touched) == len(cutover.RULES)
    for i in set(range(len(old_lines))) - touched:
        assert old_lines[i] == new_lines[i]
    assert new.endswith("\n")


def test_second_run_is_a_noop() -> None:
    new, _ = _run()
    again, summary = _run(new)
    assert again == new
    assert all(s.startswith(("already", "note")) for s in summary)


def test_matches_reference_template() -> None:
    new, _ = _run()
    template = (REPO / "deploy" / "cron" / "grid-release.crontab.template").read_text(
        encoding="utf-8"
    )
    body = [
        ln
        for ln in template.splitlines()
        if ln and (not ln.startswith("#") or ln.startswith("# OPS-W1-DISABLED"))
    ]
    assert cutover.target_block().splitlines() == body
    for rule in cutover.RULES:
        if rule.action == "rewrite":
            assert rule.target.format(dad=FAKE_DAD) in new.splitlines()


@pytest.mark.parametrize(
    "mutate",
    [
        lambda t: t.replace(
            "/tmp/grid-hourly-catchup.lock", "/tmp/other.lock"
        ),  # drifted line
        lambda t: t + t.splitlines()[6] + "\n",  # duplicated line
        lambda t: (
            t + "15 * * * * cd /home/grid/grid_v4/grid_repo && /usr/bin/python3 x.py\n"
        ),  # unknown grid_repo job
    ],
)
def test_refuses_on_drift(mutate) -> None:
    with pytest.raises(cutover.CutoverError):
        _run(mutate(LIVE))


def test_cli_refuses_to_overwrite_and_writes_lf(tmp_path: Path) -> None:
    src = tmp_path / "before.txt"
    src.write_bytes(LIVE.encode())
    dst = tmp_path / "after.txt"
    assert (
        cutover.main(["--in", str(src), "--out", str(dst), "--stamp", "20261001"]) == 0
    )
    assert b"\r" not in dst.read_bytes()
    with pytest.raises(SystemExit):
        cutover.main(["--in", str(src), "--out", str(dst)])


def test_cli_refusal_writes_nothing(tmp_path: Path) -> None:
    src = tmp_path / "before.txt"
    src.write_text("0 * * * * echo unrelated\n", encoding="utf-8")
    dst = tmp_path / "after.txt"
    assert cutover.main(["--in", str(src), "--out", str(dst)]) == 2
    assert not dst.exists()


# ── shell entry points ────────────────────────────────────────────────

needs_posix_bash = pytest.mark.skipif(
    os.name == "nt" or shutil.which("bash") is None, reason="needs a POSIX bash"
)


def _tree(tmp_path: Path) -> Path:
    root = tmp_path / "releases" / "abc123"
    (root / "scripts").mkdir(parents=True)
    for name in (
        "grid_cron_env.sh",
        "grid_cron_run.sh",
        "grid_cron.sh",
        "grid_hourly_catchup.sh",
    ):
        shutil.copy2(SCRIPTS / name, root / "scripts" / name)
        (root / "scripts" / name).chmod(0o755)
    (tmp_path / "grid_release").symlink_to(root)
    return root


def _env(tmp_path: Path, **extra: str) -> dict[str, str]:
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(tmp_path / "home"),
        "GRID_PYTHON": sys.executable,
        "PYTHONPATH": "/somewhere/else/grid_repo",
    }
    env.update(extra)
    return env


@needs_posix_bash
def test_run_wrapper_fails_closed_without_env_file(tmp_path: Path) -> None:
    _tree(tmp_path)
    proc = subprocess.run(
        [
            str(tmp_path / "grid_release" / "scripts" / "grid_cron_run.sh"),
            "-c",
            "print('ran')",
        ],
        env=_env(tmp_path, GRID_ENV_FILE=str(tmp_path / "missing.env")),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 78
    assert "ran" not in proc.stdout
    assert "GRID env file missing or unreadable" in proc.stderr


@needs_posix_bash
def test_run_wrapper_loads_env_pins_pythonpath_and_resolves_symlink(
    tmp_path: Path,
) -> None:
    root = _tree(tmp_path)
    env_file = tmp_path / "grid.env"
    env_file.write_text(
        "OPS_W1_PROBE=loaded\nexport OPS_W1_OTHER='two words'\n", encoding="utf-8"
    )
    code = "import os,sys; print(os.environ['OPS_W1_PROBE'], os.environ['OPS_W1_OTHER'], os.environ['PYTHONPATH'], os.getcwd(), sep='|')"
    proc = subprocess.run(
        [str(tmp_path / "grid_release" / "scripts" / "grid_cron_run.sh"), "-c", code],
        env=_env(tmp_path, GRID_ENV_FILE=str(env_file)),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    probe, other, pythonpath, cwd = proc.stdout.strip().split("|")
    assert (probe, other) == ("loaded", "two words")
    assert pythonpath == str(root.resolve())  # inherited /somewhere/else dropped
    assert cwd == str(root.resolve())


@needs_posix_bash
def test_run_wrapper_check_imports_without_running_target(tmp_path: Path) -> None:
    root = _tree(tmp_path)
    env_file = tmp_path / "grid.env"
    env_file.write_text("X=1\n", encoding="utf-8")
    marker = tmp_path / "target-ran"
    (root / "scripts" / "good_job.py").write_text(
        f"import json\nfrom pathlib import Path\nPath({str(marker)!r}).write_text('ran')\n",
        encoding="utf-8",
    )
    (root / "scripts" / "bad_job.py").write_text(
        "import module_that_does_not_exist_ops_w1\n", encoding="utf-8"
    )
    wrapper = str(tmp_path / "grid_release" / "scripts" / "grid_cron_run.sh")
    env = _env(tmp_path, GRID_ENV_FILE=str(env_file))

    ok = subprocess.run(
        [wrapper, "--check", "scripts/good_job.py"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert ok.returncode == 0, ok.stdout + ok.stderr
    assert "OK   scripts/good_job.py" in ok.stdout
    assert not marker.exists()  # target body never executed

    bad = subprocess.run(
        [wrapper, "--check", "scripts/good_job.py", "scripts/bad_job.py", "json"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert bad.returncode == 1
    assert "FAIL scripts/bad_job.py" in bad.stdout
    assert "FAIL json" in bad.stdout  # resolves outside the tree


@needs_posix_bash
def test_grid_cron_refuses_job_without_env_and_help_needs_none(tmp_path: Path) -> None:
    _tree(tmp_path)
    script = str(tmp_path / "grid_release" / "scripts" / "grid_cron.sh")
    env = _env(tmp_path, GRID_ENV_FILE=str(tmp_path / "missing.env"))
    proc = subprocess.run(
        [script, "analyst"], env=env, capture_output=True, text=True, check=False
    )
    assert proc.returncode == 78
    assert "not started" in proc.stderr
    helped = subprocess.run(
        [script, "help"], env=env, capture_output=True, text=True, check=False
    )
    assert helped.returncode == 0
    assert "check" in helped.stdout


@needs_posix_bash
def test_hourly_catchup_stops_before_any_step_without_env(tmp_path: Path) -> None:
    _tree(tmp_path)
    proc = subprocess.run(
        [
            "/bin/bash",
            str(tmp_path / "grid_release" / "scripts" / "grid_hourly_catchup.sh"),
        ],
        env=_env(tmp_path, GRID_ENV_FILE=str(tmp_path / "missing.env")),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 78
    assert "START" not in proc.stdout
