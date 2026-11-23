import importlib.util
import os
from pathlib import Path
import shutil
import stat
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("factory_packaging", ROOT / "scripts/prepare_factory_calendars.py")
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


def staged(tmp_path):
    root = tmp_path / "candidate"
    (root / "analysis").mkdir(parents=True)
    shutil.copyfile(ROOT / "analysis/factory_session_calendar.py", root / "analysis/factory_session_calendar.py")
    dest = root / "docs/reference/calendar-manifests"
    dest.mkdir(parents=True)
    sources = sorted(
        p for pat in ("factory-xnys-*.json", "prescreen-xnys-*.json")
        for p in (ROOT / "docs/reference/calendar-manifests").glob(pat)
    )
    for source in sources:  # factory + prescreen
        shutil.copyfile(source, dest / source.name)
        (dest / source.name).chmod(0o664)
    return root, sorted(dest.glob("*.json"))


def mode(path):
    return stat.S_IMODE(path.stat().st_mode)


def test_copied_writable_candidate_bytes_preserved_and_idempotent(tmp_path):
    root, files = staged(tmp_path)
    before = {p.name: p.read_bytes() for p in files}
    untouched = root / "other.json"
    untouched.write_text("untouched")
    untouched.chmod(0o664)
    assert M.prepare(root)["status"] == "prepared"
    assert all(mode(p) == 0o644 for p in files)
    assert before == {p.name: p.read_bytes() for p in files}
    assert mode(untouched) == 0o664
    assert all(r["before"] == r["after"] for r in M.prepare(root)["files"])


def test_real_git_checkout_under_umask002(tmp_path):
    source, _ = staged(tmp_path)
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path),
           "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"}
    def git(*args):
        return subprocess.run(["git", *args], env=env, check=True, capture_output=True)
    git("init", str(source))
    git("-C", str(source), "add", ".")
    git("-C", str(source), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
        "-c", "commit.gpgsign=false", "commit", "-m", "synthetic local fixture")
    checkout = tmp_path / "checkout"
    old = os.umask(0o002)
    try:
        git("clone", "--local", str(source), str(checkout))
    finally:
        os.umask(old)
    files = sorted((checkout / "docs/reference/calendar-manifests").glob("*.json"))
    assert files and all(mode(p) == 0o664 for p in files)
    from analysis.factory_session_calendar import FactoryCalendar

    with pytest.raises(ValueError, match="non-group/world-writable"):
        FactoryCalendar(checkout / "docs/reference/calendar-manifests")
    M.prepare(checkout)
    assert all(mode(p) == 0o644 for p in files)
    assert len(FactoryCalendar(checkout / "docs/reference/calendar-manifests").sessions) == 1010


@pytest.mark.parametrize("kind", ["hash", "missing", "symlink", "hardlink", "directory"])
def test_invalid_second_file_does_not_mutate_first(tmp_path, kind):
    root, files = staged(tmp_path)
    victim = files[-1]
    if kind == "hash":
        victim.write_text("invalid")
    else:
        content = victim.read_bytes()
        victim.unlink()
        if kind == "symlink":
            outside = tmp_path / "outside"
            outside.write_bytes(content)
            victim.symlink_to(outside)
        elif kind == "hardlink":
            outside = tmp_path / "outside"
            outside.write_bytes(content)
            os.link(outside, victim)
        elif kind == "directory":
            victim.mkdir()
    with pytest.raises((ValueError, OSError)):
        M.prepare(root)
    assert mode(files[0]) == 0o664


def test_linked_parent_refused(tmp_path):
    root, files = staged(tmp_path)
    parent = files[0].parent
    outside = tmp_path / "outside"
    parent.rename(outside)
    parent.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError):
        M.prepare(root)
    assert all(mode(p) == 0o664 for p in outside.glob("*.json"))


def test_live_alias_refused(tmp_path):
    root, _ = staged(tmp_path)
    alias = tmp_path / "live"
    alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(ValueError):
        M.prepare(alias)


def test_absent_factory_older_candidate_is_noop(tmp_path):
    assert M.prepare(tmp_path) == {"status": "not_applicable", "files": []}


def test_source_never_executed(tmp_path):
    root, _ = staged(tmp_path)
    source = root / "analysis/factory_session_calendar.py"
    source.write_text("raise RuntimeError('must not execute')\n" + source.read_text())
    assert M.prepare(root)["status"] == "prepared"


def test_hook_prepares_after_pin_before_dependency_side_effects():
    text = (ROOT / "scripts/deploy_build_hook.sh").read_text()
    assert text.index('bash "${SCRIPT_DIR}/deploy_pin_candidate_sha.sh"') < text.index(
        'python3 "${SCRIPT_DIR}/prepare_factory_calendars.py"') < text.index('pip install')


def _assigned(path, name):
    import ast

    tree = ast.parse(path.read_text())
    return [ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == name for t in n.targets)]


def test_prescreen_pins_equal_the_prescreen_module():
    """factory_session_calendar.PRESCREEN_PINS is the list the deploy prepares and market-state reads;
    factory_prescreen.PINS is the prescreen's own copy. They must not drift."""
    assert _assigned(ROOT / "analysis/factory_session_calendar.py", "PRESCREEN_PINS") == \
        _assigned(ROOT / "analysis/factory_prescreen.py", "PINS")


def test_prescreen_parts_are_prepared_with_the_factory_parts(tmp_path):
    root, files = staged(tmp_path)
    assert {p.name.split("-")[0] for p in files} == {"factory", "prescreen"}
    rows = M.prepare(root)["files"]
    assert sorted(r["file"] for r in rows) == [p.name for p in files]
    assert all(mode(p) == 0o644 for p in files)


def test_candidate_without_prescreen_pins_prepares_pins_only(tmp_path):
    root, files = staged(tmp_path)
    source = root / "analysis/factory_session_calendar.py"
    text = source.read_text()
    start = text.index("PRESCREEN_PINS = (")
    source.write_text(text[:start] + text[text.index("\n)\n", start) + 3:])
    assert not _assigned(source, "PRESCREEN_PINS")
    M.prepare(root)
    assert {p.name: mode(p) for p in files} == {
        p.name: 0o644 if p.name.startswith("factory-") else 0o664 for p in files}


def test_market_state_reads_a_umask002_candidate_only_after_prepare(tmp_path, monkeypatch):
    """Codex's deploy witness (#1324): a release built under umask 002 has 664 manifests, and the strict
    reader refuses them; the deploy's prepare() must cover every part market-state opens."""
    from datetime import date

    from intelligence.regime import market_state as MS

    source, _ = staged(tmp_path)
    env = {"PATH": os.environ["PATH"], "HOME": str(tmp_path),
           "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null"}
    def git(*args):
        return subprocess.run(["git", *args], env=env, check=True, capture_output=True)
    git("init", str(source))
    git("-C", str(source), "add", ".")
    git("-C", str(source), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
        "-c", "commit.gpgsign=false", "commit", "-m", "synthetic local fixture")
    checkout = tmp_path / "checkout"
    old = os.umask(0o002)
    try:
        git("clone", "--local", str(source), str(checkout))
    finally:
        os.umask(old)
    manifests = checkout / "docs/reference/calendar-manifests"
    assert all(mode(p) == 0o664 for p in manifests.glob("*.json"))
    monkeypatch.setattr(MS, "_MANIFEST_ROOT", manifests)
    monkeypatch.setattr(MS, "_xnys_cache", None)
    with pytest.raises(ValueError, match="non-group/world-writable"):
        MS.compute_market_state(None, date(2026, 11, 20))      # refused before any read
    M.prepare(checkout)
    assert all(mode(p) == 0o644 for p in manifests.glob("*.json"))
    assert MS.sessions_after(date(2026, 11, 19), date(2026, 11, 20)) == (1, False)
    assert MS.sessions_after(date(2020, 6, 17), date(2020, 6, 22)) == (3, False)   # a prescreen-part date
