"""Contract tests for the python-dotenv / pydantic-settings dependency.

``config.py`` reads ``.env`` through two independent paths:

* ``config.py:22`` -- ``load_dotenv(<dir of config.py>/.env)`` (python-dotenv,
  mutates ``os.environ`` with ``override=False``).
* ``config.py:927-931`` -- ``Settings.model_config`` with ``env_file=".env"``
  (cwd-relative) and ``env_file_encoding="utf-8"`` (pydantic-settings' own
  dotenv source).

These tests exercise the *real* libraries, never a stub. Every runtime check
runs in a fresh ``python -I`` child process with an explicit minimal
environment, so the parent's ``os.environ`` is never mutated and the repo's
real ``.env`` is never read. All fixture files are generated, non-secret, and
live under pytest's ``tmp_path``.

``config.py`` itself is never imported in the parent process (it has a
module-level ``load_dotenv`` at line 22, a module-level ``Settings()`` at line
937 and a GitSink attach at lines 959-966). The boundary tests instead run a
byte-identical *copy* of ``config.py`` placed under ``tmp_path`` inside an
isolated child, so line 22 resolves to a fixture ``.env`` and ``env_file=".env"``
resolves against the child's fixture cwd.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from packaging.requirements import Requirement
from packaging.version import Version

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_SOURCE = REPO_ROOT / "config.py"
REQUIREMENTS_PATH = REPO_ROOT / "requirements.txt"

DOTENV_FLOOR = Version("1.2.4")
BOM = b"\xef\xbb\xbf"


# ---------------------------------------------------------------------------
# Child-process helpers
# ---------------------------------------------------------------------------


def _child_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Minimal explicit environment for a child interpreter (no inheritance)."""
    env: dict[str, str] = {}
    for key in ("PATH", "SYSTEMROOT"):
        if key in os.environ:
            env[key] = os.environ[key]
    env["GRID_GIT_SINK_DISABLED"] = "1"  # config.py:959 gate
    if extra:
        env.update(extra)
    return env


def _run_child(
    code: str,
    args: list[str],
    cwd: Path,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-I", "-c", code, *args],
        env=_child_env(extra_env),
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def _stdout_ok(proc: subprocess.CompletedProcess[str]) -> str:
    assert proc.returncode == 0, (
        f"child failed rc={proc.returncode}\nstdout={proc.stdout}\nstderr={proc.stderr}"
    )
    return proc.stdout


def _has_token(stdout: str, token: str) -> bool:
    """True only if ``token`` is a complete output line (no substring match)."""
    return token in stdout.splitlines()


def _write_env(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Child programs (argv: 1=dotenv path, 2=mode flag where used)
# ---------------------------------------------------------------------------

_CHILD_FILE_ONLY = """
import os, sys
from dotenv import load_dotenv
rc = load_dotenv(dotenv_path=sys.argv[1], override=False)
v = os.environ.get("GRID_FIXTURE_FILE_ONLY")
if rc is True and v == "file-only-value":
    print("DOTENV_FILE_ONLY=ok")
else:
    print("DOTENV_FILE_ONLY=FAIL " + ascii((rc, v)))
"""

_CHILD_MISSING_FILE = """
import os, sys
from dotenv import load_dotenv
rc = load_dotenv(dotenv_path=sys.argv[1], override=False)
v = os.environ.get("GRID_FIXTURE_FILE_ONLY")
if rc is False and v is None:
    print("DOTENV_MISSING_FILE=absent")
else:
    print("DOTENV_MISSING_FILE=FAIL " + ascii((rc, v)))
"""

_CHILD_PRECEDENCE = """
import os, sys
from dotenv import load_dotenv
override = sys.argv[2] == "1"
load_dotenv(dotenv_path=sys.argv[1], override=override)
v = os.environ.get("GRID_FIXTURE_PRECEDENCE")
if v == "from-process":
    print("DOTENV_PRECEDENCE=process_wins")
elif v == "from-file":
    print("DOTENV_PRECEDENCE=file_wins")
else:
    print("DOTENV_PRECEDENCE=FAIL " + ascii(v))
"""

_CHILD_QUOTED_INTERP = """
import os, sys
from dotenv import load_dotenv
interpolate = sys.argv[2] == "1"
load_dotenv(dotenv_path=sys.argv[1], override=False, interpolate=interpolate)
g = os.environ.get
if not interpolate:
    if g("GRID_FIXTURE_INTERP") == "${GRID_FIXTURE_BASE}/sub":
        print("DOTENV_INTERP_OFF=literal")
    else:
        print("DOTENV_INTERP_OFF=FAIL " + ascii(g("GRID_FIXTURE_INTERP")))
else:
    if g("GRID_FIXTURE_QUOTED") == "two words # not a comment" and g("GRID_FIXTURE_SINGLE") == "single quoted":
        print("DOTENV_QUOTED=ok")
    else:
        print("DOTENV_QUOTED=FAIL " + ascii((g("GRID_FIXTURE_QUOTED"), g("GRID_FIXTURE_SINGLE"))))
    if g("GRID_FIXTURE_INTERP") == "root/sub" and g("GRID_FIXTURE_INTERP_DEFAULT") == "fallback":
        print("DOTENV_INTERP=ok")
    else:
        print("DOTENV_INTERP=FAIL " + ascii((g("GRID_FIXTURE_INTERP"), g("GRID_FIXTURE_INTERP_DEFAULT"))))
"""

_CHILD_BOM = """
import os, sys
from dotenv import load_dotenv
enc = sys.argv[2]
load_dotenv(dotenv_path=sys.argv[1], override=False, encoding=enc)
first = os.environ.get("GRID_FIXTURE_BOM_FIRST")
mangled = os.environ.get("\\ufeffGRID_FIXTURE_BOM_FIRST")
second = os.environ.get("GRID_FIXTURE_BOM_SECOND")
label = "DOTENV_BOM_" + enc.replace("-", "_").upper()
if second != "second":
    print(label + "=FAIL second_key " + ascii(second))
elif first == "first" and mangled is None:
    print(label + "=first_key_ok")
elif first is None and mangled == "first":
    print(label + "=first_key_bom_prefixed")
else:
    print(label + "=FAIL " + ascii((first, mangled)))
"""

_CHILD_DISABLED = """
import os, sys
from dotenv import load_dotenv
rc = load_dotenv(dotenv_path=sys.argv[1], override=False)
v = os.environ.get("GRID_FIXTURE_FILE_ONLY")
flag = os.environ.get("PYTHON_DOTENV_DISABLED")
if rc is False and v is None:
    print("DOTENV_DISABLED_" + flag + "=noop")
elif rc is True and v == "file-only-value":
    print("DOTENV_DISABLED_" + flag + "=loaded")
else:
    print("DOTENV_DISABLED_" + flag + "=FAIL " + ascii((rc, v)))
"""

_CHILD_VERSIONS = """
import importlib.metadata as md
print("DOTENV_VERSION=" + md.version("python-dotenv"))
print("PYDANTIC_SETTINGS_VERSION=" + md.version("pydantic-settings"))
"""

# Loads a *copy* of config.py (argv[1]) by path; exec runs config.py:22
# (load_dotenv on <copy dir>/.env) and config.py:937 (Settings(), env_file=".env"
# relative to the child's cwd).
_CHILD_SETTINGS = """
import importlib.util, os, sys
spec = importlib.util.spec_from_file_location("grid_config_fixture_copy", sys.argv[1])
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)
s = mod.settings
env = os.environ

cfg = mod.Settings.model_config
if cfg.get("env_file") == ".env" and cfg.get("env_file_encoding") == "utf-8":
    print("SETTINGS_MODEL_CONFIG=ok")
else:
    print("SETTINGS_MODEL_CONFIG=FAIL " + ascii(dict(cfg)))

# Path A: config.py:22 load_dotenv -> os.environ -> Settings field.
if env.get("BLS_API_KEY") == "module-dotenv-bls" and s.BLS_API_KEY == "module-dotenv-bls":
    print("SETTINGS_LOAD_DOTENV_PATH=ok")
elif env.get("BLS_API_KEY") is None and s.BLS_API_KEY == "":
    print("SETTINGS_LOAD_DOTENV_PATH=absent")
else:
    print("SETTINGS_LOAD_DOTENV_PATH=FAIL " + ascii((env.get("BLS_API_KEY"), s.BLS_API_KEY)))

# Path B: pydantic-settings env_file (cwd/.env); must NOT leak into os.environ.
if (
    s.TIINGO_API_KEY == "cwd-dotenv-tiingo"
    and s.DB_PASSWORD == "fixture-not-a-secret"
    and "TIINGO_API_KEY" not in env
    and "DB_PASSWORD" not in env
):
    print("SETTINGS_ENV_FILE_PATH=ok")
else:
    print("SETTINGS_ENV_FILE_PATH=FAIL " + ascii((s.TIINGO_API_KEY, "TIINGO_API_KEY" in env)))

# Precedence inside Settings: process env (populated by path A) beats env_file.
if env.get("ASTROGRID_DB_SCHEMA") == "from_module_dotenv" and s.ASTROGRID_DB_SCHEMA == "from_module_dotenv":
    print("SETTINGS_ENV_BEATS_ENV_FILE=ok")
elif env.get("ASTROGRID_DB_SCHEMA") is None and s.ASTROGRID_DB_SCHEMA == "from_cwd_dotenv":
    print("SETTINGS_ENV_BEATS_ENV_FILE=env_absent_file_used")
else:
    print("SETTINGS_ENV_BEATS_ENV_FILE=FAIL " + ascii((env.get("ASTROGRID_DB_SCHEMA"), s.ASTROGRID_DB_SCHEMA)))
"""

# Missing-cwd/.env boundary classifier. Separate from _CHILD_SETTINGS so the
# four tokens above keep their exact meaning. Without cwd/.env (and with no
# DB_PASSWORD in the process env or in <copy dir>/.env), config.py:51
# (``DB_PASSWORD: str = ""``) hits the unconditional validator at config.py:867
# ("DB_PASSWORD is empty"), which raises pydantic.ValidationError from
# ``Settings()`` at config.py:937 (pydantic-settings validates defaults).
# Only that exact error (single error, loc DB_PASSWORD, value_error, expected
# message) is classified; any other exception propagates (traceback, rc != 0)
# and, if Settings() unexpectedly loads, a distinct failing token is printed.
_CHILD_SETTINGS_MISSING_ENV_FILE = """
import importlib.util, os, sys
from pydantic import ValidationError

# Predicate proven inside the child: cwd/.env really is absent.
if os.path.exists(os.path.join(os.getcwd(), ".env")):
    print("SETTINGS_ENV_FILE_PRESENT=PRESENT")
    sys.exit(3)
print("SETTINGS_ENV_FILE_PRESENT=absent")

spec = importlib.util.spec_from_file_location("grid_config_fixture_copy", sys.argv[1])
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
try:
    spec.loader.exec_module(mod)
except ValidationError as exc:
    errs = exc.errors()
    if (
        len(errs) == 1
        and tuple(errs[0]["loc"]) == ("DB_PASSWORD",)
        and errs[0]["type"] == "value_error"
        and "DB_PASSWORD is empty" in errs[0]["msg"]
    ):
        print("SETTINGS_MISSING_ENV_FILE=db_password_validation_error")
    else:
        print("SETTINGS_MISSING_ENV_FILE=UNEXPECTED_VALIDATION_ERROR " + ascii(
            [(tuple(e["loc"]), e["type"]) for e in errs]
        ))
        sys.exit(4)
else:
    print("SETTINGS_MISSING_ENV_FILE=NO_ERROR_RAISED")
    sys.exit(5)
"""


# ---------------------------------------------------------------------------
# Dependency pins and installed versions
# ---------------------------------------------------------------------------


def _requirement(name: str) -> Requirement:
    for raw in REQUIREMENTS_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and line.lower().startswith(name.lower()):
            return Requirement(line)
    raise AssertionError(f"{name} missing from {REQUIREMENTS_PATH.name}")


def test_requirements_pin_python_dotenv_floor() -> None:
    """EXPECTED RED on main (>=1.0.0) and on the stale >=1.2.3 floor; GREEN once PR 752 (>=1.2.4) lands."""
    req = _requirement("python-dotenv")
    assert req.specifier.contains("1.2.4"), req
    assert not req.specifier.contains("1.2.3"), req
    assert not req.specifier.contains("1.2.2"), req
    assert not req.specifier.contains("1.0.0"), req


def test_installed_versions_meet_floor_in_isolated_child(tmp_path: Path) -> None:
    out = _stdout_ok(_run_child(_CHILD_VERSIONS, [], tmp_path))
    versions = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
    assert Version(versions["DOTENV_VERSION"]) >= DOTENV_FLOOR, versions
    assert Version(versions["PYDANTIC_SETTINGS_VERSION"]) >= Version("2.1.0"), versions


# ---------------------------------------------------------------------------
# python-dotenv: real parsing and precedence
# ---------------------------------------------------------------------------


def test_file_only_value_is_loaded(tmp_path: Path) -> None:
    env_file = _write_env(tmp_path / "fixture.env", "GRID_FIXTURE_FILE_ONLY=file-only-value\n")
    out = _stdout_ok(_run_child(_CHILD_FILE_ONLY, [str(env_file)], tmp_path))
    assert _has_token(out, "DOTENV_FILE_ONLY=ok"), out


def test_missing_fixture_file_cannot_fabricate_value(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.env"
    assert not missing.exists()
    out = _stdout_ok(_run_child(_CHILD_MISSING_FILE, [str(missing)], tmp_path))
    assert _has_token(out, "DOTENV_MISSING_FILE=absent"), out
    # The positive token must be impossible without the file.
    assert not _has_token(out, "DOTENV_FILE_ONLY=ok")


def test_existing_process_value_wins_with_override_false(tmp_path: Path) -> None:
    env_file = _write_env(tmp_path / "fixture.env", "GRID_FIXTURE_PRECEDENCE=from-file\n")
    proc = _run_child(
        _CHILD_PRECEDENCE,
        [str(env_file), "0"],
        tmp_path,
        extra_env={"GRID_FIXTURE_PRECEDENCE": "from-process"},
    )
    out = _stdout_ok(proc)
    assert _has_token(out, "DOTENV_PRECEDENCE=process_wins"), out


def test_precedence_reversal_with_override_true_is_detected(tmp_path: Path) -> None:
    """Control: override=True must flip the outcome, so the test above rejects it."""
    env_file = _write_env(tmp_path / "fixture.env", "GRID_FIXTURE_PRECEDENCE=from-file\n")
    proc = _run_child(
        _CHILD_PRECEDENCE,
        [str(env_file), "1"],
        tmp_path,
        extra_env={"GRID_FIXTURE_PRECEDENCE": "from-process"},
    )
    out = _stdout_ok(proc)
    assert _has_token(out, "DOTENV_PRECEDENCE=file_wins"), out
    # Exactly the predicate used by the override=False test must reject this.
    assert not _has_token(out, "DOTENV_PRECEDENCE=process_wins")


_QUOTED_INTERP_FIXTURE = (
    "GRID_FIXTURE_BASE=root\n"
    'GRID_FIXTURE_QUOTED="two words # not a comment"\n'
    "GRID_FIXTURE_SINGLE='single quoted'\n"
    "GRID_FIXTURE_INTERP=${GRID_FIXTURE_BASE}/sub\n"
    "GRID_FIXTURE_INTERP_DEFAULT=${GRID_FIXTURE_MISSING_VAR:-fallback}\n"
)


def test_quoted_and_interpolated_values_parse(tmp_path: Path) -> None:
    env_file = _write_env(tmp_path / "fixture.env", _QUOTED_INTERP_FIXTURE)
    out = _stdout_ok(_run_child(_CHILD_QUOTED_INTERP, [str(env_file), "1"], tmp_path))
    assert _has_token(out, "DOTENV_QUOTED=ok"), out
    assert _has_token(out, "DOTENV_INTERP=ok"), out


def test_interpolation_control_literal_when_disabled(tmp_path: Path) -> None:
    """Control: interpolate=False leaves ${VAR} literal, so DOTENV_INTERP=ok is real."""
    env_file = _write_env(tmp_path / "fixture.env", _QUOTED_INTERP_FIXTURE)
    out = _stdout_ok(_run_child(_CHILD_QUOTED_INTERP, [str(env_file), "0"], tmp_path))
    assert _has_token(out, "DOTENV_INTERP_OFF=literal"), out
    assert not _has_token(out, "DOTENV_INTERP=ok")


# ---------------------------------------------------------------------------
# UTF-8 BOM
# ---------------------------------------------------------------------------


def _bom_file(tmp_path: Path) -> Path:
    path = tmp_path / "bom.env"
    path.write_bytes(
        BOM + b"GRID_FIXTURE_BOM_FIRST=first\nGRID_FIXTURE_BOM_SECOND=second\n"
    )
    assert path.read_bytes().startswith(BOM)
    return path


def test_bom_file_with_utf8_sig_yields_first_key(tmp_path: Path) -> None:
    """Certain: the utf-8-sig codec strips the BOM, so the first key is intact."""
    out = _stdout_ok(_run_child(_CHILD_BOM, [str(_bom_file(tmp_path)), "utf-8-sig"], tmp_path))
    assert _has_token(out, "DOTENV_BOM_UTF_8_SIG=first_key_ok"), out


def test_bom_file_with_default_utf8_first_key_behaviour(tmp_path: Path) -> None:
    """Observed on python-dotenv 1.2.4: the BOM is stripped even with utf-8.

    ``load_dotenv`` defaults to ``encoding="utf-8"`` (not ``utf-8-sig``), yet the
    first key is stored intact and the second key is unaffected. The original
    expectation (a ``"\\ufeff"``-prefixed first key) was flipped to the measured
    behaviour; a BOM-prefixed first key or a broken second key now fails.
    """
    out = _stdout_ok(_run_child(_CHILD_BOM, [str(_bom_file(tmp_path)), "utf-8"], tmp_path))
    assert _has_token(out, "DOTENV_BOM_UTF_8=first_key_ok"), out
    assert not _has_token(out, "DOTENV_BOM_UTF_8=first_key_bom_prefixed"), out


# ---------------------------------------------------------------------------
# PYTHON_DOTENV_DISABLED (python-dotenv >=1.2.0 per the official CHANGELOG)
# ---------------------------------------------------------------------------


def test_python_dotenv_disabled_1_is_noop(tmp_path: Path) -> None:
    env_file = _write_env(tmp_path / "fixture.env", "GRID_FIXTURE_FILE_ONLY=file-only-value\n")
    out = _stdout_ok(
        _run_child(_CHILD_DISABLED, [str(env_file)], tmp_path, {"PYTHON_DOTENV_DISABLED": "1"})
    )
    assert _has_token(out, "DOTENV_DISABLED_1=noop"), out


def test_python_dotenv_disabled_0_does_not_disable(tmp_path: Path) -> None:
    env_file = _write_env(tmp_path / "fixture.env", "GRID_FIXTURE_FILE_ONLY=file-only-value\n")
    out = _stdout_ok(
        _run_child(_CHILD_DISABLED, [str(env_file)], tmp_path, {"PYTHON_DOTENV_DISABLED": "0"})
    )
    assert _has_token(out, "DOTENV_DISABLED_0=loaded"), out


# ---------------------------------------------------------------------------
# Boundary: the real config.Settings and config.py:22 load_dotenv
# ---------------------------------------------------------------------------


def _stage_config_copy(tmp_path: Path) -> tuple[Path, Path]:
    """Return (config_copy_path, child_cwd) with fixture .env files in place.

    The copy is byte-identical to the checked-in config.py, relocated so that
    ``os.path.dirname(__file__)`` (config.py:22) is ``tmp_path/mod`` and the
    cwd-relative ``env_file=".env"`` (config.py:928) is ``tmp_path/cwd``.
    """
    mod_dir = tmp_path / "mod"
    cwd_dir = tmp_path / "cwd"
    mod_dir.mkdir()
    cwd_dir.mkdir()
    copy = mod_dir / "config.py"
    copy.write_bytes(CONFIG_SOURCE.read_bytes())
    _write_env(
        mod_dir / ".env",
        "BLS_API_KEY=module-dotenv-bls\nASTROGRID_DB_SCHEMA=from_module_dotenv\n",
    )
    _write_env(
        cwd_dir / ".env",
        "DB_PASSWORD=fixture-not-a-secret\n"
        "TIINGO_API_KEY=cwd-dotenv-tiingo\n"
        "ASTROGRID_DB_SCHEMA=from_cwd_dotenv\n",
    )
    return copy, cwd_dir


def test_real_settings_class_reads_both_dotenv_paths(tmp_path: Path) -> None:
    copy, cwd_dir = _stage_config_copy(tmp_path)
    assert copy.read_bytes() == CONFIG_SOURCE.read_bytes()
    out = _stdout_ok(_run_child(_CHILD_SETTINGS, [str(copy)], cwd_dir))
    assert _has_token(out, "SETTINGS_MODEL_CONFIG=ok"), out
    assert _has_token(out, "SETTINGS_LOAD_DOTENV_PATH=ok"), out
    assert _has_token(out, "SETTINGS_ENV_FILE_PATH=ok"), out
    assert _has_token(out, "SETTINGS_ENV_BEATS_ENV_FILE=ok"), out


def test_real_settings_disabled_dotenv_isolates_module_path(tmp_path: Path) -> None:
    """Control: PYTHON_DOTENV_DISABLED=1 silences config.py:22 only.

    The pydantic-settings env_file path must still work (it reads the file
    itself), and the process-env-beats-file check must fall through to the
    cwd file value.
    """
    copy, cwd_dir = _stage_config_copy(tmp_path)
    out = _stdout_ok(
        _run_child(_CHILD_SETTINGS, [str(copy)], cwd_dir, {"PYTHON_DOTENV_DISABLED": "1"})
    )
    assert _has_token(out, "SETTINGS_LOAD_DOTENV_PATH=absent"), out
    assert _has_token(out, "SETTINGS_ENV_FILE_PATH=ok"), out
    assert _has_token(out, "SETTINGS_ENV_BEATS_ENV_FILE=env_absent_file_used"), out
    assert not _has_token(out, "SETTINGS_LOAD_DOTENV_PATH=ok")


def test_real_settings_missing_env_file_cannot_fabricate_values(tmp_path: Path) -> None:
    """Control: without cwd/.env the Path-B token must never appear.

    Expected boundary (explicit, not "anything goes"): the child proves
    cwd/.env is absent (``SETTINGS_ENV_FILE_PRESENT=absent``), then
    ``Settings()`` raises exactly one pydantic ValidationError for
    ``DB_PASSWORD`` from the unconditional validator (config.py:867). The
    child catches only that error, prints a dedicated token and exits 0.
    Any other crash/traceback, non-zero exit, or a successful load fails.
    """
    copy, cwd_dir = _stage_config_copy(tmp_path)
    (cwd_dir / ".env").unlink()
    assert not (cwd_dir / ".env").exists()
    proc = _run_child(_CHILD_SETTINGS_MISSING_ENV_FILE, [str(copy)], cwd_dir)
    out = _stdout_ok(proc)  # rc == 0 required; message carries stdout/stderr
    assert "Traceback" not in proc.stderr, proc.stderr
    assert _has_token(out, "SETTINGS_ENV_FILE_PRESENT=absent"), out
    assert _has_token(out, "SETTINGS_MISSING_ENV_FILE=db_password_validation_error"), out
    assert not any(
        line.startswith("SETTINGS_MISSING_ENV_FILE=") and "db_password_validation_error" not in line
        for line in out.splitlines()
    ), out
    assert not _has_token(out, "SETTINGS_ENV_FILE_PATH=ok"), out
