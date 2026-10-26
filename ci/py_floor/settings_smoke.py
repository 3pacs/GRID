"""Real-Settings smoke for the Python-floor qualification job.

Run from the repo root of a checkout, inside the venv under test:
    python ci/py_floor/settings_smoke.py

Imports the real repo-root config.py (real python-dotenv + pydantic-settings),
with a fixture DB_PASSWORD (config.py rejects an empty one unconditionally) and
the git sink disabled. Reads no real .env: it runs with cwd set to a fresh temp
dir and asserts no repo-root .env exists. Prints the resolved library versions.
"""
from __future__ import annotations

import os
import sys
import tempfile
from importlib import metadata
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
assert not (REPO_ROOT / ".env").exists(), "repo-root .env present; refusing to read it"

os.environ["DB_PASSWORD"] = "ci-smoke-fixture-not-a-secret"
os.environ["GRID_GIT_SINK_DISABLED"] = "1"
for k in ("ENVIRONMENT", "DB_HOST", "DB_PORT"):
    os.environ.pop(k, None)

sys.path.insert(0, str(REPO_ROOT))
with tempfile.TemporaryDirectory() as tmp:
    os.chdir(tmp)  # cwd-relative env_file=".env" must not find anything
    import config  # noqa: E402  (module-level Settings() runs here)

    s = config.settings
    assert type(s).__name__ == "Settings"
    assert s.DB_PASSWORD == "ci-smoke-fixture-not-a-secret"
    assert s.ENVIRONMENT == "development"
    assert s.DB_HOST == "localhost" and s.DB_PORT == 5432  # defaults, no leakage

    # pydantic-settings' own dotenv source reads a fixture file explicitly.
    envfile = Path(tmp) / "fixture.env"
    envfile.write_text("DB_HOST=fixture-host\nDB_PORT=6543\n", encoding="utf-8")
    s2 = config.Settings(_env_file=str(envfile))
    assert s2.DB_HOST == "fixture-host" and s2.DB_PORT == 6543, (s2.DB_HOST, s2.DB_PORT)

    # Negative: an empty password must still be rejected by the real validator.
    os.environ["DB_PASSWORD"] = ""
    try:
        config.Settings(_env_file=None)
    except Exception as exc:  # pydantic ValidationError
        assert "DB_PASSWORD is empty" in str(exc), str(exc)
    else:
        raise AssertionError("empty DB_PASSWORD was accepted")

for dist in ("python-dotenv", "pydantic", "pydantic-settings", "numpy", "scipy",
             "tsfresh", "boto3", "botocore"):
    print(f"SMOKE_VERSION {dist}=={metadata.version(dist)}")
print("SETTINGS_SMOKE=ok")
