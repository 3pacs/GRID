"""Guard: no API key / token literal is committed anywhere in the repo.

Backs the secrets rule in ``.claude/rules/security.md``. The scanner lives in
``scripts/audit_literal_secrets.py`` so an operator can run it by hand; this
file pins its behaviour and runs it over the whole tree on every test run.
"""

from __future__ import annotations

import pytest

from scripts.audit_literal_secrets import (
    find_literal_secrets,
    looks_like_secret,
    scan_repo,
)

# Assembled from short fragments so this file never contains a key-shaped literal itself.
FAKE_KEY = "".join(("QWERTYUIOP", "12345678"))  # 18 chars, upper + digit
MINIO_LIKE_KEY = "".join(("MinioPass", "20260910"))  # 17 chars, mixed case + digits


@pytest.mark.unit
@pytest.mark.parametrize(
    "line",
    [
        f"AV_KEY = '{FAKE_KEY}'",  # python assignment
        f'NEWS_KEY = "{FAKE_KEY}"',
        f'EIA_KEY="{FAKE_KEY}"',  # shell assignment
        f"export NOAA_TOKEN={FAKE_KEY}",
        f"ENV ALPHA_VANTAGE_KEY={FAKE_KEY}",  # Dockerfile
        f"Key: {FAKE_KEY}",  # prose in a prompt or doc
        f"  api_key: {FAKE_KEY}",  # yaml
        f'url = "https://api.example.com/q?api_key={FAKE_KEY}&f=json"',  # query string
        f'run_download "x" "https://x.gov/v2?apikey={FAKE_KEY}" out.json',
        f'FRED_KEY = os.environ.get("FRED_API_KEY", "{FAKE_KEY}")',  # getenv default
        f'KEY = os.getenv("ALPHA_VANTAGE_KEY", "{FAKE_KEY}")',
        f'    MINIO_SECRET_KEY: str = "{MINIO_LIKE_KEY}"',  # typed pydantic-settings default
        f"      MINIO_ROOT_PASSWORD: ${{MINIO_ROOT_PASSWORD:-{MINIO_LIKE_KEY}}}",  # compose default
        f'mc alias set grid http://minio:9000 root "{MINIO_LIKE_KEY}"; echo PASSWORD={MINIO_LIKE_KEY}',
    ],
)
def test_detects_key_shaped_literals(line: str) -> None:
    findings = find_literal_secrets(line, "sample.py")
    assert len(findings) == 1
    assert findings[0].line_no == 1
    assert FAKE_KEY not in str(findings[0]), "findings must be redacted"
    assert MINIO_LIKE_KEY not in str(findings[0]), "findings must be redacted"


@pytest.mark.unit
@pytest.mark.parametrize(
    "line",
    [
        "FRED_API_KEY=your_fred_api_key_here",  # .env.example placeholder
        "AV_KEY = settings.ALPHAVANTAGE_API_KEY",  # config-driven
        '_AV_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "")',  # empty default
        'EIA_KEY="${EIA_API_KEY:-}"',  # shell env lookup
        'mock_settings.FRED_API_KEY = "test-fred-key-1234"',  # hyphenated test value
        "const LS_KEY = 'grid_recent_searches';",  # snake_case constant
        'STORAGE_KEY = "gridwidgetpreferences"',  # single character class
        'params = {"apikey": AV_KEY}',  # variable, not literal
        "url = f'https://x/q?apikey={AV_KEY}'",  # interpolation, not literal
    ],
)
def test_ignores_env_lookups_placeholders_and_constants(line: str) -> None:
    assert find_literal_secrets(line, "sample.py") == []


@pytest.mark.unit
def test_looks_like_secret_requires_two_character_classes() -> None:
    assert looks_like_secret(FAKE_KEY)
    assert looks_like_secret("".join(("abcdefghij", "1234567890")))
    assert not looks_like_secret("abcdefghijklmnopqrstu")
    assert not looks_like_secret("1234567890123456")


@pytest.mark.unit
def test_repo_contains_no_literal_secrets() -> None:
    findings = scan_repo()
    assert not findings, "literal secrets committed:\n" + "\n".join(map(str, findings))
