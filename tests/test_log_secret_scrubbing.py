"""Credentials never reach the journal or pull_log through logged exception text.

2026-11-04: a puller's ``requests`` HTTPError message ("400 Client Error ... for url: ...?token=<key>") went to the
journal and pull_log.error_message verbatim. The log patcher installed in config.py and PullContext now scrub it.
"""

from __future__ import annotations

import io
import types

import pytest
from loguru import logger

from server_log.sanitizer import Sanitizer, scrub_secrets, secret_values_from_settings

KEY = "SYNTHETIC_" + "x" * 31  # 40 chars, key-shaped; built at runtime so it is never a committed literal


@pytest.mark.parametrize("url", [
    f"https://api.tiingo.com/tiingo/fundamentals/ABBV/daily?token={KEY}&startDate=2020-01-01",
    f"https://financialmodelingprep.com/api/v3/x?apikey={KEY}",
    f"https://api.polygon.io/v2/aggs?adjusted=true&apiKey={KEY}",
    f"https://api.eia.gov/v2/x?frequency=daily&api_key={KEY}&start=2026-01-01",
    f"https://quickstats.nass.usda.gov/api?key={KEY}&format=JSON",
    f"https://example.com/x?access_token={KEY}#frag",
])
def test_query_string_credentials_scrubbed(url):
    out = Sanitizer(generic=False).scrub(f"400 Client Error: Bad Request for url: {url}")
    assert KEY not in out
    assert "[REDACTED]" in out
    assert out.startswith("400 Client Error: Bad Request for url: https://")  # the useful part survives


def test_non_secret_query_params_kept():
    out = Sanitizer(generic=False).scrub("GET https://x.test/a?startDate=2020-01-01&monkey=1&tokens_used=5")
    assert out == "GET https://x.test/a?startDate=2020-01-01&monkey=1&tokens_used=5"


def test_every_line_mode_keeps_git_shas():
    sha = "35b25120cd8f237a96c3b86002cb34e8b50ce0fb"
    msg = f"release dir: /data/grid_v4/grid_release.releases/{sha} code_sha: {sha}"
    assert Sanitizer(generic=False).scrub(msg) == msg
    assert "code_sha: [REDACTED_KEY]" in Sanitizer(generic=True).scrub(msg)  # the error-file sink keeps its rule


def test_secret_values_collected_by_field_name():
    settings = types.SimpleNamespace(
        TIINGO_API_KEY="tiingo-value-1234", SCHWAB_APP_SECRET="schwab-secret-1234", NOAA_TOKEN="noaa-token-1234",
        KALSHI_PASSWORD="kalshi-pass-1234", SCHWAB_TOKEN_FILE="/home/grid/.schwab/token.json",
        VAPID_PUBLIC_KEY="public-not-secret", HERMES_MAX_COMPLETION_TOKENS=512, LOG_LEVEL="INFO",
        JQUANTS_EMAIL="someone@example.test",
    )
    values = set(secret_values_from_settings(settings))
    assert {"tiingo-value-1234", "schwab-secret-1234", "noaa-token-1234", "kalshi-pass-1234",
            "someone@example.test"} <= values
    assert not values & {"/home/grid/.schwab/token.json", "public-not-secret", "INFO"}


def test_exact_values_scrubbed_anywhere():
    s = Sanitizer(["tiingo-value-1234"], generic=False)
    assert "tiingo-value-1234" not in s.scrub("puller init with key tiingo-value-1234 failed")


def test_short_setting_values_not_scrubbed_in_every_line_mode():
    s = Sanitizer(["USDC", "tiingo-value-1234"], generic=False, min_secret_len=8)
    assert s.scrub("paid 5 USDC; key tiingo-value-1234") == "paid 5 USDC; key [REDACTED]"


def test_scrub_secrets_handles_empty_and_never_raises():
    assert scrub_secrets("") == ""
    assert KEY not in scrub_secrets(f"for url: https://x.test/?token={KEY}")


def test_config_installs_log_patcher(monkeypatch):
    """Every record is scrubbed before a sink sees it, including sinks added after config import."""
    monkeypatch.setenv("DB_PASSWORD", "synthetic-test-password")  # config refuses to load without one
    import config  # noqa: F401  (installs the patcher)

    buf = io.StringIO()
    sink_id = logger.add(buf, format="{message}", level="DEBUG")
    try:
        logger.error("Pull FAILED: X — HTTPError: 400 for url: https://x.test/a?apikey={k}&b=1", k=KEY)
    finally:
        logger.remove(sink_id)
    out = buf.getvalue()
    assert KEY not in out
    assert "apikey=[REDACTED]" in out


def test_console_sink_scrubs_tracebacks_and_extras(monkeypatch, capsys):
    """log.exception() renders the exception text only inside the sink; the console sink scrubs that line too."""
    monkeypatch.setenv("DB_PASSWORD", "synthetic-test-password")
    import config  # noqa: F401

    try:
        raise RuntimeError(f"400 Client Error: Bad Request for url: https://x.test/a?token={KEY}")
    except RuntimeError:
        logger.bind(url=f"https://x.test/b?api_key={KEY}").opt(colors=False).exception("pull failed")
    err = capsys.readouterr().err
    assert "pull failed" in err and "RuntimeError" in err  # the traceback is still there
    assert KEY not in err
    assert "token=[REDACTED]" in err
    assert "└" not in err  # no diagnose variable dump: bare values there carry no key= for the patterns to catch


def test_config_imported_before_log_remove():
    """Importing config adds its console sink, so a script must import it before log.remove(), not after.

    Imported after, the first import re-adds config's sink next to the script's own and every line logs twice
    (seen on grid-backlinker, 2026-11-05).
    """
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[1]
    wrong = []
    for top in ("api", "ingestion", "intelligence", "scripts", "orchestration", "store", "evals"):
        for p in (root / top).rglob("*.py") if (root / top).is_dir() else ():
            src = p.read_text(encoding="utf-8", errors="replace")
            for m in re.finditer(r"^([ \t]*)(?:log|logger)\.remove\(\)[^\n]*\n\1from config import[^\n]*scrubbed_stderr", src, re.M):
                wrong.append(f"{p.relative_to(root)}:{src[:m.start()].count(chr(10)) + 1}")
    assert wrong == []


def test_stderr_sinks_keep_diagnose_off():
    """Every scrubbed_stderr sink passes diagnose=LOG_DIAGNOSE, so tracebacks never dump local variable values."""
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[1]
    add = re.compile(r"\b(?:log|logger)\.add\(\s*scrubbed_stderr\b[^)]*\)", re.S)
    missing = []
    for top in ("api", "ingestion", "intelligence", "scripts", "orchestration", "store", "evals"):
        for p in (root / top).rglob("*.py") if (root / top).is_dir() else ():
            for call in add.findall(p.read_text(encoding="utf-8", errors="replace")):
                if "diagnose=LOG_DIAGNOSE" not in call:
                    missing.append(f"{p.relative_to(root)}: {call}")
    assert missing == []


def test_value_with_ampersand_or_hash_fully_scrubbed():
    """A credential containing & or # is redacted whole, not cut at the delimiter."""
    out = Sanitizer(generic=False).scrub("config: api_key=qa1&qb2#qc3 password: qd4&qe5#qf6 token=qg7&qh8")
    assert out == "config: api_key=[REDACTED] password: [REDACTED] token=[REDACTED]"


def test_no_raw_stderr_sinks_outside_config():
    """A sink added as sys.stderr would print unscrubbed tracebacks; scripts must use config.scrubbed_stderr."""
    import pathlib
    import re

    root = pathlib.Path(__file__).resolve().parents[1]
    raw = re.compile(r"\b(?:log|logger)\.add\(\s*sys\.stderr\b")
    offenders = [
        f"{p.relative_to(root)}"
        for top in ("api", "ingestion", "intelligence", "scripts", "orchestration", "store", "evals")
        if (root / top).is_dir()
        for p in (root / top).rglob("*.py")
        if raw.search(p.read_text(encoding="utf-8", errors="replace"))
    ]
    assert offenders == []


def test_pull_context_scrubs_persisted_error():
    from sqlalchemy import create_engine, text

    from ingestion.pull_context import PullContext

    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text(  # same shape as tests/test_pr735_composed_callers.py
            "CREATE TABLE pull_log (id INTEGER PRIMARY KEY AUTOINCREMENT, puller_name TEXT NOT NULL, "
            "source_id INTEGER, started_at TIMESTAMP NOT NULL, completed_at TIMESTAMP, status TEXT, "
            "rows_inserted INTEGER, rows_expected INTEGER, error_message TEXT, node_name TEXT, features_affected TEXT)"))
    with pytest.raises(RuntimeError):
        # require_persisted_log reads the UPDATE ... RETURNING row, which SQLite needs before it can commit
        with PullContext(engine, "Example_Source", require_persisted_log=True):
            raise RuntimeError(f"400 Client Error: Bad Request for url: https://x.test/a?token={KEY}")
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT status, error_message FROM pull_log")).fetchall()
    assert rows, "PullContext wrote no pull_log row"
    status, err = rows[-1]
    assert status == "FAILED"
    assert KEY not in err and "token=[REDACTED]" in err
