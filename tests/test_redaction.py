"""utils.redaction: credentials must not reach logs, alert emails or LLM prompts.

2026-11-04: the Polygon key appeared in 60 grid-scheduler journal lines in
7 days, via ingestion.base.retry_on_failure logging str(exc) with the
request URL in it.
"""

from __future__ import annotations

import pytest
import requests

from utils.redaction import redact_secrets


@pytest.mark.parametrize(
    "text, secret",
    [
        ("403 for url: https://api.polygon.io/v3/snapshot/options/SPY?apiKey=PolyKey123456", "PolyKey123456"),
        ("https://api.stlouisfed.org/fred/series/observations?series_id=X&api_key=fredkey999", "fredkey999"),
        ("400 for url: https://firms.modaps.eosdis.nasa.gov/api/area/csv/0123456789abcdef0123456789abcdef/VIIRS_SNPP_NRT/world/1",
         "0123456789abcdef0123456789abcdef"),
        ("postgresql://grid:p@ss@db:5432/griddb", "p@ss"),
        ('{"api_key": "json secret"}', "json secret"),
        ("Authorization: Bearer abcdefghijkl", "abcdefghijkl"),
    ],
)
def test_secret_is_removed(text: str, secret: str) -> None:
    assert secret not in redact_secrets(text)


def test_plain_text_is_unchanged() -> None:
    msg = "SOMA par value missing for 2026-10-28"
    assert redact_secrets(msg) == msg
    assert redact_secrets(None) is None


def test_retry_wrapper_never_logs_the_url_key(monkeypatch) -> None:
    from ingestion import base

    logged: list[str] = []
    monkeypatch.setattr(base.log, "warning", lambda msg, **kw: logged.append(msg.format(**kw)))
    monkeypatch.setattr(base.time, "sleep", lambda s: None)

    @base.retry_on_failure(max_attempts=2, backoff=0.0)
    def fetch() -> None:
        raise requests.exceptions.ConnectionError(
            "Max retries exceeded with url: https://api.polygon.io/v3/x?apiKey=PolyKey123456"
        )

    with pytest.raises(requests.exceptions.ConnectionError):
        fetch()
    assert logged and all("PolyKey123456" not in line for line in logged)


def test_log_pull_failure_redacts(monkeypatch) -> None:
    from ingestion import base

    logged: list[str] = []
    monkeypatch.setattr(base.log, "warning", lambda msg, **kw: logged.append(msg.format(**kw)))
    base.log_pull_failure("polygon", "SPY", requests.exceptions.HTTPError(
        "403 Client Error for url: https://api.polygon.io/v3/x?apiKey=PolyKey123456"))
    assert logged and "PolyKey123456" not in logged[0]


def test_nasa_fire_pull_failure_redacts_map_key(monkeypatch) -> None:
    from ingestion.physical import nasa_fire

    key = "0123456789abcdef0123456789abcdef"
    cls = next(v for v in vars(nasa_fire).values() if isinstance(v, type) and hasattr(v, "_fetch_csv"))
    puller = object.__new__(cls)

    def boom(days: int = 1):
        raise requests.exceptions.HTTPError(
            f"400 Client Error for url: https://firms.modaps.eosdis.nasa.gov/api/area/csv/{key}/VIIRS_SNPP_NRT/x/1"
        )

    logged: list[str] = []
    monkeypatch.setattr(puller, "_fetch_csv", boom, raising=False)
    monkeypatch.setattr(nasa_fire.log, "warning", lambda msg, **kw: logged.append(msg.format(**kw)))
    out = puller.pull()
    assert out["status"] == "FAILED" and key not in out["error"]
    assert logged and key not in logged[0]
