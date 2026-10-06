"""FRED transport failures: no failure row dated today, and a real read timeout.

Through September 2026 the FRED job at 16:02Z/20:01Z wrote ``FAILED value=0``
rows for *today's* VIXCLS/T10Y2Y/DFF with payload
``RetryError[ReadTimeout]`` — fedfred's hard-coded 10 s timeout, logged as
an application ERROR, then rate-limiting the next cycle via ``_row_exists``.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest
import tenacity
from loguru import logger

from ingestion import fred


def _retry_error(inner: BaseException) -> tenacity.RetryError:
    """What fedfred raises after its three tenacity attempts."""
    attempt = tenacity.Future(attempt_number=3)
    attempt.set_exception(inner)
    return tenacity.RetryError(attempt)


def _puller(monkeypatch, raise_exc: BaseException):
    puller = fred.FREDPuller.__new__(fred.FREDPuller)
    puller.engine, puller.source_id = MagicMock(), 1

    def fetch(_sid, **_kwargs):
        raise raise_exc

    puller.fred = SimpleNamespace(get_series_observations=fetch)
    monkeypatch.setattr(fred.time, "sleep", lambda *_a: None)
    return puller


def _levels(fn) -> list[tuple[str, str]]:
    records: list[tuple[str, str]] = []
    sink = logger.add(lambda m: records.append((m.record["level"].name, m.record["message"])),
                      level="WARNING")
    try:
        fn()
    finally:
        logger.remove(sink)
    return records


@pytest.mark.unit
@pytest.mark.parametrize("exc", [
    _retry_error(httpx.ReadTimeout("FRED slow")),
    _retry_error(httpx.ConnectError("reset")),
    httpx.ReadTimeout("bare timeout"),
    _retry_error(TimeoutError("socket")),
])
def test_transport_failure_is_skipped_without_a_failure_row(monkeypatch, exc):
    puller = _puller(monkeypatch, exc)
    with patch.object(puller, "_record_failure") as record_failure:
        records = _levels(lambda: setattr(puller, "_out", puller.pull_series("VIXCLS")))

    out = puller._out
    assert out["status"] == "SKIPPED"
    assert out["rows_inserted"] == 0
    assert any(e.startswith("transient ") for e in out["errors"])
    record_failure.assert_not_called()
    puller.engine.connect.assert_not_called()
    levels = {lvl for lvl, _ in records}
    assert "WARNING" in levels and "ERROR" not in levels, records
    assert "VIXCLS" in " ".join(msg for _, msg in records)


@pytest.mark.unit
def test_non_transport_bug_still_fails_with_a_failure_row(monkeypatch):
    puller = _puller(monkeypatch, _retry_error(ValueError("bad frame")))
    with patch.object(puller, "_record_failure") as record_failure:
        records = _levels(lambda: setattr(puller, "_out", puller.pull_series("VIXCLS")))

    assert puller._out["status"] == "FAILED"
    record_failure.assert_called_once()
    assert "ERROR" in {lvl for lvl, _ in records}


@pytest.mark.unit
def test_fedfred_client_uses_the_configured_read_timeout():
    import fedfred.clients as fedfred_clients

    fred._install_patient_httpx()
    fred._install_patient_httpx()  # idempotent: second call must not re-wrap
    assert isinstance(fedfred_clients.httpx, fred._PatientHttpx)
    assert fedfred_clients.httpx.Client is fred._PatientClient
    assert fedfred_clients.httpx.Timeout is httpx.Timeout  # delegation intact

    seen: dict[str, object] = {}

    def fake_get(self, url, *args, timeout=None, **kwargs):
        seen["timeout"] = timeout
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {})

    with patch.object(httpx.Client, "get", fake_get):
        with fedfred_clients.httpx.Client() as client:
            # Exactly what fedfred does: an explicit timeout=10 on every GET.
            client.get("https://api.stlouisfed.org/fred/series/observations", timeout=10)

    assert seen["timeout"] == fred.FRED_HTTP_TIMEOUT
    assert fred.FRED_HTTP_TIMEOUT > 10
