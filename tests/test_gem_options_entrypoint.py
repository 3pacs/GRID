"""The retired GEM timer must enter the batch-aware options writer only."""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from ingestion import options
from scripts import pull_options_gem_tickers as gem

EXPECTED_TICKERS = [
    "OPCH", "SPGI", "FLUT", "GEHC", "GLND", "BHRB",
    "SPY", "QQQ", "IWM",
]


def _mock_writer(monkeypatch: pytest.MonkeyPatch, results: list[dict]) -> list:
    calls: list = []
    engine = object()
    monkeypatch.setitem(sys.modules, "db", SimpleNamespace(get_engine=lambda: engine))

    class Puller:
        def __init__(self, db_engine: object) -> None:
            calls.append(("constructor", db_engine))

        def pull_all(self, **kwargs) -> list[dict]:
            calls.append(("pull_all", kwargs))
            return results

    monkeypatch.setattr(options, "OptionsPuller", Puller)
    return calls


def test_gem_entrypoint_preserves_nine_tickers_and_six_expiries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert list(gem.GEM_TICKERS) == EXPECTED_TICKERS
    calls = _mock_writer(monkeypatch, [
        {"ticker": ticker, "status": "SUCCESS"} for ticker in EXPECTED_TICKERS
    ])

    assert gem.main() == 0
    assert calls[0][0] == "constructor"
    assert calls[1] == ("pull_all", {
        "tickers": EXPECTED_TICKERS,
        "include_catalyst_universe": False,
        "max_expirations": 6,
    })
    assert len(calls) == 2


@pytest.mark.parametrize("status", ["SKIPPED", "FAILED", "UNKNOWN", None])
def test_gem_entrypoint_exits_nonzero_for_any_incomplete_ticker(
    monkeypatch: pytest.MonkeyPatch, status: str | None,
) -> None:
    results = [{"ticker": ticker, "status": "SUCCESS"} for ticker in EXPECTED_TICKERS]
    results[4]["status"] = status
    _mock_writer(monkeypatch, results)

    assert gem.main() == 1


@pytest.mark.parametrize("results", [
    [],
    [{"ticker": ticker, "status": "SUCCESS"} for ticker in EXPECTED_TICKERS[:-1]],
    [{"ticker": EXPECTED_TICKERS[0], "status": "SUCCESS"}] * len(EXPECTED_TICKERS),
])
def test_gem_entrypoint_rejects_missing_or_duplicate_results(
    monkeypatch: pytest.MonkeyPatch, results: list[dict],
) -> None:
    _mock_writer(monkeypatch, results)
    assert gem.main() == 1


def test_gem_entrypoint_exception_is_nonzero_without_secret_text(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setitem(sys.modules, "db", SimpleNamespace(get_engine=lambda: object()))

    class BrokenPuller:
        def __init__(self, db_engine: object) -> None:
            raise RuntimeError("sensitive connection detail")

    monkeypatch.setattr(options, "OptionsPuller", BrokenPuller)
    assert gem.main() == 1
    assert "sensitive connection detail" not in capsys.readouterr().err
