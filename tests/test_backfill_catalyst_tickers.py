"""Tests for the catalyst_calendar ticker backfill resolver.

The trial ingestor stored sponsor-name fragments (sponsor[:10]) in
catalyst_calendar.ticker whenever the SEC fuzzy match failed, breaking the
upcoming_catalysts join. These tests cover the pure classification/resolution
helpers offline (fake resolver, no network/DB) and confirm the script's run()
defaults to dry-run with no writes.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from scripts.backfill_catalyst_tickers import (
    looks_like_ticker,
    resolve_catalyst_ticker,
    run,
)


# ── looks_like_ticker ──

def test_real_tickers_are_recognized():
    for t in ["MRNA", "AAPL", "A", "BRK.A", "RDS-B", "TSM"]:
        assert looks_like_ticker(t), t


def test_sponsor_name_fragments_are_rejected():
    # These are exactly what sponsor[:10] produces.
    for bad in [
        "Moderna, I", "Genentech ", "Pfizer Inc", "moderna",
        "Novartis A", "", None, "AbbVie Inc", "TOOLONGSYM",
    ]:
        assert not looks_like_ticker(bad), repr(bad)


# ── resolve_catalyst_ticker ──

def _resolver(mapping):
    """Fake of _resolve_ticker_sec: lowercases + strips common suffixes."""
    _suffixes = [
        ", inc.", ", inc", " inc.", " inc", ", ltd.", ", ltd", " ltd.",
        " ltd", " llc", " plc", " corp.", " corp", " co.", " co",
        " s.a.", " ag", " se", " nv", " gmbh", " pty", " srl",
    ]

    def _norm(name: str) -> str:
        n = name.strip().lower()
        for suf in _suffixes:
            if n.endswith(suf):
                return n[: -len(suf)].strip()
        return n

    def _r(name: str):
        return mapping.get(_norm(name))
    return _r


def test_keeps_existing_valid_ticker_no_update():
    r = _resolver({"moderna": "MRNA"})
    # Already a good ticker -> None (no change needed).
    assert resolve_catalyst_ticker("MRNA", None, None, r) is None


def test_resolves_garbage_ticker_via_sponsor_in_notes():
    r = _resolver({"moderna": "MRNA"})
    out = resolve_catalyst_ticker(
        "Moderna, I", None, "Sponsor: Moderna, Inc.", r
    )
    assert out == "MRNA"


def test_does_not_resolve_using_garbage_value_as_last_resort_name():
    # No sponsor metadata; truncated ticker text is unsafe SEC substring input.
    r = _resolver({"genentech": "DNA"})
    out = resolve_catalyst_ticker("genentech", None, None, r)
    assert out is None


def test_does_not_resolve_institution_fragment_without_sponsor_metadata():
    r = _resolver({"university": "UNIB"})
    out = resolve_catalyst_ticker("University", None, None, r)
    assert out is None


def test_explicit_sponsor_name_takes_priority():
    r = _resolver({"acme therapeutics": "ACME"})
    out = resolve_catalyst_ticker(
        "Acme Ther", "Acme Therapeutics", "Sponsor: Wrong Co", r
    )
    assert out == "ACME"


def test_unresolvable_returns_none_never_writes_a_guess():
    r = _resolver({})  # resolver knows nothing
    assert resolve_catalyst_ticker("Unknown Bi", None, None, r) is None


def test_does_not_update_when_resolved_equals_current():
    r = _resolver({"moderna": "MRNA"})
    # Current already MRNA-equivalent -> looks_like_ticker short-circuits.
    assert resolve_catalyst_ticker("MRNA", "Moderna", None, r) is None


def test_rejects_resolver_returning_non_ticker():
    r = _resolver({"weird co": "this is not a ticker"})
    assert resolve_catalyst_ticker("Weird Co f", "Weird Co", None, r) is None


def test_parses_leadsponsor_equals_notes_format():
    r = _resolver({"biontech se": "BNTX", "biontech": "BNTX"})
    out = resolve_catalyst_ticker(
        "BioNTech S", None, "leadSponsor=BioNTech SE", r
    )
    assert out == "BNTX"


# ── run() dry-run safety ──

def test_run_dry_run_makes_no_writes():
    fake_rows = [
        (1, "Moderna, I", "Study title", "Moderna, Inc."),  # resolvable
        (2, "AAPL", None, None),                             # already valid
        (3, "Unknown Bi", None, None),                       # unresolvable
    ]
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = fake_rows
    engine.connect.return_value.__enter__.return_value = conn
    begin_conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = begin_conn

    with patch(
        "scripts.backfill_catalyst_tickers._get_engine", return_value=engine
    ), patch(
        "grid.signals.trial_signal._resolve_ticker_sec",
        side_effect=_resolver({"moderna": "MRNA"}),
    ):
        summary = run(apply=False, limit=None, deactivate_unresolved=False)

    assert summary["dry_run"] is True
    assert summary["scanned"] == 3
    assert summary["needs_fix"] == 2   # rows 1 and 3
    assert summary["resolved"] == 1    # only row 1
    assert summary["writes"] == 0      # dry-run: nothing written
    # engine.begin (the write transaction) must never be entered in dry-run.
    engine.begin.assert_not_called()


def test_run_apply_writes_resolved_and_deactivates_unresolved():
    fake_rows = [
        (1, "Moderna, I", "Study title", "Moderna, Inc."),  # update
        (2, "AAPL", None, None),                            # valid -> skip
        (3, "Unknown Bi", None, None),                      # deactivate
    ]
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = fake_rows
    engine.connect.return_value.__enter__.return_value = conn
    begin_conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = begin_conn

    with patch(
        "scripts.backfill_catalyst_tickers._get_engine", return_value=engine
    ), patch(
        "grid.signals.trial_signal._resolve_ticker_sec",
        side_effect=_resolver({"moderna": "MRNA"}),
    ):
        summary = run(apply=True, limit=None, deactivate_unresolved=True)

    assert summary["writes"] == 1       # row 1 ticker updated
    assert summary["deactivated"] == 1  # row 3 deactivated
    # One UPDATE for the ticker + one UPDATE for the deactivation.
    assert begin_conn.execute.call_count == 2
