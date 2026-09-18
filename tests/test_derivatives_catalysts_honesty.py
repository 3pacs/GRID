"""Audit C-H3 — the catalyst calendar may not be invented.

``api/routers/derivatives.py::_generate_catalysts`` used to plant a
hand-typed list of 2026 FOMC and CPI dates into the /flow-timeline response,
and ``intelligence/catalyst_aggregator.py`` held a second, *disagreeing*
hand-typed FOMC list. Neither was ingested, dated or sourced: they were
guesses rendered as observed events.

GRID ingests no macro event calendar — nothing stores a scheduled Fed or
BLS release with a ``source`` and an ``as_of``. So the honest output is an
explicit unavailable state, and the only catalysts that survive are the ones
that come out of a stored table (``earnings_calendar``) carrying their own
provenance.

These tests pin:

  * /flow-timeline reports ``catalysts_status`` / ``catalysts_reason``
    instead of emitting macro events,
  * a stored earnings row is rendered with its source and as_of,
  * a row without a real date yields no event at all,
  * neither source file contains a literal date calendar any more,
  * the aggregator seeds no FOMC event, and a catalyst-free window scores
    0.0 rather than defaulting to something.
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

os.environ.setdefault("DB_PASSWORD", "test-password")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("GRID_JWT_SECRET", "test-secret-key-for-testing-only")
os.environ.setdefault("GRID_JWT_EXPIRE_HOURS", "1")

from passlib.context import CryptContext

_pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
os.environ.setdefault("GRID_MASTER_PASSWORD_HASH", _pwd_ctx.hash("testpassword123"))

from fastapi.testclient import TestClient

from api.auth import create_token
from api.main import app
from intelligence.catalyst_aggregator import (
    CATALYST_FOMC,
    MACRO_CALENDAR_UNAVAILABLE_REASON,
    _seeded_market_events,
    events_for_window,
    proximity_score,
)

client = TestClient(app)

REPO_ROOT = Path(__file__).resolve().parents[1]

EXPECTED_REASON = (
    "no ingested macro event calendar; scheduled FOMC/CPI dates are not stored"
)


def _auth_header() -> dict[str, str]:
    return {"Authorization": f"Bearer {create_token(expires_hours=1)}"}


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _signal_rows(days_back: tuple[int, ...]) -> list[tuple]:
    """Rows shaped like the options_daily_signals SELECT in the endpoint."""
    return [
        (_today() - timedelta(days=n), 500.0, 0.2, 0.9, 1000)
        for n in days_back
    ]


def _wire_db(mock_db, *, signal_rows, earnings_rows):
    """Route the endpoint's two SELECTs to their own fixtures."""

    def _execute(query, params=None, *_a, **_kw):
        sql = str(query)
        result = MagicMock()
        if "earnings_calendar" in sql:
            result.fetchall.return_value = earnings_rows
        elif "options_daily_signals" in sql:
            result.fetchall.return_value = signal_rows
        else:
            result.fetchall.return_value = []
        return result

    conn = MagicMock()
    conn.execute = _execute
    mock_db.return_value.connect.return_value.__enter__ = MagicMock(
        return_value=conn
    )
    mock_db.return_value.connect.return_value.__exit__ = MagicMock(
        return_value=False
    )


def _profile(ticker, snap_date=None, **_kw):
    used = snap_date or _today()
    return {
        "available": True, "ticker": ticker, "snap_date": str(used),
        "spot": 500.0, "gex_aggregate": 1.5e9, "regime": "LONG_GAMMA",
    }


# ── 1. /flow-timeline response shape ───────────────────────────────────────


class TestFlowTimelineCatalysts:
    @patch("api.routers.derivatives._get_gex_engine")
    @patch("api.routers.derivatives.get_db_engine")
    def test_no_macro_calendar_is_unavailable_not_invented(self, mock_db, mock_gex):
        """Nothing stored → catalysts: [] plus an explicit status + reason."""
        _wire_db(mock_db, signal_rows=_signal_rows((20, 10)), earnings_rows=[])
        mock_gex.return_value.compute_gex_profile = _profile

        payload = client.get(
            "/api/v1/derivatives/flow-timeline/SPY?days=90", headers=_auth_header()
        ).json()

        assert payload["catalysts"] == []
        assert payload["catalysts_status"] == "unavailable"
        assert payload["catalysts_reason"] == EXPECTED_REASON
        assert payload["catalysts_reason"] == MACRO_CALENDAR_UNAVAILABLE_REASON

    @patch("api.routers.derivatives._get_gex_engine")
    @patch("api.routers.derivatives.get_db_engine")
    def test_no_fomc_or_cpi_marker_is_ever_emitted(self, mock_db, mock_gex):
        """Even with earnings present, no macro event appears."""
        pulled = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
        _wire_db(
            mock_db,
            signal_rows=_signal_rows((20, 10)),
            earnings_rows=[(_today() + timedelta(days=12), pulled)],
        )
        mock_gex.return_value.compute_gex_profile = _profile

        payload = client.get(
            "/api/v1/derivatives/flow-timeline/AAPL?days=90", headers=_auth_header()
        ).json()

        types = {c["type"] for c in payload["catalysts"]}
        assert "fomc" not in types
        assert "cpi" not in types

    @patch("api.routers.derivatives._get_gex_engine")
    @patch("api.routers.derivatives.get_db_engine")
    def test_stored_earnings_row_carries_source_and_as_of(self, mock_db, mock_gex):
        """The one real dated source in the tree is reported with provenance."""
        earnings_date = _today() + timedelta(days=12)
        pulled = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
        _wire_db(
            mock_db,
            signal_rows=_signal_rows((20, 10)),
            earnings_rows=[(earnings_date, pulled)],
        )
        mock_gex.return_value.compute_gex_profile = _profile

        payload = client.get(
            "/api/v1/derivatives/flow-timeline/AAPL?days=90", headers=_auth_header()
        ).json()

        assert len(payload["catalysts"]) == 1
        cat = payload["catalysts"][0]
        assert cat["date"] == str(earnings_date)
        assert cat["type"] == "earnings"
        assert cat["source"] == "earnings_calendar"
        assert cat["as_of"] == "2026-09-01"
        # Earnings are stored, the macro calendar still is not.
        assert payload["catalysts_status"] == "partial"
        assert payload["catalysts_reason"] == EXPECTED_REASON

    @patch("api.routers.derivatives._get_gex_engine")
    @patch("api.routers.derivatives.get_db_engine")
    def test_row_without_a_real_date_is_not_an_event(self, mock_db, mock_gex):
        """A NULL/garbage date column may not be turned into a marker."""
        _wire_db(
            mock_db,
            signal_rows=_signal_rows((20, 10)),
            earnings_rows=[(None, None), ("soon", None), (500.0, None)],
        )
        mock_gex.return_value.compute_gex_profile = _profile

        payload = client.get(
            "/api/v1/derivatives/flow-timeline/AAPL?days=90", headers=_auth_header()
        ).json()

        assert payload["catalysts"] == []
        assert payload["catalysts_status"] == "unavailable"


# ── 2. Source guards: no literal date calendars ────────────────────────────


_DATE_CTOR = re.compile(r"\bdate\(\s*(?:19|20)\d\d\s*,\s*\d+\s*,\s*\d+\s*\)")
_ISO_LITERAL = re.compile(r"""['"](?:19|20)\d\d-\d\d-\d\d['"]""")

_GUARDED = (
    Path("api") / "routers" / "derivatives.py",
    Path("intelligence") / "catalyst_aggregator.py",
)


class TestNoLiteralCalendarInSource:
    """Criterion: neither file may hold a typed-out calendar again.

    Scoped to *literal* dates — ``date(2026, 1, 28)`` constructor calls and
    ``"2026-01-28"`` strings. Date *parsing* and arithmetic
    (``date.fromisoformat``, ``date.today()``, ``timedelta``) is untouched
    by these patterns.
    """

    def test_no_literal_date_constructors(self):
        for rel in _GUARDED:
            src = (REPO_ROOT / rel).read_text(encoding="utf-8")
            hits = _DATE_CTOR.findall(src)
            assert not hits, f"{rel} carries literal calendar dates: {hits}"

    def test_no_literal_iso_date_strings(self):
        for rel in _GUARDED:
            src = (REPO_ROOT / rel).read_text(encoding="utf-8")
            hits = _ISO_LITERAL.findall(src)
            assert not hits, f"{rel} carries literal ISO dates: {hits}"

    def test_no_named_fomc_or_cpi_calendar_lists(self):
        agg = (REPO_ROOT / _GUARDED[1]).read_text(encoding="utf-8")
        deriv = (REPO_ROOT / _GUARDED[0]).read_text(encoding="utf-8")
        assert "_FOMC_DATES" not in agg
        assert "fomc_dates" not in deriv
        assert "cpi_dates" not in deriv


# ── 3. Aggregator: an empty macro calendar stays empty ─────────────────────


def _empty_engine():
    """Engine mock whose every SELECT returns no rows."""
    eng = MagicMock()
    conn = MagicMock()
    result = MagicMock()
    result.fetchall.return_value = []
    conn.execute.return_value = result
    conn.__enter__.return_value = conn
    conn.__exit__.return_value = False
    eng.connect.return_value = conn
    return eng


class TestAggregatorMacroCalendar:
    def test_seeded_events_contain_no_fomc(self):
        """The window that used to yield two seeded FOMC dates yields none."""
        events = _seeded_market_events(
            start=date(2026, 3, 1), end=date(2026, 4, 30),
        )
        assert [e for e in events if e.event_type == CATALYST_FOMC] == []

    def test_seeded_events_still_compute_opex(self):
        """OPEX is a rule of the listing calendar, not an observation."""
        events = _seeded_market_events(
            start=date(2026, 3, 1), end=date(2026, 4, 30),
        )
        assert events, "monthly OPEX should still be computed"
        assert all(e.event_type != CATALYST_FOMC for e in events)

    def test_window_with_no_catalyst_is_empty_not_placeholder(self):
        events = events_for_window(
            _empty_engine(), start=date(2026, 5, 1), end=date(2026, 5, 5),
        )
        assert events == []

    def test_fomc_week_scores_zero_rather_than_defaulting(self):
        """An unknown catalyst is absence of evidence, not a default score."""
        result = proximity_score(
            _empty_engine(), "SPY", as_of=date(2026, 4, 29), horizon_days=7,
        )
        assert result["score"] == 0.0
        assert result["nearest"] is None
        assert result["catalyst_type"] is None
        assert result["window_density"] == 0
