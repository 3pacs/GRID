"""Scheduler wiring for the SEC 13F live (``institutional_holdings``) writer.

``ingestion.scheduler.run_monthly_pulls`` is the job that already runs on a
live cadence (see ``ingestion/scheduler.py``'s BLS + institutional_flows
calls above the block under test). It was reviving ``institutional_holdings``
by adding a ``SEC13FLiveIngestor.run()`` call there — previously that writer
was only catalogued in ``scripts/hermes_operator.py``'s ``_SOURCE_EXTRAS``
(read by the PULL FIXER's diagnostics) but never actually invoked by
anything, which is why the table stopped ingesting new quarters after
2026-04-12.

These tests mock every external dependency (BLS, EDGAR-backed pullers, DB
engine) — no live endpoints are hit — and assert only the wiring: the new
call happens, uses the shared engine, and a failure there is caught the same
way the sibling BLS/institutional_flows calls are (logged, alerted, and does
not stop the rest of the monthly job).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import db
import ingestion.altdata.institutional_flows as iflows_mod
import ingestion.altdata.sec_13f_live as sec13f_mod
import ingestion.bls as bls_mod
from ingestion import scheduler as scheduler_mod


def _quiet_bls_and_institutional_flows(monkeypatch, fake_engine) -> None:
    """Neutralize the two pulls that run before the 13F-live block."""
    monkeypatch.setattr(db, "get_engine", lambda: fake_engine)

    fake_bls = MagicMock()
    fake_bls.pull_series.return_value = {"rows_inserted": 0, "status": "ok"}
    monkeypatch.setattr(bls_mod, "BLSPuller", MagicMock(return_value=fake_bls))

    fake_iflows = MagicMock()
    fake_iflows.pull_13f_only.return_value = []
    monkeypatch.setattr(
        iflows_mod, "InstitutionalFlowsPuller", MagicMock(return_value=fake_iflows)
    )

    # Alerts spawn a background thread when misconfigured — harmless, but
    # mock it too so the test asserts nothing about email side effects.
    monkeypatch.setattr("alerts.email.alert_on_failure", MagicMock())


def test_run_monthly_pulls_invokes_sec_13f_live_ingestor(monkeypatch):
    fake_engine = object()
    _quiet_bls_and_institutional_flows(monkeypatch, fake_engine)

    fake_ingestor = MagicMock()
    fake_ingestor.run.return_value = []
    ctor = MagicMock(return_value=fake_ingestor)
    monkeypatch.setattr(sec13f_mod, "SEC13FLiveIngestor", ctor)

    scheduler_mod.run_monthly_pulls()

    ctor.assert_called_once_with(engine=fake_engine)
    fake_ingestor.run.assert_called_once_with()


def test_run_monthly_pulls_survives_sec_13f_live_failure(monkeypatch):
    """A 13F-live failure must not take down the rest of the monthly job."""
    fake_engine = object()
    _quiet_bls_and_institutional_flows(monkeypatch, fake_engine)

    monkeypatch.setattr(
        sec13f_mod, "SEC13FLiveIngestor", MagicMock(side_effect=RuntimeError("boom"))
    )

    # Must not raise.
    scheduler_mod.run_monthly_pulls()
