"""Tests that dead 13F CIKs (404) are skipped, not flooded as errors.

Regression guard for the production symptom where three dead CIKs
(BlackRock, ExodusPoint, Tiger Global) produced 14 ERROR-level log
entries each per pull cycle.  The puller now treats permanent HTTP
failures as SKIPPED + warning.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from ingestion.altdata.institutional_flows import InstitutionalFlowsPuller


def _make_puller() -> InstitutionalFlowsPuller:
    puller = InstitutionalFlowsPuller.__new__(InstitutionalFlowsPuller)
    puller.engine = MagicMock()
    puller.source_id = 1
    return puller


def _http_error(status: int) -> requests.HTTPError:
    resp = MagicMock()
    resp.status_code = status
    return requests.HTTPError(f"{status} Client Error", response=resp)


class TestPull13FDeadCIK:
    def test_404_cik_is_skipped_not_failed(self):
        """A 404 CIK returns status=SKIPPED, never status=FAILED."""
        puller = _make_puller()

        with patch.object(
            puller, "_fetch_13f_index", side_effect=_http_error(404)
        ), patch(
            "ingestion.altdata.institutional_flows.TOP_13F_FILERS",
            {"9999999": "Dead Fund"},
        ):
            results = puller._pull_13f_filings()

        assert len(results) == 1
        assert results[0]["status"] == "SKIPPED"
        assert "404" in results[0]["error"]

    def test_403_cik_is_skipped_not_failed(self):
        """Permanent 403s are also treated as skipped."""
        puller = _make_puller()

        with patch.object(
            puller, "_fetch_13f_index", side_effect=_http_error(403)
        ), patch(
            "ingestion.altdata.institutional_flows.TOP_13F_FILERS",
            {"9999999": "Locked Fund"},
        ):
            results = puller._pull_13f_filings()

        assert results[0]["status"] == "SKIPPED"

    def test_transient_error_still_fails(self):
        """Non-HTTP exceptions still bubble up as FAILED for visibility."""
        puller = _make_puller()

        with patch.object(
            puller, "_fetch_13f_index", side_effect=RuntimeError("boom")
        ), patch(
            "ingestion.altdata.institutional_flows.TOP_13F_FILERS",
            {"9999999": "Unknown Failure"},
        ):
            results = puller._pull_13f_filings()

        assert results[0]["status"] == "FAILED"
        assert "boom" in results[0]["error"]
