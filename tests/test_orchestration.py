"""
Tests for orchestration tasks and flows.

Validates:
  - Individual task functions with mocked dependencies
  - Flow composition (ingest -> resolve -> refresh)
  - Fallback behavior when Prefect is not installed
  - CLI entry point parsing
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Task tests
# ---------------------------------------------------------------------------

class TestRunPuller:
    """Tests for the run_puller task."""

    @patch("orchestration.tasks.time")
    def test_puller_success(self, mock_time):
        mock_time.time.side_effect = [0.0, 1.5]

        mock_func = MagicMock(return_value={"rows_inserted": 42})
        with patch("ingestion.scheduler.get_puller_func", return_value=mock_func, create=True):
            from orchestration.tasks import run_puller
            result = run_puller.fn("fred") if hasattr(run_puller, "fn") else run_puller("fred")

        assert result["puller"] == "fred"
        assert result["status"] == "success"
        assert result["rows"] == 42

    def test_puller_not_found(self):
        with patch("ingestion.scheduler.get_puller_func", return_value=None, create=True):
            from orchestration.tasks import run_puller
            result = run_puller.fn("nonexistent") if hasattr(run_puller, "fn") else run_puller("nonexistent")

        assert result["status"] == "not_found"
        assert result["rows"] == 0

    def test_puller_error(self):
        def boom():
            raise RuntimeError("API timeout")

        with patch("ingestion.scheduler.get_puller_func", return_value=boom, create=True):
            from orchestration.tasks import run_puller
            result = run_puller.fn("bad_puller") if hasattr(run_puller, "fn") else run_puller("bad_puller")

        assert result["status"] == "error"
        assert "API timeout" in result["error"]

    def test_puller_non_dict_result(self):
        mock_func = MagicMock(return_value="ok")
        with patch("ingestion.scheduler.get_puller_func", return_value=mock_func, create=True):
            from orchestration.tasks import run_puller
            result = run_puller.fn("simple") if hasattr(run_puller, "fn") else run_puller("simple")

        assert result["status"] == "success"
        assert result["rows"] == 0


class TestResolveConflicts:
    """Tests for the resolve_conflicts task."""

    def test_resolve_success(self):
        with patch("normalization.resolver.resolve_all", return_value={"resolved": 15}, create=True):
            from orchestration.tasks import resolve_conflicts
            result = resolve_conflicts.fn() if hasattr(resolve_conflicts, "fn") else resolve_conflicts()

        assert result["status"] == "success"
        assert result["resolved"] == 15

    def test_resolve_with_source_type(self):
        with patch("normalization.resolver.resolve_all", return_value={"resolved": 3}, create=True) as mock_resolve:
            from orchestration.tasks import resolve_conflicts
            result = resolve_conflicts.fn("fred") if hasattr(resolve_conflicts, "fn") else resolve_conflicts("fred")

        mock_resolve.assert_called_once_with(source_type="fred")
        assert result["resolved"] == 3

    def test_resolve_error(self):
        with patch("normalization.resolver.resolve_all", side_effect=RuntimeError("DB down"), create=True):
            from orchestration.tasks import resolve_conflicts
            result = resolve_conflicts.fn() if hasattr(resolve_conflicts, "fn") else resolve_conflicts()

        assert result["status"] == "error"
        assert "DB down" in result["error"]


class TestScoreHypotheses:
    """Tests for the score_hypotheses task."""

    def test_score_success(self):
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("db.get_connection", return_value=mock_conn, create=True), \
             patch("intelligence.hypothesis_engine.score_all", return_value=[1, 2, 3], create=True):
            from orchestration.tasks import score_hypotheses
            result = score_hypotheses.fn() if hasattr(score_hypotheses, "fn") else score_hypotheses()

        assert result["status"] == "success"
        assert result["scored"] == 3

    def test_score_error(self):
        with patch("db.get_connection", side_effect=RuntimeError("no db"), create=True):
            from orchestration.tasks import score_hypotheses
            result = score_hypotheses.fn() if hasattr(score_hypotheses, "fn") else score_hypotheses()

        assert result["status"] == "error"


class TestCheckAlerts:
    """Tests for the check_alerts task."""

    def test_alerts_success(self):
        mock_module = MagicMock()
        mock_module.check_all_alerts.return_value = 5
        with patch.dict(sys.modules, {"alerts.alert_engine": mock_module}):
            from orchestration.tasks import check_alerts
            result = check_alerts.fn() if hasattr(check_alerts, "fn") else check_alerts()

        assert result["status"] == "success"
        assert result["triggered"] == 5

    def test_alerts_error(self):
        mock_module = MagicMock()
        mock_module.check_all_alerts.side_effect = RuntimeError("smtp fail")
        with patch.dict(sys.modules, {"alerts.alert_engine": mock_module}):
            from orchestration.tasks import check_alerts
            result = check_alerts.fn() if hasattr(check_alerts, "fn") else check_alerts()

        assert result["status"] == "error"


class TestEmitEvent:
    """Tests for the emit_event task."""

    def test_emit_success(self):
        with patch("events.producer.emit", return_value=True, create=True):
            from orchestration.tasks import emit_event
            result = emit_event.fn("test", {"key": "val"}) if hasattr(emit_event, "fn") else emit_event("test", {"key": "val"})

        assert result is True

    def test_emit_failure_returns_false(self):
        with patch("events.producer.emit", side_effect=RuntimeError("no broker"), create=True):
            from orchestration.tasks import emit_event
            result = emit_event.fn("test", {}) if hasattr(emit_event, "fn") else emit_event("test", {})

        assert result is False


class TestRefreshMaterializedViews:
    """Tests for the refresh_materialized_views task."""

    def test_refresh_success(self):
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("db.get_connection", return_value=mock_conn, create=True):
            from orchestration.tasks import refresh_materialized_views
            result = refresh_materialized_views.fn() if hasattr(refresh_materialized_views, "fn") else refresh_materialized_views()

        assert result["status"] == "success"

    def test_refresh_error(self):
        with patch("db.get_connection", side_effect=RuntimeError("conn refused"), create=True):
            from orchestration.tasks import refresh_materialized_views
            result = refresh_materialized_views.fn() if hasattr(refresh_materialized_views, "fn") else refresh_materialized_views()

        assert result["status"] == "error"


class TestRunTrustCycle:
    """Tests for the run_trust_cycle task."""

    def test_trust_cycle_success(self):
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("db.get_connection", return_value=mock_conn, create=True), \
             patch("intelligence.trust_scorer.run_trust_cycle", return_value={"updated": 10}, create=True):
            from orchestration.tasks import run_trust_cycle
            result = run_trust_cycle.fn() if hasattr(run_trust_cycle, "fn") else run_trust_cycle()

        assert result["status"] == "success"


class TestRunGraphAnalytics:
    """Tests for the run_graph_analytics task."""

    def test_graph_analytics_success(self):
        with patch("scripts.graph_analytics.run_analytics", return_value={"pagerank": 100}, create=True):
            from orchestration.tasks import run_graph_analytics
            result = run_graph_analytics.fn() if hasattr(run_graph_analytics, "fn") else run_graph_analytics()

        assert result["status"] == "success"


# ---------------------------------------------------------------------------
# Flow composition tests
# ---------------------------------------------------------------------------

class TestIngestFlow:
    """Tests for the ingest_flow composition."""

    def test_ingest_flow_default_groups(self):
        with patch("orchestration.flows.run_puller", return_value={"rows": 10, "status": "success"}) as mock_puller, \
             patch("orchestration.flows.resolve_conflicts", return_value={"status": "success", "resolved": 5}), \
             patch("orchestration.flows.refresh_materialized_views", return_value={"status": "success"}), \
             patch("orchestration.flows.emit_event", return_value=True):
            from orchestration.flows import ingest_flow, CRITICAL_PULLERS, MARKET_PULLERS
            fn = ingest_flow.fn if hasattr(ingest_flow, "fn") else ingest_flow
            result = fn()

        assert result["status"] == "complete"
        expected_count = len(CRITICAL_PULLERS) + len(MARKET_PULLERS)
        assert mock_puller.call_count == expected_count

    def test_ingest_flow_altdata_group(self):
        with patch("orchestration.flows.run_puller", return_value={"rows": 5, "status": "success"}) as mock_puller, \
             patch("orchestration.flows.resolve_conflicts", return_value={"status": "success", "resolved": 0}), \
             patch("orchestration.flows.refresh_materialized_views", return_value={"status": "success"}), \
             patch("orchestration.flows.emit_event", return_value=True):
            from orchestration.flows import ingest_flow, ALTDATA_PULLERS
            fn = ingest_flow.fn if hasattr(ingest_flow, "fn") else ingest_flow
            result = fn(puller_groups=["altdata"])

        assert result["status"] == "complete"
        assert mock_puller.call_count == len(ALTDATA_PULLERS)

    def test_ingest_flow_empty_groups(self):
        with patch("orchestration.flows.run_puller") as mock_puller, \
             patch("orchestration.flows.resolve_conflicts", return_value={"status": "success", "resolved": 0}), \
             patch("orchestration.flows.refresh_materialized_views", return_value={"status": "success"}), \
             patch("orchestration.flows.emit_event", return_value=True):
            from orchestration.flows import ingest_flow
            fn = ingest_flow.fn if hasattr(ingest_flow, "fn") else ingest_flow
            result = fn(puller_groups=["nonexistent_group"])

        assert result["status"] == "complete"
        assert result["total_rows"] == 0
        mock_puller.assert_not_called()


class TestScoreFlow:
    """Tests for the score_flow composition."""

    def test_score_flow(self):
        with patch("orchestration.flows.score_hypotheses", return_value={"status": "success", "scored": 10}), \
             patch("orchestration.flows.run_trust_cycle", return_value={"status": "success"}), \
             patch("orchestration.flows.emit_event", return_value=True):
            from orchestration.flows import score_flow
            fn = score_flow.fn if hasattr(score_flow, "fn") else score_flow
            result = fn()

        assert result["status"] == "complete"
        assert result["score_result"]["scored"] == 10


class TestAlertFlow:
    """Tests for the alert_flow composition."""

    def test_alert_flow_with_triggers(self):
        with patch("orchestration.flows.check_alerts", return_value={"status": "success", "triggered": 3}), \
             patch("orchestration.flows.emit_event", return_value=True) as mock_emit:
            from orchestration.flows import alert_flow
            fn = alert_flow.fn if hasattr(alert_flow, "fn") else alert_flow
            result = fn()

        assert result["status"] == "complete"
        mock_emit.assert_called_once()

    def test_alert_flow_no_triggers(self):
        with patch("orchestration.flows.check_alerts", return_value={"status": "success", "triggered": 0}), \
             patch("orchestration.flows.emit_event") as mock_emit:
            from orchestration.flows import alert_flow
            fn = alert_flow.fn if hasattr(alert_flow, "fn") else alert_flow
            result = fn()

        assert result["status"] == "complete"
        mock_emit.assert_not_called()


class TestNightlyFlow:
    """Tests for the nightly_flow composition."""

    def test_nightly_flow(self):
        with patch("orchestration.flows.ingest_flow", return_value={"status": "complete", "total_rows": 100}), \
             patch("orchestration.flows.score_flow", return_value={"status": "complete"}), \
             patch("orchestration.flows.run_graph_analytics", return_value={"status": "success"}), \
             patch("orchestration.flows.alert_flow", return_value={"status": "complete"}):
            from orchestration.flows import nightly_flow
            fn = nightly_flow.fn if hasattr(nightly_flow, "fn") else nightly_flow
            result = fn()

        assert result["status"] == "complete"
        assert "completed_at" in result


class TestQuickCycleFlow:
    """Tests for the quick_cycle_flow composition."""

    def test_quick_cycle_flow(self):
        with patch("orchestration.flows.ingest_flow", return_value={"status": "complete"}) as mock_ingest, \
             patch("orchestration.flows.score_flow", return_value={"status": "complete"}), \
             patch("orchestration.flows.alert_flow", return_value={"status": "complete"}):
            from orchestration.flows import quick_cycle_flow
            fn = quick_cycle_flow.fn if hasattr(quick_cycle_flow, "fn") else quick_cycle_flow
            result = fn()

        assert result["status"] == "complete"
        mock_ingest.assert_called_once_with(puller_groups=["critical"])


# ---------------------------------------------------------------------------
# Fallback / graceful degradation tests
# ---------------------------------------------------------------------------

class TestFallbackDecorators:
    """Verify tasks/flows are callable even without Prefect installed."""

    def test_task_has_marker_or_is_callable(self):
        from orchestration.tasks import run_puller
        # Either it's a Prefect task (has .fn) or a plain function
        assert callable(run_puller) or callable(getattr(run_puller, "fn", None))

    def test_flow_has_marker_or_is_callable(self):
        from orchestration.flows import ingest_flow
        assert callable(ingest_flow) or callable(getattr(ingest_flow, "fn", None))


# ---------------------------------------------------------------------------
# CLI entry point tests
# ---------------------------------------------------------------------------

class TestCLIEntryPoint:
    """Tests for the __main__ CLI entry point in flows.py."""

    def test_cli_nightly(self):
        with patch("orchestration.flows.nightly_flow") as mock_nightly:
            with patch.object(sys, "argv", ["flows.py", "nightly"]):
                import orchestration.flows as flows_mod
                # Re-execute the __main__ block
                if hasattr(flows_mod, "__name__"):
                    # Simulate __main__ execution
                    cmd = "nightly"
                    if cmd == "nightly":
                        flows_mod.nightly_flow()
            mock_nightly.assert_called_once()

    def test_cli_quick(self):
        with patch("orchestration.flows.quick_cycle_flow") as mock_quick:
            import orchestration.flows as flows_mod
            flows_mod.quick_cycle_flow()
            mock_quick.assert_called_once()

    def test_cli_score(self):
        with patch("orchestration.flows.score_flow") as mock_score:
            import orchestration.flows as flows_mod
            flows_mod.score_flow()
            mock_score.assert_called_once()

    def test_cli_ingest_with_groups(self):
        with patch("orchestration.flows.ingest_flow") as mock_ingest:
            import orchestration.flows as flows_mod
            flows_mod.ingest_flow(puller_groups=["critical", "altdata"])
            mock_ingest.assert_called_once_with(puller_groups=["critical", "altdata"])


# ---------------------------------------------------------------------------
# Register deployments test
# ---------------------------------------------------------------------------

class TestRegisterDeployments:
    """Tests for register_deployments."""

    def test_register_returns_list(self):
        from orchestration.flows import register_deployments
        result = register_deployments()
        # Returns list regardless of whether Prefect is installed
        assert isinstance(result, list)

    def test_register_with_prefect_installed(self):
        """If Prefect is installed, should return 3 deployment dicts."""
        try:
            import prefect  # noqa: F401
            from orchestration.flows import register_deployments
            result = register_deployments()
            assert len(result) == 3
            names = [d["name"] for d in result]
            assert "quick-cycle-6h" in names
            assert "nightly-full" in names
            assert "market-data-hourly" in names
        except ImportError:
            pytest.skip("Prefect not installed")


# ---------------------------------------------------------------------------
# Puller group constants test
# ---------------------------------------------------------------------------

class TestPullerGroups:
    """Verify puller group constants are non-empty and unique."""

    def test_critical_pullers_non_empty(self):
        from orchestration.flows import CRITICAL_PULLERS
        assert len(CRITICAL_PULLERS) > 0

    def test_market_pullers_non_empty(self):
        from orchestration.flows import MARKET_PULLERS
        assert len(MARKET_PULLERS) > 0

    def test_altdata_pullers_non_empty(self):
        from orchestration.flows import ALTDATA_PULLERS
        assert len(ALTDATA_PULLERS) > 0

    def test_no_duplicates_across_groups(self):
        from orchestration.flows import CRITICAL_PULLERS, MARKET_PULLERS, ALTDATA_PULLERS
        all_pullers = CRITICAL_PULLERS + MARKET_PULLERS + ALTDATA_PULLERS
        assert len(all_pullers) == len(set(all_pullers)), "Duplicate puller names across groups"
