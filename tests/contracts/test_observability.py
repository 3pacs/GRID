from __future__ import annotations

from contracts.observability import (
    emitted,
    dispatched,
    failed,
    record_duration,
    snapshot,
    render_prometheus,
    reset,
)


def setup_function(_func):
    reset()


def test_emitted_counter_increments():
    emitted("PullLifecycle")
    emitted("PullLifecycle")
    emitted("SignalFired")
    snap = snapshot()
    assert snap["emitted"]["PullLifecycle"] == 2
    assert snap["emitted"]["SignalFired"] == 1


def test_dispatched_counter_keys_by_handler():
    dispatched("PullLifecycle", "contracts.handlers.alerts.x")
    dispatched("PullLifecycle", "contracts.handlers.alerts.x")
    dispatched("PullLifecycle", "contracts.handlers.sse.y")
    snap = snapshot()
    assert snap["dispatched"][("PullLifecycle", "contracts.handlers.alerts.x")] == 2
    assert snap["dispatched"][("PullLifecycle", "contracts.handlers.sse.y")] == 1


def test_failed_counter_by_error_type():
    failed("PullLifecycle", "contracts.handlers.x", "CONSUMER_EXCEPTION")
    snap = snapshot()
    assert snap["failed"][("PullLifecycle", "contracts.handlers.x", "CONSUMER_EXCEPTION")] == 1


def test_duration_histogram_records_samples():
    record_duration("PullLifecycle", "contracts.handlers.x", 0.12)
    record_duration("PullLifecycle", "contracts.handlers.x", 0.08)
    snap = snapshot()
    assert snap["duration_count"][("PullLifecycle", "contracts.handlers.x")] == 2
    assert abs(snap["duration_sum"][("PullLifecycle", "contracts.handlers.x")] - 0.20) < 1e-9


def test_render_prometheus_produces_text_format():
    emitted("PullLifecycle")
    dispatched("PullLifecycle", "h.x")
    failed("PullLifecycle", "h.x", "BOOM")
    record_duration("PullLifecycle", "h.x", 0.05)

    body = render_prometheus()
    assert "# HELP contracts_emitted_total" in body
    assert 'contracts_emitted_total{contract="PullLifecycle"} 1' in body
    assert 'contracts_dispatched_total{contract="PullLifecycle",consumer="h.x"} 1' in body
    assert 'contracts_failed_total{contract="PullLifecycle",consumer="h.x",error="BOOM"} 1' in body
    assert "contracts_handler_duration_seconds_sum" in body
    assert "contracts_handler_duration_seconds_count" in body
