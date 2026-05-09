"""Pin error-severity classifications against repeat-offender flooders.

Each case here represents a class of message that previously polluted
``.server-logs/errors.jsonl`` at ERROR severity even though it was a
transient/environmental issue handled gracefully by the caller. CLAUDE.md
reserves ERROR for unhandled application bugs — the assertions below
guard the boundary so future regressions are caught by CI before they
ship.
"""

from __future__ import annotations

from typing import Any

import pytest
from loguru import logger


def _capture(callable_, *args, **kwargs) -> list[tuple[str, str]]:
    records: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda msg: records.append(
            (msg.record["level"].name, msg.record["message"])
        ),
        level="WARNING",
    )
    try:
        callable_(*args, **kwargs)
    finally:
        logger.remove(sink_id)
    return records


class TestFinBERTSeverityRegressions:
    """FinBERT scoring failures from transformers init / GPU issues are
    environmental, not application bugs. They must classify as WARNING."""

    def test_transformers_init_wrap_classifies_as_warning(self):
        from ingestion.ml._finbert_severity import is_transformers_init_wrap

        exc = RuntimeError(
            "Could not import module 'BertForSequenceClassification'. "
            "Are this object's requirements defined correctly?"
        )
        assert is_transformers_init_wrap(exc) is True

    def test_oom_classifies_as_warning(self):
        from ingestion.ml._finbert_severity import is_transformers_init_wrap

        assert is_transformers_init_wrap(MemoryError("out of memory")) is True
        assert is_transformers_init_wrap(OSError("disk")) is True

    def test_unknown_runtime_error_classifies_as_error(self):
        from ingestion.ml._finbert_severity import is_transformers_init_wrap

        # Genuine bugs must NOT be classified as warning so they surface
        # in errors.jsonl at ERROR severity.
        assert is_transformers_init_wrap(RuntimeError("unrelated bug")) is False
        assert is_transformers_init_wrap(KeyError("missing")) is False


class TestTimesFMSeverityRegressions:
    """The P100 GPU lacks the SM compute capability TimesFM was built for.
    The CUDA kernel error is environmental — must log WARNING, not ERROR."""

    def test_cuda_kernel_incompat_classifier(self):
        try:
            from inference.timesfm_service import _is_cuda_kernel_incompat
        except Exception:
            pytest.skip("timesfm_service deps missing")

        cuda_err = RuntimeError(
            "CUDA error: no kernel image is available for execution on the device. "
            "Search for `cudaErrorNoKernelImageForDevice'..."
        )
        assert _is_cuda_kernel_incompat(cuda_err) is True
        assert _is_cuda_kernel_incompat(RuntimeError("OOM in attention layer")) is False

    def test_cuda_kernel_incompat_logs_warning(self):
        try:
            from inference.timesfm_service import _log_forecast_failure
        except Exception:
            pytest.skip("timesfm_service deps missing")

        cuda_err = RuntimeError(
            "CUDA error: no kernel image is available for execution on the device. "
            "Search for `cudaErrorNoKernelImageForDevice'..."
        )
        records = _capture(_log_forecast_failure, cuda_err)
        levels = {lvl for lvl, _ in records}
        assert "ERROR" not in levels, (
            f"CUDA-kernel-incompat must log WARNING (environmental, "
            f"not a bug), got {levels}"
        )
        assert "WARNING" in levels

    def test_unknown_forecast_error_logs_error(self):
        try:
            from inference.timesfm_service import _log_forecast_failure
        except Exception:
            pytest.skip("timesfm_service deps missing")

        records = _capture(_log_forecast_failure, RuntimeError("genuine bug"))
        levels = {lvl for lvl, _ in records}
        # Genuine bugs must still surface at ERROR.
        assert "ERROR" in levels


class TestHTTPSeverityClassifier:
    """The shared HTTP-status severity classifier underpins both the FRED
    and credit-card puller fixes. Pin its behaviour so future status-code
    changes can't silently re-pollute errors.jsonl."""

    def test_5xx_classified_as_transient(self):
        from ingestion._http_severity import is_transient_http, is_warning_worthy

        for code in (500, 502, 503, 504):
            assert is_transient_http(code) is True
            assert is_warning_worthy(code) is True

    def test_4xx_classified_as_permanent(self):
        from ingestion._http_severity import is_permanent_http, is_warning_worthy

        for code in (400, 401, 403, 404, 429):
            assert is_permanent_http(code) is True
            assert is_warning_worthy(code) is True

    def test_2xx_3xx_not_warning_worthy(self):
        from ingestion._http_severity import is_warning_worthy

        # Successful / redirect codes never reach the error path; if they
        # somehow do, they shouldn't be classified as expected warnings.
        for code in (200, 201, 204, 301, 302, 304):
            assert is_warning_worthy(code) is False

    def test_none_not_warning_worthy(self):
        # When we can't extract a status, fall through to the generic
        # error path — don't silently swallow unknown failures.
        from ingestion._http_severity import is_warning_worthy

        assert is_warning_worthy(None) is False

    def test_5xx_outside_known_set_is_not_transient(self):
        # Defensive: 599 etc. aren't in our explicit transient set so they
        # surface as ERROR rather than being silently downgraded.
        from ingestion._http_severity import is_transient_http

        assert is_transient_http(599) is False
        assert is_transient_http(418) is False
