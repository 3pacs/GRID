"""
Regression tests for the 2026-05-08 errors.jsonl triage pass.

Each test pins a behaviour change so a future refactor doesn't silently
re-open one of these log floods. Every fix targets a pattern observed
in `.server-logs/errors.jsonl` on 2026-05-07/08 (155 ERROR rows in 24h):

  - FRED 5xx after retries (ERROR -> WARNING)
  - Freightos historical: JSON parse must not propagate through the
    retry decorator (was burning 3 attempts + ERROR per cycle)
  - IMF IFS: network/DNS failures (ERROR -> WARNING)
  - IMF WEO: missing class in upstream library (ERROR -> WARNING + skip)
  - OECD CLI/MEI: tenacity RetryError[HTTPError] (ERROR -> WARNING)
  - Tiingo: read/connect timeout (ERROR -> WARNING)
  - FinBERT: model load failure logs once, not per-source
  - TimesFM: CUDA kernel-image mismatch is environmental (ERROR -> WARNING)

Run:
    python -m pytest tests/test_log_flood_fixes_2026_05_08.py -v
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# FRED — transient 5xx after retries should WARN, not ERROR
# ---------------------------------------------------------------------------


class TestFREDTransient5xx:
    def test_handler_includes_5xx_branch(self):
        from pathlib import Path
        src = (
            Path(__file__).resolve().parent.parent / "ingestion" / "fred.py"
        ).read_text()
        # The fix adds an explicit 5xx branch that demotes to log.warning
        # and returns SKIPPED. Without that branch, fedfred 500/502/503/504
        # after retries falls through to log.opt(exception=True).error.
        assert "status_code in (500, 502, 503, 504)" in src
        # The 5xx branch must precede the generic .error logger.
        idx_5xx = src.index("status_code in (500, 502, 503, 504)")
        idx_err = src.index("log.opt(exception=True).error")
        assert idx_5xx < idx_err, "5xx branch must short-circuit before .error"


# ---------------------------------------------------------------------------
# Freightos historical — JSON parse must not propagate through retries
# ---------------------------------------------------------------------------


class TestFreightosHistoricalJSON:
    def test_json_parse_outside_request_handler(self):
        """In requests >=2.27, JSONDecodeError is a subclass of
        RequestException. If `resp.json()` lives inside the same
        try/except RequestException, an HTML/captcha body re-raises into
        the @retry_on_failure decorator and burns 3 attempts before
        logging an ERROR. The fix moves resp.json() outside that handler.
        """
        from pathlib import Path
        src = (
            Path(__file__).resolve().parent.parent
            / "ingestion"
            / "altdata"
            / "supply_chain.py"
        ).read_text()
        # Find the historical fetcher.
        marker = "_fetch_freightos_historical"
        start = src.index(f"def {marker}")
        # Bound the slice at the next def at column 0 inside the class
        # (i.e. "    def _pull_freightos") which always follows.
        end = src.index("    def _pull_freightos", start)
        body = src[start:end]
        # raise_for_status (request layer) must come before json()
        # decoded outside that try-block.
        rfs_idx = body.index("raise_for_status()")
        # The post-fix layout has a fresh `try:` introducing the json()
        # call AFTER the request RequestException handler.
        post = body[rfs_idx:]
        assert "except requests.RequestException" in post
        # JSONDecodeError handler must exist and live AFTER the
        # RequestException handler (i.e. on a separate try block).
        re_idx = post.index("except requests.RequestException")
        assert "json.JSONDecodeError" in post[re_idx:], (
            "JSONDecodeError handler missing after RequestException — "
            "JSON parse will get caught as RequestException"
        )


# ---------------------------------------------------------------------------
# IMF — network errors as WARNING, WEO ImportError as graceful skip
# ---------------------------------------------------------------------------


class TestIMFErrorHygiene:
    def test_uses_log_pull_failure(self):
        from pathlib import Path
        src = (
            Path(__file__).resolve().parent.parent
            / "ingestion"
            / "international"
            / "imf.py"
        ).read_text()
        assert "from ingestion.base import BasePuller, log_pull_failure" in src
        assert 'log_pull_failure("IMF IFS"' in src
        assert 'log_pull_failure("IMF WEO"' in src

    def test_weo_import_error_logs_warning_and_skips(self):
        from pathlib import Path
        src = (
            Path(__file__).resolve().parent.parent
            / "ingestion"
            / "international"
            / "imf.py"
        ).read_text()
        # The pull_weo body must catch ImportError around `from imfdatapy.imf
        # import WEO` and return SKIPPED. Otherwise upstream library API
        # drift ("cannot import name 'WEO'") spams ERROR every cycle.
        weo_start = src.index("def pull_weo")
        weo_end = src.index("\n    def ", weo_start + 1)
        body = src[weo_start:weo_end]
        assert "from imfdatapy.imf import WEO" in body
        assert "except ImportError" in body
        assert 'result["status"] = "SKIPPED"' in body


# ---------------------------------------------------------------------------
# OECD — RetryError[HTTPError] is upstream wedge, not GRID bug
# ---------------------------------------------------------------------------


class TestOECDErrorHygiene:
    def test_uses_log_pull_failure(self):
        from pathlib import Path
        src = (
            Path(__file__).resolve().parent.parent
            / "ingestion"
            / "international"
            / "oecd.py"
        ).read_text()
        assert "from ingestion.base import BasePuller, log_pull_failure" in src
        assert 'log_pull_failure("OECD CLI"' in src
        assert 'log_pull_failure("OECD MEI"' in src
        # The bare log.error("OECD ... pull failed ...") sites must be gone.
        assert 'log.error("OECD CLI pull failed' not in src
        assert 'log.error("OECD MEI pull failed' not in src


# ---------------------------------------------------------------------------
# Tiingo — read/connect timeouts are transient, not bugs
# ---------------------------------------------------------------------------


class TestTiingoTimeoutHygiene:
    def test_timeout_branch_logs_warning(self):
        from pathlib import Path
        src = (
            Path(__file__).resolve().parent.parent
            / "ingestion"
            / "tiingo_pull.py"
        ).read_text()
        # The new branch catches ConnectionError / Timeout / ReadTimeout
        # / ConnectTimeout and demotes to WARNING.
        assert "requests.exceptions.ReadTimeout" in src
        assert "requests.exceptions.ConnectTimeout" in src
        # And it must precede the generic Exception handler so timeouts
        # don't fall through to log.error.
        timeout_idx = src.index("requests.exceptions.ReadTimeout")
        # The IndexCorrupted breaker still lives inside the generic
        # Exception handler that follows.
        bare_except_idx = src.index("except Exception as exc:", timeout_idx)
        assert timeout_idx < bare_except_idx


# ---------------------------------------------------------------------------
# FinBERT — log model load failure ONCE, not per-source
# ---------------------------------------------------------------------------


class TestFinBERTModelLoadOnce:
    def test_score_all_sources_loads_model_first(self):
        from pathlib import Path
        src = (
            Path(__file__).resolve().parent.parent
            / "ingestion"
            / "ml"
            / "finbert_scorer.py"
        ).read_text()
        # The fix calls _ensure_model() at the top of score_all_sources
        # under a try/except that returns SKIPPED for every source on
        # failure, so the BertForSequenceClassification import error
        # logs ONCE per cycle instead of 5x (one per source).
        sas_idx = src.index("def score_all_sources")
        body = src[sas_idx:]
        assert "self._ensure_model()" in body[: body.index("for source_name")]
        assert "skipping all sources this cycle" in body

    def test_skipped_results_have_status_skipped(self):
        """Mock the model loader so we exercise the failure path without
        importing torch/transformers."""
        torch = pytest.importorskip("torch")  # noqa: F841 — finbert imports torch
        from ingestion.ml.finbert_scorer import FinBERTScorer

        engine = MagicMock()
        scorer = FinBERTScorer.__new__(FinBERTScorer)
        scorer.engine = engine
        scorer.batch_size = 64
        scorer.tokenizer = None
        scorer.model = None
        scorer.source_id = None

        with patch.object(
            FinBERTScorer,
            "_ensure_model",
            side_effect=RuntimeError(
                "Could not import module 'BertForSequenceClassification'"
            ),
        ):
            results = scorer.score_all_sources()

        assert len(results) >= 1
        assert all(r["status"] == "SKIPPED" for r in results)
        assert all("model unavailable" in r["error"] for r in results)


# ---------------------------------------------------------------------------
# TimesFM — CUDA kernel-image mismatch is environmental, not a code bug
# ---------------------------------------------------------------------------


class TestTimesFMCUDAEnvDemotion:
    def test_cuda_branch_logs_warning(self):
        from pathlib import Path
        src = (
            Path(__file__).resolve().parent.parent
            / "inference"
            / "timesfm_service.py"
        ).read_text()
        # The fix adds a CUDA-env detection branch around the forecast
        # call's exception handler that downgrades the recurring kernel-
        # image-mismatch ERRORs to WARNING.
        assert "no kernel image is available" in src
        assert "cudaErrorNoKernelImageForDevice" in src
        # Generic forecast failures still log ERROR (real bugs), so
        # both branches must coexist.
        assert 'log.warning("TimesFM forecast skipped (CUDA env issue)' in src
        assert 'log.error("TimesFM forecast failed' in src
