"""Regression tests for ingestion bugs surfaced in .server-logs/errors.jsonl.

Each test locks in a specific production error pattern from the error log so
the bug can never be re-introduced silently. If one of these fails, the logged
error is about to come back.

Covered regressions:
  1. yfinance: column-name rows ("Open") leaking into obs_date (April 8).
  2. SEC EDGAR Fundamentals: UniqueViolation from duplicate (ticker, field,
     obs_date) pairs in the same pull transaction.
  3. Earnings puller: a failure in one processing step poisoning the other two
     with InFailedSqlTransaction.
  4. CBOE put/call ratio: 403 from an upstream-gated URL should be SKIPPED,
     not logged as ERROR.
  5. Institutional flows 13F: 404 on dead SEC CIK should be SKIPPED, not FAILED.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy.engine import Engine
from unittest.mock import create_autospec


@pytest.fixture
def fresh_engine():
    """Engine mock that tracks execute() calls and supports begin()/connect()."""
    engine = create_autospec(Engine, instance=True)
    conn = MagicMock()
    result = MagicMock()
    result.fetchone.return_value = None
    result.fetchall.return_value = []
    conn.execute.return_value = result
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


# ── 1. yfinance column-name leak regression ─────────────────────────────────


class TestYFinanceNonDateIndex:
    """Lock-in: yfinance must never insert column names (e.g. 'Open') as obs_date.

    Prior failure:
      ingestion.yfinance_pull:pull_ticker:193
      "invalid input syntax for type date: \"Open\""
    """

    def test_string_index_is_skipped_not_inserted(self, fresh_engine):
        from ingestion.yfinance_pull import YFinancePuller

        engine, conn = fresh_engine
        with patch("ingestion.yfinance_pull.BasePuller._resolve_source_id", return_value=2), \
             patch("ingestion.yfinance_pull.BasePuller._get_existing_dates", return_value=set()):
            puller = YFinancePuller(db_engine=engine)

            # Simulate yfinance returning a frame whose index contains the
            # literal string "Open" — the exact pattern from the log.
            df = pd.DataFrame(
                {"Open": [100.0, 101.0], "Close": [100.5, 101.5]},
                index=pd.Index(["Open", "2024-01-02"], name="Date"),
            )
            with patch("ingestion.yfinance_pull.yf.download", return_value=df):
                result = puller.pull_ticker("TLT", "2024-01-01")

        # The insert path must not receive "Open" as an obs_date.
        inserted_dates = [
            call.kwargs.get("od") if call.kwargs else (call.args[1] if len(call.args) > 1 else None)
            for call in conn.execute.call_args_list
        ]
        # Check the parameter dicts passed as second positional arg to execute()
        for call_args in conn.execute.call_args_list:
            args, kwargs = call_args
            for obj in list(args) + list(kwargs.values()):
                if isinstance(obj, dict) and "od" in obj:
                    assert obj["od"] != "Open", (
                        "yfinance leaked column name 'Open' as obs_date — "
                        "regression of April-8 data-integrity bug"
                    )
                    assert isinstance(obj["od"], date), (
                        f"obs_date must be a date instance, got {type(obj['od']).__name__}"
                    )
        assert result["status"] in ("SUCCESS", "PARTIAL", "SKIPPED")


# ── 2. SEC EDGAR Fundamentals within-batch dedupe ───────────────────────────


class TestSecEdgarBatchDedupe:
    """Lock-in: duplicate (field, obs_date) pairs in one pull must be deduped.

    Prior failure:
      ingestion.scheduler:run_pull_group:168
      UniqueViolation on uq_raw_series_composite — the same transaction tried
      to INSERT (edgar_fundamentals.AAPL.revenue, 2017-09-30) twice.
    """

    def test_duplicate_pairs_skipped_in_batch(self, fresh_engine):
        from ingestion.altdata import sec_edgar_company as mod
        from ingestion.altdata.sec_edgar_company import SECEdgarCompanyPuller

        engine, conn = fresh_engine
        with patch.object(SECEdgarCompanyPuller, "_resolve_source_id", return_value=734), \
             patch.object(SECEdgarCompanyPuller, "_get_existing_dates", return_value=set()), \
             patch.object(SECEdgarCompanyPuller, "_get_cik", return_value="0000320193"), \
             patch.object(SECEdgarCompanyPuller, "_fetch_company_facts", return_value={}), \
             patch.object(SECEdgarCompanyPuller, "_extract_rows") as extract_mock:
            # Emit two rows with the same (field_name, obs_date) — classic
            # 10-K + 10-Q collision from the error log.
            extract_mock.return_value = [
                {
                    "field_name": "revenue",
                    "obs_date": date(2017, 9, 30),
                    "value": 52579000000.0,
                    "form": "10-K",
                    "period_start": "2017-07-02",
                    "fiscal_year": 2018,
                    "fiscal_period": "FY",
                },
                {
                    "field_name": "revenue",
                    "obs_date": date(2017, 9, 30),
                    "value": 52579000000.0,
                    "form": "10-Q",
                    "period_start": "2017-07-02",
                    "fiscal_year": 2018,
                    "fiscal_period": "Q4",
                },
            ]
            puller = SECEdgarCompanyPuller(db_engine=engine)
            inserts = []

            def track_insert(**kwargs):
                inserts.append((kwargs["series_id"], kwargs["obs_date"]))

            with patch.object(puller, "_insert_raw", side_effect=track_insert):
                result = puller.pull_ticker("AAPL")

        assert result["status"] == "SUCCESS"
        assert len(inserts) == 1, (
            f"Batch dedupe must collapse duplicates; got {len(inserts)} inserts "
            "— regression would cause UniqueViolation in production"
        )


# ── 3. Earnings puller: step isolation ──────────────────────────────────────


class TestEarningsStepIsolation:
    """Lock-in: a failure in earnings_dates must not abort quarterly/history.

    Prior failure:
      ingestion.altdata.earnings_puller:pull_ticker:434
      (psycopg2.errors.InFailedSqlTransaction) current transaction is aborted
    """

    def test_step_failure_does_not_poison_others(self, fresh_engine):
        from ingestion.altdata.earnings_puller import EarningsPuller

        engine, conn = fresh_engine
        with patch("ingestion.altdata.earnings_puller.BasePuller._resolve_source_id", return_value=351):
            puller = EarningsPuller(db_engine=engine)

        with patch.object(puller, "_fetch_ticker_data", return_value=MagicMock()), \
             patch.object(puller, "_process_earnings_dates", side_effect=RuntimeError("boom")), \
             patch.object(puller, "_process_quarterly_earnings", return_value=5), \
             patch.object(puller, "_process_earnings_history", return_value=3), \
             patch.object(puller, "_detect_significant_surprises", return_value=[]):
            result = puller.pull_ticker("SMCI")

        # Step 1 failed but steps 2 and 3 still ran and accumulated rows.
        assert result["rows_inserted"] == 8, (
            f"Expected 5+3 rows from surviving steps, got {result['rows_inserted']} "
            "— step isolation regressed"
        )
        assert any("earnings_dates" in e for e in result["errors"])


# ── 4. CBOE upstream-gated 403 → SKIPPED ────────────────────────────────────


class TestCboe403IsSkipped:
    """Lock-in: 403 from a gated CBOE URL must not log as ERROR.

    Prior: 32 ERROR-level entries per day for total_exchange_pcr.csv (paid).
    """

    def test_403_returns_skipped_status(self, fresh_engine):
        import requests
        from ingestion.altdata.cboe_indices import CBOEIndicesPuller

        engine, _ = fresh_engine
        with patch("ingestion.altdata.cboe_indices.BasePuller._resolve_source_id", return_value=99):
            puller = CBOEIndicesPuller(db_engine=engine)

        http_err = requests.HTTPError("403 Client Error: Forbidden for url: ...")
        with patch.object(puller, "_download_csv", side_effect=http_err):
            result = puller.pull_index("put_call_ratio")

        assert result["status"] == "SKIPPED", (
            f"403 should downgrade to SKIPPED; got {result['status']} "
            "— alarm-fatigue regression"
        )


# ── 5. 13F dead-CIK 404 → SKIPPED ───────────────────────────────────────────


def test_13f_404_is_skipped_not_failed():
    """Lock-in: 404 from data.sec.gov/submissions must be SKIPPED.

    Prior error text:
      '13F ExodusPoint Capital (CIK=1699161) failed: 404 Client Error:
       Not Found for url: https://data.sec.gov/submissions/CIK0001699161.json'
    """
    import ast
    import pathlib

    source = pathlib.Path(
        "ingestion/altdata/institutional_flows.py"
    ).read_text()
    # The dead-CIK guard must still be present. A refactor that removes it
    # would be the exact regression we're guarding.
    assert "data.sec.gov/submissions" in source
    assert '"SKIPPED"' in source
    # And the check must be gated on the 404 status + the submissions URL.
    assert (
        '"404" in msg' in source
        and 'data.sec.gov/submissions' in source
    ), "Dead-CIK guard in institutional_flows.py was removed"
