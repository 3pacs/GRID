from __future__ import annotations

from datetime import datetime, timezone

from intelligence import source_quality_ablation as sqa


def _source(
    name: str,
    cost_tier: str,
    *,
    rows_total: int = 100,
    success_rows: int = 95,
    failed_rows: int = 5,
) -> sqa.SourceOperationalStats:
    return sqa.SourceOperationalStats(
        source_id=1,
        name=name,
        cost_tier=cost_tier,
        active=True,
        trust_score="HIGH",
        priority_rank=1,
        last_pull_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
        latest_success_at=datetime(2026, 5, 20, tzinfo=timezone.utc),
        rows_total=rows_total,
        success_rows=success_rows,
        failed_rows=failed_rows,
        partial_rows=0,
        last_error=None,
    )


def test_source_quality_report_flags_paid_source_when_free_peer_beats_it() -> None:
    report = sqa.build_source_quality_report(
        [_source("Tiingo", "PAID"), _source("yfinance", "FREE")],
        prediction_metrics={
            "Tiingo": sqa.SourcePredictionStats(
                source_name="Tiingo",
                prediction_count=40,
                hits=17,
                partials=0,
                misses=23,
                brier_sum=14.4,
            ),
            "yfinance": sqa.SourcePredictionStats(
                source_name="yfinance",
                prediction_count=44,
                hits=31,
                partials=0,
                misses=13,
                brier_sum=7.4,
            ),
        },
        redundancy_metrics={},
        fallback_lookup=lambda name: [{"provider": "stooq_daily_csv", "source": name}],
        min_prediction_sample=20,
    )

    tiingo = next(row for row in report.sources if row.source_name == "Tiingo")
    yfinance = next(row for row in report.sources if row.source_name == "yfinance")

    assert report.paid_vs_free_summary["status"] == "ok"
    assert tiingo.recommendation == "replace_candidate"
    assert "free_peer_outperformed" in tiingo.reasons
    assert tiingo.free_fallback_candidates[0]["provider"] == "stooq_daily_csv"
    assert yfinance.recommendation == "keep"


def test_source_quality_report_does_not_overclaim_sparse_prediction_attribution() -> None:
    report = sqa.build_source_quality_report(
        [_source("Polygon", "PAID", failed_rows=0)],
        prediction_metrics={
            "Polygon": sqa.SourcePredictionStats(
                source_name="Polygon",
                prediction_count=3,
                hits=0,
                partials=0,
                misses=3,
                brier_sum=2.0,
            )
        },
        redundancy_metrics={},
        fallback_lookup=lambda _name: [],
        min_prediction_sample=20,
    )

    polygon = report.sources[0]

    assert polygon.recommendation == "insufficient_evidence"
    assert "prediction_sample_below_threshold" in polygon.reasons
    assert report.paid_vs_free_summary["status"] == "not_enough_attributed_predictions"


def test_paid_source_with_no_prediction_lineage_is_instrument_or_disable() -> None:
    report = sqa.build_source_quality_report(
        [_source("Tiingo", "PAID"), _source("Reddit", "FREE")],
        prediction_metrics={
            "Reddit": sqa.SourcePredictionStats(
                source_name="Reddit",
                prediction_count=40,
                hits=18,
                partials=0,
                misses=22,
                brier_sum=12.0,
            )
        },
        redundancy_metrics={},
        fallback_lookup=lambda name: [{"provider": "public_peer", "source": name}],
        min_prediction_sample=20,
        total_prediction_rows=100,
    )

    tiingo = next(row for row in report.sources if row.source_name == "Tiingo")

    assert tiingo.recommendation == "instrument_or_disable"
    assert "paid_source_not_attributed_to_predictions" in tiingo.reasons
    assert tiingo.free_fallback_candidates[0]["provider"] == "public_peer"
    assert report.paid_vs_free_summary["status"] == "paid_source_lineage_gap"


def test_extract_prediction_source_names_matches_catalog_names_from_signals() -> None:
    names = sqa.extract_prediction_source_names(
        signals={
            "items": [
                {"name": "tiingo_news:sentiment"},
                {"name": "alpha_research:credit_cycle"},
            ],
            "signal_contributions": {"FRED:yield_curve": 0.8},
        },
        model_weights={"quiverquant_congress": 1.1},
        catalog_source_names=["Tiingo_News", "FRED", "quiverquant", "Polygon"],
    )

    assert names == {"Tiingo_News", "FRED", "quiverquant"}


def test_extract_prediction_source_names_does_not_match_generic_prefixes() -> None:
    names = sqa.extract_prediction_source_names(
        signals={"items": [{"name": "alpha_research:credit_cycle"}]},
        model_weights={},
        catalog_source_names=["ALPHA_VANTAGE", "AlphaQuery"],
    )

    assert names == set()


def test_report_redacts_secret_like_failure_payloads() -> None:
    report = sqa.build_source_quality_report(
        [
            _source(
                "Polygon",
                "PAID",
                success_rows=0,
                failed_rows=5,
                rows_total=5,
            )
        ],
        prediction_metrics={},
        redundancy_metrics={},
        fallback_lookup=lambda _name: [],
        min_prediction_sample=20,
    )

    report.sources[0].last_error = sqa.scrub_secret_text(
        "request failed https://api.example.test?apikey=SECRET123&token=ABC456"
    )

    assert "SECRET123" not in report.sources[0].last_error
    assert "ABC456" not in report.sources[0].last_error
    assert "apikey=[REDACTED]" in report.sources[0].last_error


def test_operational_stats_query_uses_per_source_lateral_scan() -> None:
    class _Result:
        def mappings(self):
            return self

        def all(self):
            return []

    class _Conn:
        def __init__(self) -> None:
            self.sql: list[str] = []

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def execute(self, stmt, _params=None):
            self.sql.append(str(stmt))
            return _Result()

    class _Engine:
        def __init__(self) -> None:
            self.conn = _Conn()

        def connect(self):
            return self.conn

    engine = _Engine()

    sqa.load_source_operational_stats(engine, days=30, limit=10)

    joined = "\n".join(engine.conn.sql)
    assert "LEFT JOIN LATERAL" in joined
    assert "rs.source_id = selected.id" in joined
