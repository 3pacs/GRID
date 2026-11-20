"""Synthetic actor evidence: no network, private source rows or live databases."""
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from intelligence import actor_signal_bridge as bridge

NOW = datetime(2026, 1, 20, 12, tzinfo=timezone.utc)


def row(row_id=1, **updates):
    value = {"id": row_id, "actor": "Example Actor", "signal_type": "congressional",
             "source_id": f"CONGRESS:synthetic:{row_id}:BUY", "direction": "BULL",
             "signal_date": date(2026, 1, 10), "created_at": NOW - timedelta(days=9),
             "actor_trust": .5, "actor_influence": .9, "data": {"synthetic": True}}
    value.update(updates)
    return value


@pytest.mark.parametrize("raw,expected", [
    ("BULL", "bullish"), (" BEAR ", "bearish"), ("Buy", "bullish"),
    ("SELL", "bearish"), ("long", "bullish"), ("short", "bearish"),
    ("bullish", "bullish"), ("bearish", "bearish"), ("call", "bullish"),
    ("put", "bearish"), ("up", "bullish"), ("down", "bearish"),
    (None, None), ("NEUTRAL", None), ("unknown", None), ("", None),
    ("not bullish", None), ("BUY AND SELL", None), (True, None),
])
def test_explicit_direction_only(raw, expected):
    assert bridge.normalize_actor_direction(raw) == expected


def test_five_aggregate_null_observations_never_vote():
    rows = [row(i, actor="qq_off_exchange", signal_type="quiverquant:offexchange",
                direction=None, signal_date=date(2026, 1, 10 + i)) for i in range(5)]
    evidence = bridge.select_actor_evidence(rows, as_of=NOW)
    assert evidence["signals"] == []
    assert len(evidence["observations"]) == 5
    assert all(r["status"] == "excluded" for r in evidence["observations"])


@pytest.mark.parametrize("actor,channel", [
    ("Example Actor", "quiverquant:offexchange"),
    ("Example Actor", "quiverquant:gov_contracts"),
    ("Example Actor", "quiverquant:wsb"),
    ("qq_house_trading:synthetic-key", "quiverquant:house"),
    ("cluster_example", "insider"), ("unknown", "insider"), ("", "insider"),
])
def test_non_actor_sources_rejected_even_with_known_direction(actor, channel):
    assert not bridge.select_actor_evidence([row(actor=actor, signal_type=channel)], as_of=NOW)["signals"]


def test_latest_date_one_vote_across_event_source_ids_and_whitespace():
    rows = [row(i, actor="Example  Actor" if i % 2 else " Example\tActor ",
                signal_date=date(2026, 1, 10 + i)) for i in range(5)]
    evidence = bridge.select_actor_evidence(rows, as_of=NOW)
    assert len(evidence["signals"]) == 1
    selected = evidence["signals"][0]
    assert selected["id"] == 4
    assert selected["actor"] == "Example Actor"
    assert [r["row_id"] for r in selected["provenance"]["supporting_rows"]] == [4]
    assert selected["provenance"]["observation_count"] == 5
    assert selected["provenance"]["superseded_count"] == 4
    assert [r["reason"] for r in evidence["observations"][:4]] == ["superseded_event_date"] * 4


@pytest.mark.parametrize("latest", [None, "NEUTRAL", "mystery"])
def test_unknown_latest_never_revives_older_vote(latest):
    evidence = bridge.select_actor_evidence([
        row(1), row(2, signal_date=date(2026, 1, 11), direction=latest)], as_of=NOW)
    assert evidence["signals"] == []
    assert evidence["observations"][1]["reason"] == "unknown_latest_direction"


@pytest.mark.parametrize("other", ["BEAR", None, "NEUTRAL"])
def test_same_date_conflict_is_not_resolved_by_arrival_order(other):
    rows = [row(1), row(2, direction=other, created_at=NOW, source_id="CONGRESS:synthetic:SELL")]
    for candidates in (rows, list(reversed(rows))):
        evidence = bridge.select_actor_evidence(candidates, as_of=NOW)
        assert evidence["signals"] == []
        assert all(r["reason"] in {"conflicting_latest_directions", "unknown_latest_direction"}
                   for r in evidence["observations"])


def test_same_date_duplicates_keep_lineage_without_extra_votes():
    evidence = bridge.select_actor_evidence([row(2), row(1), row(3, direction="buy")], as_of=NOW)
    assert len(evidence["signals"]) == 1
    p = evidence["signals"][0]["provenance"]
    assert [r["row_id"] for r in p["supporting_rows"]] == [1, 2, 3]
    assert {r["source_id"] for r in p["supporting_rows"]} == {row(i)["source_id"] for i in (1, 2, 3)}


def test_distinct_real_actors_and_channels_remain_distinct_dedup_units():
    evidence = bridge.select_actor_evidence([
        row(1), row(2, actor="Other Actor"), row(3, signal_type="insider")], as_of=NOW)
    assert len(evidence["signals"]) == 3  # Not a claim of statistical independence.


@pytest.mark.parametrize("changes,reason", [
    ({"signal_date": None}, "unknown_event_date"),
    ({"signal_date": "invalid"}, "unknown_event_date"),
    ({"signal_date": date(2025, 1, 1)}, "outside_lookback"),
    ({"signal_date": date(2026, 1, 21)}, "future_event_date"),
    ({"created_at": None}, "unknown_availability"),
    ({"created_at": datetime(2026, 1, 20)}, "unknown_availability"),
    ({"created_at": NOW + timedelta(seconds=1)}, "not_yet_available"),
])
def test_timing_exclusions(changes, reason):
    evidence = bridge.select_actor_evidence([row(**changes)], as_of=NOW)
    assert evidence["signals"] == []
    assert evidence["observations"][0]["reason"] == reason


def test_observation_date_and_ingestion_time_are_not_event_timestamp():
    p = bridge.select_actor_evidence([row()], as_of=NOW)["signals"][0]["provenance"]
    assert p["signal_date"] == "2026-01-10"
    assert p["event_time_basis"] == "signal_date_date_only"
    assert p["available_at"] == "2026-01-11T12:00:00+00:00"
    assert p["availability_basis"] == "signal_data.created_at_ingestion"
    assert p["freshness_hours"] is None
    assert p["date_age_days"] == 10
    assert p["freshness_hours_lower_bound"] == 228
    same_day = bridge.select_actor_evidence([row(signal_date=NOW.date(), created_at=NOW)], as_of=NOW)
    p = same_day["signals"][0]["provenance"]
    assert p["freshness_hours"] is None
    assert p["freshness_hours_lower_bound"] == 0  # Lower bound, not exact age.


def test_naive_decision_clock_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        bridge.select_actor_evidence([row()], as_of=NOW.replace(tzinfo=None))


def test_limit_sentinel_withholds_all_votes_and_keeps_bounded_lineage(monkeypatch):
    monkeypatch.setattr(bridge, "MAX_ACTOR_OBSERVATIONS", 2)
    evidence = bridge.select_actor_evidence([row(i) for i in range(3)], as_of=NOW)
    assert evidence["signals"] == []
    assert evidence["complete"] is False
    assert evidence["reason"] == "candidate_limit_exceeded"
    assert evidence["candidate_count_lower_bound"] == 3
    assert len(evidence["observations"]) == 3
    assert all(r["reason"] == "candidate_limit_exceeded" for r in evidence["observations"])


def test_reader_binds_both_clocks_and_preserves_zero_actor_scores():
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    result = MagicMock()
    result.mappings.return_value.all.return_value = [row()]
    actor_result = MagicMock()
    actor_result.fetchall.return_value = [("example_actor", 0.0, .9, "synthetic", "test")]
    conn.execute.side_effect = [result, actor_result]
    evidence = bridge.get_actor_evidence_for_ticker(engine, "TEST", as_of=NOW)
    sql, params = conn.execute.call_args_list[0].args
    assert "signal_date BETWEEN :cutoff AND :as_of_date" in str(sql)
    assert "created_at <= :as_of" in str(sql)
    assert params["as_of"] == NOW
    assert params["as_of_date"] == NOW.date()
    assert params["candidate_limit"] == bridge.MAX_ACTOR_OBSERVATIONS + 1
    assert evidence["signals"][0]["actor_trust"] == 0.0
    assert evidence["actor_attributes_basis"] == "current_actor_table_not_historical"


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, 1.01, None, True])
def test_invalid_actor_score_is_not_directional_evidence(value):
    evidence = bridge.select_actor_evidence([row(actor_trust=value)], as_of=NOW)
    assert evidence["signals"] == []
    assert evidence["observations"][0]["reason"] == "invalid_actor_scores"
    assert evidence["observations"][0]["reason"] == "invalid_actor_scores"


def test_source_channel_is_canonical_in_selected_output():
    evidence = bridge.select_actor_evidence([row(signal_type=" CONGRESSIONAL ")], as_of=NOW)
    assert evidence["signals"][0]["signal_type"] == "congressional"


def test_reader_query_applies_both_bounds_before_selection():
    # Execute the source SELECT against a real in-memory SQL table. The actor
    # lookup is a fake (its PostgreSQL ANY syntax is not SQLite-compatible).
    from contextlib import contextmanager
    from sqlalchemy import create_engine, text
    db = create_engine("sqlite://")
    with db.begin() as conn:
        conn.execute(text("""CREATE TABLE signal_data (
            id INTEGER, signal_type TEXT, actor TEXT, direction TEXT, magnitude REAL,
            confidence TEXT, signal_date DATE, data TEXT, source_id TEXT, created_at TIMESTAMP,
            ticker TEXT)"""))
        for candidate in [row(1), row(2, signal_date=date(2026, 1, 21)),
                          row(3, created_at=NOW + timedelta(seconds=1)),
                          row(4, signal_date=date(2025, 1, 1)), row(5, created_at=None)]:
            conn.execute(text("""INSERT INTO signal_data
                (id, signal_type, actor, direction, signal_date, source_id, created_at, ticker)
                VALUES (:id, :signal_type, :actor, :direction, :signal_date, :source_id, :created_at, 'TEST')"""),
                {key: candidate[key] for key in ("id", "signal_type", "actor", "direction",
                                                 "signal_date", "source_id", "created_at")})
    class Connection:
        def __init__(self, conn):
            self.conn = conn
        def execute(self, statement, params):
            if "FROM actors" in str(statement):
                result = MagicMock()
                result.fetchall.return_value = [("example_actor", .5, .9, "test", "test")]
                return result
            return self.conn.execute(statement, params)
    class Engine:
        @contextmanager
        def connect(self):
            with db.connect() as conn:
                yield Connection(conn)
    evidence = bridge.get_actor_evidence_for_ticker(Engine(), "TEST", as_of=NOW)
    assert [r["row_id"] for r in evidence["observations"]] == [5, 1]
    assert evidence["observations"][0]["reason"] == "unknown_availability"
    assert [s["id"] for s in evidence["signals"]] == [1]
    db.dispose()


def test_too_many_used_support_refs_withholds_group():
    evidence = bridge.select_actor_evidence([row(i) for i in range(21)], as_of=NOW)
    assert evidence["signals"] == []
    assert evidence["observation_count"] == 21
    assert evidence["observations_complete"] is False
    assert len(evidence["observations"]) == bridge.MAX_ACTOR_AUDIT_SAMPLES
    assert evidence["disposition_counts"] == {"supporting_lineage_limit_exceeded": 21}


def test_sample_clipping_is_explicit_and_never_clips_used_source_refs():
    evidence = bridge.select_actor_evidence([row(source_id="x" * 200)], as_of=NOW)
    sample = evidence["observations"][0]
    assert len(sample["source_id"]) == 160
    assert sample["truncated_fields"] == ["source_id"]
    assert evidence["observations_complete"] is False
    assert evidence["signals"][0]["provenance"]["supporting_rows"][0]["source_id"] == "x" * 200
