"""Synthetic mechanics tests; these do not establish predictive edge."""

from copy import deepcopy

from scripts.intraday_lab.time_of_day import normalize_activity


def fixture():
    sessions, observations = [], []
    for i in range(4):
        opening = 100000 + i * 86400
        session = f"s{i}"
        sessions.append(
            {"session": session, "open_at": opening, "close_at": opening + 23400}
        )
        observations.append(
            {
                "session": session,
                "source_id": "trusted_volume",
                "instrument": "SPY",
                "metric": "volume",
                "value": 100 if i < 3 else 200,
                "event_at": opening + 300,
                "available_at": opening + 301,
                "bucket_start_at": opening,
                "bucket_end_at": opening + 300,
                "clock": "trusted_receipt",
                "status": "available",
            }
        )
    return {
        "session": "s3",
        "decision_at": sessions[-1]["open_at"] + 310,
        "sessions": sessions,
        "observations": observations,
    }


def run(packet):
    return normalize_activity(packet, "volume", min_sessions=3)


def test_normalizes_and_preserves_lineage():
    result = run(fixture())
    assert result["value"] == 2
    assert result["baseline_sessions"] == ["s0", "s1", "s2"]
    assert (
        result["latest_available_at"] == fixture()["observations"][-1]["available_at"]
    )


def test_current_session_never_enters_baseline():
    packet = fixture()
    packet["observations"][-1]["value"] = 900000
    assert run(packet)["baseline_median"] == 100


def test_future_receipt_cannot_fill_history():
    packet = fixture()
    packet["observations"][0]["available_at"] = packet["decision_at"] + 1
    assert run(packet)["reason"] == "insufficient prior-session history"


def test_duplicate_session_cannot_inflate_sample_size():
    packet = fixture()
    packet["observations"].append(deepcopy(packet["observations"][0]))
    assert run(packet)["reason"] == "duplicate prior-session bucket"


def test_different_bucket_does_not_fill_history():
    packet = fixture()
    packet["observations"][0]["bucket_start_at"] += 300
    assert run(packet)["status"] == "unavailable"


def test_source_mixing_and_callback_clock_rejected():
    for field, value in [("source_id", "other"), ("clock", "anik_callback")]:
        packet = fixture()
        packet["observations"][0][field] = value
        assert run(packet)["status"] == "unavailable"


def test_zero_baseline_and_invalid_values_unavailable():
    packet = fixture()
    for row in packet["observations"][:-1]:
        row["value"] = 0
    assert run(packet)["reason"] == "nonpositive baseline"
    packet = fixture()
    packet["observations"][-1]["value"] = float("nan")
    assert run(packet)["status"] == "unavailable"


def test_incomplete_current_bucket_not_used():
    packet = fixture()
    packet["decision_at"] -= 20
    assert run(packet)["status"] == "unavailable"


def test_calendar_offsets_handle_dst_without_utc_hour_matching():
    packet = fixture()
    for calendar, row in zip(packet["sessions"][:-1], packet["observations"][:-1]):
        calendar["open_at"] += 3600
        calendar["close_at"] += 3600
        for key in ("event_at", "available_at", "bucket_start_at", "bucket_end_at"):
            row[key] += 3600
    assert run(packet)["value"] == 2


def test_short_session_without_matching_bucket_excluded():
    packet = fixture()
    packet["sessions"][0]["close_at"] = packet["sessions"][0]["open_at"] + 200
    assert run(packet)["reason"] == "insufficient prior-session history"


def test_missing_calendar_and_expired_bucket_fail_closed():
    packet = fixture()
    packet["sessions"] = packet["sessions"][:-1]
    assert run(packet)["status"] == "unavailable"
    packet = fixture()
    packet["decision_at"] += 300
    assert run(packet)["status"] == "unavailable"


def test_history_window_is_deterministic_and_bounded():
    result = normalize_activity(fixture(), "volume", min_sessions=2, max_sessions=2)
    assert result["baseline_sessions"] == ["s1", "s2"]


def test_invalid_configuration_returns_unavailable():
    for options in [
        {"bucket_seconds": True},
        {"min_sessions": True},
        {"max_sessions": 0},
    ]:
        assert (
            normalize_activity(fixture(), "volume", **options)["status"]
            == "unavailable"
        )


def test_nonfinite_baseline_cannot_become_zero_activity():
    packet = fixture()
    for row in packet["observations"][:-1]:
        row["value"] = 1e308
    result = normalize_activity(packet, "volume", min_sessions=2, max_sessions=2)
    assert result["status"] == "unavailable"


def test_malformed_packets_fail_closed():
    for packet in (
        None,
        [],
        {},
        {"sessions": [None], "observations": []},
        {"sessions": [], "observations": ["bad"]},
    ):
        assert normalize_activity(packet, "volume")["status"] == "unavailable"
