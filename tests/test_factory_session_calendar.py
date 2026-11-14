from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
from analysis.factory_session_calendar import FactoryCalendar, PINS
from analysis import module_factory as m


def bars(n=390):
    return m.Session({m._hm(570 + i): (100, 101, 99, 100, 1000) for i in range(n)})


def spec():
    return m.RunFile.from_dict(
        dict(
            run_id="calendar-synthetic",
            first_session="2022-11-14",
            last_session="2026-11-10",
            decision_times=["14:00"],
            horizons=["60m"],
            features=["F1"],
            z_thresholds=[1.0],
            target_unit="dollars",
            perms=99,
        )
    )


def test_boundary_and_special_holidays():
    c = FactoryCalendar()
    assert len(c.sessions) == 1010
    assert c.previous_day("2025-01-02") == "2024-12-31"
    assert c.previous_day("2025-01-10") == "2025-01-08"
    assert c.for_day("2025-01-09") is None  # Carter national mourning closure
    assert c.for_day("2023-06-19") is None
    assert c.for_day("2026-07-03") is None
    with pytest.raises(ValueError):
        c.for_day("2022-10-31")
    with pytest.raises(ValueError):
        c.for_day("2026-11-11")


@pytest.mark.parametrize(
    "day",
    [
        "2022-11-25",
        "2023-07-03",
        "2023-11-24",
        "2024-07-03",
        "2024-11-29",
        "2024-12-24",
        "2025-07-03",
        "2025-11-28",
        "2025-12-24",
    ],
)
def test_early_closes(day):
    s = FactoryCalendar().for_day(day)
    assert (
        s.closes.astimezone(ZoneInfo("America/New_York")).strftime("%H:%M") == "13:00"
    )
    assert (s.closes - s.opens).total_seconds() == 210 * 60


def test_dst_changes_utc_not_local_open():
    c = FactoryCalendar()
    assert c.for_day("2023-03-10").opens.hour == 14
    assert c.for_day("2023-03-13").opens.hour == 13
    assert c.for_day("2023-11-03").opens.hour == 13
    assert c.for_day("2023-11-06").opens.hour == 14


def test_missing_whole_session_does_not_substitute():
    families, cov = m.build_rows(
        {"2023-01-03": bars(), "2023-01-05": bars()}, spec(), FactoryCalendar()
    )
    assert families["SPY|change|14:00+60m"] == []
    assert cov["calendar_qualification"].startswith("pinned XNYS")


def test_holiday_gap_is_not_a_missing_session():
    families, _ = m.build_rows(
        {"2023-01-13": bars(), "2023-01-17": bars()}, spec(), FactoryCalendar()
    )
    assert len(families["SPY|change|14:00+60m"]) == 1


def test_shortened_session_and_next_day_stay_withheld():
    families, _ = m.build_rows(
        {"2023-11-22": bars(), "2023-11-24": bars(210), "2023-11-27": bars()},
        spec(),
        FactoryCalendar(),
    )
    assert families["SPY|change|14:00+60m"] == []


def test_fake_full_bars_on_short_day_cannot_create_afterclose_label():
    families, _ = m.build_rows(
        {"2023-11-22": bars(), "2023-11-24": bars()}, spec(), FactoryCalendar()
    )
    assert families["SPY|change|14:00+60m"] == []


def test_manifest_tamper_fails_closed(tmp_path):
    root = Path(m.__file__).resolve().parents[1] / "docs/reference/calendar-manifests"
    for name, _ in PINS:
        (tmp_path / name).write_bytes((root / name).read_bytes())
        (tmp_path / name).chmod(0o644)
    p = tmp_path / PINS[0][0]
    p.write_bytes(p.read_bytes() + b" ")
    with pytest.raises(ValueError, match="hash mismatch"):
        FactoryCalendar(tmp_path)


def test_real_run_path_checks_calendar_before_data(monkeypatch, tmp_path):
    import json

    p = tmp_path / "run.json"
    body = dict(
        run_id="out-of-calendar",
        first_session="2026-11-11",
        last_session="2026-11-12",
        decision_times=["14:00"],
        horizons=["60m"],
        features=["F1"],
        z_thresholds=[1.0],
    )
    p.write_text(json.dumps(body))
    monkeypatch.setattr(
        m, "check_data", lambda *a: pytest.fail("data touched before calendar gate")
    )
    with pytest.raises(ValueError, match="outside qualified"):
        m.run(p, tmp_path, None, tmp_path / "out")


def test_fabricated_full_short_prior_stays_withheld():
    families, _ = m.build_rows(
        {"2023-11-24": bars(), "2023-11-27": bars()}, spec(), FactoryCalendar()
    )
    assert families["SPY|change|14:00+60m"] == []
