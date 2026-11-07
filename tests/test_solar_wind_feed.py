"""Solar wind speed from NOAA SWPC (2026-11-07).

``products/solar-wind/plasma-7-day.json`` was retired (404). The puller now
reads ``products/geospace/propagated-solar-wind.json``, whose columns differ
(speed is second, not third), so columns are found by header name.
"""

from __future__ import annotations

from datetime import date

from ingestion.celestial.solar import SolarActivityPuller


def _puller(monkeypatch, payload):
    p = object.__new__(SolarActivityPuller)  # no DB: only the parser is exercised
    urls: list[str] = []

    def fake_fetch(url):  # noqa: ANN001
        urls.append(url)
        return payload

    monkeypatch.setattr(p, "_fetch_json", fake_fetch, raising=False)
    return p, urls


HEADER = ["time_tag", "speed", "density", "temperature", "bx", "by", "bz", "bt",
          "vx", "vy", "vz", "propagated_time_tag"]


def test_daily_average_reads_the_speed_column_by_name(monkeypatch) -> None:
    payload = [
        HEADER,
        ["2026-11-06T23:58:00Z", 400.0, 5.0, 1e5, 0, 0, 0, 0, 0, 0, 0, "x"],
        ["2026-11-06T23:59:00Z", 420.0, 6.0, 1e5, 0, 0, 0, 0, 0, 0, 0, "x"],
        ["2026-11-07T00:00:00Z", 500.0, 9.9, 1e5, 0, 0, 0, 0, 0, 0, 0, "x"],
        ["2026-11-07T00:01:00Z", None, 9.9, 1e5, 0, 0, 0, 0, 0, 0, 0, "x"],   # gap
        ["2026-11-07T00:02:00Z", -1, 9.9, 1e5, 0, 0, 0, 0, 0, 0, 0, "x"],     # fill value
    ]
    p, urls = _puller(monkeypatch, payload)
    got = p._pull_solar_wind()
    assert urls == ["https://services.swpc.noaa.gov/products/geospace/propagated-solar-wind.json"]
    assert got == {date(2026, 11, 6): 410.0, date(2026, 11, 7): 500.0}


def test_reordered_columns_still_read_speed(monkeypatch) -> None:
    payload = [["time_tag", "density", "speed", "temperature"],
               ["2026-11-06T12:00:00Z", 5.0, 380.0, 1e5]]
    p, _ = _puller(monkeypatch, payload)
    assert p._pull_solar_wind() == {date(2026, 11, 6): 380.0}


def test_feed_without_header_or_speed_yields_nothing(monkeypatch) -> None:
    p, _ = _puller(monkeypatch, [{"time_tag": "2026-11-06T12:00:00", "speed": 400.0}])
    assert p._pull_solar_wind() == {}
    p, _ = _puller(monkeypatch, [["time_tag", "density"], ["2026-11-06T12:00:00Z", 5.0]])
    assert p._pull_solar_wind() == {}
