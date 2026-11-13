"""store.fred_vintages + ingestion.fred_vintages: what FRED showed for an observation on a given date."""

from __future__ import annotations

import io
import json
import sqlite3
from datetime import date, datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine

from ingestion import fred_vintages as F
from store import fred_vintages as V

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

D = date


@pytest.fixture()
def engine():
    return create_engine("sqlite://")


# USEPUINDXD 2022-06-01/02 as ALFRED shows them (first prints and the next versions, from the 2026-11-13 probe).
ROWS = [
    {"obs_date": D(2022, 6, 1), "realtime_start": D(2022, 6, 2), "realtime_end": D(2022, 6, 2), "value": 115.91},
    {"obs_date": D(2022, 6, 1), "realtime_start": D(2022, 6, 3), "realtime_end": D(2022, 6, 5), "value": 165.72},
    {"obs_date": D(2022, 6, 1), "realtime_start": D(2022, 6, 6), "realtime_end": V.OPEN_END, "value": 163.44},
    {"obs_date": D(2022, 6, 2), "realtime_start": D(2022, 6, 3), "realtime_end": D(2022, 6, 5), "value": 60.23},
    {"obs_date": D(2022, 6, 2), "realtime_start": D(2022, 6, 6), "realtime_end": V.OPEN_END, "value": 152.06},
]


def test_value_is_the_version_live_at_the_end_of_as_of(engine):
    V.upsert(engine, "USEPUINDXD", ROWS)
    df = V.load(engine, "USEPUINDXD", D(2022, 5, 1), D(2022, 6, 30), D(2022, 6, 30))
    obs = [D(2022, 6, 1), D(2022, 6, 2)]
    assert V.values_as_of(df, obs, D(2022, 6, 1)) == {D(2022, 6, 1): None, D(2022, 6, 2): None}   # not printed yet
    assert V.values_as_of(df, obs, D(2022, 6, 2)) == {D(2022, 6, 1): 115.91, D(2022, 6, 2): None}
    assert V.values_as_of(df, obs, D(2022, 6, 4)) == {D(2022, 6, 1): 165.72, D(2022, 6, 2): 60.23}
    assert V.values_as_of(df, obs, D(2022, 6, 20)) == {D(2022, 6, 1): 163.44, D(2022, 6, 2): 152.06}


def test_load_never_returns_a_version_published_after_rt_until(engine):
    V.upsert(engine, "USEPUINDXD", ROWS)
    df = V.load(engine, "USEPUINDXD", D(2022, 5, 1), D(2022, 6, 30), D(2022, 6, 3))
    assert df["realtime_start"].max() <= D(2022, 6, 3)


def test_reader_refuses_past_its_coverage():
    with pytest.raises(ValueError):
        V.values_as_of(pd.DataFrame(ROWS), [D(2022, 6, 1)], D(2022, 6, 1) + pd.Timedelta(days=V.MAX_AGE_DAYS + 1).to_pytimedelta())


def test_upsert_only_extends_a_version_end(engine):
    clipped = dict(ROWS[2], realtime_end=D(2022, 7, 1))
    V.upsert(engine, "USEPUINDXD", [clipped])
    V.upsert(engine, "USEPUINDXD", [dict(ROWS[2], realtime_end=D(2022, 8, 1))])
    V.upsert(engine, "USEPUINDXD", [dict(ROWS[2], realtime_end=D(2022, 6, 10))])     # an older, shorter view
    df = V.load(engine, "USEPUINDXD", D(2022, 6, 1), D(2022, 6, 1), D(2030, 1, 1))
    assert list(df["realtime_end"]) == [D(2022, 8, 1)]


def test_a_refresh_duplicate_clipped_at_its_window_start_reads_the_same(engine):
    V.upsert(engine, "USEPUINDXD", ROWS)
    # A refresh from 2022-06-10 returns the live version with realtime_start clipped to the window start.
    V.upsert(engine, "USEPUINDXD", [dict(ROWS[2], realtime_start=D(2022, 6, 10))])
    df = V.load(engine, "USEPUINDXD", D(2022, 5, 1), D(2022, 6, 30), D(2022, 6, 30))
    assert V.values_as_of(df, [D(2022, 6, 1)], D(2022, 6, 4)) == {D(2022, 6, 1): 165.72}
    assert V.values_as_of(df, [D(2022, 6, 1)], D(2022, 6, 12)) == {D(2022, 6, 1): 163.44}


def test_fetch_parses_alfred_rows(monkeypatch):
    body = {"observations": [{"date": "2022-06-02", "realtime_start": "2022-06-03", "realtime_end": "2022-06-05",
                              "value": "60.23000"},
                             {"date": "2022-06-04", "realtime_start": "2022-06-06", "realtime_end": "9999-12-31",
                              "value": "."}]}
    seen = {}

    class R(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def fake(req, timeout):
        seen["url"] = req.full_url
        return R(json.dumps(body).encode())

    monkeypatch.setattr(F.urllib.request, "urlopen", fake)
    rows = F.fetch("USEPUINDXD", D(2022, 6, 1), D(2022, 6, 5), D(2022, 6, 1), D(2022, 7, 5), "k")
    assert rows[0] == {"obs_date": D(2022, 6, 2), "realtime_start": D(2022, 6, 3), "realtime_end": D(2022, 6, 5),
                       "value": 60.23}
    assert rows[1]["value"] is None and rows[1]["realtime_end"] == V.OPEN_END
    assert "realtime_start=2022-06-01" in seen["url"] and "realtime_end=2022-07-05" in seen["url"]


def test_backfill_pads_every_chunk_past_the_reader_window(engine, monkeypatch):
    calls = []
    monkeypatch.setattr(F, "_key", lambda: "k")
    monkeypatch.setattr(F, "fetch", lambda s, o0, o1, r0, r1, k: calls.append((o0, o1, r0, r1)) or [])
    out = F.backfill(engine, "USEPUINDXD", D(2018, 1, 1), D(2022, 1, 1))
    assert out["status"] == "SUCCESS" and out["chunks"] == len(calls) >= 2
    for o0, o1, r0, r1 in calls[:-1]:
        assert (r1 - o1).days == F.PAD_DAYS > V.MAX_AGE_DAYS and r0 <= o0
    assert calls[-1][3] == V.OPEN_END
    assert calls[0][2] == D(2018, 1, 1) and calls[-1][1] == D(2022, 1, 1)


def test_missing_key_skips_without_raising(engine, monkeypatch):
    monkeypatch.setattr(F, "_key", lambda: None)
    assert F.backfill(engine, "USEPUINDXD", D(2022, 1, 1), D(2022, 2, 1))["status"] == "SKIPPED"
