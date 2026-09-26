"""S09 real-panel adapter: vintage-safe reads and the distinct PIT origin.

Runs the real ``store.observations.read_window`` SQL against an in-memory
SQLite ``raw_series`` shaped like production (FAILED zero markers, several
vintages per date, a revision pulled after ``as_of_ts``, an observation dated
after ``as_of``). No production DB.
"""

from __future__ import annotations

import copy
import sqlite3
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    event,
)

from analysis import research_real_panel as rp
from analysis.offline_research_proof import (
    PIT_ORIGIN,
    Protocol,
    discover,
    evaluate_holdout,
    run_proof,
)

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

START, AS_OF = date(2020, 1, 1), date(2021, 12, 31)
PULLED = datetime(2022, 1, 5, 6, 0, 0)  # noqa: DTZ001 - naive UTC, as SQLite returns it
AS_OF_TS = datetime(2022, 2, 1, tzinfo=timezone.utc)
SPLIT = "2021-01-04T00:00:00+00:00"
END = "2022-01-01T00:00:00+00:00"
FEATURES = (
    rp.SeriesSpec("FEAT_D", "diff", lag_days=1),
    rp.SeriesSpec("FEAT_W", "pct", lag_days=6, stale_sessions=10),
)
TARGETS = (rp.TargetSpec("TGT", "change", lag_days=1),)
HORIZONS = (1, 5)


@pytest.fixture()
def engine():
    engine = create_engine("sqlite://")
    md = MetaData()
    raw = Table(
        "raw_series",
        md,
        Column("series_id", String, nullable=False),
        Column("source_id", String, nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)
    rng = np.random.default_rng(9)
    days = pd.bdate_range(START, AS_OF)
    rows = []

    def row(sid, d, v, status="SUCCESS", pulled=PULLED):
        rows.append(
            {
                "series_id": sid,
                "source_id": "fred",
                "obs_date": d,
                "pull_timestamp": pulled,
                "value": float(v),
                "raw_payload": "{}",
                "pull_status": status,
            }
        )

    tgt = np.cumsum(rng.normal(0, 0.05, len(days))) + 4
    feat = np.cumsum(rng.normal(0, 1, len(days)))
    for i, d in enumerate(days):
        row("TGT", d.date(), tgt[i])
        row("FEAT_D", d.date(), feat[i])
    for d in pd.date_range(START, AS_OF, freq="W-SAT"):  # weekly, Saturday-dated
        row("FEAT_W", d.date(), 100 + rng.normal())
    # pull-log defects the vintage-safe path must ignore
    row("FEAT_D", date(2021, 6, 1), 0.0, status="FAILED", pulled=PULLED + timedelta(1))
    row("FEAT_D", date(2021, 6, 2), -999.0, pulled=PULLED - timedelta(days=3))  # older vintage
    row("TGT", date(2021, 3, 3), 99.0, pulled=PULLED + timedelta(days=55))  # after as_of_ts
    row("TGT", date(2022, 1, 3), 99.0)  # observed after as_of
    row("snap:llm_tokens", date(2021, 1, 4), 5.0)
    with engine.begin() as c:
        c.execute(raw.insert(), rows)
    return engine


def load(conn, features=FEATURES, targets=TARGETS):
    return rp.load_pit_panel(
        conn, features, targets, start=START, as_of=AS_OF, as_of_ts=AS_OF_TS
    )


def protocol_for(panel, **overrides):
    return Protocol(
        run_id="s09-test",
        features=panel.feature_names(),
        split=SPLIT,
        end=END,
        origin=PIT_ORIGIN,
        families=panel.family_names(HORIZONS),
        step=5,
        perms=199,
        start="2020-05-01T00:00:00+00:00",
        pit_receipt=panel.receipt_sha,
        **overrides,
    )


# --- reads -------------------------------------------------------------------------


def test_panel_reads_only_through_the_vintage_safe_path(engine):
    statements = []
    event.listen(
        engine,
        "before_cursor_execute",
        lambda conn, cursor, stmt, *a: statements.append(stmt),
    )
    with engine.connect() as conn:
        panel = load(conn)
    assert len(statements) == 3  # one bounded read per declared series
    for stmt in statements:
        low = stmt.lower()
        assert "from raw_series" in low and "pull_status" in low
        assert "hypothes" not in low and "insert" not in low and "update" not in low
    assert panel.receipt["reader"] == "store.observations.read_window"


def test_failed_late_and_future_rows_never_enter_the_panel(engine):
    with engine.connect() as conn:
        panel = load(conn)
    feat = {o.obs_date: o.value for o in panel.series_observations("FEAT_D")}
    tgt = {o.obs_date: o.value for o in panel.series_observations("TGT")}
    assert feat[date(2021, 6, 1)] != 0.0  # FAILED zero ignored
    assert feat[date(2021, 6, 2)] != -999.0  # latest vintage wins
    assert tgt[date(2021, 3, 3)] != 99.0  # revision pulled after as_of_ts invisible
    assert max(tgt) <= AS_OF  # nothing observed after as_of
    assert all(
        o.pull_timestamp.replace(tzinfo=timezone.utc) <= AS_OF_TS
        for sid in ("FEAT_D", "FEAT_W", "TGT")
        for o in panel.series_observations(sid)
    )


@pytest.mark.parametrize(
    "sid", ["snap:llm_tokens", "hermes_llm_calls", "astro:moon_phase", "YF:SPY:close"]
)
def test_excluded_or_unverified_series_are_refused(engine, sid):
    with engine.connect() as conn:
        with pytest.raises(ValueError, match="excluded|refused"):
            load(conn, features=(*FEATURES, rp.SeriesSpec(sid)))
        with pytest.raises(ValueError, match="excluded|refused"):
            load(conn, targets=(rp.TargetSpec(sid),))


def test_revised_series_without_vintage_history_are_refused(engine):
    with engine.connect() as conn, pytest.raises(ValueError, match="vintage history"):
        load(conn, features=(*FEATURES, rp.SeriesSpec("NFCI", revised=True)))


def test_adapter_never_names_hypothesis_tables_or_writes():
    source = Path(rp.__file__).read_text(encoding="utf-8").lower()
    code = source.split('"""', 2)[2]  # skip the module docstring
    for forbidden in ("discovered_hypotheses", "hypothesis_registry", "insert ", "text("):
        assert forbidden not in code


# --- availability ------------------------------------------------------------------


def test_features_use_only_published_observations(engine):
    with engine.connect() as conn:
        panel = load(conn)
    index = panel.session_index()
    daily = panel._available_level(FEATURES[0], index)
    obs = {o.obs_date: o.value for o in panel.series_observations("FEAT_D")}
    monday = pd.Timestamp("2021-03-08", tz="UTC")
    assert daily[monday] == obs[date(2021, 3, 5)]  # Friday's value, published Saturday
    weekly = panel._available_level(FEATURES[1], index)
    wobs = {o.obs_date: o.value for o in panel.series_observations("FEAT_W")}
    # Saturday 2021-03-06 + 6 days = Friday 2021-03-12: invisible before, visible on it
    assert weekly[pd.Timestamp("2021-03-11", tz="UTC")] == wobs[date(2021, 2, 27)]
    assert weekly[pd.Timestamp("2021-03-12", tz="UTC")] == wobs[date(2021, 3, 6)]


def test_target_known_at_respects_publication_lag_and_window(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    discovery = panel.family_rows(protocol, "discovery")
    for rows in discovery.values():
        assert rows
        for row in rows:
            end, known = pd.Timestamp(row["label_end"]), pd.Timestamp(row["target_known_at"])
            assert known > end and known < pd.Timestamp(SPLIT)
            assert row["label"] == "change"


# --- the distinct PIT origin -------------------------------------------------------


def test_pit_run_end_to_end_is_exploratory_and_never_promotes(engine, tmp_path):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    result = run_proof(
        protocol,
        panel.family_rows(protocol, "discovery"),
        panel.family_rows(protocol, "holdout"),
        tmp_path / "run",
        pit_panel=panel,
    )
    assert result["state"] == "PIT_VINTAGE_READ_EXPLORATORY"
    assert not result["promotion_allowed"] and result["forward_evidence_count"] == 0


def test_pit_label_without_a_verified_panel_is_refused(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    rows = panel.family_rows(protocol, "discovery")
    with pytest.raises(ValueError, match="vintage-safe read path"):
        discover(protocol, rows)
    with pytest.raises(ValueError, match="vintage-safe read path"):
        discover(protocol, rows, pit_panel=object())
    with pytest.raises(ValueError, match="receipt"):
        discover(replace(protocol, pit_receipt="0" * 64), rows, pit_panel=panel)
    with pytest.raises(ValueError, match="receipt"):
        replace(protocol, pit_receipt="").validate()
    with pytest.raises(ValueError, match="receipt"):
        replace(protocol, origin="exploratory_replay").validate()
    with pytest.raises(TypeError):
        rp.PitPanel(
            token=object(),
            features=FEATURES,
            targets=TARGETS,
            start=START,
            as_of=AS_OF,
            as_of_ts=AS_OF_TS,
            data={},
        )


def test_rows_not_derived_from_the_panel_are_refused(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    rows = panel.family_rows(protocol, "discovery")
    tampered = copy.deepcopy(rows)
    tampered[protocol.families[0]][3]["target"] += 0.5
    with pytest.raises(ValueError, match="re-derived"):
        discover(protocol, tampered, pit_panel=panel)
    frozen = discover(protocol, rows, pit_panel=panel)
    holdout = panel.family_rows(protocol, "holdout")
    with pytest.raises(ValueError, match="vintage-safe read path"):
        evaluate_holdout(frozen, holdout)
    evaluate_holdout(frozen, holdout, pit_panel=panel)


def test_a_panel_changed_after_its_read_is_refused(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    rows = panel.family_rows(protocol, "discovery")
    obs = list(panel._data["TGT"])
    obs[10] = replace(obs[10], value=obs[10].value + 1)
    panel._data["TGT"] = tuple(obs)
    with pytest.raises(ValueError, match="changed after"):
        discover(protocol, rows, pit_panel=panel)


def test_panel_is_refused_for_non_pit_origins(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = replace(protocol_for(panel), origin="exploratory_replay", pit_receipt="")
    rows = panel.family_rows(protocol, "discovery")
    with pytest.raises(ValueError, match="only accepted with origin"):
        discover(protocol, rows, pit_panel=panel)


def test_window_past_as_of_is_refused(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    late = replace(protocol, end="2022-03-01T00:00:00+00:00")
    with pytest.raises(ValueError, match="past the panel"):
        discover(late, panel.family_rows(late, "discovery"), pit_panel=panel)
