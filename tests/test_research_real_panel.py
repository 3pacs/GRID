"""S09/S09b real-panel adapter: latest-vintage reads, publication-time known_at,
the revised-series denylist, proxy groups and the latest_vintage_read origin.

Runs the real ``store.observations.read_window`` SQL against an in-memory
SQLite ``raw_series`` shaped like production (FAILED zero markers, several
vintages per date, a revision pulled after ``as_of_ts``, an observation dated
after ``as_of``). No production DB.
"""

from __future__ import annotations

import copy
import sqlite3
from dataclasses import fields, replace
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
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    event,
)

from analysis import research_real_panel as rp
from analysis.offline_research_proof import (
    LATEST_VINTAGE_ORIGIN,
    Protocol,
    digest,
    discover,
    evaluate_holdout,
    run_proof,
    validate_rows,
)

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

START, AS_OF = date(2020, 1, 1), date(2021, 12, 31)
PULLED = datetime(2022, 1, 5, 6, 0, 0)  # noqa: DTZ001 - naive UTC, as SQLite returns it
AS_OF_TS = datetime(2022, 2, 1, tzinfo=timezone.utc)
SPLIT = "2021-01-04T00:00:00+00:00"
END = "2022-01-01T00:00:00+00:00"
FEATURES = (
    rp.SeriesSpec("FEAT_D", "diff", "FRB_H15"),
    rp.SeriesSpec("FEAT_W", "pct", "FRB_H10", stale_sessions=10),
)
TARGETS = (rp.TargetSpec("TGT", "change", "FRB_H15"),)
HORIZONS = (1, 5)


@pytest.fixture(autouse=True)
def tgt_proxy_group(monkeypatch):
    """The synthetic target declares itself and FEAT_D as its proxy group."""
    monkeypatch.setitem(rp.PROXY_GROUPS, "TGT", frozenset({"TGT", "FEAT_D"}))


@pytest.fixture()
def engine():
    engine = create_engine("sqlite://")
    md = MetaData()
    source_catalog = Table(
        "source_catalog", md,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
    )
    raw = Table(
        "raw_series",
        md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)
    FRED_SRC = 1
    rng = np.random.default_rng(9)
    days = pd.bdate_range(START, AS_OF)
    rows = []

    def row(sid, d, v, status="SUCCESS", pulled=PULLED):
        rows.append(
            {
                "series_id": sid,
                "source_id": FRED_SRC,
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
    # pull-log defects the latest-vintage path must ignore
    row("FEAT_D", date(2021, 6, 1), 0.0, status="FAILED", pulled=PULLED + timedelta(1))
    row("FEAT_D", date(2021, 6, 2), -999.0, pulled=PULLED - timedelta(days=3))  # older vintage
    row("TGT", date(2021, 3, 3), 99.0, pulled=PULLED + timedelta(days=55))  # after as_of_ts
    row("TGT", date(2022, 1, 3), 99.0)  # observed after as_of
    row("snap:llm_tokens", date(2021, 1, 4), 5.0)
    with engine.begin() as c:
        c.execute(source_catalog.insert(), [{"id": FRED_SRC, "name": "fred"}])
        c.execute(raw.insert(), rows)
    return engine


def load(conn, features=FEATURES, targets=TARGETS):
    return rp.load_latest_vintage_panel(
        conn, features, targets, start=START, as_of=AS_OF, as_of_ts=AS_OF_TS
    )


def protocol_for(panel, **overrides):
    return Protocol(
        run_id="s09-test",
        features=panel.feature_names(),
        split=SPLIT,
        end=END,
        origin=LATEST_VINTAGE_ORIGIN,
        families=panel.family_names(HORIZONS),
        step=5,
        perms=199,
        start="2020-05-01T00:00:00+00:00",
        read_receipt=panel.receipt_sha,
        self_lag=panel.self_lag(panel.family_names(HORIZONS)),
        **overrides,
    )


def utc(text):
    return pd.Timestamp(text, tz="UTC")


# --- reads -------------------------------------------------------------------------


def test_panel_reads_only_through_read_window(engine):
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


@pytest.mark.parametrize(
    "sid", ["NFCI", "NFCICREDIT", "ANFCI", "STLFSI4", "ICSA", "PAYEMS", "DTWEXBGS"]
)
def test_revised_series_are_refused_by_the_adapter_not_the_caller(engine, sid):
    # no caller-declared flag exists any more: the denylist lives in the adapter
    assert "revised" not in {f.name for f in fields(rp.SeriesSpec)}
    assert "revised" in rp.refusal(sid)
    with engine.connect() as conn:
        with pytest.raises(ValueError, match="vintage history"):
            load(conn, features=(*FEATURES, rp.SeriesSpec(sid)))
        with pytest.raises(ValueError, match="vintage history"):
            load(conn, targets=(rp.TargetSpec(sid),))


def test_revised_denylist_documents_its_sources():
    assert "NFCI" in rp.REVISED_PREFIXES and "STLFSI" in rp.REVISED_PREFIXES
    assert any("ALFRED" in source for source in rp.REVISED_SOURCES)
    for sid in ("DGS2", "T10Y2Y", "BAMLH0A0HYM2", "VIXCLS", "WALCL", "MORTGAGE30US"):
        assert rp.refusal(sid) is None


def test_undeclared_source_or_proxy_group_is_refused(engine):
    with engine.connect() as conn:
        with pytest.raises(ValueError, match="publication source"):
            load(conn, features=(*FEATURES, rp.SeriesSpec("FEAT_X", source="guess")))
        with pytest.raises(ValueError, match="proxy group"):
            load(conn, targets=(rp.TargetSpec("FEAT_D"),))


def test_adapter_never_names_hypothesis_tables_or_writes():
    source = Path(rp.__file__).read_text(encoding="utf-8").lower()
    code = source.split('"""', 2)[2]  # skip the module docstring
    for forbidden in ("discovered_hypotheses", "hypothesis_registry", "insert ", "text("):
        assert forbidden not in code


# --- availability ------------------------------------------------------------------


def test_publication_times_follow_the_declared_schedules():
    h15 = rp.PUBLICATIONS["FRB_H15"]
    # Friday -> Monday 21:17Z; the Friday before Presidents' Day -> Tuesday
    assert rp.publication_times([date(2021, 3, 5)], h15)[0] == utc("2021-03-08 21:17")
    assert rp.publication_times([date(2021, 2, 12)], h15)[0] == utc("2021-02-16 21:17")
    # a Saturday-dated observation is published the next business day
    assert rp.publication_times([date(2021, 3, 6)], h15)[0] == utc("2021-03-08 21:17")
    h10 = rp.PUBLICATIONS["FRB_H10"]
    assert rp.publication_times([date(2021, 3, 1)], h10)[0] == utc("2021-03-09 21:15")
    h41 = rp.PUBLICATIONS["FRB_H41"]  # Wednesday level
    assert rp.publication_times([date(2021, 3, 3)], h41)[0] == utc("2021-03-05 21:30")
    # every declared source is known strictly after the next day's 00:00Z decision
    for publication in rp.PUBLICATIONS.values():
        for d in (date(2021, 3, 1), date(2021, 3, 5), date(2021, 3, 6)):
            known = rp.publication_times([d], publication)[0]
            assert known > utc(d.isoformat()) + pd.Timedelta(days=1)
    assert h15.time_utc >= "20:17"  # reviewer-verified ~20:17Z, never earlier


def test_features_use_only_published_observations(engine):
    with engine.connect() as conn:
        panel = load(conn)
    index = panel.session_index()
    daily = panel._available_level(FEATURES[0], index)
    obs = {o.obs_date: o.value for o in panel.series_observations("FEAT_D")}
    # Friday 2021-03-05 is published Monday 21:17Z: usable Tuesday, not Monday
    assert daily[utc("2021-03-08")] == obs[date(2021, 3, 4)]
    assert daily[utc("2021-03-09")] == obs[date(2021, 3, 5)]
    # Presidents' Day 2021-02-15: Friday's value is published Tuesday 21:17Z
    assert daily[utc("2021-02-16")] == obs[date(2021, 2, 11)]
    assert daily[utc("2021-02-17")] == obs[date(2021, 2, 15)]  # newest published wins
    weekly = panel._available_level(FEATURES[1], index)
    wobs = {o.obs_date: o.value for o in panel.series_observations("FEAT_W")}
    # Saturday 2021-03-06 + 8 days = Sunday 03-14 21:15Z: first usable Monday 03-15
    assert weekly[utc("2021-03-12")] == wobs[date(2021, 2, 27)]
    assert weekly[utc("2021-03-15")] == wobs[date(2021, 3, 6)]


def test_no_feature_is_known_after_its_decision(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    stamped = 0
    for window in ("discovery", "holdout"):
        for rows in panel.family_rows(protocol, window).values():
            validate_rows(rows, protocol, window)
            for row in rows:
                decision = pd.Timestamp(row["decision_at"])
                for feature in row["features"].values():
                    known = pd.Timestamp(feature["known_at"])
                    assert known <= decision
                    if feature["value"] is not None:
                        # the declared publication stamp, not the decision time
                        assert known < decision
                        assert known.strftime("%H:%M") in ("21:17", "21:15")
                        stamped += 1
    assert stamped > 1000
    # A value stamped as known at its 00:00Z session (the pre-S09b stamping)
    # while H.15 publishes at 21:17Z is refused.
    rows = copy.deepcopy(panel.family_rows(protocol, "discovery")[protocol.families[0]])
    decision = pd.Timestamp(rows[0]["decision_at"])
    rows[0]["features"]["FEAT_D|chg5"]["known_at"] = (
        decision + pd.Timedelta(hours=21, minutes=17)
    ).isoformat()
    with pytest.raises(ValueError, match="future"):
        validate_rows(rows, protocol, "discovery")


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
            published = rp.publication_times([end.date()], rp.PUBLICATIONS["FRB_H15"])
            assert known == published[0]
            assert row["label"] == "change"


# --- the distinct latest_vintage_read origin ---------------------------------------


def test_latest_vintage_run_end_to_end_is_exploratory_and_never_promotes(engine, tmp_path):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    result = run_proof(
        protocol,
        panel.family_rows(protocol, "discovery"),
        panel.family_rows(protocol, "holdout"),
        tmp_path / "run",
        panel=panel,
    )
    assert result["state"] == "LATEST_VINTAGE_READ_EXPLORATORY"
    assert not result["promotion_allowed"] and result["forward_evidence_count"] == 0
    assert panel.receipt["origin"] == "latest_vintage_read"
    assert "hindsight" in panel.receipt["vintage"]
    assert panel.receipt["proxy_groups"] == {"TGT": ["FEAT_D", "TGT"]}


def test_origin_label_without_a_verified_panel_is_refused(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    rows = panel.family_rows(protocol, "discovery")
    with pytest.raises(ValueError, match="read_window"):
        discover(protocol, rows)
    with pytest.raises(ValueError, match="read_window"):
        discover(protocol, rows, panel=object())
    with pytest.raises(ValueError, match="receipt"):
        discover(replace(protocol, read_receipt="0" * 64), rows, panel=panel)
    with pytest.raises(ValueError, match="receipt"):
        replace(protocol, read_receipt="").validate()
    with pytest.raises(ValueError, match="receipt"):
        replace(protocol, origin="exploratory_replay").validate()
    with pytest.raises(ValueError, match="not implemented"):
        replace(protocol, origin="pit_vintage_read").validate()  # the old label is gone
    with pytest.raises(TypeError):
        rp.LatestVintagePanel(
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
        discover(protocol, tampered, panel=panel)
    frozen = discover(protocol, rows, panel=panel)
    holdout = panel.family_rows(protocol, "holdout")
    with pytest.raises(ValueError, match="read_window"):
        evaluate_holdout(frozen, holdout)
    evaluate_holdout(frozen, holdout, panel=panel)


def test_a_panel_changed_after_its_read_is_refused(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    rows = panel.family_rows(protocol, "discovery")
    obs = list(panel._data["TGT"])
    obs[10] = replace(obs[10], value=obs[10].value + 1)
    panel._data["TGT"] = tuple(obs)
    with pytest.raises(ValueError, match="changed after"):
        discover(protocol, rows, panel=panel)


def test_panel_is_refused_for_other_origins(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = replace(
        protocol_for(panel), origin="exploratory_replay", read_receipt=""
    )
    rows = panel.family_rows(protocol, "discovery")
    with pytest.raises(ValueError, match="only accepted with origin"):
        discover(protocol, rows, panel=panel)


def test_window_past_as_of_is_refused(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    late = replace(protocol, end="2022-03-01T00:00:00+00:00")
    with pytest.raises(ValueError, match="past the panel"):
        discover(late, panel.family_rows(late, "discovery"), panel=panel)


# --- proxy groups / SELF_LAG ---------------------------------------------------------


def test_proxy_trials_are_measured_but_never_selectable(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    assert set(protocol.self_lag) == {
        (family, f"FEAT_D|{suffix}")
        for family in protocol.families
        for suffix in ("chg5", "chg20", "z60")
    }
    payload = discover(
        protocol, panel.family_rows(protocol, "discovery"), panel=panel
    )["payload"]
    proxies = [t for t in payload["ledger"] if t["status"] == "self_lag"]
    assert len(proxies) == payload["self_lag_count"] == 6
    for trial in proxies:
        assert trial["p"] == 1.0 and not trial["selected"] and trial["r"] is None
        assert trial["self_lag_p"] is not None and trial["self_lag_r"] is not None
    assert all(
        t["status"] != "self_lag" for t in payload["ledger"] if "FEAT_W" in t["feature"]
    )


def test_a_protocol_that_drops_the_proxy_groups_is_refused(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = replace(protocol_for(panel), self_lag=())
    with pytest.raises(ValueError, match="proxy groups"):
        discover(protocol, panel.family_rows(protocol, "discovery"), panel=panel)


def test_a_forged_self_lag_selection_never_becomes_a_candidate(engine):
    with engine.connect() as conn:
        panel = load(conn)
    protocol = protocol_for(panel)
    frozen = discover(protocol, panel.family_rows(protocol, "discovery"), panel=panel)
    forged = copy.deepcopy(frozen)
    next(t for t in forged["payload"]["ledger"] if t["status"] == "self_lag")[
        "selected"
    ] = True
    forged["sha256"] = digest(forged["payload"])
    with pytest.raises(ValueError, match="never become candidates"):
        evaluate_holdout(forged, panel.family_rows(protocol, "holdout"), panel=panel)
