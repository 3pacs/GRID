"""Tests for the VS1 v5 harness (``analysis/panel_insider_density_v5.py``).

Synthetic data only: no production DB, no network, no Tiingo or TwelveData call,
no price or outcome of any real issuer.
"""

from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd
import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v4 as v4
from analysis import panel_insider_density_v5 as v5
from tests.test_panel_insider_density import NOW, _row, _submission
from tests.test_panel_insider_density_v2 import (
    _discovery_key,
    _holdings,
    _inputs,
    _observed,
    _register,
    _universe_frame,
    _Vault,
)


def _tiingo_db(series, kaggle=None):
    """SQLite raw_series shaped like production: TIINGO adj_close rows (+ refused KAGGLE_BULK rows, same ids)."""
    from sqlalchemy import Column, Date, DateTime, Float, Integer, MetaData, String, Table, Text, create_engine

    engine = create_engine("sqlite://")
    md = MetaData()
    catalog = Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String))
    raw = Table("raw_series", md, Column("series_id", String), Column("source_id", Integer),
                Column("obs_date", Date), Column("pull_timestamp", DateTime), Column("value", Float),
                Column("raw_payload", Text), Column("pull_status", String))
    md.create_all(engine)
    from datetime import datetime as _dt

    pulled = _dt(2026, 9, 20, 6, 0)  # before the frozen as_of_ts of the synthetic registry
    rows = []
    for source, table in ((524, series), (522, kaggle or {})):
        for ticker, values in table.items():
            rows.extend({"series_id": f"YF:{ticker}:adj_close", "source_id": source, "obs_date": d.date(),
                         "pull_timestamp": pulled, "value": float(v), "raw_payload": "{}",
                         "pull_status": "SUCCESS"} for d, v in values.items())
    with engine.begin() as c:
        c.execute(catalog.insert(), [{"id": 524, "name": "TIINGO"}, {"id": 522, "name": "KAGGLE_BULK"}])
        c.execute(raw.insert(), rows)
    return engine


def _vault(root, **kw):
    return _Vault(root, h=v5, seeds=("vs1-v1", "vs1-v2"), **kw)


@pytest.fixture(autouse=True)
def _v5_not_superseded(request, monkeypatch):
    """v5 is superseded by v6 (pinned); the machinery tests exercise v5 as if it were current."""
    if request.node.name != "test_v4_refuses_on_its_pin":
        monkeypatch.setattr(v5, "SUPERSEDED_BY", None)


def _seed_v3_v4(vault):
    from analysis import panel_insider_density_v3 as v3

    vault.add_file(v3.WITNESS_PATH, v3.REGISTERED_ANCHOR_LINE + b"\n")
    vault.add_file(v4.WITNESS_PATH, v4.REGISTERED_ANCHOR_LINE + b"\n")


# --- pins and text --------------------------------------------------------------------------------------


def test_v5_prereg_hashes_to_the_pin_and_states_the_rules():
    assert v5.check_prereg() == v5.PREREG_BODY_SHA256 != v4.PREREG_BODY_SHA256
    body = v1.prereg_body((v5.REPO / v5.PREREG_PATH).read_text(encoding="utf-8"))
    for text in ("**|f_t / f_s − 1| ≤ 1e-4**", "L_T = max(S_T, F_{e,T})", "Jaccard similarity",
                 "**0.5**", "TIINGO_API_KEY", "`KAGGLE_BULK` or `yfinance` rows under `YF:AAOI:close`",
                 "The 2020-01-01 boundary"):
        assert text in body, text
    for pin in (v4.PREREG_BODY_SHA256, v4.REGISTERED_RECORD_SHA256[1]):
        assert pin in body


def test_v5_keeps_v4s_crosscheck_and_primary():
    assert v5.CROSSCHECK is v4.CROSSCHECK and v5.crosscheck_statistics is v4.crosscheck_statistics
    assert v5.PRIMARY_TRIAL == "A90|fwd5" and v5.V5.pins.number == 5 and v5.V5.pins.holdout_probe_required
    assert [e.version for e in v5.V5.pins.earlier] == ["vs1-v1", "vs1-v2", "vs1-v3", "vs1-v4"]
    assert v5.WITNESS_PATH == v1.canonical_witness_path("vs1-v5") and v5.SPLICE_TOL == 1e-4
    records = v5.registration_records(v5.REGISTERED_AT, v5.REGISTERED_CODE_SHA)
    assert tuple(v1.chained_sha256(records)) == v5.REGISTERED_RECORD_SHA256
    assert records[1]["price_admission"]["splice"]["tolerance"] == 1e-4


# --- manifest ---------------------------------------------------------------------------------------------


def _manifest(**over):
    base = dict(source="TIINGO", series_template="YF:{ticker}:adj_close", basis="split+dividend adjusted",
                benchmark="XLK", admitted=("AAA", "XLK"), probe_report_sha256="a" * 64,
                crosscheck_report_sha256="c" * 64, tiingo_meta_report_sha256="d" * 64,
                listed_from=(("AAA", "2014-01-02"),))
    return v5.PriceManifest(**{**base, **over})


def test_the_manifest_needs_l_t_for_every_admitted_ticker_and_the_meta_report():
    _manifest().validate()
    with pytest.raises(ValueError, match="listed_from"):
        _manifest(listed_from=()).validate()
    with pytest.raises(ValueError, match="metadata report"):
        _manifest(tiingo_meta_report_sha256="").validate()
    with pytest.raises(ValueError, match="TIINGO|refused"):
        _manifest(source="KAGGLE_BULK").validate()


# --- splice check -------------------------------------------------------------------------------------------


def _series(n=60, step_at=None, step=1.0, td_step=None):
    sessions = [f"2015-01-{i:02d}T{i:03d}" for i in range(n)]
    close = {d: 100.0 + i for i, d in enumerate(sessions)}
    factor = np.ones(n)
    if step_at is not None:
        factor[step_at:] *= step
    adj = {d: close[d] * factor[i] for i, d in enumerate(sessions)}
    batch = {d: ("2026-09-28" if i < (step_at or n // 2) else "2026-04-07") for i, d in enumerate(sessions)}
    td_factor = np.ones(n)
    if td_step is not None and step_at is not None:
        td_factor[step_at:] *= td_step
    td_adj = {d: close[d] * td_factor[i] for i, d in enumerate(sessions)}
    return sessions, batch, adj, close, td_adj, dict(close)


def test_a_continuous_batch_boundary_passes():
    out = v5.splice_check(*_series())
    assert out["boundaries"] == 1 and out["passed"]


def test_a_readjusted_vintage_splice_fails():
    out = v5.splice_check(*_series(step_at=30, step=1.006))  # 0.6% of dividends between the two pulls
    assert out["boundaries"] == 1 and not out["passed"] and out["failed"]


def test_a_splice_step_corroborated_by_twelvedata_passes():
    assert v5.splice_check(*_series(step_at=30, step=0.98, td_step=0.98))["passed"]
    assert not v5.splice_check(*_series(step_at=30, step=0.98, td_step=0.99))["passed"]


# --- ticker-reuse bound and entity check ----------------------------------------------------------------------


@pytest.mark.parametrize("sec,tiingo,expected", [
    ("ADVANCED MICRO DEVICES INC", "Advanced Micro Devices Inc", True),
    ("Element Solutions Inc", "Element Solutions Inc.", True),
    ("NVIDIA CORP", "NVIDIA Corporation", True),
    ("Alphabet Inc.", "Alphabet Inc Class A", True),
    ("Element Solutions Inc", "ITT Educational Services Inc", False),
    ("Gen Digital Inc.", "Genesis Healthcare Inc", False),
    ("", "Anything", False),
])
def test_name_match_rule(sec, tiingo, expected):
    assert v5.name_match(sec, tiingo) is expected


def test_listing_bound_is_the_later_date():
    assert v5.listing_bound("2012-05-18", "2014-03-01") == "2014-03-01"
    assert v5.listing_bound("2019-06-03T00:00:00", "2014-03-01") == "2019-06-03"
    assert v5.listing_bound(None, "2014-03-01") is None


def test_first_naming_dates_come_from_the_issuers_own_filings():
    rows = [
        {"accession_number": "a1", "filing_date": "2013-02-01", "issuer_cik": "100", "issuer_ticker": "OLD"},
        {"accession_number": "a2", "filing_date": "2016-05-02", "issuer_cik": "100", "issuer_ticker": "NEW"},
        {"accession_number": "a3", "filing_date": "2017-01-03", "issuer_cik": "100", "issuer_ticker": "NEW"},
    ]
    subs = pd.DataFrame([{**_submission(**r), "issuer_ticker": r["issuer_ticker"]} for r in rows])
    universe = _universe_frame([("NEW", 100, ["NEW"])])
    admission = v2.build_admission(subs, universe)
    assert v5.first_naming_dates(admission, universe) == {"NEW": "2016-05-02"}


def test_closes_before_l_t_are_blanked_before_any_label(tmp_path):
    """A reused symbol: the old company's closes (before L_T) never enter a label or feature."""
    rng = np.random.default_rng(3)
    tickers = [f"T{i:02d}" for i in range(22)]
    ciks = list(range(1000, 1022))
    dates = pd.bdate_range("2011-11-01", "2019-12-31")
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.01, (len(dates), 23)), axis=0)),
                          index=dates, columns=tickers + ["XLK"])
    engine = _tiingo_db({c: prices[c] for c in prices.columns})
    rows = [_row(accession_number=f"p-{c}-{i}", issuer_cik=str(c), owner_cik="7",
                 filing_date=str(dates[i + 1].date()), transaction_date=str(dates[i].date()))
            for c in ciks for i in range(100, len(dates) - 2, 150)]

    def ticker_of(cik, d):
        return tickers[cik - 1000]

    subs = pd.DataFrame([{**_submission(accession_number=r["accession_number"], filing_date=r["filing_date"],
                                        issuer_cik=r["issuer_cik"], owner_cik="7"),
                          "issuer_ticker": ticker_of(int(r["issuer_cik"]), None)} for r in rows]
                        + _holdings(ciks, dates, ticker_of))
    tx = pd.DataFrame(rows).astype("string")
    tx.columns = [c.upper() for c in tx.columns]
    events = v1.build_events(tx, submissions=subs.rename(columns=str.upper))
    universe = _universe_frame([(t, c, [t]) for t, c in zip(tickers, ciks)])
    admission = v5.build_admission(subs, universe)
    listed = tuple((t, "2016-01-04" if t == "T00" else "2011-11-01") for t in tickers)
    manifest = _manifest(admitted=tuple(sorted(tickers + ["XLK"])), listed_from=listed)
    vault = _vault(tmp_path / "vault")
    _seed_v3_v4(vault)
    key, _ = _discovery_key(tmp_path / "reg", vault, manifest)
    with engine.connect() as conn:
        panel = v5.load_price_panel(conn, manifest, tickers, start=date(2011, 11, 1), as_of=date(2019, 12, 31),
                                    window="discovery", key=key)
    panels = v5.build_trial_panels(events, admission, universe, panel, "discovery")
    primary = panels["A90|fwd5"]
    decided = pd.DatetimeIndex(primary.decision_at)
    early = decided < pd.Timestamp("2016-01-04", tz="UTC")
    j = primary.entities.index("T00")
    assert np.isnan(primary.label[early, j]).all() and np.isnan(primary.feature[early, j]).all()
    assert np.isfinite(primary.label[~early, j]).sum() > 0
    k = primary.entities.index("T01")
    assert np.isfinite(primary.label[early, k]).sum() > 0  # other tickers untouched
    unblanked = v2.build_trial_panels(events, admission, universe, panel, "discovery")["A90|fwd5"]
    assert np.isfinite(unblanked.label[early, j]).sum() > 0  # v2-v4 behaviour unchanged by default


# --- supersession -----------------------------------------------------------------------------------------------


def test_v5_opens_only_while_v1_to_v4_are_unopened(tmp_path):
    vault = _vault(tmp_path / "vault")
    _seed_v3_v4(vault)
    vault.add_file(v4.WITNESS_PATH, v4.REGISTERED_ANCHOR_LINE + b"\n" + b'{"head_sha256":"' + b"e" * 64
                   + b'","prev_anchor_sha256":"x","records":4,"run_at":"2026-10-01T00:00:00+00:00"}\n')
    _register(tmp_path / "reg", v5)
    vault.publish(tmp_path / "reg")
    v5.freeze_inputs(tmp_path / "reg", NOW, _inputs())
    with pytest.raises(PermissionError, match="VS1 registry was opened"):
        v5.open_discovery(tmp_path / "reg", NOW, _observed(_inputs()), vault.witness())


def test_v4_refuses_on_its_pin():
    from analysis import panel_insider_density_v6 as v6

    assert v4.SUPERSEDED_BY["version"] == v5.SUPERSEDED_BY["version"] == "vs1-v6"
    assert v4.SUPERSEDED_BY["registry_head_sha256"] == v6.REGISTERED_RECORD_SHA256[1]
    with pytest.raises(PermissionError, match="superseded by vs1-v6"):
        v1.refuse_superseded(4, v4.SUPERSEDED_BY)
    assert json.loads(v5.REGISTERED_ANCHOR_LINE)["records"] == 2
