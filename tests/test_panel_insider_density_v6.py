"""Tests for the VS1 v6 harness (``analysis/panel_insider_density_v6.py``): C1 applied to prices.

Synthetic data only: no production DB, no network, no price or outcome of any real issuer.
"""

from __future__ import annotations

import json
from datetime import date

import numpy as np
import pandas as pd
import pytest

from analysis import panel_insider_density as v1
from analysis import panel_insider_density_v2 as v2
from analysis import panel_insider_density_v5 as v5
from analysis import panel_insider_density_v6 as v6
from tests.test_panel_insider_density import NOW, _row, _submission
from tests.test_panel_insider_density_v2 import _discovery_key, _inputs, _observed, _register, _universe_frame, _Vault


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
    vault = _Vault(root, h=v6, seeds=("vs1-v1", "vs1-v2"), **kw)
    from analysis import panel_insider_density_v3 as v3
    from analysis import panel_insider_density_v4 as v4

    for m in (v3, v4, v5):
        vault.add_file(m.WITNESS_PATH, m.REGISTERED_ANCHOR_LINE + b"\n")
    return vault


def test_v6_prereg_hashes_and_cites_c1_with_one_interval():
    assert v6.check_prereg() == v6.PREREG_BODY_SHA256 != v5.PREREG_BODY_SHA256
    body = v1.prereg_body((v6.REPO / v6.PREREG_PATH).read_text(encoding="utf-8"))
    assert "candidate amendment C1" in body and "vs1-v2-candidate-amendments.md" in body
    assert "It is the **one** interval definition in this text" in body
    assert "No second interval, start date or bound is defined anywhere" in body
    section = body[body.index("**Ticker-reuse price bound (v6"):body.index("- **Tiingo metadata.**")]
    assert "L_T" not in section and "F_{e,T}" not in section
    for pin in (v5.PREREG_BODY_SHA256, v5.REGISTERED_RECORD_SHA256[1]):
        assert pin in body


def test_v6_is_v5_except_the_price_interval():
    assert v6.splice_check is v5.splice_check and v6.name_match is v5.name_match
    assert v6.CROSSCHECK is v5.CROSSCHECK and v6.PRIMARY_TRIAL == "A90|fwd5"
    assert v6.V6.pins.number == 6 and v6.WITNESS_PATH == v1.canonical_witness_path("vs1-v6")
    assert [e.version for e in v6.V6.pins.earlier] == [f"vs1-v{n}" for n in range(1, 6)]
    assert not hasattr(v6, "listing_bound")
    records = v6.registration_records(v6.REGISTERED_AT, v6.REGISTERED_CODE_SHA)
    assert tuple(v1.chained_sha256(records)) == v6.REGISTERED_RECORD_SHA256
    assert "C1" in records[1]["price_admission"]["ticker_reuse_bound"]


def _panel_inputs(tmp_path):
    """22 issuers; issuer 1000 filed as OLD until 2014-06, then T00 (today's ticker), then OLD again in 2017-2018."""
    rng = np.random.default_rng(5)
    tickers = [f"T{i:02d}" for i in range(22)]
    ciks = list(range(1000, 1022))
    dates = pd.bdate_range("2011-11-01", "2019-12-31")
    prices = pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0, 0.01, (len(dates), 23)), axis=0)),
                          index=dates, columns=tickers + ["XLK"])
    kaggle = {"T00": prices["T00"] * 1.5}  # refused rows under the same series id: ignored by every read
    engine = _tiingo_db({c: prices[c] for c in prices.columns}, kaggle)

    def ticker_of(cik, d):
        if cik == 1000 and (d < pd.Timestamp("2014-06-02") or pd.Timestamp("2017-03-01") <= d < pd.Timestamp("2018-03-01")):
            return "OLD"
        return tickers[cik - 1000]

    rows = [_row(accession_number=f"p-{c}-{i}", issuer_cik=str(c), owner_cik="7",
                 filing_date=str(dates[i + 1].date()), transaction_date=str(dates[i].date()))
            for c in ciks for i in range(100, len(dates) - 2, 150)]
    subs = [{**_submission(accession_number=r["accession_number"], filing_date=r["filing_date"],
                           issuer_cik=r["issuer_cik"], owner_cik="7"),
             "issuer_ticker": ticker_of(int(r["issuer_cik"]), pd.Timestamp(r["filing_date"]))} for r in rows]
    subs += [{**_submission(accession_number=f"h-{c}-{d.date()}", issuer_cik=str(c), filing_date=str(d.date()),
                            document_type="4", owner_cik="9"), "issuer_ticker": ticker_of(c, d)}
             for c in ciks for d in dates[::20]]
    tx = pd.DataFrame(rows).astype("string")
    tx.columns = [c.upper() for c in tx.columns]
    submissions = pd.DataFrame(subs)
    events = v1.build_events(tx, submissions=submissions.rename(columns=str.upper))
    universe = _universe_frame([(t, c, [t]) for t, c in zip(tickers, ciks)])
    admission = v2.build_admission(submissions, universe)
    manifest = v6.PriceManifest(source="TIINGO", series_template="YF:{ticker}:adj_close",
                                basis="split+dividend adjusted", benchmark="XLK",
                                admitted=tuple(sorted(tickers + ["XLK"])), probe_report_sha256="a" * 64,
                                crosscheck_report_sha256="c" * 64, tiingo_meta_report_sha256="d" * 64,
                                listed_from=tuple((t, "2012-06-01" if t == "T01" else "2011-11-01") for t in tickers))
    return engine, events, admission, universe, manifest


def test_prices_outside_the_c1_interval_are_blanked_with_the_same_mask_as_features(tmp_path):
    engine, events, admission, universe, manifest = _panel_inputs(tmp_path)
    vault = _vault(tmp_path / "vault")
    key, _ = _discovery_key(tmp_path / "reg", vault, manifest)
    with engine.connect() as conn:
        panel = v6.load_price_panel(conn, manifest, list(universe["ticker"]), start=date(2011, 11, 1),
                                    as_of=date(2019, 12, 31), window="discovery", key=key)
    panels = v6.build_trial_panels(events, admission, universe, panel, "discovery")
    p = panels["A90|fwd5"]
    decided = pd.DatetimeIndex(p.decision_at)
    j = p.entities.index("T00")
    # one interval: wherever the (rule 3) feature mask abstains, the label has no close either
    inside = v2.ticker_mask(admission, [1000], decided)[1000].to_numpy()
    assert np.isnan(p.label[~inside, j]).all() and np.isnan(p.feature[~inside, j]).all()
    before = decided < pd.Timestamp("2014-06-01", tz="UTC")
    gap = (decided > pd.Timestamp("2017-03-10", tz="UTC")) & (decided < pd.Timestamp("2018-02-20", tz="UTC"))
    after = decided > pd.Timestamp("2018-04-01", tz="UTC")
    assert np.isnan(p.label[before, j]).all() and np.isnan(p.label[gap, j]).all()
    assert np.isfinite(p.label[after, j]).sum() > 0
    k = p.entities.index("T01")  # Tiingo startDate bound (listed_from) also blanks
    assert np.isnan(p.label[decided < pd.Timestamp("2012-05-25", tz="UTC"), k]).all()
    assert np.isfinite(p.label[decided > pd.Timestamp("2012-07-01", tz="UTC"), k]).sum() > 0
    other = p.entities.index("T02")
    assert np.isfinite(p.label[before, other]).sum() > 0


def test_v5_would_have_used_the_gap_closes_v6_does_not(tmp_path):
    """v5's start-only L_T left the 2017-2018 re-use gap's closes in labels; the single C1 interval does not."""
    engine, events, admission, universe, manifest = _panel_inputs(tmp_path)
    vault = _vault(tmp_path / "vault")
    key, _ = _discovery_key(tmp_path / "reg", vault, manifest)
    with engine.connect() as conn:
        panel = v6.load_price_panel(conn, manifest, list(universe["ticker"]), start=date(2011, 11, 1),
                                    as_of=date(2019, 12, 31), window="discovery", key=key)
    start_only = v2.build_trial_panels(events, admission, universe, panel, "discovery", blank_before_listing=True)
    c1 = v6.build_trial_panels(events, admission, universe, panel, "discovery")
    decided = pd.DatetimeIndex(c1["A90|fwd20"].decision_at)
    j = c1["A90|fwd20"].entities.index("T00")
    gap = (decided > pd.Timestamp("2017-02-01", tz="UTC")) & (decided < pd.Timestamp("2018-02-20", tz="UTC"))
    assert np.isfinite(start_only["A90|fwd20"].label[gap, j]).sum() > 0
    assert np.isnan(c1["A90|fwd20"].label[gap, j]).all()


def test_v6_refuses_while_v5_is_opened(tmp_path):
    vault = _vault(tmp_path / "vault")
    vault.add_file(v5.WITNESS_PATH, v5.REGISTERED_ANCHOR_LINE + b"\n" + b'{"head_sha256":"' + b"e" * 64
                   + b'","prev_anchor_sha256":"x","records":4,"run_at":"2026-10-01T00:00:00+00:00"}\n')
    _register(tmp_path / "reg", v6)
    vault.publish(tmp_path / "reg")
    v6.freeze_inputs(tmp_path / "reg", NOW, _inputs())
    with pytest.raises(PermissionError, match="VS1 registry was opened"):
        v6.open_discovery(tmp_path / "reg", NOW, _observed(_inputs()), vault.witness())


def test_v5_refuses_on_its_pin():
    assert v5.SUPERSEDED_BY["version"] == "vs1-v6"
    assert v5.SUPERSEDED_BY["registry_head_sha256"] == v6.REGISTERED_RECORD_SHA256[1]
    with pytest.raises(PermissionError, match="superseded by vs1-v6"):
        v1.refuse_superseded(5, v5.SUPERSEDED_BY)
    assert json.loads(v6.REGISTERED_ANCHOR_LINE)["records"] == 2
