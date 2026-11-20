"""Anomaly report: fault detection, verdicts, precedents, and the e-mail built from it.

The fixtures reproduce what the live scan showed on 2026-11-13: an unadjusted 3-for-1 split read as a
-4 sigma "anomaly", an options feed returning ~2 % of normal volume, and a real 25 % decline ranked below both.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from alerts import email as grid_email
from intelligence import anomaly_report as ar

DAYS = pd.bdate_range("2023-01-02", periods=900)


def _walk(seed: int, start: float = 100.0, vol: float = 0.01, n: int = len(DAYS)) -> pd.Series:
    rng = np.random.default_rng(seed)
    return pd.Series(start * np.exp(np.cumsum(rng.normal(0.0003, vol, n))), index=DAYS[:n])


def _scan_row(name: str, series: pd.Series, window: int = 175) -> dict:
    """What ``_compute_anomalies`` would emit for ``series`` (mean/std over the scan window)."""
    w = series.dropna().tail(window)
    z = (w.iloc[-1] - w.mean()) / w.std()
    return {"feature": name, "current_value": round(float(w.iloc[-1]), 4), "zscore": round(float(z), 3),
            "historical_mean": round(float(w.mean()), 4), "historical_std": round(float(w.std()), 4),
            "severity": "extreme" if abs(z) > 3 else "moderate", "broken_correlations": []}


@pytest.fixture()
def market() -> dict:
    split = _walk(1, 210.0, 0.008)
    split.iloc[-7:] = split.iloc[-7:] / 3.0           # 3-for-1, never re-based

    fall = _walk(2, 200.0, 0.012)
    fall.iloc[-13:] = fall.iloc[-14] * np.linspace(0.98, 0.75, 13)   # -25 % in 13 sessions

    rng3 = np.random.default_rng(3)
    drift = pd.Series(500.0 + rng3.normal(0.0, 3.0, len(DAYS)), index=DAYS)
    drift.iloc[-8:] += 60.0                           # stepped up eight sessions ago and stayed there

    spy = _walk(4, 500.0, 0.008)
    frame = pd.DataFrame({"ita": split, "nvda_full": fall, "brk-b_full": drift, "spy_full": spy})

    rng = np.random.default_rng(5)
    tickers = ["blk", "gd", "ma", "lly", "jpm", "gs", "tmo", "dvn"]
    for i, t in enumerate(tickers):
        vol = pd.Series(rng.normal(5000 + 400 * i, 400, len(DAYS)).clip(min=500), index=DAYS)
        vol.iloc[-5:] = vol.iloc[-5:] * 0.02          # the feed breaks five sessions ago
        frame[f"{t}_opt_vol"] = vol
        frame[f"{t}_full"] = _walk(10 + i, 100.0 + i)
    conc = pd.Series(rng.normal(0.015, 0.002, len(DAYS)), index=DAYS)
    conc.iloc[-3:] = 0.067                            # options-derived, jumps after the feed broke
    frame["tmo_oi_conc"] = conc

    registry = pd.DataFrame(
        [("ita", "breadth", "iShares US Aerospace Defense ETF"), ("nvda_full", "equity", "NVDA Full History"),
         ("brk-b_full", "equity", "BRK-B Full History"), ("spy_full", "equity", "Close SPY"),
         ("tmo_oi_conc", "sentiment", "TMO OI Concentration")]
        + [(f"{t}_opt_vol", "sentiment", f"{t.upper()} Total Options Volume") for t in tickers]
        + [(f"{t}_full", "equity", f"{t.upper()} Full History") for t in tickers],
        columns=["name", "family", "description"],
    )
    flagged = ["ita", "tmo_oi_conc", "blk_opt_vol", "gd_opt_vol", "brk-b_full", "nvda_full"]
    scan = sorted((_scan_row(n, frame[n]) for n in flagged), key=lambda a: -abs(a["zscore"]))
    return {"frame": frame, "registry": registry, "scan": scan}


def _report(market: dict) -> dict:
    return ar.build_report(market["scan"], market["frame"].tail(350), history=market["frame"],
                           registry=market["registry"], as_of=date(2026, 6, 12), features_scanned=22)


def _item(report: dict, name: str) -> dict:
    return next(i for i in report["items"] if i["feature"] == name)


class TestIdentity:
    def test_kinds_tickers_and_labels(self):
        assert ar.feature_kind("blk_opt_vol", "sentiment") == "count"
        assert ar.feature_kind("spy_term_slope", "vol") == "options_derived"
        assert ar.feature_kind("nvda_full", "equity") == "price"
        assert ar.feature_kind("ita", "breadth") == "price"
        assert ar.feature_kind("gdelt_actor_powell_tone", "sentiment") == "other"
        assert ar.feature_ticker("brk_b_opt_vol") == "BRK-B"
        assert ar.feature_ticker("gdelt_actor_powell_tone") is None
        assert ar.feature_label("blk_opt_vol", "BLK Total Options Volume", "count") == "BLK options volume"
        assert ar.feature_label("nvda_full", "NVDA Full History", "price") == "NVDA price"
        assert ar.feature_label("ita", "iShares US Aerospace Defense ETF", "price") == (
            "iShares US Aerospace Defense ETF (ITA)")

    def test_linked_price_series(self):
        cols = ["brk-b_full", "spy_full", "nvda_full", "ita"]
        assert ar.linked_price_feature("brk_b_opt_vol", "count", cols) == "brk-b_full"
        assert ar.linked_price_feature("nvda_full", "price", cols) == "nvda_full"
        assert ar.linked_price_feature("wn_energy_sentiment_avg", "other", cols) == "spy_full"
        assert ar.linked_price_feature("zzz_opt_vol", "count", ["nvda_full"]) is None


class TestFaults:
    def test_unadjusted_split_is_found_with_its_ratio(self, market):
        hit = ar.split_like_jump(market["frame"]["ita"])
        assert hit["kind"] == "split" and hit["ratio"] == 3
        assert hit["before"] / hit["after"] == pytest.approx(3.0, rel=0.05)

    def test_reverse_split(self):
        s = _walk(7, 4.0, 0.01)
        s.iloc[-20:] = s.iloc[-20:] * 10.0
        hit = ar.split_like_jump(s)
        assert hit["kind"] == "reverse_split" and hit["ratio"] == 10

    def test_a_jump_by_an_uncommon_factor_is_a_basis_break_not_a_split(self, market):
        # nasdaq_full on 2026-03-23: an ETF price (582) replaced by the index level (21,947).
        s = _walk(12, 580.0, 0.008)
        s.iloc[-150:] = s.iloc[-150:] * 38.0
        hit = ar.split_like_jump(s)
        assert hit["ratio"] == 38 and hit["shape"] == "basis_break"
        text = grid_email._an_fault_text({**hit, "id": "split:nasdaq_full", "label": "NASDAQ price",
                                          "feature": "nasdaq_full"}, {"nasdaq_full": {"zscore": 3.1}})
        assert "changed by a factor of about 38" in text and "source or unit" in text and "split" not in text
        assert ar.split_like_jump(market["frame"]["ita"])["shape"] == "split"

    def test_a_crash_off_an_integer_ratio_is_not_a_split(self):
        s = _walk(8, 50.0, 0.01)
        s.iloc[-10:] = s.iloc[-10:] * 0.55            # -45 %: pre/post 1.82, not a split ratio
        assert ar.split_like_jump(s) is None

    def test_an_ordinary_series_has_no_split(self):
        assert ar.split_like_jump(_walk(9)) is None

    def test_feed_collapse_names_the_group_and_its_onset(self, market):
        faults = ar.feed_collapses(market["frame"].tail(350))
        fault = faults["_opt_vol"]
        assert fault["members"] == 8 and fault["collapsed"] == 8
        assert fault["onset"] == DAYS[-5].date().isoformat()
        assert fault["median_ratio"] < 0.05

    def test_one_quiet_member_is_not_a_feed_fault(self, market):
        frame = market["frame"].copy()
        cols = [c for c in frame.columns if c.endswith("_opt_vol")]
        late = frame.index >= DAYS[-5]
        frame.loc[late, cols] = frame.loc[late, cols] * 50.0          # undo the collapse
        frame.loc[late, "blk_opt_vol"] *= 0.02
        assert ar.feed_collapses(frame.tail(350)) == {}


class TestVerdicts:
    def test_each_flagged_feature_gets_the_right_verdict(self, market):
        report = _report(market)
        assert _item(report, "ita")["verdict"] == "data_fault"
        assert _item(report, "ita")["fault"]["kind"] == "split"
        assert _item(report, "blk_opt_vol")["verdict"] == "data_fault"
        assert _item(report, "gd_opt_vol")["fault"]["id"] == "feed:opt_vol"
        assert _item(report, "tmo_oi_conc")["verdict"] == "suspect"
        assert _item(report, "nvda_full")["verdict"] in ("sharp_move", "new_extreme")
        assert _item(report, "brk-b_full")["verdict"] == "stretched"

    def test_real_moves_sort_ahead_of_faults(self, market):
        report = _report(market)
        assert report["items"][0]["feature"] == "nvda_full"
        assert [i["verdict"] for i in report["items"]][-3:] == ["data_fault"] * 3
        assert report["counts"]["data_fault"] == 3 and report["counts"]["suspect"] == 1

    def test_move_is_measured_in_percent_with_its_start_date(self, market):
        nvda = _item(_report(market), "nvda_full")
        d20 = nvda["changes"]["d20"]
        assert d20["unit"] == "pct" and d20["value"] < -0.2
        assert d20["from_date"] == DAYS[-21].date().isoformat()
        assert nvda["excursion"]["sessions"] >= 1 and nvda["excursion"]["since"] is not None

    def test_faults_list_what_they_explain(self, market):
        report = _report(market)
        feed = next(f for f in report["faults"] if f["id"] == "feed:opt_vol")
        assert sorted(feed["flagged"]) == ["blk_opt_vol", "gd_opt_vol"]
        assert feed["suspect"] == ["tmo_oi_conc"]
        split = next(f for f in report["faults"] if f["id"] == "split:ita")
        assert split["flagged"] == ["ita"] and split["ratio"] == 3

    def test_headline_key_holds_moves_faults_and_only_far_stretched(self, market, monkeypatch):
        key = _report(market)["headline_key"]
        assert "nvda_full" in key and "split:ita" in key and "feed:opt_vol" in key
        assert "blk_opt_vol" not in key and "tmo_oi_conc" not in key
        assert "brk-b_full" in key                     # stretched, but past HEADLINE_Z
        monkeypatch.setattr(ar, "HEADLINE_Z", 10.0)
        assert "brk-b_full" not in _report(market)["headline_key"]

    def test_new_headlines_are_additions_only(self, market):
        report = _report(market)
        assert ar.new_headlines(report, report["headline_key"]) == []
        assert ar.new_headlines(report, report["headline_key"] + ["gone"]) == []
        assert ar.new_headlines(report, [k for k in report["headline_key"] if k != "nvda_full"]) == ["nvda_full"]
        assert set(ar.new_headlines(report, None)) == set(report["headline_key"])

    def test_level_is_read_on_the_features_own_latest_observation(self, market):
        # The scan row says -9 sigma; the series itself has since come back. The report reads the series.
        frame = market["frame"].copy()
        frame["calm_full"] = 100.0 + np.sin(np.arange(len(DAYS)))      # never far from its own average
        scan = [{"feature": "calm_full", "zscore": -9.0, "current_value": 1.0, "historical_mean": 100.0,
                 "historical_std": 0.7}]
        report = ar.build_report(scan, frame.tail(350), history=frame, registry=market["registry"])
        item = report["items"][0]
        assert item["verdict"] == "faded" and abs(item["zscore"]) < 2.5 and item["scan_zscore"] == -9.0
        assert item["obs_date"] == DAYS[-1].date().isoformat()
        assert item["value"] == pytest.approx(float(frame["calm_full"].iloc[-1]), rel=1e-4)
        assert "calm_full" not in report["headline_key"]
        titles = [s["title"] for s in grid_email.anomaly_report_sections(report)]
        assert "Back inside the threshold" in titles

    def test_a_daily_series_a_week_old_is_stale_and_a_monthly_one_is_not(self, market):
        frame = market["frame"].copy()
        frame["gdelt_tone"] = 100.0 + np.sin(np.arange(len(DAYS)))
        frame.loc[frame.index[-6:], "gdelt_tone"] = np.nan          # last print six sessions ago
        monthly = pd.Series(np.nan, index=DAYS)
        monthly.iloc[::21] = 3.0 + 0.01 * np.arange(len(monthly.iloc[::21]))
        frame["cpi_yoy"] = monthly
        scan = [{"feature": n, "zscore": 3.0, "current_value": 1.0, "historical_mean": 0.0, "historical_std": 1.0}
                for n in ("gdelt_tone", "cpi_yoy")]
        report = ar.build_report(scan, frame.tail(350), history=frame, registry=market["registry"])
        tone = _item(report, "gdelt_tone")
        assert tone["verdict"] == "stale" and tone["fault"]["days_behind"] >= 6
        assert _item(report, "cpi_yoy")["verdict"] != "stale"
        plain = grid_email._render_plain(grid_email.anomaly_report_sections(report))
        assert "no observation since" in plain and "carried-forward" in plain

    def test_price_items_carry_their_one_year_range(self, market):
        nvda = _item(_report(market), "nvda_full")
        assert nvda["range"]["off_high"] < -0.2 and nvda["range"]["high"] > nvda["value"]
        assert _item(_report(market), "tmo_oi_conc")["range"] is None

    def test_a_sharp_move_gets_precedents_for_the_move_itself(self, market):
        nvda = _item(_report(market), "nvda_full")
        assert nvda["verdict"] == "sharp_move"
        move = nvda["move_history"]
        assert move["basis"] in ("change_5", "change_20") and move["unit"] == "pct"
        assert move["direction"] == "below" and nvda["history"]["basis"] == "level"
        sessions = int(move["basis"].split("_")[1])
        assert move["cut"] == pytest.approx(abs(nvda["changes"][f"d{sessions}"]["value"]) * 0.8, rel=1e-3)

    def test_move_read_counts_prior_moves_of_similar_size(self):
        price = pd.Series(100.0, index=DAYS)
        level, values = 100.0, []
        drops = (200, 400, 600)                        # three earlier 20 % falls over 10 sessions each
        for i in range(len(DAYS)):
            if any(d < i <= d + 10 for d in drops) or i > len(DAYS) - 11:
                level *= 0.8 ** (1 / 10)
            elif any(d + 10 < i <= d + 40 for d in drops):
                level *= 1.25 ** (1 / 30)              # and a full recovery over the next 30
            values.append(level)
        price[:] = values
        change = {"sessions": 20, "value": float(price.iloc[-1] / price.iloc[-21] - 1.0)}
        read = ar.move_read(price, price, change, as_pct=True, target_name="x_full")
        assert read["available"] and read["n"] == 3 and read["direction"] == "below"
        assert read["horizons"]["fwd_20"]["share_up"] == 1.0
        text = grid_email._an_history(read)
        assert "3 prior times it fell 16.0% or more in 20 sessions" in text and "too few to read" in text

    def test_report_is_json_serialisable(self, market):
        import json

        json.dumps(_report(market))


class TestHistoryRead:
    def _spiky(self) -> tuple[pd.Series, pd.Series]:
        """A flat noisy feature that spikes every 100 sessions; the target gains 5 % after each spike."""
        rng = np.random.default_rng(11)
        feature = pd.Series(rng.normal(0.0, 1.0, len(DAYS)), index=DAYS)
        target = pd.Series(100.0, index=DAYS)
        level = 100.0
        spikes = list(range(300, 860, 100))
        values = []
        for i in range(len(DAYS)):
            if any(s < i <= s + 20 for s in spikes):
                level *= 1.05 ** (1 / 20)
            values.append(level)
        target[:] = values
        for s in spikes:
            feature.iloc[s] = 8.0
        return feature, target

    def test_counts_prior_crossings_and_their_forward_returns(self):
        feature, target = self._spiky()
        read = ar.history_read(feature, target, z_now=4.0, target_name="spy_full")
        assert read["available"] and read["n"] == 6 and read["enough"]
        fwd20 = read["horizons"]["fwd_20"]
        assert fwd20["n"] == 6 and fwd20["share_up"] == 1.0
        assert fwd20["median"] == pytest.approx(0.05, abs=0.002)
        assert fwd20["baseline_median"] < fwd20["median"]

    def test_the_current_excursion_is_not_its_own_precedent(self):
        feature, target = self._spiky()
        feature.iloc[-30:] = 8.0                       # still beyond today; started 30 sessions ago
        read = ar.history_read(feature, target, z_now=4.0, target_name="spy_full",
                               current_since=DAYS[-30].date().isoformat())
        assert read["n"] == 6
        assert all(e["date"] < DAYS[-30].date().isoformat() for e in read["episodes"])

    def test_direction_matters(self):
        feature, target = self._spiky()
        read = ar.history_read(feature, target, z_now=-4.0, target_name="spy_full")
        assert read["direction"] == "below"
        assert all(e["signal"] <= -2.5 for e in read["episodes"])
        spike_days = {DAYS[i].date().isoformat() for i in range(300, 860, 100)}
        assert not spike_days & {e["date"] for e in read["episodes"]}

    def test_short_history_says_so(self):
        feature, target = self._spiky()
        read = ar.history_read(feature.tail(100), target, z_now=4.0, target_name="spy_full")
        assert not read["available"] and "too short" in read["reason"]

    def test_split_in_the_price_history_blocks_the_read(self, market):
        feature, _ = self._spiky()
        read = ar.history_read(feature, market["frame"]["ita"], z_now=4.0, target_name="ita")
        assert not read["available"] and "unadjusted split" in read["reason"]

    def test_trailing_baseline_never_includes_the_value_read(self):
        # One spike in an otherwise constant-variance series: with the spike in its own baseline the z
        # would be far smaller. The trailing z must see the full size of it.
        feature, target = self._spiky()
        read = ar.history_read(feature, target, z_now=4.0, target_name="spy_full")
        assert all(e["signal"] > 6.0 for e in read["episodes"])


class TestEmail:
    def test_subject_names_the_move_and_counts_faults(self, market):
        subject = grid_email.anomaly_report_subject(_report(market))
        assert subject.startswith("GRID anomalies Jun 12: NVDA price -")
        assert "in 20 sessions" in subject and subject.endswith("; 2 data faults")

    def test_sections_put_the_move_first_and_faults_apart(self, market):
        sections = grid_email.anomaly_report_sections(_report(market))
        titles = [s["title"] for s in sections]
        assert titles[0] == "At a glance"
        assert "NVDA price" in titles[1]
        assert "Stretched, nothing new" in titles and "Data faults: not market signals" in titles
        assert titles.index("Stretched, nothing new") < titles.index("Data faults: not market signals")
        assert not any("ITA" in t or "options volume" in t for t in titles[1:2])

    def test_body_carries_the_details_and_no_escaped_markup(self, market):
        sections = grid_email.anomaly_report_sections(_report(market))
        markup = grid_email._render_html("s", sections)
        plain = grid_email._render_plain(sections)
        assert "&lt;br&gt;" not in markup and "&lt;strong&gt;" not in markup
        for text in (markup, plain):
            assert "3-for-1 split" in text
            assert "8 of 8 series have been under a fifth of their own normal" in text
            assert "TMO open-interest concentration" in text
            assert "What followed before." in text
            assert "the trend showing up in the average, not an event" in text
        assert "<br>" not in plain and "<strong>" not in plain

    def test_no_new_move_says_so(self, market):
        report = _report(market)
        report["items"] = [i for i in report["items"] if i["verdict"] not in ("sharp_move", "new_extreme")]
        assert "no new market move" in grid_email.anomaly_report_subject(report)
        assert "No new market move in this scan." in grid_email.anomaly_report_headline(report)[1]

    def test_send_renders_one_message(self, market, monkeypatch):
        sent: list[tuple[str, list[dict]]] = []
        monkeypatch.setattr(grid_email, "_send", lambda subject, sections, footer_note="": sent.append(
            (subject, sections)))
        grid_email.send_anomaly_report(_report(market))
        grid_email.send_anomaly_report({"items": []})
        assert len(sent) == 1 and sent[0][0].startswith("GRID anomalies")
