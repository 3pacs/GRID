"""Unit tests for ingestion/altdata/ais_ground_truth.py."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.ais_ground_truth import (
    AIS_PORTS,
    AISSnapshot,
    SOURCE_PRIORITY,
    VESSELFINDER_URL_FMT,
    _parse_vesselfinder_html,
    _round_to_bucket,
    compute_capacity_utilization,
    run_ais_ground_truth_puller,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_engine() -> MagicMock:
    """Create a mock engine that resolves source_id correctly.

    Ensures ``_resolve_source_id`` finds a row on first lookup and that
    ``_insert_raw`` / ``_bucket_row_exists`` never raise.
    """
    engine = MagicMock()

    # begin() returns a connection used for inserts and bucket-exists checks
    begin_conn = MagicMock()
    begin_result = MagicMock()
    begin_result.fetchone.return_value = None  # bucket does NOT exist
    begin_result.fetchall.return_value = []
    begin_conn.execute.return_value = begin_result
    engine.begin.return_value.__enter__ = MagicMock(return_value=begin_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # connect() is used by _resolve_source_id — must return (id,) on first call
    connect_conn = MagicMock()
    source_result = MagicMock()
    source_result.fetchone.return_value = (1,)
    connect_conn.execute.return_value = source_result
    engine.connect.return_value.__enter__ = MagicMock(return_value=connect_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    return engine


def _make_dup_engine() -> MagicMock:
    """Mock engine where bucket-exists check returns a row (duplicate)."""
    engine = MagicMock()

    begin_conn = MagicMock()
    begin_result = MagicMock()
    begin_result.fetchone.return_value = (1,)  # row exists -> dedup
    begin_result.fetchall.return_value = []
    begin_conn.execute.return_value = begin_result
    engine.begin.return_value.__enter__ = MagicMock(return_value=begin_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    connect_conn = MagicMock()
    source_result = MagicMock()
    source_result.fetchone.return_value = (1,)
    connect_conn.execute.return_value = source_result
    engine.connect.return_value.__enter__ = MagicMock(return_value=connect_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    return engine


def _canned_html(
    in_port: int = 12,
    at_anchor: int = 5,
    expected: int = 8,
    departed: int = 3,
) -> str:
    """Build a minimal VesselFinder-like HTML fixture."""
    return f"""
    <html><body>
      <section class="in-port">
        <h2>In Port</h2>
        <span class="count">{in_port} vessels in port</span>
      </section>
      <section class="at-anchor">
        <h2>At Anchor</h2>
        <span class="count">{at_anchor} vessels at anchor</span>
      </section>
      <section class="expected">
        <h2>Expected Arrivals</h2>
        <span class="count">{expected} vessels expected</span>
      </section>
      <section class="departed">
        <h2>Departed</h2>
        <span class="count">{departed} vessels departed</span>
      </section>
    </body></html>
    """


def _fake_response(text: str, status: int = 200) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = text
    r.raise_for_status = MagicMock()
    return r


# ---------------------------------------------------------------------------
# AIS_PORTS catalog
# ---------------------------------------------------------------------------


class TestAISPorts:
    def test_port_count_is_fifteen(self):
        assert len(AIS_PORTS) == 15

    def test_all_ports_have_nonempty_metadata(self):
        for port in AIS_PORTS:
            assert port.slug
            assert port.slug == port.slug.lower()
            assert port.display_name
            assert port.country
            assert len(port.country) == 2
            assert -90.0 <= port.lat <= 90.0
            assert -180.0 <= port.lng <= 180.0

    def test_bounding_box_ordering(self):
        for port in AIS_PORTS:
            min_lat, min_lng, max_lat, max_lng = port.bounding_box
            assert min_lat < max_lat, f"{port.slug}: min_lat >= max_lat"
            assert min_lng < max_lng, f"{port.slug}: min_lng >= max_lng"

    def test_port_slugs_unique(self):
        slugs = [p.slug for p in AIS_PORTS]
        assert len(slugs) == len(set(slugs))

    def test_portspec_is_frozen(self):
        port = AIS_PORTS[0]
        with pytest.raises(dataclasses.FrozenInstanceError):
            port.slug = "mutated"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# AISSnapshot dataclass
# ---------------------------------------------------------------------------


class TestAISSnapshot:
    def test_snapshot_is_frozen(self):
        snap = AISSnapshot(
            timestamp=datetime.now(timezone.utc),
            port_slug="qingdao",
            ships_at_berth=10,
            ships_at_anchor=5,
            ships_expected=3,
            ships_departed_24h=2,
            capacity_utilization=0.66,
            source="vesselfinder",
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            snap.ships_at_berth = 99  # type: ignore[misc]

    def test_capacity_utilization_bounds(self):
        snap = AISSnapshot(
            timestamp=datetime.now(timezone.utc),
            port_slug="qingdao",
            ships_at_berth=10,
            ships_at_anchor=5,
            ships_expected=0,
            ships_departed_24h=0,
            capacity_utilization=0.66,
            source="vesselfinder",
        )
        assert 0.0 <= snap.capacity_utilization <= 1.0


# ---------------------------------------------------------------------------
# compute_capacity_utilization
# ---------------------------------------------------------------------------


class TestComputeCapacityUtilization:
    def test_happy_path(self):
        assert compute_capacity_utilization(6, 4) == 0.6

    def test_both_zero_returns_none(self):
        assert compute_capacity_utilization(0, 0) is None

    def test_only_at_berth_is_one(self):
        assert compute_capacity_utilization(5, 0) == 1.0

    def test_only_at_anchor_is_zero(self):
        assert compute_capacity_utilization(0, 5) == 0.0

    def test_negative_returns_none(self):
        assert compute_capacity_utilization(-1, 3) is None


# ---------------------------------------------------------------------------
# _round_to_bucket
# ---------------------------------------------------------------------------


class TestRoundToBucket:
    def test_rounds_down(self):
        ts = datetime(2026, 4, 13, 7, 34, 12, tzinfo=timezone.utc)
        out = _round_to_bucket(ts)
        assert out.hour == 4
        assert out.minute == 0
        assert out.second == 0

    def test_handles_naive_as_utc(self):
        ts = datetime(2026, 4, 13, 14, 0, 0)
        out = _round_to_bucket(ts)
        assert out.tzinfo is not None
        assert out.hour == 12


# ---------------------------------------------------------------------------
# _parse_vesselfinder_html
# ---------------------------------------------------------------------------


class TestParseVesselfinderHtml:
    def test_parses_in_port_count(self):
        port = AIS_PORTS[0]
        html = _canned_html(in_port=12, at_anchor=0, expected=0, departed=0)
        snap = _parse_vesselfinder_html(html, port)
        assert snap is not None
        assert snap.ships_at_berth == 12
        assert snap.port_slug == port.slug

    def test_empty_html_returns_none(self):
        assert _parse_vesselfinder_html("", AIS_PORTS[0]) is None

    def test_non_string_returns_none(self):
        assert _parse_vesselfinder_html(None, AIS_PORTS[0]) is None  # type: ignore[arg-type]

    def test_missing_sections_returns_none(self):
        html = "<html><body><h1>No data here</h1></body></html>"
        assert _parse_vesselfinder_html(html, AIS_PORTS[0]) is None

    def test_all_three_sections_populate(self):
        port = AIS_PORTS[1]  # shanghai
        html = _canned_html(in_port=20, at_anchor=7, expected=11, departed=4)
        snap = _parse_vesselfinder_html(html, port)
        assert snap is not None
        assert snap.ships_at_berth == 20
        assert snap.ships_at_anchor == 7
        assert snap.ships_expected == 11
        assert snap.ships_departed_24h == 4
        assert snap.capacity_utilization is not None
        assert 0.0 <= snap.capacity_utilization <= 1.0
        assert snap.source == "vesselfinder"

    def test_malformed_html_no_crash(self):
        html = "<html><body><section>In port: not-a-number</section></body></html>"
        # Should not raise; may return None
        snap = _parse_vesselfinder_html(html, AIS_PORTS[0])
        assert snap is None or isinstance(snap, AISSnapshot)


# ---------------------------------------------------------------------------
# run_ais_ground_truth_puller
# ---------------------------------------------------------------------------


class TestRunAISGroundTruthPuller:
    def test_happy_path_all_ports_succeed(self):
        engine = _make_mock_engine()
        html = _canned_html(in_port=10, at_anchor=4, expected=6, departed=2)
        with patch(
            "ingestion.altdata.ais_ground_truth.requests.get",
            return_value=_fake_response(html, 200),
        ), patch(
            "ingestion.altdata.ais_ground_truth.time.sleep",
            lambda *a, **k: None,
        ):
            result = run_ais_ground_truth_puller(engine)

        assert result["fetched"] == 15
        assert len(result["ports_scraped"]) == 15
        assert result["ports_failed"] == []
        # 5 series per snapshot (at_berth, at_anchor, expected, departed, util)
        assert result["inserted"] == 15 * 5
        assert result["source_mix"]["vesselfinder"] == 15
        assert result["source_mix"]["aishub"] == 0
        assert result["source_mix"]["none"] == 0

    def test_half_ports_failing(self):
        engine = _make_mock_engine()
        html = _canned_html(in_port=10, at_anchor=4, expected=6, departed=2)
        call_count = {"i": 0}

        def flaky_get(*args, **kwargs):
            call_count["i"] += 1
            if call_count["i"] % 2 == 0:
                return _fake_response("<html>empty</html>", 200)
            return _fake_response(html, 200)

        with patch(
            "ingestion.altdata.ais_ground_truth.requests.get",
            side_effect=flaky_get,
        ), patch(
            "ingestion.altdata.ais_ground_truth.time.sleep",
            lambda *a, **k: None,
        ):
            # AISHub path is skipped (no key), so failing requests == failed ports
            import os

            os.environ.pop("AISHUB_API_KEY", None)
            result = run_ais_ground_truth_puller(engine)

        assert result["fetched"] < 15
        assert result["fetched"] > 0
        assert len(result["ports_failed"]) > 0
        assert len(result["ports_scraped"]) == result["fetched"]
        assert result["inserted"] == result["fetched"] * 5
        assert result["source_mix"]["none"] == len(result["ports_failed"])

    def test_all_ports_failing_does_not_crash(self):
        engine = _make_mock_engine()
        with patch(
            "ingestion.altdata.ais_ground_truth.requests.get",
            return_value=_fake_response("<html></html>", 500),
        ), patch(
            "ingestion.altdata.ais_ground_truth.time.sleep",
            lambda *a, **k: None,
        ):
            import os

            os.environ.pop("AISHUB_API_KEY", None)
            result = run_ais_ground_truth_puller(engine)

        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert result["ports_scraped"] == []
        assert len(result["ports_failed"]) == 15
        assert result["source_mix"]["vesselfinder"] == 0
        assert result["source_mix"]["aishub"] == 0
        assert result["source_mix"]["none"] == 15

    def test_aishub_key_unset_does_not_crash(self):
        import os

        os.environ.pop("AISHUB_API_KEY", None)
        engine = _make_mock_engine()
        # VesselFinder returns nothing, AISHub path should gracefully skip
        with patch(
            "ingestion.altdata.ais_ground_truth.requests.get",
            side_effect=Exception("network down"),
        ), patch(
            "ingestion.altdata.ais_ground_truth.time.sleep",
            lambda *a, **k: None,
        ):
            result = run_ais_ground_truth_puller(engine)
        # Exception path is swallowed inside pull() loop
        assert result["fetched"] == 0
        assert len(result["ports_failed"]) == 15

    def test_idempotent_rerun_skips_duplicates(self):
        """When bucket row already exists, no new inserts happen."""
        engine = _make_dup_engine()
        html = _canned_html(in_port=10, at_anchor=4, expected=6, departed=2)
        with patch(
            "ingestion.altdata.ais_ground_truth.requests.get",
            return_value=_fake_response(html, 200),
        ), patch(
            "ingestion.altdata.ais_ground_truth.time.sleep",
            lambda *a, **k: None,
        ):
            result = run_ais_ground_truth_puller(engine)

        # Still fetched 15, but inserted 0 because bucket rows "exist"
        assert result["fetched"] == 15
        assert result["inserted"] == 0


# ---------------------------------------------------------------------------
# Constants sanity
# ---------------------------------------------------------------------------


class TestConstants:
    def test_vesselfinder_url_template(self):
        url = VESSELFINDER_URL_FMT.format(code="CNQIN001")
        assert url.startswith("https://")
        assert "vesselfinder.com" in url
        assert "CNQIN001" in url

    def test_source_priority_order(self):
        assert SOURCE_PRIORITY[0] == "vesselfinder"
        assert "aishub" in SOURCE_PRIORITY

    def test_regional_coverage(self):
        """Must cover China, US, EU, SE-Asia, ME, Taiwan."""
        countries = {p.country for p in AIS_PORTS}
        assert "CN" in countries
        assert "US" in countries
        assert "NL" in countries or "DE" in countries or "BE" in countries
        assert "SG" in countries or "MY" in countries
        assert "AE" in countries or "SA" in countries
        assert "TW" in countries
