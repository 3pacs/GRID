"""Unit tests for ingestion/altdata/social_port_activity.py."""

from __future__ import annotations

import dataclasses
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.social_port_activity import (
    ALL_SERIES_SUFFIXES,
    COMPOSITE_WEIGHTS,
    NITTER_INSTANCES,
    PORT_SPECS,
    REDDIT_SUBREDDITS,
    SERIES_BILIBILI,
    SERIES_COMPOSITE,
    SERIES_NITTER,
    SERIES_PREFIX,
    SERIES_REDDIT,
    SERIES_YOUTUBE,
    SocialActivitySnapshot,
    SocialPortActivityPuller,
    SocialPortSpec,
    _fetch_bilibili_counts,
    _fetch_nitter_counts,
    _fetch_reddit_counts,
    _fetch_youtube_counts,
    _series_id,
    compute_composite_velocity,
    run_social_port_activity_puller,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_engine():
    """SQLAlchemy engine mock that yields source_id=42."""
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.connect.return_value = ctx
    engine.begin.return_value = ctx

    row_mock = MagicMock()
    row_mock.__getitem__ = lambda self, idx: 42
    conn.execute.return_value.fetchone.return_value = row_mock
    conn.execute.return_value.fetchall.return_value = []

    return engine


@pytest.fixture
def puller(mock_engine):
    # Force YOUTUBE_API_KEY unset so youtube path is deterministic.
    with patch.dict("os.environ", {}, clear=False):
        import os as _os
        _os.environ.pop("YOUTUBE_API_KEY", None)
        return SocialPortActivityPuller(mock_engine)


def _fake_reddit_payload(n: int) -> dict:
    """Build a fake Reddit search JSON payload with n items."""
    return {
        "kind": "Listing",
        "data": {
            "after": None,
            "dist": n,
            "children": [
                {
                    "kind": "t3",
                    "data": {
                        "title": f"port post {i}",
                        "created_utc": 9999999999,  # far future → inside window
                    },
                }
                for i in range(n)
            ],
        },
    }


def _make_response(
    status_code: int = 200,
    json_payload: dict | None = None,
    text: str = "",
) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    if json_payload is not None:
        resp.json.return_value = json_payload
    else:
        resp.json.side_effect = ValueError("no json")
    resp.text = text
    return resp


# ---------------------------------------------------------------------------
# PORT_SPECS invariants
# ---------------------------------------------------------------------------


class TestPortSpecs:
    def test_exactly_fifteen_ports(self):
        assert len(PORT_SPECS) == 15

    def test_slugs_unique(self):
        slugs = [p.slug for p in PORT_SPECS]
        assert len(set(slugs)) == len(slugs)

    def test_keywords_non_empty(self):
        for p in PORT_SPECS:
            assert isinstance(p.search_keywords, tuple)
            assert len(p.search_keywords) >= 1
            for kw in p.search_keywords:
                assert isinstance(kw, str) and kw.strip()

    def test_chinese_ports_have_chinese_names(self):
        chinese_slugs = {"qingdao", "shanghai", "ningbo", "tianjin"}
        for p in PORT_SPECS:
            if p.slug in chinese_slugs:
                assert p.chinese_name is not None
            else:
                assert p.chinese_name is None

    def test_four_chinese_ports(self):
        chinese = [p for p in PORT_SPECS if p.chinese_name is not None]
        assert len(chinese) == 4


# ---------------------------------------------------------------------------
# SocialActivitySnapshot invariants
# ---------------------------------------------------------------------------


class TestSnapshot:
    def test_frozen(self):
        snap = SocialActivitySnapshot(
            date=date(2026, 4, 13),
            port_slug="la",
            reddit_post_count=3,
            youtube_video_count=5,
            nitter_tweet_count=2,
            bilibili_video_count=None,
            composite_velocity=14.0,
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            snap.reddit_post_count = 99  # type: ignore[misc]

    def test_composite_non_negative(self):
        snap = SocialActivitySnapshot(
            date=date(2026, 4, 13),
            port_slug="la",
            reddit_post_count=0,
            youtube_video_count=None,
            nitter_tweet_count=None,
            bilibili_video_count=None,
            composite_velocity=0.0,
        )
        assert snap.composite_velocity >= 0


# ---------------------------------------------------------------------------
# compute_composite_velocity
# ---------------------------------------------------------------------------


class TestComputeComposite:
    def test_reddit_only(self):
        # 10 * 1.0 = 10
        assert compute_composite_velocity(10, None, None, None) == 10.0

    def test_reddit_plus_youtube(self):
        # 10*1 + 3*2 = 16
        assert compute_composite_velocity(10, 3, None, None) == 16.0

    def test_all_four_sources(self):
        # 10*1 + 3*2 + 8*0.5 + 4*3 = 10 + 6 + 4 + 12 = 32
        assert compute_composite_velocity(10, 3, 8, 4) == 32.0

    def test_all_none_except_reddit_zero(self):
        assert compute_composite_velocity(0, None, None, None) == 0.0

    def test_negative_clamped(self):
        # Defensive: negatives clamp to 0
        assert compute_composite_velocity(-5, None, None, None) == 0.0

    def test_weights_constant_exposed(self):
        assert COMPOSITE_WEIGHTS == {
            "reddit": 1.0,
            "youtube": 2.0,
            "nitter": 0.5,
            "bilibili": 3.0,
        }


# ---------------------------------------------------------------------------
# _fetch_reddit_counts
# ---------------------------------------------------------------------------


PORT_LA = PORT_SPECS[4]  # Los Angeles (non-Chinese)
PORT_SHANGHAI = PORT_SPECS[1]  # Shanghai (Chinese)


class TestFetchReddit:
    def test_happy_path(self):
        payload = _fake_reddit_payload(7)
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            return_value=_make_response(200, json_payload=payload),
        ):
            count = _fetch_reddit_counts(PORT_LA)
        assert count == 7

    def test_rate_limit_returns_none(self):
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            return_value=_make_response(429, text="rate limited"),
        ):
            count = _fetch_reddit_counts(PORT_LA)
        assert count is None

    def test_forbidden_returns_none(self):
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            return_value=_make_response(403, text="forbidden"),
        ):
            count = _fetch_reddit_counts(PORT_LA)
        assert count is None

    def test_malformed_json_returns_none(self):
        bad_resp = MagicMock()
        bad_resp.status_code = 200
        bad_resp.json.side_effect = ValueError("not json")
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            return_value=bad_resp,
        ):
            count = _fetch_reddit_counts(PORT_LA)
        assert count is None

    def test_network_exception_returns_none(self):
        import requests
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            side_effect=requests.ConnectionError("boom"),
        ):
            count = _fetch_reddit_counts(PORT_LA)
        assert count is None


# ---------------------------------------------------------------------------
# _fetch_youtube_counts
# ---------------------------------------------------------------------------


class TestFetchYoutube:
    def test_no_api_key_no_network_call(self):
        with patch(
            "ingestion.altdata.social_port_activity.requests.get"
        ) as mock_get:
            count = _fetch_youtube_counts(PORT_LA, api_key=None)
        assert count is None
        mock_get.assert_not_called()

    def test_with_api_key_happy_path(self):
        payload = {
            "kind": "youtube#searchListResponse",
            "items": [{"id": f"vid{i}"} for i in range(12)],
            "pageInfo": {"totalResults": 12, "resultsPerPage": 12},
        }
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            return_value=_make_response(200, json_payload=payload),
        ):
            count = _fetch_youtube_counts(PORT_LA, api_key="FAKE-KEY")
        assert count == 12

    def test_with_api_key_http_error(self):
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            return_value=_make_response(403, text="quota exceeded"),
        ):
            count = _fetch_youtube_counts(PORT_LA, api_key="FAKE-KEY")
        assert count is None


# ---------------------------------------------------------------------------
# _fetch_nitter_counts
# ---------------------------------------------------------------------------


_NITTER_HTML = """
<html><body>
<div class="timeline-item">tweet 1</div>
<div class="timeline-item">tweet 2</div>
<div class="timeline-item">tweet 3</div>
</body></html>
"""


class TestFetchNitter:
    def test_first_instance_succeeds(self):
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            return_value=_make_response(200, json_payload=None, text=_NITTER_HTML),
        ):
            count = _fetch_nitter_counts(PORT_LA, instances=list(NITTER_INSTANCES))
        assert count == 3

    def test_first_instance_fails_falls_through(self):
        import requests as _rq

        call_count = {"n": 0}

        def side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise _rq.ConnectionError("first down")
            return _make_response(200, json_payload=None, text=_NITTER_HTML)

        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            side_effect=side_effect,
        ):
            count = _fetch_nitter_counts(PORT_LA, instances=["https://a", "https://b"])

        assert count == 3
        assert call_count["n"] == 2  # exactly 2 tries

    def test_all_instances_fail(self):
        import requests as _rq
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            side_effect=_rq.ConnectionError("down"),
        ):
            count = _fetch_nitter_counts(
                PORT_LA, instances=["https://a", "https://b", "https://c"]
            )
        assert count is None

    def test_http_error_on_all_instances(self):
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            return_value=_make_response(502, text="bad gateway"),
        ):
            count = _fetch_nitter_counts(
                PORT_LA, instances=["https://a", "https://b"]
            )
        assert count is None


# ---------------------------------------------------------------------------
# _fetch_bilibili_counts
# ---------------------------------------------------------------------------


_BILIBILI_HTML = """
<html><body>
<a class="bili-video-card__cover" href="/video/BVabc123">vid1</a>
<a class="bili-video-card__cover" href="/video/BVdef456">vid2</a>
</body></html>
"""


class TestFetchBilibili:
    def test_non_chinese_port_short_circuits(self):
        with patch(
            "ingestion.altdata.social_port_activity.requests.get"
        ) as mock_get:
            count = _fetch_bilibili_counts(PORT_LA)
        assert count is None
        mock_get.assert_not_called()

    def test_chinese_port_happy_path(self):
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            return_value=_make_response(200, json_payload=None, text=_BILIBILI_HTML),
        ):
            count = _fetch_bilibili_counts(PORT_SHANGHAI)
        assert count is not None
        assert count >= 1  # 2 cards → halved ≈ 2

    def test_chinese_port_http_error_returns_none(self):
        with patch(
            "ingestion.altdata.social_port_activity.requests.get",
            return_value=_make_response(500, text="oops"),
        ):
            count = _fetch_bilibili_counts(PORT_SHANGHAI)
        assert count is None


# ---------------------------------------------------------------------------
# run_social_port_activity_puller — full path
# ---------------------------------------------------------------------------


class TestRunPuller:
    def test_happy_path_all_sources(self, mock_engine):
        """All four sources return counts → 15 snapshots, 75 rows (15×5 series)."""
        payload = _fake_reddit_payload(4)

        def fake_get(url, *args, **kwargs):
            if "reddit.com" in url:
                return _make_response(200, json_payload=payload)
            if "googleapis.com" in url:
                return _make_response(
                    200,
                    json_payload={"items": [{"id": f"v{i}"} for i in range(3)]},
                )
            if "bilibili.com" in url:
                return _make_response(200, json_payload=None, text=_BILIBILI_HTML)
            # nitter
            return _make_response(200, json_payload=None, text=_NITTER_HTML)

        with patch.dict("os.environ", {"YOUTUBE_API_KEY": "fake-key"}), patch(
            "ingestion.altdata.social_port_activity.requests.get",
            side_effect=fake_get,
        ), patch(
            "ingestion.altdata.social_port_activity._polite_sleep"
        ), patch.object(
            SocialPortActivityPuller, "_get_existing_dates", return_value=set()
        ), patch.object(
            SocialPortActivityPuller, "_insert_raw"
        ) as mock_insert:
            result = run_social_port_activity_puller(mock_engine)

        assert result["fetched"] == 15
        assert result["ports_done"] == 15
        # 15 ports × 5 series = 75 inserts
        assert result["inserted"] == 75
        assert mock_insert.call_count == 75

        # Source mix: reddit/youtube/nitter fire for every port; bilibili
        # fires only for the 4 Chinese ports.
        assert result["source_mix"]["reddit"] == 15
        assert result["source_mix"]["youtube"] == 15
        assert result["source_mix"]["nitter"] == 15
        assert result["source_mix"]["bilibili"] == 4

    def test_all_sources_failing_still_produces_snapshots(self, mock_engine):
        """Every source explodes → 15 snapshots with composite=0, no crash."""
        import requests as _rq

        with patch.dict("os.environ", {}, clear=False):
            import os as _os
            _os.environ.pop("YOUTUBE_API_KEY", None)
            with patch(
                "ingestion.altdata.social_port_activity.requests.get",
                side_effect=_rq.ConnectionError("everything down"),
            ), patch(
                "ingestion.altdata.social_port_activity._polite_sleep"
            ), patch.object(
                SocialPortActivityPuller, "_get_existing_dates", return_value=set()
            ), patch.object(
                SocialPortActivityPuller, "_insert_raw"
            ) as mock_insert:
                result = run_social_port_activity_puller(mock_engine)

        assert result["fetched"] == 15
        assert result["ports_done"] == 15
        # All 75 rows still insert (reddit=0, youtube=0, nitter=0, bili=0, composite=0)
        assert result["inserted"] == 75
        assert mock_insert.call_count == 75
        assert result["source_mix"]["reddit"] == 0
        assert result["source_mix"]["youtube"] == 0
        assert result["source_mix"]["nitter"] == 0
        assert result["source_mix"]["bilibili"] == 0

        # Confirm every composite row got value=0.0
        composite_calls = [
            c for c in mock_insert.call_args_list
            if SERIES_COMPOSITE in c.kwargs["series_id"]
        ]
        assert len(composite_calls) == 15
        for call in composite_calls:
            assert call.kwargs["value"] == 0.0

    def test_idempotent_rerun_same_date(self, mock_engine):
        """Existing date set for every series → zero new inserts."""
        today = date.today()
        existing = {today}

        payload = _fake_reddit_payload(2)

        def fake_get(url, *args, **kwargs):
            if "reddit.com" in url:
                return _make_response(200, json_payload=payload)
            return _make_response(200, json_payload=None, text=_NITTER_HTML)

        with patch.dict("os.environ", {}, clear=False):
            import os as _os
            _os.environ.pop("YOUTUBE_API_KEY", None)
            with patch(
                "ingestion.altdata.social_port_activity.requests.get",
                side_effect=fake_get,
            ), patch(
                "ingestion.altdata.social_port_activity._polite_sleep"
            ), patch.object(
                SocialPortActivityPuller, "_get_existing_dates", return_value=existing
            ), patch.object(
                SocialPortActivityPuller, "_insert_raw"
            ) as mock_insert:
                result = run_social_port_activity_puller(mock_engine)

        assert result["fetched"] == 15
        assert result["inserted"] == 0
        mock_insert.assert_not_called()


# ---------------------------------------------------------------------------
# Series namespace + constants
# ---------------------------------------------------------------------------


class TestSeriesNamespaces:
    def test_prefix_and_suffixes(self):
        assert SERIES_PREFIX == "social_port"
        assert SERIES_REDDIT == "reddit_posts"
        assert SERIES_YOUTUBE == "youtube_videos"
        assert SERIES_NITTER == "nitter_tweets"
        assert SERIES_BILIBILI == "bilibili_videos"
        assert SERIES_COMPOSITE == "composite_velocity"

    def test_all_suffixes_exported(self):
        assert set(ALL_SERIES_SUFFIXES) == {
            SERIES_REDDIT,
            SERIES_YOUTUBE,
            SERIES_NITTER,
            SERIES_BILIBILI,
            SERIES_COMPOSITE,
        }
        assert len(ALL_SERIES_SUFFIXES) == 5

    def test_series_id_helper(self):
        assert _series_id(SERIES_REDDIT, "la") == "social_port:reddit_posts:la"
        assert (
            _series_id(SERIES_COMPOSITE, "shanghai")
            == "social_port:composite_velocity:shanghai"
        )

    def test_reddit_subreddits_exposed(self):
        assert isinstance(REDDIT_SUBREDDITS, tuple)
        assert len(REDDIT_SUBREDDITS) >= 3
        for sr in REDDIT_SUBREDDITS:
            assert isinstance(sr, str) and sr.strip()

    def test_nitter_instances_exposed(self):
        assert isinstance(NITTER_INSTANCES, tuple)
        assert len(NITTER_INSTANCES) >= 2
        for inst in NITTER_INSTANCES:
            assert inst.startswith("https://")
