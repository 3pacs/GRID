"""Dynamic ``YF:<ticker>:<field>`` resolution in EntityMap.

~650,000 unmapped rows on griddb — 60%+ of all unmapped volume — were
per-ticker yfinance OHLCV series of the form ``YF:<TICKER>:<field>`` with
field in {open, high, low, close, volume, adj_close}.  Enumerating them in
the static dicts would have meant ~5,000 new lines.

``EntityMap.get_feature_id`` now falls back to deriving candidate feature
names from the conventions the static dicts already use, and resolves only
to names that feature_registry actually has.

Three things these tests hold down:

1. Static mappings ALWAYS win.  Nothing that resolves today changes.
2. An unregistered candidate is NEVER returned.  Returning one would be a
   ghost mapping — looks resolved, drops every row (migrations 0062/0063).
3. A miss is cached.  This path misses by design and must not re-walk the
   registry (or re-query it) per row.

The DB engine is mocked, as in tests/test_entity_map.py — the only real DB
interaction is loading feature_registry into an in-memory cache.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from normalization.entity_map import (
    SEED_MAPPINGS,
    EntityMap,
    yf_candidate_feature_names,
    yf_ticker_stems,
)


def _engine(features: list[tuple[int, str]]) -> MagicMock:
    """Mock engine whose feature_registry holds exactly ``features``."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = list(features)
    return engine


@pytest.fixture
def em() -> EntityMap:
    """EntityMap over a registry that deliberately mixes conventions.

    ``nvda_full`` uses the *_full convention; ``smh_close`` uses *_close;
    ``btc_usd_full`` uses the underscored-hyphen form; ``aapl_volume`` and
    ``aapl_open`` cover the non-price fields.  ``ftnt`` is registered under
    NEITHER convention, so every candidate for it must miss.
    """
    return EntityMap(db_engine=_engine([
        (1, "yld_curve_2s10s"),   # T10Y2Y, static
        (2, "sp500_full"),        # YF:^GSPC:close, static
        (3, "vix_spot"),          # YF:^VIX:close, static
        (10, "nvda_full"),
        (11, "smh_close"),
        (12, "btc_usd_full"),
        (13, "aapl_volume"),
        (14, "aapl_open"),
        (15, "cl_close"),
        (16, "gspc_full"),        # deliberately shadows the static YF:^GSPC
        (17, "uup_full"),         # deliberately shadows the static YF:UUP
    ]))


# ── 1. Static always wins ────────────────────────────────────────────────

class TestStaticMappingsAlwaysWin:
    def test_static_yf_close_is_untouched_by_the_pattern_path(self, em) -> None:
        # YF:^GSPC:close is static -> sp500_full (id 2).  The registry also
        # contains gspc_full (id 16), which the pattern path WOULD pick.
        # Static wins, so the answer must still be 2.
        assert SEED_MAPPINGS["YF:^GSPC:close"] == "sp500_full"
        assert em.get_feature_id("YF:^GSPC:close") == 2

    def test_static_typo_fix_target_is_not_second_guessed(self, em) -> None:
        # YF:UUP:close is deliberately mapped to uup_etf_close (the ETF price,
        # NOT dxy).  uup_etf_close is absent from this registry while uup_full
        # is present — the pattern path must not "helpfully" rescue it, or the
        # documented semantics of that mapping would silently flip.
        assert SEED_MAPPINGS["YF:UUP:close"] == "uup_etf_close"
        assert em.get_feature_id("YF:UUP:close") is None

    def test_non_yf_static_mappings_unaffected(self, em) -> None:
        assert em.get_feature_id("T10Y2Y") == 1

    def test_every_static_yf_ohlcv_mapping_resolves_as_before(self) -> None:
        """Regression net: over the whole static dict, resolution is identical
        with and without the pattern fallback.

        Registry = every static target, so each static id resolves; the
        pattern path is then given every chance to override and must not.
        """
        yf_ids = [
            sid for sid in SEED_MAPPINGS
            if yf_candidate_feature_names(sid)
        ]
        assert len(yf_ids) > 100, "static dict should cover many YF series"

        names = sorted({SEED_MAPPINGS[sid] for sid in yf_ids})
        mapper = EntityMap(db_engine=_engine(
            [(i + 1, n) for i, n in enumerate(names)]
        ))
        for sid in yf_ids:
            expected = mapper._feature_cache[SEED_MAPPINGS[sid]]
            assert mapper.get_feature_id(sid) == expected, sid
        # And nothing leaked into the pattern cache — static short-circuits.
        assert mapper._pattern_cache == {}


# ── 2. Pattern resolution, and only onto registered names ────────────────

class TestPatternResolution:
    def test_full_convention(self) -> None:
        # ~5,000 lines of dict avoided: any registered <ticker>_full now
        # resolves without an entry of its own.
        mapper = EntityMap(db_engine=_engine([(20, "ftnt_full")]))
        assert "YF:FTNT:adj_close" not in SEED_MAPPINGS
        assert mapper.get_feature_id("YF:FTNT:adj_close") == 20

    def test_close_convention_for_unlisted_ticker(self) -> None:
        mapper = EntityMap(db_engine=_engine([(20, "ftnt_full")]))
        assert "YF:FTNT:close" not in SEED_MAPPINGS
        assert mapper.get_feature_id("YF:FTNT:close") == 20
        assert mapper.get_feature_id("YF:FTNT:adj_close") == 20

    def test_falls_back_to_close_suffix_when_full_absent(self, em) -> None:
        assert "YF:SMH:adj_close" not in SEED_MAPPINGS
        assert em.get_feature_id("YF:SMH:adj_close") == 11  # smh_close

    def test_hyphen_ticker_resolves_via_underscore_stem(self) -> None:
        # zzz-usd_full is not a thing; zzz_usd_full is. The underscore stem
        # is the second candidate and must be tried. (Synthetic ticker so the
        # test does not depend on which crypto pairs are in the static dict.)
        mapper = EntityMap(db_engine=_engine([(40, "zzz_usd_full")]))
        assert "YF:ZZZ-USD:adj_close" not in SEED_MAPPINGS
        assert mapper.get_feature_id("YF:ZZZ-USD:adj_close") == 40
        # A field with no registered feature still misses.
        assert mapper.get_feature_id("YF:ZZZ-USD:high") is None

    def test_futures_contract_suffix_is_stripped(self) -> None:
        mapper = EntityMap(db_engine=_engine([(15, "ng_close")]))
        assert "YF:NG=F:close" not in SEED_MAPPINGS
        assert mapper.get_feature_id("YF:NG=F:close") == 15

    def test_volume_and_open_fields(self, em) -> None:
        assert em.get_feature_id("YF:AAPL:volume") == 13
        assert em.get_feature_id("YF:AAPL:open") == 14

    def test_total_volume_convention(self) -> None:
        mapper = EntityMap(db_engine=_engine([(30, "doge_total_volume")]))
        assert mapper.get_feature_id("YF:DOGE:volume") == 30

    def test_price_field_never_resolves_onto_a_volume_feature(self, em) -> None:
        # aapl_volume is registered; aapl_full/aapl_close are not.  A close
        # request must not land on the volume feature.
        assert "YF:AAPL:low" not in SEED_MAPPINGS
        assert em.get_feature_id("YF:AAPL:low") is None

    def test_volume_field_never_resolves_onto_a_price_feature(self) -> None:
        mapper = EntityMap(db_engine=_engine([(10, "nvda_full")]))
        assert mapper.get_feature_id("YF:NVDA:volume") is None


class TestNoGhostMappings:
    def test_unregistered_candidate_is_not_returned(self) -> None:
        """The whole point: a derived name that nothing registers is a miss,
        not a mapping.  Migrations 0062/0063 exist because ghost mappings
        look resolved while dropping every row."""
        mapper = EntityMap(db_engine=_engine([]))
        for sid in [
            "YF:FTNT:close", "YF:FTNT:adj_close", "YF:FTNT:open",
            "YF:FTNT:high", "YF:FTNT:low", "YF:FTNT:volume",
        ]:
            assert mapper.get_feature_id(sid) is None, sid

    def test_pattern_path_never_refreshes_the_registry_on_a_miss(self) -> None:
        """A static miss re-queries the DB; a pattern miss must not.

        get_feature_id's static branch reloads the feature cache when a
        mapped name is absent.  The pattern path must never do that — it
        misses on most of the raw feed.
        """
        engine = _engine([(10, "nvda_full")])
        mapper = EntityMap(db_engine=engine)
        before = engine.connect.call_count
        for _ in range(50):
            mapper.get_feature_id("YF:FTNT:close")
        assert engine.connect.call_count == before

    def test_registry_reload_invalidates_the_pattern_cache(self) -> None:
        engine = _engine([])
        mapper = EntityMap(db_engine=engine)
        assert mapper.get_feature_id("YF:FTNT:close") is None
        assert mapper._pattern_cache["YF:FTNT:close"] is None

        engine.connect.return_value.__enter__.return_value.execute \
            .return_value.fetchall.return_value = [(99, "ftnt_full")]
        mapper._load_feature_cache()
        assert mapper._pattern_cache == {}
        assert mapper.get_feature_id("YF:FTNT:close") == 99


# ── 3. Caching ───────────────────────────────────────────────────────────

class TestPatternCache:
    def test_miss_is_cached_as_a_miss(self) -> None:
        mapper = EntityMap(db_engine=_engine([]))
        assert mapper.get_feature_id("YF:FTNT:close") is None
        assert "YF:FTNT:close" in mapper._pattern_cache
        assert mapper._pattern_cache["YF:FTNT:close"] is None

    def test_hit_is_cached_and_reused(self, monkeypatch) -> None:
        mapper = EntityMap(db_engine=_engine([(20, "ftnt_full")]))
        assert mapper.get_feature_id("YF:FTNT:close") == 20

        calls: list[str] = []
        import normalization.entity_map as mod
        real = mod.yf_candidate_feature_names

        def spy(series_id: str) -> list[str]:
            calls.append(series_id)
            return real(series_id)

        monkeypatch.setattr(mod, "yf_candidate_feature_names", spy)
        assert mapper.get_feature_id("YF:FTNT:close") == 20
        assert calls == [], "cached series_id should not re-derive candidates"

    def test_non_yf_series_still_return_none(self) -> None:
        """A non-YF unmapped id takes the same cheap path: no candidates,
        no registry query, still None."""
        engine = _engine([])
        mapper = EntityMap(db_engine=engine)
        before = engine.connect.call_count
        for _ in range(10):
            assert mapper.get_feature_id("SOME_FRED_CODE") is None
        assert engine.connect.call_count == before


# ── Candidate derivation (pure functions) ────────────────────────────────

class TestCandidateDerivation:
    @pytest.mark.parametrize("series_id", [
        "T10Y2Y", "FRED:real_ffr", "WEB:repo_volume",
        "YF:AAPL", "YF:AAPL:vwap", "YF:AAPL:close:extra",
        "yf:aapl:close", "", "YF::close",
    ])
    def test_non_matching_ids_yield_no_candidates(self, series_id) -> None:
        assert yf_candidate_feature_names(series_id) == []

    def test_close_prefers_full_over_close(self) -> None:
        assert yf_candidate_feature_names("YF:AAPL:close") == [
            "aapl_full", "aapl_close",
        ]

    def test_all_six_fields_are_recognised(self) -> None:
        for field in ["open", "high", "low", "close", "volume", "adj_close"]:
            assert yf_candidate_feature_names(f"YF:AAPL:{field}"), field

    def test_stems_are_ordered_literal_first_and_deduped(self) -> None:
        assert yf_ticker_stems("AAPL") == ["aapl"]
        assert yf_ticker_stems("BTC-USD") == ["btc-usd", "btc_usd"]
        assert yf_ticker_stems("^GSPC") == ["^gspc", "gspc"]
        assert yf_ticker_stems("CL=F") == ["cl=f", "cl"]

    def test_candidate_list_is_bounded(self) -> None:
        for sid in ["YF:BRK-B:adj_close", "YF:^BTC-USD=F:adj_close"]:
            assert len(yf_candidate_feature_names(sid)) <= 12
