"""store.pit.PITWindowCache: in-memory PIT reads must equal the SQL contract.

W2-7 (2026-11-06): rotation backtests read the same PIT window ~117 times per
job. ``PITWindowCache`` reads every vintage of the window once and answers the
reads from memory; a snapshot file of the same window lets a remote worker run
with no database reads. These tests hold the in-memory pick to a brute-force
reference of the SQL in ``PITStore.get_pit`` (filters, retraction cutoff,
vintage policy, obs_date bounds), and the snapshot to its hash receipt.
The same comparison against PostgreSQL itself is in
``tests/test_pit_window_cache_pg.py``.
"""

from __future__ import annotations

import gzip
import json
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from store.pit import (
    PIT_WINDOW_COLUMNS,
    PITStore,
    PITWindowCache,
    PITWindowMiss,
    retraction_cutoff,
)

D0 = date(2025, 1, 1)


def _fixture_rows() -> list[tuple]:
    """Three features, several vintages per obs_date, retractions on some."""
    rows = []
    for fid in (1, 2, 3):
        for k in range(40):
            obs = D0 + timedelta(days=k)
            # first release the next day, a revision 5 days later, and for
            # every 7th obs a late revision 20 days later
            rows.append((fid, obs, 100.0 * fid + k, obs + timedelta(days=1), obs + timedelta(days=1), None))
            rows.append((fid, obs, 100.0 * fid + k + 0.5, obs + timedelta(days=5), obs + timedelta(days=5), None))
            if k % 7 == 0:
                rows.append(
                    (fid, obs, 100.0 * fid + k + 0.75, obs + timedelta(days=20), obs + timedelta(days=20), None)
                )
    # retract: feature 1's revision of obs D0+3 (at noon UTC on D0+10), and
    # feature 2's first release of obs D0+14 (at D0+30)
    out = []
    for r in rows:
        fid, obs, value, release, vintage, _ = r
        retracted = None
        if fid == 1 and obs == D0 + timedelta(days=3) and vintage == obs + timedelta(days=5):
            retracted = datetime.combine(D0 + timedelta(days=10), datetime.min.time(), timezone.utc) + timedelta(hours=12)
        if fid == 2 and obs == D0 + timedelta(days=14) and vintage == obs + timedelta(days=1):
            retracted = datetime.combine(D0 + timedelta(days=30), datetime.min.time(), timezone.utc)
        out.append((fid, obs, value, release, vintage, retracted))
    return out


def _reference_pit(rows, fids, as_of, policy, start=None, end=None) -> pd.DataFrame:
    """Brute-force twin of PITStore.get_pit's SQL."""
    cutoff = retraction_cutoff(as_of)
    best: dict[tuple, tuple] = {}
    for fid, obs, value, release, vintage, retracted in rows:
        if fid not in fids or obs > as_of or release > as_of:
            continue
        if (start is not None and obs < start) or (end is not None and obs > end):
            continue
        if retracted is not None and retracted <= cutoff:
            continue
        key = (fid, obs)
        cur = best.get(key)
        if cur is None or (policy == "LATEST_AS_OF" and vintage > cur[4]) or (
            policy == "FIRST_RELEASE" and vintage < cur[4]
        ):
            best[key] = (fid, obs, value, release, vintage)
    return pd.DataFrame(
        sorted(best.values()),
        columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"],
    )


def _cache(rows=None, **kw) -> PITWindowCache:
    rows = _fixture_rows() if rows is None else rows
    args = {
        "feature_ids": [1, 2, 3],
        "start_date": None,  # the fixture holds each feature's whole history
        "end_date": D0 + timedelta(days=70),  # no obs after D0+39 exist
        "max_as_of": D0 + timedelta(days=70),
        "feature_names": {"a_full": 1, "b_full": 2, "c_full": 3, "gone_full": None},
    }
    args.update(kw)
    return PITWindowCache(pd.DataFrame(rows, columns=list(PIT_WINDOW_COLUMNS)), **args)


def _assert_same(got: pd.DataFrame, want: pd.DataFrame) -> None:
    got = got.reset_index(drop=True)
    assert list(got.columns) == list(want.columns)
    assert got.to_dict("records") == want.to_dict("records")


@pytest.mark.parametrize("policy", ["LATEST_AS_OF", "FIRST_RELEASE"])
def test_every_as_of_matches_the_sql_reference(policy):
    rows = _fixture_rows()
    cache = _cache(rows)
    for offset in range(71):
        as_of = D0 + timedelta(days=offset)
        for fids in ([1, 2, 3], [2], [1, 3]):
            _assert_same(
                cache.get_pit(fids, as_of, policy),
                _reference_pit(rows, set(fids), as_of, policy),
            )


def test_obs_date_bounds_match_the_reference():
    rows = _fixture_rows()
    cache = _cache(rows)
    for offset in (12, 25, 45, 70):
        as_of = D0 + timedelta(days=offset)
        for start, end in ((D0 + timedelta(days=3), D0 + timedelta(days=9)), (D0 + timedelta(days=10), None)):
            _assert_same(
                cache.get_pit([1, 2], as_of, "LATEST_AS_OF", start_date=start, end_date=end),
                _reference_pit(rows, {1, 2}, as_of, "LATEST_AS_OF", start, end),
            )


def test_retraction_hides_the_row_from_its_utc_day_on():
    cache = _cache()
    obs = D0 + timedelta(days=3)
    before = cache.get_pit([1], D0 + timedelta(days=9), start_date=obs, end_date=obs)
    on_day = cache.get_pit([1], D0 + timedelta(days=10), start_date=obs, end_date=obs)
    # the D0+8 revision is visible the day before its retraction, the first
    # release (never retracted) comes back on the retraction's UTC day
    assert before["vintage_date"].tolist() == [obs + timedelta(days=5)]
    assert on_day["vintage_date"].tolist() == [obs + timedelta(days=1)]


def test_feature_matrix_is_the_inherited_pivot():
    rows = _fixture_rows()
    cache = _cache(rows)
    as_of = D0 + timedelta(days=50)
    matrix = cache.get_feature_matrix([1, 2], D0 + timedelta(days=5), D0 + timedelta(days=30), as_of, "LATEST_AS_OF")
    ref = _reference_pit(rows, {1, 2}, as_of, "LATEST_AS_OF", D0 + timedelta(days=5), D0 + timedelta(days=30))
    assert matrix.shape == (26, 2)
    assert matrix.loc[pd.Timestamp(D0 + timedelta(days=7)), 1] == ref[
        (ref.feature_id == 1) & (ref.obs_date == D0 + timedelta(days=7))
    ]["value"].item()


@pytest.mark.parametrize(
    "fids, as_of, start, end",
    [
        ([4], D0 + timedelta(days=20), D0, None),  # feature not held
        ([1], D0 + timedelta(days=71), D0, None),  # as_of past max_as_of
        ([1], D0 + timedelta(days=20), None, None),  # unbounded below
        ([1], D0 + timedelta(days=20), D0 - timedelta(days=1), None),  # starts before window
        ([1], D0 + timedelta(days=60), D0, D0 + timedelta(days=45)),  # ends after window
    ],
)
def test_reads_outside_the_window_raise_without_fallback(fids, as_of, start, end):
    with pytest.raises(PITWindowMiss):
        _cache(start_date=D0, end_date=D0 + timedelta(days=39)).get_pit(fids, as_of, start_date=start, end_date=end)


def test_end_is_clipped_to_as_of_when_checking_coverage():
    # obs window D0..D0+30, as_of D0+30: the open end is as_of, inside the window
    cache = _cache(end_date=D0 + timedelta(days=39))
    assert cache.covers([1], D0 + timedelta(days=30), D0, None)
    assert not cache.covers([1], D0 + timedelta(days=45), D0, None)
    assert cache.covers([1], D0 + timedelta(days=45), D0, D0 + timedelta(days=39))


def test_miss_falls_back_to_the_database_only_when_allowed(monkeypatch):
    calls = []

    def fake_db_get_pit(self, feature_ids, as_of_date, vintage_policy="LATEST_AS_OF", *, start_date=None, end_date=None):
        calls.append((tuple(feature_ids), as_of_date, start_date, end_date))
        return pd.DataFrame(columns=["feature_id", "obs_date", "value", "release_date", "vintage_date"])

    monkeypatch.setattr(PITStore, "get_pit", fake_db_get_pit)
    cache = _cache(engine=object(), allow_db_fallback=True)
    cache.get_pit([9], D0 + timedelta(days=20), start_date=D0)
    assert calls == [((9,), D0 + timedelta(days=20), D0, None)]
    assert cache.db_fallbacks == 1


def test_feature_names_answer_from_the_build_time_map():
    cache = _cache()
    assert cache.feature_ids_by_name(["a_full", "gone_full", "c_full"]) == {"a_full": 1, "c_full": 3}
    with pytest.raises(PITWindowMiss):
        cache.feature_ids_by_name(["never_asked_full"])


# ── snapshot files ───────────────────────────────────────────────────────


def test_snapshot_round_trips_to_identical_reads(tmp_path):
    rows = _fixture_rows()
    cache = _cache(rows)
    path = cache.write_snapshot(tmp_path, built_by="test")
    assert path.name == f"pit-window-{cache.content_sha256()}.json.gz"

    loaded = PITWindowCache.read_snapshot(path, expected_sha256=cache.content_sha256())
    assert loaded.receipt["sha256"] == cache.content_sha256()
    assert loaded.receipt["n_rows"] == len(rows)
    for offset in (5, 18, 33, 70):
        as_of = D0 + timedelta(days=offset)
        for policy in ("LATEST_AS_OF", "FIRST_RELEASE"):
            _assert_same(loaded.get_pit([1, 2, 3], as_of, policy), cache.get_pit([1, 2, 3], as_of, policy))
    assert loaded.feature_ids_by_name(["b_full", "gone_full"]) == {"b_full": 2}


def test_same_window_gives_the_same_file_name(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    first = _cache().write_snapshot(tmp_path / "a")
    second = _cache().write_snapshot(tmp_path / "b")
    assert first.name == second.name
    # writing again into the same directory reuses the verified file
    assert _cache().write_snapshot(tmp_path / "a") == first


def test_tampered_snapshot_is_refused(tmp_path):
    path = _cache().write_snapshot(tmp_path)
    with gzip.open(path, "rb") as gz:
        envelope = json.loads(gz.read())
    envelope["content"]["rows"][0][2] += 1.0  # one value changed, receipt kept
    tampered = tmp_path / "tampered.json.gz"
    with gzip.open(tampered, "wb") as gz:
        gz.write(json.dumps(envelope).encode())
    with pytest.raises(ValueError, match="does not match its receipt"):
        PITWindowCache.read_snapshot(tampered)


def test_unexpected_digest_is_refused(tmp_path):
    path = _cache().write_snapshot(tmp_path)
    with pytest.raises(ValueError, match="not the expected"):
        PITWindowCache.read_snapshot(path, expected_sha256="0" * 64)


def test_snapshot_excludes_nothing_a_read_inside_the_window_needs(tmp_path):
    """A retraction after max_as_of is dropped at load time (cutoff), and
    that never changes a read the window can answer."""
    rows = _fixture_rows()
    late = datetime(2030, 1, 1, tzinfo=timezone.utc)
    rows_late = [r if r[0] != 3 else (*r[:5], late) for r in rows]
    a = _cache(rows)
    b = _cache(rows_late)
    for offset in (10, 40, 70):
        as_of = D0 + timedelta(days=offset)
        _assert_same(b.get_pit([3], as_of), a.get_pit([3], as_of))
