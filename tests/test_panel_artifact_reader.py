"""Frozen-artifact reader (GD6 acceptance 4.6): refuse wrong hash, missing receipt key, extra column,
naive or non-UTC timestamps, look-ahead; a correct artifact round-trips."""

from __future__ import annotations

import hashlib
import json

import pandas as pd
import pytest

from analysis.panel_artifact_reader import RECEIPT_KEYS, read_frozen_artifact, receipt_path

KIND = "people_density"


def _frame(tz="UTC") -> pd.DataFrame:
    decided = pd.date_range("2026-07-06 20:00", periods=4, freq="7D", tz=tz)
    return pd.DataFrame({
        "entity_id": ["E1", "E2", "E1", "E2"],
        "decision_at": decided,
        "known_at": decided - pd.Timedelta(days=1),
        "A": [0.0, 1.5, 2.0, 0.0],
    })


def _write(tmp_path, frame: pd.DataFrame, **receipt_over):
    path = tmp_path / "density.parquet"
    frame.to_parquet(path, index=False)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    receipt = {
        "kind": KIND, "artifact_sha256": sha, "columns": list(frame.columns), "row_count": len(frame),
        "timestamp_columns": ["decision_at", "known_at"], "input_event_set_sha256": "1" * 64,
        "spec_sha256s": {"A90": "2" * 64}, "code_sha": "c0de" * 10, "as_of": "2026-09-30T00:00:00+00:00",
        "membership_map_sha256": "3" * 64,
    }
    receipt.update(receipt_over)
    receipt = {k: v for k, v in receipt.items() if v is not ...}
    receipt_path(path).write_text(json.dumps(receipt), encoding="utf-8")
    return path, sha


def test_round_trip(tmp_path):
    path, sha = _write(tmp_path, _frame())
    frame, receipt = read_frozen_artifact(path, expected_sha256=sha, allowed_kinds=[KIND])
    pd.testing.assert_frame_equal(frame, _frame())
    assert receipt["artifact_sha256"] == sha and len(receipt["receipt_sha256"]) == 64


def test_wrong_hash_refused(tmp_path):
    path, _ = _write(tmp_path, _frame())
    with pytest.raises(PermissionError, match="unknown artifact"):
        read_frozen_artifact(path, expected_sha256="0" * 64, allowed_kinds=[KIND])


@pytest.mark.parametrize("missing", RECEIPT_KEYS)
def test_missing_receipt_key_refused(tmp_path, missing):
    path, sha = _write(tmp_path, _frame(), **{missing: ...})
    with pytest.raises(PermissionError):
        read_frozen_artifact(path, expected_sha256=sha, allowed_kinds=[KIND])


def test_extra_column_refused(tmp_path):
    frame = _frame()
    path, sha = _write(tmp_path, frame.assign(extra=1.0), columns=list(frame.columns))
    with pytest.raises(PermissionError, match="columns"):
        read_frozen_artifact(path, expected_sha256=sha, allowed_kinds=[KIND])


def test_naive_and_non_utc_timestamps_refused(tmp_path):
    naive = _frame()
    naive["decision_at"] = naive["decision_at"].dt.tz_localize(None)
    path, sha = _write(tmp_path, naive)
    with pytest.raises(PermissionError, match="naive"):
        read_frozen_artifact(path, expected_sha256=sha, allowed_kinds=[KIND])
    ny = tmp_path / "ny"
    ny.mkdir()
    path, sha = _write(ny, _frame(tz="America/New_York"))
    with pytest.raises(PermissionError, match="not UTC"):
        read_frozen_artifact(path, expected_sha256=sha, allowed_kinds=[KIND])
    asof = tmp_path / "asof"
    asof.mkdir()
    path, sha = _write(asof, _frame(), as_of="2026-09-30T00:00:00")
    with pytest.raises(PermissionError, match="UTC"):
        read_frozen_artifact(path, expected_sha256=sha, allowed_kinds=[KIND])


def test_undeclared_timestamp_column_refused(tmp_path):
    path, sha = _write(tmp_path, _frame(), timestamp_columns=["known_at"])
    with pytest.raises(PermissionError, match="not declared"):
        read_frozen_artifact(path, expected_sha256=sha, allowed_kinds=[KIND])


def test_kind_and_receipt_mismatch_refused(tmp_path):
    path, sha = _write(tmp_path, _frame())
    with pytest.raises(PermissionError, match="kind"):
        read_frozen_artifact(path, expected_sha256=sha, allowed_kinds=["flywheel"])
    other = tmp_path / "o"
    other.mkdir()
    path, sha = _write(other, _frame(), artifact_sha256="9" * 64)
    with pytest.raises(PermissionError, match="another artifact"):
        read_frozen_artifact(path, expected_sha256=sha, allowed_kinds=[KIND])


def test_known_at_after_as_of_refused(tmp_path):
    path, sha = _write(tmp_path, _frame(), as_of="2026-07-01T00:00:00+00:00")
    with pytest.raises(PermissionError, match="look-ahead"):
        read_frozen_artifact(path, expected_sha256=sha, allowed_kinds=[KIND])
