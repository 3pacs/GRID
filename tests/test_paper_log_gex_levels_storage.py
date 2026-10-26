from __future__ import annotations

import hashlib
import json
import threading
import time
from pathlib import Path

import pytest

from paper_log.gex_levels.config import PREREG_SHA256
from paper_log.gex_levels.storage import (
    LockTimeoutError,
    PaperLogStore,
    _canonical_json,
    resolve_code_sha,
)


def test_first_record_carries_prereg_sha256_and_null_prev(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    written = store.append({"kind": "preopen", "n": 1})
    assert written["prereg_sha256"] == PREREG_SHA256
    assert written["prev_sha256"] is None


def test_second_record_carries_hash_of_first_lines_exact_bytes(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    first = store.append({"kind": "preopen", "n": 1})
    second = store.append({"kind": "postclose", "n": 2})

    first_line_bytes = _canonical_json({k: v for k, v in first.items()})
    expected_hash = hashlib.sha256(first_line_bytes).hexdigest()
    assert second["prev_sha256"] == expected_hash


def test_only_the_first_record_ever_carries_prereg_sha256(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append({"kind": "preopen", "n": 1})
    second = store.append({"kind": "postclose", "n": 2})
    assert "prereg_sha256" not in second


def test_append_never_rewrites_existing_lines(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append({"kind": "preopen", "n": 1})
    before = store.log_path.read_bytes()
    store.append({"kind": "postclose", "n": 2})
    after = store.log_path.read_bytes()
    assert after.startswith(before)


def test_read_all_returns_records_in_file_order(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append({"kind": "preopen", "n": 1})
    store.append({"kind": "postclose", "n": 2})
    store.append({"kind": "preopen", "n": 3})
    records = store.read_all()
    assert [r["n"] for r in records] == [1, 2, 3]


def test_verify_chain_ok_on_untampered_log(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    for i in range(5):
        store.append({"kind": "preopen", "n": i})
    result = store.verify_chain()
    assert result.ok is True
    assert result.n_records == 5
    assert result.first_broken_index is None


def test_verify_chain_empty_log_is_ok(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    result = store.verify_chain()
    assert result.ok is True
    assert result.n_records == 0


def test_verify_chain_detects_edited_line(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append({"kind": "preopen", "n": 1})
    store.append({"kind": "postclose", "n": 2})
    store.append({"kind": "preopen", "n": 3})

    lines = store.log_path.read_bytes().splitlines()
    tampered = json.loads(lines[1])
    tampered["n"] = 999  # tamper with the middle record's payload
    lines[1] = _canonical_json(tampered)
    store.log_path.write_bytes(b"\n".join(lines) + b"\n")

    result = store.verify_chain()
    assert result.ok is False
    # record 2 (index 2) now carries prev_sha256 computed from the
    # ORIGINAL line 1; the tampered line 1 hashes to something else.
    assert result.first_broken_index == 2


def test_verify_chain_detects_deleted_line(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append({"kind": "preopen", "n": 1})
    store.append({"kind": "postclose", "n": 2})
    store.append({"kind": "preopen", "n": 3})

    lines = store.log_path.read_bytes().splitlines()
    del lines[1]
    store.log_path.write_bytes(b"\n".join(lines) + b"\n")

    result = store.verify_chain()
    assert result.ok is False
    assert result.first_broken_index == 1


def test_verify_chain_detects_reordered_lines(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append({"kind": "preopen", "n": 1})
    store.append({"kind": "postclose", "n": 2})

    lines = store.log_path.read_bytes().splitlines()
    lines[0], lines[1] = lines[1], lines[0]
    store.log_path.write_bytes(b"\n".join(lines) + b"\n")

    result = store.verify_chain()
    assert result.ok is False


def test_verify_chain_detects_wrong_first_record_prereg_hash(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append({"kind": "preopen", "n": 1})

    lines = store.log_path.read_bytes().splitlines()
    tampered = json.loads(lines[0])
    tampered["prereg_sha256"] = "0" * 64
    store.log_path.write_bytes(_canonical_json(tampered) + b"\n")

    result = store.verify_chain()
    assert result.ok is False
    assert result.first_broken_index == 0


def test_concurrent_appends_are_serialized_and_chain_stays_valid(tmp_path: Path) -> None:
    """Two "runs" (threads) appending at the same time must never interleave
    partial writes, and the resulting file must still be a single valid
    hash chain (no lost or corrupted records)."""
    store = PaperLogStore(tmp_path)
    n_per_thread = 15
    errors: list[Exception] = []

    def _writer(tag: str) -> None:
        try:
            local_store = PaperLogStore(tmp_path)
            for i in range(n_per_thread):
                local_store.append({"kind": "preopen", "tag": tag, "n": i})
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=_writer, args=(tag,)) for tag in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors
    records = store.read_all()
    assert len(records) == 2 * n_per_thread
    result = store.verify_chain()
    assert result.ok is True, result.detail


def test_lock_file_is_removed_after_append(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.append({"kind": "preopen", "n": 1})
    assert not store.lock_path.exists()


def test_stale_lock_is_taken_over(tmp_path: Path) -> None:
    store = PaperLogStore(tmp_path)
    store.lock_path.write_text("pid=99999 stale", encoding="utf-8")
    old_time = time.time() - 10_000  # far older than LOCK_STALE_AFTER_S
    import os
    os.utime(store.lock_path, (old_time, old_time))

    # Should take over the stale lock rather than raising.
    written = store.append({"kind": "preopen", "n": 1})
    assert written["n"] == 1
    assert not store.lock_path.exists()


def test_fresh_lock_blocks_until_timeout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import paper_log.gex_levels.storage as storage_mod

    monkeypatch.setattr(storage_mod, "LOCK_TIMEOUT_S", 0.3)
    store = PaperLogStore(tmp_path)
    store.lock_path.write_text("pid=12345 held by another run right now", encoding="utf-8")

    with pytest.raises(LockTimeoutError):
        store.append({"kind": "preopen", "n": 1})


# ── resolve_code_sha ───────────────────────────────────────────────────


def test_resolve_code_sha_prefers_explicit_override(tmp_path: Path) -> None:
    assert resolve_code_sha(tmp_path, override="abc123") == "abc123"


def test_resolve_code_sha_reads_version_file(tmp_path: Path) -> None:
    (tmp_path / "VERSION").write_text("deadbeef1234\n", encoding="utf-8")
    assert resolve_code_sha(tmp_path) == "deadbeef1234"


def test_resolve_code_sha_falls_back_to_git_when_no_version_file(tmp_path: Path) -> None:
    # tmp_path is not a git repo — this exercises (and confirms) the
    # failure path rather than a real git checkout, keeping the test
    # network/DB-free and not dependent on this repo's own git state.
    with pytest.raises(RuntimeError, match="could not resolve code_sha"):
        resolve_code_sha(tmp_path)
