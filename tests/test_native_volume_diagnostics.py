"""Synthetic failure qualification; no provider or configuration imports."""
import json
import stat

import pytest

from ingestion.realtime import native_volume_cache as cache
from tests.test_native_volume_cache import Client, T, run
from tests.test_native_volume_worker import call


@pytest.fixture
def root(tmp_path):
    tmp_path.chmod(0o700)
    return tmp_path


@pytest.fixture
def setup(root):
    from ingestion.realtime.minute_retention import initialize
    store = root / "minutes.db"
    initialize(store)
    return {"store": store, "receipts_dir": root}


@pytest.mark.parametrize("payload", [{"symbol": "WRONG", "candles": []}, [], None])
def test_invalid_capture_private_unchanged(setup, payload):
    class Bad(Client):
        def price_history(self, *a, **k):
            self.calls += 1
            return payload
    c = Bad()
    before = setup["store"].read_bytes()
    with pytest.raises(cache.NativeVolumeFailure) as caught:
        run(c, setup)
    assert caught.value.stage == "validation"
    assert c.calls == 1 and setup["store"].read_bytes() == before
    files = list(setup["receipts_dir"].glob("unvalidated-*.json"))
    assert len(files) == 1
    assert stat.S_IMODE(files[0].stat().st_mode) == 0o600
    assert files[0].stat().st_nlink == 1
    assert json.loads(files[0].read_text())["payload"] == payload
    assert not list(setup["receipts_dir"].glob("[0-9a-f]" * 64 + ".json"))


@pytest.mark.parametrize("payload", [{"x": float("nan")}, {"x": object()}, {"x": "x" * 200}])
def test_unencodable_or_oversize_not_saved(setup, monkeypatch, payload):
    monkeypatch.setattr(cache, "MAX_BYTES", 100)
    class Bad(Client):
        def price_history(self, *a, **k):
            self.calls += 1
            return payload
    c = Bad()
    before = setup["store"].read_bytes()
    with pytest.raises(cache.NativeVolumeFailure) as caught:
        run(c, setup)
    assert caught.value.stage == "capture"
    assert c.calls == 1 and setup["store"].read_bytes() == before
    assert not list(setup["receipts_dir"].glob("*.json"))


def test_worker_sanitized_failure_and_no_retry(root):
    class Bad(Client):
        def price_history(self, *a, **k):
            self.calls += 1
            raise Exception("SECRET URL RAW PROVIDER PAYLOAD")
    c = Bad()
    with pytest.raises(cache.NativeVolumeFailure):
        call(c, root, initialize_store=True)
    state = json.loads((root / "checkpoint.json").read_text())
    assert state["status"] == "failed" and state["failure_stage"] == "request"
    assert "SECRET" not in json.dumps(state)
    with pytest.raises(ValueError, match="reconciliation"):
        call(c, root)
    assert c.calls == 1


def test_reconcile_rejects_unvalidated_filename(setup):
    from scripts.native_volume_reconcile import _read_receipt
    class Bad(Client):
        def price_history(self, *a, **k):
            return {"symbol": "WRONG", "candles": []}
    with pytest.raises(cache.NativeVolumeFailure):
        run(Bad(), setup)
    capture = next(setup["receipts_dir"].glob("unvalidated-*.json"))
    # Even forced semantic validation of the private capture must refuse it.
    with pytest.raises(ValueError):
        cache._validate(json.loads(capture.read_text()))
    with pytest.raises(ValueError):
        _read_receipt(setup["receipts_dir"], capture.stem)


def test_cli_generic_exception_sanitized(tmp_path, monkeypatch, capsys):
    import scripts.native_volume_once as cli
    from tests.test_native_volume_once import Calendar
    tmp_path.chmod(0o700)
    def bad(*a, **k):
        raise Exception("SECRET ARBITRARY ERROR")
    monkeypatch.setattr(cli, "run_once", bad)
    assert cli.main(["--fetch", "--root", str(tmp_path)], calendar_factory=Calendar,
                    clock=lambda: T) == 1
    output = capsys.readouterr().out
    assert "SECRET" not in output
    assert json.loads(output)["failure_stage"] == "worker"


def test_real_root_unvalidated_digest_not_recoverable(root):
    import os
    from scripts.native_volume_reconcile import _read_receipt
    class Bad(Client):
        def price_history(self, *a, **k):
            self.calls += 1
            return {"symbol": "WRONG", "candles": [], "extra": "SECRET"}
    with pytest.raises(cache.NativeVolumeFailure):
        call(Bad(), root, initialize_store=True)
    capture = next((root / "receipts").glob("unvalidated-*.json"))
    digest = capture.stem.removeprefix("unvalidated-")
    before = [(root / name).read_bytes() for name in ("checkpoint.json", "minutes.sqlite")]
    with pytest.raises(FileNotFoundError):
        _read_receipt(root, digest)
    assert before == [(root / name).read_bytes() for name in ("checkpoint.json", "minutes.sqlite")]
    assert "SECRET" not in (root / "checkpoint.json").read_text()
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        used, available = cache.disk_inventory(fd)
    finally:
        os.close(fd)
    expected = sum(max(p.stat().st_size, p.stat().st_blocks * 512)
                   for p in root.rglob("*") if p.is_file())
    assert used == expected and used >= capture.stat().st_size and available > 0


@pytest.mark.parametrize("during", ["validation", "promotion"])
def test_capture_interruption_held_no_retry(root, monkeypatch, during):
    import os
    from scripts.native_volume_once import InvocationDeadline
    from scripts.native_volume_reconcile import _read_receipt
    from tests.test_native_volume_worker import Client as WorkerClient
    c = WorkerClient()
    if during == "validation":
        def interrupt(*a, **k):
            raise InvocationDeadline()
        monkeypatch.setattr(cache, "_validate", interrupt)
    else:
        original = os.link
        def interrupt(source, target, **kwargs):
            original(source, target, **kwargs)
            if str(source).split("/")[-1].startswith("unvalidated-"):
                raise InvocationDeadline()
        monkeypatch.setattr(cache.os, "link", interrupt)
    with pytest.raises(InvocationDeadline):
        call(c, root, initialize_store=True)
    assert json.loads((root / "checkpoint.json").read_text())["status"] == "in_progress"
    with pytest.raises(ValueError, match="reconciliation"):
        call(c, root)
    assert len(c.calls) == 1
    capture = next((root / "receipts").glob("unvalidated-*.json"))
    digest = capture.stem.removeprefix("unvalidated-")
    if during == "promotion":
        assert capture.stat().st_nlink == 2
        with pytest.raises(ValueError, match="custody"):
            _read_receipt(root, digest)
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            with pytest.raises(ValueError, match="custody"):
                cache.disk_inventory(fd)
        finally:
            os.close(fd)
    else:
        with pytest.raises(FileNotFoundError):
            _read_receipt(root, digest)
