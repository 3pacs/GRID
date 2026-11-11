from datetime import timedelta
import json
import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ingestion.realtime.native_volume_cache import (
    disk_inventory,
    MAX_BYTES,
    MIN_FREE_BYTES,
)
from ingestion.realtime.native_volume_health import notification_condition
from ingestion.realtime.native_volume_worker import run_once
from tests.test_native_volume_worker import Client, Calendar, T, call
from tests import test_native_volume_health as fixtures


def prepare(root):
    root.chmod(0o700)
    (root / "receipts").mkdir(mode=0o700)
    (root / "minutes.sqlite").write_bytes(b"store")
    (root / "minutes.sqlite").chmod(0o600)


def inventory(root):
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        return disk_inventory(fd)
    finally:
        os.close(fd)


def test_inventory_includes_archives_snapshot_and_sqlite_journal(tmp_path):
    prepare(tmp_path)
    before, available = inventory(tmp_path)
    for name in (
        "checkpoint.before-reconcile.saved.json",
        "native-volume.json",
        "minutes.sqlite-journal",
    ):
        p = tmp_path / name
        p.write_bytes(b"x" * 10000)
        p.chmod(0o600)
    after, _ = inventory(tmp_path)
    assert after >= before + 30000 and available > 0


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo", "directory", "public"])
def test_inventory_refuses_unsafe_extra_entries(tmp_path, kind):
    prepare(tmp_path)
    p = tmp_path / "extra"
    if kind == "symlink":
        p.symlink_to(tmp_path / "minutes.sqlite")
    elif kind == "hardlink":
        os.link(tmp_path / "minutes.sqlite", p)
    elif kind == "fifo":
        os.mkfifo(p, 0o600)
    elif kind == "directory":
        p.mkdir(mode=0o700)
    else:
        p.write_bytes(b"x")
        p.chmod(0o644)
    with pytest.raises(ValueError):
        inventory(tmp_path)


def test_inventory_receipt_count_bound(tmp_path):
    prepare(tmp_path)
    for i in range(3):
        p = tmp_path / "receipts" / str(i)
        p.write_bytes(b"x")
        p.chmod(0o600)
    with patch("ingestion.realtime.native_volume_cache.MAX_INVENTORY_FILES", 2):
        with pytest.raises(ValueError, match="inventory bound"):
            inventory(tmp_path)


def test_inventory_physical_free_capacity(tmp_path):
    prepare(tmp_path)
    with patch(
        "ingestion.realtime.native_volume_cache.os.fstatvfs",
        return_value=SimpleNamespace(f_bavail=7, f_frsize=4096),
    ):
        assert inventory(tmp_path)[1] == 7 * 4096


def test_low_free_space_stops_before_provider_or_checkpoint(tmp_path):
    tmp_path.chmod(0o700)
    c = Client()
    with patch(
        "ingestion.realtime.native_volume_worker.disk_inventory",
        return_value=(1000, MIN_FREE_BYTES + 2 * MAX_BYTES - 1),
    ):
        with pytest.raises(ValueError, match="filesystem reserve"):
            call(c, tmp_path, initialize_store=True)
    assert not c.calls and not (tmp_path / "checkpoint.json").exists()


def test_archives_count_against_budget_before_provider(tmp_path):
    tmp_path.chmod(0o700)
    c = Client()
    with patch(
        "ingestion.realtime.native_volume_worker.disk_inventory",
        return_value=(105_000_000, 1_000_000_000),
    ):
        with pytest.raises(ValueError, match="budget"):
            call(c, tmp_path, initialize_store=True)
    assert not c.calls


def test_incremental_only_refuses_empty_and_initialize(tmp_path):
    tmp_path.chmod(0o700)
    c = Client()
    with pytest.raises(ValueError, match="separate explicit"):
        call(c, tmp_path, incremental_only=True)
    with pytest.raises(ValueError, match="cannot initialize"):
        call(c, tmp_path, incremental_only=True, initialize_store=True)
    assert not c.calls and not (tmp_path / "minutes.sqlite").exists()


def test_incremental_only_allows_current_completed_checkpoint(tmp_path):
    tmp_path.chmod(0o700)
    c = Client()
    call(c, tmp_path, initialize_store=True)
    x = call(c, tmp_path, T + timedelta(minutes=1, seconds=5), incremental_only=True)
    assert x["mode"] == "incremental" and len(c.calls) == 2


def test_incremental_only_refuses_new_session(tmp_path):
    tmp_path.chmod(0o700)
    c = Client()
    call(c, tmp_path, initialize_store=True)
    with pytest.raises(ValueError, match="separate explicit"):
        run_once(
            c,
            root=tmp_path,
            calendar=Calendar(),
            session_key="next",
            clock=lambda: T + timedelta(seconds=5),
            incremental_only=True,
        )
    assert len(c.calls) == 1


def test_cli_incremental_initialize_conflict_is_read_only():
    from scripts.native_volume_once import main

    with (
        patch("builtins.print"),
        patch(
            "scripts.native_volume_once.os.open",
            side_effect=AssertionError("no path reads"),
        ),
    ):
        assert (
            main(["--root", "/unused", "--fetch", "--incremental-only", "--initialize"])
            == 2
        )


def health_fixture():
    fixture = fixtures.HealthTests()
    fixture.setUp()
    return fixture, fixture.check()


def test_health_requires_real_free_space_and_alert_adapter_is_safe():
    f, healthy = health_fixture()
    assert notification_condition(healthy, now=f.now)["bad"] is False
    for free in (None, True, -1, MIN_FREE_BYTES + 2 * MAX_BYTES - 1):
        x = f.check(free_bytes=free)
        assert x["alert"] and not x["ready"]
        condition = notification_condition(x, now=f.now)
        assert condition["bad"] is True and condition["delivery_verified"] is False


def test_pending_idle_disabled_do_not_clear_incident():
    f, healthy = health_fixture()
    reports = [
        f.check(enabled=False),
        f.check(session=None),
        f.check(checkpoint=dict(f.checkpoint, status="in_progress")),
        f.check(read_errors=["observation_raced"]),
    ]
    for report in reports:
        assert notification_condition(report, now=f.now)["bad"] is None


def test_alert_adapter_never_transmits_unrecognized_reason_or_raw_detail():
    f, healthy = health_fixture()
    variants = [
        dict(healthy, reasons=["secret-value"]),
        dict(healthy, alert=True),
        dict(healthy, requested=True),
        dict(healthy, checked_at=(f.now + timedelta(seconds=1)).isoformat()),
        dict(healthy, checked_at=(f.now - timedelta(seconds=66)).isoformat()),
    ]
    for report in variants:
        condition = notification_condition(report, now=f.now)
        assert condition["bad"] is True and not condition["ready"]
        assert "secret" not in json.dumps(condition)


def test_alert_adapter_drives_existing_notifier_interface_mock_only():
    f, healthy = health_fixture()
    sent = []

    class ExistingNotifier:
        def _health(self, now, key, bad, bad_message, ok_message):
            sent.append((key, bad, bad_message if bad else ok_message))

    sink = ExistingNotifier()
    for report in [
        f.check(free_bytes=0),
        f.check(checkpoint=dict(f.checkpoint, status="in_progress")),
        healthy,
    ]:
        condition = notification_condition(report, now=f.now)
        if condition["bad"] is not None:
            sink._health(
                f.now.timestamp(),
                condition["key"],
                condition["bad"],
                condition["message"],
                condition["message"],
            )
    assert [x[1] for x in sent] == [True, False]


def test_cadence_templates_fail_closed_and_do_not_retry():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    service = (root / "deploy/systemd/grid-native-volume.service.template").read_text()
    timer = (root / "deploy/systemd/grid-native-volume.timer.template").read_text()
    assert "GRID_NATIVE_VOLUME_CADENCE_ENABLED=0" in service
    assert (
        "ExecCondition=/usr/bin/test ${GRID_NATIVE_VOLUME_CADENCE_ENABLED} = 1"
        in service
    )
    assert "--incremental-only" in service and "--initialize" not in service
    assert "Restart=no" in service and "TimeoutStartSec=55" in service
    assert "Persistent=false" in timer and "RandomizedDelaySec=0" in timer
    assert "09..16:*:10 America/New_York" in timer


def test_inventory_lock_grace_is_bounded_and_does_not_hide_real_errors():
    f, _ = health_fixture()
    current = dict(f.checkpoint, status="in_progress")
    kwargs = dict(checkpoint=current, disk_bytes=None, read_errors=["inventory_busy"])
    assert f.check(**kwargs)["status"] == "pending"
    assert f.check(**dict(kwargs, now=f.now + timedelta(seconds=46)))["alert"]
    assert f.check(**dict(kwargs, free_bytes=0))["alert"]
    assert f.check(**dict(kwargs, disk_budget_bytes=0))["alert"]
    broken = dict(f.snapshot, provenance=None)
    assert f.check(**dict(kwargs, snapshot=broken))["alert"]


def test_health_reader_refuses_to_inventory_while_writer_publishes(tmp_path):
    from ingestion.realtime.native_volume_worker import owner_lock
    from scripts.native_volume_health import _stable_inventory, InventoryBusy

    prepare(tmp_path)
    with owner_lock(tmp_path):
        # Simulate the real immutable-receipt publish interval (link then unlink).
        temporary = tmp_path / "receipts" / ".native-receipt-test"
        temporary.write_bytes(b"fixture")
        temporary.chmod(0o600)
        os.link(temporary, tmp_path / "receipts" / ("a" * 64 + ".json"))
        fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            with pytest.raises(InventoryBusy):
                _stable_inventory(fd)
        finally:
            os.close(fd)
    temporary.unlink()
    assert inventory(tmp_path)[0] > 0


def test_stable_inventory_never_creates_a_missing_lock(tmp_path):
    from scripts.native_volume_health import _stable_inventory

    prepare(tmp_path)
    fd = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert _stable_inventory(fd)[0] > 0
    finally:
        os.close(fd)
    assert not (tmp_path / "writer.lock").exists()


def test_combined_checkpoint_and_inventory_race_is_unknown_not_ready():
    f, _ = health_fixture()
    args = dict(disk_bytes=None, read_errors=["inventory_busy", "observation_raced"])
    result = f.check(**args)
    assert result["status"] == "unknown" and not result["ready"] and not result["alert"]
    assert f.check(**dict(args, free_bytes=0))["alert"]
    assert f.check(**dict(args, last_exit_code=1))["alert"]
    assert f.check(**dict(args, snapshot=dict(f.snapshot, provenance=None)))["alert"]


def test_reader_composes_real_inventory_lock_and_changed_checkpoint():
    import scripts.native_volume_health as module

    f = fixtures.ReaderTests()
    f.setUp()
    original = module._json
    calls = []

    def reader(fd, name, cap):
        value = original(fd, name, cap)
        if name == "checkpoint.json":
            calls.append(name)
            if len(calls) == 2:
                value["status"] = "in_progress"
        return value

    try:
        with (
            patch.object(module, "_json", side_effect=reader),
            patch.object(module, "_stable_inventory", side_effect=module.InventoryBusy),
        ):
            result = f.inspect()
        assert (
            result["status"] == "unknown"
            and not result["alert"]
            and not result["ready"]
        )
    finally:
        f.doCleanups()


def test_nonready_alert_report_reasons_must_match_status():
    f, healthy = health_fixture()
    for status in ("disabled", "idle", "pending", "unknown"):
        report = dict(healthy, status=status, ready=False, reasons=["disk_free_low"])
        assert notification_condition(report, now=f.now)["bad"] is True
