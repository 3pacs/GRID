"""GEM daily systemd drop-ins: pin rendering, quarantine, timer, CONTAIN."""

from datetime import date, datetime, time, timezone
from zoneinfo import ZoneInfo

import pytest

from scripts import gem_daily_capture as daily
from scripts import gem_daily_contain as contain
from scripts import render_gem_daily_units as render

PIN = "0123456789abcdef0123456789abcdef01234567"


@pytest.mark.parametrize("bad", ["", "abc", PIN.upper(), PIN[:-1], PIN + "0", "../" + PIN[3:]])
def test_render_refuses_anything_but_a_full_sha(tmp_path, bad) -> None:
    with pytest.raises(ValueError):
        render.render(bad, tmp_path)
    assert not any(tmp_path.rglob("*.conf"))


def test_rendered_pin_drop_in_is_immutable_and_contained(tmp_path) -> None:
    render.render(PIN, tmp_path)
    pin = (tmp_path / "grid-options-puller.service.d" / "50-grid652-immutable-pin.conf").read_text()
    root = f"/data/grid_v4/grid-options-puller-pins/{PIN}"
    lines = [line for line in pin.splitlines() if line and not line.startswith("#")]
    assert f"WorkingDirectory={root}" in lines
    assert f"Environment=GEM_DAILY_PIN_SHA={PIN}" in lines
    # ExecStart is reset first, then points only at the gated runner.
    starts = [line for line in lines if line.startswith("ExecStart=")]
    assert starts == ["ExecStart=",
                      f"ExecStart=/data/grid_v4/venv/bin/python3 {root}/scripts/gem_daily_capture.py"]
    stop_posts = [line for line in lines if line.startswith("ExecStopPost=")]
    assert stop_posts[0] == "ExecStopPost=" and len(stop_posts) == 2
    assert "Type=oneshot" in lines  # TimeoutStartSec then bounds the whole run
    assert (f"ExecStopPost=-/data/grid_v4/venv/bin/python3 {root}/scripts/gem_daily_contain.py"
            in lines)
    for setting in ("TimeoutStartSec=15min", "KillMode=control-group", "SendSIGKILL=yes",
                    "NoNewPrivileges=yes"):
        assert setting in lines
    assert "@" not in pin
    assert daily._PIN_ROOT.as_posix() + f"/{PIN}" == root


def test_quarantine_keeps_manual_start_refused_and_marker_gated(tmp_path) -> None:
    render.render(PIN, tmp_path)
    text = (tmp_path / "grid-options-puller.service.d" / "99-grid652-quarantine.conf").read_text()
    lines = [line for line in text.splitlines() if line and not line.startswith("#")]
    assert lines == ["[Unit]", "RefuseManualStart=yes",
                     f"ConditionPathExists={daily._ACTIVATED.as_posix()}"]


def test_timer_fires_once_per_weekday_after_scheduler_pull_and_before_deadline(tmp_path) -> None:
    render.render(PIN, tmp_path)
    text = (tmp_path / "grid-options-puller.timer.d" / "50-gem-daily.conf").read_text()
    lines = [line for line in text.splitlines() if line and not line.startswith("#")]
    assert lines == ["[Timer]", "OnCalendar=",
                     "OnCalendar=Mon..Fri *-*-* 10:05:00 America/New_York",
                     "Persistent=false", "RandomizedDelaySec=0", "AccuracySec=1s"]
    ny = ZoneInfo("America/New_York")
    for day in (date(2026, 10, 1), date(2026, 11, 2)):  # EDT and EST
        fire = datetime.combine(day, time(10, 5), ny).astimezone(timezone.utc)
        assert fire.time() >= time(13, 29)
        assert fire < daily._session_deadline(day)


def test_contain_and_runner_share_the_attempts_directory() -> None:
    assert contain._ATTEMPTS == daily._ATTEMPTS


def test_contain_appends_terminal_record_and_never_fails(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(contain, "_ATTEMPTS", tmp_path)
    monkeypatch.setenv("SERVICE_RESULT", "timeout")
    monkeypatch.setenv("EXIT_STATUS", "KILL")
    assert contain.main() == 0  # no claim yet: still exits 0
    assert "no-claim" in capsys.readouterr().out
    today = datetime.now(timezone.utc).date().isoformat()
    (tmp_path / today).write_text("x STARTED\n", encoding="ascii")
    assert contain.main() == 0
    assert (tmp_path / today).read_text().splitlines()[-1].endswith(
        "CONTAINED result=timeout status=KILL")
    monkeypatch.setenv("SERVICE_RESULT", "evil\nline")
    assert contain.main() == 0
    assert (tmp_path / today).read_text().splitlines()[-1].endswith(
        "CONTAINED result=unknown status=KILL")
