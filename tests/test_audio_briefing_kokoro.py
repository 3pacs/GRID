"""Local Kokoro TTS for the on-demand audio briefing (intelligence/audio_briefing.py).

The briefing's speech now comes from a local Kokoro-FastAPI server named by
``GRID_KOKORO_URL`` (gridz4:8880 on the fleet), with:
  * a bounded request (connect + read timeout, non-streaming);
  * an honest failure -- "Local TTS unavailable: <reason>", text-only, no
    audio path, no file on disk -- never a fake success and never a paid
    fallback, even when GRID_ALLOW_PAID_LLM is on;
  * output in a writable data dir, not the old checkout or the immutable
    release tree (same rule as #761's storage curator).

Every Kokoro call is mocked: ``requests.post`` is replaced, so nothing here
touches the network.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

import config
from intelligence import audio_briefing

FAKE_MP3 = b"ID3\x04\x00\x00\x00\x00\x00\x00" + b"\xff\xf3" + b"\x00" * 2048
KOKORO = "http://kokoro.test:8880"


class _Resp:
    def __init__(self, status: int = 200, content: bytes = FAKE_MP3,
                 content_type: str = "audio/mpeg", text: str = "") -> None:
        self.status_code = status
        self.content = content
        self.headers = {"Content-Type": content_type}
        self.text = text


@pytest.fixture
def briefing_dir(tmp_path, monkeypatch):
    out = tmp_path / "briefings"
    legacy = tmp_path / "legacy"
    monkeypatch.setattr(config.settings, "GRID_BRIEFING_DIR", str(out))
    monkeypatch.setattr(audio_briefing, "_LEGACY_OUTPUT_DIR", legacy)
    return out


@pytest.fixture
def kokoro_on(monkeypatch, briefing_dir):
    monkeypatch.setattr(config.settings, "GRID_KOKORO_URL", KOKORO + "/")
    monkeypatch.setattr(config.settings, "GRID_KOKORO_VOICE", "af_heart")
    monkeypatch.setattr(config.settings, "GRID_KOKORO_TIMEOUT_SECONDS", 90.0)
    monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
    return briefing_dir


@pytest.fixture
def no_paid(monkeypatch):
    """Fail loudly if any paid client is built."""
    def _boom(*a, **kw):
        raise AssertionError("paid TTS/LLM must never be used on the Kokoro path")
    monkeypatch.setattr(audio_briefing, "_get_openai_client", _boom)
    monkeypatch.setattr(audio_briefing, "_get_gemini_client", _boom)


def _stub_pipeline(monkeypatch):
    monkeypatch.setattr(
        audio_briefing, "_collect_all_data",
        lambda engine: {"date": "2026-10-01", "flow": {}, "credit": {}, "thesis": {}},
    )
    monkeypatch.setattr(
        audio_briefing, "_generate_script_text",
        lambda data: ("Good morning. GRID briefing.", "local"),
    )


# -- Kokoro request + file -----------------------------------------------------

def test_kokoro_success_writes_mp3_with_bounded_request(monkeypatch, kokoro_on, no_paid):
    calls = []

    def fake_post(url, json=None, timeout=None):
        calls.append((url, json, timeout))
        return _Resp()

    monkeypatch.setattr(requests, "post", fake_post)

    path = audio_briefing._generate_audio_file_kokoro("Hello GRID.", "2026-10-01")

    assert len(calls) == 1
    url, body, timeout = calls[0]
    assert url == f"{KOKORO}/v1/audio/speech"
    assert body["input"] == "Hello GRID."
    assert body["voice"] == "af_heart"
    assert body["response_format"] == "mp3"
    assert body["stream"] is False
    assert timeout == (audio_briefing.KOKORO_CONNECT_TIMEOUT_SECONDS, 90.0)

    out = Path(path)
    assert out.parent == kokoro_on
    assert out.name.startswith("briefing_2026-10-01_") and out.suffix == ".mp3"
    assert out.read_bytes() == FAKE_MP3
    assert not list(kokoro_on.glob("*.part"))


def test_generate_briefing_audio_end_to_end_with_kokoro(monkeypatch, kokoro_on, no_paid):
    _stub_pipeline(monkeypatch)
    monkeypatch.setattr(requests, "post", lambda *a, **kw: _Resp())

    result = audio_briefing.generate_briefing_audio(MagicMock())

    assert result.audio_status == "generated"
    assert result.tts_provider == "kokoro"
    assert result.provider == "local"
    assert result.audio_note == ""
    mp3 = Path(result.audio_path)
    assert mp3.exists() and mp3.parent == kokoro_on
    sidecar = json.loads(mp3.with_suffix(".json").read_text())
    assert sidecar["tts_provider"] == "kokoro"
    assert sidecar["audio_status"] == "generated"

    listed = audio_briefing.list_all_briefings()
    assert [b["filename"] for b in listed] == [mp3.name]
    loaded = audio_briefing.get_briefing_by_filename(mp3.name)
    assert loaded is not None and loaded.tts_provider == "kokoro"
    assert audio_briefing.get_latest_briefing().audio_path == str(mp3)


# -- Honest failures -----------------------------------------------------------

@pytest.mark.parametrize(
    ("side_effect", "expect"),
    [
        (requests.ConnectionError("refused"), "unreachable"),
        (requests.ReadTimeout("slow"), "timed out"),
        (_Resp(status=400, content=b"", content_type="application/json",
               text='{"detail":"Voice not found"}'), "HTTP 400"),
        (_Resp(status=200, content=b"<html>", content_type="text/html"), "no MP3 audio"),
        (_Resp(status=200, content=b"", content_type="audio/mpeg"), "no MP3 audio"),
    ],
)
def test_kokoro_failure_is_text_only_and_never_paid(
    monkeypatch, kokoro_on, no_paid, side_effect, expect,
):
    # Paid opt-in ON: a Kokoro failure must still not fall through to OpenAI.
    monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", True)
    _stub_pipeline(monkeypatch)

    def fake_post(*a, **kw):
        if isinstance(side_effect, Exception):
            raise side_effect
        return side_effect

    monkeypatch.setattr(requests, "post", fake_post)

    result = audio_briefing.generate_briefing_audio(MagicMock())

    assert result.audio_path is None
    assert result.audio_status == "unavailable"
    assert result.script_text == "Good morning. GRID briefing."
    assert result.audio_note.startswith("Local TTS unavailable:")
    assert expect in result.audio_note
    assert not kokoro_on.exists() or not any(kokoro_on.iterdir())


def test_not_configured_is_text_only_without_any_request(monkeypatch, briefing_dir, no_paid):
    monkeypatch.setattr(config.settings, "GRID_KOKORO_URL", "")
    monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
    _stub_pipeline(monkeypatch)

    def _no_network(*a, **kw):
        raise AssertionError("no TTS request when local TTS is not configured")
    monkeypatch.setattr(requests, "post", _no_network)

    result = audio_briefing.generate_briefing_audio(MagicMock())

    assert result.audio_path is None
    assert result.audio_status == "not_configured"
    assert "GRID_KOKORO_URL" in result.audio_note


def test_video_path_raises_when_no_tts_is_available(monkeypatch, briefing_dir):
    monkeypatch.setattr(config.settings, "GRID_KOKORO_URL", "")
    monkeypatch.setattr(config.settings, "GRID_ALLOW_PAID_LLM", False)
    with pytest.raises(audio_briefing.LocalTTSUnavailable, match="not configured"):
        audio_briefing._generate_audio_file("x", "2026-10-01")


# -- Output location -----------------------------------------------------------

def test_release_writes_to_data_root_not_release_tree(monkeypatch, tmp_path):
    monkeypatch.setattr(config.settings, "GRID_BRIEFING_DIR", "")
    data_root = tmp_path / "data" / "grid_v4"
    release_file = (
        data_root / "grid_release.releases" / "abc123" / "intelligence" / "audio_briefing.py"
    )
    assert audio_briefing.briefing_output_dir(
        source_file=release_file, data_root=data_root,
    ) == data_root / "briefings"
    # A plain checkout keeps its own git-ignored outputs/briefings.
    assert audio_briefing.briefing_output_dir(
        source_file=tmp_path / "dev" / "intelligence" / "audio_briefing.py",
        data_root=data_root,
    ) == audio_briefing._CHECKOUT_OUTPUT_DIR
    assert audio_briefing._CHECKOUT_OUTPUT_DIR.parts[-2:] == ("outputs", "briefings")


def test_explicit_briefing_dir_wins(monkeypatch, tmp_path):
    monkeypatch.setattr(config.settings, "GRID_BRIEFING_DIR", str(tmp_path / "x"))
    assert audio_briefing.briefing_output_dir() == tmp_path / "x"


def test_legacy_archive_still_listed_and_playable(monkeypatch, briefing_dir, tmp_path):
    legacy = audio_briefing._LEGACY_OUTPUT_DIR
    legacy.mkdir(parents=True)
    old = legacy / "briefing_2026-04-07_20260407_060040.mp3"
    old.write_bytes(FAKE_MP3)

    listed = audio_briefing.list_all_briefings()
    assert [b["filename"] for b in listed] == [old.name]
    loaded = audio_briefing.get_briefing_by_filename(old.name)
    assert loaded is not None and loaded.audio_path == str(old)


@pytest.mark.parametrize(
    "name",
    ["../briefing_x.mp3", "..\\briefing_x.mp3", "sub/briefing_x.mp3", "secrets.mp3", "/etc/passwd"],
)
def test_get_briefing_by_filename_rejects_paths(briefing_dir, name):
    briefing_dir.mkdir(parents=True)
    assert audio_briefing.get_briefing_by_filename(name) is None


# -- Config --------------------------------------------------------------------

def test_kokoro_settings_bind_from_env_and_blank_timeout_keeps_default(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")
    monkeypatch.setenv("GRID_KOKORO_URL", "http://gridz4:8880")
    monkeypatch.setenv("GRID_KOKORO_TIMEOUT_SECONDS", "  ")
    fresh = config.Settings(_env_file=None)
    assert fresh.GRID_KOKORO_URL == "http://gridz4:8880"
    assert fresh.GRID_KOKORO_TIMEOUT_SECONDS == 120.0
    # Default: not configured (owner sets GRID_KOKORO_URL to activate).
    assert config.Settings.model_fields["GRID_KOKORO_URL"].default == ""
