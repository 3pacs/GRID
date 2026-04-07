"""Talking-head / lip-sync generation from a portrait image + audio.

Supports SadTalker (mature, reliable) and LivePortrait (higher quality)
for turning a still image into a talking-head video driven by audio.

Usage::

    driver = LipSyncDriver()
    driver.load_model()
    driver.generate(
        portrait="headshot.png",
        audio="narration.wav",
        output="talking_head.mp4",
    )

Architecture note:
    SadTalker and LivePortrait are standalone repos with their own weight
    files.  This module wraps them via subprocess calls + Python API where
    available, keeping GRID's dependency footprint light.
"""

from __future__ import annotations

import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from loguru import logger as log


# ---------------------------------------------------------------------------
# Supported backends
# ---------------------------------------------------------------------------
BACKENDS: dict[str, dict[str, Any]] = {
    "sadtalker": {
        "repo_url": "https://github.com/OpenTalker/SadTalker",
        "install_dir": "~/.grid/models/sadtalker",
        "vram_gb": 4,
        "cpu_capable": True,
        "license": "MIT",
    },
    "liveportrait": {
        "repo_url": "https://github.com/KwaiVGI/LivePortrait",
        "install_dir": "~/.grid/models/liveportrait",
        "vram_gb": 6,
        "cpu_capable": True,
        "license": "Apache-2.0",
    },
    "wav2lip": {
        "repo_url": "https://github.com/Rudrabha/Wav2Lip",
        "install_dir": "~/.grid/models/wav2lip",
        "vram_gb": 4,
        "cpu_capable": True,
        "license": "MIT-like",
    },
}

DEFAULT_BACKEND = "sadtalker"


@dataclass
class LipSyncDriver:
    """Talking-head generation from portrait + audio.

    Parameters:
        backend:    Which lip-sync engine to use.
        output_dir: Where generated videos are saved.
        device:     ``"cuda"``, ``"cpu"``, or ``"auto"``.
    """

    backend: str = DEFAULT_BACKEND
    output_dir: str = "outputs/content/lipsync"
    device: str = "auto"
    _ready: bool = field(default=False, repr=False, init=False)
    _backend_path: Path = field(default=None, repr=False, init=False)  # type: ignore[assignment]

    def _resolve_device(self) -> str:
        if self.device != "auto":
            return self.device
        try:
            import torch

            if torch.cuda.is_available():
                return "cuda"
        except ImportError:
            pass
        return "cpu"

    def load_model(self) -> None:
        """Verify backend is installed and ready."""
        if self._ready:
            return

        spec = BACKENDS.get(self.backend)
        if spec is None:
            raise ValueError(
                f"Unknown backend '{self.backend}'. "
                f"Available: {list(BACKENDS.keys())}"
            )

        install_dir = Path(spec["install_dir"]).expanduser()
        self._backend_path = install_dir

        if not install_dir.exists():
            log.warning(
                "Backend {b} not found at {d}. Run setup_backend() to install.",
                b=self.backend,
                d=install_dir,
            )
            return

        self._ready = True
        log.info(
            "Lip-sync backend ready: {b} at {d}",
            b=self.backend,
            d=install_dir,
        )

    def setup_backend(self) -> None:
        """Clone and set up the backend repository.

        Downloads model weights and installs dependencies.
        This is a one-time operation.
        """
        spec = BACKENDS[self.backend]
        install_dir = Path(spec["install_dir"]).expanduser()

        if install_dir.exists():
            log.info("Backend {b} already installed at {d}", b=self.backend, d=install_dir)
            self._ready = True
            return

        log.info(
            "Installing {b} to {d}...", b=self.backend, d=install_dir
        )
        install_dir.parent.mkdir(parents=True, exist_ok=True)

        subprocess.run(
            ["git", "clone", "--depth", "1", spec["repo_url"], str(install_dir)],
            check=True,
        )

        # Install requirements if they exist
        req_file = install_dir / "requirements.txt"
        if req_file.exists():
            subprocess.run(
                ["pip", "install", "-r", str(req_file)],
                check=True,
            )

        self._backend_path = install_dir
        self._ready = True
        log.info("Backend {b} installed successfully", b=self.backend)

    def generate(
        self,
        portrait: str | Path,
        audio: str | Path,
        output: str | Path | None = None,
        *,
        still_mode: bool = False,
        enhancer: str | None = "gfpgan",
        preprocess: str = "crop",
    ) -> Path:
        """Generate a talking-head video.

        Args:
            portrait:   Path to the portrait image (face photo).
            audio:      Path to the driving audio file.
            output:     Output video path. Auto-generated if None.
            still_mode: Reduce head motion (SadTalker only).
            enhancer:   Face enhancer to use (``"gfpgan"`` or None).
            preprocess: Face preprocessing (``"crop"``, ``"resize"``, ``"full"``).

        Returns:
            Path to the generated video.
        """
        if not self._ready:
            self.load_model()
        if not self._ready:
            raise RuntimeError(
                f"Backend '{self.backend}' not installed. "
                f"Call setup_backend() first."
            )

        portrait = Path(portrait)
        audio = Path(audio)
        if not portrait.exists():
            raise FileNotFoundError(f"Portrait not found: {portrait}")
        if not audio.exists():
            raise FileNotFoundError(f"Audio not found: {audio}")

        if output is None:
            out_dir = Path(self.output_dir)
            out_dir.mkdir(parents=True, exist_ok=True)
            ts = int(time.time())
            output = out_dir / f"{ts}_talking_head.mp4"
        output = Path(output)
        output.parent.mkdir(parents=True, exist_ok=True)

        log.info(
            "Generating talking head: portrait={p}, audio={a}",
            p=portrait.name,
            a=audio.name,
        )
        t0 = time.monotonic()

        if self.backend == "sadtalker":
            result = self._run_sadtalker(
                portrait, audio, output, still_mode, enhancer, preprocess
            )
        elif self.backend == "liveportrait":
            result = self._run_liveportrait(portrait, audio, output)
        elif self.backend == "wav2lip":
            result = self._run_wav2lip(portrait, audio, output)
        else:
            raise ValueError(f"Unknown backend: {self.backend}")

        elapsed = time.monotonic() - t0
        log.info("Talking head done in {t:.1f}s → {path}", t=elapsed, path=result)
        return result

    def _run_sadtalker(
        self,
        portrait: Path,
        audio: Path,
        output: Path,
        still_mode: bool,
        enhancer: str | None,
        preprocess: str,
    ) -> Path:
        """Run SadTalker via its inference script."""
        script = self._backend_path / "inference.py"
        if not script.exists():
            raise FileNotFoundError(
                f"SadTalker inference.py not found at {script}. "
                "Ensure the repo was cloned correctly."
            )

        cmd = [
            "python", str(script),
            "--driven_audio", str(audio),
            "--source_image", str(portrait),
            "--result_dir", str(output.parent),
            "--preprocess", preprocess,
        ]

        if still_mode:
            cmd.append("--still")
        if enhancer:
            cmd.extend(["--enhancer", enhancer])
        if self._resolve_device() == "cpu":
            cmd.append("--cpu")

        log.debug("Running: {cmd}", cmd=" ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(self._backend_path))

        if result.returncode != 0:
            log.error("SadTalker failed: {err}", err=result.stderr[-500:])
            raise RuntimeError(f"SadTalker exited with code {result.returncode}")

        # SadTalker saves to result_dir — find the latest mp4
        generated = sorted(output.parent.glob("*.mp4"), key=lambda p: p.stat().st_mtime)
        if generated:
            latest = generated[-1]
            if latest != output:
                shutil.move(str(latest), str(output))
        return output

    def _run_liveportrait(
        self,
        portrait: Path,
        audio: Path,
        output: Path,
    ) -> Path:
        """Run LivePortrait via its inference script."""
        script = self._backend_path / "inference.py"
        if not script.exists():
            # Try alternate entry point
            script = self._backend_path / "run.py"

        if not script.exists():
            raise FileNotFoundError(
                f"LivePortrait inference script not found at {self._backend_path}. "
                "Ensure the repo was cloned correctly."
            )

        cmd = [
            "python", str(script),
            "--source_image", str(portrait),
            "--driving_audio", str(audio),
            "--output_path", str(output),
        ]
        if self._resolve_device() == "cpu":
            cmd.extend(["--device", "cpu"])

        log.debug("Running: {cmd}", cmd=" ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(self._backend_path))

        if result.returncode != 0:
            log.error("LivePortrait failed: {err}", err=result.stderr[-500:])
            raise RuntimeError(f"LivePortrait exited with code {result.returncode}")

        return output

    def _run_wav2lip(
        self,
        portrait: Path,
        audio: Path,
        output: Path,
    ) -> Path:
        """Run Wav2Lip via its inference script."""
        script = self._backend_path / "inference.py"
        if not script.exists():
            raise FileNotFoundError(
                f"Wav2Lip inference.py not found at {self._backend_path}."
            )

        # Wav2Lip needs a checkpoint — look for it
        ckpt = self._backend_path / "checkpoints" / "wav2lip_gan.pth"
        if not ckpt.exists():
            ckpt = self._backend_path / "checkpoints" / "wav2lip.pth"

        cmd = [
            "python", str(script),
            "--checkpoint_path", str(ckpt),
            "--face", str(portrait),
            "--audio", str(audio),
            "--outfile", str(output),
        ]

        log.debug("Running: {cmd}", cmd=" ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(self._backend_path))

        if result.returncode != 0:
            log.error("Wav2Lip failed: {err}", err=result.stderr[-500:])
            raise RuntimeError(f"Wav2Lip exited with code {result.returncode}")

        return output

    @property
    def is_ready(self) -> bool:
        return self._ready

    @staticmethod
    def available_backends() -> list[str]:
        return list(BACKENDS.keys())

    @staticmethod
    def check_installation() -> dict[str, bool]:
        """Check which backends are installed."""
        status: dict[str, bool] = {}
        for name, spec in BACKENDS.items():
            install_dir = Path(spec["install_dir"]).expanduser()
            status[name] = install_dir.exists()
        return status
