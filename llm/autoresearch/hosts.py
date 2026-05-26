"""Per-host hardware profiles, runtime GPU detection, and model-fit logic.

Hardware specs go stale the moment a card is swapped, so this module does
**not** trust a hardcoded table. Resolution order for a host profile:

    1. runtime detection via ``nvidia-smi`` (authoritative, self-healing)
    2. an operator-maintained JSON override (``host_profiles.json``)
    3. a baked-in fallback table — explicitly marked STALE, last resort only

Recommendations are expressed as a **VRAM-tier decision tree** keyed on the
*resolved* VRAM/arch, so they stay correct regardless of which cards are in
a box today. GPU architecture (detected from the card name) drives the
flash-attention / FP8 capability flags.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Bytes-per-weight by quant scheme (approximate, for VRAM estimation).
QUANT_BPW: dict[str, float] = {
    "f16": 16.0, "q8_0": 8.5, "q6_k": 6.6, "q5_k_m": 5.7,
    "q4_k_m": 4.8, "q4_0": 4.5, "q3_k_m": 3.9, "iq4_xs": 4.3, "iq3_xxs": 3.1,
}

DEFAULT_OVERHEAD_GB = 2.5
PROFILE_OVERRIDE_PATH = Path(__file__).parent / "host_profiles.json"


@dataclass(frozen=True)
class HostProfile:
    """Resolved hardware profile for one fleet host.

    Attributes:
        host: Logical name (matches Endpoint.host).
        vram_gb: Usable VRAM per inference card, in GB.
        gpus: Number of GPUs available for one model.
        arch: GPU architecture (drives feature support).
        flash_attn: Whether Flash Attention is supported.
        fp8: Whether FP8 weights/KV are supported.
        gpu_name: Detected GPU model string (if known).
        source: Where this profile came from (detected | override | fallback).
        notes: Free-text caveats.
    """

    host: str
    vram_gb: float
    gpus: int = 1
    arch: str = "unknown"
    flash_attn: bool = True
    fp8: bool = False
    gpu_name: str = ""
    source: str = "fallback"
    notes: str = ""

    @property
    def total_vram_gb(self) -> float:
        return self.vram_gb * self.gpus


# Architecture capability map. (substring in GPU name) -> (arch, flash_attn, fp8)
_ARCH_RULES: tuple[tuple[tuple[str, ...], str, bool, bool], ...] = (
    (("b200", "b100", "gb200", "gb10", "rtx pro", "blackwell", "rtx 50"), "blackwell", True, True),
    (("h100", "h200", "hopper"), "hopper", True, True),
    (("l40", "l4", "rtx 40", "ada"), "ada", True, True),
    (("a100", "a40", "a6000", "a10", "rtx 30", "ampere"), "ampere", True, False),
    (("t4", "rtx 20", "titan rtx", "turing"), "turing", True, False),
    (("p100", "p40", "gtx 10", "titan xp", "pascal"), "pascal", False, False),
    (("titan x", "gtx 9", "maxwell", "m40", "m60"), "maxwell", False, False),
)


def arch_from_name(gpu_name: str) -> tuple[str, bool, bool]:
    """Infer ``(arch, flash_attn, fp8)`` from a GPU name string.

    Pascal "TITAN Xp" is matched before Maxwell "TITAN X" by rule order.
    Unknown cards default to a modern assumption (flash-attn on, fp8 off).
    """
    low = gpu_name.lower()
    for needles, arch, fa, fp8 in _ARCH_RULES:
        if any(n in low for n in needles):
            return arch, fa, fp8
    return "unknown", True, False


def detect_local_gpus() -> list[tuple[str, float]]:
    """Return ``[(gpu_name, vram_gb), ...]`` for the local machine.

    Uses ``nvidia-smi``; returns an empty list if it is unavailable (e.g. a
    CPU-only node or a sandbox). Never raises.
    """
    try:
        out = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        if out.returncode != 0:
            return []
        gpus: list[tuple[str, float]] = []
        for line in out.stdout.strip().splitlines():
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 2:
                name = parts[0]
                try:
                    vram_gb = round(float(parts[1]) / 1024.0, 1)  # MiB -> GB
                except ValueError:
                    continue
                gpus.append((name, vram_gb))
        return gpus
    except Exception:
        return []


def detect_local_profile(host: str) -> HostProfile | None:
    """Build a HostProfile for the local machine via nvidia-smi.

    Groups identical cards (vram per card + count). Returns None if no GPU
    is detected. Run this *on each host* to populate ``host_profiles.json``.
    """
    gpus = detect_local_gpus()
    if not gpus:
        return None
    name, vram = gpus[0][0], gpus[0][1]
    arch, fa, fp8 = arch_from_name(name)
    return HostProfile(
        host=host, vram_gb=vram, gpus=len(gpus), arch=arch,
        flash_attn=fa, fp8=fp8, gpu_name=name, source="detected",
        notes=f"detected {len(gpus)}x {name}",
    )


# Baked-in fallback — STALE by nature. Used only when neither runtime
# detection nor an operator override is available. DO NOT trust these
# numbers for capacity planning; run `--detect` on each host instead.
_FALLBACK_PROFILES: dict[str, HostProfile] = {
    "grid-svr": HostProfile("grid-svr", 48.0, 1, "blackwell", True, True, source="fallback", notes="STALE fallback"),
    "panda": HostProfile("panda", 24.0, 1, "ampere", True, False, source="fallback", notes="STALE fallback"),
    "ocr-node": HostProfile("ocr-node", 16.0, 1, "ampere", True, False, source="fallback", notes="STALE fallback"),
    "koala": HostProfile("koala", 12.0, 2, "maxwell", False, False, source="fallback", notes="STALE fallback"),
    "z400": HostProfile("z400", 12.0, 1, "pascal", False, False, source="fallback", notes="STALE fallback"),
}


def load_host_profiles(override_path: Path = PROFILE_OVERRIDE_PATH) -> dict[str, HostProfile]:
    """Load operator-maintained host profiles from JSON, else the fallback.

    JSON schema: ``{"<host>": {"vram_gb": 24, "gpus": 1, "gpu_name": "...",
    "arch": "ampere", ...}}``. ``arch``/``flash_attn``/``fp8`` are inferred
    from ``gpu_name`` when omitted.
    """
    if not override_path.exists():
        return dict(_FALLBACK_PROFILES)
    try:
        raw = json.loads(override_path.read_text(encoding="utf-8"))
    except Exception:
        return dict(_FALLBACK_PROFILES)

    profiles: dict[str, HostProfile] = {}
    for host, spec in raw.items():
        gpu_name = spec.get("gpu_name", "")
        arch, fa, fp8 = arch_from_name(gpu_name) if gpu_name else (
            spec.get("arch", "unknown"), spec.get("flash_attn", True), spec.get("fp8", False)
        )
        profiles[host] = HostProfile(
            host=host,
            vram_gb=float(spec.get("vram_gb", 0.0)),
            gpus=int(spec.get("gpus", 1)),
            arch=spec.get("arch", arch),
            flash_attn=bool(spec.get("flash_attn", fa)),
            fp8=bool(spec.get("fp8", fp8)),
            gpu_name=gpu_name,
            source="override",
            notes=spec.get("notes", "operator override"),
        )
    return profiles


# Resolved at import: override file if present, else stale fallback.
HOST_PROFILES: dict[str, HostProfile] = load_host_profiles()


@dataclass(frozen=True)
class ModelSpec:
    """A candidate model variant to consider deploying."""

    name: str
    params_b: float
    quant: str
    meets_bar: bool = True
    repo: str = ""

    def estimated_vram_gb(self, overhead_gb: float = DEFAULT_OVERHEAD_GB) -> float:
        bpw = QUANT_BPW.get(self.quant.lower(), 5.0)
        weights_gb = (self.params_b * 1e9 * bpw / 8.0) / 1e9
        return round(weights_gb + overhead_gb, 1)


def fits_on(spec: ModelSpec, host: HostProfile, *, allow_multi_gpu: bool = True) -> tuple[bool, str]:
    """Decide whether ``spec`` fits ``host`` and note any caveats."""
    need = spec.estimated_vram_gb()
    budget = host.total_vram_gb if allow_multi_gpu else host.vram_gb
    if need > budget:
        return False, f"needs ~{need}GB > {budget}GB available on {host.host}"
    caveats = []
    if not host.flash_attn:
        caveats.append("no flash-attn (slower KV)")
    if spec.params_b >= 20 and host.gpus > 1 and need > host.vram_gb:
        caveats.append("requires tensor-split across GPUs")
    note = f"fits (~{need}GB / {budget}GB)" + (" — " + "; ".join(caveats) if caveats else "")
    return True, note


def recommend_for_host(host: HostProfile) -> dict[str, Any]:
    """VRAM-tier decision tree for a Qwen 3.6+ deployment.

    Returns a dict with the recommended model, quant, llama-server flags,
    and rationale. Keyed on *resolved* VRAM/arch so it never goes stale.
    """
    vram = host.total_vram_gb
    fa = "-fa" if host.flash_attn else "(no -fa: arch lacks it)"
    kv = "--cache-type-k q8_0 --cache-type-v q8_0"
    mtp = "--spec-type draft-mtp"

    if vram >= 40:
        rec = dict(model="Qwen3.6-27B (dense)", quant="Q6_K",
                   rationale="ample VRAM — run dense at high quant for best quality")
    elif vram >= 24:
        rec = dict(model="Qwen3.6-27B (dense)", quant="Q5_K_M",
                   rationale="fits dense 27B at near-lossless quant")
    elif vram >= 16:
        rec = dict(model="Qwen3.6-35B-A3B (MoE)", quant="Q4_K_M",
                   flags_extra='-ot ".ffn_.*_exps.=CPU"',
                   rationale="MoE (3B active) with expert-offload fits 16GB and stays fast")
    elif vram >= 12:
        if host.gpus > 1 and vram >= 20:
            rec = dict(model="Qwen3.6-27B (dense)", quant="Q4_K_M",
                       flags_extra="-ts 1,1 -sm layer",
                       rationale="tensor-split dense 27B across multiple GPUs")
        else:
            rec = dict(model="Qwen3.6-35B-A3B (MoE)", quant="IQ4_XS",
                       flags_extra='-ot ".ffn_.*_exps.=CPU"  # heavy CPU offload; expect lower tok/s',
                       rationale="12GB is tight for the Qwen3.6 floor — MoE + heavy offload is the only on-bar option")
    else:
        return dict(model=None, quant=None,
                    rationale=f"{vram:.0f}GB cannot hold any Qwen 3.6 model in VRAM — "
                              "repurpose as embeddings/draft/utility node instead of a reasoner")

    flags = f"{mtp} {fa} {kv} -ngl 99"
    if rec.get("flags_extra"):
        flags += " " + rec.pop("flags_extra")
    rec["flags"] = flags
    rec["arch"] = host.arch
    rec["resolved_vram_gb"] = vram
    rec["profile_source"] = host.source
    return rec
