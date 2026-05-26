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
import os
import subprocess
from collections import Counter
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

# Live fleet inventory dashboard. It SSH-polls every host on a fixed cycle and
# publishes per-GPU name/VRAM/architecture, so it is the self-updating source
# of truth for host profiles (overridable via env for a different deployment).
DEFAULT_SNAPSHOT_URL = "http://network.stepdad.finance/api/snapshot"
SNAPSHOT_TIMEOUT_SECONDS = 6.0


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


# Capability map keyed on an architecture *label* (e.g. from the dashboard's
# `architecture` field), as opposed to a full GPU name. -> (flash_attn, fp8).
_ARCH_CAPS: dict[str, tuple[bool, bool]] = {
    "blackwell": (True, True), "hopper": (True, True),
    "ada": (True, True), "ada lovelace": (True, True),
    "ampere": (True, False), "turing": (True, False), "volta": (True, False),
    "pascal": (False, False), "maxwell": (False, False),
}


def arch_caps(arch: str) -> tuple[bool, bool]:
    """Infer ``(flash_attn, fp8)`` from an architecture label.

    Unknown architectures default to a modern assumption (flash-attn on,
    fp8 off) to avoid wrongly disabling a capable card.
    """
    return _ARCH_CAPS.get(arch.strip().lower(), (True, False))


def _aggregate_profile(
    host: str, cards: list[tuple[str, float, str]], source: str
) -> HostProfile:
    """Fold a host's GPUs into one HostProfile.

    ``cards`` is ``[(gpu_name, vram_gb, arch_label), ...]``; an empty
    ``arch_label`` falls back to inferring arch/caps from the name. VRAM is
    summed across *all* cards (so heterogeneous boxes report their true
    total), capabilities are the conservative intersection (a feature is
    only claimed if every card supports it), and ``vram_gb`` is the per-card
    average so that ``vram_gb * gpus == total``.
    """
    archs: list[str] = []
    fa_flags: list[bool] = []
    fp8_flags: list[bool] = []
    for name, _vram, arch_label in cards:
        if arch_label:
            arch = arch_label.strip().lower()
            fa, fp8 = arch_caps(arch)
        else:
            arch, fa, fp8 = arch_from_name(name)
        archs.append(arch)
        fa_flags.append(fa)
        fp8_flags.append(fp8)

    total_vram = sum(v for _, v, _ in cards)
    count = len(cards)
    unique_archs = sorted(set(archs))
    arch_label = unique_archs[0] if len(unique_archs) == 1 else "mixed:" + "+".join(unique_archs)
    name_counts = Counter(name for name, _, _ in cards)
    gpu_name = ", ".join(
        f"{n}x {nm}" if n > 1 else nm for nm, n in name_counts.items()
    )
    return HostProfile(
        host=host,
        vram_gb=round(total_vram / count, 1) if count else 0.0,
        gpus=count,
        arch=arch_label,
        flash_attn=all(fa_flags),
        fp8=all(fp8_flags),
        gpu_name=gpu_name,
        source=source,
        notes=f"{source}: {count}x [{gpu_name}] = {total_vram:.0f}GB total",
    )


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

    Sums VRAM across *all* detected cards (heterogeneous boxes are common on
    this fleet, so first-card-times-count would undercount), and takes the
    conservative capability intersection. Returns None if no GPU is detected.
    """
    gpus = detect_local_gpus()
    if not gpus:
        return None
    cards = [(name, vram, "") for name, vram in gpus]
    return _aggregate_profile(host, cards, "detected")


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


def _parse_override(override_path: Path) -> dict[str, HostProfile]:
    """Parse the operator-maintained JSON override file (or ``{}``)."""
    if not override_path.exists():
        return {}
    try:
        raw = json.loads(override_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

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


def _snapshot_cards(host_entry: dict[str, Any]) -> list[tuple[str, float, str]]:
    """Extract real NVIDIA GPUs from one dashboard host entry.

    The snapshot's ``gpus`` list also contains display adapters (Matrox,
    Intel iGPU) with no UUID / null memory / blank architecture — those are
    filtered out so only inference-capable cards count.
    """
    cards: list[tuple[str, float, str]] = []
    for gpu in host_entry.get("gpus") or []:
        mib = gpu.get("memoryTotalMiB")
        uuid = gpu.get("uuid") or ""
        arch = (gpu.get("architecture") or "").strip()
        if not mib or not uuid or not arch:
            continue
        name = (gpu.get("name") or "").strip()
        cards.append((name, round(float(mib) / 1024.0, 1), arch))
    return cards


def profiles_from_snapshot(
    url: str = DEFAULT_SNAPSHOT_URL, *, timeout: float = SNAPSHOT_TIMEOUT_SECONDS
) -> dict[str, HostProfile]:
    """Build host profiles from the live fleet inventory dashboard.

    Only hosts reporting ``status == "ok"`` with at least one real NVIDIA GPU
    are returned. Requires ``requests``; raises on network/parse failure so
    the caller can decide whether to fall back.
    """
    import requests  # lazy: keep this module importable without requests

    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    hosts = ((resp.json() or {}).get("snapshot") or {}).get("hosts") or []
    profiles: dict[str, HostProfile] = {}
    for host_entry in hosts:
        if host_entry.get("status") != "ok":
            continue
        cards = _snapshot_cards(host_entry)
        if not cards:
            continue
        host = host_entry.get("id") or host_entry.get("hostname")
        if host:
            profiles[host] = _aggregate_profile(host, cards, "snapshot")
    return profiles


def _snapshot_enabled(use_snapshot: bool | None) -> bool:
    if use_snapshot is not None:
        return use_snapshot
    return os.environ.get("GRID_FLEET_SNAPSHOT_DISABLE", "").lower() not in ("1", "true", "yes")


def load_host_profiles(
    override_path: Path = PROFILE_OVERRIDE_PATH,
    *,
    use_snapshot: bool | None = None,
    snapshot_url: str | None = None,
) -> dict[str, HostProfile]:
    """Resolve host profiles, highest-priority source winning per host.

    Layering (later overrides earlier): baked-in fallback -> operator JSON
    override -> live dashboard snapshot. The snapshot is only consulted when
    ``use_snapshot`` is True (default reads ``GRID_FLEET_SNAPSHOT_DISABLE``);
    a network/parse failure degrades gracefully to the lower layers. JSON
    schema: ``{"<host>": {"vram_gb": 24, "gpus": 1, "gpu_name": "...",
    "arch": "ampere", ...}}`` (arch/flash_attn/fp8 inferred when omitted).
    """
    profiles: dict[str, HostProfile] = dict(_FALLBACK_PROFILES)
    profiles.update(_parse_override(override_path))
    if _snapshot_enabled(use_snapshot):
        url = snapshot_url or os.environ.get("GRID_FLEET_SNAPSHOT_URL", DEFAULT_SNAPSHOT_URL)
        try:
            profiles.update(profiles_from_snapshot(url))
        except Exception:
            pass  # graceful degradation — keep override/fallback layers
    return profiles


# Resolved at import: offline layers only (override file, else stale fallback)
# so importing the package never does network I/O. Pass use_snapshot=True (the
# CLI does) to fold in the live dashboard at runtime.
HOST_PROFILES: dict[str, HostProfile] = load_host_profiles(use_snapshot=False)


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
