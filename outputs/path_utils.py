"""Helpers for output directories that may be dangling symlinks locally."""

from __future__ import annotations

from pathlib import Path

from loguru import logger as log


def ensure_output_dir(path: Path) -> Path:
    """Return a writable directory for ``path``.

    GRID worktrees often carry ``outputs/*`` symlinks into ``/data`` from a
    different host. On local Mac audit runs those symlinks can be dangling,
    which makes ``mkdir(..., exist_ok=True)`` raise ``FileExistsError``.
    Prefer the configured path when it works, otherwise fall back to a local
    sibling directory named ``_{name}_local`` so read/write routes stay honest
    instead of crashing at import or first use.
    """

    try:
        path.mkdir(parents=True, exist_ok=True)
        return path
    except FileExistsError:
        if path.is_symlink():
            target = path.resolve(strict=False)
            if target.exists() and target.is_dir():
                return target
            log.warning(
                "output_dir: dangling symlink for {path} -> {target}; using local fallback",
                path=path,
                target=target,
            )
        else:
            log.warning("output_dir: {path} exists but is not a directory", path=path)
    except Exception as exc:
        log.warning("output_dir: could not create {path}: {error}", path=path, error=str(exc))

    fallback = path.parent / f"_{path.name}_local"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback
