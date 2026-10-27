"""Helpers for output directories that may be dangling symlinks locally."""

from __future__ import annotations

from pathlib import Path
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import os
import stat

from loguru import logger as log


class OutputStateError(ValueError):
    """An explicit API state capability is missing or unsafe; never fall back."""


def _trusted_state_root(value) -> Path:
    if value is None:
        raise OutputStateError("explicit API state root required")
    root = Path(value)
    if not root.is_absolute() or ".." in root.parts:
        raise OutputStateError("API state root must be absolute without traversal")
    if any(part.is_symlink() for part in (root, *root.parents)):
        raise OutputStateError("API state root may not traverse symlinks")
    if not root.is_dir():
        raise OutputStateError("API state root must already exist as a directory")
    metadata = root.stat()
    if metadata.st_uid != os.geteuid() or metadata.st_mode & 0o022:
        raise OutputStateError("API state root must be process-owned and not group/world writable")
    return root


@dataclass(frozen=True)
class APIOutputState:
    """Explicit per-process API capability; no environment/default/history copy.

    Caller owns this root and excludes hostile concurrent mutations. Path checks
    refuse existing symlinks; they are not a filesystem sandbox against another
    trusted same-UID writer racing after a returned Path is checked.
    """
    state_root: Path
    source_directory: Path
    namespace: str = "market_briefings"

    def __post_init__(self):
        root = _trusted_state_root(self.state_root)
        source = Path(self.source_directory)
        if not source.is_absolute() or ".." in source.parts:
            raise OutputStateError("explicit absolute source directory required")
        if (self.namespace != "market_briefings" or source.name != self.namespace
                or source.parent.name != "outputs"):
            raise OutputStateError("unsupported API output namespace")
        # State must be independently supplied, not the source directory itself.
        bundle = source.parent.parent
        if root == bundle or bundle in root.parents or root in source.parents:
            raise OutputStateError("API state and source trees must be separate")
        object.__setattr__(self, "state_root", root)
        object.__setattr__(self, "source_directory", source)

    def directory(self, requested: Path) -> Path:
        if Path(requested) != self.source_directory:
            raise OutputStateError("undeclared API output caller/directory")
        root = _trusted_state_root(self.state_root)
        target = root / self.namespace
        if target.is_symlink():
            raise OutputStateError("API output directory may not be a symlink")
        target.mkdir(mode=0o700, exist_ok=True)
        if target.is_symlink() or not target.is_dir() or target.resolve() != target:
            raise OutputStateError("unsafe API output directory")
        # A protected root does not make an existing namespace or its files
        # safe: another UID could own or modify them independently.
        _trusted_state_root(target)
        for item in target.iterdir():
            metadata = item.lstat()
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1
                    or metadata.st_uid != os.geteuid() or metadata.st_mode & 0o022):
                raise OutputStateError("API output entries must be protected process-owned regular files")
        return target

    def file(self, filename: str) -> Path:
        if (not isinstance(filename, str) or not filename or filename in (".", "..")
                or "/" in filename or "\\" in filename or "\0" in filename
                or not filename.endswith(".md")):
            raise OutputStateError("a single briefing .md filename is required")
        target = self.directory(self.source_directory) / filename
        if target.is_symlink() or (target.exists() and not target.is_file()):
            raise OutputStateError("unsafe briefing file")
        return target


_API_OUTPUT_STATE = ContextVar("grid_api_output_state", default=None)


@contextmanager
def api_output_state(binding: APIOutputState):
    if not isinstance(binding, APIOutputState):
        raise OutputStateError("explicit API output capability required")
    token = _API_OUTPUT_STATE.set(binding)
    try:
        yield
    finally:
        _API_OUTPUT_STATE.reset(token)


def ensure_output_dir(path: Path) -> Path:
    """Return a writable directory for ``path``.

    GRID worktrees often carry ``outputs/*`` symlinks into ``/data`` from a
    different host. On local Mac audit runs those symlinks can be dangling,
    which makes ``mkdir(..., exist_ok=True)`` raise ``FileExistsError``.
    Prefer the configured path when it works, otherwise fall back to a local
    sibling directory named ``_{name}_local`` so read/write routes stay honest
    instead of crashing at import or first use.
    """

    binding = _API_OUTPUT_STATE.get()
    if binding is not None:
        return binding.directory(path)

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
