"""Enable ``python -m intelligence.hermes`` as an alias for the CLI."""

from __future__ import annotations

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
