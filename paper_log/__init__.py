"""GRID — forward paper-logging jobs.

Each sub-package under ``paper_log`` implements one pre-registered forward
test (see ``docs/paper_log/``). Nothing in this package writes to the GRID
database, places orders, or calls a brokerage API — it only reads market
data and GRID's read-only signal tables, and appends to an append-only
JSONL log on disk.
"""

from __future__ import annotations
