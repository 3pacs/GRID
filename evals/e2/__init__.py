"""E2: the forward scoreboard (GRID evals plan, milestone M2).

The single objective a future self-improving engine may climb. Every
forward-logged prediction stream (S10 hypothesis forward log, GEX-levels v1
paper log, later VS1 survivors and GEM-derived calls) is normalized by a
pinned adapter into one prediction record, resolved point-in-time (only
outcomes that were observable at the run instant), scored by rules fixed in
advance (``rules.json``, ``cost_model.json``) and appended to an append-only,
hash-chained ledger whose anchors are exported for an off-host witness.

Every file here is hash-pinned in ``MANIFEST.sha256``; a change is a new
scoreboard version with its own ledger file (old versions are never
rescored in place). See ``evals/e2/README.md``.
"""

VERSION = "e2-v1"
