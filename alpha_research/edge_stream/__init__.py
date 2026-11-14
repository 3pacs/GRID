"""Edge stream: Hermes registers and runs edge-search looks continuously, honestly.

Design: ``docs/specs/2026-11-13-edge-stream-design.md``; runbook ``docs/reference/EDGE_STREAM.md``.
Modules: ``policy`` (frozen numbers), ``ledger`` (insert-only records on the S11 hash chain),
``sanity`` (the gate), ``families`` (rule code + queue), ``stats`` (pass test), ``data``
(Polygon bars, cached), ``runner`` (one cycle), ``hermes`` (daily spawn). It never places,
reviews or cancels an order and never promotes anything.
"""
