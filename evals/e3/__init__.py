"""E3: hill-climb harness. Slice E3A: the candidate (trial) ledger.

Every candidate a hill-climber generates is recorded from the moment it is
proposed, before any data is read, through every stage transition, including
the ones that are screened out, abandoned, withdrawn or fail. The ledger is
append-only, hash-chained and externally anchored, so multiple-testing budgets
(E3B/E3C) and yield per trial (E4B) are computed over every candidate, not
only the survivors. See ``evals/e3/README.md``.

Every file here is hash-pinned in ``MANIFEST.sha256``; a change is a new
suite version.
"""

VERSION = "e3-v1"
