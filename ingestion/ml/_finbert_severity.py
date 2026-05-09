"""Severity classifier for FinBERT scoring failures.

Lives in a torch-free module so unit tests can import the classifier
without forcing torch/transformers into the test environment. CLAUDE.md
reserves ERROR for unhandled application bugs; transformers wraps GPU
OOM and cache-corruption issues as a generic
``Could not import module 'BertForSequenceClassification'`` which is
environmental, not a bug. We downgrade those to WARNING.
"""

from __future__ import annotations


def is_transformers_init_wrap(exc: BaseException) -> bool:
    """Return True if exc is a known environmental transformers-init failure.

    Matches the OOM-wrap message and bare OSError/MemoryError instances
    which are the documented failure modes on the grid-svr P100 box.
    """
    err_str = str(exc)
    return (
        "Could not import module" in err_str
        or "BertForSequenceClassification" in err_str
        or isinstance(exc, (OSError, MemoryError))
    )
