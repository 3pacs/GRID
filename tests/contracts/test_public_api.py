from __future__ import annotations


def test_public_exports():
    import contracts
    assert hasattr(contracts, "emit")
    assert hasattr(contracts, "pull_lifecycle")
    assert hasattr(contracts, "Dispatcher")
    assert hasattr(contracts, "new_correlation_id")
    assert hasattr(contracts, "correlation_scope")


def test_handlers_package_importable():
    import contracts.handlers  # noqa: F401


def test_handlers_docstring_reflects_shipped_modules():
    """Guard against the stale "empty in Phase 1" docstring returning.

    ``contracts/handlers/`` now ships handler modules routed by
    ``contracts/router.py::ROUTES``; the package docstring must describe the
    populated Phase 2 surface, not claim the package is empty.
    """
    import contracts.handlers

    doc = contracts.handlers.__doc__ or ""
    assert "empty in Phase 1" not in doc
    assert "Phase 2 contract handlers" in doc
