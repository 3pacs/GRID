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
