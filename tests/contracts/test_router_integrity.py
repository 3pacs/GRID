from __future__ import annotations


import pytest

from contracts.router import ROUTES, resolve_handler


def test_routes_is_a_dict():
    assert isinstance(ROUTES, dict)


def test_every_handler_in_routes_is_importable():
    """Every handler path in ROUTES must resolve to a real callable.

    In Phase 1 ROUTES is empty, so this test passes trivially. In Phase 2+
    it catches typos and renames at test time instead of at dispatch time.
    """
    for contract_type, handler_paths in ROUTES.items():
        assert isinstance(handler_paths, list)
        for path in handler_paths:
            handler = resolve_handler(path)
            assert callable(handler), f"{path} is not callable"


def test_resolve_handler_imports_dotted_path():
    # Use an existing stdlib function as a stand-in.
    handler = resolve_handler("json.dumps")
    assert callable(handler)


def test_resolve_handler_raises_on_missing_module():
    with pytest.raises(ModuleNotFoundError):
        resolve_handler("nonexistent_module_xyz.func")


def test_resolve_handler_raises_on_missing_attribute():
    with pytest.raises(AttributeError):
        resolve_handler("json.not_a_real_function")
