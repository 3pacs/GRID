from __future__ import annotations

from uuid import UUID

from contracts.correlation import (
    new_correlation_id,
    get_current_correlation_id,
    correlation_scope,
)


def test_new_correlation_id_is_uuid():
    cid = new_correlation_id()
    assert isinstance(cid, UUID)


def test_get_current_returns_none_outside_scope():
    assert get_current_correlation_id() is None


def test_correlation_scope_sets_and_resets():
    outer_before = get_current_correlation_id()
    with correlation_scope() as cid:
        assert get_current_correlation_id() == cid
    assert get_current_correlation_id() == outer_before


def test_correlation_scope_accepts_explicit_id():
    fixed = new_correlation_id()
    with correlation_scope(fixed) as cid:
        assert cid == fixed
        assert get_current_correlation_id() == fixed


def test_nested_scopes_restore_parent():
    with correlation_scope() as parent:
        with correlation_scope() as child:
            assert get_current_correlation_id() == child
            assert child != parent
        assert get_current_correlation_id() == parent
