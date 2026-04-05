"""Tests for vault API router."""

from __future__ import annotations

import pytest


class TestVaultRouter:
    def test_router_has_correct_prefix(self):
        from api.routers.vault import router
        assert router.prefix == "/api/v1/vault"

    def test_router_has_tag(self):
        from api.routers.vault import router
        assert "vault" in router.tags

    def test_list_notes_endpoint_exists(self):
        from api.routers.vault import router
        paths = [r.path for r in router.routes]
        assert any(p.endswith("/notes") for p in paths)

    def test_search_endpoint_exists(self):
        from api.routers.vault import router
        paths = [r.path for r in router.routes]
        assert any(p.endswith("/search") for p in paths)

    def test_dashboard_endpoint_exists(self):
        from api.routers.vault import router
        paths = [r.path for r in router.routes]
        assert any(p.endswith("/dashboard") for p in paths)

    def test_sync_endpoint_exists(self):
        from api.routers.vault import router
        paths = [r.path for r in router.routes]
        assert any(p.endswith("/sync") for p in paths)

    def test_status_change_endpoint_exists(self):
        from api.routers.vault import router
        paths = [r.path for r in router.routes]
        assert any(p.endswith("/notes/{note_id}/status") for p in paths)
