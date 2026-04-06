"""Tests for vault API router.

Stubs api.auth to avoid heavy transitive deps (psycopg2, jose, passlib)
that may not be installed in lightweight CI environments.
"""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

# ---------------------------------------------------------------------------
# Stub api.auth before vault.py imports it — avoids psycopg2/jose/passlib
# ---------------------------------------------------------------------------

_auth_stub = ModuleType("api.auth")
_auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
sys.modules.setdefault("api.auth", _auth_stub)

# Also stub api.dependencies if missing heavy db deps
if "api.dependencies" not in sys.modules:
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub


class TestVaultRouter:
    @pytest.fixture(autouse=True)
    def _import_router(self):
        from api.routers.vault import router
        self.router = router

    def test_router_has_correct_prefix(self):
        assert self.router.prefix == "/api/v1/vault"

    def test_router_has_tag(self):
        assert "vault" in self.router.tags

    def test_list_notes_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/notes") for p in paths)

    def test_search_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/search") for p in paths)

    def test_dashboard_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/dashboard") for p in paths)

    def test_sync_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/sync") for p in paths)

    def test_status_change_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/notes/{note_id}/status") for p in paths)

    def test_create_note_endpoint_exists(self):
        methods = {}
        for r in self.router.routes:
            if hasattr(r, "methods"):
                for m in r.methods:
                    methods.setdefault(r.path, set()).add(m)
        notes_paths = [p for p in methods if p.endswith("/notes")]
        assert any("POST" in methods[p] for p in notes_paths)

    def test_actions_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/actions") for p in paths)

    def test_valid_statuses_includes_all_workflow_states(self):
        """Ensure PATCH status endpoint accepts all statuses used by frontend and agent."""
        import inspect
        from api.routers import vault as vault_mod

        source = inspect.getsource(vault_mod.update_note_status)
        for status in ("inbox", "review", "evaluating", "approved", "rejected", "active", "archived", "done"):
            assert status in source, f"Status '{status}' missing from valid_statuses"
