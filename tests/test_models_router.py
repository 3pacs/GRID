"""Tests for the model-registry API router pagination envelope."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_engine(total: int, rows: list[dict]) -> MagicMock:
    """Build a MagicMock engine whose two execute() calls return count then rows."""
    engine = MagicMock()
    conn = MagicMock()

    count_result = MagicMock()
    count_result.scalar_one.return_value = total
    rows_result = MagicMock()
    rows_result.fetchall.return_value = rows

    conn.execute.side_effect = [count_result, rows_result]
    engine.connect.return_value.__enter__.return_value = conn
    return engine


class TestGetAllPaginationEnvelope:
    """get_all must expose the total/limit/offset/has_more contract."""

    def _call(self, total: int, rows: list[dict], limit: int, offset: int) -> dict:
        engine = _make_engine(total, rows)
        with patch("api.routers.models.get_db_engine", return_value=engine):
            from api.routers.models import get_all

            return get_all(layer=None, state=None, limit=limit, offset=offset, _token="test")

    def test_envelope_keys_present(self):
        result = self._call(total=3, rows=[{"id": 1}], limit=1, offset=0)
        assert set(result) >= {"models", "total", "limit", "offset", "has_more"}

    def test_has_more_true_when_more_pages_remain(self):
        result = self._call(total=10, rows=[{"id": 1}, {"id": 2}], limit=2, offset=0)
        assert result["total"] == 10
        assert result["has_more"] is True

    def test_has_more_false_on_last_page(self):
        result = self._call(total=4, rows=[{"id": 3}, {"id": 4}], limit=2, offset=2)
        assert result["has_more"] is False

    def test_has_more_false_at_exact_boundary(self):
        result = self._call(total=4, rows=[{"id": 1}, {"id": 2}], limit=2, offset=2)
        assert result["has_more"] is False

    def test_models_serialized_via_row_to_dict(self):
        result = self._call(total=1, rows=[{"id": 7, "created_at": None}], limit=100, offset=0)
        assert result["models"] == [{"id": 7, "created_at": None}]
