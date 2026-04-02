"""
Tests for the A2A (Agent-to-Agent) protocol module.

Tests Agent Cards, A2A client, and task manager without live servers.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from a2a.agent_card import AgentCard, AgentSkill, build_grid_agent_card
from a2a.client import A2AClient, A2ATask, TaskState
from a2a.server import A2ATaskManager


# ---------------------------------------------------------------------------
# AgentCard
# ---------------------------------------------------------------------------


class TestAgentCard:
    def test_create_card(self) -> None:
        card = AgentCard(
            name="Test Agent",
            description="A test agent",
            url="https://test.example.com",
        )
        assert card.name == "Test Agent"
        assert card.version == "1.0.0"
        assert "text/plain" in card.input_modes

    def test_card_to_dict(self) -> None:
        card = AgentCard(
            name="Test",
            description="Test",
            url="https://test.example.com",
            skills=[
                AgentSkill(
                    id="forecast",
                    name="Forecast",
                    description="Generate forecasts",
                    tags=["forecast"],
                )
            ],
        )
        d = card.to_dict()
        assert d["name"] == "Test"
        assert len(d["skills"]) == 1
        assert d["skills"][0]["id"] == "forecast"
        assert "payment" not in d  # None payment excluded

    def test_card_with_payment(self) -> None:
        card = AgentCard(
            name="Test",
            description="Test",
            url="https://test.example.com",
            payment={"protocol": "x402", "token": "USDC"},
        )
        d = card.to_dict()
        assert d["payment"]["protocol"] == "x402"

    def test_build_grid_agent_card(self) -> None:
        mock_settings = MagicMock()
        mock_settings.X402_ENABLED = False

        with patch("config.settings", mock_settings):
            card = build_grid_agent_card("https://grid.example.com")

        assert card.name == "GRID Trading Intelligence"
        assert len(card.skills) == 6
        assert card.url == "https://grid.example.com"
        assert card.payment is None

    def test_build_grid_agent_card_with_x402(self) -> None:
        mock_settings = MagicMock()
        mock_settings.X402_ENABLED = True
        mock_settings.X402_NETWORK = "base"
        mock_settings.X402_TOKEN = "USDC"
        mock_settings.X402_RECEIVER_ADDRESS = "0xabc"
        mock_settings.X402_PRICE_FORECAST = 0.01
        mock_settings.X402_PRICE_PREDICTION = 0.02
        mock_settings.X402_PRICE_SIGNAL = 0.01
        mock_settings.X402_PRICE_REGIME = 0.005
        mock_settings.X402_PRICE_ACTOR = 0.02
        mock_settings.X402_PRICE_OPTIONS = 0.02

        with patch("config.settings", mock_settings):
            card = build_grid_agent_card("https://grid.example.com")

        assert card.payment is not None
        assert card.payment["protocol"] == "x402"
        assert card.payment["pricing"]["forecast"] == 0.01


class TestAgentSkill:
    def test_create_skill(self) -> None:
        skill = AgentSkill(
            id="test",
            name="Test Skill",
            description="Does testing",
            tags=["test"],
            examples=["Run tests"],
        )
        assert skill.id == "test"
        assert "test" in skill.tags


# ---------------------------------------------------------------------------
# A2AClient
# ---------------------------------------------------------------------------


class TestA2AClient:
    def test_discover_success(self) -> None:
        client = A2AClient(timeout=5)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "name": "Remote Agent",
            "skills": [{"id": "analyze"}],
        }

        with patch("a2a.client.requests.get", return_value=mock_resp):
            card = client.discover("https://agent.example.com")

        assert card is not None
        assert card["name"] == "Remote Agent"

    def test_discover_cached(self) -> None:
        client = A2AClient()
        cached_card = {"name": "Cached", "skills": []}
        client._card_cache["https://agent.example.com/.well-known/agent.json"] = cached_card

        card = client.discover("https://agent.example.com")
        assert card is cached_card

    def test_discover_failure(self) -> None:
        client = A2AClient()

        with patch("a2a.client.requests.get", side_effect=ConnectionError):
            card = client.discover("https://offline.example.com")

        assert card is None

    def test_discover_404(self) -> None:
        client = A2AClient()

        mock_resp = MagicMock()
        mock_resp.status_code = 404

        with patch("a2a.client.requests.get", return_value=mock_resp):
            card = client.discover("https://no-card.example.com")

        assert card is None

    def test_send_task_success(self) -> None:
        client = A2AClient()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "id": "task-123",
            "state": "completed",
            "output": {"text": "Analysis complete"},
        }

        with patch("a2a.client.requests.post", return_value=mock_resp):
            task = client.send_task(
                "https://agent.example.com",
                skill_id="analyze",
                input_text="Analyze BTC",
            )

        assert task is not None
        assert task.state == TaskState.COMPLETED
        assert task.output == "Analysis complete"

    def test_send_task_402_payment_required(self) -> None:
        client = A2AClient()

        mock_resp = MagicMock()
        mock_resp.status_code = 402
        mock_resp.json.return_value = {
            "x402": {"amount": "0.01", "token": "USDC"},
        }

        with patch("a2a.client.requests.post", return_value=mock_resp):
            task = client.send_task(
                "https://agent.example.com",
                skill_id="forecast",
                input_text="Forecast SPY",
            )

        assert task is not None
        assert task.state == TaskState.INPUT_REQUIRED
        assert "payment_required" in task.metadata

    def test_send_task_failure(self) -> None:
        client = A2AClient()

        with patch("a2a.client.requests.post", side_effect=ConnectionError):
            task = client.send_task(
                "https://offline.example.com",
                skill_id="test",
                input_text="test",
            )

        assert task is None

    def test_get_task(self) -> None:
        client = A2AClient()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "id": "task-456",
            "state": "completed",
            "skill_id": "analyze",
            "input": {"text": "test"},
            "output": {"text": "done"},
        }

        with patch("a2a.client.requests.get", return_value=mock_resp):
            task = client.get_task("https://agent.example.com", "task-456")

        assert task is not None
        assert task.state == TaskState.COMPLETED

    def test_clear_cache(self) -> None:
        client = A2AClient()
        client._card_cache["key"] = {"name": "cached"}
        client.clear_cache()
        assert len(client._card_cache) == 0


# ---------------------------------------------------------------------------
# A2ATaskManager
# ---------------------------------------------------------------------------


class TestA2ATaskManager:
    def test_submit_task_no_handler(self) -> None:
        mgr = A2ATaskManager()
        task = mgr.submit_task("unknown_skill", "test input")

        assert task.state == TaskState.SUBMITTED
        assert task.skill_id == "unknown_skill"
        assert task.input_text == "test input"

    def test_submit_task_with_handler(self) -> None:
        mgr = A2ATaskManager()
        mgr.register_handler("echo", lambda text: f"Echo: {text}")

        task = mgr.submit_task("echo", "hello")

        assert task.state == TaskState.COMPLETED
        assert task.output == "Echo: hello"

    def test_submit_task_handler_error(self) -> None:
        mgr = A2ATaskManager()
        mgr.register_handler("fail", lambda text: 1 / 0)

        task = mgr.submit_task("fail", "trigger error")

        assert task.state == TaskState.FAILED
        assert "error" in task.metadata

    def test_get_task(self) -> None:
        mgr = A2ATaskManager()
        submitted = mgr.submit_task("test", "input")

        retrieved = mgr.get_task(submitted.id)
        assert retrieved is not None
        assert retrieved.id == submitted.id

    def test_get_task_not_found(self) -> None:
        mgr = A2ATaskManager()
        assert mgr.get_task("nonexistent") is None

    def test_cancel_task(self) -> None:
        mgr = A2ATaskManager()
        task = mgr.submit_task("test", "input")

        assert mgr.cancel_task(task.id) is True
        cancelled = mgr.get_task(task.id)
        assert cancelled is not None
        assert cancelled.state == TaskState.CANCELED

    def test_cancel_completed_task_fails(self) -> None:
        mgr = A2ATaskManager()
        mgr.register_handler("echo", lambda t: t)
        task = mgr.submit_task("echo", "done")

        assert task.state == TaskState.COMPLETED
        assert mgr.cancel_task(task.id) is False

    def test_list_tasks(self) -> None:
        mgr = A2ATaskManager()
        mgr.submit_task("a", "1")
        mgr.submit_task("b", "2")
        mgr.submit_task("c", "3")

        all_tasks = mgr.list_tasks()
        assert len(all_tasks) == 3

    def test_list_tasks_filtered(self) -> None:
        mgr = A2ATaskManager()
        mgr.register_handler("echo", lambda t: t)
        mgr.submit_task("echo", "1")  # completed
        mgr.submit_task("no_handler", "2")  # submitted

        completed = mgr.list_tasks(state=TaskState.COMPLETED)
        assert len(completed) == 1

    def test_max_tasks_eviction(self) -> None:
        mgr = A2ATaskManager(max_tasks=3)
        mgr.submit_task("a", "1")
        mgr.submit_task("b", "2")
        mgr.submit_task("c", "3")
        mgr.submit_task("d", "4")  # Should evict first

        assert mgr.task_count == 3

    def test_registered_skills(self) -> None:
        mgr = A2ATaskManager()
        mgr.register_handler("skill_a", lambda t: t)
        mgr.register_handler("skill_b", lambda t: t)

        assert "skill_a" in mgr.registered_skills
        assert "skill_b" in mgr.registered_skills

    def test_custom_task_id(self) -> None:
        mgr = A2ATaskManager()
        task = mgr.submit_task("test", "input", task_id="custom-id-123")
        assert task.id == "custom-id-123"
