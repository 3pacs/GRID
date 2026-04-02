"""
A2A Client — discover and delegate to remote agents.

Implements the client side of the A2A protocol: fetching remote Agent Cards,
sending task requests, and polling for results.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import requests
from loguru import logger as log


class TaskState(str, Enum):
    """A2A task lifecycle states."""

    SUBMITTED = "submitted"
    WORKING = "working"
    INPUT_REQUIRED = "input-required"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


@dataclass
class A2ATask:
    """Represents a task sent to or received from a remote agent.

    Attributes:
        id: Unique task identifier.
        state: Current lifecycle state.
        skill_id: Which skill was requested.
        input_text: The task prompt/input.
        output: The agent's response (when completed).
        metadata: Additional task metadata.
        created_at: Unix timestamp of creation.
    """

    id: str
    state: TaskState
    skill_id: str
    input_text: str
    output: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)


class A2AClient:
    """Client for discovering and communicating with remote A2A agents.

    Parameters:
        timeout: HTTP request timeout in seconds.
    """

    def __init__(self, timeout: int = 30) -> None:
        self.timeout = timeout
        self._card_cache: dict[str, dict[str, Any]] = {}

    def discover(self, agent_url: str) -> dict[str, Any] | None:
        """Fetch a remote agent's Agent Card.

        Parameters:
            agent_url: Base URL of the remote agent.

        Returns:
            dict: The Agent Card JSON, or None if unreachable.
        """
        url = f"{agent_url.rstrip('/')}/.well-known/agent.json"

        # Check cache first
        if url in self._card_cache:
            return self._card_cache[url]

        try:
            resp = requests.get(url, timeout=self.timeout)
            if resp.status_code != 200:
                log.warning(
                    "A2A discovery failed for {url}: {status}",
                    url=url,
                    status=resp.status_code,
                )
                return None

            card = resp.json()
            self._card_cache[url] = card
            log.info(
                "A2A discovered agent '{name}' at {url} — {n} skills",
                name=card.get("name", "unknown"),
                url=agent_url,
                n=len(card.get("skills", [])),
            )
            return card

        except Exception as exc:
            log.warning(
                "A2A discovery error for {url}: {err}",
                url=agent_url,
                err=str(exc),
            )
            return None

    def send_task(
        self,
        agent_url: str,
        skill_id: str,
        input_text: str,
        auth_token: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> A2ATask | None:
        """Send a task request to a remote agent.

        Parameters:
            agent_url: Base URL of the remote agent.
            skill_id: Which skill to invoke.
            input_text: The task prompt.
            auth_token: Bearer token for authentication.
            metadata: Additional task metadata.

        Returns:
            A2ATask: The created task with initial state, or None on failure.
        """
        url = f"{agent_url.rstrip('/')}/a2a/tasks"
        task_id = str(uuid.uuid4())

        payload = {
            "id": task_id,
            "skill_id": skill_id,
            "input": {"text": input_text},
            "metadata": metadata or {},
        }

        headers: dict[str, str] = {"Content-Type": "application/json"}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        try:
            resp = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=self.timeout,
            )

            if resp.status_code == 402:
                log.info(
                    "A2A task requires payment — agent={url}, skill={skill}",
                    url=agent_url,
                    skill=skill_id,
                )
                return A2ATask(
                    id=task_id,
                    state=TaskState.INPUT_REQUIRED,
                    skill_id=skill_id,
                    input_text=input_text,
                    metadata={"payment_required": resp.json()},
                )

            if resp.status_code not in (200, 201, 202):
                log.warning(
                    "A2A send_task failed: {status} {body}",
                    status=resp.status_code,
                    body=resp.text[:300],
                )
                return None

            data = resp.json()
            return A2ATask(
                id=data.get("id", task_id),
                state=TaskState(data.get("state", "submitted")),
                skill_id=skill_id,
                input_text=input_text,
                output=data.get("output", {}).get("text"),
                metadata=data.get("metadata", {}),
            )

        except Exception as exc:
            log.warning(
                "A2A send_task error: {err}",
                err=str(exc),
            )
            return None

    def get_task(
        self,
        agent_url: str,
        task_id: str,
        auth_token: str | None = None,
    ) -> A2ATask | None:
        """Poll a remote agent for task status/result.

        Parameters:
            agent_url: Base URL of the remote agent.
            task_id: Task identifier to check.
            auth_token: Bearer token for authentication.

        Returns:
            A2ATask: Updated task state, or None on failure.
        """
        url = f"{agent_url.rstrip('/')}/a2a/tasks/{task_id}"

        headers: dict[str, str] = {}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        try:
            resp = requests.get(url, headers=headers, timeout=self.timeout)
            if resp.status_code != 200:
                return None

            data = resp.json()
            return A2ATask(
                id=data["id"],
                state=TaskState(data.get("state", "submitted")),
                skill_id=data.get("skill_id", ""),
                input_text=data.get("input", {}).get("text", ""),
                output=data.get("output", {}).get("text"),
                metadata=data.get("metadata", {}),
            )

        except Exception as exc:
            log.warning("A2A get_task error: {err}", err=str(exc))
            return None

    def clear_cache(self) -> None:
        """Clear the Agent Card discovery cache."""
        self._card_cache.clear()
