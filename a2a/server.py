"""
A2A Server — receive and process task requests from external agents.

Implements the server side of the A2A protocol: accepting task submissions,
managing task lifecycle, and returning results.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from loguru import logger as log

from a2a.client import A2ATask, TaskState


class A2ATaskManager:
    """Manages inbound A2A tasks from external agents.

    Stores tasks in memory with an extensible handler registry.
    Each skill_id maps to a handler function that processes the task.

    Parameters:
        max_tasks: Maximum number of tasks to keep in memory.
    """

    def __init__(self, max_tasks: int = 1000) -> None:
        self._tasks: dict[str, A2ATask] = {}
        self._handlers: dict[str, Callable[[str], str | None]] = {}
        self._max_tasks = max_tasks

    def register_handler(
        self,
        skill_id: str,
        handler: Callable[[str], str | None],
    ) -> None:
        """Register a handler function for a skill.

        Parameters:
            skill_id: The skill identifier to handle.
            handler: Function that takes input text and returns output text.
        """
        self._handlers[skill_id] = handler
        log.debug("A2A handler registered for skill={s}", s=skill_id)

    def submit_task(
        self,
        skill_id: str,
        input_text: str,
        task_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> A2ATask:
        """Accept a new task submission.

        Parameters:
            skill_id: Which skill to invoke.
            input_text: The task prompt.
            task_id: Optional pre-assigned task ID.
            metadata: Additional task metadata.

        Returns:
            A2ATask: The created task (may be immediately completed
                     if a synchronous handler is registered).
        """
        tid = task_id or str(uuid.uuid4())

        task = A2ATask(
            id=tid,
            state=TaskState.SUBMITTED,
            skill_id=skill_id,
            input_text=input_text,
            metadata=metadata or {},
        )

        # Evict oldest tasks if at capacity
        if len(self._tasks) >= self._max_tasks:
            oldest_key = next(iter(self._tasks))
            del self._tasks[oldest_key]

        self._tasks[tid] = task

        # Try to execute immediately if handler exists
        handler = self._handlers.get(skill_id)
        if handler is not None:
            task = A2ATask(
                id=task.id,
                state=TaskState.WORKING,
                skill_id=task.skill_id,
                input_text=task.input_text,
                metadata=task.metadata,
                created_at=task.created_at,
            )
            self._tasks[tid] = task

            try:
                result = handler(input_text)
                task = A2ATask(
                    id=task.id,
                    state=TaskState.COMPLETED,
                    skill_id=task.skill_id,
                    input_text=task.input_text,
                    output=result,
                    metadata=task.metadata,
                    created_at=task.created_at,
                )
                self._tasks[tid] = task
                log.debug(
                    "A2A task {id} completed — skill={s}",
                    id=tid[:8],
                    s=skill_id,
                )
            except Exception as exc:
                task = A2ATask(
                    id=task.id,
                    state=TaskState.FAILED,
                    skill_id=task.skill_id,
                    input_text=task.input_text,
                    output=None,
                    metadata={**task.metadata, "error": str(exc)},
                    created_at=task.created_at,
                )
                self._tasks[tid] = task
                log.warning(
                    "A2A task {id} failed — skill={s}: {err}",
                    id=tid[:8],
                    s=skill_id,
                    err=str(exc),
                )
        else:
            log.warning(
                "A2A no handler for skill={s} — task {id} stays submitted",
                s=skill_id,
                id=tid[:8],
            )

        return self._tasks[tid]

    def get_task(self, task_id: str) -> A2ATask | None:
        """Retrieve a task by ID.

        Parameters:
            task_id: The task identifier.

        Returns:
            A2ATask or None if not found.
        """
        return self._tasks.get(task_id)

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending or working task.

        Parameters:
            task_id: The task identifier.

        Returns:
            bool: True if cancelled, False if not found or already terminal.
        """
        task = self._tasks.get(task_id)
        if task is None:
            return False

        if task.state in (TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELED):
            return False

        self._tasks[task_id] = A2ATask(
            id=task.id,
            state=TaskState.CANCELED,
            skill_id=task.skill_id,
            input_text=task.input_text,
            output=task.output,
            metadata=task.metadata,
            created_at=task.created_at,
        )
        return True

    def list_tasks(
        self,
        state: TaskState | None = None,
        limit: int = 50,
    ) -> list[A2ATask]:
        """List tasks, optionally filtered by state.

        Parameters:
            state: Filter by this state (None = all).
            limit: Maximum number to return.

        Returns:
            list[A2ATask]: Tasks sorted by creation time (newest first).
        """
        tasks = list(self._tasks.values())
        if state is not None:
            tasks = [t for t in tasks if t.state == state]
        tasks.sort(key=lambda t: t.created_at, reverse=True)
        return tasks[:limit]

    @property
    def task_count(self) -> int:
        """Return the number of tasks in memory."""
        return len(self._tasks)

    @property
    def registered_skills(self) -> list[str]:
        """Return list of skill IDs with registered handlers."""
        return list(self._handlers.keys())
