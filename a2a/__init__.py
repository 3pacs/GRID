"""
GRID Agent-to-Agent (A2A) Protocol.

Implements Google's A2A open protocol for inter-agent communication.
Allows GRID agents to discover, communicate, and delegate tasks to
specialized remote agents via Agent Cards (JSON capability descriptors).

A2A complements MCP (which provides tools/context) by enabling agents
to communicate as peers rather than as tool-callers.

Architecture:
  - AgentCard: JSON descriptor advertising agent capabilities
  - A2AServer: Receives task requests from external agents
  - A2AClient: Discovers and delegates to remote agents
  - TaskManager: Manages async task lifecycle (submitted → working → done)
"""

from a2a.agent_card import AgentCard, build_grid_agent_card
from a2a.client import A2AClient
from a2a.server import A2ATaskManager

__all__ = ["AgentCard", "A2AClient", "A2ATaskManager", "build_grid_agent_card"]
