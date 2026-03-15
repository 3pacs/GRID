# PRIVACY BOUNDARY: This module uses Hyperspace for local inference
# and embeddings only. No GRID signal logic, feature values, discovered
# cluster structures, or hypothesis details are sent to the network.
"""
GRID Hyperspace integration layer.

Provides local LLM inference, semantic embeddings, reasoning assistance,
and node monitoring through the Hyperspace P2P network.  All Hyperspace
calls are optional and fail gracefully — GRID operates fully without them.
"""
