"""Contract-type → event-bus channel mapping."""
from __future__ import annotations

import re

from contracts.schemas import ALL_CONTRACTS, BaseContract


_CAMEL_SPLIT = re.compile(r"(?<!^)(?=[A-Z])")


def _to_snake(name: str) -> str:
    return _CAMEL_SPLIT.sub("_", name).lower()


def channel_for(contract_cls: type[BaseContract]) -> str:
    """Return the event-bus channel name for a contract type."""
    return f"grid_contracts_{_to_snake(contract_cls.__name__)}"


# Reverse lookup cache: channel → contract class.
_CHANNEL_TO_CONTRACT: dict[str, type[BaseContract]] = {
    channel_for(cls): cls for cls in ALL_CONTRACTS
}


def contract_for_channel(channel: str) -> type[BaseContract] | None:
    """Return the contract class for a channel, or None if unknown."""
    return _CHANNEL_TO_CONTRACT.get(channel)


ALL_CHANNELS: tuple[str, ...] = tuple(
    channel_for(cls) for cls in ALL_CONTRACTS
)
