from __future__ import annotations

from contracts.channels import channel_for, contract_for_channel, ALL_CHANNELS
from contracts.schemas import (
    ALL_CONTRACTS,
    PostmortemCompleted,
    PredictionScored,
    PullLifecycle,
)


def test_channel_for_postmortem():
    assert channel_for(PostmortemCompleted) == "grid_contracts_postmortem_completed"


def test_channel_for_prediction_scored():
    assert channel_for(PredictionScored) == "grid_contracts_prediction_scored"


def test_channel_for_pull_lifecycle():
    assert channel_for(PullLifecycle) == "grid_contracts_pull_lifecycle"


def test_every_contract_has_a_channel():
    for cls in ALL_CONTRACTS:
        ch = channel_for(cls)
        assert ch.startswith("grid_contracts_")
        assert ch == ch.lower()


def test_all_channels_is_complete():
    assert len(ALL_CHANNELS) == len(ALL_CONTRACTS)


def test_reverse_lookup_roundtrip():
    for cls in ALL_CONTRACTS:
        ch = channel_for(cls)
        assert contract_for_channel(ch) is cls
