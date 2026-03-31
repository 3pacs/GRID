"""Signal adapters — wrap intelligence modules into RegisteredSignal producers."""

from intelligence.adapters.flow_thesis_adapter import FlowThesisAdapter
from intelligence.adapters.trust_scorer_adapter import TrustScorerAdapter
from intelligence.adapters.cross_reference_adapter import CrossReferenceAdapter
from intelligence.adapters.lever_pullers_adapter import LeverPullersAdapter
from intelligence.adapters.forensics_adapter import ForensicsAdapter
from intelligence.adapters.feature_adapter import FeatureAdapter
from intelligence.adapters.earnings_adapter import EarningsAdapter
from intelligence.adapters.news_adapter import NewsAdapter
from intelligence.adapters.pattern_adapter import PatternAdapter
from intelligence.adapters.sleuth_adapter import SleuthAdapter
from intelligence.adapters.thesis_tracker_adapter import ThesisTrackerAdapter
from intelligence.adapters.dollar_flows_adapter import DollarFlowsAdapter
from intelligence.adapters.sector_network_adapter import SectorNetworkAdapter

ALL_ADAPTERS = [
    FlowThesisAdapter,
    TrustScorerAdapter,
    CrossReferenceAdapter,
    LeverPullersAdapter,
    ForensicsAdapter,
    FeatureAdapter,
    EarningsAdapter,
    NewsAdapter,
    PatternAdapter,
    SleuthAdapter,
    ThesisTrackerAdapter,
    DollarFlowsAdapter,
    SectorNetworkAdapter,
]

__all__ = [cls.__name__ for cls in ALL_ADAPTERS] + ["ALL_ADAPTERS"]
