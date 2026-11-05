"""Constants pinned by ``docs/paper_log/trade-edge-v2-preregistration.md``.

Every number here traces to a section of the pre-registration (cited inline).
None may change after the header record is written on grid-svr; a change is
a v3 with a new log file.
"""

from __future__ import annotations

from datetime import time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

VERSION = "trade-edge-v2"
EASTERN = ZoneInfo("America/New_York")

PREREG_PATH = Path("docs/paper_log/trade-edge-v2-preregistration.md")
# sha256 of the pre-registration's LF bytes (``research_forward_log.lf_sha256``).
# Re-pin only before the header record is written; afterwards a change is v3.
PREREG_SHA256 = "b9230b7538c390b62bd13f95f3aff6227b538aae7f5fc6c1242c6ae1b30a8451"

LOG_FILENAME = "trade_edge_v2.jsonl"
ANCHOR_FILENAME = "trade_edge_v2.anchors.jsonl"
LOCK_FILENAME = ".trade_edge_v2.lock"
REPORTS_DIRNAME = "reports"

BANNER_SUFFIX = "not investment advice, research paper log"

# §2.1 candidates
SOURCE_NAME = "SEC_INSIDER"
SERIES_PATTERN = "INSIDER:%:BUY"
CANDIDATE_LOOKBACK = timedelta(days=10)  # first ingest >= genesis - this
SEC_MAX_FETCHES_PER_RUN = 300
SEC_DELAY_S = 0.15  # SEC asks for <= 10 requests per second
SEC_ATTEMPTS = 2  # "after one retry"

# §2.2 qualifying lines (VS1 §2.1)
PURCHASE_CODE = "P"
PURCHASE_FORM = "4"
MIN_SHARES = 100.0
MIN_TRADE_USD = 10_000.0
MAX_FILING_LAG_DAYS = 365

# §2.4 known_at
PUBLIC_FALLBACK_LOCAL = time(22, 0)
# raw_series.pull_timestamp defaults to the inserting transaction's start time,
# so a row can become visible some time after its stamp; first ingest is
# therefore taken as pull_timestamp + this margin.
INGEST_VISIBILITY_MARGIN = timedelta(minutes=15)

# §3 timing / strata / horizons
LARGE_LINE_USD = 500_000.0
HORIZONS = (5, 20, 30)
PRIMARY_HORIZON = 30
DATA_READY_AFTER_CLOSE = timedelta(minutes=30)

# §4 statuses
PRICE_GRACE_SESSIONS = 5
DELIST_PENALTY = -0.30
ST_OPENED = "opened"
ST_NO_PRICE = "no_price"
ST_UNRESOLVED = "unresolved_ticker"
ST_LATE_FILING = "late_filing"
ST_CLOSED = "closed"
ST_CLOSED_DELISTED = "closed_delisted"

# §5 prices
BENCHMARK = "SPY"

# §6 market-cap buckets
CAP_SMALL_MAX = 300e6
CAP_MID_MAX = 2e9
BUCKET_MICRO = "<300M"
BUCKET_MID = "300M-2B"
BUCKET_LARGE = ">=2B"
BUCKET_UNKNOWN = "unknown"
BUCKETS = (BUCKET_MICRO, BUCKET_MID, BUCKET_LARGE, BUCKET_UNKNOWN)
CAP_LOOKBACK_DAYS = 10

# §7 costs (round trip, basis points)
COST_BPS_ROUND_TRIP = {
    BUCKET_LARGE: 10.0,
    BUCKET_MID: 30.0,
    BUCKET_MICRO: 100.0,
    BUCKET_UNKNOWN: 100.0,
}
V1_COST_BPS_ROUND_TRIP = 5.0

# §8 reporting
MISSING_LABEL_WARNING = 0.05
STRATUM_LARGE = "large"
STRATUM_SMALL = "small"

# §10 label rule
LOOKS = (100, 200, 300)
LOOK_T = 2.3
MIN_CLUSTERS_AT_LOOK = 30
LABEL_UNPROVEN = "UNPROVEN"
LABEL_SUPPORTED = "SUPPORTED_FORWARD"
LABEL_CONTRARY = "CONTRARY"
LABEL_NOT_SUPPORTED = "NOT_SUPPORTED"
