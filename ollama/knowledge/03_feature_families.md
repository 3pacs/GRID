# GRID Feature Families

GRID organizes 464+ features into families. Each feature has a defined transformation, normalization method, and lag structure.

## Rates Family

| Feature | Description | Source | Signal |
|---------|-------------|--------|--------|
| `yld_curve_2s10s` | 2y-10y Treasury spread | FRED T10Y2Y | Inversion = recession risk (lead ~12-18mo) |
| `yld_curve_3m10y` | 3m-10y Treasury spread | FRED T10Y3M | More reliable inversion signal than 2s10s |
| `fed_funds_rate` | Effective FFR | FRED DFF | Current monetary policy stance |
| `fed_funds_3m_chg` | 63-day change in FFR | Derived | Direction and speed of policy change |
| `real_ffr` | FFR minus CPI YoY | Derived | Real monetary tightness — the number that matters |

### How to Read Rates

- **Inverted curve** (2s10s or 3m10y < 0): Market expects rate cuts, historically precedes recessions
- **Steepening from inversion**: Often the actual recession signal (the un-inversion)
- **Rising real FFR**: Tightening financial conditions, negative for risk assets with a lag
- **Fed funds 3m change rising**: Active tightening cycle; falling = easing cycle

## Credit Family

| Feature | Description | Signal |
|---------|-------------|--------|
| `hy_spread_proxy` | HYG/LQD yield ratio | Credit stress; widening = risk-off |
| `ig_spread_proxy` | LQD vs IEF differential | Investment-grade stress |
| `hy_spread_3m_chg` | 63-day change in HY spread | Speed of credit deterioration/improvement |

### How to Read Credit

- **HY spread widening**: Credit markets pricing in default risk — leads equity drawdowns
- **HY spread narrowing**: Risk appetite returning — bullish for risk assets
- **IG vs HY divergence**: If IG widens but HY doesn't, it's usually a liquidity issue, not credit

## Breadth Family

| Feature | Description | Signal |
|---------|-------------|--------|
| `sp500_pct_above_200ma` | % of S&P stocks above 200-day MA | Market health; <40% = weak, >70% = strong |
| `sp500_adline` | Cumulative advance-decline line | Internal market strength |
| `sp500_adline_slope` | 20-day slope of A/D line | Momentum of breadth |
| `sp500_mom_12_1` | 12-month minus 1-month momentum | Classic trend signal with mean-reversion filter |
| `sp500_mom_3m` | 3-month price momentum | Medium-term trend |

### How to Read Breadth

- **Price at highs + breadth declining**: Classic bearish divergence — narrow rally
- **Price at lows + breadth improving**: Positive divergence — bottoming signal
- **A/D slope turning positive after prolonged decline**: Early recovery signal

## Volatility Family

| Feature | Description | Signal |
|---------|-------------|--------|
| `vix_spot` | VIX spot level | Fear gauge; >30 = panic, <15 = complacency |
| `vix_3m_ratio` | VIX / VIX3M | Term structure; >1 = backwardation = acute stress |
| `vix_1m_chg` | 21-day change in VIX | Velocity of fear |

### How to Read Volatility

- **VIX backwardation** (spot > 3m): Active crisis — hedging demand concentrated short-term
- **VIX contango** (spot < 3m): Normal conditions — carry trade is profitable
- **VIX spike + contango maintained**: Not a real panic — buy the dip candidate
- **VIX elevated + backwardated for weeks**: Structural stress — stay defensive

## FX Family

| Feature | Description | Signal |
|---------|-------------|--------|
| `dxy_index` | US Dollar Index | Global risk sentiment; strong USD = risk-off |
| `dxy_3m_chg` | 63-day change in DXY | Direction of dollar trend |

### How to Read FX

- **Rising DXY**: Tightening global financial conditions (USD funding squeeze)
- **Falling DXY**: Easing conditions, positive for EM and commodities
- **DXY + rising rates**: True tightening — most negative combo for risk assets
- **DXY falling + rising rates**: Growth optimism — global reflation

## Commodity Family

| Feature | Description | Signal |
|---------|-------------|--------|
| `copper_gold_ratio` | Copper / Gold price ratio | Global growth expectations |
| `copper_gold_slope` | 63-day slope of Cu/Au ratio | Direction of growth expectations |

### How to Read Commodities

- **Rising Cu/Au ratio**: Industrial demand > safe haven demand = growth optimism
- **Falling Cu/Au ratio**: Safe haven demand > industrial demand = growth pessimism
- **Cu/Au slope turning positive from below zero**: Early reflation signal

## Macro Family

| Feature | Description | Signal |
|---------|-------------|--------|
| `ism_pmi_mfg` | ISM Manufacturing PMI | >50 = expansion, <50 = contraction |
| `ism_pmi_new_orders` | ISM New Orders sub-index | Leading component of PMI |
| `conf_board_lei` | Conference Board Leading Index | Composite leading indicator |
| `conf_board_lei_slope` | 63-day slope of LEI | Direction of leading indicators |
| `cpi_yoy` | CPI year-over-year | Inflation trajectory |

### How to Read Macro

- **LEI slope turning negative**: Recession risk rising (6-12 month lead)
- **PMI crossing 50**: Expansion/contraction boundary — regime change signal
- **New Orders diverging from headline PMI**: New Orders leads by 1-2 months
- **CPI YoY falling + PMI rising**: Goldilocks — best environment for risk assets
