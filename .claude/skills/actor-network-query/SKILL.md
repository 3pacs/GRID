# actor-network-query

Query and analyze GRID's actor network intelligence layer to understand who moves markets, why they move them, and which actors control critical money flows. Combines 475+ named actors, 250K+ discovery scale, and intelligence networks across energy, real estate, commodities, and media sectors.

## When to Use This Skill

- Building predictions that cite specific actor actions (levers for alpha-validation)
- Analyzing cross-border money flows and regulatory arbitrage opportunities
- Identifying conflicted interests in policy decisions (SEC, Fed, Congress)
- Tracking wealth concentration and asset class rotation opportunities
- Investigating geopolitical signals and their market impact
- Conducting intelligence sweeps before major market events
- Supporting post-mortem analysis to understand why predictions failed

## Core Actor Network Architecture

### Layer 1: Named Actors (475+)

Core universe tracked in `intelligence/actor_network.py`:

**US Federal Government**
- Federal Reserve Board members and voting committee
- Treasury Department officials (Secretary, Undersecretary for International Affairs, CFIUS)
- SEC leadership (Chair, Commissioners, Director of Enforcement)
- White House (President, economic advisors, OMB director)
- Congress (key committee chairs — Finance, Banking, Energy, Appropriations)

**Institutional Investors**
- Top-10 asset managers by AUM (BlackRock, Vanguard, State Street, Fidelity, etc.)
- Hedge funds with >$500M in macro strategies (Citadel, Renaissance, Elliott, Pershing)
- Pension funds and their fund managers (CalPERS, NYPF, etc.)
- Insurance companies with significant equity/credit positioning (Berkshire, MetLife, etc.)

**Financial Infrastructure**
- Major exchanges (NYSE, NASDAQ, CBOE, CME, ICE)
- OTC derivatives dealers (Goldman, JPMorgan, Morgan Stanley, Bank of America, Citi)
- Clearinghouses (DTCC, LCH, Eurex)
- Payment system operators (Federal Reserve, SWIFT, private settlement rails)

**Central Banks & Treasuries**
- Federal Reserve (all 12 regional banks, Board)
- ECB Governing Council
- Bank of England, Bank of Japan, People's Bank of China
- BIS, IMF, World Bank leadership

**Wealth Concentration**
- Ultra-high-net-worth individuals ($10B+)
- Family offices controlling significant capital
- Billionaire-linked entities and their portfolio companies
- Real estate moguls and construction magnates

**Technology & Telecommunications**
- BigTech founders and shareholders (Meta, Apple, Microsoft, Google, Amazon, Tesla, Nvidia)
- Private equity sponsors (Blackstone, KKR, Apollo, Carlyle)
- Infrastructure funds and operators
- Data brokers and AI companies

**Energy Sector**
- Oil majors (Exxon, Chevron, Shell, BP, Saudi Aramco)
- OPEC+ member governments
- Renewable energy incumbents (NextEra, Dominion, Duke)
- Commodity traders (Trafigura, Vitol, Mercuria, Gunvor)

**Real Estate & Construction**
- REIT operators (public and private)
- Real estate investment banks (JLL, CBRE, Cushman, Colliers)
- Developer moguls (Hong Kong tycoons, UAE royals, Chinese developers)
- Commercial real estate lenders (banks, securitization)

**Agriculture & Commodities**
- ABCD grain traders (Archer-Daniels, Bunge, Cargill, Louis Dreyfus)
- Mining giants (Glencore, Vale, Rio Tinto, BHP)
- Precious metals dealers and central bank gold reserve managers
- Agtech monopolies (Monsanto, Syngenta, Corteva)

**Media & Information Control**
- Broadcast networks (Fox, CNN, MSNBC, Bloomberg, Reuters)
- Entertainment studios (Disney, WBD, Paramount, Sony, Netflix)
- Social platforms (Meta/Facebook, Twitter/X, TikTok, Snap, Reddit)
- Data aggregators (Bloomberg, Refinitiv, FactSet, S&P)

### Layer 2: Actor Discovery (250K+ Scale)

`intelligence/actor_discovery.py` expands the named universe through:

**3-Degree BFS Expansion**
```
Seed actor (Fed Chair) → 1-degree (staff, family, associates)
                      → 2-degree (their business partners, investments)
                      → 3-degree (broader network ecosystem)
```

Example: Fed Chair Powell → Board members → their institutional affiliations → board interlocks → 250K+ extended network

**Data Sources for Discovery**
- SEC Form 4 filings (insider trading) — batch import of all corporate insiders
- SEC Form 13F filings (institutional holdings) — who holds what
- Congressional financial disclosures (House, Senate, Executive branch)
- ICIJ Panama Papers & Pandora Papers — 5M+ leaked entities from offshore schemes
- Board interlock analysis — who sits on multiple boards
- Employment history — who moved between finance, government, tech

**Batch Discovery Pipeline**
```
1. Pull Form 4 + 13F bulk data (SEC)
2. Parse names, identify transactions and holdings
3. Expand via known relationships (board seats, family links)
4. Cross-reference with Panama/Pandora Papers
5. Build network graph (actor → connections → impact surface)
```

### Layer 3: Sector-Specific Networks

#### Energy Network (`intelligence/energy_network.py`)
Tracks influence and control across oil, gas, renewables, and commodity trading:

**Key Nodes**
- OPEC+ members and their governments
- Oil majors' upstream, downstream, refining segments
- LNG exporters and importers
- Renewable energy incumbents vs disruptors
- Commodity traders (physical + derivatives)
- Central banks' energy reserve strategies

**Critical Flows**
- Oil production cuts/increases (supply valve)
- OPEC+ policy shifts (political leverage)
- Sanctions on oil producers (geopolitical valve)
- Energy transition capex (capex rotation)

**Example Query**: "Who controls crude oil supply valve?" → OPEC+ nations, Saudi Arabia (swing producer), plus commodity traders hedging flows.

#### Real Estate Network (`intelligence/real_estate_network.py`)
Maps capital flows in the world's largest asset class:

**Key Nodes**
- US REIT operators (public and private)
- Hong Kong tycoons and their mainland holdings
- UAE royal family real estate vehicles
- Chinese state developers and provincial governments
- US commercial real estate lenders
- Private equity real estate sponsors

**Critical Flows**
- Foreign direct investment in US RE (EB-5 programs, CFIUS oversight)
- China's hidden RE debt (defaults ripple globally)
- US CRE crisis exposure (office space, retail, hospitality)
- Mortgage origination capacity (when banks are stressed, originations fall)

**Example Query**: "Who is exposed to CRE crisis?" → Regional banks, commercial RE lenders, REITs with office exposure, Chinese developers.

#### Commodities & Agriculture Network (`intelligence/commodities_agriculture_network.py`)
Monitors food security, metals, and ag-tech concentration:

**Key Nodes**
- ABCD grain traders (control 60%+ of global grain flows)
- Mining giants and their jurisdictional exposure
- Central bank gold reserves (geopolitical barometer)
- Agtech monopolies (Monsanto post-Bayer, Syngenta, Corteva)
- Food security hotspots (Ukraine, Argentina, Southeast Asia)
- Shipping and logistics bottlenecks

**Critical Flows**
- Grain supply disruptions (weather, war, sanctions)
- Mining output swings (China rare earths, Brazil iron ore)
- Central bank gold accumulation (de-dollarization signal)
- Agtech patent concentration (food inflation risk)

**Example Query**: "What controls food prices?" → Weather (exogenous), ABCD traders (supply control), commodity financialization (speculators), geopolitical disruptions.

#### Media Network (`intelligence/media_network.py`)
Tracks information control and narrative propagation:

**Key Nodes**
- Broadcast networks and their owners
- Entertainment studios and streaming platforms
- Social media platforms (content moderation policies)
- News aggregators (Bloomberg, Reuters, AP)
- FinTwit influencers and their platforms
- Google Ads network and algorithmic promotion

**Critical Flows**
- Narrative alignment (Fed messaging, company guidance)
- Earnings commentary angle (spin on results)
- Market sentiment signals (CNBC tone, social media engagement)
- Regulatory pressure stories (PR campaigns by industries)

**Example Query**: "Who controls market narrative?" → Bloomberg/Reuters headlines influence short-term flows, FinTwit retail flow, traditional media editorial lines.

### Layer 4: Cross-Reference Intelligence (`intelligence/cross_reference.py`)

The "lie detector" — compares government statistics against physical reality:

**Example Checks**
- US GDP growth vs actual freight volumes (rail, trucking, ports)
- CPI inflation vs observed prices at retailers
- Unemployment rate vs help-wanted listings, wage growth
- Fed balance sheet activity vs reported liquidity conditions
- Central bank gold reserves vs audited holdings

**Output Format**
```
Metric: US CPI YoY (Official: 3.2%)
Physical Proxy: Walmart/Target price checks (3.8% observed)
Gap: +0.6% (official understates)
Confidence: derived (multiple retail sources)
Market Impact: If official CPI is understated, Fed cutting will accelerate
             → expect duration rally, yield curve flattening
```

## Query Patterns

### Query 1: Who Moves a Specific Liquidity Valve?

**Example**: "Who controls the credit valve?"

```
ANSWER STRUCTURE:
1. Primary actors: [Fed chair] (policy), [Treasury secretary] (issuance policy)
2. Secondary actors: [major banks] (supply of credit), [hedge funds] (demand/hedging)
3. Tertiary network: [insurance cos] (credit demand), [pension funds] (duration preference)
4. Threshold: 75% of credit flows controlled by Fed + Top-5 banks + big 3 asset managers
5. Pressure points: If insurance sells, if banks reduce lending, if demand spikes
6. Geopolitical exposure: If Europe stress, credit tightens

CONFIRMATION:
- Congressional hearing on bank lending (forms = pressure point)
- Fed senior loan officer survey (actual credit availability)
- High-yield spread widening (credit demand falling)
```

### Query 2: Track Wealth Flow Direction

**Example**: "Where is capital rotating?"

```
DATA SOURCES TO QUERY:
- ETF flows (daily, shows institutional intent)
- 13F filings (quarterly, shows completed positioning)
- Options positioning (weekly via options_scanner.py)
- Foreign exchange flows (weekly TIC data)
- Commodity trader positioning (CFTC Commitment of Traders, weekly)

PATTERN DETECTION:
1. Momentum phase: flows into growth, crypto, commodities, JPY weakness
2. Risk-off phase: flows into US treasuries, gold, GBP, JPY strength
3. Rotation phase: flows from growth → value, from equities → RE, from USD → EM

TRIGGER IDENTIFICATION:
- What actor initiated the flow? (Fed guidance, earnings miss, geopolitical event)
- Is flow directional (one-way) or oscillating (two-sided)?
- Does flow persist or reverse intraday? (conviction measure)
```

### Query 3: Identify Actor Conflict of Interest

**Example**: "Is SEC chair conflicted on a regulation?"

```
INVESTIGATION FRAMEWORK:
1. Previous employer (rotation risk)
   - SEC chair X was partner at law firm Y for 15 years
   - Law firm Y represents 8 major exchanges and 12 fintech firms
   - Potential conflict if those entities are the regulation target

2. Family/personal holdings (incentive risk)
   - SEC chair holds $2M in tech stocks (via spouse)
   - Proposed regulation targets tech platforms
   - Potential conflict if holdings rise/fall with regulation

3. Board interlocks (influence risk)
   - Fed governor sits on board of private equity firm
   - PE firm invests in financial services
   - Potential conflict if Fed policy favors PE-backed businesses

4. Political donation patterns (allegiance risk)
   - Central bank official donated to specific candidates
   - Those candidates advocate for particular monetary policy
   - Potential conflict if official shifts policy to candidate preferences

CONFIDENCE SCORING:
- Direct conflict (previous employer): confirmed high
- Indirect conflict (family holdings): derived medium
- Network conflict (board interlocks): estimated medium
- Political conflict (donation patterns): estimated low (may be public service)
```

### Query 4: Geopolitical Signal to Market Impact

**Example**: "China restricts rare earth exports — what markets move?"

```
ACTOR NETWORK TRACE:
1. Primary: China (source), rare earth miners (CNMC, Molycorp legacy)
2. Secondary: Apple, Tesla, defense contractors (consumers)
3. Tertiary: US rare earth producers (Mountain Pass, MP Materials)
4. Financial nodes: Commodity dealers, options traders, ETF sponsors

VALVE IDENTIFICATION:
- Supply valve closes (rare earth supply restricted)
- Cost valve opens (manufacturers' costs rise)
- Substitution valve opens (alternative materials adopted)
- Margin valve closes (defense contractor margins compress)

THESIS CONSTRUCTION:
LEVER: China restricts rare earths export (confirmed via state media)
       → supply valve closes for semiconductor, defense, EV industries
CONDITION: Weak USD environment (companies don't have hedging option)
THESIS: Expect 8-12% decline in consumer tech stocks (Apple, Tesla)
        within 30 days as margin guidance falls
INVALIDATION: China reverses restriction OR alternative supply opens
              (India increases exports, US production ramps)
```

### Query 5: Which Actors Hold Conflicting Positions?

**Example**: "Who is long bonds but also invests in inflation hedges (commodities)?"

```
QUERY DATABASE:
- 13F filings: Extract long positions in:
  * TLT, IEF (bond ETFs/fund holdings)
  * GLD, DBB (commodity holdings)
- Form 4: Executive options vesting (implicit long equity)
- Real estate holdings: Private RE holdings (inflation hedge)

ACTORS MATCHING PATTERN:
- Insurance companies (long bonds for duration, inflation hedges via real estate)
- Pension funds (liability-driven bonds, inflation hedges via commodities)
- Family offices (diversified across all asset classes)

INTERPRETATION:
- If these actors are increasing commodity hedges while holding bonds,
  they expect inflation persistence despite rate hikes
- Market implication: Yield curve may not steepen as much as history suggests
- Trade: Flatten curve (long bonds/short commodities), not steepen
```

## Trust Scorer Evaluation Windows

Before citing a source for an actor/action, verify it's within its evaluation window per `intelligence/trust_scorer.py`:

| Signal Type | Window | Recency Half-Life | Bayesian Weight | Integration Point |
|---|---|---|---|---|
| congressional | 30d | 30d | 0.95 (high trust) | Form 4, House/Senate disclosures |
| insider | 14d | 30d | 0.90 (high trust) | SEC EDGAR Form 4 filings |
| darkpool | 5d | 7d | 0.75 (medium) | FINRA weekly reports |
| social | 5d | 3d | 0.60 (low trust) | Reddit, FinTwit, Finviz |
| scanner | 7d | 7d | 0.70 (medium) | Options mispricing detector |
| foreign_lobbying | 45d | 45d | 0.85 (high trust) | DOJ FARA registry |
| geopolitical | 7d | 7d | 0.65 (medium) | GDELT event signals |
| diplomatic_cable | 30d | 90d | 0.80 (high trust) | Declassified State Dept/NSA |
| lobbying | 30d | 30d | 0.85 (high trust) | Senate LDA + OpenSecrets |
| campaign_finance | 60d | 60d | 0.80 (high trust) | FEC filings, OpenSecrets |
| offshore_leak | 14d | 365d | 0.90 (high trust) | ICIJ Panama/Pandora Papers |

**Recency Half-Life** = Signal weight drops to 50% after N days
**Bayesian Weight** = Prior probability assigned to this source type's accuracy

**Usage Rule**: If citing a signal to explain an actor's action:
- Congressional insider trading 2 days old → weight 0.95, very high confidence
- Diplomatic cable 45 days old (halfway through 90d window) → weight 0.45, medium confidence
- Social media signal 5 days old → weight 0.60, low confidence

## Confidence Labels

Every actor network query result must include a confidence label from the standard set:

| Label | Definition | Typical Sources | Example |
|---|---|---|---|
| confirmed | Multiple independent sources, direct evidence | Congressional filing + Form 4 + news | "Fed Chair Powell raised rates 25bp — confirmed via Fed statement + FOMC minutes" |
| derived | Calculated from confirmed sources | 13F + SEC filings + market data | "BlackRock's total equity allocation increased 3% — derived from comparing Q1 vs Q4 13F filings" |
| estimated | Model output, analyst consensus | Actor discovery algorithm, trust scorer | "Intelligence network estimate: 250K actors connected within 3-degree BFS, estimated from seed + recursion" |
| rumored | Single source, unverified | FinTwit, single whistleblower | "Tycoon X planning major RE acquisition — rumored via Reuters tipster, not confirmed" |
| inferred | Logical deduction from secondary signals | Behavioral inference from positioning | "Manager holds both bonds and commodity ETFs — inferred hedging against inflation (not disclosed)" |

## Database Query Examples

### Example 1: Find All Actors in Energy Sector

```python
from intelligence.energy_network import EnergyNetwork

energy = EnergyNetwork()

# Get OPEC+ member governments
opec_actors = energy.get_actors(category="government", region="OPEC")
# Returns: [Saudi Arabia, Russia, UAE, Kuwait, Iraq, Iran, Venezuela, ...]

# Get oil major companies
majors = energy.get_actors(category="corporation", type="oil_major")
# Returns: [Exxon, Chevron, Shell, BP, Saudi Aramco, ...]

# Get commodity traders
traders = energy.get_actors(category="corporation", type="commodity_trader")
# Returns: [Trafigura, Vitol, Mercuria, Gunvor, ...]
```

### Example 2: Query Actor Conflict Detection

```python
from intelligence.actor_network import ActorNetwork
from intelligence.cross_reference import ConflictDetector

network = ActorNetwork()
detector = ConflictDetector()

# Get SEC chair's previous employer
sec_chair = network.get_actor("gary_gensler")
prev_employers = sec_chair.employment_history  # [MIT, Goldman, Commodity Futures Trading Commission, ...]

# Check for conflicts
conflicts = detector.check_conflicts(
    actor=sec_chair,
    regulation_target="cryptocurrency"
)

# Output:
# [
#   {
#     "type": "previous_employer",
#     "risk_level": "high",
#     "detail": "Goldman Sachs previously lobbied against digital asset regulation",
#     "confidence": "confirmed",
#   },
#   {
#     "type": "family_holding",
#     "risk_level": "medium",
#     "detail": "Spouse holds $500K in tech stocks (via disclosed filings)",
#     "confidence": "confirmed",
#   }
# ]
```

### Example 3: Track Wealth Flow via ETF Inflows

```python
from intelligence.dollar_flows import DollarFlows

flows = DollarFlows()

# Get daily ETF flow direction for past 30 days
flow_data = flows.get_etf_flows(
    etfs=["TLT", "GLD", "QQQ"],  # Bonds, gold, tech
    days_back=30
)

# Analyze rotation pattern
rotation = flows.detect_rotation(flow_data)
# Output: "TLT inflows +$5B, GLD inflows +$2B, QQQ outflows -$8B"
# → Suggests: rotation from growth/tech into bonds and safe havens

# Attribute to actor
probable_actors = flows.attribute_to_actors(rotation)
# Output: ["BlackRock", "Vanguard", "pension funds"]
# → These asset managers likely driving rotation
```

### Example 4: Map Congressional Trading to Market Events

```python
from intelligence.actor_discovery import ActorDiscovery
from ingestion.altdata.congressional import CongressionalTrades

discovery = ActorDiscovery()
congress = CongressionalTrades()

# Get Form 4 trades in past 30 days (insider trading)
form4_trades = congress.get_recent_trades(days_back=30)

# Filter for unusual clusters (batch buys = bullish signal)
clusters = discovery.find_trade_clusters(form4_trades)

for cluster in clusters:
    print(f"""
    Stock: {cluster.ticker}
    Cluster Type: {cluster.type}  (buy/sell)
    Trader Count: {cluster.num_insiders}
    Aggregate Volume: {cluster.total_shares}
    Timeframe: {cluster.date_range}
    Confidence: {cluster.confidence}

    Interpretation: {'Insiders bullish' if cluster.type == 'buy' else 'Insiders bearish'}
    """)
```

### Example 5: Query Real Estate Network for Exposure

```python
from intelligence.real_estate_network import RealEstateNetwork

re = RealEstateNetwork()

# Find who is exposed to commercial real estate crisis
cre_exposed = re.get_actors(
    exposure_type="office_space",
    exposure_min_pct=5  # >5% of portfolio in office
)

# Output: [RealPage, SL Green, VORNADO, Brookfield, Welltower, ...]

# Get their liabilities and financial health
for actor in cre_exposed[:5]:
    health = re.get_actor_health(actor)
    print(f"""
    {actor.name}:
    - Loan-to-Value: {health.ltv}%
    - Debt maturity wall: {health.debt_maturity_dates}
    - Refinancing risk: {health.refi_risk_level}
    - Default probability (est): {health.default_prob}%
    """)
```

## Integration with Predictions (alpha-validation)

When using actor network queries to build predictions, ensure predictions follow the Prediction Causation Standard:

```
PREDICTION BUILT FROM ACTOR NETWORK QUERY:

Query: "Which actors are net short duration (bonds)?"
Result: BlackRock (8% underweight), Pimco (10% underweight), hedge funds (+$20B net short)

LEVER: Major asset managers reduced bond positioning (confirmed via 13F + manager commentary)
       → duration supply valve: fewer bids for long bonds

CONDITION: Fed signaling potential pause in rate hikes
           → market expects higher duration demand (pensions buying)

THESIS: Expect 15-25bp yield decline in 10-year over 2 weeks as structural
        bond buyers enter (pension rebalancing) against reduced supply from
        large asset managers

INVALIDATION: Invalidated if asset managers reverse shorts (new 13F show reinstatement)
              OR if Fed chair signals "higher for longer" (removes duration demand)

CONFIDENCE: derived (13F data is confirmed, but timing of inflows is estimated)
PROBABILITY: 0.68
```

## Actor Network Performance Tracking

`intelligence/source_audit.py` maintains accuracy metrics for actor signals:

After a prediction resolves, verify:
- Did the cited actor actually take the cited action? (confirm signal validity)
- Did the action move the valve as expected? (confirm causal link)
- Did the condition amplify or dampen as expected? (confirm condition role)

Use results to update trust scores and confidence for future actor network queries.

## See Also

- `intelligence/trust_scorer.py` — Bayesian trust scoring by source type
- `intelligence/actor_discovery.py` — 250K+ actor expansion algorithm
- `intelligence/postmortem.py` — Analyze prediction failures by actor involvement
- `intelligence/dollar_flows.py` — Normalize actor actions to USD flow equivalents
- `intelligence/cross_reference.py` — Validate actor claims against physical data
- `intelligence/source_audit.py` — Track actor signal accuracy over time
- `alpha-validation` skill — Validate predictions using actor network levers
- `data-health` skill — Monitor freshness of actor discovery data sources
