# View Architecture — 6 World Views

Each view answers ONE question. Together they are the complete picture.

---

## 1. THE FLOW (Sankey)
**Question: WHERE is money going?**

Central Banks → Banking → Markets → Sectors → Stocks. Animated particles
showing direction and volume. This is the plumbing of the financial system.
You see where liquidity enters, where it accumulates, where it drains.

- **Viz type**: Sankey / alluvial flow diagram
- **Data**: Fed balance sheet, TGA, reverse repo, ETF flows, sector rotation, dark pool
- **Interaction**: Click any node to drill down. Time slider to see how flows changed.
- **Existing**: MoneyFlow.jsx (built)
- **Sells as**: "Global Capital Flow API"

---

## 2. THE POWER MAP (Force Graph)
**Question: WHO controls what?**

Named actors — Fed governors, congressional traders, fund managers, insiders —
connected by money, influence, and information. Size = influence. Brightness =
recent activity. See who's connected to who and what they're doing.

- **Viz type**: Force-directed graph with animated wealth particles
- **Data**: Actor network, 13F holdings, congressional disclosures, insider filings
- **Interaction**: Click actor for detail. Filter by tier/category. Search.
- **Existing**: ActorNetwork.jsx (built)
- **Sells as**: "Influence Network API"

---

## 3. THE TRUTH (Heatmap Matrix)
**Question: What's REAL and what's NOT?**

Government stats vs physical reality. Every cell in the matrix is a cross-reference
check. Green = consistent. Red = contradiction. Click any cell to see the official
story vs the physical evidence. Track the lies over time.

- **Viz type**: Heatmap matrix with divergence overlay detail panels
- **Data**: Cross-reference engine, FRED, BLS, ECB, VIIRS, shipping, Comtrade
- **Interaction**: Click cell → detail with sparkline comparison. Red flag banner.
- **Existing**: CrossReference.jsx (built)
- **Sells as**: "Macro Truth API"

---

## 4. THE GLOBE (Spatial / Choropleth)
**Question: What's happening WHERE in the world?**

A world map showing capital flows between countries, trade imbalances,
currency movements, GDP divergences, commodity flows. Color intensity =
economic activity. Arrows = capital direction. Hotspots = where the action is.

- **Viz type**: D3 world map / choropleth with flow arrows
- **Data**: UN Comtrade bilateral trade, ECB/BOJ/PBOC policy, FX pairs,
  VIIRS night lights by country, commodity trade routes, sovereign wealth flows
- **Interaction**: Click country → macro snapshot. Toggle layers (trade, FX, GDP, commodity).
  Time scrubber to watch flows shift over months.
- **Status**: NOT BUILT — needs new view
- **Sells as**: "Global Intelligence Map API"

---

## 5. THE RISK (Treemap / Bubble)
**Question: What could go WRONG?**

Every position, every exposure, sized by risk and colored by threat level.
GEX regime tells you if dealer hedging amplifies or dampens moves.
Vanna/charm tells you where time is pushing the market. Concentration
shows where you're overexposed. Correlation breakdown shows what's decoupling.

- **Viz type**: Treemap (size = exposure) + bubble overlay (color = risk level)
- **Data**: GEX profile, vanna/charm, portfolio positions, options positioning,
  VIX term structure, correlation matrix, credit spreads
- **Interaction**: Click any bubble → risk breakdown. Toggle: dealer risk, vol risk,
  concentration risk, correlation risk. Animate to show risk evolution.
- **Status**: NOT BUILT — needs new view
- **Sells as**: "Risk Intelligence API"

---

## 6. THE SIGNAL (Command Dashboard)
**Question: What should I DO right now?**

The action view. Top-of-page: overall confidence gauge. Then: trade
recommendations with sanity status. Convergence alerts. Lever-puller activity.
Prediction scoreboard. Everything ranked by conviction and urgency.

- **Viz type**: Card grid + alert feed + confidence gauge
- **Data**: Options recommender, trust scorer, lever pullers, oracle predictions,
  post-mortems, regime state
- **Interaction**: Click recommendation → full thesis. Click alert → source detail.
  Refresh button. Filter by ticker.
- **Existing**: IntelDashboard.jsx (built)
- **Sells as**: "Trade Signal API"

---

## Navigation

Bottom tab bar (mobile) / top nav (desktop):

```
FLOW | POWER | TRUTH | GLOBE | RISK | SIGNAL
```

Six tabs. Six views. Six questions. One worldview.

The home dashboard (watchlist) is separate — it's your personal view.
These six are the WORLD views.

---

## API Product

Each view maps to a sellable API:

| View | API Product | Price Point |
|------|-------------|-------------|
| Flow | Global Capital Flow | $99/mo |
| Power | Influence Network | $149/mo |
| Truth | Macro Truth Engine | $99/mo |
| Globe | Global Intelligence Map | $79/mo |
| Risk | Risk Intelligence | $149/mo |
| Signal | Trade Signals | $199/mo |
| Bundle | Everything | $499/mo |

Data confidence labels on every response. Rate limited. API keys.
Premium tier gets WebSocket real-time pushes.
