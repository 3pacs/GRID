# AstroGrid 90-Day Execution Roadmap

Last updated: 2026-03-26
Branch: `codex/astrogrid-prototype`

## Objective

Ship AstroGrid from prototype to paid product readiness over the next 90 days, with a path to $100,000/month in recurring revenue by December 2026.

## Revenue Thesis

AstroGrid should not monetize as a novelty astrology app. It should monetize as a premium celestial-intelligence layer for traders, research desks, and funds that want differentiated macro/behavioral context.

Target December 2026 revenue mix:

- 150 `Pro` subscribers at $299/month = $44,850 MRR
- 20 `Desk` subscribers at $999/month = $19,980 MRR
- 7 `Institutional` pilots at $5,000/month = $35,000 MRR

Target total: about $99,830 MRR

## Product Positioning

AstroGrid becomes:

- a premium add-on inside GRID for discretionary traders
- a daily celestial-market briefing product for paid subscribers
- a research surface for correlations, timing windows, and event monitoring
- an institutional pilot offering with custom reports, exports, and analyst support

## Constraints

- Codex scope remains inside `grid/astrogrid/`
- Backend and shared API wiring stay coordinated through the operator and Claude Code
- Near-term frontend must be production-safe even when some backend endpoints are not live yet

## Success Metrics For The Next 90 Days

- AstroGrid SPA is stable enough for daily internal use
- Live celestial telemetry is clearly separated from mock/demo data
- The product has a paid-beta funnel, pricing narrative, and operator-ready demo flow
- We have a concrete sales motion for retail, desk, and institutional users

## Phase 1: Production Prototype
Dates: 2026-03-26 to 2026-04-19

Primary goal: make the existing AstroGrid frontend reliable enough for repeated internal and demo use.

Tasks:

- Harden app shell, routing, auth/session behavior, and local state persistence
- Centralize live celestial data bootstrap and degraded-state handling
- Make every view explicit about live data, fallback data, and missing backend support
- Improve Orrery mobile/runtime safety and reduce performance risk on small devices
- Build a session-level status model so operators know whether they are seeing live, cached, or demo data
- Document required backend endpoints and payload shapes still needed for launch

Definition of done:

- All seven views render without navigation dead ends
- `npm run build` succeeds after changes
- Live/fallback state is visible in the UI
- No critical route or storage failures on first load

## Phase 2: Paid Beta System
Dates: 2026-04-20 to 2026-05-24

Primary goal: turn AstroGrid into something we can put in front of design partners and early paying users.

Tasks:

- Add demo-ready narrative flow, onboarding copy, and first-session walkthrough content
- Add saved preferences for trader persona, time horizon, and watched assets
- Build a market-watch configuration layer around SPY, QQQ, BTC, gold, rates, and volatility
- Add export surfaces for screenshots, narrative snapshots, or event summaries
- Define pricing tiers, value props, FAQ, and operator-led sales demo script
- Create a paid beta waitlist and intake questionnaire outside the app
- Prepare weekly release cadence with changelog and demo dataset refreshes

Revenue tasks:

- Identify first 20 high-intent users from existing GRID and operator network
- Run 10 design-partner calls
- Close first 5 paid beta seats
- Package one institutional pilot offer with custom reporting and onboarding

Definition of done:

- Operators can demo a coherent end-to-end story in under 10 minutes
- At least one repeatable pricing and onboarding path exists
- AstroGrid can support paid-beta users with clear expectations around live versus upcoming features

## Phase 3: Launch And Sales Engine
Dates: 2026-05-25 to 2026-06-30

Primary goal: shift from prototype credibility to repeatable revenue motion.

Tasks:

- Add conversion-focused landing narrative inside the product shell
- Add case-study-ready views, shareable reports, and market-event retrospectives
- Tighten mobile polish and presentation mode for calls and webinars
- Create a feature matrix for `Pro`, `Desk`, and `Institutional`
- Add admin/operator checklist for daily signal review and demo prep
- Prepare enterprise asks: API access, export formats, private briefings, and analyst support

Revenue tasks:

- Publish weekly celestial-market briefings to attract top-of-funnel interest
- Run 4 live demos or webinars in June 2026
- Start outbound to funds, macro communities, and alt-data buyers
- Convert first institutional pilot and push for annualized commitments where possible

Definition of done:

- Retail offer is clear enough to self-serve
- Desk and institutional offers are clear enough to pitch live
- Product has a believable route to scale from beta to recurring revenue

## Weekly Operating Cadence

- Monday: roadmap review, priorities, backlog pruning
- Tuesday: ship frontend reliability and view improvements
- Wednesday: demo polish, screenshots, narrative output, and content prep
- Thursday: user feedback synthesis, pricing, and sales collateral
- Friday: production build, branch hygiene, handoff notes, and next-week plan

## Backlog By Track

### Track A: Product Reliability

- Central bootstrap for celestial telemetry
- Route sync and recoverable startup
- Stronger error boundaries and degraded-mode banners
- Better mobile ergonomics across Orrery and heatmaps

### Track B: Data Credibility

- Standardize normalization of celestial categories
- Render API-backed data where endpoints exist
- Keep fallback data clearly marked
- Log missing backend contract needs for Claude Code

### Track C: Conversion And Monetization

- Demo narrative and storytelling
- Pricing and plan definitions
- Shareable artifacts and reports
- User profile and watchlist settings

### Track D: Operator Enablement

- Handoff notes and work journal
- Daily run checklist
- Demo dataset notes
- Launch readiness checklist

## Immediate Next 10 Tasks

1. Harden AstroGrid routing, settings navigation, and storage safety
2. Create a session-level telemetry status model
3. Make Correlations, Timeline, Lunar, and Narrative clearly label fallback/demo content
4. Align Orrery and Narrative date handling with the shared selected date
5. Improve mobile performance for the 3D hero path
6. Add operator-facing data status banners/cards where needed
7. Capture missing endpoint asks in a local note for relay to Claude Code
8. Verify build again when permissions are available
9. Push branch and produce a release-style handoff summary
10. Prepare the paid-beta product narrative and pricing document

## Risks

- Backend endpoint contract is still partial, so the frontend must remain graceful under mixed live/mock conditions
- Heavy 3D payloads can degrade low-end mobile performance
- Without clear live-versus-demo labeling, operator trust will erode during demos
- Revenue target depends as much on packaging and sales motion as on UI completion
