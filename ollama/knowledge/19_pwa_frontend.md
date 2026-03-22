# PWA Frontend — User Interface Architecture

## Stack

- **Framework**: React + Vite (fast HMR, optimized builds)
- **State Management**: Zustand (lightweight, no boilerplate)
- **Styling**: CSS-in-JS, IBM Plex Sans Mono font
- **Real-time**: WebSocket for live agent progress and notifications
- **Build output**: `pwa_dist/` served by FastAPI at `/`

## Pages (Views)

| Page | Route | Purpose |
|------|-------|---------|
| Dashboard | `/dashboard` | System overview: regime, journal, status indicators |
| Regime | `/regime` | Current regime state, confidence, transition probability |
| Signals | `/signals` | Feature snapshot table with latest values |
| Journal | `/journal` | Decision history with verdict filtering |
| JournalEntry | `/journal/:id` | Single decision drill-down |
| Models | `/models` | Model registry, state transitions, rollback |
| Discovery | `/discovery` | Hypothesis list, orthogonality audit, clustering |
| Agents | `/agents` | Agent runs, deliberation details, backtest comparison |
| Briefings | `/briefings` | AI market briefings (hourly/daily/weekly) |
| Workflows | `/workflows` | Enable/disable workflows, wave execution view |
| Physics | `/physics` | Verification results, OU parameters, Hurst, energy |
| Backtest | `/backtest` | Agent vs GRID performance comparison |
| Hyperspace | `/hyperspace` | Hyperspace node status and research leaderboard |
| SystemLogs | `/system-logs` | Backend log viewer |
| Settings | `/settings` | Auth management, logout |

## Key Components

| Component | Purpose |
|-----------|---------|
| RegimeCard | Display current regime with color-coded state |
| ConfidenceMeter | Visual confidence bar (0–100%) |
| SignalCard | Individual feature display with value and trend |
| TransitionGauge | Regime transition probability dial |
| DecisionModal | Capture manual decisions with context |
| KillSwitch | Emergency stop for automated systems |
| StatusDot | Green/red indicator for system health |
| NavBar | Bottom navigation with 5 primary tabs |

## State Management (Zustand)

Global store tracks:
- `activeView` — Current page
- `auth` — JWT token and user info
- `notifications` — Toast messages
- `jobs` — Discovery job status (queued, running, complete, failed)
- `hypotheses` — Hypothesis registry state
- `agentProgress` — Live agent run progress (stage, detail, %)
- `agentLastComplete` — Trigger for refreshing agent runs

## WebSocket

- Connects to `/ws?token=<jwt>` on page load
- Receives real-time updates: agent progress, job completion, regime changes
- Auto-reconnects on disconnect with exponential backoff

## API Client (api.js)

Comprehensive REST client with methods for every GRID endpoint:
- Auth (login, token refresh)
- Regime, signals, journal, models, discovery
- Agents (run, list, backtest)
- Briefings (generate, list, ask)
- Workflows (list, enable, disable)
- Physics (verify, transforms)
- Backtest, paper trade, system health

## Key Files

- `pwa/src/app.jsx` — Main React app, routing
- `pwa/src/store.js` — Zustand global state
- `pwa/src/api.js` — REST/WebSocket client
- `pwa/src/views/*.jsx` — Page components
- `pwa/src/components/*.jsx` — Reusable UI
- `pwa/vite.config.js` — Build configuration
