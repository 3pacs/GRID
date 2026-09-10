# Frontend Rules

These rules apply when working on the React PWA in `grid/pwa/`.

## Stack

- React 18 with functional components and hooks
- Zustand for state management — keep stores focused and minimal
- Lucide React for icons
- Vite for bundling — dev server on port 5173, proxies `/api` to backend port 8000

## Patterns

- Components go in `pwa/src/` following existing structure
- Use the existing Zustand store pattern — don't introduce Redux or Context API
- API calls should go through a centralized fetch wrapper
- Handle loading and error states for all async operations
- The PWA is served from FastAPI in production — ensure builds work with `npm run build`

### Gotchas

- PWA static serving in `api/main.py:719-761` assumes `pwa_dist/` or `pwa/` exists — falls back to serving `pwa/` source directly in dev if `pwa_dist/` is missing, and silently serves `index.html` for any unmatched path once one of the two exists (#37)
- 16 Vitest suites (109 tests) exist under `pwa/src/__tests__/` and run in CI (`.github/workflows/test.yml`'s `frontend-build` job: `npm ci`, `npx tsc --noEmit`, `npm run test`, `npm run build`) — run locally with `npm run test`
- Service worker (`service-worker.js`) and manifest (`manifest.json`) are at the PWA root

## Commands

```bash
cd grid/pwa && npm install    # Install dependencies
cd grid/pwa && npm run dev    # Dev server with hot reload
cd grid/pwa && npm run build  # Production build → served by FastAPI
```
