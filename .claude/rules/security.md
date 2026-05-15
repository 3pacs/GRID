# Security Rules

These rules apply when writing API endpoints, database queries, or authentication code.

## SQL Safety

- NEVER use f-strings, `.format()`, or string concatenation for SQL queries.
- ALWAYS use SQLAlchemy `text()` with bound parameters: `text("SELECT ... WHERE id = :id")` with the params dict, or `.bindparams(id=value)`.
- For dynamic intervals, use `make_interval(days => :days)` (or `hours`, `weeks`) and bind the value — never interpolate the unit count into the SQL string.

## Authentication

- JWT secret MUST be set via `GRID_JWT_SECRET`. `api/auth.py` rejects the placeholder `"dev-secret-change-me"` and raises on startup if it's still in use — don't paper over that check.
- New WebSocket endpoints MUST use first-message auth (the pattern in `api/main.py`'s `/ws` handler — accept, then `receive_text()` with a short timeout, parse `{"token": "<jwt>"}`, verify, close on failure). Don't reintroduce query-param tokens — they leak to proxy and access logs.
- Rate limiting is persisted in the `grid_rate_limits` SQLite table (see `api/auth.py`). Extend that table when adding new rate-limited endpoints; don't reintroduce a per-process in-memory dict — it breaks multi-worker deployments.

## API Endpoints

- Add security headers for any new middleware (currently missing: X-Content-Type-Options, X-Frame-Options, HSTS, CSP).
- List endpoints must return `total` alongside `entries`, plus `limit`, `offset`, and `has_more` — follow the pattern in `api/routers/journal.py::get_all`.
- Validate all user-supplied parameters at the boundary (FastAPI `Query(..., ge=, le=)` or pydantic models). Don't trust that downstream code will clamp.
- Keep route handlers thin — delegate business logic to domain modules.

### Gotchas

- `config.py` enforces a non-`changeme` DB password outside `development` env. Don't add a code path that bypasses the check.
- The DB engine in `api/dependencies.py` is a clearable module-level singleton (not `@lru_cache`). If you change DB config at runtime, call the existing clear helper rather than re-introducing an unclearable cache.
- The `/health` endpoint now checks data freshness, pool saturation, scheduler threads, WebSocket clients, disk, and LLM availability — extend `api/routers/system.py::health` when adding new subsystems instead of writing a parallel check endpoint.

## Secrets

- Never commit `.env` files, API keys, or credentials.
- All secrets must come from environment variables via `config.py` (pydantic-settings).
- Check `.env.example` exists and is kept in sync when adding new config fields.
