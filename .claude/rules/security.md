# Security Rules

These rules apply when writing API endpoints, database queries, or authentication code.

## SQL Safety

- NEVER use f-strings, `.format()`, or string concatenation for SQL queries
- ALWAYS use SQLAlchemy `text()` with `.bindparams()`: `text("SELECT ... WHERE id = :id").bindparams(id=value)`
- Existing SQL injection bugs — fix on sight:
  - `api/routers/regime.py:85-93` — `.format()` for INTERVAL with user `days` param
  - `journal/log.py:241` — string interpolation in interval clause

## Authentication

- JWT secret MUST be set via `GRID_JWT_SECRET` env var — default is `"dev-secret-change-me"` (`api/auth.py:35`)
- WebSocket auth uses query params (`api/main.py:117-152`) — tokens leak to logs/proxies. Don't make this worse; prefer subprotocol or first-message auth for new WebSocket endpoints
- Rate limiting is in-memory dict (`_login_attempts` in `api/auth.py:110-120`) — resets on restart, doesn't work multi-instance

## API Endpoints

- Add security headers for any new middleware (currently missing: X-Content-Type-Options, X-Frame-Options, HSTS, CSP)
- Implement proper pagination (offset/limit + total count) — `journal.py:36-66` limits to 100 rows without returning total (#9)
- Validate all user-supplied parameters before use
- Keep route handlers thin — delegate business logic to domain modules

### Gotchas

- Default DB password is `"changeme"` (`config.py:50`) — never deploy this to production
- `@lru_cache()` on database engine in `api/dependencies.py:19-40` never clears — config changes require restart (#18)
- Health endpoint only checks DB connectivity, not data freshness, pool health, or LLM availability (#30)

## Secrets

- Never commit `.env` files, API keys, or credentials
- All secrets must come from environment variables via `config.py` (pydantic-settings)
- Check `.env.example` exists and is kept in sync when adding new config fields
