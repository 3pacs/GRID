# GRID Security Audit Report
**Date:** 2026-03-30
**Scope:** Full Python codebase security review
**Focus:** OWASP Top 10, authentication, data protection, and code execution

---

## Executive Summary

The GRID codebase has **3 CRITICAL** security vulnerabilities, **5 HIGH** risk issues, and **4 MEDIUM** concerns. The authentication system is reasonably secure but has token leakage and rate-limiting issues. Database queries are mostly safe (uses parameterized queries), but the security headers middleware is well-implemented. Major risks center on WebSocket token exposure, secrets management, and incomplete API key validation.

---

## CRITICAL Issues (Block Deployment)

### 1. WebSocket Token Leakage via Query Parameters
**File:** `api/main.py:571-591`
**Severity:** CRITICAL
**CWE:** CWE-598 (Use of GET Request with Sensitive Query Strings)

**Problem:**
```python
@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(default=""),  # <-- TOKEN IN QUERY PARAMS
) -> None:
    if not token or not verify_token(token):
        await websocket.close(code=4001, reason="Invalid token")
```

Tokens passed as query parameters are:
- Logged in server access logs, proxy logs, and browser history
- Visible in Referer headers when users click links
- Exposed to intermediaries (load balancers, CDNs)
- Vulnerable to log injection attacks

**Impact:** High-privilege users connecting to WebSocket can have their JWTs leaked to server logs and third-party services.

**Remediation (HIGH effort):**
- [ ] Implement **WebSocket subprotocol authentication** (pass token in first frame after connection)
- [ ] Alternative: Use Bearer header upgrade via HTTP before WebSocket handshake
- [ ] Log token hash, not full token: `token[:8]` instead of token
- [ ] Advise clients to connect from HTTPS-only contexts

---

### 2. Default Database Password in Non-Development Environments
**File:** `config.py:50, 224-230`
**Severity:** CRITICAL
**CWE:** CWE-521 (Weak Password Requirements)

**Problem:**
```python
DB_PASSWORD: str = "changeme"  # line 50

@field_validator("DB_PASSWORD")
@classmethod
def _check_db_password(cls, v: str) -> str:
    """Reject default password in non-development environments."""
    env = os.getenv("ENVIRONMENT", "development")
    if env != "development" and v == "changeme":
        raise ValueError(...)
```

**Current Status:** The validation raises an error in production, but:
1. Default is still `"changeme"` in code
2. If someone misconfigures `ENVIRONMENT` env var, database is exposed
3. `.env.example` documents the default, making it discoverable

**Impact:** Database compromise if misdeployed or if env var logic is bypassed.

**Remediation (MEDIUM effort):**
- [ ] Remove hardcoded default: `DB_PASSWORD: str = ""`
- [ ] Change error to a **startup crash** (not just validation error) if production without secret
- [ ] Audit `.env.example` — document that example value must be replaced
- [ ] Add pre-startup check in `api/main.py` lifespan to reject default password in production

---

### 3. JWT Secret Default "dev-secret-change-me" in Non-Production
**File:** `api/auth.py:55-68`
**Severity:** CRITICAL
**CWE:** CWE-798 (Use of Hard-Coded Credentials)

**Problem:**
```python
def _get_settings() -> tuple[str, str, int]:
    """Return (password_hash, jwt_secret, expire_hours) from env."""
    jwt_secret = os.getenv("GRID_JWT_SECRET", "")
    if not jwt_secret:
        environment = os.getenv("ENVIRONMENT", "development")
        if environment != "development":
            raise RuntimeError(
                "GRID_JWT_SECRET must be set in non-development environments..."
            )
        jwt_secret = "dev-secret-change-me"  # <-- HARDCODED
```

**Issue:**
- If `ENVIRONMENT` env var is missing (defaults to "development"), the hardcoded secret is used even in production
- The secret is weak and well-known (appears in production code)
- Any JWT signed with this secret can be forged

**Impact:** Full authentication bypass. Attackers can forge admin JWTs and gain full system access.

**Remediation (MEDIUM effort):**
- [ ] Set `ENVIRONMENT` to explicit production mode: require `ENVIRONMENT=production` (not just "not development")
- [ ] Change hardcoded secret to a random placeholder that fails on use
- [ ] Add a startup check in `lifespan()` that crashes if `ENVIRONMENT=production` and `GRID_JWT_SECRET` is weak or empty

---

## HIGH Issues (Should Fix Before Production)

### 4. Incomplete API Key Validation at Startup
**File:** `config.py:208-218`
**Severity:** HIGH
**CWE:** CWE-347 (Improper Verification of Cryptographic Signature)

**Problem:**
```python
@field_validator("FRED_API_KEY")
@classmethod
def _check_fred_key(cls, v: str) -> str:
    """Allow empty key only in development; raise otherwise."""
    env = os.getenv("ENVIRONMENT", "development")
    if env != "development" and not v:
        raise ValueError("FRED_API_KEY must be set...")
    return v
```

**Issue:** Only `FRED_API_KEY` is validated at startup. Other critical keys are ignored:
- `KOSIS_API_KEY`, `COMTRADE_API_KEY`, `JQUANTS_EMAIL`, `USDA_NASS_API_KEY`, `NOAA_TOKEN`, `EIA_API_KEY` — **no validation**
- If missing in production, data ingestion silently fails (graceful degradation may hide errors)
- No operator awareness that critical sources are offline

**Impact:** Silent data quality issues. Operator doesn't know which sources are misconfigured.

**Remediation (LOW effort):**
- [ ] Add validators for all critical data source keys (at least KOSIS, COMTRADE in non-development)
- [ ] Log warnings in `api/main.py` startup if optional keys are missing
- [ ] Update `audit_api_keys()` to check non-empty values, not just existence

---

### 5. Rate Limiting Not Persistent Across Restarts
**File:** `api/auth.py:43-48, 277-306`
**Severity:** HIGH
**CWE:** CWE-613 (Insufficient Session Expiration)

**Problem:**
```python
_RATE_LIMIT_PATH = str(
    Path(os.getenv("GRID_DATA_DIR", tempfile.gettempdir())) / "grid_rate_limits"
)

def _check_rate_limit(client_ip: str) -> None:
    """Raise 429 if too many login attempts."""
    try:
        with shelve.open(_rate_limit_path) as db:  # <-- shelve can fail
            attempts: list[float] = db.get(client_ip, [])
            attempts = [t for t in attempts if now - t < _RATE_LIMIT_WINDOW]
```

**Issues:**
1. **Multi-instance:** If service restarts or second instance starts, rate limit resets
2. **No DB persistence:** shelve is file-based, no transaction safety
3. **Temp directory risk:** If `GRID_DATA_DIR` is not set, falls back to `/tmp`, which may be cleared
4. **Silent failure:** Exception handlers log warning but allow request to proceed

**Impact:** Brute force attack on login endpoint after service restart. Attackers can make unlimited login attempts after each restart.

**Remediation (MEDIUM effort):**
- [ ] Move rate limiting to PostgreSQL: `CREATE TABLE login_rate_limits (client_ip TEXT, attempt_count INT, window_start TIMESTAMPTZ, PRIMARY KEY (client_ip))`
- [ ] Query database to check/record attempts atomically
- [ ] Set `GRID_DATA_DIR` to persistent location (not `/tmp`) or require it in production config

---

### 6. Missing WebSocket Rate Limit Enforcement
**File:** `api/main.py:474-504`
**Severity:** HIGH
**CWE:** CWE-770 (Allocation of Resources Without Limits or Throttling)

**Problem:**
```python
_MAX_WS_CONNECTIONS = 200  # prevent memory exhaustion
_WS_MAX_CONNECTS_PER_MIN = 10

def _check_ws_rate(ip: str) -> bool:
    """Return True if IP is within WebSocket connection rate limit."""
    now = time.time()
    attempts = _ws_connect_attempts.get(ip, [])
    attempts = [t for t in attempts if t > now - 60]  # <-- in-memory, resets on restart
    _ws_connect_attempts[ip] = attempts
    if len(attempts) >= _WS_MAX_CONNECTS_PER_MIN:
        return False
    attempts.append(now)
    return True
```

**Issues:**
1. Same as rate limiting: in-memory, resets on restart
2. Only enforces **connection rate** (10/min), not **message rate**
3. Attacker can spam messages within a single connection to exhaust resources
4. No per-user limits, only per-IP (shared IPs behind corporate NAT are blocked together)

**Impact:** Denial-of-service via WebSocket message flooding.

**Remediation (MEDIUM effort):**
- [ ] Add database-backed message rate limiting per token (not IP)
- [ ] Limit messages/second per connection: 100/sec default
- [ ] Drop high-frequency connections after threshold

---

### 7. Incomplete CORS Configuration in Development
**File:** `api/main.py:392-404`
**Severity:** HIGH
**CWE:** CWE-346 (Origin Validation Error)

**Problem:**
```python
allowed_origins = os.getenv("GRID_ALLOWED_ORIGINS", "").split(",")
allowed_origins = [o.strip() for o in allowed_origins if o.strip()]
if _environment == "development":
    allowed_origins = ["http://localhost:5173", "http://localhost:8000", "http://127.0.0.1:5173"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,  # <-- allows cookie-based auth
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)
```

**Issues:**
1. **`allow_credentials=True` with broadened origins** in development
2. In production, if `GRID_ALLOWED_ORIGINS` is empty, CORS is effectively disabled (no origins allowed)
3. No validation that `GRID_ALLOWED_ORIGINS` is a comma-separated list of valid URLs

**Impact:**
- Development: Cross-site request forgery (CSRF) via credentials
- Production: Accidental CORS misconfiguration blocks legitimate requests

**Remediation (LOW effort):**
- [ ] In development, set explicit dev-only origins (do NOT use `*`)
- [ ] In production, validate `GRID_ALLOWED_ORIGINS` is non-empty and properly formatted
- [ ] Consider `allow_credentials=False` for stateless API (JWTs don't need credentials)

---

## MEDIUM Issues (Should Fix)

### 8. Insufficient Error Message Detail Leakage
**File:** `api/auth.py:321-323, 348-349`
**Severity:** MEDIUM
**CWE:** CWE-209 (Information Exposure Through an Error Message)

**Problem:**
```python
if not user or not verify_password(body.password, user["pw_hash"]):
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid username or password",  # Acceptable
    )

if not verify_password(body.password, pw_hash):
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid password",  # <-- Leaks: master password mode exists
    )
```

**Issue:** Different error messages for "username not found" vs "password wrong" leak whether master password is in use.

**Impact:** Attackers can enumerate users and determine auth mode (user-based vs master password).

**Remediation (LOW effort):**
- [ ] Use same error message for both failures: `"Invalid credentials"`
- [ ] Avoid mentioning "password" in error (could leak auth mechanism)

---

### 9. JWT Expiry Not Validated on Token Refresh
**File:** `api/auth.py:145-152`
**Severity:** MEDIUM
**CWE:** CWE-613 (Insufficient Session Expiration)

**Problem:**
```python
def verify_token(token: str) -> bool:
    """Return True if token is valid and not expired."""
    _, jwt_secret, _ = _get_settings()
    try:
        jwt.decode(token, jwt_secret, algorithms=["HS256"])  # <-- decode checks exp
        return True
    except JWTError:
        return False
```

**Issue:** `jwt.decode()` checks `exp` claim if present, but there's no explicit expiry validation in logs. If token is passed with manipulated `exp` in the future, it may pass. Also, no revocation mechanism exists.

**Impact:** Tokens cannot be revoked or force-expired (e.g., after password change).

**Remediation (MEDIUM effort):**
- [ ] Add token revocation list (simple: `SET` in Redis, or DB table with revoked token hashes)
- [ ] Check revocation list in `verify_token()` and `require_auth()`
- [ ] On password change, revoke all user's tokens

---

### 10. No Rate Limiting on Password Reset / Account Creation
**File:** `api/auth.py:361-409`
**Severity:** MEDIUM
**CWE:** CWE-770 (Allocation of Resources Without Limits or Throttling)

**Problem:**
```python
@router.post("/register", response_model=LoginResponse)
async def register(body: RegisterRequest, request: Request) -> LoginResponse:
    """Self-registration for new contributor accounts."""
    client_ip = request.client.host if request.client else "unknown"
    _check_rate_limit(client_ip)  # <-- reuses login rate limit (5/min)
    _record_login_attempt(client_ip)
```

**Issue:**
1. Registration and login share the same rate limit bucket
2. After 5 login attempts, cannot register (DoS)
3. No email verification for registration (anyone can register any email)

**Impact:**
- Account enumeration attack
- Spam registrations with random emails
- DoS: Fill rate limit bucket with registrations, blocking logins

**Remediation (MEDIUM effort):**
- [ ] Separate rate limits: `REGISTER_RATE_LIMIT = 3/hour` (email verification rate limit)
- [ ] Require email verification before account activation
- [ ] Check for existing registrations from same IP

---

## LOW Issues (Minor Concerns)

### 11. Security Headers Could Be Stricter
**File:** `api/main.py:339-360`
**Severity:** LOW
**CWE:** CWE-345 (Insufficient Verification of Data Authenticity)

**Current headers:**
```python
response.headers["X-Content-Type-Options"] = "nosniff"  # Good
response.headers["X-Frame-Options"] = "DENY"           # Good
response.headers["X-XSS-Protection"] = "1; mode=block" # Obsolete (modern browsers ignore)
response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"  # Good
response.headers["Content-Security-Policy"] = (
    "default-src 'self'; "                          # Good but strict
    "script-src 'self'; "                           # Good
    "style-src 'self' 'unsafe-inline'; "            # ⚠ unsafe-inline needed?
    "img-src 'self' data:; "                        # Good
    "connect-src 'self' ws: wss:; "                 # Good
    ...
)
```

**Minor issues:**
1. CSP `style-src 'unsafe-inline'` — can be tightened with nonce if React doesn't require it
2. Missing `Permissions-Policy` header (formerly `Feature-Policy`)
3. Missing `X-Permitted-Cross-Domain-Policies`

**Impact:** Low. Current headers are strong. CSS injection possible but unlikely.

**Remediation (LOW effort):**
- [ ] Remove `X-XSS-Protection` (obsolete)
- [ ] Add `Permissions-Policy: geolocation=(), microphone=(), camera=()`
- [ ] Test if `style-src 'unsafe-inline'` can be removed or use nonce

---

### 12. Logging Sensitive Data in Error Messages
**File:** `api/auth.py:387, 391`
**Severity:** LOW
**CWE:** CWE-532 (Insertion of Sensitive Information into Log File)

**Problem:**
```python
except psycopg2.errors.UniqueViolation:
    raise HTTPException(409, f"Username '{body.username}' already exists")  # <-- logs username
except Exception as e:
    log.error("Failed to register user: {e}", e=e)  # <-- logs full exception
```

**Impact:** Usernames appear in logs/errors, could leak to monitoring systems.

**Remediation (LOW effort):**
- [ ] Return generic error: `"Username already in use"`
- [ ] Log error ID instead of full exception: `log.error("Registration failed: {id}", id=uuid.uuid4())`

---

## Summary Table

| ID | Issue | Severity | File | Effort | Status |
|----|-------|----------|------|--------|--------|
| 1 | WebSocket token in query params | CRITICAL | `api/main.py:574` | HIGH | OPEN |
| 2 | Default DB password in code | CRITICAL | `config.py:50` | MEDIUM | OPEN |
| 3 | Default JWT secret | CRITICAL | `api/auth.py:66` | MEDIUM | OPEN |
| 4 | Incomplete API key validation | HIGH | `config.py:208` | LOW | OPEN |
| 5 | Rate limit not persistent | HIGH | `api/auth.py:282` | MEDIUM | OPEN |
| 6 | WebSocket rate limit not DB-backed | HIGH | `api/main.py:482` | MEDIUM | OPEN |
| 7 | CORS allow_credentials=True | HIGH | `api/main.py:401` | LOW | OPEN |
| 8 | Error message leakage | MEDIUM | `api/auth.py:321` | LOW | OPEN |
| 9 | No token revocation | MEDIUM | `api/auth.py:145` | MEDIUM | OPEN |
| 10 | No registration rate limit | MEDIUM | `api/auth.py:361` | MEDIUM | OPEN |
| 11 | Security headers minor gaps | LOW | `api/main.py:339` | LOW | OPEN |
| 12 | Logging sensitive data | LOW | `api/auth.py:387` | LOW | OPEN |

---

## Positive Findings (Well Done)

1. **SQL Injection Prevention:** Nearly all database queries use parameterized queries (`text()` + bindparams). No active SQL injection found.
2. **Password Hashing:** Uses bcrypt via `passlib.context.CryptContext` — strong algorithm.
3. **Security Headers Middleware:** Well-implemented with CSP, HSTS (in production), X-Frame-Options, etc.
4. **Role-Based Access Control:** `require_role()` decorator properly enforces admin/contributor roles.
5. **Input Validation:** Registration validates username length, password length before processing.
6. **Secret Management:** Uses environment variables via pydantic-settings, no hardcoded API keys in source code.

---

## Remediation Priority

### Phase 1 (Blocking — Fix Before Deployment)
1. **Fix CRITICAL #3:** Change JWT secret validation to crash if weak in production
2. **Fix CRITICAL #1:** Replace WebSocket token query param with subprotocol or first-message auth
3. **Fix CRITICAL #2:** Remove DB password default, require explicit secret

### Phase 2 (Before Production Use)
4. **Fix HIGH #5:** Move rate limiting to PostgreSQL
5. **Fix HIGH #6:** Add database-backed WebSocket message rate limiting
6. **Fix HIGH #4:** Validate all critical API keys at startup
7. **Fix HIGH #7:** Review CORS configuration, use explicit development origins

### Phase 3 (Deployment)
8. Fix MEDIUM #9, #10, #8
9. Add monitoring: log all auth failures, token usage, rate limit triggers
10. Set up alerting for repeated failed logins, unusual token generation

---

## Testing Recommendations

1. **Unit Tests:**
   - Token expiry validation with expired and future-dated tokens
   - Rate limit reset behavior across multiple calls
   - Password hashing and verification

2. **Integration Tests:**
   - WebSocket authentication with valid/invalid tokens
   - Concurrent login attempts (rate limiting)
   - CORS preflight requests from different origins

3. **Security Tests (Manual):**
   - Attempt WebSocket connection without token → should fail
   - Attempt to forge JWT with default secret (only in dev!)
   - Brute-force login endpoint with 100+ attempts → should be rate limited
   - Test CORS with `curl -H "Origin: https://attacker.com"`

---

**Generated:** 2026-03-30 | **Reviewer:** Claude Security Agent
