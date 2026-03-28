# AstroGrid Next Agent

## Done
- buildless AstroGrid app at [`astrogrid_web/index.html`](/Users/anikdang/dev/17th/grid/astrogrid_web/index.html)
- shell at [`astrogrid_web/app.js`](/Users/anikdang/dev/17th/grid/astrogrid_web/app.js)
- styles at [`astrogrid_web/styles.css`](/Users/anikdang/dev/17th/grid/astrogrid_web/styles.css)
- deterministic engine layer at [`astrogrid_web/engines.js`](/Users/anikdang/dev/17th/grid/astrogrid_web/engines.js)
- visual helpers at [`astrogrid_web/visuals.js`](/Users/anikdang/dev/17th/grid/astrogrid_web/visuals.js)
- local ephemeris copy at [`astrogrid_web/lib/ephemeris.js`](/Users/anikdang/dev/17th/grid/astrogrid_web/lib/ephemeris.js)
- FastAPI mount added in [`api/main.py`](/Users/anikdang/dev/17th/grid/api/main.py)

## What Works
- AstroGrid no longer depends on Node to exist as a web surface.
- The app computes the sky locally from the ephemeris module.
- Lenses, engines, Seer, persona Q&A, and local logs are wired in browser-side.
- GRID token reuse is same-origin through `localStorage.getItem('grid_token')`.
- If the token is absent or backend calls fail, the local observatory still renders.
- When the shell is running on localhost, it defaults its upstream API base to `https://grid.stepdad.finance`.
- The shell also supports a manual API base and token override in the UI.
- `api.main:app` now boots locally and serves AstroGrid at [http://127.0.0.1:8000/astrogrid](http://127.0.0.1:8000/astrogrid).
- `api/main.py` now skips optional routers that fail import so AstroGrid can boot without the full research stack.
- The shared LLM path now prefers OpenAI first, then falls back to llama.cpp, then Ollama.

## What Changed To Unblock Boot
Dependency installation originally hit broken package pins:
- `fedfred>=3.0.0` in [`requirements.txt`](/Users/anikdang/dev/17th/grid/requirements.txt) does not resolve from PyPI
- `edgartools>=5.0.0` in [`requirements.txt`](/Users/anikdang/dev/17th/grid/requirements.txt) also does not resolve from PyPI in this environment

Those are now patched in [`requirements.txt`](/Users/anikdang/dev/17th/grid/requirements.txt), and `api/main.py` now lazy-loads non-critical routers.

## Current Local State
- AstroGrid shell verified:
  - [http://127.0.0.1:8000/astrogrid](http://127.0.0.1:8000/astrogrid)
- AstroGrid API verified with auth token:
  - [`/api/v1/astrogrid/overview`](/Users/anikdang/dev/17th/grid/api/routers/astrogrid.py)
- Local PostgreSQL is still not running on `localhost:5432`, so DB-backed routes degrade or warn.
- No `OPENAI_API_KEY` or `AGENTS_OPENAI_API_KEY` is currently set in this environment, so the new provider order falls through to local backends.

## Fastest Path To Finish
1. Set `OPENAI_API_KEY` in `.env` if you want OpenAI to be the active LLM.
2. Start PostgreSQL if you want DB-backed GRID routes to fully work.
3. Install into the repo virtualenv:
   - `.venv/bin/pip install -r requirements.txt`
4. Boot:
   - `.venv/bin/python -m uvicorn api.main:app --host 127.0.0.1 --port 8000`
5. Open:
   - [http://127.0.0.1:8000/astrogrid](http://127.0.0.1:8000/astrogrid)

## Likely Next Improvements
- replace copied ephemeris lib with a cleaner shared source path or generated artifact
- add authoritative backend object payload for more than the current core set
- push Seer logs into DB instead of browser localStorage
- add explicit object registry and precision badges from backend contracts
- wire live timeline/correlation/briefing panels against real AstroGrid endpoints after API boot is restored
