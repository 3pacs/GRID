FROM autoagent-base

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Python deps
RUN pip install --no-cache-dir \
    psycopg2-binary \
    requests \
    pandas \
    python-dateutil \
    sqlalchemy

# Environment — GRID db credentials and API keys are injected at runtime
# (docker run -e / --env-file). Never bake secrets into the image: the
# Alpha Vantage key used by tests/test.py is read from ALPHAVANTAGE_API_KEY
# (legacy name ALPHA_VANTAGE_KEY is also accepted) at runtime.
ENV GRID_DB_HOST=host.docker.internal
ENV GRID_DB_PORT=5432

# ClinicalTrials.gov API (no key needed)
ENV CT_GOV_BASE=https://clinicaltrials.gov/api/v2/studies

# EDGAR
ENV EDGAR_BASE=https://efts.sec.gov/LATEST/search-index

WORKDIR /task
