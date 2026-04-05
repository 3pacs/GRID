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

# Environment — GRID db credentials injected at runtime
ENV GRID_DB_HOST=host.docker.internal
ENV GRID_DB_PORT=5432
ENV ALPHA_VANTAGE_KEY=SPT9IOAEYVUT7X6J

# ClinicalTrials.gov API (no key needed)
ENV CT_GOV_BASE=https://clinicaltrials.gov/api/v2/studies

# EDGAR
ENV EDGAR_BASE=https://efts.sec.gov/LATEST/search-index

WORKDIR /task
