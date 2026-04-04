FROM autoagent-base

# PostgreSQL client library for GRID DB access
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Quantitative finance stack
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    pandas==2.2.2 \
    numpy==1.26.4 \
    scikit-learn==1.5.1 \
    scipy==1.14.0 \
    statsmodels==0.14.2 \
    lightgbm==4.4.0 \
    xgboost==2.1.0

# Copy GRID bridge module into the container
COPY files/ /app/files/

# Ensure /app/files is on Python path
ENV PYTHONPATH="/app/files:/app:${PYTHONPATH}"

WORKDIR /app
