# =============================================================================
# Dockerfile
# Custom Apache Airflow image with project Python dependencies pre-installed.
# =============================================================================
# Build:
#   docker build -t iot-airflow:2.7.2 .
# Or via docker-compose (set AIRFLOW_IMAGE_NAME in .env):
#   AIRFLOW_IMAGE_NAME=iot-airflow:2.7.2 docker-compose build
# =============================================================================

ARG AIRFLOW_VERSION=2.7.2
ARG PYTHON_VERSION=3.9

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Switch to root temporarily to install OS-level packages
USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user for pip installs
USER airflow

# Copy requirements first (Docker layer cache optimisation)
COPY --chown=airflow:root requirements.txt /requirements.txt

# Install project dependencies
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt

# Copy project scripts so they're available inside all Airflow containers
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/

# Verify key imports are resolvable (fail the build early if something is wrong)
RUN python -c "import pandas; import psycopg2; import numpy; print('All dependencies OK')"
