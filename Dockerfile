# ─────────────────────────────────────────────────────────────────────────────
# Base image: OS + Python deps
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS base

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      libpq-dev \
      git \
      bash \
      gettext-base \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# ─────────────────────────────────────────────────────────────────────────────
# ETL image: render config.yaml at build-time and run ETL
# ─────────────────────────────────────────────────────────────────────────────
FROM base AS etl

# copy only the template (never write back to host)
COPY config.yaml.tpl .
# render into the image’s own FS
RUN envsubst < config.yaml.tpl > config.yaml

COPY run_etl.sh .
RUN chmod +x run_etl.sh

# ETL entrypoint runs and then container exits
ENTRYPOINT ["bash", "run_etl.sh"]


# ─────────────────────────────────────────────────────────────────────────────
# App image: also has config.yaml baked in
# ─────────────────────────────────────────────────────────────────────────────
FROM base AS app

COPY config.yaml.tpl .
RUN envsubst < config.yaml.tpl > config.yaml

COPY . .

EXPOSE 8501
ENTRYPOINT ["streamlit","run","app.py","--server.port","8501","--server.address","0.0.0.0"]
