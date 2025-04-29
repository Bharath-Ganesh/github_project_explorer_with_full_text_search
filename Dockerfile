FROM python:3.9-slim AS etl
RUN apt-get update \
 && apt-get install -y --no-install-recommends git bash gettext-base \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# only install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Entrypoint script
ENTRYPOINT ["bash"]
