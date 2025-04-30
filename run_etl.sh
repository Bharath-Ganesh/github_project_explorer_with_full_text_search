#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# 0) Canonicalize this script’s directory
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "Entering script directory: $SCRIPT_DIR"
pushd "$SCRIPT_DIR" >/dev/null

# ---------------------------------------------------------------------------
# 1) Ensure config.yaml is present (render from .tpl if needed)
# ---------------------------------------------------------------------------
if [[ ! -f config.yaml ]]; then
  if [[ -f config.yaml.tpl ]]; then
    echo "Rendering config.yaml from config.yaml.tpl"
    envsubst < config.yaml.tpl > config.yaml
  else
    echo "Error: neither config.yaml nor config.yaml.tpl found in $SCRIPT_DIR"
    exit 1
  fi
else
  echo "config.yaml found; skipping render"
fi

# ---------------------------------------------------------------------------
# 2) Run ETL steps inside project_utils/
# ---------------------------------------------------------------------------
pushd project_utils >/dev/null
echo "Running ETL in: $PWD"

echo "1/2: Fetching & cloning forks"
python github_utils.py

echo "2/2: Extracting README metadata & loading into Postgres"
python postgres_uploader.py

popd >/dev/null

# ---------------------------------------------------------------------------
# 3) Return to original working directory
# ---------------------------------------------------------------------------
popd >/dev/null
echo "Returned to original directory: $PWD"
