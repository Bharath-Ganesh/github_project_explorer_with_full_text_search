#!/usr/bin/env bash
set -euo pipefail

# 1) Render config.yaml from the template into /tmp
echo "→ Rendering config.yaml into /tmp/config.yaml"
envsubst < /app/config.yaml.tpl > /tmp/config.yaml

# 2) Point the Python loader at that file
export CONFIG_PATH=/tmp/config.yaml

# 3) Hand off to the container’s CMD
exec "$@"
