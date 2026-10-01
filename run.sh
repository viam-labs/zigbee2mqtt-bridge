#!/usr/bin/env bash
# Viam module entrypoint. Bootstraps a venv on first run, then execs the
# module server.
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -d ".venv" ] || [ ! -x "./.venv/bin/pip" ]; then
    rm -rf .venv
    python3 -m venv .venv
    ./.venv/bin/python -m ensurepip --upgrade
fi

if ! ./.venv/bin/python -c "import zigbee2mqtt_bridge" 2>/dev/null; then
    ./.venv/bin/pip install --upgrade pip
    ./.venv/bin/pip install .
fi

exec ./.venv/bin/python -m zigbee2mqtt_bridge.main "$@"
