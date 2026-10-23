#!/usr/bin/env bash
set -euo pipefail
PACKAGE_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# All Java/native components are precompiled. Python only orchestrates processes.
exec python3 "$PACKAGE_HOME/scripts/run-logical-cluster.py" --stage "${1:-bench}"
