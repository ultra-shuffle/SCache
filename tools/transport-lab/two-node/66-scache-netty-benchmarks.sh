#!/usr/bin/env bash
set -Eeuo pipefail
exec bash "$(cd -- "$(dirname -- "$0")" && pwd -P)/run-stage.sh" 66 "$@"
