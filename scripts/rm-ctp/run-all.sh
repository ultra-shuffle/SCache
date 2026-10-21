#!/usr/bin/env bash
# One invocation per host; never edits Spark/SCache source or host drivers.
set -Eeuo pipefail
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
unset PYTHONHOME PYTHONPATH LD_PRELOAD JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS CLASSPATH
HERE=$(cd -- "$(dirname -- "$0")" && pwd -P)
command -v python3 >/dev/null || { echo 'PREREQUISITE_MISSING: python3 (required by the original detector too)'; exit 1; }
exec python3 "$HERE/auto-run.py" "$@"
