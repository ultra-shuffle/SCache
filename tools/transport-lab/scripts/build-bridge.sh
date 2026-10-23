#!/usr/bin/env bash
set -euo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec 9>"$R/build/logical-cluster.lock"
flock -n 9 || { echo "A cluster/build is active; do not overwrite its binaries"; exit 1; }

B="$R/build/umdk"
g++ -std=c++17 -O2 -g -Wall -Wextra -Werror -pthread -DSCACHE_URPC_SMOKE_MAIN \
 -I"$R/repos/umdk/src/urpc/include/framework" -I"$R/repos/umdk/src/urma/lib/urma/core/include" \
 "$R/repos/SCache/native/spark_urpc/urpc_bridge.cpp" \
 -L"$B/urpc/framework" -L"$B/urma/lib/urma/core" -L"$B/urma/common" \
 -lurpc_framework -lurma -lurma_common -o "$R/build/urpc-bridge-smoke"
echo URPC_BRIDGE_BUILD_PASS
