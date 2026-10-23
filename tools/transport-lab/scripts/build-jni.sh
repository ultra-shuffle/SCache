#!/usr/bin/env bash
set -euo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec 9>"$R/build/logical-cluster.lock"
flock -n 9 || { echo "A cluster/build is active; do not overwrite its binaries"; exit 1; }

B="$R/build/umdk"
JDK=/home/xinzhang/codex-fullkit-runtime-20260918/toolchain/jdk-17-aarch64
g++ -std=c++17 -O2 -g -fPIC -shared -Wall -Wextra -Werror -pthread \
 -I"$JDK/include" -I"$JDK/include/linux" \
 -I"$R/repos/umdk/src/urpc/include/framework" -I"$R/repos/umdk/src/urma/lib/urma/core/include" \
 "$R/repos/SCache/native/spark_urpc/urpc_jni.cpp" \
 -L"$B/urpc/framework" -L"$B/urma/lib/urma/core" -L"$B/urma/common" \
 -lurpc_framework -lurma -lurma_common -o "$R/build/libScacheUrpc.so"
echo URPC_JNI_BUILD_PASS
