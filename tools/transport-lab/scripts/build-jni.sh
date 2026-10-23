#!/usr/bin/env bash
set -euo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec 9>"$R/build/logical-cluster.lock"
flock -n 9 || { echo ACTIVE_CLUSTER_OR_BUILD; exit 1; }
JDK="$R/runtime/package/dependencies/jdk"
g++ -std=c++17 -O2 -g -fPIC -shared -Wall -Wextra -Werror -pthread \
 -I"$JDK/include" -I"$JDK/include/linux" \
 -I"$R/repos/umdk/src/urpc/include/umq" \
 "$R/repos/SCache/native/spark_umq/umq_jni.cpp" \
 -L"$R/build/umdk/urpc/umq" -L"$R/build/umdk/urpc/umq/qbuf" \
 -lumq -lumq_buf -ldl -o "$R/build/libScacheUmq.so"
echo UMQ_JNI_BUILD_PASS
