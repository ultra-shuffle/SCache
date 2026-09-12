#!/usr/bin/env bash
# Reproducible Release build for the production JNI library.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SPARK_URMA_BUILD_DIR:-$HERE/build}"
INSTALL_PREFIX="${SPARK_URMA_INSTALL_PREFIX:-$BUILD_DIR/install}"
JOBS="${SPARK_URMA_BUILD_JOBS:-2}"

if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVAC="$(command -v javac || true)"
  [[ -n "$JAVAC" ]] || { echo "set JAVA_HOME or install a JDK" >&2; exit 1; }
  JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$JAVAC")")")"
fi

CMAKE_UMDK=()
if [[ -n "${UMDK_ROOT:-}" ]]; then
  CMAKE_UMDK=(-DUMDK_ROOT="$UMDK_ROOT")
fi

cmake -S "$HERE" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
  "${CMAKE_UMDK[@]}" \
  -DBUILD_TESTS=OFF \
  -DBUILD_STANDALONE=OFF \
  -DJAVA_HOME="$JAVA_HOME"
cmake --build "$BUILD_DIR" --target SparkUrma --parallel "$JOBS"
cmake --install "$BUILD_DIR"

LIBRARY="$INSTALL_PREFIX/lib/libSparkUrma.so"
[[ -f "$LIBRARY" ]] || { echo "missing installed library: $LIBRARY" >&2; exit 1; }
if readelf -d "$LIBRARY" | grep -Eq 'RPATH|RUNPATH'; then
  echo "unexpected RPATH/RUNPATH in $LIBRARY" >&2
  exit 1
fi
sha256sum "$LIBRARY"
