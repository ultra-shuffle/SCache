#!/usr/bin/env bash
# Reproducible Release build for the production JNI library.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
URMA_ROOT="${URMA_ROOT:-}"
BUILD_DIR="${SPARK_URMA_BUILD_DIR:-$HERE/build}"
INSTALL_PREFIX="${SPARK_URMA_INSTALL_PREFIX:-$BUILD_DIR/install}"
if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVAC_PATH="$(command -v javac || true)"
  [[ -n "$JAVAC_PATH" ]] || { echo "Set JAVA_HOME to JDK 17; javac not found" >&2; exit 1; }
  JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$JAVAC_PATH")")")"
fi
[[ -x "$JAVA_HOME/bin/javac" ]] || { echo "Invalid JDK: $JAVA_HOME" >&2; exit 1; }
JAVA_VERSION="$("$JAVA_HOME/bin/javac" -version 2>&1)"
[[ "$JAVA_VERSION" == javac\ 17.* ]] || { echo "JDK 17 required; found $JAVA_VERSION" >&2; exit 1; }
JOBS="${SPARK_URMA_BUILD_JOBS:-2}"
SDK_ARGS=()
for SDK_VAR in URMA_INC URMA_LIBRARY URMA_COMMON_LIBRARY; do
  [[ -z "${!SDK_VAR:-}" ]] || SDK_ARGS+=("-D$SDK_VAR=${!SDK_VAR}")
done

# find_path/find_library cache successful results even after URMA_ROOT changes.
# Re-resolve only the selected SDK inputs; do not delete the build tree or keep
# linking the previous reference SDK. Explicit environment overrides below win.
cmake -S "$HERE" -B "$BUILD_DIR" \
  -UURMA_INC -UURMA_LIBRARY -UURMA_COMMON_LIBRARY \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$INSTALL_PREFIX" \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DURMA_ROOT="$URMA_ROOT" \
  -DBUILD_TESTS=OFF \
  -DBUILD_STANDALONE=OFF \
  -DJAVA_HOME="$JAVA_HOME" "${SDK_ARGS[@]}"
cmake --build "$BUILD_DIR" --target SparkUrma --parallel "$JOBS"
cmake --install "$BUILD_DIR"

LIBRARY="$INSTALL_PREFIX/lib/libSparkUrma.so"
[[ -f "$LIBRARY" ]] || { echo "missing installed library: $LIBRARY" >&2; exit 1; }
if readelf -d "$LIBRARY" | grep -Eq 'RPATH|RUNPATH'; then
  echo "unexpected RPATH/RUNPATH in $LIBRARY" >&2
  exit 1
fi
sha256sum "$LIBRARY"
