#!/usr/bin/env bash
# Clean production + integration artifact build with package-content gates.
set -euo pipefail

SCACHE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_ROOT="${SCACHE_RELEASE_BUILD_ROOT:-$(mktemp -d /tmp/scache_release.XXXXXX)}"
INSTALL_DIR="${SCACHE_RELEASE_INSTALL_DIR:-$SCACHE_ROOT/target/release}"
SBT_BIN="${SBT_BIN:-sbt}"
SBT_LAUNCH_JAR="${SBT_LAUNCH_JAR:-}"
TARGET_DIR="$BUILD_ROOT/target"
SCALA_DIR="$TARGET_DIR/scala-2.13"
ASSEMBLY="$SCALA_DIR/SCache-assembly-0.1.0-SNAPSHOT.jar"
INTEGRATION="$SCALA_DIR/SCache-integration-0.1.0-SNAPSHOT.jar"
BUILD_INTEGRATION="${SCACHE_BUILD_INTEGRATION:-0}"

if [[ -z "${JAVA_HOME:-}" ]]; then
  JAVAC="$(command -v javac || true)"
  [[ -n "$JAVAC" ]] || { echo "set JAVA_HOME or install a JDK" >&2; exit 1; }
  JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$JAVAC")")")"
fi

mkdir -p "$BUILD_ROOT" "$INSTALL_DIR"
cd "$SCACHE_ROOT"
if [[ -n "$SBT_LAUNCH_JAR" ]]; then
  [[ -f "$SBT_LAUNCH_JAR" ]] || {
    echo "missing SBT_LAUNCH_JAR: $SBT_LAUNCH_JAR" >&2
    exit 1
  }
  SBT_COMMAND=("$JAVA_HOME/bin/java" -jar "$SBT_LAUNCH_JAR")
  SBT_OPTIONS=()
else
  SBT_COMMAND=("$SBT_BIN")
  SBT_OPTIONS=(-batch)
fi
SBT_TASKS=(clean assembly)
if [[ "$BUILD_INTEGRATION" == "1" ]]; then
  SBT_TASKS+=("Integration / packageBin")
fi
"${SBT_COMMAND[@]}" "${SBT_OPTIONS[@]}" \
  "set target := file(\"$TARGET_DIR\")" "${SBT_TASKS[@]}"

[[ -f "$ASSEMBLY" ]]
FORBIDDEN='org/scache/(deploy/(DaemonClientIpcIntegrationTest|DaemonRpcLifecycleIntegrationNode|NettyBlockTransferIntegrationNode|PoolIpcSelfTest)|network/ub/(SCacheUrmaTest|UbPhase4ContractTest|UbPhase4LifecycleTest|UbPhase4ServiceInitTest|UrmaClient|UrmaLocalContractTest|UrmaRegistrationLifecycleTest|UrmaRuntimeLifecycleTest|UrmaServer)).*class$'
if "$JAVA_HOME/bin/jar" tf "$ASSEMBLY" | grep -Eq "$FORBIDDEN"; then
  echo "production assembly contains integration/diagnostic entry classes" >&2
  exit 1
fi
ARTIFACTS=("$ASSEMBLY")
if [[ "$BUILD_INTEGRATION" == "1" ]]; then
  [[ -f "$INTEGRATION" ]]
  "$JAVA_HOME/bin/jar" tf "$INTEGRATION" | grep -q \
    'org/scache/deploy/NettyBlockTransferIntegrationNode.class'
  ARTIFACTS+=("$INTEGRATION")
fi

CHECKSUM_FILES=()
for artifact in "${ARTIFACTS[@]}"; do
  cp "$artifact" "$INSTALL_DIR/"
  CHECKSUM_FILES+=("$INSTALL_DIR/$(basename "$artifact")")
done
sha256sum "${CHECKSUM_FILES[@]}" | tee "$INSTALL_DIR/SHA256SUMS"
printf 'build_root=%s\ninstall_dir=%s\n' "$BUILD_ROOT" "$INSTALL_DIR"
