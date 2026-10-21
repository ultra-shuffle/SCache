#!/usr/bin/env bash
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/package-env.sh"
: "${JAVA_HOME:?Set JAVA_HOME to JDK 17}"
: "${SCACHE_TEST_CLASSPATH_FILE:?Run prepare-tests.sh first}"
: "${SCACHE_JNI_DIR:?Directory containing the new branch libSparkUrma.so}"
: "${UMDK_LIBRARY_DIRS:?Colon-separated directories of matching installed UMDK libraries}"
MODE=${1:-}
if [[ "$MODE" == --check ]]; then
  [[ $# == 2 ]] || { echo 'usage: run-node.sh --check log-directory' >&2; exit 2; }
  LOG_ROOT=$2
elif [[ $# == 9 && ( "$MODE" == A || "$MODE" == B ) ]]; then
  LOG_ROOT=$2
else
  echo 'usage: run-node.sh A|B work-dir device eid-index priority bind-ip control-port peer-ip peer-control-port' >&2
  exit 2
fi
mkdir -p "$LOG_ROOT"
LOG=$(mktemp -d "$LOG_ROOT/launcher-XXXXXXXX")
exec > >(tee "$LOG/terminal.log") 2>&1
trap 'rc=$?; printf "exit_code=%s\nlog=%s\n" "$rc" "$LOG" | tee "$LOG/status.txt"' EXIT
echo "LOG=$LOG"
unset JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS CLASSPATH LD_PRELOAD
[[ $("$JAVA_HOME/bin/javac" -version 2>&1) == javac\ 17.* ]] || { echo 'JDK17 required'; exit 1; }
[[ -f "$SCACHE_JNI_DIR/libSparkUrma.so" ]] || { echo 'JNI library missing'; exit 1; }
mapfile -t CP_LINES < "$SCACHE_TEST_CLASSPATH_FILE"
[[ ${#CP_LINES[@]} == 1 && -n "${CP_LINES[0]}" ]] || { echo 'Invalid classpath file'; exit 1; }
IFS=: read -r -a ENTRIES <<< "${CP_LINES[0]}"
for entry in "${ENTRIES[@]}"; do [[ "$entry" == /* && -e "$entry" ]] || { echo "Missing/nonabsolute classpath: $entry"; exit 1; }; done
IFS=: read -r -a LIB_DIRS <<< "$UMDK_LIBRARY_DIRS"
for entry in "${LIB_DIRS[@]}"; do [[ "$entry" == /* && -d "$entry" ]] || { echo "Invalid UMDK directory: $entry"; exit 1; }; done
export LD_LIBRARY_PATH="$UMDK_LIBRARY_DIRS:$SCACHE_JNI_DIR"
"$JAVA_HOME/bin/java" -version
sha256sum "$SCACHE_JNI_DIR/libSparkUrma.so"
ldd "$SCACHE_JNI_DIR/libSparkUrma.so" | tee "$LOG/ldd.log"
if grep -q 'not found' "$LOG/ldd.log"; then echo 'JNI dependency unresolved'; exit 1; fi
for soname in liburma.so.0 liburma_common.so.0; do
  loaded=$(awk -v name="$soname" '$1 == name && $2 == "=>" {print $3}' "$LOG/ldd.log")
  [[ -n "$loaded" && -f "$loaded" ]] || { echo "Required dependency not resolved: $soname"; exit 1; }
  actual=$(readlink -f "$loaded")
  matched=false
  for entry in "${LIB_DIRS[@]}"; do
    if [[ -f "$entry/$soname" && $(readlink -f "$entry/$soname") == "$actual" ]]; then matched=true; fi
  done
  "$matched" || { echo "Unexpected $soname: $actual; outside explicitly selected UMDK"; exit 1; }
  sha256sum "$actual"
done
JAVA=("$JAVA_HOME/bin/java" -Xmx1g "-Djava.library.path=$SCACHE_JNI_DIR" -cp "${CP_LINES[0]}")
# Actually load this branch's JNI and check ABI before creating device objects.
"${JAVA[@]}" org.scache.network.ub.RmCtpJniAbiTest
if [[ "$MODE" == --check ]]; then
  "${JAVA[@]}" org.scache.network.ub.UnsignedUbvaContractTest
  "${JAVA[@]}" org.scache.network.ub.RmCtpEndpointContractTest
  "${JAVA[@]}" org.scache.network.ub.RmCtpLogicalMetricsTest
  "${JAVA[@]}" org.scache.network.ub.NegotiatedCapabilitiesTest
  "${JAVA[@]}" org.scache.network.ub.RmCtpControlProtocolTest
  "${JAVA[@]}" org.scache.network.ub.UbRmCtpLeaseSafetyTest
  "${JAVA[@]}" org.scache.network.ub.RmCtpServiceProtocolTest
  "${JAVA[@]}" org.scache.network.ub.UbSharedArenaIsolationTest
  "${JAVA[@]}" org.scache.deploy.UbPoolBatchSafetyTest
  echo 'SCACHE_RM_CTP_LAUNCHER_CHECK_PASS (no device initialized)'
else
  # Do not silently alter host limits or driver configuration.
  echo "memlock_soft_kib=$(ulimit -Sl) memlock_hard_kib=$(ulimit -Hl)"
  printf 'COMMAND '; printf '%q ' "${JAVA[@]}" org.scache.network.ub.RmCtpServiceNode "$@"; printf '\n'
  "${JAVA[@]}" org.scache.network.ub.RmCtpServiceNode "$@"
fi
