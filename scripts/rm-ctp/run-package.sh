#!/usr/bin/env bash
# Adapter for the ORIGINAL SINGLE-NODE package. No Spark processes are started.
set -Eeuo pipefail
HERE=$(cd -- "$(dirname -- "$0")" && pwd -P)
REPO=$(cd "$HERE/../.." && pwd -P)
PACKAGE=${RM_CTP_PACKAGE:-$HOME/ub-source/ultrashuffle/spark-3.5.7-benchmark-runtime-aarch64}
WORK=$HOME/ub-source/ultrashulfe-work/scache-rm-ctp
PHASE=check DEVICE=auto EID=auto PRIORITY=auto ROLE= PEER= LOCAL_IP= PORT=19171 PEER_PORT=19171
CLASSES= DISCOVERY_ARGS=()
while (($#)); do
  case "$1" in
    --package-home) PACKAGE=${2:?}; shift 2;;
    --workspace) WORK=${2:?}; shift 2;;
    --phase) PHASE=${2:?}; shift 2;;
    --classes) CLASSES=${2:?}; shift 2;;
    --device) DEVICE=${2:?}; shift 2;;
    --eid) EID=${2:?}; shift 2;;
    --priority) PRIORITY=${2:?}; shift 2;;
    --role) ROLE=${2:?}; shift 2;;
    --peer-ip) PEER=${2:?}; shift 2;;
    --local-ip) LOCAL_IP=${2:?}; shift 2;;
    --port) PORT=${2:?}; shift 2;;
    --peer-port) PEER_PORT=${2:?}; shift 2;;
    --provider-lib|--umdk-root|--search-root) DISCOVERY_ARGS+=("$1" "${2:?}"); shift 2;;
    --help|-h)
      echo 'run-package.sh --phase detect|jni|check|node [--package-home DIR] [--workspace DIR]'
      echo 'check/node: --classes EXPORTED_RUNTIME_DIR; node: --role A|B --peer-ip IP'
      echo 'Optional: --device NAME --eid INDEX --priority N --local-ip IP --port N --peer-port N'
      echo 'Original detector options: --provider-lib FILE --umdk-root DIR --search-root DIR'
      exit 0;;
    *) echo "Unknown argument: $1" >&2; exit 2;;
  esac
done
[[ $PHASE =~ ^(detect|jni|check|node|launch)$ ]] || exit 2
PACKAGE=$(cd "$PACKAGE" && pwd -P)
mkdir -p "$WORK"; WORK=$(cd "$WORK" && pwd -P)
STATE=$WORK/state
mkdir -p "$STATE" "$WORK/logs"
RUN=$(mktemp -d "$WORK/logs/package-$PHASE-XXXXXXXX")
exec > >(tee "$RUN/terminal.log") 2>&1
finish() {
  rc=$?; trap - EXIT
  printf 'phase=%s\nexit_code=%s\nlog=%s\n' "$PHASE" "$rc" "$RUN" | tee "$RUN/status.txt"
  if ((rc)); then
    echo "========== FAILURE REPORT phase=$PHASE =========="
    echo "Command log: $RUN/commands.log"
    echo "Full report: $RUN/terminal.log"
    echo 'Earlier detector/compiler/Java messages in this terminal are the original failure evidence.'
  fi
}
trap finish EXIT
exec 8>"$RUN/commands.log"; export BASH_XTRACEFD=8; PS4='+ ${BASH_SOURCE}:${LINENO}: '; set -x
exec 9>"$STATE/package.lock"; flock -n 9 || { echo 'Workspace already in use'; exit 1; }
unset LD_LIBRARY_PATH LD_PRELOAD JAVA_HOME JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS CLASSPATH
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
export RM_CTP_PACKAGE=$PACKAGE
source "$HERE/package-env.sh"
[[ $("$JAVA_HOME/bin/javac" -version 2>&1) == javac\ 17.* ]] || { echo 'Bundled JDK17 required'; exit 1; }
BASE=$PACKAGE/bin/real-ub
[[ -f $BASE/00-detect-vendor-umdk.sh ]] || { echo 'Original single-node detector missing'; exit 1; }
if ((${#DISCOVERY_ARGS[@]}==0)) && [[ -f $STATE/discovery-options.sh ]]; then
  source "$STATE/discovery-options.sh"
fi
if [[ $DEVICE == auto && -f $STATE/endpoint.json ]]; then
  DEVICE=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["device"])' "$STATE/endpoint.json")
fi
# Compilation deliberately uses software-only discovery: no device or priority gate.
DETECT_WORK=$WORK/vendor
if [[ $PHASE != launch ]]; then
args=(--package-home "$PACKAGE" --work-root "$DETECT_WORK" "${DISCOVERY_ARGS[@]}")
[[ $DEVICE == auto ]] || args+=(--device "$DEVICE")
bash "$BASE/00-detect-vendor-umdk.sh" "${args[@]}" --software-only
# The software-only detector may export DEVICE='' because it deliberately
# performs no device selection. Do not let that replace the requested 'auto'
# and accidentally turn an offline check into hardware discovery.
requested_device=$DEVICE
source "$DETECT_WORK/state/detected-umdk.env"
DEVICE=$requested_device
export LD_LIBRARY_PATH="${URMA_LIB%/*}:${URMA_COMMON_LIB%/*}${PROVIDER_DIR:+:$PROVIDER_DIR}"
if [[ $PHASE == detect || $PHASE == node || ( $PHASE == check && $DEVICE != auto ) ]]; then
  "$URMA_ADMIN" show | tee "$RUN/urma-admin-show.log"
  DEVICE=$(python3 "$HERE/select-endpoint.py" --show "$RUN/urma-admin-show.log" --device "${DEVICE:-auto}")
  hardware_discovery_args=("${DISCOVERY_ARGS[@]}")
  explicit_provider=false
  for option in "${DISCOVERY_ARGS[@]}"; do
    [[ $option != --provider-lib ]] || explicit_provider=true
  done
  if [[ $explicit_provider == false ]]; then
    # The original detector sees all sysfs devices. On mixed UDMA/UBAGG hosts
    # that leaves two valid providers even after a bonding device was selected.
    # Match only this device's driver and let the detector validate the result.
    selected_provider=$(python3 "$HERE/select-endpoint.py" --provider-for-device \
      --devices-table "$DETECT_LOG_DIR/devices.tsv" \
      --candidates-table "$DETECT_LOG_DIR/candidates.tsv" --device "$DEVICE")
    hardware_discovery_args+=(--provider-lib "$selected_provider")
    echo "PROVIDER_FOR_DEVICE device=$DEVICE path=$selected_provider"
  fi
  bash "$BASE/00-detect-vendor-umdk.sh" --package-home "$PACKAGE" --work-root "$DETECT_WORK" \
    "${hardware_discovery_args[@]}" --device "$DEVICE"
  source "$DETECT_WORK/state/detected-umdk.env"
  export LD_LIBRARY_PATH="${URMA_LIB%/*}:${URMA_COMMON_LIB%/*}:$PROVIDER_DIR"
  "$URMA_ADMIN" show --all --whole | tee "$RUN/urma-admin-all.log"
  if [[ -f $STATE/endpoint.json ]]; then
    saved_device=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["device"])' "$STATE/endpoint.json")
    if [[ $DEVICE == "$saved_device" ]]; then
      [[ $EID != auto ]] || EID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["eid"])' "$STATE/endpoint.json")
      [[ $PRIORITY != auto ]] || PRIORITY=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["priority"])' "$STATE/endpoint.json")
    fi
  fi
  python3 "$HERE/select-endpoint.py" --show "$RUN/urma-admin-show.log" --all "$RUN/urma-admin-all.log" \
    --device "$DEVICE" --eid "$EID" --priority "$PRIORITY" > "$RUN/endpoint.json"
  cp "$RUN/endpoint.json" "$STATE/endpoint.json"
  EID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["eid"])' "$STATE/endpoint.json")
  PRIORITY=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["priority"])' "$STATE/endpoint.json")
  cat "$STATE/endpoint.json"
fi
declare -p DISCOVERY_ARGS > "$STATE/discovery-options.sh"
[[ $PHASE != detect ]] || { echo 'RM_CTP_PACKAGE_DETECT_PASS (selection only; not connectivity)'; exit 0; }
# Reuse the original private-toolchain builder, but with an isolated source view.
stamp=$({
  find "$REPO/native/spark_urma/src" "$REPO/native/spark_urma/include" -type f -exec sha256sum {} +
  find "$URMA_INCLUDE" -type f -name '*.h' -exec sha256sum {} +
  sha256sum "$REPO/native/spark_urma/CMakeLists.txt" "$PACKAGE/bin/build-lib-spark-urma.sh" \
    "$HERE/run-package.sh" "$URMA_LIB" "$URMA_COMMON_LIB" "$PACKAGE/dependencies/native-toolchain/SHA256SUMS" "$JAVA_HOME/release"
} | sort | sha256sum | awk '{print $1}')
if [[ -f $STATE/jni-stamp && $(< "$STATE/jni-stamp") == "$stamp" && -f $STATE/jni.env ]] &&
    sha256sum -c "$STATE/jni-output.sha256" >/dev/null 2>&1; then
  source "$STATE/jni.env"; echo 'RM_CTP_VENDOR_JNI_REUSED'
else
  VIEW=$(mktemp -d "$WORK/build-view-XXXXXXXX")
  mkdir -p "$VIEW/bin" "$VIEW/sources/scache-jni"
  ln -s "$PACKAGE/dependencies" "$VIEW/dependencies"
  cp "$PACKAGE/bin/build-lib-spark-urma.sh" "$VIEW/bin/"
  ln -s "$REPO/native/spark_urma/src" "$VIEW/sources/scache-jni/src"
  ln -s "$REPO/native/spark_urma/include" "$VIEW/sources/scache-jni/include"
  # Original builder passes OU_UMDK_INCLUDE; adapt its CMake entry without
  # editing either the original package or the experimental native source.
  printf 'cmake_minimum_required(VERSION 3.16)\nproject(rm_ctp_package LANGUAGES C CXX)\nset(URMA_INC [==[%s]==] CACHE PATH "Selected public SDK" FORCE)\ninclude([==[%s]==])\n' \
    "$URMA_INCLUDE" "$REPO/native/spark_urma/CMakeLists.txt" > "$VIEW/sources/scache-jni/CMakeLists.txt"
  bash "$VIEW/bin/build-lib-spark-urma.sh" --workspace "$VIEW/output" \
    --urma-lib "$URMA_LIB" --urma-common-lib "$URMA_COMMON_LIB" \
    --urma-include "$URMA_INCLUDE" --umdk-label "$UMDK_LABEL"
  mapfile -t statuses < <(find "$VIEW/output" -name status.txt -type f)
  [[ ${#statuses[@]} == 1 ]] || { echo 'Ambiguous build status'; exit 1; }
  grep -qx 'result=PASS' "${statuses[0]}"
  artifact=$(sed -n 's/^artifact=//p' "${statuses[0]}")
  [[ -f $artifact ]] || exit 1
  SCACHE_JNI_DIR=${artifact%/*}
  printf 'SCACHE_JNI_DIR=%q\n' "$SCACHE_JNI_DIR" > "$STATE/jni.env"
  sha256sum "$SCACHE_JNI_DIR"/*.so* > "$STATE/jni-output.sha256"
  printf '%s\n' "$stamp" > "$STATE/jni-stamp"
fi
[[ $PHASE != jni ]] || { echo 'RM_CTP_PACKAGE_JNI_PASS'; exit 0; }
else
  # Internal fast start after BOTH hosts completed detection/build/check.
  # No slow filesystem search/recompile inside the peer's 60-second accept window.
  for required in "$STATE/endpoint.json" "$STATE/jni.env" "$STATE/jni-output.sha256" "$DETECT_WORK/state/detected-umdk.env"; do
    [[ -f $required ]] || { echo "PREPARED_STATE_MISSING: $required; run run-all.sh"; exit 1; }
  done
  source "$DETECT_WORK/state/detected-umdk.env"
  source "$STATE/jni.env"
  sha256sum -c "$STATE/jni-output.sha256"
  DEVICE=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["device"])' "$STATE/endpoint.json")
  EID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["eid"])' "$STATE/endpoint.json")
  PRIORITY=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["priority"])' "$STATE/endpoint.json")
fi
if [[ -z $CLASSES && -f $STATE/classes.path ]]; then CLASSES=$(< "$STATE/classes.path"); fi
[[ -n $CLASSES && -f $CLASSES/classpath.relative ]] || { echo 'New RM/CTP runtime missing: use prepare-tests.sh export; old package SCache JAR is not a substitute'; exit 1; }
CLASSES=$(cd "$CLASSES" && pwd -P)
(cd "$CLASSES"; sha256sum -c SHA256SUMS) > "$RUN/runtime-integrity.log"
source_id=$(cd "$REPO"; { find src/main src/integration -type f -exec sha256sum {} +; sha256sum build.sbt; } | sort | sha256sum | awk '{print $1}')
[[ $(< "$CLASSES/source-id.txt") == "$source_id" ]] || { echo 'RM_CTP_RUNTIME_SOURCE_MISMATCH: export classes built from this source version'; exit 1; }
printf '%s\n' "$CLASSES" > "$STATE/classes.path"
cp_entries=()
while IFS= read -r entry; do
  [[ $entry =~ ^entry-[0-9]+$ && -e $CLASSES/$entry ]] || { echo "Invalid runtime entry: $entry"; exit 1; }
  cp_entries+=("$CLASSES/$entry")
done < "$CLASSES/classpath.relative"
[[ ${#cp_entries[@]} -gt 0 ]] || exit 1
(IFS=:; printf '%s\n' "${cp_entries[*]}") > "$RUN/classpath.txt"
export SCACHE_TEST_CLASSPATH_FILE=$RUN/classpath.txt SCACHE_JNI_DIR
export UMDK_LIBRARY_DIRS="${URMA_LIB%/*}:${URMA_COMMON_LIB%/*}${PROVIDER_DIR:+:$PROVIDER_DIR}"
if [[ $PHASE == check ]]; then
  bash "$HERE/run-node.sh" --check "$RUN/check"
else
  [[ $ROLE == A || $ROLE == B ]] && [[ -n $PEER ]] || { echo 'node requires --role A|B --peer-ip IP'; exit 1; }
  [[ -n $LOCAL_IP ]] || LOCAL_IP=$(ip -j route get "$PEER" | python3 -c 'import json,sys; r=json.load(sys.stdin)[0]; print(r.get("prefsrc", r.get("src", "")))')
  [[ -n $LOCAL_IP ]] || { echo 'Cannot determine source IP; use --local-ip'; exit 1; }
  for port in "$PORT" "$PEER_PORT"; do [[ $port =~ ^[0-9]+$ ]] && ((port>1024 && port<65535)) || exit 1; done
  ports=("$PORT"); [[ $ROLE != A ]] || ports+=("$((PORT+1))")
  for port in "${ports[@]}"; do [[ -z $(ss -H -ltn "sport = :$port") ]] || { echo "Port occupied: $port"; exit 1; }; done
  ulimit -l unlimited 2>/dev/null || true
  limit=$(ulimit -Sl)
  echo "memlock_soft_kib=$limit hard=$(ulimit -Hl) required_minimum_kib=68608"
  [[ $limit == unlimited ]] || ((limit>=68608)) || { echo 'MEMLOCK_TOO_LOW: raise shell hard/soft limit; no driver changes performed'; exit 1; }
  bash "$HERE/run-node.sh" "$ROLE" "$RUN/node" "$DEVICE" "$EID" "$PRIORITY" "$LOCAL_IP" "$PORT" "$PEER" "$PEER_PORT"
fi
