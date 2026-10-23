#!/usr/bin/env bash
set -Eeuo pipefail
HERE=$(cd -- "$(dirname -- "$0")" && pwd -P)
source "$HERE/lib/path-policy.sh"
PHASE=${1:?phase required}; shift
CONFIG="$HERE/config.env"
while (($#)); do
  case "$1" in
    --config) CONFIG=${2:?}; shift 2 ;;
    -h|--help) echo 'Run one of 05/10/20/30/40/50/60/65/66/70 scripts on A with --config /absolute/config.env'; exit 0 ;;
    *) echo "unknown argument $1" >&2; exit 2 ;;
  esac
done
[[ -r $CONFIG ]] || { echo "Copy config.env.example to config.env and fill in both hosts: $CONFIG" >&2; exit 2; }
source "$CONFIG"
fail() { FAIL_REASON=$*; echo "ERROR: $*" >&2; exit 1; }
for v in A_DEVICE B_DEVICE A_PROVIDER B_PROVIDER A_EID B_EID A_PRIORITY B_PRIORITY; do
  printf -v "$v" '%s' "${!v:-auto}"
done
A_SDK_ROOT=${A_SDK_ROOT:-}; B_SDK_ROOT=${B_SDK_ROOT:-}
UB_CHUNK_BYTES=${UB_CHUNK_BYTES:-10485760}
declare -p A_SEARCH_ROOTS >/dev/null 2>&1 || A_SEARCH_ROOTS=()
declare -p B_SEARCH_ROOTS >/dev/null 2>&1 || B_SEARCH_ROOTS=()
declare -p SSH_OPTIONS >/dev/null 2>&1 || SSH_OPTIONS=(-o BatchMode=yes -o ConnectTimeout=10)
[[ $(declare -p SSH_OPTIONS) == declare\ -a* ]] || fail 'SSH_OPTIONS must be an array'
[[ $(declare -p NATIVE_EXTRA_ARGS 2>/dev/null || true) == declare\ -a* ]] || fail 'NATIVE_EXTRA_ARGS must be an array (use NATIVE_EXTRA_ARGS=())'
[[ $(declare -p NATIVE_SIZES 2>/dev/null || true) == declare\ -a* ]] || fail 'NATIVE_SIZES must be an array (use NATIVE_SIZES=(4096 application))'
case "$PHASE" in 05|10|20|30|40|50|60|65|66|70) ;; *) fail 'phase must be 05/10/20/30/40/50/60/65/66/70' ;; esac
for v in B_SSH A_IP B_IP A_PACKAGE B_PACKAGE A_WORK B_WORK A_DEVICE B_DEVICE A_PROVIDER B_PROVIDER MODE; do
  [[ -n ${!v:-} ]] || fail "missing config $v"
done
[[ $A_IP != "$B_IP" && $A_IP != 127.* && $B_IP != 127.* ]] || fail 'two distinct non-loopback management IPv4 addresses required'
[[ $A_IP != 192.0.2.* && $B_IP != 192.0.2.* ]] || fail 'replace example IP addresses in config.env'
for v in A_PACKAGE B_PACKAGE A_WORK B_WORK; do [[ ${!v} == /* ]] || fail "$v must be absolute"; done
for v in A_WORK B_WORK; do
  case "${!v%/}" in
    /|/home|/root|/tmp|/var/tmp|/opt|/mnt) fail "$v must be a dedicated subdirectory, not a broad system directory" ;;
  esac
done
[[ -z ${HOME:-} || ( ${A_WORK%/} != "${HOME%/}" && ${B_WORK%/} != "${HOME%/}" ) ]] || fail 'workspace must not be the user home directory'
if [[ $PHASE == 05 || $PHASE == 60 || $PHASE == 66 || $PHASE == 70 ]]; then
  [[ ${SHARED_ROOT:-} == /* && ${SHARED_ROOT:-} != / ]] || fail 'SHARED_ROOT must be a non-root absolute path for benchmark phases'
fi
for v in A_PACKAGE B_PACKAGE A_WORK B_WORK A_SDK_ROOT B_SDK_ROOT A_PROVIDER B_PROVIDER SHARED_ROOT; do
  [[ ${!v:-} != *[[:space:]]* ]] || fail "$v contains whitespace; the bundled native build and Spark scripts require paths without whitespace"
done
for root in "${A_SEARCH_ROOTS[@]}" "${B_SEARCH_ROOTS[@]}"; do
  [[ $root != *[[:space:]]* ]] || fail "SEARCH_ROOT contains whitespace: $root"
done
for v in A_PROVIDER B_PROVIDER; do [[ ${!v} == auto || ${!v} == /* ]] || fail "$v must be auto or absolute"; done
[[ $MODE == rm-ctp || $MODE == rc-rtp ]] || fail 'MODE must be rm-ctp or rc-rtp'
for flag in "${NATIVE_EXTRA_ARGS[@]}"; do
  case "$flag" in
    -p*|--trans_mode*|--ctp*|-j*|--share_jfr*|-S*|--server*|-d*|--dev*|--eid_idx*|-P*|--port*|-O*|--priority*|-s*|--size*|-n*|--iters*|--bind_ip*|--single_path*)
      fail "NATIVE_EXTRA_ARGS must not override mode/endpoint/test identity: $flag" ;;
  esac
  # The native prerequisite validates this JNI's ordinary Jetty import path.
  # A perftest-only API/context override does not validate the SCache path.
  # Match GNU long-option abbreviations too, as getopt_long accepts them.
  for profile_option in simplex_mode use_flat_api tp_aware tp_reuse enable_user_tp \
      async_import order_type sub_trans_mode token_policy aggr_mode bond_mode bond_level; do
    if [[ $flag == --* && --$profile_option == "${flag%%=*}"* ]]; then
      fail "NATIVE_EXTRA_ARGS changes the SCache link profile: $flag; current JNI uses duplex Jetty, ordinary synchronous import, default ordering, TOKEN_NONE and the recorded SDK-specific bonding context policy. Use the independent vendor diagnostic for alternate paths; a perftest-only option is not applied to SCache."
    fi
  done
  case "$flag" in
    -b*|-f*|-K*)
      fail "NATIVE_EXTRA_ARGS changes the SCache link profile: $flag; simplex/flat API/token-policy overrides are not implemented by the current JNI" ;;
  esac
done
[[ $B_SSH != -* && $B_SSH != *[[:space:]]* ]] || fail 'invalid SSH destination'
for v in A_EID B_EID A_PRIORITY B_PRIORITY; do [[ ${!v} == auto || ${!v} =~ ^[0-9]+$ ]] || fail "$v must be auto or integer"; done
for v in A_PRIORITY B_PRIORITY; do [[ ${!v} == auto ]] || (( ${!v}<=15 )) || fail "$v outside 0..15"; done
for v in NATIVE_PORT NATIVE_TIMEOUT ARENA_MIB BENCH_ARENA_MIB UB_CHUNK_BYTES APP_TIMEOUT; do
  [[ ${!v:-} =~ ^[0-9]+$ ]] || fail "$v must be an integer"
done
((NATIVE_PORT>1024 && NATIVE_PORT<65530 && NATIVE_TIMEOUT>0 && APP_TIMEOUT>0)) || fail 'invalid port/timeout'
for v in ARENA_MIB BENCH_ARENA_MIB; do (( ${!v}>=128 && ${!v}<=32768 && ${!v}%64==0 )) || fail 'arena must be 128..32768 MiB, multiple of 64'; done
((UB_CHUNK_BYTES>=4096 && UB_CHUNK_BYTES<=10485760)) || fail 'UB_CHUNK_BYTES must be 4096..10485760'
((${#NATIVE_SIZES[@]}>0)) || fail 'NATIVE_SIZES empty'
for n in "${NATIVE_SIZES[@]}"; do
  [[ $n == application ]] && continue
  [[ $n =~ ^[0-9]+$ ]] && ((n>0 && n<=268435456)) || fail 'invalid native size'
done
((NATIVE_PORT+4*${#NATIVE_SIZES[@]}<=65535)) || fail 'native port range exceeds 65535'
[[ $PHASE != 70 ]] || ARENA_MIB=$BENCH_ARENA_MIB
A_PATH_INFO=$(validate_workspace_paths A "$A_PACKAGE" "$A_WORK") || exit 1
mkdir -p "$A_WORK/logs" "$A_WORK/state"
LOG=$(mktemp -d "$A_WORK/logs/phase-$PHASE-$(date +%Y%m%d-%H%M%S)-XXXXXX")
SESSION=${LOG##*/}
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')
SHARED_RUN="${SHARED_ROOT:-}/$SESSION"
DEPLOYED_A=0 DEPLOYED_B=0 UB_APPLICATION_STARTED=0
exec > >(tee "$LOG/terminal.log") 2>&1
printf 'PHASE=%s\nSESSION=%s\nLOG=%s\nMODE=%s\n' "$PHASE" "$SESSION" "$LOG" "$MODE"
echo "$A_PATH_INFO"
[[ $PHASE != 60 ]] || echo 'DATA_PATH=standard-spark-shuffle SCACHE_SERVICE_CHECK=netty UB_TESTED=false'
[[ $PHASE != 65 ]] || echo 'DATA_PATH=scache-netty-integrated UB_TESTED=false'
[[ $PHASE != 66 ]] || echo 'DATA_PATH=scache-netty-benchmarks UB_TESTED=false'
remote_command() { local command; printf -v command '%q ' "$@"; ssh "${SSH_OPTIONS[@]}" "$B_SSH" "$command"; }
node() {
  local which=$1; shift
  if [[ $which == A ]]; then
    env -u LD_PRELOAD -u LD_LIBRARY_PATH bash "$A_WORK/runs/$SESSION/tool/node.sh" "$@"
  else
    remote_command env -u LD_PRELOAD -u LD_LIBRARY_PATH bash "$B_WORK/runs/$SESSION/tool/node.sh" "$@"
  fi
}
step() {
  local host=$1 action=$2 rc; shift 2
  LAST_STEP="node=$host action=$action"
  echo "STEP_START node=$host action=$action"
  { printf 'ACTION node=%s action=%s args=' "$host" "$action"; printf '%q ' "$@"; printf '\n'; } | tee -a "$LOG/$host-$action.log"
  if node "$host" "$action" "$@" 2>&1 | tee -a "$LOG/$host-$action.log"; then
    echo "STEP_PASS node=$host action=$action"
  else
    rc=$?
    echo "STEP_FAIL node=$host action=$action exit_code=$rc"
    return "$rc"
  fi
}
finish() {
  local rc=$? host deployed result file ub_tested=false dma_unresolved=false stop_failed=0
  trap - EXIT INT TERM ERR
  set +e
  if [[ $rc != 0 && ( $PHASE == 40 || $PHASE == 50 || $PHASE == 70 ) ]]; then
    if [[ $UB_APPLICATION_STARTED == 1 ]]; then
      dma_unresolved=true
      echo 'UB_FAILURE_UNKNOWN_DMA_STATE: SCache was started; failure cannot prove all provider-owned requests completed' \
        | tee "$LOG/dma-state-unknown.txt"
    fi
    if grep -Eq 'ERR_DMA_UNRESOLVED|UB_CLOSE_QUARANTINED' "$LOG/terminal.log"; then
      dma_unresolved=true
    fi
    for host in A B; do
      deployed=DEPLOYED_$host
      [[ ${!deployed} == 1 ]] || continue
      if node "$host" dma-unresolved > "$LOG/$host-dma-quarantine.log" 2>&1; then
        dma_unresolved=true
      fi
    done
  fi
  if [[ $dma_unresolved == true ]]; then
    echo 'DMA_UNRESOLVED_QUARANTINE: retaining both endpoint processes, MRs and leases; do not force-stop until provider ownership is resolved' | tee "$LOG/dma-quarantine.txt"
  fi
  if [[ $dma_unresolved != true ]]; then
    # A remote worker can still hold a block from the other SCache. Stop
    # application groups on BOTH machines before stopping EITHER SCache.
    for host in A B; do
      deployed=DEPLOYED_$host
      [[ ${!deployed} == 1 ]] || continue
      echo "CLEANUP_START node=$host action=stop-app"
      if ! node "$host" stop-app > "$LOG/$host-stop-app.log" 2>&1; then
        stop_failed=1; rc=1
      fi
    done
    if [[ $stop_failed == 0 ]]; then
      for host in A B; do
        deployed=DEPLOYED_$host
        [[ ${!deployed} == 1 ]] || continue
        echo "CLEANUP_START node=$host action=app-quiescent"
        if ! node "$host" app-quiescent > "$LOG/$host-app-quiescent.log" 2>&1; then
          stop_failed=1; rc=1
        fi
      done
    fi
    if [[ $stop_failed == 0 ]]; then
      for host in A B; do
        deployed=DEPLOYED_$host
        [[ ${!deployed} == 1 ]] || continue
        echo "CLEANUP_START node=$host action=stop-scache"
        if ! node "$host" stop-scache > "$LOG/$host-stop-scache.log" 2>&1; then
          stop_failed=1; rc=1
        fi
      done
    else
      echo 'SCACHE_RETAINED: at least one endpoint application group is not confirmed stopped'
    fi
    if [[ $stop_failed == 0 ]]; then
      for host in A B; do
        deployed=DEPLOYED_$host
        [[ ${!deployed} == 1 ]] || continue
        node "$host" release > "$LOG/$host-release.log" 2>&1 || rc=1
      done
    fi
  fi
  for host in A B; do
    deployed=DEPLOYED_$host
    [[ ${!deployed} == 1 ]] || continue
    node "$host" evidence > "$LOG/$host-evidence.log" 2>&1 || true
    if node "$host" archive > "$LOG/$host-logs.tar.gz" 2> "$LOG/$host-collect.log"; then
      mkdir -p "$LOG/$host-evidence"
      tar -xzf "$LOG/$host-logs.tar.gz" -C "$LOG/$host-evidence" || rc=1
    else
      echo "LOG_COLLECTION_FAILED node=$host remote workspace retained"; rc=1
    fi
  done
  [[ $rc == 0 ]] && result=PASS || result=FAIL
  if [[ $rc == 0 && ( $PHASE == 30 || $PHASE == 40 || $PHASE == 50 || $PHASE == 70 ) ]]; then
    ub_tested=true
  fi
  printf 'result=%s\nphase=%s\nmode=%s\nexit_code=%s\nub_link_tested=%s\nlogs=%s\n' \
    "$result" "$PHASE" "$MODE" "$rc" "$ub_tested" "$LOG" > "$LOG/status.txt"
  printf 'dma_unresolved=%s\n' "$dma_unresolved" >> "$LOG/status.txt"
  [[ $PHASE != 60 ]] || printf 'spark_backend=standard\nscache_probe_backend=netty\nub_tested=false\n' >> "$LOG/status.txt"
  [[ $PHASE != 65 ]] || printf 'spark_backend=scache-netty\nub_tested=false\n' >> "$LOG/status.txt"
  [[ $PHASE != 66 ]] || printf 'spark_backend=scache-netty\nub_tested=false\n' >> "$LOG/status.txt"
  {
    cat "$LOG/status.txt"
    [[ $dma_unresolved != true ]] || cat "$LOG/dma-quarantine.txt"
    for host in A B; do
      file="$LOG/$host-sdk-version.txt"
      [[ ! -f $file ]] || printf 'vendor_umdk_%s=%s\n' "$host" "$(< "$file")"
    done
    [[ ! -f $LOG/peer-endpoints.log ]] || cat "$LOG/peer-endpoints.log"
    if [[ $ub_tested == true ]]; then
      echo 'UB_STAGE_SUMMARY (native perftest and any subsequent application gates passed)'
      grep -E '^(NATIVE_PAIR_PASS|SCACHE_TWO_NODE_PASS|SPARK_UB_TWO_NODE_PASS|BENCHMARK_TWO_NODE_PASS|TPCDS_REFERENCE_MATCH_103)' \
        "$LOG/terminal.log" || true
    fi
    if [[ $PHASE == 60 ]]; then
      echo 'STANDARD_TWO_NODE_SUMMARY (SCache Netty check is separate from standard Spark Shuffle)'
      for host in A B; do
        file="$LOG/$host-evidence/runs/$SESSION/topology.json"
        [[ ! -f $file ]] || { echo "=== topology $host ==="; cat "$file"; echo; }
        file="$LOG/$host-ctl.log"
        [[ ! -f $file ]] || { echo "=== SCache $host ==="; cat "$file"; }
      done
      for file in "$LOG/A-events.log" "$LOG/A-bench-check.log"; do
        [[ ! -f $file ]] || cat "$file"
      done
      file="$LOG/A-evidence/runs/$SESSION/standard-tpcds.log"
      [[ ! -f $file ]] || grep '^TPCDS_PASS ' "$file" || true
    fi
    [[ $rc == 0 ]] || {
      echo 'FAILURE REPORT (full logs retained alongside this report)'
      printf 'reason=%s\nlast_step=%s\n' "${FAIL_REASON:-command failed; inspect the logs below}" "${LAST_STEP:-not started}"
      [[ ! -f $LOG/failure.txt ]] || cat "$LOG/failure.txt"
      while IFS= read -r file; do printf '\n=== %s ===\n' "$file"; tail -n 60 "$file"; done < <(
        find "$LOG" -type f \( -name '*.log' -o -name '*.exit' -o -name '*.command' -o -name '*selection.json' \) ! -name terminal.log ! -path '*/worker/*' | sort
      )
    }
  } > "$LOG/REPORT.txt"
  cat "$LOG/REPORT.txt"
  echo "TWO_NODE_STAGE_${result} phase=$PHASE report=$LOG/REPORT.txt"
  exit "$rc"
}
trap finish EXIT
trap 'printf "line=%s\ncommand=%s\n" "$LINENO" "$BASH_COMMAND" > "$LOG/failure.txt"' ERR
trap 'exit 130' INT
trap 'exit 143' TERM
for command in ssh tar flock tee sha256sum readlink; do command -v "$command" >/dev/null || fail "controller command missing: $command"; done
exec 9>"$A_WORK/state/orchestrator.lock"
flock -n 9 || fail 'another dual-node run is using this workspace'
deploy() {
  local host=$1 v package work dev provider eid priority ip peer staging
  v=${host}_PACKAGE; package=${!v}; v=${host}_WORK; work=${!v}
  v=${host}_DEVICE; dev=${!v}; v=${host}_PROVIDER; provider=${!v}
  v=${host}_EID; eid=${!v}; v=${host}_PRIORITY; priority=${!v}; v=${host}_IP; ip=${!v}
  [[ $host == A ]] && peer=$B_IP || peer=$A_IP
  staging="$LOG/tool-$host"; mkdir -p "$staging"
  for v in "$HERE/lib/"*; do [[ ! -f $v ]] || cp "$v" "$staging/"; done
  {
    for v in SESSION START_TIME MODE ARENA_MIB UB_CHUNK_BYTES NATIVE_TIMEOUT APP_TIMEOUT SHARED_RUN PHASE; do printf '%s=%q\n' "$v" "${!v}"; done
    printf 'NODE=%q\nPACKAGE=%q\nWORK=%q\nDEVICE=%q\nPROVIDER=%q\nEID=%q\nPRIORITY=%q\nNODE_IP=%q\nMASTER_IP=%q\nPEER_IP=%q\n' \
      "$host" "$package" "$work" "$dev" "$provider" "$eid" "$priority" "$ip" "$A_IP" "$peer"
    declare -p NATIVE_EXTRA_ARGS
    v=${host}_SDK_ROOT; printf 'SDK_ROOT=%q\n' "${!v}"
    v=${host}_SEARCH_ROOTS
    declare -p "$v" | sed "s/ $v=/ SEARCH_ROOTS=/"
  } > "$staging/node.env"
  if [[ $host == A ]]; then
    mkdir -p "$work/runs/$SESSION/tool"; cp "$staging/"* "$work/runs/$SESSION/tool/"; DEPLOYED_A=1
  else
    remote_command mkdir -p "$work/runs/$SESSION/tool"
    tar -cf - -C "$staging" . | remote_command tar -xf - -C "$work/runs/$SESSION/tool"
    DEPLOYED_B=1
  fi
}
# Resolve B's paths on B before creating its workspace or deploying any files.
path_check="$(declare -f validate_workspace_paths)"$'\n''validate_workspace_paths "$@"'
remote_command bash -c "$path_check" bash B "$B_PACKAGE" "$B_WORK" | tee "$LOG/B-path-policy.log"
deploy A; deploy B
step A lease; step B lease
step A probe; step B probe
[[ $(node A boot-id) != "$(node B boot-id)" ]] || fail 'A and B resolve to the same running kernel; this is not a two-host test'
node A runtime-hashes > "$LOG/A-runtime-hashes.txt"
node B runtime-hashes > "$LOG/B-runtime-hashes.txt"
cmp "$LOG/A-runtime-hashes.txt" "$LOG/B-runtime-hashes.txt" || fail 'A/B application artifacts differ'
detect_both() {
  local failed=0
  step A detect "$@" || failed=1
  step B detect "$@" || failed=1
  ((failed==0)) || fail 'environment detection failed; both node reports collected'
  node A sdk-version > "$LOG/A-sdk-version.txt"
  node B sdk-version > "$LOG/B-sdk-version.txt"
  cmp "$LOG/A-sdk-version.txt" "$LOG/B-sdk-version.txt" ||
    fail 'A/B vendor UMDK core/devel versions differ; align the supported vendor release before testing'
  if [[ ${1:-} != --software-only ]]; then
    node A selection-json > "$LOG/A-selection.json"
    node B selection-json > "$LOG/B-selection.json"
    python3 - "$LOG/A-selection.json" "$LOG/B-selection.json" <<'PY' | tee "$LOG/peer-endpoints.log"
import json
import sys

a, b = [json.load(open(path))['endpoint'] for path in sys.argv[1:]]
for label, endpoint in (('A', a), ('B', b)):
    print('PEER_ENDPOINT node={} device={} eid={} priority={} mode={}'.format(
        label, endpoint['device'], endpoint['eid_address'], endpoint['priority'], endpoint['mode']))
if a['eid_address'] == b['eid_address']:
    print('PEER_EID_DUPLICATE_WARNING: both nodes selected the same EID; confirm the vendor supernode topology before native READ/WRITE')
if a['priority'] != b['priority']:
    print('PEER_PRIORITY_DIFFERENCE_WARNING: verify the two selected CTP priorities with the vendor before native READ/WRITE')
PY
  fi
}
build_both() { detect_both --software-only; step A jni; step B jni; }
native_both() {
  local direction server client peer op size requested prior seen port=$NATIVE_PORT tag app_a app_b app_limit payload_from payload_to
  local -a native_resolved=()
  detect_both
  if [[ " ${NATIVE_SIZES[*]} " == *' application '* ]]; then
    app_a=$(node A max-request); app_b=$(node B max-request)
    [[ $app_a =~ ^[0-9]+$ && $app_b =~ ^[0-9]+$ ]] || fail 'invalid application request limit from device probe'
    app_limit=$((app_a < app_b ? app_a : app_b))
    ((app_limit>=4096)) || fail "application request limit below 4 KiB: A=$app_a B=$app_b"
    echo "NATIVE_APPLICATION_SIZE size=$app_limit endpoint_A=$app_a endpoint_B=$app_b"
  fi
  for requested in "${NATIVE_SIZES[@]}"; do
    size=$requested; [[ $size != application ]] || size=$app_limit
    seen=false
    for prior in "${native_resolved[@]}"; do [[ $prior != "$size" ]] || seen=true; done
    [[ $seen == true ]] || native_resolved+=("$size")
  done
  printf 'NATIVE_PORT_RANGE start=%s end=%s sizes=%s directions=AB,BA operations=READ,WRITE\n' \
    "$NATIVE_PORT" "$((NATIVE_PORT + 4 * ${#native_resolved[@]} - 1))" "${native_resolved[*]}"
  for direction in AB BA; do
    if [[ $direction == AB ]]; then server=A; client=B; peer=$A_IP; else server=B; client=A; peer=$B_IP; fi
    for op in read_bw write_bw; do for size in "${native_resolved[@]}"; do
      if [[ $op == read_bw ]]; then payload_from=$server; payload_to=$client
      else payload_from=$client; payload_to=$server; fi
      tag="native-$direction-$op-$size"
      printf 'NATIVE_PAIR_START direction=%s operation=%s server=%s client=%s initiator=%s target=%s payload_from=%s payload_to=%s size=%s\n' \
        "$direction" "$op" "$server" "$client" "$client" "$server" "$payload_from" "$payload_to" "$size"
      step "$server" native-server "$op" "$size" "$port" "$tag-server"
      step "$client" native-client "$op" "$size" "$port" "$peer" "$tag-client"
      step "$server" native-wait "$tag-server"
      printf 'NATIVE_PAIR_PASS direction=%s operation=%s size=%s mode=%s server=%s client=%s initiator=%s target=%s payload_from=%s payload_to=%s\n' \
        "$direction" "$op" "$size" "$MODE" "$server" "$client" "$client" "$server" "$payload_from" "$payload_to"
      port=$((port+1))
    done; done
  done
}
source_gate() {
  # This independent package contains the RM/CTP SCache and Spark build. The
  # device capability and native READ/WRITE gates still run before SCache.
  [[ $MODE == rm-ctp || $MODE == rc-rtp ]] || fail "unsupported product mode: $MODE"
}
metric() {
  awk -v key="urma.$2" '{for(i=1;i<=NF;i++){split($i,a,"=");if(a[1]==key){value=a[2];found++}}}
    END{if(found!=1 || value !~ /^[0-9]+$/){print "INVALID_OR_MISSING_METRIC "key > "/dev/stderr";exit 1} print value}' "$1"
}
snapshot() {
  local h key file
  for h in A B; do
    file="$LOG/metrics-$h-$1.txt"
    node "$h" ctl METRICS > "$file"
    for key in submittedBytes completedBytes sharedPoolDestinationBytes submittedChunks providerTerminalChunks apiTerminalChunks tcpPayloadBytes nettyPayloadBytes fallbacks failedChunks checksumErrors generationErrors sharedPoolDestinationIdentityMismatches transportFailedOperations transportTimedOutOperations transportInflightOperations activeLeases activeImports activeMappings activeSlots activeTokens arenaPublishedBlocks arenaFreeBytes; do
      metric "$file" "$key" >/dev/null || fail "invalid/missing counter node=$h snapshot=$1 key=$key file=$file"
    done
  done
}
metrics_check() {
  local tag=$1 h f key submitted completed
  for h in A B; do
    f="$LOG/metrics-$h-$tag.txt"; submitted=$(metric "$f" submittedBytes); completed=$(metric "$f" completedBytes)
    ((submitted>0 && submitted==completed)) || fail "byte accounting failed $h $tag"
    [[ $(metric "$f" submittedChunks) == "$(metric "$f" providerTerminalChunks)" && $(metric "$f" submittedChunks) == "$(metric "$f" apiTerminalChunks)" ]] || fail 'CQ accounting failed'
    for key in tcpPayloadBytes nettyPayloadBytes fallbacks failedChunks checksumErrors generationErrors sharedPoolDestinationIdentityMismatches transportFailedOperations transportTimedOutOperations transportInflightOperations; do
      [[ $(metric "$f" "$key") == 0 ]] || fail "$h $tag nonzero $key"
    done
  done
}
quiet_check() {
  local tag=$1 attempt h key quiet
  for ((attempt=0;attempt<60;attempt++)); do
    snapshot "$tag"; quiet=true
    for h in A B; do
      for key in activeLeases activeImports activeMappings activeSlots activeTokens transportInflightOperations arenaPublishedBlocks; do
        [[ $(metric "$LOG/metrics-$h-$tag.txt" "$key") == 0 ]] || quiet=false
      done
      [[ $(metric "$LOG/metrics-$h-base.txt" arenaFreeBytes) == "$(metric "$LOG/metrics-$h-$tag.txt" arenaFreeBytes)" ]] || quiet=false
    done
    [[ $quiet != true ]] || return 0
    sleep 1
  done
  fail "resource recovery failed $tag"
}
delta_check() {
  local before=$1 after=$2 require_each=${3:-false} h x y bytes=0 pool=0 node_bytes node_pool
  for h in A B; do
    x=$(metric "$LOG/metrics-$h-$before.txt" submittedBytes); y=$(metric "$LOG/metrics-$h-$after.txt" submittedBytes)
    ((y>=x)) || fail 'URMA counters reset'; node_bytes=$((y-x)); bytes=$((bytes+node_bytes))
    x=$(metric "$LOG/metrics-$h-$before.txt" sharedPoolDestinationBytes); y=$(metric "$LOG/metrics-$h-$after.txt" sharedPoolDestinationBytes)
    ((y>=x)) || fail 'pool counters reset'; node_pool=$((y-x)); pool=$((pool+node_pool))
    if [[ $require_each == true ]]; then
      ((node_bytes>0 && node_pool>0)) || fail "bidirectional transfer did not exercise URMA/destination pool on node $h"
    fi
  done
  ((bytes>0 && pool>0)) || fail 'application did not exercise URMA/U1 destination pool'
  echo "URMA_DELTA bytes=$bytes destination_pool=$pool"
}
scache_pair() {
  source_gate; build_both; native_both
  step A application-check; step B application-check
  step A scache-master
  UB_APPLICATION_STARTED=1
  step A scache-node; step B scache-node
  snapshot base
  # Keep one reduce partition unpublished for each map. Otherwise map-complete
  # prefetch may populate the consumer before the explicit remote FETCH probe.
  step A ctl 'REGISTER dualsmoke 0 0 2 2'
  step A ctl 'PUT scache_dualsmoke_0_0_0_0 1048576 42'
  step B ctl 'FETCH scache_dualsmoke_0_0_0_0 1048576 42'
  step B ctl 'PUT scache_dualsmoke_0_0_1_0 1048576 43'
  step A ctl 'FETCH scache_dualsmoke_0_0_1_0 1048576 43'
  snapshot transfer; metrics_check transfer; delta_check base transfer true
  for h in A B; do
    step "$h" ctl 'REMOVE scache_dualsmoke_0_0_0_0'; step "$h" ctl 'REMOVE scache_dualsmoke_0_0_1_0'
  done
  quiet_check clean-transfer
  echo SCACHE_TWO_NODE_PASS
}
spark_cluster() { step A spark-master; step A spark-worker "$1"; step B spark-worker "$1"; }
standard_scache_pair() {
  step A scache-master
  step A scache-standard-node; step B scache-standard-node
  step A ctl INFO; step B ctl INFO
  # Publish only partition 0 of each two-partition map. A completed Netty map
  # automatically prefetches to peers, racing FETCH's required local-miss check.
  # This probe tests explicit remote transfer, not map-completion prefetch.
  step A ctl 'REGISTER standarddual 0 0 2 2'
  step A ctl 'PUT scache_standarddual_0_0_0_0 1048576 42'
  step B ctl 'FETCH scache_standarddual_0_0_0_0 1048576 42'
  step B ctl 'PUT scache_standarddual_0_0_1_0 1048576 43'
  step A ctl 'FETCH scache_standarddual_0_0_1_0 1048576 43'
  step A ctl 'REMOVE scache_standarddual_0_0_0_0'
  step B ctl 'REMOVE scache_standarddual_0_0_1_0'
  echo 'SCACHE_NETTY_TWO_NODE_PASS (separate service check, not Spark Shuffle or UB)'
}
ub_smoke() {
  snapshot before-smoke; step A smoke ub; step A events smoke
  snapshot after-smoke; metrics_check after-smoke; delta_check before-smoke after-smoke; quiet_check clean-smoke
  echo SPARK_UB_TWO_NODE_PASS
}
shared_verify() {
  [[ ${SHARED_ROOT:-} == /* && $SHARED_ROOT != / ]] || fail 'SHARED_ROOT must be an existing shared filesystem mounted at the same path on both hosts'
  step A shared-write; step B shared-check A; step B shared-write; step A shared-check B
  echo 'SHARED_FILESYSTEM_TWO_NODE_PASS (visibility only; no UB traffic)'
}
shared_prepare() {
  shared_verify
  step A prepare-bench
}
benchmarks() {
  local backend=$1 name
  for name in wordcount sort terasort repartition tpcds; do
    [[ $backend != ub ]] || snapshot "before-$name"
    step A bench "$backend" "$name"; step A events "$backend-$name"
    if [[ $backend == ub ]]; then
      snapshot "after-$name"; metrics_check "after-$name"; delta_check "before-$name" "after-$name"; quiet_check "clean-$name"
    fi
  done
  step A bench-check "$backend"
  echo "BENCHMARK_TWO_NODE_PASS backend=$backend hibench=WordCount,Sort,TeraSort,Repartition tpcds=103"
}
case "$PHASE" in
  05) shared_verify ;;
  10) detect_both ;;
  20) build_both ;;
  30) native_both ;;
  40) scache_pair ;;
  50) scache_pair; spark_cluster ub; ub_smoke ;;
  60)
    shared_prepare; standard_scache_pair; spark_cluster standard
    step A topology; step B topology
    step A smoke standard; step A events smoke; benchmarks standard
    step A topology; step B topology
    ;;
  65)
    standard_scache_pair; spark_cluster scache-netty
    step A smoke scache-netty; step A events smoke
    step A scache-integration-evidence; step B scache-integration-evidence
    step A topology; step B topology
    echo 'SPARK_SCACHE_NETTY_TWO_NODE_PASS (application integration only; UB not tested)'
    ;;
  66)
    shared_prepare; standard_scache_pair; spark_cluster scache-netty
    step A smoke scache-netty; step A events smoke
    step A scache-integration-evidence; step B scache-integration-evidence
    benchmarks standard; benchmarks scache-netty
    step A compare scache-netty
    step A topology; step B topology
    echo 'SPARK_SCACHE_NETTY_BENCHMARKS_PASS (full 103 SQL; UB not tested)'
    ;;
  70) source_gate; shared_prepare; scache_pair; spark_cluster ub; ub_smoke; benchmarks standard; benchmarks ub; step A compare ub ;;
esac
