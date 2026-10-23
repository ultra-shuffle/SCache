#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname "$0")/node.env"
RUN="$WORK/runs/$SESSION"
STATE="$WORK/state"
mkdir -p "$RUN" "$STATE"
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
unset LD_PRELOAD LD_LIBRARY_PATH CLASSPATH JAVA_TOOL_OPTIONS _JAVA_OPTIONS JDK_JAVA_OPTIONS SPARK_HOME SPARK_CONF_DIR JAVA_HOME
unset SPARK_SUBMIT_OPTS SPARK_DAEMON_JAVA_OPTS SPARK_WORKER_OPTS SPARK_MASTER_OPTS SPARK_DIST_CLASSPATH SPARK_PREPEND_CLASSES SPARK_JAVA_OPTS SPARK_LOCAL_DIRS HADOOP_CONF_DIR HADOOP_HOME YARN_CONF_DIR PYSPARK_PYTHON PYSPARK_DRIVER_PYTHON
unset SPARK_LOCAL_HOSTNAME SPARK_PUBLIC_DNS SPARK_ENV_LOADED SPARK_SCACHE_HOME SCACHE_HOME SCACHE_CLIENT_HOST SCACHE_CLIENT_PORT
fail() { echo "ERROR: $*" >&2; exit 1; }
BASE="$PACKAGE/bin/real-ub"
JAVA_HOME="$PACKAGE/dependencies/jdk"
SPARK_HOME="$PACKAGE/runtime/spark"
SCACHE_JAR="$PACKAGE/runtime/scache/SCache-assembly-0.1.0-SNAPSHOT.jar"
TEST_JAR="$PACKAGE/runtime/scache/SCache-ub-integration-test.jar"
export JAVA_HOME SPARK_HOME
export SPARK_CONF_DIR="$RUN/spark-conf" SPARK_LOCAL_IP="$NODE_IP"
export PATH="$JAVA_HOME/bin:$PATH"
mkdir -p "$SPARK_CONF_DIR"
printf 'export JAVA_HOME=%q\n' "$JAVA_HOME" > "$SPARK_CONF_DIR/spark-env.sh"
umdk() {
  [[ -f $STATE/detected-umdk.env ]] || fail 'run phase 10 first'
  source "$STATE/detected-umdk.env"
  export LD_LIBRARY_PATH="${URMA_LIB%/*}:${URMA_COMMON_LIB%/*}:$PROVIDER_DIR"
}
jni() {
  umdk
  local current_device=$DEVICE current_libraries="$URMA_LIB|$URMA_COMMON_LIB|$PROVIDER_LIB"
  [[ -f $STATE/vendor-jni.env ]] || fail 'run phase 20 first'
  source "$STATE/vendor-jni.env"
  [[ $current_libraries == "$URMA_LIB|$URMA_COMMON_LIB|$PROVIDER_LIB" ]] || fail 'cached JNI belongs to different vendor libraries; rerun phase 20'
  DEVICE=$current_device
  sha256sum -c "$STATE/vendor-inputs.sha256" > "$RUN/jni-checksums.log"
  export LD_LIBRARY_PATH="$JNI_LIB_DIR:${URMA_LIB%/*}:${URMA_COMMON_LIB%/*}:$PROVIDER_DIR"
}
port_free() { [[ -z $(ss -H -ltn "sport = :$1") ]] || fail "port $1 is already in use; no existing process stopped"; }
install_logging() {
  mkdir -p "$1/conf"
  if [[ -f $BASE/log4j.properties ]]; then
    cp "$BASE/log4j.properties" "$1/conf/"
  else
    cp "$(dirname "$0")/log4j.properties" "$1/conf/"
  fi
}
start() {
  local name=$1; shift
  nohup setsid env UB_TWO_NODE_SESSION="$SESSION" "$@" > "$RUN/$name.log" 2>&1 < /dev/null &
  echo "$!" > "$RUN/$name.pid"
}
alive() { kill -0 "$(< "$RUN/$1.pid")" 2>/dev/null; }
run_app() {
  local name=$1; shift
  start "$name" timeout --kill-after=30s "$APP_TIMEOUT" "$@"
  wait "$(< "$RUN/$name.pid")"
}
ready() {
  local name=$1 pattern=$2 i
  for ((i=0;i<300;i++)); do
    alive "$name" || { cat "$RUN/$name.log"; fail "$name exited"; }
    if grep -q -- "$pattern" "$RUN/$name.log"; then return; fi
    sleep .2
  done
  tail -n 100 "$RUN/$name.log"; fail "$name did not become ready"
}
group_live() {
  ps -eo pid=,pgid=,stat= | awk -v group="$1" '$2==group && $3 !~ /^Z/ {print $1}'
}
session_apps_live() {
  local listing pid pgid status name allowed
  local -a scache_groups=()
  for name in scache-node scache-master; do
    [[ -f $RUN/$name.pid ]] || continue
    pid=$(< "$RUN/$name.pid")
    [[ $pid =~ ^[0-9]+$ ]] || { echo "INVALID_PID_FILE $RUN/$name.pid"; return 1; }
    scache_groups+=("$pid")
  done
  listing=$(ps -u "$(id -u)" -o pid=,pgid=,stat=) || fail 'cannot enumerate session processes'
  while read -r pid pgid status; do
    [[ $pid =~ ^[0-9]+$ && $pgid =~ ^[0-9]+$ && $status != Z* ]] || continue
    [[ -r /proc/$pid/environ ]] || continue
    if tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep -Fxq "UB_TWO_NODE_SESSION=$SESSION"; then
      allowed=false
      for name in "${scache_groups[@]}"; do [[ $pgid != "$name" ]] || allowed=true; done
      if [[ $allowed == false ]]; then
        echo "SESSION_APPLICATION_STILL_LIVE pid=$pid pgid=$pgid"
        return 1
      fi
    fi
  done <<< "$listing"
  return 0
}
stop_all() {
  local scope=${1:-all} f pid member members i bad=0 owned pass class name
  [[ $scope == all || $scope == app || $scope == scache ]] || fail "invalid stop scope: $scope"
  shopt -s nullglob
  if [[ $scope == scache ]]; then
    session_apps_live || { echo 'SCACHE_RETAINED: session application process exists outside SCache groups'; return 1; }
    for f in "$RUN"/*.pid; do
      name=${f##*/}
      [[ $name != scache-node.pid && $name != scache-master.pid ]] || continue
      pid=$(< "$f")
      [[ $pid =~ ^[0-9]+$ ]] || { echo "INVALID_PID_FILE $f"; return 1; }
      [[ -z $(group_live "$pid") ]] || { echo "SCACHE_RETAINED: application group $pid is still live"; return 1; }
    done
  fi
  # A stopped Spark driver/worker must not keep reading from an already
  # stopped SCache node. Quiesce application groups first, SCache last.
  for pass in 1 2 3 4 5; do
    [[ $scope != app || $pass -le 3 ]] || continue
    [[ $scope != scache || $pass -ge 4 ]] || continue
    for f in "$RUN"/*.pid; do
      name=${f##*/}
      case "$name" in
        spark-worker.pid) class=2 ;;
        spark-master.pid) class=3 ;;
        scache-node.pid) class=4 ;;
        scache-master.pid) class=5 ;;
        *) class=1 ;;
      esac
      [[ $class == "$pass" ]] || continue
      pid=$(< "$f")
      [[ $pid =~ ^[0-9]+$ ]] || { echo "INVALID_PID_FILE $f"; bad=1; continue; }
      members=$(group_live "$pid")
      [[ -n $members ]] || continue
      # A launcher may have exited while its children remain. Inspect all live
      # members before signalling the group; zombies cannot perform more work.
      owned=1
      for member in $members; do
        if ! tr '\0' '\n' < "/proc/$member/environ" | grep -Fx "UB_TWO_NODE_SESSION=$SESSION" >/dev/null; then
          [[ ! -d /proc/$member ]] || { echo "PID_NOT_OWNED $member $f"; owned=0; }
        fi
      done
      [[ $owned == 1 ]] || { bad=1; continue; }
      kill -TERM -- "-$pid" 2>/dev/null || true
      # Spark's master JVM can spend more than ten seconds in shutdown hooks.
      # Wait up to thirty seconds, then retain SCache rather than force-kill.
      for ((i=0;i<300;i++)); do
        [[ -n $(group_live "$pid") ]] || break
        sleep .1
      done
      if [[ -n $(group_live "$pid") ]]; then
        echo "PROCESS_GROUP_STILL_PRESENT $pid after_termination_wait_seconds=30 file=$f"
        ps -eo pid=,ppid=,pgid=,stat=,etime=,comm= | awk -v group="$pid" '$3==group {print "PROCESS_GROUP_MEMBER",$0}'
        bad=1
      fi
    done
    if ((bad!=0 && pass<4)); then
      echo 'SCACHE_RETAINED: an application/native process group is not confirmed stopped'
      return 1
    fi
  done
  return "$bad"
}
native_args() {
  umdk
  local op=$1 size=$2 port=$3
  if [[ $op == read_bw ]]; then
    ((size<=${MAX_READ:-0})) || fail "requested read size $size exceeds confirmed max_read ${MAX_READ:-unknown}; rerun 10"
  else
    ((size<=${MAX_WRITE:-0})) || fail "requested write size $size exceeds confirmed max_write ${MAX_WRITE:-unknown}; rerun 10"
  fi
  "$URMA_PERFTEST" "$op" -h > "$RUN/perftest-help.log" 2>&1 || true
  grep -q -- '--eid_idx' "$RUN/perftest-help.log" || fail 'unsupported perftest CLI'
  grep -q -- '--share_jfr' "$RUN/perftest-help.log" || fail 'UB perftest requires --share_jfr support'
  # The vendor UB path requires a shared JFR; perftest defaults to false.
  # Mirror the application Jetty configuration instead of testing a different
  # endpoint shape that can fail before any READ/WRITE is attempted.
  ARGS=("$op" -d "$DEVICE" --eid_idx "$EID" -P "$port" -n 1000 -s "$size" -O "$PRIORITY" -j true)
  case "$MODE" in
    rm-ctp) grep -q -- '--ctp' "$RUN/perftest-help.log" || fail 'CTP option unavailable'; ARGS+=(-p 0 --ctp) ;;
    rc-rtp)
      ARGS+=(-p 1)
      if [[ $DEVICE == bonding* ]]; then
        grep -q -- '--single_path' "$RUN/perftest-help.log" || fail 'vendor single-path CLI needed; bond_level port is not assumed equivalent'
        ARGS+=(--single_path)
      fi ;;
  esac
  ARGS+=("${NATIVE_EXTRA_ARGS[@]}")
  ulimit -l unlimited 2>/dev/null || true
  printf 'memlock_soft=%s hard=%s\n' "$(ulimit -Sl)" "$(ulimit -Hl)"
}
common_submit() {
  local backend=$1 events=$2
  local job_jars="$PACKAGE/benchmarks/hibench/hadoop-mapreduce-examples-3.3.4.jar"
  [[ -f $job_jars ]] || fail "Hadoop 3.3.4 TeraSort examples JAR missing: $job_jars"
  [[ $backend != ub && $backend != scache-netty ]] || job_jars="$job_jars,$SCACHE_JAR"
  port_free 19778; port_free 19779
  export SCACHE_CLIENT_HOST="$NODE_IP" SCACHE_CLIENT_PORT=19071 SPARK_SCACHE_HOME="$RUN/scache"
  SUBMIT=(--master "spark://$MASTER_IP:19777" --deploy-mode client --driver-memory 3g --jars "$job_jars"
    --conf spark.cores.max=4 --conf spark.executor.cores=2 --conf spark.executor.memory=2g
    --conf "spark.driver.host=$NODE_IP" --conf "spark.driver.bindAddress=$NODE_IP"
    --conf spark.driver.port=19778 --conf spark.driver.blockManager.port=19779
    --conf spark.blockManager.port=19780 --conf spark.port.maxRetries=0 --conf spark.default.parallelism=4
    --conf spark.sql.shuffle.partitions=4 --conf spark.sql.adaptive.enabled=false
    --conf spark.scheduler.minRegisteredResourcesRatio=1.0
    --conf spark.scheduler.maxRegisteredResourcesWaitingTime=60s
    --conf spark.serializer=org.apache.spark.serializer.JavaSerializer
    --conf spark.eventLog.enabled=true --conf spark.eventLog.compress=false
    --conf spark.eventLog.rolling.enabled=false --conf "spark.eventLog.dir=file://$events")
  if [[ $backend == ub || $backend == scache-netty ]]; then
    SUBMIT+=(--conf spark.shuffle.useOldFetchProtocol=true
      --conf spark.scache.enable=true --conf spark.scache.strict=true
      --conf spark.scache.productionFallback.enabled=false)
  fi
  if [[ $backend == ub ]]; then
    jni
    SUBMIT+=(--conf spark.scache.shuffle.noLocalFiles=true
      --conf spark.scache.blockTransfer.backend=ub
      --conf spark.scache.ub.pool.enabled=true
      --conf spark.shuffle.manager=sort
      --conf spark.shuffle.push.enabled=false
      --conf "spark.driver.extraLibraryPath=$LD_LIBRARY_PATH"
      --conf 'spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED'
      --conf 'spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED')
    # Executor library/config paths come from each worker's own environment.
  elif [[ $backend != scache-netty ]]; then
    SUBMIT+=(--conf spark.scache.enable=false --conf spark.shuffle.manager=sort)
  fi
}
action=${1:?}; shift
case "$action" in
  lease)
    if ! mkdir "$STATE/dual-node-active" 2>/dev/null; then
      fail "workspace already reserved; inspect $STATE/dual-node-active/session and stop that session first"
    fi
    printf '%s\n' "$SESSION" > "$STATE/dual-node-active/session"
    ;;
  release)
    if [[ -f $STATE/dual-node-active/session && $(< "$STATE/dual-node-active/session") == "$SESSION" ]]; then
      rm -- "$STATE/dual-node-active/session"
      rmdir -- "$STATE/dual-node-active"
    fi
    ;;
  probe)
    [[ $(uname -m) == aarch64 ]] || fail 'aarch64 required'
    for cmd in bash timeout ss setsid nohup tar sha256sum ps; do command -v "$cmd" || fail "missing $cmd"; done
    [[ -x $JAVA_HOME/bin/java && -x $SPARK_HOME/bin/spark-submit ]] || fail 'runtime package missing'
    build_record="$PACKAGE/metadata/rm-ctp-build.txt"
    [[ -f $build_record ]] || fail 'RM/CTP isolated build marker missing; do not use the old U1 package'
    grep -Fxq 'build_kind=isolated-rm-ctp-experiment' "$build_record" || fail 'package build kind mismatch'
    grep -Fxq 'spark_version=3.5.7' "$build_record" || fail 'package Spark version mismatch'
    for spec in \
      "scache_assembly_sha256:$SCACHE_JAR" \
      "scache_integration_sha256:$TEST_JAR" \
      "spark_core_sha256:$SPARK_HOME/jars/spark-core_2.13-3.5.7.jar"; do
      key=${spec%%:*}; file=${spec#*:}
      expected=$(sed -n "s/^$key=//p" "$build_record")
      [[ $expected =~ ^[0-9a-f]{64}$ ]] || fail "invalid build record: $key"
      [[ $(sha256sum "$file" | awk '{print $1}') == "$expected" ]] || fail "artifact hash mismatch: $file"
    done
    "$JAVA_HOME/bin/java" -version 2>&1 | tee "$RUN/java-version.log"
    grep -q 'version "17\.' "$RUN/java-version.log" || fail 'bundled Java is not 17'
    [[ -f $SPARK_HOME/jars/spark-core_2.13-3.5.7.jar ]] || fail 'Spark 3.5.7 Scala 2.13 JAR missing'
    ip -o -4 addr show | awk '{split($4,a,"/");print a[1]}' | grep -Fxq "$NODE_IP" || fail 'configured node IP is not local'
    cat /proc/sys/kernel/random/boot_id > "$RUN/boot-id.txt"
    uname -a; cat /etc/os-release
    sha256sum "$SCACHE_JAR" "$TEST_JAR" "$SPARK_HOME/jars/spark-core_2.13-3.5.7.jar" "$PACKAGE/benchmarks/tools/benchmark-checks.jar" | awk '{print $1}' > "$RUN/runtime-hashes.txt"
    # The core JAR alone cannot detect a stale Spark SQL/Hadoop/Scala JAR on
    # the peer. Hash every installed Spark JAR using relative paths, so hosts
    # with different installation directories remain comparable.
    (cd "$SPARK_HOME/jars" && find . -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum | awk '{print $1}') >> "$RUN/runtime-hashes.txt"
    (cd "$PACKAGE/sources/scache-jni"; find . -type f -exec sha256sum {} + | sort | sha256sum | awk '{print $1}') >> "$RUN/runtime-hashes.txt"
    ;;
  boot-id) cat "$RUN/boot-id.txt" ;;
  runtime-hashes) cat "$RUN/runtime-hashes.txt" ;;
  sdk-version)
    [[ -f $STATE/detected-umdk.env ]] || fail 'vendor SDK has not been detected'
    source "$STATE/detected-umdk.env"
    printf '%s\n' "$UMDK_LABEL"
    ;;
  selection-json)
    [[ -f $STATE/device-selection.json ]] || fail 'device/EID selection has not been confirmed by phase 10'
    cat "$STATE/device-selection.json"
    ;;
  max-request)
    umdk
    [[ ${MAX_MSG:-} =~ ^[0-9]+$ && ${MAX_READ:-} =~ ^[0-9]+$ && ${MAX_WRITE:-} =~ ^[0-9]+$ ]] ||
      fail 'device request limits missing; rerun phase 10'
    python3 -c 'import sys
values = [int(x) for x in sys.argv[1:]]
if any(x <= 0 for x in values):
    raise SystemExit("device reported a zero request limit")
print(min(values))' "$MAX_MSG" "$MAX_READ" "$MAX_WRITE" "$UB_CHUNK_BYTES"
    ;;
  detect)
    if [[ ${1:-} == --software-only ]]; then
      # Building a .so does not require a hardware selection. If 10 has run,
      # prefer its provider record; do not invent a new runtime selection.
      build_device=$DEVICE; build_provider=$PROVIDER
      if [[ -f $STATE/device-selection.json ]]; then
        build_device=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["endpoint"]["device"])' "$STATE/device-selection.json")
        build_provider=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["endpoint"]["provider"])' "$STATE/device-selection.json")
      fi
      discovery_args=(--software-only)
      [[ $build_device == auto ]] || discovery_args+=(--device "$build_device")
      [[ $build_provider == auto ]] || discovery_args+=(--provider-lib "$build_provider")
      [[ -z ${SDK_ROOT:-} ]] || discovery_args+=(--umdk-root "$SDK_ROOT")
      for root in "${SEARCH_ROOTS[@]}"; do discovery_args+=(--search-root "$root"); done
      bash "$BASE/00-detect-vendor-umdk.sh" --package-home "$PACKAGE" --work-root "$WORK" "${discovery_args[@]}"
    else
      policy=reuse; [[ ${PHASE:-} != 10 ]] || policy=confirm
      bash "$(dirname "$0")/discover.sh" "$policy"
    fi
    cp "$STATE/detected-umdk.env" "$RUN/detected-umdk.env"
    umdk
    ;;
  application-check)
    umdk
    if [[ $MODE == rc-rtp ]]; then
      [[ $EID == 0 && $PRIORITY == 0 ]] || fail 'RC/RTP requires EID 0 and priority 0 in this legacy test path'
    else
      [[ $MODE == rm-ctp && $EID =~ ^[0-9]+$ && $PRIORITY =~ ^[0-9]+$ ]] ||
        fail 'RM/CTP requires a selected EID index and priority'
      ((PRIORITY<=15)) || fail 'RM/CTP priority exceeds 15'
    fi
    ;;
  jni)
    umdk
    selected="$URMA_LIB|$URMA_COMMON_LIB|$PROVIDER_LIB"
    # Include transitive SDK headers and build helpers, not just urma_api.h.
    source_stamp=$({
      find "$PACKAGE/sources/scache-jni" -type f -exec sha256sum {} +
      find "$URMA_INCLUDE" -type f -name '*.h' -exec sha256sum {} +
      sha256sum "$BASE/01-build-jni-with-vendor-umdk.sh" "$BASE/lib/common.sh" "$PACKAGE/bin/build-lib-spark-urma.sh"
    } | sort | sha256sum | awk '{print $1}')
    previous=
    [[ ! -f $STATE/vendor-jni.env ]] || previous=$(source "$STATE/vendor-jni.env"; echo "$URMA_LIB|$URMA_COMMON_LIB|$PROVIDER_LIB")
    if [[ $selected == "$previous" && -f $STATE/jni-source.txt && $(< "$STATE/jni-source.txt") == "$source_stamp" ]] && sha256sum -c "$STATE/vendor-inputs.sha256" >/dev/null 2>&1; then
      echo VENDOR_JNI_REUSED
    else
      bash "$BASE/01-build-jni-with-vendor-umdk.sh" --package-home "$PACKAGE" --work-root "$WORK"
      printf '%s\n' "$source_stamp" > "$STATE/jni-source.txt"
    fi
    jni; ldd "$JNI_ARTIFACT" | tee "$RUN/jni-ldd.log"
    ! grep -q 'not found' "$RUN/jni-ldd.log" || fail 'JNI unresolved library'
    ;;
  native-server)
    op=$1 size=$2 port=$3 tag=$4
    native_args "$op" "$size" "$port"; port_free "$port"
    grep -q -- '--bind_ip' "$RUN/perftest-help.log" || fail 'server requires --bind_ip to bind its management address'
    ARGS+=(--bind_ip "$NODE_IP")
    printf '%q ' "$URMA_PERFTEST" "${ARGS[@]}" > "$RUN/$tag.command"
    start "$tag" bash "$0" native-child "$tag" "$URMA_PERFTEST" "${ARGS[@]}"
    for ((i=0;i<200;i++)); do
      alive "$tag" || { cat "$RUN/$tag.log"; fail 'native server exited before listening'; }
      [[ -z $(ss -H -ltn "sport = :$port") ]] || { echo "NATIVE_SERVER_READY ip=$NODE_IP port=$port"; exit 0; }
      sleep .1
    done
    fail 'native server never listened'
    ;;
  native-child)
    tag=$1; shift; umdk; ulimit -l unlimited 2>/dev/null || true
    set +e
    timeout --kill-after=10s "$NATIVE_TIMEOUT" "$@"
    rc=$?; printf '%s\n' "$rc" > "$RUN/$tag.exit"; exit "$rc"
    ;;
  native-client)
    op=$1 size=$2 port=$3 peer=$4 tag=$5
    native_args "$op" "$size" "$port"; ARGS+=(-S "$peer")
    printf '%q ' "$URMA_PERFTEST" "${ARGS[@]}" > "$RUN/$tag.command"
    start "$tag" timeout --kill-after=10s "$NATIVE_TIMEOUT" "$URMA_PERFTEST" "${ARGS[@]}"
    # Print the vendor diagnostic even when perftest exits nonzero. Otherwise
    # set -e hides the actionable client error until someone opens an archive.
    if wait "$(< "$RUN/$tag.pid")"; then rc=0; else rc=$?; fi
    printf '%s\n' "$rc" > "$RUN/$tag.exit"
    cat "$RUN/$tag.log"
    if ((rc==0)); then
      # Official perftest REPORT_BW_FMT: bytes, iterations, peak BW,
      # average BW, message rate. A clean process exit alone is not a WR gate.
      awk -v size="$size" '
        $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && NF>=5 &&
          $1==size && $2>=1000 && $4+0>0 {found=1}
        END {exit !found}
      ' "$RUN/$tag.log" || fail "NATIVE_RESULT_MISSING: vendor tool exited 0 without a matching completed bandwidth row; log=$RUN/$tag.log"
    fi
    exit "$rc"
    ;;
  native-wait)
    tag=$1
    for ((i=0;i<NATIVE_TIMEOUT;i++)); do [[ ! -f $RUN/$tag.exit ]] || break; sleep 1; done
    cat "$RUN/$tag.log"
    [[ -f $RUN/$tag.exit && $(< "$RUN/$tag.exit") == 0 ]] || fail 'server failed or timed out'
    ;;
  scache-master)
    port_free 19070; mkdir -p "$RUN/master/conf"
    install_logging "$RUN/master"
    printf 'scache.master.ip="%s"\nscache.master.port=19070\n' "$MASTER_IP" > "$RUN/master/conf/scache.conf"
    if [[ $PHASE == 40 || $PHASE == 50 || $PHASE == 70 ]]; then
      # The master's map-complete callback otherwise retains legacy heap
      # prefetch, despite the data nodes using registered UB arena slices.
      printf 'scache.ub.pool.enabled=true\n' >> "$RUN/master/conf/scache.conf"
    fi
    start scache-master env SCACHE_HOME="$RUN/master" "$JAVA_HOME/bin/java" -Xmx1g -cp "$SCACHE_JAR" org.scache.deploy.ScacheMaster --ip "$MASTER_IP" --port 19070 --local false
    for ((i=0;i<200;i++)); do alive scache-master || fail 'SCache master exited'; [[ -z $(ss -H -ltn 'sport = :19070') ]] || exit 0; sleep .1; done
    fail 'SCache master did not listen'
    ;;
  scache-node)
    jni; ulimit -l unlimited 2>/dev/null || true
    limit=$(ulimit -Sl); required=$((ARENA_MIB*1024+65536))
    echo "MEMLOCK soft_kib=$limit required_kib=$required"
    [[ $limit == unlimited ]] || ((limit>=required)) || fail 'memlock too low for arena'
    for p in 19071 19072 19073; do port_free "$p"; done
    mkdir -p "$RUN/scache/conf" "$RUN/scache/tmp"
    cat > "$RUN/scache/conf/scache.conf" <<EOF
scache.master.ip="$MASTER_IP"
scache.master.port=19070
scache.client.host="$NODE_IP"
scache.client.port=19071
scache.storage.network.enabled=true
scache.storage.cxl.shared.enabled=false
scache.blockTransfer.backend="ub"
scache.ub.transport="real"
scache.ub.mode="$MODE"
scache.ub.device="$DEVICE"
scache.ub.eidIndex=$EID
scache.ub.priority=$PRIORITY
scache.ub.pool.enabled=true
scache.ub.pool.path="$RUN/scache/ub-pool"
scache.ub.pool.forceOnCommit=false
scache.ub.checksum.enabled=true
scache.ub.diagnostics.level="trace"
scache.daemon.ipc.backend="pool"
scache.daemon.ipc.pool.path="$RUN/scache/ipc.pool"
scache.daemon.ipc.pool.size="128m"
scache.daemon.ipc.pool.mapChunk="64m"
scache.daemon.ipc.pool.align=4096
scache.daemon.ipc.pool.zeroCopy.put=true
EOF
    cp "$BASE/log4j.properties" "$RUN/scache/conf/"
    [[ $NODE == A ]] && role=listen || role=connect
    ub_args=(--ub-device "$DEVICE")
    if [[ $MODE == rm-ctp ]]; then
      ub_args+=(--ub-mode rm-ctp --eid-index "$EID" --priority "$PRIORITY"
        --ub-pool-path "$RUN/scache/ub-pool" --ipc-pool-path "$RUN/scache/ipc.pool")
    else
      ub_args+=(--wire-role "$role" --wire-path hardware)
    fi
    start scache-node env SCACHE_HOME="$RUN/scache" \
      "$JAVA_HOME/bin/java" -Xmx1g --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
      -Djava.library.path="$LD_LIBRARY_PATH" -cp "$SCACHE_JAR:$TEST_JAR" org.scache.deploy.NettyBlockTransferIntegrationNode \
      --name "two-$NODE" --host "$NODE_IP" --rpc-port 19071 --control-port 19072 --master-host "$MASTER_IP" --master-port 19070 \
      --backend ub --ub-transport real --ub-control-port 19073 "${ub_args[@]}" \
      --arena-bytes 67108864 --arena-count "$((ARENA_MIB/64))" --queue-depth 64 --chunk-size "$UB_CHUNK_BYTES" --lease-timeout-ms 60000 --timeout-ms 60000
    ready scache-node SCACHE_TEST_NODE_READY
    cp "/proc/$(< "$RUN/scache-node.pid")/maps" "$RUN/scache-maps.txt"
    grep -Fq "$PROVIDER_LIB" "$RUN/scache-maps.txt" || fail 'selected provider not loaded'
    grep -Fq "$URMA_LIB" "$RUN/scache-maps.txt" || fail 'selected liburma not loaded'
    ! grep -Ei 'liburma.*(openurma|vdev)' "$RUN/scache-maps.txt" || fail 'virtual provider loaded'
    ;;
  scache-standard-node)
    # Standard Spark is independent of UB. Start a real SCache process with
    # its Netty backend for the explicit two-host topology/service check.
    for p in 19071 19072 19074; do port_free "$p"; done
    mkdir -p "$RUN/scache/conf" "$RUN/scache/tmp"
    printf 'scache.master.ip="%s"\nscache.master.port=19070\nscache.client.host="%s"\nscache.client.port=19071\nscache.storage.network.enabled=true\nscache.storage.cxl.shared.enabled=false\nscache.blockTransfer.backend="netty"\n' \
      "$MASTER_IP" "$NODE_IP" > "$RUN/scache/conf/scache.conf"
    printf 'scache.blockManager.port=19074\n' >> "$RUN/scache/conf/scache.conf"
    install_logging "$RUN/scache"
    start scache-node env SCACHE_HOME="$RUN/scache" \
      "$JAVA_HOME/bin/java" -Xmx1g --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
      -cp "$SCACHE_JAR:$TEST_JAR" org.scache.deploy.NettyBlockTransferIntegrationNode \
      --name "standard-$NODE" --host "$NODE_IP" --rpc-port 19071 --control-port 19072 \
      --master-host "$MASTER_IP" --master-port 19070 --backend netty --timeout-ms 60000
    ready scache-node SCACHE_TEST_NODE_READY
    grep 'SCACHE_TEST_NODE_READY.*backend=netty' "$RUN/scache-node.log"
    grep -q 'blockManagerId=BlockManagerId([^,]*, [^,]*, 19074)' "$RUN/scache-node.log" || fail 'SCache Netty did not bind the configured port 19074'
    ;;
  ctl)
    exec 7<>"/dev/tcp/$NODE_IP/19072"; printf '%s\n' "$1" >&7
    IFS= read -r -t 75 reply <&7 || fail 'SCache control timeout'; exec 7>&-
    echo "$reply"; [[ $reply == OK\ * ]] || fail 'SCache control rejected command'
    ;;
  spark-master)
    port_free 19777; port_free 19880
    start spark-master "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master --host "$NODE_IP" --port 19777 --webui-port 19880
    ready spark-master 'Starting Spark master'
    ;;
  spark-worker)
    backend=$1; port_free 19181; port_free 19881; port_free 19780
    if [[ $backend == ub || $backend == scache-netty ]]; then
      [[ $backend != ub ]] || jni
      # Each worker may have a different package/workspace path. The driver
      # --jars upload supplies Spark's classloader, while this local fallback
      # lets ScacheDaemon's reflection loader find the same JAR on both hosts.
      mkdir -p "$RUN/scache/jars"
      cp "$SCACHE_JAR" "$RUN/scache/jars/"
    fi
    start spark-worker env SCACHE_CLIENT_HOST="$NODE_IP" SCACHE_CLIENT_PORT=19071 SPARK_SCACHE_HOME="$RUN/scache" \
      "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker --host "$NODE_IP" --port 19181 --webui-port 19881 \
      --cores 2 --memory 3g --work-dir "$RUN/worker" "spark://$MASTER_IP:19777"
    ready spark-worker 'Successfully registered with master'
    ;;
  smoke)
    backend=$1; events="$RUN/events/smoke"; mkdir -p "$events"
    common_submit "$backend" "$events"
    run_app smoke "$SPARK_HOME/bin/spark-submit" "${SUBMIT[@]}" \
      --class StandardSparkShuffleSmoke "$PACKAGE/benchmarks/tools/standard-spark-smoke.jar"
    grep 'STANDARD_SPARK_357_SMOKE_PASS' "$RUN/smoke.log"
    ;;
  scache-integration-evidence)
    grep -Rq 'SCACHE_READY strict=true' "$RUN/worker" || fail 'Spark executor did not initialize strict SCache'
    [[ $NODE != A ]] || grep -q 'SCACHE_READY strict=true' "$RUN/smoke.log" ||
      fail 'Spark driver did not initialize strict SCache'
    count=$(grep -c 'Block scache_app-' "$RUN/scache-node.log" || true)
    ((count>0)) || fail 'no Spark shuffle blocks were published to this SCache node'
    evidence_paths=("$RUN/worker")
    [[ $NODE != A ]] || evidence_paths+=("$RUN/smoke.log")
    ! grep -R -q 'SCACHE_FALLBACK' "${evidence_paths[@]}" || fail 'SCache fallback detected'
    echo "SCACHE_INTEGRATION_EVIDENCE node=$NODE published_blocks=$count strict=true"
    ;;
  shared-write)
    [[ -d ${SHARED_RUN%/*} ]] || fail 'shared root must already exist; mount shared filesystem first'
    mkdir -p "$SHARED_RUN"; printf '%s\n' "$SESSION-$NODE" > "$SHARED_RUN/marker-$NODE"
    ;;
  shared-check) [[ $(< "$SHARED_RUN/marker-$1") == "$SESSION-$1" ]] || fail 'shared filesystem marker mismatch' ;;
  prepare-bench)
    mkdir -p "$SHARED_RUN/package/benchmarks" "$SHARED_RUN/standard" "$SHARED_RUN/ub" "$SHARED_RUN/scache-netty"
    cp -a "$PACKAGE/benchmarks/tpcds" "$SHARED_RUN/package/benchmarks/"
    for kind in standard ub scache-netty; do
      "$JAVA_HOME/bin/java" -cp "$PACKAGE/benchmarks/tools/benchmark-checks.jar:$SPARK_HOME/jars/*" BenchmarkChecks prepare "$SHARED_RUN/$kind"
    done
    ;;
  bench)
    backend=$1; name=$2; events="$RUN/events/$backend-$name"; mkdir -p "$events"
    bench="$SHARED_RUN/$backend"; common_submit "$backend" "$events"
    export SPARKBENCH_PROPERTIES_FILES="$bench/hibench.properties"
    if [[ $name == tpcds ]]; then
      mkdir -p "$bench/tpcds"
      app=(--class BenchmarkChecks "$PACKAGE/benchmarks/tools/benchmark-checks.jar" tpcds "$SHARED_RUN/package" "$bench/tpcds")
    else
      case "$name" in
        wordcount) cls=ScalaWordCount ;;
        sort) cls=ScalaSort ;;
        terasort) cls=ScalaTeraSort ;;
        repartition) cls=ScalaRepartition ;;
        *) fail "Unknown HiBench workload: $name" ;;
      esac
      input_dir="$bench/input"
      if [[ $name == terasort || $name == repartition ]]; then
        input_dir="$bench/input-tera"
        # The deterministic 1 MiB input otherwise creates one map task. Split it
        # so this two-worker smoke tests both remote executors, not just the
        # scheduler's first idle worker.
        SUBMIT+=(--conf spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=65536)
      fi
      app=(--class "com.intel.hibench.sparkbench.micro.$cls" "$PACKAGE/benchmarks/hibench/sparkbench-assembly-7.1.1-dist.jar" "file://$input_dir" "file://$bench/$name")
      [[ $name != repartition ]] || app+=(false false)
    fi
    run_app "$backend-$name" "$SPARK_HOME/bin/spark-submit" "${SUBMIT[@]}" \
      --conf "spark.executorEnv.SPARKBENCH_PROPERTIES_FILES=$bench/hibench.properties" "${app[@]}"
    [[ $name != tpcds ]] || grep 'TPCDS_PASS files=99 statements=103' "$RUN/$backend-$name.log"
    ;;
  bench-check)
    "$JAVA_HOME/bin/java" -cp "$PACKAGE/benchmarks/tools/benchmark-checks.jar:$SPARK_HOME/jars/*" BenchmarkChecks check "$SHARED_RUN/$1"
    ;;
  compare)
    target=${1:?comparison backend required}
    [[ $target == ub || $target == scache-netty ]] || fail 'unsupported comparison backend'
    count=0
    for f in "$SHARED_RUN/standard/tpcds/"*-results.jsonl; do
      name=${f##*/}; q=${name%-results.jsonl}
      cmp "$f" "$SHARED_RUN/$target/tpcds/$name"
      cmp "$SHARED_RUN/standard/tpcds/$q-schema.json" "$SHARED_RUN/$target/tpcds/$q-schema.json"
      count=$((count+1))
    done
    ((count==103)) || fail "expected 103 comparisons, got $count"
    echo "TPCDS_REFERENCE_MATCH_103 backend=$target"
    ;;
  events)
    shift_name=$1
    awk -v a="$MASTER_IP" -v b="$PEER_IP" '
      /SparkListenerTaskEnd/ {tasks++; compact=$0; gsub(/[[:space:]]/,"",compact); if(index(compact,"\"Host\":\""a"\""))na++; if(index(compact,"\"Host\":\""b"\""))nb++; if($0 !~ /"Reason"[[:space:]]*:[[:space:]]*"Success"/)bad++}
      /SparkListenerApplicationEnd/ {ends++}
      END{printf "tasks=%d a=%d b=%d failed=%d ends=%d\n",tasks,na,nb,bad,ends; if(!na||!nb||bad||ends!=1)exit 1}
    ' "$RUN/events/$shift_name/"*
    ;;
  topology)
    python3 "$(dirname "$0")/verify-topology.py" --run "$RUN" --node "$NODE" \
      --ip "$NODE_IP" --master "$MASTER_IP" --peer "$PEER_IP"
    ;;
  dma-unresolved)
    # A CQ timeout with accepted provider-owned WRs is not a DMA fence. The
    # controller must leave BOTH endpoints alive so neither MR/lease is torn
    # down by an automatic test cleanup before an operator confirms recovery.
    matches=$(find "$RUN" -type f \( -name '*.log' -o -name stdout -o -name stderr \) \
      -exec grep -l -E 'ERR_DMA_UNRESOLVED|UB_CLOSE_QUARANTINED' {} + 2>/dev/null) || true
    [[ -n $matches ]] || exit 1
    printf 'DMA_UNRESOLVED_LOGS node=%s\n%s\n' "$NODE" "$matches"
    ;;
  stop) stop_all all ;;
  stop-app) stop_all app ;;
  app-quiescent) session_apps_live ;;
  stop-scache) stop_all scache ;;
  evidence)
    timeout 10 journalctl -k --since "$START_TIME" --no-pager > "$RUN/kernel.log" 2>&1 || true
    ss -ltnp > "$RUN/sockets.log" 2>&1 || true
    [[ ! -f $STATE/vendor-jni.env ]] || cp "$STATE/vendor-jni.env" "$RUN/"
    # Spark's worker directory contains one copied application JAR per executor.
    # Preserve diagnostic streams while excluding those redundant binaries from
    # evidence archives; they are already fingerprinted in the package manifest.
    if [[ -d $RUN/worker ]]; then
      (cd "$RUN" && mkdir -p executor-logs && find worker -type f \
        \( -name stdout -o -name stderr -o -name '*.log' \) \
        -exec cp --parents -t executor-logs {} +)
    fi
    ;;
  archive) tar --exclude="runs/$SESSION/worker" --exclude="runs/$SESSION/scache/jars" \
    -czf - -C "$WORK" "runs/$SESSION" ;;
  *) fail "unknown node action: $action" ;;
esac
