#!/usr/bin/env bash
set -Eeuo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd -P)
TEST_ROOT=$(mktemp -d /tmp/ub-two-node-checks-XXXXXX)
mkdir -p "$TEST_ROOT/tool" "$TEST_ROOT/work/state"
cp "$HERE/lib/node.sh" "$TEST_ROOT/tool/node.sh"
cp "$HERE/tests/fake-perftest.sh" "$TEST_ROOT/perftest"
chmod +x "$TEST_ROOT/perftest"
{
  printf 'PACKAGE=%q\nWORK=%q\n' "$TEST_ROOT/package with spaces" "$TEST_ROOT/work"
  printf 'SESSION=node-checks\nNODE=A\nNODE_IP=10.55.0.1\nMASTER_IP=10.55.0.1\nPEER_IP=10.55.0.2\nMODE=rm-ctp\nDEVICE=bonding_dev_0\nEID=0\nPRIORITY=6\nNATIVE_EXTRA_ARGS=()\nNATIVE_TIMEOUT=5\nSTART_TIME=2026-09-29\n'
} > "$TEST_ROOT/tool/node.env"
{
  printf 'URMA_PERFTEST=%q\n' "$TEST_ROOT/perftest"
  printf 'URMA_LIB=/usr/lib/liburma.so\nURMA_COMMON_LIB=/usr/lib/liburma_common.so\nPROVIDER_DIR=/usr/lib\nMAX_READ=268435456\nMAX_WRITE=268435456\n'
} > "$TEST_ROOT/work/state/detected-umdk.env"
# The generic protocol negotiates one common SEND/READ/WRITE limit; preserve
# its message bound until there is operation-specific peer negotiation.
printf 'MAX_MSG=4096\nMAX_READ=67108864\nMAX_WRITE=33554432\n' >> "$TEST_ROOT/work/state/detected-umdk.env"
UB_CHUNK_BYTES=10485760 bash "$TEST_ROOT/tool/node.sh" max-request > "$TEST_ROOT/max-request.log"
grep -Fxq 4096 "$TEST_ROOT/max-request.log"
printf 'MAX_MSG=67108864\n' >> "$TEST_ROOT/work/state/detected-umdk.env"
UB_CHUNK_BYTES=10485760 bash "$TEST_ROOT/tool/node.sh" max-request > "$TEST_ROOT/max-request-configured.log"
grep -Fxq 10485760 "$TEST_ROOT/max-request-configured.log"
UB_CHUNK_BYTES=67108864 bash "$TEST_ROOT/tool/node.sh" max-request > "$TEST_ROOT/max-request-device.log"
grep -Fxq 33554432 "$TEST_ROOT/max-request-device.log"
if bash "$TEST_ROOT/tool/node.sh" dma-unresolved > "$TEST_ROOT/dma-empty.log" 2>&1; then
  echo 'empty DMA hazard scan reported a false positive'; exit 1
fi
printf 'ERR_DMA_UNRESOLVED: synthetic provider-owned request\n' > "$TEST_ROOT/work/runs/node-checks/scache-node.log"
bash "$TEST_ROOT/tool/node.sh" dma-unresolved > "$TEST_ROOT/dma-found.log"
grep -q 'DMA_UNRESOLVED_LOGS node=A' "$TEST_ROOT/dma-found.log"
bash "$TEST_ROOT/tool/node.sh" native-client read_bw 4096 19190 10.55.0.2 sample > "$TEST_ROOT/client.log"
grep -Fq -- '[-p] [0] [--ctp]' "$TEST_ROOT/client.log"
grep -Fq -- '[-j] [true]' "$TEST_ROOT/client.log"
grep -Fq -- '[-S] [10.55.0.2]' "$TEST_ROOT/client.log"
! grep -Fq '127.0.0.1' "$TEST_ROOT/client.log"
printf 'NATIVE_EXTRA_ARGS=(--fake-fail)\n' >> "$TEST_ROOT/tool/node.env"
if bash "$TEST_ROOT/tool/node.sh" native-client read_bw 4096 19190 10.55.0.2 failing \
    > "$TEST_ROOT/client-failed.log" 2>&1; then
  echo 'failed vendor perftest was accepted'; exit 1
fi
grep -Fq 'MOCK_VENDOR_PERFTEST_FAILURE: failed to import jetty' "$TEST_ROOT/client-failed.log"
[[ $(< "$TEST_ROOT/work/runs/node-checks/failing.exit") == 42 ]]
printf 'NATIVE_EXTRA_ARGS=(--fake-empty-success)\n' >> "$TEST_ROOT/tool/node.env"
if bash "$TEST_ROOT/tool/node.sh" native-client read_bw 4096 19190 10.55.0.2 empty \
    > "$TEST_ROOT/client-empty.log" 2>&1; then
  echo 'empty successful vendor output was accepted as completed READ'; exit 1
fi
grep -q NATIVE_RESULT_MISSING "$TEST_ROOT/client-empty.log"
printf 'NATIVE_EXTRA_ARGS=()\n' >> "$TEST_ROOT/tool/node.env"
# Execute the real argument builder without starting Spark or a provider.
mkdir -p "$TEST_ROOT/package with spaces/benchmarks/hibench"
touch "$TEST_ROOT/package with spaces/benchmarks/hibench/hadoop-mapreduce-examples-3.3.4.jar"
bash -c '
  source <(sed "/^action=/,\$d" "$0")
  port_free() { :; }
  jni() { export LD_LIBRARY_PATH=/mock/vendor; }
  common_submit ub /mock/events
  printf "%s\n" "${SUBMIT[@]}"
' "$TEST_ROOT/tool/node.sh" > "$TEST_ROOT/ub-submit.log"
grep -Fxq 'spark.scache.blockTransfer.backend=ub' "$TEST_ROOT/ub-submit.log"
grep -Fxq 'spark.scache.ub.pool.enabled=true' "$TEST_ROOT/ub-submit.log"
grep -Fxq 'spark.shuffle.manager=sort' "$TEST_ROOT/ub-submit.log"
grep -Fxq 'spark.shuffle.push.enabled=false' "$TEST_ROOT/ub-submit.log"
bash -c '
  source <(sed "/^action=/,\$d" "$0")
  port_free() { :; }
  common_submit standard /mock/events
  printf "%s\n" "${SUBMIT[@]}"
' "$TEST_ROOT/tool/node.sh" > "$TEST_ROOT/standard-submit.log"
! grep -q 'spark.scache.ub.pool.enabled=true' "$TEST_ROOT/standard-submit.log"
grep -Fxq 'spark.scache.enable=false' "$TEST_ROOT/standard-submit.log"
# Inspect the actual Master configuration action with process operations stubbed.
for phase in 40 50 70 60 65 66; do
  bash -c '
    source <(sed "/^action=/,\$d" "$0")
    PHASE=$1
    port_free() { :; }
    install_logging() { :; }
    start() { :; }
    alive() { return 0; }
    ss() { echo LISTEN; }
    set -- scache-master
    eval "$(sed -n "/^action=/,\$p" "$0")"
  ' "$TEST_ROOT/tool/node.sh" "$phase" > "$TEST_ROOT/master-$phase.log"
  if [[ $phase == 40 || $phase == 50 || $phase == 70 ]]; then
    grep -Fxq 'scache.ub.pool.enabled=true' "$TEST_ROOT/work/runs/node-checks/master/conf/scache.conf"
  else
    ! grep -q 'scache.ub.pool.enabled' "$TEST_ROOT/work/runs/node-checks/master/conf/scache.conf"
  fi
done
bash "$TEST_ROOT/tool/node.sh" lease
if bash "$TEST_ROOT/tool/node.sh" lease > "$TEST_ROOT/lease.log" 2>&1; then echo 'lease unexpectedly reused'; exit 1; fi
bash "$TEST_ROOT/tool/node.sh" release
[[ ! -d $TEST_ROOT/work/state/dual-node-active ]]
# Exercise the actual JNI state-loading function without loading any library.
# These files are cache fixtures, not functional native artifacts.
mkdir -p "$TEST_ROOT/vendor"
touch "$TEST_ROOT/vendor/liburma.so" "$TEST_ROOT/vendor/libcommon.so" "$TEST_ROOT/vendor/provider.so" "$TEST_ROOT/vendor/jni.so"
{
  printf 'DEVICE=new-device\nURMA_LIB=%q\nURMA_COMMON_LIB=%q\nPROVIDER_LIB=%q\nPROVIDER_DIR=%q\n' "$TEST_ROOT/vendor/liburma.so" "$TEST_ROOT/vendor/libcommon.so" "$TEST_ROOT/vendor/provider.so" "$TEST_ROOT/vendor"
} > "$TEST_ROOT/work/state/detected-umdk.env"
{
  cat "$TEST_ROOT/work/state/detected-umdk.env"
  printf 'DEVICE=old-device\nJNI_LIB_DIR=%q\n' "$TEST_ROOT/vendor"
} > "$TEST_ROOT/work/state/vendor-jni.env"
sha256sum "$TEST_ROOT/vendor/"*.so > "$TEST_ROOT/work/state/vendor-inputs.sha256"
bash -c 'source <(sed "/^action=/,\$d" "$0"); jni; [[ $DEVICE == new-device ]]' "$TEST_ROOT/tool/node.sh"
printf 'URMA_LIB=/different/vendor/liburma.so\n' >> "$TEST_ROOT/work/state/vendor-jni.env"
if bash -c 'source <(sed "/^action=/,\$d" "$0"); jni' "$TEST_ROOT/tool/node.sh" > "$TEST_ROOT/jni-mismatch.log" 2>&1; then
  echo 'mismatched JNI cache accepted'; exit 1
fi
grep -q 'cached JNI belongs to different vendor libraries' "$TEST_ROOT/jni-mismatch.log"

mkdir -p "$TEST_ROOT/work/runs/node-checks/events/mock"
printf '%s\n' \
  '{"Event":"SparkListenerTaskEnd", "Task Info":{"Host": "10.55.0.1"}, "Task End Reason":{"Reason":"Success"}}' \
  '{"Event":"SparkListenerTaskEnd", "Task Info":{"Host": "10.55.0.2"}, "Task End Reason":{"Reason":"Success"}}' \
  '{"Event":"SparkListenerApplicationEnd"}' > "$TEST_ROOT/work/runs/node-checks/events/mock/events"
bash "$TEST_ROOT/tool/node.sh" events mock

setsid env UB_TWO_NODE_SESSION=node-checks sleep 30 &
owned=$!
setsid sleep 30 &
unowned=$!
trap 'kill -TERM "$owned" "$unowned" 2>/dev/null || true; wait "$owned" "$unowned" 2>/dev/null || true' EXIT
printf '%s\n' "$owned" > "$TEST_ROOT/work/runs/node-checks/owned.pid"
printf '%s\n' "$unowned" > "$TEST_ROOT/work/runs/node-checks/unowned.pid"
sleep .2
if bash "$TEST_ROOT/tool/node.sh" stop > "$TEST_ROOT/stop.log" 2>&1; then echo 'unowned PID was not reported'; exit 1; fi
grep -q PID_NOT_OWNED "$TEST_ROOT/stop.log"
! kill -0 "$owned" 2>/dev/null
kill -0 "$unowned"
# An exited launcher must not hide a still-running child in its session group.
rm "$TEST_ROOT/work/runs/node-checks/unowned.pid"
setsid env UB_TWO_NODE_SESSION=node-checks bash -c 'sleep 30 & echo $! > "$1"' bash "$TEST_ROOT/orphan-child" &
orphan_leader=$!
wait "$orphan_leader"
printf '%s\n' "$orphan_leader" > "$TEST_ROOT/work/runs/node-checks/orphan.pid"
bash "$TEST_ROOT/tool/node.sh" stop > "$TEST_ROOT/orphan-stop.log" 2>&1
child=$(< "$TEST_ROOT/orphan-child")
[[ ! -e /proc/$child ]] || [[ $(ps -o stat= -p "$child") == Z* ]]
mkdir -p "$TEST_ROOT/work/runs/node-checks/worker/app-1"
printf 'executor diagnostic\n' > "$TEST_ROOT/work/runs/node-checks/worker/app-1/stdout"
printf 'redundant binary\n' > "$TEST_ROOT/work/runs/node-checks/worker/app-1/user.jar"
bash "$TEST_ROOT/tool/node.sh" evidence
bash "$TEST_ROOT/tool/node.sh" archive > "$TEST_ROOT/scoped-evidence.tar.gz"
tar -tzf "$TEST_ROOT/scoped-evidence.tar.gz" > "$TEST_ROOT/archive-list.txt"
grep -Fq 'runs/node-checks/executor-logs/worker/app-1/stdout' "$TEST_ROOT/archive-list.txt"
! grep -Fq 'runs/node-checks/worker/app-1/user.jar' "$TEST_ROOT/archive-list.txt"
for name in spark-worker scache-node; do
  setsid env UB_TWO_NODE_SESSION=node-checks bash -c '
    log=$1; label=$2
    trap '\''printf "%s\n" "$label" >> "$log"; exit 0'\'' TERM
    while :; do sleep .2; done
  ' bash "$TEST_ROOT/stop-order.log" "$name" &
  printf '%s\n' "$!" > "$TEST_ROOT/work/runs/node-checks/$name.pid"
done
sleep .2
bash "$TEST_ROOT/tool/node.sh" stop > "$TEST_ROOT/ordered-stop.log" 2>&1
printf 'spark-worker\nscache-node\n' | cmp - "$TEST_ROOT/stop-order.log"
setsid sleep 30 & foreign_worker=$!
setsid env UB_TWO_NODE_SESSION=node-checks bash -c '
  trap '\''exit 0'\'' TERM
  while :; do sleep .2; done
' & protected_scache=$!
printf '%s\n' "$foreign_worker" > "$TEST_ROOT/work/runs/node-checks/spark-worker.pid"
printf '%s\n' "$protected_scache" > "$TEST_ROOT/work/runs/node-checks/scache-node.pid"
sleep .2
if bash "$TEST_ROOT/tool/node.sh" stop-scache > "$TEST_ROOT/direct-scache-guard.log" 2>&1; then
  echo 'direct SCache stop accepted while a worker was live'; exit 1
fi
grep -q 'SCACHE_RETAINED: application group' "$TEST_ROOT/direct-scache-guard.log"
if bash "$TEST_ROOT/tool/node.sh" stop > "$TEST_ROOT/retain-scache.log" 2>&1; then
  echo 'SCache stopped despite an unowned Spark worker'; exit 1
fi
grep -q SCACHE_RETAINED "$TEST_ROOT/retain-scache.log"
kill -0 "$protected_scache"
rm -- "$TEST_ROOT/work/runs/node-checks/spark-worker.pid"
kill -TERM "$foreign_worker" 2>/dev/null || true
bash "$TEST_ROOT/tool/node.sh" stop > "$TEST_ROOT/stop-after-worker.log" 2>&1
setsid env UB_TWO_NODE_SESSION=node-checks sleep 30 & detached_app=$!
sleep .2
if bash "$TEST_ROOT/tool/node.sh" stop-scache > "$TEST_ROOT/detached-app.log" 2>&1; then
  echo 'SCache stop missed a live session application without a PID file'; exit 1
fi
grep -q 'SESSION_APPLICATION_STILL_LIVE' "$TEST_ROOT/detached-app.log"
kill -TERM "$detached_app" 2>/dev/null || true
wait "$detached_app" 2>/dev/null || true
printf 'invalid-pid\n' > "$TEST_ROOT/work/runs/node-checks/spark-worker.pid"
if bash "$TEST_ROOT/tool/node.sh" stop > "$TEST_ROOT/invalid-pid.log" 2>&1; then
  echo 'malformed PID file was accepted as stopped'; exit 1
fi
grep -q 'INVALID_PID_FILE' "$TEST_ROOT/invalid-pid.log"
echo "NODE_CHECKS_PASS logs=$TEST_ROOT"
