#!/usr/bin/env bash
set -Eeuo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd -P)
TEST_ROOT=$(mktemp -d /tmp/ub-two-node-control-flow-XXXXXX)
mkdir -p "$TEST_ROOT/runner/lib" "$TEST_ROOT/bin"
cp "$HERE/run-stage.sh" "$TEST_ROOT/runner/"
cp "$HERE/tests/fake-node.sh" "$TEST_ROOT/runner/lib/node.sh"
cp "$HERE/lib/path-policy.sh" "$TEST_ROOT/runner/lib/"
cp "$HERE/tests/fake-ssh.sh" "$TEST_ROOT/bin/ssh"
chmod +x "$TEST_ROOT/bin/ssh"
export PATH="$TEST_ROOT/bin:$PATH"
config() {
  local name=$1 mode=$2 extra=${3:-}
  {
    printf 'B_SSH=mock-b\nSSH_OPTIONS=(-p 10000 -o BatchMode=yes)\nA_IP=10.55.0.1\nB_IP=10.55.0.2\n'
    printf 'A_PACKAGE=/mock/package-a\nB_PACKAGE=/mock/package-b\nA_WORK=%q\nB_WORK=%q\n' "$TEST_ROOT/$name/A" "$TEST_ROOT/$name/B"
    printf 'A_DEVICE=bonding_dev_0\nB_DEVICE=bonding_dev_0\nA_PROVIDER=/mock/a.so\nB_PROVIDER=/mock/b.so\nA_EID=0\nB_EID=0\nA_PRIORITY=0\nB_PRIORITY=0\n'
    printf 'MODE=%q\nNATIVE_EXTRA_ARGS=(%q)\n' "$mode" "$extra"
    printf 'NATIVE_SIZES=(4096)\nNATIVE_TIMEOUT=2\nNATIVE_PORT=19190\nARENA_MIB=128\nBENCH_ARENA_MIB=2048\nAPP_TIMEOUT=10\nSHARED_ROOT=/mock/shared\n'
  } > "$TEST_ROOT/$name.env"
}
run_case() {
  local name=$1 phase=$2 mode=$3 expect=$4 extra=${5:-} rc=0
  config "$name" "$mode" "$extra"
  bash "$TEST_ROOT/runner/run-stage.sh" "$phase" --config "$TEST_ROOT/$name.env" > "$TEST_ROOT/$name.log" 2>&1 || rc=$?
  if [[ $expect == pass ]]; then
    ((rc==0)) || { cat "$TEST_ROOT/$name.log"; exit 1; }
    grep -q "TWO_NODE_STAGE_PASS phase=$phase" "$TEST_ROOT/$name.log"
  else
    ((rc!=0)) || { echo "unexpected pass $name"; exit 1; }
    grep -q "TWO_NODE_STAGE_FAIL phase=$phase" "$TEST_ROOT/$name.log"
  fi
  for host in A B; do
    action_file=$(find "$TEST_ROOT/$name/$host/runs" -name actions.log -print -quit)
    [[ -n $action_file ]]
    grep -q "$host stop" "$action_file"
  done
  echo "CONTROL_FLOW_CASE_PASS $name"
}
run_quarantine_case() {
  local name=$1 phase=$2 extra=$3 rc=0 host actions
  config "$name" rm-ctp "$extra"
  bash "$TEST_ROOT/runner/run-stage.sh" "$phase" --config "$TEST_ROOT/$name.env" > "$TEST_ROOT/$name.log" 2>&1 || rc=$?
  ((rc!=0)) || { echo "unexpected pass $name"; exit 1; }
  grep -q "TWO_NODE_STAGE_FAIL phase=$phase" "$TEST_ROOT/$name.log"
  grep -q 'DMA_UNRESOLVED_QUARANTINE: retaining both endpoint processes' "$TEST_ROOT/$name.log"
  for host in A B; do
    actions=$(find "$TEST_ROOT/$name/$host/runs" -name actions.log -print -quit)
    [[ -n $actions ]]
    ! grep -q "$host stop" "$actions"
    ! grep -q "$host release" "$actions"
  done
  echo "CONTROL_FLOW_CASE_PASS $name"
}
run_case shared_fs 05 rm-ctp pass
grep -q SHARED_FILESYSTEM_TWO_NODE_PASS "$TEST_ROOT/shared_fs.log"
! grep -q 'STEP_START node=A action=detect' "$TEST_ROOT/shared_fs.log"
run_case detection 10 rm-ctp pass
grep -q 'PEER_ENDPOINT node=A device=mock-A' "$TEST_ROOT/detection.log"
grep -q 'PEER_ENDPOINT node=B device=mock-B' "$TEST_ROOT/detection.log"
run_case same_eid_warning 10 rm-ctp pass mock-same-eid
grep -q 'PEER_EID_DUPLICATE_WARNING' "$TEST_ROOT/same_eid_warning.log"
run_case priority_warning 10 rm-ctp pass mock-priority-diff
grep -q 'PEER_PRIORITY_DIFFERENCE_WARNING' "$TEST_ROOT/priority_warning.log"
run_case build 20 rm-ctp pass
run_case native 30 rm-ctp pass
[[ $(awk '/^result=/{exit} /^NATIVE_PAIR_PASS/{n++} END{print n+0}' "$TEST_ROOT/native.log") == 4 ]]
grep -q '^NATIVE_PAIR_PASS direction=AB operation=read_bw .*initiator=B target=A payload_from=A payload_to=B$' "$TEST_ROOT/native.log"
grep -q '^NATIVE_PAIR_PASS direction=AB operation=write_bw .*initiator=B target=A payload_from=B payload_to=A$' "$TEST_ROOT/native.log"
grep -q '^NATIVE_PAIR_PASS direction=BA operation=read_bw .*initiator=A target=B payload_from=B payload_to=A$' "$TEST_ROOT/native.log"
grep -q '^NATIVE_PAIR_PASS direction=BA operation=write_bw .*initiator=A target=B payload_from=A payload_to=B$' "$TEST_ROOT/native.log"
config application_native rm-ctp mock-app-limit-b
printf 'NATIVE_SIZES=(4096 application)\n' >> "$TEST_ROOT/application_native.env"
bash "$TEST_ROOT/runner/run-stage.sh" 30 --config "$TEST_ROOT/application_native.env" > "$TEST_ROOT/application_native.log" 2>&1
grep -q 'NATIVE_APPLICATION_SIZE size=32768 endpoint_A=65536 endpoint_B=32768' "$TEST_ROOT/application_native.log"
grep -q 'NATIVE_PORT_RANGE start=19190 end=19197 sizes=4096 32768' "$TEST_ROOT/application_native.log"
[[ $(awk '/^result=/{exit} /^NATIVE_PAIR_PASS/{n++} END{print n+0}' "$TEST_ROOT/application_native.log") == 8 ]]
echo 'CONTROL_FLOW_CASE_PASS application_native'
config application_native_dedup rm-ctp mock-app-limit-4k
printf 'NATIVE_SIZES=(4096 application)\n' >> "$TEST_ROOT/application_native_dedup.env"
bash "$TEST_ROOT/runner/run-stage.sh" 30 --config "$TEST_ROOT/application_native_dedup.env" > "$TEST_ROOT/application_native_dedup.log" 2>&1
grep -q 'NATIVE_APPLICATION_SIZE size=4096 endpoint_A=4096 endpoint_B=4096' "$TEST_ROOT/application_native_dedup.log"
grep -q 'NATIVE_PORT_RANGE start=19190 end=19193 sizes=4096' "$TEST_ROOT/application_native_dedup.log"
[[ $(awk '/^result=/{exit} /^NATIVE_PAIR_PASS/{n++} END{print n+0}' "$TEST_ROOT/application_native_dedup.log") == 4 ]]
echo 'CONTROL_FLOW_CASE_PASS application_native_dedup'
run_case rm_scache 40 rm-ctp pass
grep -q 'STEP_START node=A action=scache-master' "$TEST_ROOT/rm_scache.log"
grep -Fq 'REGISTER dualsmoke 0 0 2 2' "$(find "$TEST_ROOT/rm_scache/A/runs" -name actions.log -print -quit)"
grep '^CLEANUP_START ' "$TEST_ROOT/rm_scache.log" | sed 's/^CLEANUP_START //' | \
  cmp - <(printf 'node=A action=stop-app\nnode=B action=stop-app\nnode=A action=app-quiescent\nnode=B action=app-quiescent\nnode=A action=stop-scache\nnode=B action=stop-scache\n')
echo 'CONTROL_FLOW_CASE_PASS global_stop_order'
run_quarantine_case one_sided_transfer 40 mock-one-sided
grep -q 'bidirectional transfer did not exercise URMA/destination pool on node B' "$TEST_ROOT/one_sided_transfer.log"
config dma_unresolved rm-ctp mock-dma-unresolved
if bash "$TEST_ROOT/runner/run-stage.sh" 40 --config "$TEST_ROOT/dma_unresolved.env" > "$TEST_ROOT/dma_unresolved.log" 2>&1; then
  echo 'unresolved DMA was accepted'; exit 1
fi
grep -q 'DMA_UNRESOLVED_QUARANTINE: retaining both endpoint processes' "$TEST_ROOT/dma_unresolved.log"
for host in A B; do
  actions=$(find "$TEST_ROOT/dma_unresolved/$host/runs" -name actions.log -print -quit)
  grep -q "$host dma-unresolved" "$actions"
  ! grep -q "$host stop" "$actions"
  ! grep -q "$host release" "$actions"
done
echo 'CONTROL_FLOW_CASE_PASS dma_unresolved'
run_quarantine_case app_timeout 50 mock-app-timeout
grep -q 'STEP_FAIL node=A action=smoke exit_code=124' "$TEST_ROOT/app_timeout.log"
quarantined=$(find "$TEST_ROOT/app_timeout/A/logs" -name dma-quarantine.txt -print -quit)
[[ -n $quarantined ]]
session=${quarantined%/dma-quarantine.txt}; session=${session##*/}
if bash "$HERE/90-stop.sh" --config "$TEST_ROOT/app_timeout.env" --session "$session" > "$TEST_ROOT/quarantined-stop.log" 2>&1; then
  echo '90-stop accepted quarantined session without provider clearance'; exit 1
fi
grep -q 'REFUSE_QUARANTINED_SESSION' "$TEST_ROOT/quarantined-stop.log"
echo 'CONTROL_FLOW_CASE_PASS quarantined_manual_stop_guard'
exec 8>"$TEST_ROOT/app_timeout/A/state/orchestrator.lock"
flock -n 8
if bash "$HERE/90-stop.sh" --config "$TEST_ROOT/app_timeout.env" --session "$session" --provider-cleared \
  > "$TEST_ROOT/active-orchestrator-stop.log" 2>&1; then
  echo 'manual stop raced an active stage controller'; exit 1
fi
grep -q 'REFUSE_ACTIVE_ORCHESTRATOR' "$TEST_ROOT/active-orchestrator-stop.log"
flock -u 8
exec 8>&-
echo 'CONTROL_FLOW_CASE_PASS active_orchestrator_stop_guard'
bash "$HERE/90-stop.sh" --config "$TEST_ROOT/app_timeout.env" --session "$session" --provider-cleared \
  > "$TEST_ROOT/provider-cleared-stop.log" 2>&1
grep -q "SESSION_STOPPED $session" "$TEST_ROOT/provider-cleared-stop.log"
grep -q 'PROVIDER_CLEARANCE_ASSERTED' "$TEST_ROOT/provider-cleared-stop.log"
grep '^CLEANUP_START ' "$TEST_ROOT/provider-cleared-stop.log" | sed 's/^CLEANUP_START //' | \
  cmp - <(printf 'node=A action=stop-app\nnode=B action=stop-app\nnode=A action=app-quiescent\nnode=B action=app-quiescent\nnode=A action=stop-scache\nnode=B action=stop-scache\n')
echo 'CONTROL_FLOW_CASE_PASS provider_cleared_stop'
cp "$TEST_ROOT/app_timeout.env" "$TEST_ROOT/broad-stop.env"
printf 'A_WORK=/root\n' >> "$TEST_ROOT/broad-stop.env"
if bash "$HERE/90-stop.sh" --config "$TEST_ROOT/broad-stop.env" --session "$session" --provider-cleared \
  > "$TEST_ROOT/broad-stop.log" 2>&1; then
  echo '90-stop accepted a broad workspace'; exit 1
fi
grep -q 'REFUSE_BROAD_WORKSPACE' "$TEST_ROOT/broad-stop.log"
echo 'CONTROL_FLOW_CASE_PASS broad_manual_stop_guard'
run_case rc_scache 40 rc-rtp pass
run_case rc_smoke 50 rc-rtp pass
run_case standard 60 rm-ctp pass
! grep -q 'STEP_START node=A action=detect' "$TEST_ROOT/standard.log"
for workload in wordcount sort terasort repartition tpcds; do
  grep -Fq "ACTION node=A action=bench args=standard $workload" "$TEST_ROOT/standard.log"
done
for host in A B; do
  grep -q "STEP_START node=$host action=scache-standard-node" "$TEST_ROOT/standard.log"
  grep -q "STEP_START node=$host action=topology" "$TEST_ROOT/standard.log"
done
grep -q SCACHE_NETTY_TWO_NODE_PASS "$TEST_ROOT/standard.log"
grep -Fq 'REGISTER standarddual 0 0 2 2' "$(find "$TEST_ROOT/standard/A/runs" -name actions.log -print -quit)"
run_case integrated_scache 65 rm-ctp pass
grep -q SPARK_SCACHE_NETTY_TWO_NODE_PASS "$TEST_ROOT/integrated_scache.log"
run_case integrated_benchmarks 66 rm-ctp pass
grep -q 'BENCHMARK_TWO_NODE_PASS backend=scache-netty' "$TEST_ROOT/integrated_benchmarks.log"
for workload in wordcount sort terasort repartition tpcds; do
  grep -Fq "ACTION node=A action=bench args=scache-netty $workload" "$TEST_ROOT/integrated_benchmarks.log"
done
grep -q SPARK_SCACHE_NETTY_BENCHMARKS_PASS "$TEST_ROOT/integrated_benchmarks.log"
run_case rc_bench 70 rc-rtp pass
grep -q 'BENCHMARK_TWO_NODE_PASS backend=ub' "$TEST_ROOT/rc_bench.log"
run_case native_failure 30 rm-ctp fail mock-native-fail
grep -q MOCK_IMPORT_JETTY_FAILURE "$TEST_ROOT/native_failure.log"
run_case cleanup_failure 10 rm-ctp fail mock-stop-fail
run_case peer_worker_stop_failure 10 rm-ctp fail mock-stop-b-app-fail
! grep -q 'action=stop-scache' "$TEST_ROOT/peer_worker_stop_failure.log"
grep -q 'SCACHE_RETAINED: at least one endpoint application group' "$TEST_ROOT/peer_worker_stop_failure.log"
echo 'CONTROL_FLOW_CASE_PASS peer_worker_retain_scache'
run_case peer_detached_app 10 rm-ctp fail mock-b-detached-app
! grep -q 'action=stop-scache' "$TEST_ROOT/peer_detached_app.log"
grep -q 'SCACHE_RETAINED: at least one endpoint application group' "$TEST_ROOT/peer_detached_app.log"
echo 'CONTROL_FLOW_CASE_PASS peer_detached_app_retain_scache'
run_quarantine_case missing_metric 40 mock-missing-metric
grep -q 'INVALID_OR_MISSING_METRIC urma.arenaFreeBytes' "$TEST_ROOT/missing_metric.log"
run_case rm_smoke 50 rm-ctp pass
run_case rm_bench 70 rm-ctp pass
grep -q 'BENCHMARK_TWO_NODE_PASS backend=ub' "$TEST_ROOT/rm_bench.log"
for workload in wordcount sort terasort repartition tpcds; do
  grep -Fq "ACTION node=A action=bench args=ub $workload" "$TEST_ROOT/rm_bench.log"
done
run_case both_detection_fail 10 rm-ctp fail mock-detect-fail
grep -q 'STEP_FAIL node=A action=detect exit_code=42' "$TEST_ROOT/both_detection_fail.log"
grep -q 'STEP_FAIL node=B action=detect exit_code=42' "$TEST_ROOT/both_detection_fail.log"
run_case sdk_mismatch 10 rm-ctp fail mock-sdk-mismatch
grep -q 'vendor UMDK core/devel versions differ' "$TEST_ROOT/sdk_mismatch.log"
config whitespace_path rm-ctp
printf 'A_WORK=%q\n' "$TEST_ROOT/work with space" >> "$TEST_ROOT/whitespace_path.env"
if bash "$TEST_ROOT/runner/run-stage.sh" 10 --config "$TEST_ROOT/whitespace_path.env" > "$TEST_ROOT/whitespace_path.log" 2>&1; then
  echo 'whitespace path was accepted'; exit 1
fi
grep -q 'A_WORK contains whitespace' "$TEST_ROOT/whitespace_path.log"
echo 'CONTROL_FLOW_CASE_PASS whitespace_path'
config broad_workspace rm-ctp
printf 'A_WORK=/root\n' >> "$TEST_ROOT/broad_workspace.env"
if bash "$TEST_ROOT/runner/run-stage.sh" 10 --config "$TEST_ROOT/broad_workspace.env" > "$TEST_ROOT/broad_workspace.log" 2>&1; then
  echo 'broad workspace was accepted'; exit 1
fi
grep -q 'must be a dedicated subdirectory' "$TEST_ROOT/broad_workspace.log"
echo 'CONTROL_FLOW_CASE_PASS broad_workspace'
config missing_native_array rm-ctp
printf 'unset NATIVE_SIZES\n' >> "$TEST_ROOT/missing_native_array.env"
if bash "$TEST_ROOT/runner/run-stage.sh" 10 --config "$TEST_ROOT/missing_native_array.env" > "$TEST_ROOT/missing_native_array.log" 2>&1; then
  echo 'missing native sizes was accepted'; exit 1
fi
grep -q 'NATIVE_SIZES must be an array' "$TEST_ROOT/missing_native_array.log"
echo 'CONTROL_FLOW_CASE_PASS missing_native_array'
# The native prerequisite must not silently test an API/context different from
# the one the current SCache JNI actually implements. No hardware is simulated
# here: this checks controller argument validation before SSH or process launch.
for flag in -b --simplex_mode -f --use_flat_api --tp_aware --enable_user_tp \
    --async_import --order_type=3 --sub_trans_mode=1 -K1 --token_policy=1 \
    --aggr_mode=balance --bond_mode=balance --bond_level=port \
    --simplex --order=3 --bond_l=port --tp_reu; do
  config native_profile_override rm-ctp "$flag"
  if bash "$TEST_ROOT/runner/run-stage.sh" 30 --config "$TEST_ROOT/native_profile_override.env" \
      > "$TEST_ROOT/native_profile_override.log" 2>&1; then
    echo "native prerequisite accepted a different application profile: $flag"; exit 1
  fi
  grep -q 'NATIVE_EXTRA_ARGS changes the SCache link profile' "$TEST_ROOT/native_profile_override.log"
  [[ ! -d "$TEST_ROOT/native_profile_override/A" ]]
done
echo 'CONTROL_FLOW_CASE_PASS native_application_profile'
report=$(find "$TEST_ROOT/rm_bench/A/logs" -name REPORT.txt -print -quit)
grep -q 'result=PASS' "$report"
echo "CONTROL_FLOW_TESTS_PASS logs=$TEST_ROOT"
