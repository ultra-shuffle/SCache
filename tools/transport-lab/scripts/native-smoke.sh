#!/usr/bin/env bash
set -euo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec 9>"$R/build/logical-cluster.lock"
flock -n 9 || { echo ACTIVE_CLUSTER_OR_BUILD; exit 1; }
U="$R/repos/umdk/src/urpc"
g++ -std=c++17 -O2 -g -Wall -Wextra -Werror -pthread \
 -I"$U/include/umq" "$R/repos/SCache/native/spark_umq/umq_smoke.cpp" \
 -L"$R/build/umdk/urpc/umq" -L"$R/build/umdk/urpc/umq/qbuf" \
 -lumq -lumq_buf -o "$R/build/umq-native-smoke"
export LD_LIBRARY_PATH
LD_LIBRARY_PATH="$(find "$R/build/umdk" -name '*.so' -printf '%h\n' | sort -u | paste -sd :)"
export LD_PRELOAD="$R/repos/OpenURMA/integration/umdk/build/umq-functional/openurma_shim.so"
export OPENURMA_PROVIDER_LOG=summary
RUN=$(mktemp -d "$R/logs/umq-native-XXXXXX")
WIRE=$(mktemp -d /tmp/scache-umq-XXXXXX)
echo "RUN=$RUN"
for node in A B; do
 if [[ "$node" == A ]]; then dev=openurma0; eid=fe80::1; role=listen; else dev=openurma1; eid=fe80::2; role=connect; fi
 bash "$R/repos/OpenURMA/integration/umdk/tools/openurma_mkdev.sh" "$dev" "$eid" "$RUN/root-$node" >/dev/null
 env OPENURMA_FAKE_ROOT="$RUN/root-$node" OPENURMA_WIRE_ROLE="$role" OPENURMA_WIRE_PATH="$WIRE/wire" \
  "$R/build/umq-native-smoke" "$node" "$RUN" >"$RUN/$node.log" 2>&1 &
 if [[ "$node" == A ]]; then a_pid=$!; else b_pid=$!; fi
done
set +e
wait "$a_pid"; a_rc=$?
wait "$b_pid"; b_rc=$?
set -e
printf 'A_EXIT=%s B_EXIT=%s\n' "$a_rc" "$b_rc"
tail -12 "$RUN/A.log"
tail -12 "$RUN/B.log"
test "$a_rc" == 0 && test "$b_rc" == 0
grep -q OFFICIAL_UMQ_RM_CTP_READ_WRITE_PASS "$RUN/B.log"
grep -q SCACHE_UMQ_CLOSE_PASS "$RUN/A.log"
grep -q SCACHE_UMQ_CLOSE_PASS "$RUN/B.log"
echo UMQ_NATIVE_SMOKE_PASS
