#!/usr/bin/env bash
set -Eeuo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
B="$R/build/umdk"; O="$R/repos/OpenURMA/integration/umdk"
JDK=/home/xinzhang/codex-fullkit-runtime-20260918/toolchain/jdk-17-aarch64
RUN="$(mktemp -d "$R/logs/ssm-XXXXXX")"
mkdir -p "$RUN/a" "$RUN/b" "$RUN/conf"
cp "$R/runtime/package/bin/real-ub/log4j.properties" "$RUN/conf/"
export SCACHE_HOME="$RUN"
echo "RUN=$RUN"
CP="$R/repos/SCache/target/scala-2.13/SCache-assembly-0.1.0-SNAPSHOT.jar:$R/repos/SCache/target/scala-2.13/SCache-integration-0.1.0-SNAPSHOT.jar"
bash "$O/tools/openurma_mkdev.sh" openurma0 fe80::1 "$RUN/root-a"
bash "$O/tools/openurma_mkdev.sh" openurma1 fe80::2 "$RUN/root-b"
export LD_LIBRARY_PATH="$B/urpc/framework:$B/urma/lib/urma/core:$B/urma/common:$R/build/systemc-install/lib64"
export LD_PRELOAD="$O/build/tier_s/openurma_shim.so"
export OPENURMA_WIRE_PATH="$RUN/wire" OPENURMA_PROVIDER_LOG=0
export SCACHE_URPC_LIBRARY="$R/build/libScacheUrpc.so"
JAVA=("$JDK/bin/java" -Xmx1g --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -cp "$CP")
OPENURMA_FAKE_ROOT="$RUN/root-a" OPENURMA_WIRE_ROLE=listen \
 SCACHE_URPC_EID=fe80::1 SCACHE_URPC_HOST=127.0.0.1 SCACHE_URPC_PORT=25184 SCACHE_URPC_SOCKET_DIR="$RUN/a" \
 timeout --signal=TERM --kill-after=3 120 "${JAVA[@]}" org.scache.network.ub.UrpcServiceNode \
 A "$RUN/work-a" openurma0 0 0 127.0.0.1 25200 127.0.0.2 25202 >"$RUN/A.log" 2>&1 &
A_PID=$!
B_PID=
trap 'kill "$A_PID" ${B_PID:+"$B_PID"} 2>/dev/null || true; wait "$A_PID" ${B_PID:+"$B_PID"} 2>/dev/null || true' EXIT
for ((i=0;i<200;i++)); do
 grep -q RM_CTP_SERVICE_READY "$RUN/A.log" && break
 kill -0 "$A_PID" 2>/dev/null || { cat "$RUN/A.log"; exit 1; }
 sleep 0.1
done
grep -q RM_CTP_SERVICE_READY "$RUN/A.log" || { cat "$RUN/A.log"; exit 1; }
OPENURMA_FAKE_ROOT="$RUN/root-b" OPENURMA_WIRE_ROLE=connect \
 SCACHE_URPC_EID=fe80::2 SCACHE_URPC_HOST=127.0.0.2 SCACHE_URPC_PORT=25185 SCACHE_URPC_SOCKET_DIR="$RUN/b" \
 timeout --signal=TERM --kill-after=3 120 "${JAVA[@]}" org.scache.network.ub.UrpcServiceNode \
 B "$RUN/work-b" openurma1 0 0 127.0.0.2 25202 127.0.0.1 25200 >"$RUN/B.log" 2>&1 &
B_PID=$!
set +e
wait "$A_PID"; A_RC=$?
wait "$B_PID"; B_RC=$?
set -e
echo "A_EXIT=$A_RC B_EXIT=$B_RC"
tail -65 "$RUN/A.log"; tail -65 "$RUN/B.log"
[[ "$A_RC" == 0 && "$B_RC" == 0 ]] || exit 1
grep -q URPC_SERVICE_PASS "$RUN/A.log"
grep -q URPC_SERVICE_PASS "$RUN/B.log"
grep -q SCACHE_URPC_CLOSE_PASS "$RUN/A.log"
grep -q SCACHE_URPC_CLOSE_PASS "$RUN/B.log"
echo SCACHE_URPC_TWO_JVM_SERVICE_PASS
