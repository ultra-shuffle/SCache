#!/usr/bin/env bash
set -Eeuo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
B="$R/build/umdk"; O="$R/repos/OpenURMA/integration/umdk"
JDK=/home/xinzhang/codex-fullkit-runtime-20260918/toolchain/jdk-17-aarch64
RUN="$(mktemp -d "$R/logs/jsm-XXXXXX")"
mkdir -p "$RUN/a" "$RUN/b" "$R/build/probe-classes"
echo "RUN=$RUN"
"$JDK/bin/javac" -cp "$R/repos/SCache/target/scala-2.13/classes" \
 -d "$R/build/probe-classes" "$R/repos/SCache/src/integration/java/org/scache/network/ub/UrpcTransportProbe.java"
bash "$O/tools/openurma_mkdev.sh" openurma0 fe80::1 "$RUN/root-a"
bash "$O/tools/openurma_mkdev.sh" openurma1 fe80::2 "$RUN/root-b"
export LD_LIBRARY_PATH="$B/urpc/framework:$B/urma/lib/urma/core:$B/urma/common:$R/build/systemc-install/lib64"
export LD_PRELOAD="$O/build/tier_s/openurma_shim.so"
export OPENURMA_WIRE_PATH="$RUN/wire" OPENURMA_PROVIDER_LOG=1
export SCACHE_URPC_LIBRARY="${URPC_PROBE_NATIVE:-$R/build/libScacheUrpc.so}" SCACHE_URPC_HOST=127.0.0.1
CP="${URPC_PROBE_CLASSPATH_PREFIX:+$URPC_PROBE_CLASSPATH_PREFIX:}$R/build/probe-classes:$R/repos/SCache/target/scala-2.13/classes"
OPENURMA_FAKE_ROOT="$RUN/root-a" OPENURMA_WIRE_ROLE=listen \
 SCACHE_URPC_EID=fe80::1 SCACHE_URPC_PORT=25182 SCACHE_URPC_SOCKET_DIR="$RUN/a" \
 timeout --signal=TERM --kill-after=3 45 "$JDK/bin/java" -cp "$CP" \
 org.scache.network.ub.UrpcTransportProbe server "$RUN/peer" >"$RUN/server.log" 2>&1 &
SERVER=$!
trap 'kill "$SERVER" 2>/dev/null || true; wait "$SERVER" 2>/dev/null || true' EXIT
for ((i=0;i<100;i++)); do
 [[ ! -s "$RUN/peer" ]] || break
 kill -0 "$SERVER" 2>/dev/null || { cat "$RUN/server.log"; exit 1; }
 sleep 0.1
done
[[ -s "$RUN/peer" ]] || { cat "$RUN/server.log"; exit 1; }
set +e
OPENURMA_FAKE_ROOT="$RUN/root-b" OPENURMA_WIRE_ROLE=connect \
 SCACHE_URPC_EID=fe80::2 SCACHE_URPC_PORT=25183 SCACHE_URPC_SOCKET_DIR="$RUN/b" \
 timeout --signal=TERM --kill-after=3 40 "$JDK/bin/java" -cp "$CP" \
 org.scache.network.ub.UrpcTransportProbe client "$RUN/peer" >"$RUN/client.log" 2>&1
RC=$?
wait "$SERVER"
SERVER_RC=$?
set -e
echo "CLIENT_EXIT=$RC SERVER_EXIT=$SERVER_RC"
tail -40 "$RUN/server.log"; tail -60 "$RUN/client.log"
[[ "$RC" == 0 && "$SERVER_RC" == 0 ]] || exit 1
grep -q URPC_JAVA_DATA_PASS "$RUN/client.log"
grep -q URPC_JAVA_CLOSE_PASS "$RUN/client.log"
grep -q URPC_JAVA_CLOSE_PASS "$RUN/server.log"
echo URPC_JNI_FUNCTIONAL_AND_LIFECYCLE_PASS
