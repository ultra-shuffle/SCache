#!/usr/bin/env bash
set -euo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
B="$R/build/umdk"; O="$R/repos/OpenURMA/integration/umdk"
RUN="$(mktemp -d "$R/logs/bsm-XXXXXX")"
mkdir -p "$RUN/a" "$RUN/b"
echo "RUN=$RUN"
bash "$O/tools/openurma_mkdev.sh" openurma0 fe80::1 "$RUN/root-a"
bash "$O/tools/openurma_mkdev.sh" openurma1 fe80::2 "$RUN/root-b"
export LD_LIBRARY_PATH="$B/urpc/framework:$B/urma/lib/urma/core:$B/urma/common:$R/build/systemc-install/lib64"
export LD_PRELOAD="$O/build/tier_s/openurma_shim.so"
export OPENURMA_WIRE_PATH="$RUN/wire"
export OPENURMA_PROVIDER_LOG=1
EX="$R/build/urpc-bridge-smoke"
OPENURMA_FAKE_ROOT="$RUN/root-a" OPENURMA_WIRE_ROLE=listen \
 timeout --signal=TERM --kill-after=3 35 stdbuf -oL -eL "$EX" server fe80::1 25178 "$RUN/a" "$RUN/peer" >"$RUN/server.log" 2>&1 &
SERVER=$!
trap 'kill "$SERVER" 2>/dev/null || true; wait "$SERVER" 2>/dev/null || true' EXIT
for ((i=0;i<100;i++)); do
 [[ ! -s "$RUN/peer" ]] || break
 kill -0 "$SERVER" 2>/dev/null || { cat "$RUN/server.log"; exit 1; }
 sleep 0.1
done
if [[ ! -s "$RUN/peer" ]]; then cat "$RUN/server.log"; exit 1; fi
set +e
OPENURMA_FAKE_ROOT="$RUN/root-b" OPENURMA_WIRE_ROLE=connect \
 timeout --signal=TERM --kill-after=3 25 stdbuf -oL -eL "$EX" client fe80::2 25179 "$RUN/b" "$RUN/peer" >"$RUN/client.log" 2>&1
RC=$?
wait "$SERVER"
SERVER_RC=$?
set -e
echo "CLIENT_EXIT=$RC SERVER_EXIT=$SERVER_RC"
tail -40 "$RUN/server.log"; tail -60 "$RUN/client.log"
if [[ "$RC" != 0 ]] || ! grep -q SCACHE_OFFICIAL_URPC_READ_WRITE_PASS "$RUN/client.log"; then exit 1; fi
if [[ "$SERVER_RC" != 0 ]] ||
 ! grep -q SCACHE_URPC_CLOSE_PASS "$RUN/client.log" ||
 ! grep -q SCACHE_URPC_CLOSE_PASS "$RUN/server.log"; then
 echo URPC_BRIDGE_LIFECYCLE_FAIL; exit 1
fi
echo URPC_BRIDGE_DATA_SMOKE_PASS
echo URPC_BRIDGE_LIFECYCLE_PASS
