#!/usr/bin/env bash
set -euo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
B="$R/build/umdk"
O="$R/repos/OpenURMA/integration/umdk"
RUN="$(mktemp -d "$R/logs/fsm-XXXXXX")"
mkdir -p "$RUN/a" "$RUN/b"
echo "RUN=$RUN"
bash "$O/tools/openurma_mkdev.sh" openurma0 fe80::1 "$RUN/root-a"
bash "$O/tools/openurma_mkdev.sh" openurma1 fe80::2 "$RUN/root-b"
export LD_LIBRARY_PATH="$B/urpc/framework:$B/urma/lib/urma/core:$B/urma/common:$R/build/systemc-install/lib64"
export LD_PRELOAD="$O/build/tier_s/openurma_shim.so"
export OPENURMA_WIRE_PATH="$RUN/wire"
export OPENURMA_PROVIDER_LOG=1
EX="$B/urpc/examples/framework/urpc_framework_example"
PORT="${URPC_SMOKE_PORT:-25175}"
OPENURMA_FAKE_ROOT="$RUN/root-a" OPENURMA_WIRE_ROLE=listen \
  timeout --signal=TERM --kill-after=3 45 stdbuf -oL -eL "$EX" --server -d openurma0 --assign_mode 2 --eid fe80::1 -T 1 -e 0 -p "$PORT" -i 127.0.0.1 -F "$RUN/a" >"$RUN/server.log" 2>&1 &
SERVER=$!
trap 'kill "$SERVER" 2>/dev/null || true; wait "$SERVER" 2>/dev/null || true' EXIT
sleep 1
set +e
OPENURMA_FAKE_ROOT="$RUN/root-b" OPENURMA_WIRE_ROLE=connect \
  timeout --signal=TERM --kill-after=3 30 stdbuf -oL -eL "$EX" --client -d openurma1 --assign_mode 2 --eid fe80::2 -T 1 -e 0 -p "$PORT" -i 127.0.0.1 -F "$RUN/b" >"$RUN/client.log" 2>&1
RC=$?
set -e
echo "CLIENT_EXIT=$RC"
tail -40 "$RUN/server.log"
tail -60 "$RUN/client.log"
if [[ "$RC" != 0 ]] || ! grep -q 'rsp: hello client!' "$RUN/client.log"; then
  echo OFFICIAL_URPC_FRAMEWORK_SMOKE_FAIL
  exit 1
fi
echo OFFICIAL_URPC_FRAMEWORK_SMOKE_PASS
