#!/usr/bin/env bash
set -euo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec 9>"$R/build/logical-cluster.lock"
flock -n 9 || { echo "A cluster/build is active; do not overwrite its binaries"; exit 1; }

B="$R/build/umdk"
O="$R/repos/OpenURMA/integration/umdk"
SYS="$R/build/systemc-install"
OUT="$O/build/tier_s"
mkdir -p "$OUT"
# SystemC topology/facade are built once by build_provider.sh NIC_IMPL=sc.
test -f "$OUT/openurma_nic.o"
gcc -O2 -g -fPIC -Wall -Wextra -Werror -c -o "$OUT/openurma_provider.o" \
  "$O/provider/openurma_provider.c" -I"$R/repos/umdk/src/urma/lib/urma/core/include" \
  -I"$R/repos/umdk/src/urma/common/include" -I"$O/provider"
g++ -shared -Wl,--build-id=sha1 -o "$OUT/liburma_openurma.so" \
  "$OUT/openurma_provider.o" "$OUT/openurma_nic.o" \
  -L"$B/urma/lib/urma/core" -L"$B/urma/common" -L"$SYS/lib64" \
  -lurma -lurma_common -lstdc++ -lsystemc -lpthread
mkdir -p "$B/urma/lib/urma/core/urma"
ln -sfn "$OUT/liburma_openurma.so" "$B/urma/lib/urma/core/urma/liburma_openurma.so"
echo SIM_PROVIDER_BUILD_PASS
