#!/usr/bin/env bash
set -euo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$R/build"
exec 9>"$R/build/logical-cluster.lock"
flock -n 9 || { echo ACTIVE_CLUSTER_OR_BUILD; exit 1; }
O="$R/repos/OpenURMA/integration/umdk"
B="$R/build/umdk"
OUT="$O/build/umq-functional"
mkdir -p "$OUT" "$B/urma/lib/urma/core/urma"
gcc -O2 -g -fPIC -Wall -Wextra -Werror -c "$O/provider/openurma_provider.c" \
 -I"$R/repos/umdk/src/urma/lib/urma/core/include" -I"$R/repos/umdk/src/urma/common/include" \
 -I"$O/provider" -o "$OUT/provider.o"
g++ -std=c++17 -O2 -g -fPIC -shared -Wall -Wextra -Werror -pthread \
 "$OUT/provider.o" "$O/provider/openurma_nic_functional.cpp" -I"$O/provider" \
 -L"$B/urma/lib/urma/core" -L"$B/urma/common" -lurma -lurma_common \
 -o "$OUT/liburma_openurma.so"
gcc -O2 -fPIC -shared -Wall -Wextra -Werror "$O/shim/openurma_shim.c" "$O/shim/openurma_ioctl_624.c" \
 -I"$R/repos/umdk/src/urma/lib/urma/core/include" -I"$R/repos/umdk/src/urma/lib/urma/core" \
 -I"$R/repos/umdk/src/urma/common/include" \
 -ldl -o "$OUT/openurma_shim.so"
ln -sfn "$OUT/liburma_openurma.so" "$B/urma/lib/urma/core/urma/liburma_openurma.so"
echo UMQ_RM_CTP_FUNCTIONAL_PROVIDER_BUILD_PASS
