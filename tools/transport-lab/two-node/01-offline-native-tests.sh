#!/usr/bin/env bash
# Rebuild and run JNI/native contract tests using only bundled ARM tools and reference UMDK.
# No device is opened; this does not validate a vendor provider or UB hardware.
set -Eeuo pipefail
if [[ ${UB_OFFLINE_NATIVE_CLEAN_ENV:-} != 1 ]]; then
  exec /usr/bin/env -i UB_OFFLINE_NATIVE_CLEAN_ENV=1 HOME="${HOME:-/tmp}" \
    LANG=C.UTF-8 LC_ALL=C.UTF-8 PATH=/usr/bin:/bin /bin/bash "$0" "$@"
fi
HERE=$(cd -- "$(dirname -- "$0")" && pwd -P)
source "$HERE/lib/path-policy.sh"
PKG=$(cd -- "$HERE/../.." && pwd -P)
WORK=
while (($#)); do
  case "$1" in
    --workspace) WORK=${2:?}; shift 2 ;;
    -h|--help) echo "Usage: bash $0 --workspace /absolute/work"; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ $WORK == /* ]] || { echo 'Specify an absolute --workspace' >&2; exit 2; }
[[ $PKG != *[[:space:]]* && $WORK != *[[:space:]]* ]] || {
  echo 'UNSUPPORTED_WHITESPACE_PATH: bundled CMake/GCC requires package and workspace paths without whitespace' >&2; exit 2;
}
validate_workspace_paths local "$PKG" "$WORK" || exit 2
[[ $(uname -m) == aarch64 ]] || { echo 'Linux aarch64 required' >&2; exit 2; }
mkdir -p "$WORK"
RUN=$(mktemp -d "$WORK/native-contracts-$(date +%Y%m%d-%H%M%S)-XXXXXX")
TOOL="$PKG/dependencies/native-toolchain"
JNI="$PKG/sources/scache-jni"
REF="$PKG/dependencies/umdk-reference"
URMA_INC="$REF/include/lib/urma/core/include"
URMA_LIB="$REF/build/urma/lib/urma/core/liburma.so"
COMMON_LIB="$REF/build/urma/common/liburma_common.so"
export JAVA_HOME="$PKG/dependencies/jdk"
export PATH="$TOOL/usr/bin:/usr/bin:/bin"
export LD_LIBRARY_PATH="$REF/build/urma/lib/urma/core:$REF/build/urma/common:$TOOL/usr/lib64:$TOOL/usr/lib:$TOOL/lib64:$TOOL/lib"
export LIBRARY_PATH="$TOOL/usr/lib64:$TOOL/usr/lib:$TOOL/lib64:$TOOL/lib"
for f in "$TOOL/usr/bin/gcc" "$TOOL/usr/bin/g++" "$TOOL/usr/bin/cmake" \
    "$TOOL/usr/bin/ctest" "$URMA_INC/urma_api.h" "$URMA_LIB" "$COMMON_LIB"; do
  [[ -e $f ]] || { echo "Missing bundled test dependency: $f" >&2; exit 1; }
done
CFLAGS="-nostdinc -isystem $TOOL/usr/lib/gcc/aarch64-openEuler-linux/12/include -isystem $TOOL/usr/include"
CXXFLAGS="-nostdinc -nostdinc++ -isystem $TOOL/usr/include/c++/12 -isystem $TOOL/usr/include/c++/12/aarch64-openEuler-linux -isystem $TOOL/usr/include/c++/12/backward -isystem $TOOL/usr/lib/gcc/aarch64-openEuler-linux/12/include -isystem $TOOL/usr/include"
status=FAIL
finish() {
  rc=$?
  printf 'result=%s\nexit_code=%s\nhardware_exercised=false\nreference_umdk=true\nlog=%s\n' \
    "$status" "$rc" "$RUN/native-contracts.log" > "$RUN/status.txt"
  [[ $status == PASS ]] || { echo '===== NATIVE CONTRACT FAILURE LOG TAIL =====' >&2; tail -n 100 "$RUN/native-contracts.log" >&2 || true; }
}
trap finish EXIT
exec > >(tee "$RUN/native-contracts.log") 2>&1
"$TOOL/usr/bin/cmake" -S "$JNI" -B "$RUN/build" \
  -DCMAKE_C_COMPILER="$TOOL/usr/bin/gcc" -DCMAKE_CXX_COMPILER="$TOOL/usr/bin/g++" \
  -DCMAKE_C_FLAGS="$CFLAGS" -DCMAKE_CXX_FLAGS="$CXXFLAGS" \
  -DURMA_INC="$URMA_INC" -DURMA_LIBRARY="$URMA_LIB" \
  -DURMA_COMMON_LIBRARY="$COMMON_LIB" -DJAVA_HOME="$JAVA_HOME" \
  -DBUILD_TESTS=ON -DBUILD_STANDALONE=OFF
"$TOOL/usr/bin/cmake" --build "$RUN/build" --parallel 4
"$TOOL/usr/bin/ctest" --test-dir "$RUN/build" --output-on-failure
status=PASS
echo "OFFLINE_NATIVE_CONTRACTS_PASS log=$RUN/native-contracts.log (no UB hardware)"
