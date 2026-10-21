#!/usr/bin/env bash
# Build-host utility. Copies the existing source/runtime; does not compile or patch product code.
set -Eeuo pipefail
HERE=$(cd "$(dirname "$0")" && pwd -P)
REPO=$(cd "$HERE/../.." && pwd -P)
RUNTIME=$(cd "${1:?exported runtime directory}" && pwd -P)
OUTPUT=${2:?new archive absolute path}
[[ $OUTPUT == /* && ! -e $OUTPUT ]] || { echo 'Output must be a new absolute file path'; exit 1; }
IDENTITY=$(cd "$REPO"; { find src/main src/integration -type f -exec sha256sum {} +; sha256sum build.sbt; } | sort | sha256sum | awk '{print $1}')
[[ $(< "$RUNTIME/source-id.txt") == "$IDENTITY" ]] || { echo 'Runtime/source mismatch'; exit 1; }
(cd "$RUNTIME"; sha256sum -c SHA256SUMS >/dev/null)
mkdir -p "$(dirname "$OUTPUT")"
STAGE=$(mktemp -d "$(dirname "$OUTPUT")/rm-ctp-delivery-XXXXXXXX")
ROOT=$STAGE/scache-rm-ctp-20260928
mkdir -p "$ROOT/src" "$ROOT/native/spark_urma" "$ROOT/scripts" "$ROOT/project"
cp -a "$REPO/src/main" "$REPO/src/integration" "$ROOT/src/"
cp -a "$REPO/native/spark_urma/src" "$REPO/native/spark_urma/include" "$REPO/native/spark_urma/CMakeLists.txt" "$ROOT/native/spark_urma/"
cp -a "$REPO/scripts/rm-ctp" "$ROOT/scripts/"
cp -a "$REPO/build.sbt" "$ROOT/"
cp -a "$REPO/project/build.properties" "$ROOT/project/"
cp -a "$RUNTIME" "$ROOT/runtime"
tar --exclude='__pycache__' --exclude='*.pyc' -czf "$OUTPUT" -C "$STAGE" scache-rm-ctp-20260928
echo "DELIVERY=$OUTPUT"
echo "STAGING_RETAINED=$STAGE"
sha256sum "$OUTPUT"
du -h "$OUTPUT"
