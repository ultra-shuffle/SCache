#!/usr/bin/env bash
set -Eeuo pipefail
HERE=$(cd "$(dirname "$0")" && pwd -P)
FROM=${1:?exported runtime}; WORK=${2:?test directory}
mkdir -p "$WORK"
DEST=$(mktemp -d "$WORK/relocated-runtime-XXXXXXXX")
cp -a "$FROM/." "$DEST/"
(cd "$DEST"; sha256sum -c SHA256SUMS) > "$DEST/relocation-check.log"
entries=()
while IFS= read -r entry; do entries+=("$DEST/$entry"); done < "$DEST/classpath.relative"
(IFS=:; printf '%s\n' "${entries[*]}") > "$DEST/absolute-classpath.txt"
export SCACHE_TEST_CLASSPATH_FILE=$DEST/absolute-classpath.txt
bash "$HERE/run-node.sh" --check "$WORK/launcher"
echo "RELOCATED_RUNTIME_CHECK_PASS runtime=$DEST (no device initialization)"
