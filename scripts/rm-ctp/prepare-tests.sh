#!/usr/bin/env bash
# Build only SCache integration classes and export their exact classpath.
set -euo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/package-env.sh"
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
: "${JAVA_HOME:?Set JAVA_HOME to JDK 17}"
: "${SBT_LAUNCH:?Set SBT_LAUNCH to sbt-launch-1.11.7.jar}"
OUT=${1:?usage: prepare-tests.sh output-directory}
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
[[ $("$JAVA_HOME/bin/javac" -version 2>&1) == javac\ 17.* ]] || exit 1
cd "$REPO"
env -u JAVA_TOOL_OPTIONS -u JDK_JAVA_OPTIONS -u _JAVA_OPTIONS -u CLASSPATH \
  "$JAVA_HOME/bin/java" -Xmx3g -Dsbt.log.noformat=true -jar "$SBT_LAUNCH" \
  'Test / compile' 'Integration / compile' 'export Integration / fullClasspath' 2>&1 | tee "$OUT/prepare.log"
# sbt export emits a single unprefixed absolute classpath line.
mapfile -t CANDIDATES < <(grep '^/' "$OUT/prepare.log" | grep ':')
[[ ${#CANDIDATES[@]} == 1 ]] || { echo 'Cannot identify unique exported classpath; see prepare.log' >&2; exit 1; }
IFS=: read -r -a ENTRIES <<< "${CANDIDATES[0]}"
VALID=()
for entry in "${ENTRIES[@]}"; do
  # With no Test sources sbt exports this output directory without creating it.
  if [[ ! -e "$entry" && "$entry" == "$REPO"/target/scala-*/test-classes && ! -d "$REPO/src/test" ]]; then
    echo "OPTIONAL_EMPTY_TEST_CLASSES=$entry"
    continue
  fi
  [[ -e "$entry" ]] || { echo "Missing classpath entry: $entry" >&2; exit 1; }
  VALID+=("$entry")
done
(IFS=:; printf '%s\n' "${VALID[*]}") > "$OUT/classpath.txt"
printf 'SCACHE_TEST_CLASSES_READY classpath=%s/classpath.txt\n' "$OUT"
if [[ -n ${2:-} ]]; then
  mkdir -p "$2"
  EXPORT=$(mktemp -d "$(cd "$2" && pwd -P)/rm-ctp-runtime-XXXXXXXX")
  index=0
  for entry in "${VALID[@]}"; do
    cp -RL -- "$entry" "$EXPORT/entry-$index"
    printf 'entry-%s\n' "$index" >> "$EXPORT/classpath.relative"
    index=$((index+1))
  done
  (find src/main src/integration -type f -exec sha256sum {} +; sha256sum build.sbt) |
    sort | sha256sum | awk '{print $1}' > "$EXPORT/source-id.txt"
  (cd "$EXPORT"; find . -type f ! -name SHA256SUMS -exec sha256sum {} + | sort > SHA256SUMS)
  printf 'RM_CTP_RUNTIME_EXPORTED=%s\n' "$EXPORT"
fi
