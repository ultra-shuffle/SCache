#!/usr/bin/env bash
set -Eeuo pipefail
R="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec 9>"$R/build/logical-cluster.lock"
flock -n 9 || { echo "A cluster/build is active; do not overwrite its binaries"; exit 1; }

KIT=/home/xinzhang/codex-fullkit-runtime-20260918
export JAVA_HOME="$KIT/toolchain/jdk-17-aarch64"
export PATH="$JAVA_HOME/bin:/usr/bin:/bin"
unset JAVA_TOOL_OPTIONS _JAVA_OPTIONS JDK_JAVA_OPTIONS CLASSPATH LD_PRELOAD LD_LIBRARY_PATH
if [[ ! -d "$R/build/sbt-cache" ]]; then
 cp -a --reflink=auto /home/xinzhang/urma-official-review-20261015/sbt-cache "$R/build/sbt-cache"
fi
export COURSIER_CACHE="$R/build/sbt-cache/coursier"
export COURSIER_REPOSITORIES=https://repo1.maven.org/maven2
cd "$R/repos/SCache"
"$JAVA_HOME/bin/java" -Xmx3g \
 -Dsbt.boot.directory="$R/build/sbt-cache/sbt/boot" \
 -Dsbt.global.base="$R/build/sbt-cache/sbt" \
 -Dsbt.ivy.home="$R/build/sbt-cache/ivy2" \
 -Dsbt.coursier.home="$R/build/sbt-cache/coursier" \
 -Dsbt.repository.config=/home/xinzhang/urma-official-review-20261015/sbt-repositories \
 -Dsbt.override.build.repos=true -Dsbt.offline=true \
 -jar "$KIT/toolchain/sbt-launch-1.11.7.jar" \
 'Integration / compile' 'Integration / packageBin' assembly
echo URPC_SCACHE_BUILD_PASS
