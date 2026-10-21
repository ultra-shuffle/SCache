#!/usr/bin/env bash
# Source only. Reuse the original single-node package's private JDK.
RM_CTP_PACKAGE=${RM_CTP_PACKAGE:-${PACKAGE_HOME:-$HOME/ub-source/ultrashuffle/spark-3.5.7-benchmark-runtime-aarch64}}
if [[ -d $RM_CTP_PACKAGE/dependencies/jdk ]]; then
  export JAVA_HOME="$RM_CTP_PACKAGE/dependencies/jdk"
  unset JAVA_TOOL_OPTIONS JDK_JAVA_OPTIONS _JAVA_OPTIONS CLASSPATH LD_PRELOAD
  export PATH="$JAVA_HOME/bin:/usr/sbin:/usr/bin:/sbin:/bin"
fi
