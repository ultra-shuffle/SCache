#!/bin/bash

# resolve links - $0 may be a softlink
this="${BASH_SOURCE-$0}"
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$common_bin/$script"

# convert relative path to absolute path
config_bin=`dirname "$this"`
script=`basename "$this"`
config_bin=`cd "$config_bin"; pwd`
this="$config_bin/$script"

export SCACHE_PREFIX=`dirname "$this"`/..
export SCACHE_HOME=`cd "${SCACHE_PREFIX}"; pwd`
export SCACHE_CONF_DIR="$SCACHE_HOME/conf"

# Defaults (override by exporting before calling scripts)
export SCACHE_LOG_DIR="${SCACHE_LOG_DIR:-$SCACHE_HOME/logs}"
export SCACHE_PID_DIR="${SCACHE_PID_DIR:-$SCACHE_HOME/tmp/pids}"
export SCACHE_JAVA_OPTS="${SCACHE_JAVA_OPTS:-}"
export SCACHE_SSH_OPTS="${SCACHE_SSH_OPTS:--o BatchMode=yes -o ConnectTimeout=5 -o NumberOfPasswordPrompts=0 -o GSSAPIAuthentication=no}"

