#!/bin/bash

set -euo pipefail

usage() {
    echo "Usage: start-scache"
    exit 1
}

sbin=`dirname "$0"`
sbin=`cd "$sbin"; pwd`

. "$sbin/config.sh"

SLAVES_FILE="${SCACHE_CONF_DIR}/slaves"
if [[ ! -f "$SLAVES_FILE" ]]; then
    echo "ERROR: slaves file not found: $SLAVES_FILE" >&2
    exit 1
fi


echo "stop clients"
while IFS= read -r slave || [[ -n "$slave" ]]; do
    [[ -z "${slave// /}" ]] && continue
    [[ "$slave" =~ ^# ]] && continue
    echo "stop Scache on $slave"

    if [[ "$slave" == "localhost" || "$slave" == "127.0.0.1" || "$slave" == "::1" ||
                "$slave" == "$(hostname)" || "$slave" == "$(hostname -f 2>/dev/null || hostname)" ]]; then
        "${SCACHE_HOME}/sbin/stop-client.sh" &
    else
        ssh -n $SCACHE_SSH_OPTS "$slave" "${SCACHE_HOME}/sbin/stop-client.sh" </dev/null &
    fi
done < "$SLAVES_FILE"

wait

sleep 1

echo "stop master"
"$sbin/stop-master.sh"


