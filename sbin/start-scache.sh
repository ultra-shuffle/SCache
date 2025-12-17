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


echo "start master"
"$sbin/start-master.sh"
sleep 5

echo "start clients"
while IFS= read -r slave || [[ -n "$slave" ]]; do
    # ignore blanks and comments
    [[ -z "${slave// /}" ]] && continue
    [[ "$slave" =~ ^# ]] && continue
    echo "start Scache on $slave"

        if [[ "$slave" == "localhost" || "$slave" == "127.0.0.1" || "$slave" == "::1" ||
                    "$slave" == "$(hostname)" || "$slave" == "$(hostname -f 2>/dev/null || hostname)" ]]; then
            "${SCACHE_HOME}/sbin/start-client.sh" &
        else
            # -n and </dev/null prevent ssh from reading stdin (which can stall loops and jobs)
            ssh -n $SCACHE_SSH_OPTS "$slave" "${SCACHE_HOME}/sbin/start-client.sh" </dev/null &
        fi
done < "$SLAVES_FILE"

wait
