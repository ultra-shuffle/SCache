#!/usr/bin/env bash
# Control-flow fixture only: no hardware/Spark behavior is claimed by this mock.
set -Eeuo pipefail
source "$(dirname "$0")/node.env"
RUN="$WORK/runs/$SESSION"; mkdir -p "$RUN"
action=$1; shift
echo "$NODE $action $*" >> "$RUN/actions.log"
[[ -f $RUN/counter ]] || echo 0 > "$RUN/counter"
inc() { echo "$(( $(< "$RUN/counter") + 4096 ))" > "$RUN/counter"; }
case "$action" in
  probe|lease|release|jni|application-check|scache-master|scache-node|scache-standard-node|scache-integration-evidence|topology|spark-master|spark-worker|shared-write|shared-check|prepare-bench|events|bench-check|compare|evidence) echo "MOCK $NODE $action PASS" ;;
  detect)
    if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-detect-fail ]]; then echo "MOCK_DETECT_FAILURE $NODE"; exit 42; fi
    echo "MOCK $NODE detect PASS"
    ;;
  boot-id) echo "different-kernel-$NODE" ;;
  runtime-hashes) echo identical-test-artifacts ;;
  max-request)
    if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-app-limit-b && $NODE == B ]]; then echo 32768
    elif [[ ${NATIVE_EXTRA_ARGS[*]} == mock-app-limit-4k ]]; then echo 4096
    else echo 65536; fi
    ;;
  sdk-version)
    if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-sdk-mismatch && $NODE == B ]]; then
      echo vendor-release-b
    else
      echo vendor-release-a
    fi
    ;;
  selection-json)
    mock_eid=1; mock_priority=6
    [[ $NODE != B ]] || mock_eid=2
    [[ ${NATIVE_EXTRA_ARGS[*]} != mock-same-eid ]] || mock_eid=1
    [[ ${NATIVE_EXTRA_ARGS[*]} != mock-priority-diff || $NODE != B ]] || mock_priority=7
    printf '{"endpoint":{"device":"mock-%s","eid_address":"%032d","priority":%d,"mode":"%s"}}\n' "$NODE" "$mock_eid" "$mock_priority" "$MODE"
    ;;
  dma-unresolved)
    if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-dma-unresolved && $NODE == B ]]; then
      echo 'ERR_DMA_UNRESOLVED: mock provider-owned request'; exit 0
    fi
    exit 1
    ;;
  native-server|native-wait) echo 'MOCK native server success' ;;
  native-client)
    if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-native-fail ]]; then echo 'MOCK_IMPORT_JETTY_FAILURE' >&2; exit 17; fi
    echo 'MOCK native client success'
    ;;
  stop|stop-app|stop-scache)
    if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-stop-fail ]]; then echo 'MOCK_PROCESS_STILL_PRESENT'; exit 1; fi
    if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-stop-b-app-fail && $NODE == B && $action == stop-app ]]; then
      echo 'MOCK_B_WORKER_STILL_RUNNING'; exit 1
    fi
    echo 'MOCK cleaned up'
    ;;
  app-quiescent)
    if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-b-detached-app && $NODE == B ]]; then
      echo 'SESSION_APPLICATION_STILL_LIVE pid=99999 pgid=99999'; exit 1
    fi
    echo 'MOCK application groups quiescent'
    ;;
  smoke)
    if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-app-timeout && $1 == ub ]]; then
      echo 'MOCK_SPARK_SUBMIT_TIMEOUT' >&2; exit 124
    fi
    [[ $1 != ub ]] || inc ;;
  bench) [[ $1 != ub ]] || inc ;;
  ctl)
    case "$1" in
      METRICS)
        if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-one-sided && $NODE == B && $(< "$RUN/counter") == 0 ]]; then
          echo 4096 > "$RUN/counter"
        fi
        bytes=$(< "$RUN/counter")
        printf 'OK urma.submittedBytes=%s urma.completedBytes=%s urma.sharedPoolDestinationBytes=%s ' "$bytes" "$bytes" "$bytes"
        for key in submittedChunks providerTerminalChunks apiTerminalChunks; do printf 'urma.%s=1 ' "$key"; done
        for key in tcpPayloadBytes nettyPayloadBytes fallbacks failedChunks checksumErrors generationErrors sharedPoolDestinationIdentityMismatches transportFailedOperations transportTimedOutOperations transportInflightOperations activeLeases activeImports activeMappings activeSlots activeTokens arenaPublishedBlocks; do printf 'urma.%s=0 ' "$key"; done
        if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-missing-metric ]]; then echo; else echo 'urma.arenaFreeBytes=134217728'; fi
        ;;
      PUT*|FETCH*)
        if [[ ${NATIVE_EXTRA_ARGS[*]} == mock-dma-unresolved && $NODE == B && $1 == FETCH* ]]; then
          echo 'ERR_DMA_UNRESOLVED: mock provider-owned request' >&2; exit 42
        fi
        if [[ ${NATIVE_EXTRA_ARGS[*]} != mock-one-sided || $NODE != B ]]; then inc; fi
        echo 'OK content verified' ;;
      *) echo OK ;;
    esac
    ;;
  archive) tar -czf - -C "$WORK" "runs/$SESSION" ;;
  *) echo "unknown mock action $action" >&2; exit 2 ;;
esac
