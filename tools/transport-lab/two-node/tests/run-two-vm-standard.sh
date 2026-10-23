#!/usr/bin/env bash
# Complete development-host fixture, not a production deployment entry.
set -Eeuo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd -P)
LAB=${1:?usage: run-two-vm-standard.sh NEW_LAB_DIR PREPARED_ARM_IMAGE PACKAGE_DIR [60|65|66]}
IMAGE=${2:?}; PACKAGE=${3:?}; STAGE=${4:-60}
[[ $STAGE == 60 || $STAGE == 65 || $STAGE == 66 ]] || { echo 'Only no-UB stages 60, 65 and 66 are supported by this VM fixture' >&2; exit 2; }
KEY=${VM_SSH_KEY:-/home/xinzhang/vm/keys/codex_vm_ed25519}
[[ $LAB == /* && ! -e $LAB ]] || { echo 'Use a NEW absolute lab directory'; exit 1; }
guest() { local port=$1; shift; ssh -i "$KEY" -p "$port" -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=yes -o UserKnownHostsFile="$LAB/known_hosts" xinzhang@127.0.0.1 "$@"; }
finish() {
  rc=$?; trap - EXIT
  set +e
  mkdir -p "$LAB/evidence"
  for node in A B; do
    [[ -f $LAB/$node/qemu.pid ]] || continue
    port=2235; [[ $node == A ]] || port=2236
    # The stage runner already archives scoped process logs. Do not re-archive
    # Spark's worker/JAR scratch tree, which can be gigabytes for full TPC-DS.
    if [[ $node == A ]]; then
      # The controller's stage log already contains scoped evidence from both
      # nodes. B has no local stage-controller log directory.
      guest "$port" tar --warning=no-timestamp -czf - \
        -C /home/xinzhang/standard-two-node-work logs \
        > "$LAB/evidence/stage-logs.tar.gz" 2> "$LAB/evidence/stage-collect.log"
      collected=$?
      [[ $collected == 0 ]] || rc=1
    fi
    guest "$port" sudo shutdown -h now > "$LAB/evidence/$node-shutdown.log" 2>&1
  done
  echo "LAB_EXIT_CODE=$rc EVIDENCE=$LAB/evidence"
  exit "$rc"
}
trap finish EXIT
bash "$HERE/tests/start-two-vm-lab.sh" "$LAB" "$IMAGE" "$PACKAGE"
guest 2235 mkdir -p /home/xinzhang/two-node-test
tar -cf - -C "$HERE" . | guest 2235 tar -xf - -C /home/xinzhang/two-node-test
case "$STAGE" in
  60) stage_script=60-standard-benchmarks.sh ;;
  65) stage_script=65-scache-netty-smoke.sh ;;
  66) stage_script=66-scache-netty-benchmarks.sh ;;
esac
guest 2235 bash "/home/xinzhang/two-node-test/$stage_script" \
  --config /home/xinzhang/two-node-test/tests/config-standard-lab.env 2>&1 | tee "$LAB/benchmark-terminal.log"
