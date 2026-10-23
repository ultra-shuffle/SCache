#!/usr/bin/env bash
set -Eeuo pipefail
HERE=$(cd -- "$(dirname -- "$0")" && pwd -P)
source "$HERE/lib/path-policy.sh"
CONFIG="$HERE/config.env"; SESSION=; PROVIDER_CLEARED=false
while (($#)); do
  case "$1" in
    --config) CONFIG=${2:?}; shift 2 ;;
    --session) SESSION=${2:?}; shift 2 ;;
    --provider-cleared) PROVIDER_CLEARED=true; shift ;;
    *) echo 'Usage: bash 90-stop.sh --config FILE --session phase-XX-YYYYMMDD-HHMMSS-XXXXXX [--provider-cleared]'; exit 2 ;;
  esac
done
[[ $SESSION =~ ^phase-(05|10|20|30|40|50|60|65|66|70)-[0-9]{8}-[0-9]{6}-[A-Za-z0-9]{6}$ ]] || { echo 'invalid session identifier'; exit 2; }
[[ -r $CONFIG ]] || { echo "config missing: $CONFIG" >&2; exit 2; }
source "$CONFIG"
validate_workspace_paths A "${A_PACKAGE:-}" "${A_WORK:-}"
[[ ${A_WORK:-} == /* && ${B_WORK:-} == /* ]] || { echo 'A_WORK and B_WORK must be absolute' >&2; exit 2; }
for work in "$A_WORK" "$B_WORK"; do
  case "${work%/}" in
    /|/home|/root|/tmp|/var/tmp|/opt|/mnt) echo "REFUSE_BROAD_WORKSPACE: $work" >&2; exit 2 ;;
  esac
done
[[ -d $A_WORK/state ]] || { echo "SESSION_STATE_MISSING: $A_WORK/state" >&2; exit 2; }
exec 9<>"$A_WORK/state/orchestrator.lock"
flock -n 9 || {
  echo "REFUSE_ACTIVE_ORCHESTRATOR: another dual-node run is using $A_WORK; wait for it to finish before manual cleanup" >&2
  exit 2
}
[[ -d $A_WORK/runs/$SESSION/tool ]] || { echo "SESSION_TOOL_MISSING: $A_WORK/runs/$SESSION/tool" >&2; exit 2; }
declare -p SSH_OPTIONS >/dev/null 2>&1 || SSH_OPTIONS=(-o BatchMode=yes -o ConnectTimeout=10)
[[ $(declare -p SSH_OPTIONS) == declare\ -a* ]] || { echo 'SSH_OPTIONS must be an array' >&2; exit 2; }
[[ ${B_SSH:-} != -* && ${B_SSH:-} != *[[:space:]]* && -n ${B_SSH:-} ]] || { echo 'invalid B_SSH destination' >&2; exit 2; }
unsafe=false
if [[ -f $A_WORK/logs/$SESSION/dma-quarantine.txt ]]; then unsafe=true; fi
if [[ $SESSION == phase-40-* || $SESSION == phase-50-* || $SESSION == phase-70-* ]]; then
  if [[ ! -f $A_WORK/logs/$SESSION/status.txt ]] ||
     ! grep -Fxq 'result=PASS' "$A_WORK/logs/$SESSION/status.txt"; then
    unsafe=true
  fi
fi
if [[ $unsafe == true ]]; then
  if [[ $PROVIDER_CLEARED != true ]]; then
    echo "REFUSE_QUARANTINED_SESSION: $SESSION failed or has no terminal status; provider-owned DMA may remain. Obtain device/provider clearance before --provider-cleared" >&2
    exit 2
  fi
  echo "PROVIDER_CLEARANCE_ASSERTED session=$SESSION (operator assertion; this script cannot verify the device CQ)"
fi
rc=0
remote_node() {
  local command
  printf -v command '%q ' bash "$B_WORK/runs/$SESSION/tool/node.sh" "$1"
  ssh "${SSH_OPTIONS[@]}" "$B_SSH" "$command"
}
path_check="$(declare -f validate_workspace_paths)"$'\n''validate_workspace_paths "$@"'
printf -v path_command '%q ' bash -c "$path_check" bash B "${B_PACKAGE:-}" "$B_WORK"
ssh "${SSH_OPTIONS[@]}" "$B_SSH" "$path_command" || exit 2
echo 'CLEANUP_START node=A action=stop-app'
bash "$A_WORK/runs/$SESSION/tool/node.sh" stop-app || rc=1
echo 'CLEANUP_START node=B action=stop-app'
remote_node stop-app || rc=1
if ((rc!=0)); then
  echo 'SCACHE_RETAINED: one endpoint application group did not stop; neither SCache is stopped' >&2
  exit "$rc"
fi
echo 'CLEANUP_START node=A action=app-quiescent'
bash "$A_WORK/runs/$SESSION/tool/node.sh" app-quiescent || rc=1
echo 'CLEANUP_START node=B action=app-quiescent'
remote_node app-quiescent || rc=1
if ((rc!=0)); then
  echo 'SCACHE_RETAINED: an endpoint still has a session application process' >&2
  exit "$rc"
fi
echo 'CLEANUP_START node=A action=stop-scache'
bash "$A_WORK/runs/$SESSION/tool/node.sh" stop-scache || rc=1
echo 'CLEANUP_START node=B action=stop-scache'
remote_node stop-scache || rc=1
((rc==0)) || { echo 'SCache stop incomplete; leases retained' >&2; exit "$rc"; }
bash "$A_WORK/runs/$SESSION/tool/node.sh" release || rc=1
remote_node release || rc=1
((rc==0)) || { echo 'Some processes could not be stopped; workspace and logs retained'; exit "$rc"; }
echo "SESSION_STOPPED $SESSION (logs retained)"
