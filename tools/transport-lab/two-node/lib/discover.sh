#!/usr/bin/env bash
# Reuse the installed package's bounded software search, not its device policy.
set -Eeuo pipefail
TOOL_HOME=$(cd -- "$(dirname -- "$0")" && pwd -P)
source "$TOOL_HOME/node.env"
declare -p SEARCH_ROOTS >/dev/null 2>&1 || SEARCH_ROOTS=()
PACKAGE_HOME=$PACKAGE WORK_ROOT=$WORK
source "$PACKAGE/bin/real-ub/lib/common.sh"
PACKAGE_HOME=$PACKAGE WORK_ROOT=$WORK
LOG="$WORK/runs/$SESSION/discovery"
mkdir -p "$LOG"
RAW="$LOG/candidates-raw.tsv"
: > "$RAW"
: > "$LOG/candidates-rejected.tsv"
: > "$LOG/search-roots.tsv"
ERRORS=0
issue() { printf '%s\t%s\t%s\t%s\n' "$@" | tee -a "$LOG/findings.log"; [[ $1 != ERROR ]] || ERRORS=$((ERRORS+1)); }
UMDK_ROOT=${SDK_ROOT:-}
source "$PACKAGE/bin/real-ub/lib/discovery.sh"
if [[ -n $UMDK_ROOT ]]; then
  [[ $UMDK_ROOT == /* && -d $UMDK_ROOT ]] || die 'SDK_ROOT must be an existing absolute installation prefix'
  UMDK_ROOT=$(readlink -f "$UMDK_ROOT")
  scan_root "$UMDK_ROOT" explicit 0
fi
if command -v rpm >/dev/null; then
  rpm -qa --qf '%{NAME}\t%{VERSION}-%{RELEASE}\n' > "$LOG/packages-all.txt"
  awk 'tolower($1) ~ /umdk|urma|udma|ubcore|uburma/' "$LOG/packages-all.txt" > "$LOG/packages-ub.txt"
  while IFS=$'\t' read -r pkg rest; do
    [[ -n $pkg ]] || continue
    while IFS= read -r p; do add_candidate "$p" package-db 10; done < <(rpm -ql "$pkg")
  done < "$LOG/packages-ub.txt"
fi
if command -v ldconfig >/dev/null; then
  ldconfig -p > "$LOG/ldconfig.txt" 2>&1 || true
  while IFS= read -r p; do add_candidate "$p" ldconfig 10; done < <(awk '/liburma/{print $NF}' "$LOG/ldconfig.txt")
fi
for cmd in urma_admin urma_perftest; do
  p=$(command -v "$cmd" || true); [[ -z $p ]] || add_candidate "$p" PATH 10
done
for root in /usr/lib64 /usr/lib /lib64 /lib /usr/include /usr/bin /usr/sbin /usr/local /opt "${SEARCH_ROOTS[@]}"; do
  [[ -d $root ]] || continue
  scan_root "$root" search 20
done
if [[ ${PROVIDER:-auto} != auto ]]; then
  add_candidate "$PROVIDER" explicit-provider -1
fi
URMA_LIB= URMA_COMMON_LIB= URMA_API_H= URMA_ADMIN= URMA_PERFTEST=
select_candidate core URMA_LIB
select_candidate common URMA_COMMON_LIB
select_candidate api URMA_API_H
select_candidate admin URMA_ADMIN
select_candidate perftest URMA_PERFTEST
((ERRORS==0)) || die "SOFTWARE_DISCOVERY_FAILED: $LOG/findings.log"
printf 'core\t%s\ncommon\t%s\napi\t%s\nadmin\t%s\nperftest\t%s\n' \
  "$URMA_LIB" "$URMA_COMMON_LIB" "$URMA_API_H" "$URMA_ADMIN" "$URMA_PERFTEST" > "$LOG/files.tsv"
command -v python3 >/dev/null || die 'python3 is required for read-only device discovery; install the OS python3 package'
exec python3 "$TOOL_HOME/discover.py" --package "$PACKAGE" --work "$WORK" --run "$WORK/runs/$SESSION" \
  --node "$NODE" --mode "$MODE" --device "$DEVICE" --provider "$PROVIDER" --eid "$EID" --priority "$PRIORITY" \
  --policy "${1:-confirm}"
