#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${SOURCE_VERSIONS_ROOT:-$SCRIPT_DIR}"
case "${1:-}" in
 direct-urma|urpc|umq) version="$1" ;;
 *) echo "Usage: bash switch-version.sh direct-urma|urpc|umq" >&2; exit 2 ;;
esac
exec 9>"$ROOT/.switch-version.lock"
flock -n 9 || { echo "Another version switch is active" >&2; exit 1; }
repos=(SCache spark-3.5-scache OpenURMA)
branches=("versions/$version" versions/scache-shared "versions/$version")
for i in "${!repos[@]}"; do
 repo="${repos[$i]}"; branch="${branches[$i]}"
 test -d "$ROOT/$repo/.git" || { echo "Missing repository: $repo" >&2; exit 1; }
 if [[ -n "$(git -C "$ROOT/$repo" status --porcelain)" ]]; then
  echo "Refuse switch: $repo has changes; preserve them first" >&2; exit 1
 fi
 git -C "$ROOT/$repo" show-ref --verify --quiet "refs/heads/$branch" ||
  { echo "Missing branch: $repo $branch" >&2; exit 1; }
done
for i in "${!repos[@]}"; do
 repo="${repos[$i]}"; branch="${branches[$i]}"
 git -C "$ROOT/$repo" switch "$branch"
 printf '%s %s %s\n' "$repo" "$branch" "$(git -C "$ROOT/$repo" rev-parse --short HEAD)"
done
echo "SOURCE_VERSION_SELECTED=$version SPARK=versions/scache-shared (source browsing only)"
