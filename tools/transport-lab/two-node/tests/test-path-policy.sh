#!/usr/bin/env bash
set -Eeuo pipefail
source "$(dirname "$0")/../lib/path-policy.sh"
root=$(mktemp -d /tmp/ub-path-policy-XXXXXX)
mkdir -p "$root/package" "$root/work"
ln -s "$root/package" "$root/package-alias"
ln -s /root "$root/root-alias"
ln -s "$HOME" "$root/home-alias"
validate_workspace_paths A "$root/package/" "$root/work/"
validate_workspace_paths B "$root/package" "$root/new-parent/new-work"
expect_rejected() {
  local path=$1 reason=$2
  if validate_workspace_paths A "$root/package/" "$path" > "$root/rejected.log" 2>&1; then
    echo "UNSAFE_PATH_ACCEPTED $path"; exit 1
  fi
  grep -q "$reason" "$root/rejected.log"
}
expect_rejected "$root/package/work" REFUSE_PACKAGE_WORKSPACE
expect_rejected "$root/package-alias/work" REFUSE_PACKAGE_WORKSPACE
expect_rejected "$root/work/../package/work" REFUSE_PACKAGE_WORKSPACE
expect_rejected /root// REFUSE_BROAD_WORKSPACE
expect_rejected "$root/root-alias" REFUSE_BROAD_WORKSPACE
expect_rejected "$root/home-alias" 'REFUSE_HOME_WORKSPACE\|REFUSE_BROAD_WORKSPACE'
if bash "$(dirname "$0")/../00-package-preflight.sh" \
    --package-home "$root/package/" --workspace "$root/package-alias/offline-output" \
    > "$root/preflight-rejected.log" 2>&1; then
  echo 'PREFLIGHT_UNSAFE_PATH_ACCEPTED'; exit 1
fi
grep -q REFUSE_PACKAGE_WORKSPACE "$root/preflight-rejected.log"
[[ ! -e "$root/package/offline-output" ]] || {
  echo 'PREFLIGHT_WROTE_INSIDE_PACKAGE_BEFORE_PATH_CHECK'; exit 1;
}
package_root=$(cd "$(dirname "$0")/../../.." && pwd -P)
if bash "$(dirname "$0")/../01-offline-native-tests.sh" \
    --workspace "$package_root/should-never-be-offline-work" \
    > "$root/native-rejected.log" 2>&1; then
  echo 'OFFLINE_NATIVE_UNSAFE_PATH_ACCEPTED'; exit 1
fi
grep -q REFUSE_PACKAGE_WORKSPACE "$root/native-rejected.log"
[[ ! -e "$package_root/should-never-be-offline-work" ]] || {
  echo 'OFFLINE_NATIVE_WROTE_INSIDE_PACKAGE_BEFORE_PATH_CHECK'; exit 1;
}
echo 'PATH_POLICY_TESTS_PASS (read-only canonical path checks)'
