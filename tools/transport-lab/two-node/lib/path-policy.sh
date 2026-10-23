#!/usr/bin/env bash
# Read-only checks must run on the host that owns these paths. Do not resolve
# B's symlinks on A: the same spelling can refer to different filesystems.
validate_workspace_paths() {
  local node=$1 package=$2 work=$3 canonical_package canonical_work canonical_home
  [[ $package == /* && $work == /* ]] || {
    echo "WORKSPACE_PATH_INVALID node=$node: package/work must be absolute" >&2; return 1;
  }
  canonical_package=$(readlink -m -- "$package") &&
    canonical_work=$(readlink -m -- "$work") || {
    echo "WORKSPACE_PATH_UNRESOLVED node=$node package=$package work=$work" >&2; return 1;
  }
  case "$canonical_work" in
    /|/home|/root|/tmp|/var/tmp|/opt|/mnt)
      echo "REFUSE_BROAD_WORKSPACE node=$node path=$canonical_work" >&2; return 1 ;;
  esac
  if [[ -n ${HOME:-} ]]; then
    canonical_home=$(readlink -m -- "$HOME") || return 1
    [[ $canonical_work != "$canonical_home" ]] || {
      echo "REFUSE_HOME_WORKSPACE node=$node path=$canonical_work" >&2; return 1;
    }
  fi
  [[ $canonical_package != / && $canonical_work != "$canonical_package" &&
      $canonical_work != "$canonical_package/"* ]] || {
    echo "REFUSE_PACKAGE_WORKSPACE node=$node package=$canonical_package work=$canonical_work" >&2
    return 1
  }
  printf 'WORKSPACE_PATHS_VALID node=%s package=%s work=%s\n' "$node" "$canonical_package" "$canonical_work"
}
