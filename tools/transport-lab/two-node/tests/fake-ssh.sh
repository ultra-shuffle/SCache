#!/usr/bin/env bash
set -Eeuo pipefail
port=
while [[ ${1:-} == -* ]]; do
  case "$1" in
    -p) port=${2:?}; shift 2 ;;
    -o|-i|-F) shift 2 ;;
    *) echo "Unsupported mock SSH option: $1" >&2; exit 2 ;;
  esac
done
[[ $port == 10000 ]] || { echo 'Mock requires the configured nondefault SSH port 10000' >&2; exit 2; }
[[ ${1:-} == mock-b ]] || { echo 'Mock refuses any real SSH target' >&2; exit 2; }
shift
exec bash -c "$1"
