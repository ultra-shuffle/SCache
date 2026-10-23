#!/usr/bin/env bash
for arg in "$@"; do
  if [[ $arg == -h ]]; then
    echo '--eid_idx --ctp --single_path --bind_ip --priority --trans_mode --share_jfr'
    exit 0
  fi
done
for arg in "$@"; do
  if [[ $arg == --fake-fail ]]; then
    echo 'MOCK_VENDOR_PERFTEST_FAILURE: failed to import jetty' >&2
    exit 42
  fi
done
printf 'MOCK_PERFTEST_ARGS'; printf ' [%s]' "$@"; printf '\n'
for arg in "$@"; do
  [[ $arg != --fake-empty-success ]] || exit 0
done
size=4096
while (($#)); do
  if [[ $1 == -s ]]; then size=$2; shift 2; else shift; fi
done
printf '  %s 1000 100.0 90.0 1.0\n' "$size"
