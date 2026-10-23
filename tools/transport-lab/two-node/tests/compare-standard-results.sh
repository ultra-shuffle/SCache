#!/usr/bin/env bash
# Compare complete canonical query outputs; does not rerun or modify either test.
set -Eeuo pipefail
LEFT=${1:?first standard/tpcds directory}
RIGHT=${2:?second standard/tpcds directory}
shopt -s nullglob
left=("$LEFT"/*-results.jsonl)
right=("$RIGHT"/*-results.jsonl)
[[ ${#left[@]} == 103 && ${#right[@]} == 103 ]] || { echo 'INCOMPLETE_TPCDS_RESULTS'; exit 1; }
for file in "${left[@]}"; do
  name=${file##*/}
  query=${name%-results.jsonl}
  cmp "$file" "$RIGHT/$name"
  cmp "$LEFT/$query-schema.json" "$RIGHT/$query-schema.json"
done
echo 'STANDARD_TPCDS_MATCH statements=103 schemas=103'
