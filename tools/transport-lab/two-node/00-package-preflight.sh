#!/usr/bin/env bash
# Hardware-independent checks for one extracted RM/CTP delivery on one host.
set -u -o pipefail
HERE=$(cd -- "$(dirname -- "$0")" && pwd -P)
source "$HERE/lib/path-policy.sh"
PACKAGE_HOME=$(cd -- "$HERE/../.." && pwd -P)
WORKSPACE=
while (($#)); do
  case "$1" in
    --workspace) WORKSPACE=${2:?}; shift 2 ;;
    --package-home) PACKAGE_HOME=${2:?}; shift 2 ;;
    -h|--help)
      echo "Usage: bash $0 --workspace /absolute/work [--package-home /absolute/package]"
      exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ $PACKAGE_HOME == /* && -d $PACKAGE_HOME ]] || { echo 'package home must be an existing absolute directory' >&2; exit 2; }
[[ $WORKSPACE == /* ]] || { echo 'specify an absolute --workspace' >&2; exit 2; }
[[ $PACKAGE_HOME != *[[:space:]]* && $WORKSPACE != *[[:space:]]* ]] || {
  echo 'UNSUPPORTED_WHITESPACE_PATH: package and workspace paths must not contain whitespace because bundled CMake/GCC cannot build JNI there' >&2; exit 2;
}
validate_workspace_paths local "$PACKAGE_HOME" "$WORKSPACE" || exit 2
mkdir -p "$WORKSPACE/logs" || exit 2
RUN=$(mktemp -d "$WORKSPACE/logs/package-preflight-$(date +%Y%m%d-%H%M%S)-XXXXXX") || exit 2
FAILED=0
check() {
  local name=$1; shift
  if "$@" > "$RUN/$name.log" 2>&1; then
    printf 'PASS %s\n' "$name" | tee -a "$RUN/checks.txt"
  else
    local rc=$?
    printf 'FAIL %s rc=%s log=%s\n' "$name" "$rc" "$RUN/$name.log" | tee -a "$RUN/checks.txt"
    FAILED=1
  fi
}
check architecture bash -c '[[ $(uname -m) == aarch64 ]]'
check host-commands bash -c 'for cmd in bash ssh tar timeout flock ss ip setsid nohup ps sha256sum python3 ldd getconf find grep awk sed cmp sort xargs tr tee head tail date mktemp wc install readlink; do command -v "$cmd" >/dev/null || { echo "missing command: $cmd"; exit 1; }; done'
check host-glibc python3 - <<'PY'
import re
import subprocess

version = subprocess.check_output(['getconf', 'GNU_LIBC_VERSION'], text=True).strip()
print('HOST_GLIBC=' + version)
match = re.fullmatch(r'glibc (\d+)\.(\d+)', version)
assert match, 'Cannot determine target glibc version; bundled GCC/CMake require GLIBC_2.38'
assert tuple(map(int, match.groups())) >= (2, 38), (
    'HOST_GLIBC_TOO_OLD: bundled GCC/CMake require GLIBC_2.38; '
    'do not replace the system glibc. Use a toolchain built for this OS.'
)
PY
check package-layout bash -c 'test -x "$1/dependencies/jdk/bin/java" && test -x "$1/runtime/spark/bin/spark-submit" && test -r "$1/sources/scache-jni/CMakeLists.txt" && test -r "$1/metadata/rm-ctp-build.txt" && test -r "$1/benchmarks/tools/benchmark-checks.jar" && test -r "$1/benchmarks/hibench/hadoop-mapreduce-examples-3.3.4.jar"' bash "$PACKAGE_HOME"
check package-sha256 bash -c '(cd "$1" && sha256sum -c SHA256SUMS)' bash "$PACKAGE_HOME"
check benchmark-assets python3 - "$PACKAGE_HOME" <<'PY'
import pathlib
import sys
import zipfile

root = pathlib.Path(sys.argv[1])
queries = list((root / 'benchmarks/tpcds/queries').glob('q[0-9]*.sql'))
assert len(queries) == 99, 'Expected 99 TPC-DS query files, got {}'.format(len(queries))
assert list((root / 'benchmarks/tpcds/parquet').rglob('*.parquet')), 'TPC-DS Parquet data missing'
with zipfile.ZipFile(root / 'benchmarks/tools/benchmark-checks.jar') as archive:
    assert 'BenchmarkChecks.class' in archive.namelist()
with zipfile.ZipFile(root / 'benchmarks/hibench/hadoop-mapreduce-examples-3.3.4.jar') as archive:
    names = archive.namelist()
    assert 'org/apache/hadoop/examples/terasort/TeraInputFormat.class' in names
    assert 'org/apache/hadoop/examples/terasort/TeraOutputFormat.class' in names
with zipfile.ZipFile(root / 'benchmarks/hibench/sparkbench-assembly-7.1.1-dist.jar') as archive:
    names = archive.namelist()
    for name in ('ScalaWordCount', 'ScalaSort', 'ScalaTeraSort', 'ScalaRepartition'):
        assert 'com/intel/hibench/sparkbench/micro/{}.class'.format(name) in names, name
print('BENCHMARK_ASSETS_PASS query_files=99 hibench_workloads=4')
PY
check runtime-identity python3 - "$PACKAGE_HOME" <<'PY'
import hashlib
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
record = dict(line.split('=', 1) for line in
              (root / 'metadata/rm-ctp-build.txt').read_text().splitlines() if '=' in line)
assert record.get('build_kind') == 'isolated-rm-ctp-experiment', record
assert record.get('spark_version') == '3.5.7', record
jars = root / 'runtime/spark/jars'
for pattern, expected in (
        ('hadoop-client-api-*.jar', 'hadoop-client-api-3.3.4.jar'),
        ('hadoop-client-runtime-*.jar', 'hadoop-client-runtime-3.3.4.jar'),
        ('scala-library-*.jar', 'scala-library-2.13.8.jar')):
    actual = sorted(path.name for path in jars.glob(pattern))
    assert actual == [expected], '{}: {}'.format(pattern, actual)
files = {
    'scache_assembly_sha256': root / 'runtime/scache/SCache-assembly-0.1.0-SNAPSHOT.jar',
    'scache_integration_sha256': root / 'runtime/scache/SCache-ub-integration-test.jar',
    'spark_core_sha256': root / 'runtime/spark/jars/spark-core_2.13-3.5.7.jar',
}
for key, path in files.items():
    h = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    assert h.hexdigest() == record[key], '{}: {}'.format(key, path)
    print('{} {}'.format(key, h.hexdigest()))
PY
if [[ -x $PACKAGE_HOME/dependencies/jdk/bin/java ]]; then
  check java17 bash -c 'banner=$("$1" -version 2>&1); printf "%s\n" "$banner"; grep -Eq '\''version "17\.'\'' <<< "$banner"' bash "$PACKAGE_HOME/dependencies/jdk/bin/java"
fi
if [[ -d $PACKAGE_HOME/dependencies/native-toolchain ]]; then
  check bundled-toolchain-libraries bash -c '
    tool="$1/dependencies/native-toolchain"
    export LD_LIBRARY_PATH="$tool/usr/lib64:$tool/usr/lib:$tool/lib64:$tool/lib"
    failed=0
    for cmd in gcc g++ cmake ctest make ld as readelf; do
      file="$tool/usr/bin/$cmd"
      test -x "$file" || { echo "MISSING_TOOL $file"; failed=1; continue; }
      echo "=== $cmd ==="
      output=$(ldd "$file" 2>&1); rc=$?
      printf "%s\n" "$output"
      if ((rc != 0)) || grep -Eq "not found|version .+ not found" <<< "$output"; then
        echo "TOOL_LIBRARY_UNRESOLVED $cmd: provide the target-OS compatible library set or rebuild the toolchain"
        failed=1
      fi
    done
    ((failed == 0))
  ' bash "$PACKAGE_HOME"
  check bundled-toolchain bash -c '
    set -o pipefail
    tool="$1/dependencies/native-toolchain"
    export LD_LIBRARY_PATH="$tool/usr/lib64:$tool/usr/lib:$tool/lib64:$tool/lib"
    for cmd in gcc g++ cmake make readelf; do
      test -x "$tool/usr/bin/$cmd" || { echo "missing bundled $cmd"; exit 1; }
      "$tool/usr/bin/$cmd" --version | head -n 1 || exit 1
    done
  ' bash "$PACKAGE_HOME"
fi
if [[ -x $PACKAGE_HOME/runtime/spark/bin/spark-submit ]]; then
  check spark357-scala213 bash -c '
    banner=$(env -u SPARK_HOME -u SPARK_CONF_DIR -u JAVA_HOME -u JAVA_TOOL_OPTIONS -u _JAVA_OPTIONS -u JDK_JAVA_OPTIONS JAVA_HOME="$2/dependencies/jdk" "$1" --version 2>&1)
    printf "%s\n" "$banner"
    grep -Eq '\''version 3\.5\.7|Spark 3\.5\.7'\'' <<< "$banner" &&
      grep -Eq '\''Using Scala version 2\.13\.'\'' <<< "$banner"
  ' bash "$PACKAGE_HOME/runtime/spark/bin/spark-submit" "$PACKAGE_HOME"
fi
check bash-syntax bash -c 'while IFS= read -r -d "" f; do bash -n "$f" || exit; done < <(find "$1" -type f -name "*.sh" -print0)' bash "$HERE"
check python-syntax python3 - "$HERE" <<'PY'
import ast
import pathlib
import sys
for path in pathlib.Path(sys.argv[1]).rglob('*.py'):
    ast.parse(path.read_text(), filename=str(path))
    print(path)
PY
check discovery-tests python3 "$HERE/tests/test-discovery.py"
check topology-tests python3 "$HERE/tests/test-topology.py"
check path-policy-tests bash "$HERE/tests/test-path-policy.sh"
check node-checks bash "$HERE/tests/run-node-checks.sh"
check control-flow bash "$HERE/tests/run-control-flow.sh"
check offline-native-contracts bash "$HERE/01-offline-native-tests.sh" --workspace "$RUN/native-contracts"
if ((FAILED)); then result=FAIL; else result=PASS; fi
{
  printf 'result=%s\npackage=%s\nwork=%s\nhardware_exercised=false\nvendor_umdk_exercised=false\nreference_umdk_exercised=true\n' "$result" "$PACKAGE_HOME" "$RUN"
  cat "$RUN/checks.txt"
  if ((FAILED)); then
    printf '\nFailure details (full logs remain in %s):\n' "$RUN"
    while read -r _ name _; do
      [[ -f $RUN/$name.log ]] || continue
      printf '\n=== %s ===\n' "$name"
      tail -n 40 "$RUN/$name.log"
    done < <(grep '^FAIL ' "$RUN/checks.txt")
  fi
} > "$RUN/REPORT.txt"
if ((FAILED)); then cat "$RUN/REPORT.txt"; fi
printf 'PACKAGE_PREFLIGHT_%s report=%s\n' "$result" "$RUN/REPORT.txt"
((FAILED==0))
