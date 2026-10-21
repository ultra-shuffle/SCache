"""Adapter control-flow fixture only; fake tools are not UB/compiler validation."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ENTRY = Path(__file__).resolve().with_name('run-package.sh')

class Adapter(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='rm-ctp-package-test-')
        self.root = Path(self.temp.name)
        self.package = self.root / 'package with spaces'
        self.work = self.root / 'work'
        self.sdk = self.root / 'sdk'
        for name in ('dependencies/jdk/bin', 'dependencies/native-toolchain', 'bin/real-ub'):
            (self.package / name).mkdir(parents=True)
        self.sdk.mkdir()
        for name in ('liburma.so', 'liburma_common.so', 'urma_api.h'):
            (self.sdk / name).write_text('fixture\n')
        (self.package / 'dependencies/jdk/release').write_text('JAVA_VERSION="17"\n')
        (self.package / 'dependencies/native-toolchain/SHA256SUMS').write_text('fixture\n')
        self.script('dependencies/jdk/bin/javac', 'echo "javac 17.0.1"')
        self.script('dependencies/jdk/bin/java', 'echo "fixture java"')
        self.script('bin/real-ub/00-detect-vendor-umdk.sh', '''
while (($#)); do
 case "$1" in --work-root) work=$2; shift 2;; --package-home) package=$2; shift 2;; *) shift;; esac
done
mkdir -p "$work/state"
sdk=$(dirname "$package")/sdk
for pair in "URMA_LIB:$sdk/liburma.so" "URMA_COMMON_LIB:$sdk/liburma_common.so" "URMA_INCLUDE:$sdk" "UMDK_LABEL:fixture" "DEVICE:" "PROVIDER_DIR:"; do
 printf '%s=%q\n' "${pair%%:*}" "${pair#*:}"
done > "$work/state/detected-umdk.env"
''')
        self.script('bin/build-lib-spark-urma.sh', '''
while (($#)); do case "$1" in --workspace) out=$2; shift 2;; *) shift;; esac; done
mkdir -p "$out/install/lib"
printf 'fake library\n' > "$out/install/lib/libSparkUrma.so"
printf 'result=PASS\nartifact=%s/install/lib/libSparkUrma.so\n' "$out" > "$out/status.txt"
echo FIXTURE_BUILD_CALLED
''')
    def tearDown(self):
        self.temp.cleanup()
    def script(self, path, body):
        target = self.package / path
        target.write_text('#!/bin/bash\nset -eu\n' + body + '\n')
        target.chmod(0o755)
    def run_adapter(self, *extra):
        env = dict(os.environ, JAVA_HOME='/nonexistent/old-java', JAVA_TOOL_OPTIONS='invalid-old-options')
        return subprocess.run(['bash', str(ENTRY), '--package-home', str(self.package),
                               '--workspace', str(self.work), '--phase', 'jni', *extra],
                              env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    def test_build_reuse_and_sdk_change(self):
        first = self.run_adapter()
        self.assertEqual(first.returncode, 0, first.stdout)
        self.assertIn('FIXTURE_BUILD_CALLED', first.stdout)
        second = self.run_adapter()
        self.assertEqual(second.returncode, 0, second.stdout)
        self.assertIn('RM_CTP_VENDOR_JNI_REUSED', second.stdout)
        (self.sdk / 'urma_api.h').write_text('changed header\n')
        third = self.run_adapter()
        self.assertEqual(third.returncode, 0, third.stdout)
        self.assertIn('FIXTURE_BUILD_CALLED', third.stdout)
    def test_corrupt_artifact_rebuild(self):
        self.assertEqual(self.run_adapter().returncode, 0)
        artifact = next(self.work.glob('build-view-*/output/install/lib/libSparkUrma.so'))
        artifact.write_text('corrupt')
        result = self.run_adapter()
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn('FIXTURE_BUILD_CALLED', result.stdout)
    def test_missing_classes_not_old_jar(self):
        result = self.run_adapter('--phase', 'check')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('New RM/CTP runtime missing', result.stdout)
        self.assertIn('FAILURE REPORT', result.stdout)
    def test_builder_error_visible(self):
        self.script('bin/build-lib-spark-urma.sh', 'echo FIXTURE_COMPILER_FAILURE; exit 7')
        result = self.run_adapter()
        self.assertEqual(result.returncode, 7, result.stdout)
        self.assertIn('FIXTURE_COMPILER_FAILURE', result.stdout)
        self.assertIn('exit_code=7', result.stdout)

unittest.main()
