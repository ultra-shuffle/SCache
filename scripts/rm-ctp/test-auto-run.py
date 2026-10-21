import argparse
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('auto_run', Path(__file__).with_name('auto-run.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Automatic(unittest.TestCase):
    def test_missing_source_tree_cannot_produce_version_id(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / 'build.sbt').write_text('stub')
            native = root / 'native/spark_urma'
            native.mkdir(parents=True)
            (native / 'CMakeLists.txt').write_text('stub')
            with patch.object(m, 'REPO', root):
                with self.assertRaises(subprocess.CalledProcessError):
                    m.source_id()
                with self.assertRaises(subprocess.CalledProcessError):
                    m.native_source_id()

    def test_runner_identity_changes_when_adapter_changes(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            for name in ('auto-run.py', 'run-all.sh', 'run-package.sh', 'run-node.sh',
                         'select-endpoint.py', 'package-env.sh'):
                (root / name).write_text('original')
            original = m.runner_source_id(root)
            (root / 'run-node.sh').write_text('changed')
            self.assertNotEqual(original, m.runner_source_id(root))

    def test_bounded_search_spaces_and_no_symlink_recursion(self):
        with tempfile.TemporaryDirectory(prefix='search spaces ') as d:
            root = Path(d)
            (root / 'one/two').mkdir(parents=True)
            (root / 'cycle').symlink_to(root, target_is_directory=True)
            got = list(m.candidates([root], depth=1))
            self.assertIn(root / 'one', got)
            self.assertNotIn(root / 'one/two', got)
            self.assertNotIn(root / 'cycle', got)

    def test_message_roundtrip(self):
        a, b = socket.socketpair()
        with a, b:
            m.send(a, {'role': 'A', 'port': 19171})
            self.assertEqual(m.receive(b), {'role': 'A', 'port': 19171})

    def test_disconnect_not_pass(self):
        a, b = socket.socketpair()
        b.close()
        with a:
            with self.assertRaisesRegex(RuntimeError, 'PEER_DISCONNECTED'):
                m.receive(a)

    def test_no_free_port(self):
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            self.assertFalse(m.free_ports('127.0.0.1', s.getsockname()[1]))

    def test_runtime_hash_and_source(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / 'source-id.txt').write_text('abc\n')
            (root / 'classpath.relative').write_text('entry-0\n')
            (root / 'entry-0').write_text('runtime')
            result = subprocess.check_output(['sha256sum', 'source-id.txt', 'classpath.relative', 'entry-0'], cwd=root)
            (root / 'SHA256SUMS').write_bytes(result)
            self.assertTrue(m.runtime_ok(root, 'abc'))
            self.assertFalse(m.runtime_ok(root, 'other'))
            (root / 'entry-0').write_text('corrupted')
            self.assertFalse(m.runtime_ok(root, 'abc'))

    def test_recover_stale_saved_paths(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            work = root / 'work'
            (work / 'state').mkdir(parents=True)
            (work / 'state/auto-paths.json').write_text(json.dumps({'package': '/missing', 'classes': '/missing'}))
            package = root / 'moved package'
            for rel in ('dependencies/jdk/bin/java', 'dependencies/jdk/bin/javac',
                        'dependencies/native-toolchain/SHA256SUMS', 'bin/build-lib-spark-urma.sh',
                        'bin/real-ub/00-detect-vendor-umdk.sh'):
                f = package / rel
                f.parent.mkdir(parents=True, exist_ok=True)
                f.touch()
            runtime = root / 'new runtime'
            runtime.mkdir()
            (runtime / 'source-id.txt').write_text('abc\n')
            (runtime / 'classpath.relative').write_text('entry-0\n')
            (runtime / 'entry-0').write_text('new-runtime')
            (runtime / 'SHA256SUMS').write_bytes(subprocess.check_output(['sha256sum', 'source-id.txt', 'classpath.relative', 'entry-0'], cwd=runtime))
            a = argparse.Namespace(package_home=str(package), classes=None, search_root=[str(root)])
            found = m.discover(a, work, 'abc')
            self.assertEqual(found, (package, runtime))
            saved = json.loads((work / 'state/auto-paths.json').read_text())
            self.assertEqual(saved['classes'], str(runtime))

    def test_two_sided_rendezvous(self):
        with socket.socket() as s:
            s.bind(('127.0.0.1', 0))
            port = s.getsockname()[1]
        server = argparse.Namespace(role='A', sync_port=port, peer_ip='127.0.0.1', wait_seconds=20)
        client = argparse.Namespace(role='B', sync_port=port, peer_ip='127.0.0.1', wait_seconds=20)
        result = []
        def serve():
            with m.rendezvous(server, '127.0.0.1') as conn:
                conn.settimeout(10)
                result.append(m.receive(conn))
                m.send(conn, {'start': True})
        t = threading.Thread(target=serve)
        t.start()
        with m.rendezvous(client, '127.0.0.1') as conn:
            conn.settimeout(10)
            m.send(conn, {'prepared': True})
            self.assertEqual(m.receive(conn), {'start': True})
        t.join(20)
        self.assertFalse(t.is_alive())
        self.assertEqual(result, [{'prepared': True}])

    def test_complete_two_process_control_flow_with_stub_adapter(self):
        # Loopback is enabled ONLY inside this test harness. No UB calls occur.
        with tempfile.TemporaryDirectory(prefix='auto fixture ') as d:
            root = Path(d)
            package = root / 'package'
            for rel in ('dependencies/jdk/bin/java', 'dependencies/jdk/bin/javac',
                        'dependencies/native-toolchain/SHA256SUMS', 'bin/build-lib-spark-urma.sh',
                        'bin/real-ub/00-detect-vendor-umdk.sh'):
                f = package / rel
                f.parent.mkdir(parents=True, exist_ok=True)
                f.touch()
            runtime = root / 'runtime'
            runtime.mkdir()
            (runtime / 'source-id.txt').write_text(m.source_id() + '\n')
            (runtime / 'classpath.relative').write_text('entry-0\n')
            (runtime / 'entry-0').write_text('stub runtime, not Java')
            (runtime / 'SHA256SUMS').write_bytes(subprocess.check_output(['sha256sum', 'source-id.txt', 'classpath.relative', 'entry-0'], cwd=runtime))
            (root / 'run-package.sh').write_text('''#!/usr/bin/env bash
set -eu
phase= role=
while (($#)); do
  case "$1" in
    --phase) phase=$2;;
    --role) role=$2;;
  esac
  shift 2
done
echo "STUB_PHASE=$phase"
if [[ $phase == launch && $role == A ]]; then echo 'RM_CTP_SERVICE_READY stub'; fi
sleep 0.1
if [[ $phase == launch ]]; then echo 'RM_CTP_SERVICE_PASS stub'; fi
echo 'STUB_DONE'
''')
            harness = '''import importlib.util,sys,types
from pathlib import Path
spec=importlib.util.spec_from_file_location('runner',sys.argv[1])
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
m.HERE=Path(sys.argv[2])
m.ipaddress.ip_address=lambda value:types.SimpleNamespace(is_loopback=False)
sys.argv=[sys.argv[0]]+sys.argv[3:]
m.main()
'''
            with socket.socket() as s:
                s.bind(('127.0.0.1', 0))
                sync = s.getsockname()[1]
            processes = []
            for role, local, peer in [('A', '127.0.0.2', '127.0.0.3'), ('B', '127.0.0.3', '127.0.0.2')]:
                cmd = [sys.executable, '-c', harness, str(Path(m.__file__)), str(root),
                       '--role', role, '--local-ip', local, '--peer-ip', peer,
                       '--package-home', str(package), '--classes', str(runtime),
                       '--workspace', str(root / role), '--sync-port', str(sync), '--wait-seconds', '15']
                processes.append(subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True))
            try:
                for proc in processes:
                    output = proc.communicate(timeout=30)[0]
                    self.assertEqual(proc.returncode, 0, output)
                    self.assertIn('SCACHE_RM_CTP_TWO_NODE_PASS', output)
                    self.assertLess(output.index('STUB_PHASE=check'), output.index('STUB_PHASE=launch'))
            finally:
                for proc in processes:
                    if proc.poll() is None:
                        proc.kill(); proc.wait()


if __name__ == '__main__':
    unittest.main()
