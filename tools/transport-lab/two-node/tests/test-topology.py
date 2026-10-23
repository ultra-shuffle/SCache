"""Run the actual verifier against synthetic /proc and Master replies; no Spark."""
from pathlib import Path
import runpy
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parent.parent / 'lib/verify-topology.py'
REAL_PATH = type(Path('.'))


class Topology(unittest.TestCase):
    def invoke(self, worker_pids, scache_pids):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            proc = root / 'proc'
            (proc / 'sys/kernel/random').mkdir(parents=True)
            (proc / 'sys/kernel/random/boot_id').write_text('independent-test-boot')
            for pids, cls in ((worker_pids, 'org.apache.spark.deploy.worker.Worker'),
                              (scache_pids, 'org.scache.deploy.NettyBlockTransferIntegrationNode')):
                for pid in pids:
                    (proc / str(pid)).mkdir()
                    (proc / str(pid) / 'cmdline').write_bytes(b'java\0' + cls.encode() + b'\0')
            (root / 'spark-worker.pid').write_text('123')
            (root / 'scache-node.pid').write_text('456')
            def path(value):
                if value == '/proc':
                    return proc
                if str(value).startswith('/proc/'):
                    return proc / str(value)[6:]
                return REAL_PATH(value)
            argv = [str(SCRIPT), '--run', d, '--node', 'B', '--ip', '10.1.1.2',
                    '--master', '10.1.1.1', '--peer', '10.1.1.1']
            with patch('pathlib.Path', side_effect=path), patch.object(sys, 'argv', argv):
                runpy.run_path(str(SCRIPT), run_name='__main__')

    def test_exactly_one_each(self):
        self.invoke([123], [456])

    def test_missing_scache_rejected(self):
        with self.assertRaisesRegex(SystemExit, 'TOPOLOGY_PROCESS_MISMATCH'):
            self.invoke([123], [])

    def test_second_worker_rejected(self):
        with self.assertRaisesRegex(SystemExit, 'TOPOLOGY_PROCESS_MISMATCH'):
            self.invoke([123, 124], [456])

    def test_unrelated_worker_pid_rejected(self):
        with self.assertRaisesRegex(SystemExit, 'TOPOLOGY_PROCESS_MISMATCH'):
            self.invoke([999], [456])


if __name__ == '__main__':
    unittest.main()
