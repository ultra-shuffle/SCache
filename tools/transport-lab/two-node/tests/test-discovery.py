#!/usr/bin/env python3
"""Selection policy tests with synthetic data; not hardware verification."""
import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('discovery', Path(__file__).parents[1] / 'lib/discover.py')
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)

class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.providers = []
        for name in ('liburma_ubagg.so.1', 'liburma-udma.so'):
            p = Path(self.tmp.name) / name
            p.write_bytes(name.encode())
            self.providers.append({'path': str(p), 'rank': 10})
        self.devices = {'bonding_dev_0': {'driver': 'ub_agg', 'read_write': True},
                        'udmac0d1e2': {'driver': 'udma', 'read_write': True}}
        self.caps = {n: {'modes': 7, 'rm_ctp': 1, 'rc_rtp': 0, 'errors': [],
                        'eids': {0: '1' * 32}, 'ports': [[0, 4]],
                        'max_read': 268435456, 'max_write': 268435456,
                        'max_msg': 65536} for n in self.devices}
        self.priorities = {n: {0: 'RTP', 6: 'CTP', 7: 'CTP'} for n in self.devices}
        self.visible = {'bonding_dev_0': 'ACTIVE'}

    def select(self, **kw):
        return d.select_endpoint(self.devices, self.caps, self.priorities,
                                 self.visible, self.providers, kw.pop('mode', 'rm-ctp'), **kw)

    def test_unique_visible_provider(self):
        e = self.select(priority='6')
        self.assertEqual(e['device'], 'bonding_dev_0')
        self.assertIn('ubagg', e['provider'])

    def test_timeout_output_remains_readable_text(self):
        log = Path(self.tmp.name) / 'timed-out-tool.log'
        failure = d.subprocess.TimeoutExpired(['vendor-query'], .1,
            output=b'PROVIDER_ERROR\nlate diagnostic\xff\n')
        with mock.patch.object(d.subprocess, 'run', side_effect=failure):
            text = d.run_command(['vendor-query'], log, timeout=.1, accepted=(124,))
        self.assertTrue(text.startswith('PROVIDER_ERROR\nlate diagnostic'))
        self.assertIn('COMMAND_TIMEOUT', text)
        self.assertEqual(log.read_text(), text)

    def test_timeout_failure_keeps_diagnostic_in_exception_and_file(self):
        log = Path(self.tmp.name) / 'timeout-failure.log'
        failure = d.subprocess.TimeoutExpired(['vendor-query'], .1,
            output=b'first provider failure\n')
        with mock.patch.object(d.subprocess, 'run', side_effect=failure):
            with self.assertRaisesRegex(d.DiscoveryError, 'first provider failure\n'):
                d.run_command(['vendor-query'], log, timeout=.1)
        self.assertTrue(log.read_text().startswith('first provider failure\n'))

    def test_sdk_interface_check_only_compiles_never_executes(self):
        out = Path(self.tmp.name)
        files = {'api': '/sdk/include/urma_api.h', 'core': '/sdk/lib/liburma.so',
                 'common': '/sdk/lib/liburma_common.so'}
        with mock.patch.object(d, 'run_command', return_value='') as run:
            result = d.inspect_sdk_link_interfaces(files, out, '/bundled/gcc', ['-nostdinc'], {})
        self.assertEqual(run.call_count, 2)
        for call in run.call_args_list:
            args = call.args[0]
            self.assertEqual(args[0], '/bundled/gcc')
            self.assertIn('-nostdinc', args)
            self.assertIn('/sdk/lib/liburma.so', args)
            self.assertIn('/sdk/lib/liburma_common.so', args)
        self.assertTrue(result['interfaces']['context_export']['compile_link'])
        self.assertFalse(result['probes_executed'])
        self.assertEqual(result['provider_implementation'], 'not-verified')
        self.assertTrue((out / 'sdk-link-interfaces.json').is_file())

    def test_legacy_optional_api_does_not_abort_discovery(self):
        out = Path(self.tmp.name)
        files = {'api': '/sdk/include/urma_api.h', 'core': '/sdk/lib/liburma.so',
                 'common': '/sdk/lib/liburma_common.so'}
        with mock.patch.object(d, 'run_command', side_effect=[d.DiscoveryError('old API'), '']):
            result = d.inspect_sdk_link_interfaces(files, out, '/bundled/gcc', [], {})
        self.assertFalse(result['interfaces']['context_export']['compile_link'])
        self.assertTrue(result['interfaces']['explicit_bond_context']['compile_link'])

    def test_link_context_reads_features_but_does_not_choose_eids(self):
        out = Path(self.tmp.name)
        feature = out / 'feature-source'
        feature.write_text('URMA CTP-ROI+\nURMA UTP-UNO-\n')
        with mock.patch.object(d, 'run_command', return_value='topology text') as run:
            result = d.collect_optional_link_evidence('/selected/urma_admin', out, {}, feature)
        self.assertEqual(run.call_args.args[0], ['/selected/urma_admin', 'show', 'topo'])
        self.assertEqual(run.call_args.kwargs['timeout'], 15)
        self.assertTrue(result['utp_uno_disabled'])
        self.assertEqual((out / 'platform-ub-feature.log').read_text(), feature.read_text())
        self.assertNotIn('eid', result)

    def test_optional_topology_and_feature_failures_do_not_abort_discovery(self):
        out = Path(self.tmp.name)
        with mock.patch.object(d, 'run_command', side_effect=d.DiscoveryError('permission denied')):
            result = d.collect_optional_link_evidence('/selected/urma_admin', out, {}, out / 'missing')
        self.assertEqual(result['topology'], 'unavailable-see-log')
        self.assertEqual(result['platform_features'], 'unavailable-see-log')
        self.assertIsNone(result['utp_uno_disabled'])
        self.assertIn('UB_FEATURE_UNAVAILABLE', (out / 'platform-ub-feature.log').read_text())

    def test_priority_not_guessed(self):
        with self.assertRaisesRegex(d.DiscoveryError, 'PRIORITY_SELECTION_REQUIRED'):
            self.select()

    def test_priority_mismatch(self):
        with self.assertRaisesRegex(d.DiscoveryError, 'PRIORITY_INVALID'):
            self.select(priority='0')

    def test_explicit_physical(self):
        self.assertIn('udma.so', self.select(device='udmac0d1e2', priority='6')['provider'])

    def test_multiple_visible(self):
        self.visible['udmac0d1e2'] = 'ACTIVE'
        with self.assertRaisesRegex(d.DiscoveryError, 'DEVICE_SELECTION_REQUIRED'):
            self.select(priority='6')

    def test_capability_not_coarse_mask(self):
        with self.assertRaisesRegex(d.DiscoveryError, 'rc_rtp=0'):
            self.select(mode='rc-rtp', priority='0')

    def test_no_permission(self):
        self.devices['bonding_dev_0']['read_write'] = False
        with self.assertRaisesRegex(d.DiscoveryError, 'permission-denied'):
            self.select(priority='6')

    def test_multi_eid(self):
        self.caps['bonding_dev_0']['eids'][2] = '2' * 32
        with self.assertRaisesRegex(d.DiscoveryError, 'EID_SELECTION_REQUIRED'):
            self.select(priority='6')
        self.assertEqual(self.select(eid='2', priority='6')['eid_index'], 2)

    def test_all_ambiguous_fields_reported_together(self):
        self.caps['bonding_dev_0']['eids'][2] = '2' * 32
        lines = d.candidate_config_lines('A', self.devices, self.caps, self.priorities,
                                         self.visible, self.providers, 'rm-ctp')
        bond = next(line for line in lines if 'A_DEVICE=bonding_dev_0' in line)
        self.assertIn('A_EID=0,2', bond)
        self.assertIn('A_PRIORITY=6,7', bond)
        self.assertIn('liburma_ubagg.so.1', bond)

    def test_wrong_provider(self):
        with self.assertRaisesRegex(d.DiscoveryError, 'PROVIDER_DEVICE_MISMATCH'):
            self.select(provider=self.providers[1]['path'], priority='6')

    def test_previous_choice(self):
        e = self.select(priority='6')
        self.assertEqual(self.select(previous=e), e)

    def test_device_switch_does_not_reuse_aggregate_provider(self):
        old = self.select(priority='6')
        selected = self.select(device='udmac0d1e2', priority='7', previous=old)
        self.assertIn('udma.so', selected['provider'])
        self.assertEqual(selected['priority'], 7)

    def test_device_switch_does_not_reuse_another_devices_eid(self):
        old = self.select(priority='6')
        self.caps['udmac0d1e2']['eids'][2] = '2' * 32
        with self.assertRaisesRegex(d.DiscoveryError, 'EID_SELECTION_REQUIRED'):
            self.select(device='udmac0d1e2', priority='7', previous=old)
        selected = self.select(device='udmac0d1e2', eid='2', priority='7', previous=old)
        self.assertEqual(selected['eid_index'], 2)

    def test_device_switch_does_not_reuse_another_devices_priority(self):
        old = self.select(priority='6')
        with self.assertRaisesRegex(d.DiscoveryError, 'PRIORITY_SELECTION_REQUIRED'):
            self.select(device='udmac0d1e2', previous=old)

    def test_state_changed(self):
        e = self.select(priority='6')
        old = {'endpoint': e, 'identity': {'kernel': 'one'}}
        d.check_state(old, e, old['identity'], 'reuse')
        with self.assertRaisesRegex(d.DiscoveryError, 'DISCOVERY_CHANGED'):
            d.check_state(old, e, {'kernel': 'two'}, 'reuse')

    def test_requires_stage10(self):
        with self.assertRaisesRegex(d.DiscoveryError, 'NO_CONFIRMED_SELECTION'):
            d.check_state(None, {}, {}, 'reuse')

    def test_admin_parser(self):
        result = d.parse_admin('name : a\npriority : 0 6 7\ntp_type : RTP CTP CTP\nname : b\npriority : 0\ntp_type : RTP\n')
        self.assertEqual(result, {'a': {0: 'RTP', 6: 'CTP', 7: 'CTP'}, 'b': {0: 'RTP'}})

    def test_probe_parser(self):
        result = d.parse_probe('CAP\ta\t7\t0\t0\t0\t1\t1024\nMSG\ta\t65536\nEID\ta\t2\t' + '1'*32 + '\n')
        self.assertEqual(result['a']['rm_ctp'], 1)
        self.assertEqual(result['a']['max_msg'], 65536)
        self.assertEqual(result['a']['eids'], {2: '1'*32})

    def test_down_device(self):
        self.visible = {}
        self.caps['bonding_dev_0']['ports'] = [[0, 1]]
        with self.assertRaisesRegex(d.DiscoveryError, 'link-not-confirmed-active'):
            self.select(priority='6')

if __name__ == '__main__':
    unittest.main(verbosity=2)
