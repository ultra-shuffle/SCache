import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('endpoint', Path(__file__).with_name('select-endpoint.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class Selection(unittest.TestCase):
    text = '''name : bonding_dev_0
transport_type : 0 [UB]
trans_mode : 0x7 [RM(Reliable message) RC(Reliable connection) UM(Unreliable message) ]
eid0 : 4245:4944:0000:0000:0000:0000:0100:0000
priority : 0 6 7
tp_type : RTP CTP CTP
name : other
transport_type : 0 [UB]
trans_mode : 0x1 [RM(Reliable message) ]
eid3 : 1234:0000:0000:0000:0000:0000:0000:0001
priority : 0 7
tp_type : RTP CTP
'''
    def test_visible(self):
        self.assertEqual(m.devices('0 bonding_dev_0 UB eid0 123 ACTIVE\n1 other UB eid0 456 DOWN'), ['bonding_dev_0'])
    def test_unique(self):
        self.assertEqual(m.parameters(self.text, 'other'), (3, 7))
    def test_multiple_ctp_priorities_require_explicit_choice(self):
        with self.assertRaisesRegex(ValueError, 'CTP_PRIORITY_SELECTION_REQUIRED'):
            m.parameters(self.text, 'bonding_dev_0')
    def test_explicit(self):
        self.assertEqual(m.parameters(self.text, 'bonding_dev_0', priority='6'), (0, 6))
    def test_wrong_tp(self):
        with self.assertRaisesRegex(ValueError, 'CTP_PRIORITY_INVALID'):
            m.parameters(self.text, 'bonding_dev_0', priority='0')
    def test_stale_eid(self):
        with self.assertRaisesRegex(ValueError, 'EID_INVALID'):
            m.parameters(self.text, 'bonding_dev_0', eid='8', priority='6')
    def test_zero_eid(self):
        with self.assertRaisesRegex(ValueError, 'EID_SELECTION_REQUIRED'):
            m.parameters('name : x\ntransport_type : 0 [UB]\ntrans_mode : 0x1 [RM(Reliable message) ]\neid0 : 0000\npriority : 6\ntp_type : CTP', 'x')

    def test_ctp_without_rm_is_rejected(self):
        no_rm = self.text.replace('0x7 [RM(Reliable message) RC(Reliable connection) UM(Unreliable message) ]',
                                  '0x2 [RC(Reliable connection) ]')
        with self.assertRaisesRegex(ValueError, 'RM_MODE_REQUIRED'):
            m.parameters(no_rm, 'bonding_dev_0')

    def test_ctp_on_non_ub_transport_is_rejected(self):
        non_ub = self.text.replace('transport_type : 0 [UB]', 'transport_type : 1 [IB]', 1)
        with self.assertRaisesRegex(ValueError, 'UB_TRANSPORT_REQUIRED'):
            m.parameters(non_ub, 'bonding_dev_0')

    device_table = ('sysfs_entry\tubdev\tdriver_name\ttransport_type\tsysfs_path\n'
                    'bonding_dev_0\tbonding_dev_0\tub_agg\t0\t/sys/bonding\n'
                    'udmac0d1e2\tudmac0d1e2\tudma\t0\t/sys/udma\n')
    candidate_table = ('provider\t20\t/usr/lib64/urma/liburma-udma.so\tsystem\n'
                       'provider\t20\t/usr/lib64/urma/liburma_ubagg.so.0.0.3\tsystem\n')

    def test_bond_provider_ignores_physical_device(self):
        self.assertEqual(m.provider_for_device(self.device_table, self.candidate_table,
                                               'bonding_dev_0'),
                         '/usr/lib64/urma/liburma_ubagg.so.0.0.3')

    def test_physical_provider_ignores_bond_device(self):
        self.assertEqual(m.provider_for_device(self.device_table, self.candidate_table,
                                               'udmac0d1e2'),
                         '/usr/lib64/urma/liburma-udma.so')

    def test_unknown_device_cannot_choose_provider(self):
        with self.assertRaisesRegex(ValueError, 'DEVICE_DRIVER_SELECTION_REQUIRED'):
            m.provider_for_device(self.device_table, self.candidate_table, 'unknown')

    def test_unknown_driver_requires_explicit_provider(self):
        changed = self.device_table.replace('ub_agg', 'new_driver')
        with self.assertRaisesRegex(ValueError, 'PROVIDER_DRIVER_UNKNOWN'):
            m.provider_for_device(changed, self.candidate_table, 'bonding_dev_0')

    def test_matching_provider_can_have_higher_search_rank(self):
        changed = self.candidate_table.replace('provider\t20\t/usr/lib64/urma/liburma-udma.so',
                                               'provider\t10\t/usr/lib64/urma/liburma-udma.so')
        self.assertEqual(m.provider_for_device(self.device_table, changed, 'bonding_dev_0'),
                         '/usr/lib64/urma/liburma_ubagg.so.0.0.3')

    def test_provider_substring_is_not_a_driver_match(self):
        changed = 'provider\t10\t/usr/lib64/urma/liburma_custom_udma_test.so\tsystem\n'
        with self.assertRaisesRegex(ValueError, 'PROVIDER_SELECTION_REQUIRED'):
            m.provider_for_device(self.device_table, changed, 'udmac0d1e2')

unittest.main()
