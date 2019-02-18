# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
from libs.deco import multi_dimension
from libs.logger import infoLogger
import libs.ddt as ddt
from libs.test_loader import load


@ddt.ddt
class TestScan(TestCaseBase):

    @multi_dimension(True)
    @ddt.data(
        ({'card': ('string:index', '0'), 's2': ('int32', 7)}, {'card': '0'}, ' 7 '),
        ({'card': ('string', '1'), 's2': ('int32', 7)}, {'card': '1'}, 'Fail to scan table'),
        ({'card': ('string', '2'), 's2': ('int32', 77)}, {'card': '2', 's2': '77'},
         'Invalid args, tid pid should be uint32_t, st and et should be uint64_t'),
        ({'card': ('string:index', '3'), 's2': ('int32:index', 77), 's3': ('int32:index', 88)},
         {'s2': '77'}, ' 88 '),
        ({'card': ('string', '4'), 's2': ('int32', 77), 's3': ('int32', 88)},
         {'card': '4'}, 'Fail to scan table'),
    )
    @ddt.unpack
    def test_sscan_index(self, kv, scan_kv, scan_value):
        """
        创建高维表，对scan功能进行测试
        :return:
        """
        self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{k: v[0] for k, v in kv.items()})
        self.put(self.leader, self.tid, self.pid, '', self.now(), *[str(v[1]) for v in kv.values()])
        infoLogger.info(self.scan(self.leader, self.tid, self.pid, scan_kv, self.now(), 1))
        self.assertIn(
            str(scan_value), self.scan(self.leader, self.tid, self.pid, scan_kv, self.now(), 1))


    def test_sscan_ttl(self):
        """
        创建表，已经过期的数据无法再scan出来
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 1, 2)
        self.assertIn('Create table ok', rs1)
        self.put(self.leader, self.tid, self.pid, 'testkey0', self.now() - 1000000000, 'testvalue0')
        infoLogger.info(self.scan(self.leader, self.tid, self.pid, 'testkey0', self.now(), 1))
        self.assertFalse('testvalue0' in self.scan(
            self.leader, self.tid, self.pid, 'testkey0', self.now(), 1))


    @multi_dimension(True)
    def test_sscan_1index(self):
        """
        创建高维表，card设置index，scan 1 0 card0 3 0，失败
        :return:
        """
        kv = {'card': ('string:index', 'card0'), 's2': ('int32', 7)}
        self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', **{k: v[0] for k, v in kv.items()})
        self.put(self.leader, self.tid, self.pid, '', self.now(), *[str(v[1]) for v in kv.values()])
        rs1 = self.run_client(self.leader, 'scan {} {} {} {} {}'.format(self.tid, self.pid, 'card0', self.now(), 1))
        infoLogger.info(rs1)
        self.assertIn('card0', rs1)

    def test_scan_preview(self):
        """
        预览
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 0, 8)
        self.assertIn('Create table ok', rs1)
        if self.multidimension:
            self.put(self.leader, self.tid, self.pid, '', 11, 'mcc0', '1.5', 'card0')
            self.put(self.leader, self.tid, self.pid, '', 22, 'mcc1', '2.5', 'card0')
            self.put(self.leader, self.tid, self.pid, '', 33, 'mcc1', '10.2', 'card1')
        else:
            self.put(self.leader, self.tid, self.pid, 'testkey0', 11, 'testvalue0')
            self.put(self.leader, self.tid, self.pid, 'testkey0', 22, 'testvalue1')
            self.put(self.leader, self.tid, self.pid, 'testkey1', 33, 'testvalue2')
        rs2 = self.preview(self.leader, self.tid, self.pid)
        rs3 = self.preview(self.leader, self.tid, self.pid, 2)
        infoLogger.info(rs2)
        if self.multidimension:
            self.assertEqual(3, len(rs2))
            self.assertEqual(2, len(rs3))
            self.assertEqual(rs2[0]['card'], 'card0')
            self.assertEqual(rs2[0]['merchant'], 'mcc0')
            self.assertEqual(rs2[1]['card'], 'card1')
            self.assertEqual(rs2[1]['merchant'], 'mcc1')
            self.assertEqual(rs2[2]['card'], 'card0')
            self.assertEqual(rs2[2]['merchant'], 'mcc1')
            self.assertEqual(2, len(rs3))
            self.assertEqual(rs3[0]['card'], 'card0')
            self.assertEqual(rs3[0]['merchant'], 'mcc0')
            self.assertEqual(rs3[1]['card'], 'card1')
            self.assertEqual(rs3[1]['merchant'], 'mcc1')
        else:    
            self.assertEqual(3, len(rs2))
            self.assertEqual(rs2[0]['key'], 'testkey0')
            self.assertEqual(rs2[0]['data'], 'testvalue1')
            self.assertEqual(rs2[1]['key'], 'testkey0')
            self.assertEqual(rs2[1]['data'], 'testvalue0')
            self.assertEqual(rs2[2]['key'], 'testkey1')
            self.assertEqual(rs2[2]['data'], 'testvalue2')
            self.assertEqual(2, len(rs3))
            self.assertEqual(rs3[0]['key'], 'testkey0')
            self.assertEqual(rs3[0]['data'], 'testvalue1')
            self.assertEqual(rs3[1]['key'], 'testkey0')
            self.assertEqual(rs3[1]['data'], 'testvalue0')

if __name__ == "__main__":
    load(TestScan)
