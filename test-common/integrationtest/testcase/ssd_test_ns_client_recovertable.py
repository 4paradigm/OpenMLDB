# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import threading
import time
from libs.deco import multi_dimension
from libs.logger import infoLogger
import libs.ddt as ddt
from libs.test_loader import load
import libs.utils as utils

class TestRecoverTable(TestCaseBase):
    @multi_dimension(False)
    @ddt.data(
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_recover_table_normal(self,storage_mode):
        """
        测试recovertable函数。put数据之后是否会同步,先changleader，然后向新leader中put数据，再恢复之前的leader，测试是否恢复数据成功
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata_ssd(
            '"{}"'.format(name), None, 144000, 2,storage_mode,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"0-2"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false')
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs0)
        self.multidimension_vk = {'k1': ('string:index', 'testvalue0'),
                                  'k2': ('string', 'testvalue0'),
                                  'k3': ('string', 'testvalue0')}
        self.multidimension_scan_vk = {'k1': 'testvalue0'}

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]
        pid = 1

        rs_old_leader_put = self.put(self.leader, tid, pid, 'testkey0', self.now(), 'testvalue0')
        self.assertIn('Put ok', rs_old_leader_put)
        time.sleep(1)
        self.assertIn('testvalue0', self.scan(self.slave1, tid, pid, 'testkey0', self.now(), 1))
        self.assertIn('testvalue0', self.scan(self.slave2, tid, pid, 'testkey0', self.now(), 1))

        self.changeleader(self.ns_leader, name, pid, self.slave1)
        time.sleep(1)
        self.wait_op_done(name)
        rs_old_leader_put = self.put(self.leader, tid, pid, 'testkey1', self.now(), 'testvalue1')
        self.assertIn('Put ok', rs_old_leader_put)
        time.sleep(1)
        self.assertFalse('testvalue1' in self.scan(self.slave1, tid, pid, 'testkey1', self.now(), 1))
        self.assertFalse('testvalue1' in  self.scan(self.slave2, tid, pid, 'testkey1', self.now(), 1))

        rs_new_leader_put = self.put(self.slave1, tid, pid, 'testkey2', self.now(), 'testvalue2')
        self.assertIn('Put ok', rs_new_leader_put)
        time.sleep(2)
        self.assertFalse('testvalue2' in self.scan(self.leader, tid, pid, 'testkey2', self.now(), 1))
        self.assertTrue('testvalue2' in  self.scan(self.slave2, tid, pid, 'testkey2', self.now(), 1))

        self.ns_recover_table_cmd(self.ns_leader, 'recovertable', name, pid, self.leader)
        time.sleep(1)
        self.wait_op_done(name)
        time.sleep(1)
        self.assertTrue('testvalue2' in self.scan(self.leader, tid, pid, 'testkey2', self.now(), 1))



if __name__ == "__main__":
    load(TestRecoverTable)
