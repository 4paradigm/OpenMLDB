# -*- coding: utf-8 -*-
import unittest
from framework import TestCaseBase
import xmlrunner
import time

class TestAddReplica(TestCaseBase):

    def test_addreplica_leader_add(self):
        '''
        主节点addreplica slave成功
        :return:
        '''
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        self.put(self.leader,
                       self.tid,
                       self.pid,
                       'k1',
                       self.now() - 1,
                       'v1')
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('Create table ok' in rs2)
        rs3 = self.create(self.slave2, 't', self.tid, self.pid, 144000, 8, 'false', self.slave2)
        self.assertTrue('Create table ok' in rs3)
        rs4 = self.addreplica(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('AddReplica ok' in rs4)
        rs5 = self.addreplica(self.leader, self.tid, self.pid, self.slave2)
        self.assertTrue('AddReplica ok' in rs5)
        table_status1 = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status1, ['1', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])
        table_status2 = self.get_table_status(self.slave2, self.tid, self.pid)
        self.assertEqual(table_status2, ['1', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 'k2',
                 self.now() - 1,
                 'v2')
        time.sleep(1)
        self.assertTrue('v1' in self.scan(self.slave1, self.tid, self.pid, 'k1', self.now(), 1))
        self.assertTrue('v2' in self.scan(self.slave1, self.tid, self.pid, 'k2', self.now(), 1))
        self.assertTrue('v1' in self.scan(self.slave2, self.tid, self.pid, 'k1', self.now(), 1))
        self.assertTrue('v2' in self.scan(self.slave2, self.tid, self.pid, 'k2', self.now(), 1))


    def test_addreplica_change_to_normal(self):
        '''
        主节点addreplica之后，状态变回normal
        :return:
        '''
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('Create table ok' in rs2)
        rs3 = self.pausesnapshot(self.slave1, self.tid, self.pid)
        self.assertTrue('PauseSnapshot ok' in rs3)
        rs4 = self.addreplica(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('AddReplica ok' in rs4)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])


    def test_addreplica_slave_cannot_add(self):
        '''
        从节点不允许addreplica
        :return:
        '''
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('Create table ok' in rs2)
        rs3 = self.addreplica(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('Fail to Add Replica' in rs3)


    def test_delreplica_slave_cannot_scan(self):
        '''
        主节点删除replica后put数据，slave scan不出来
        :return:
        '''
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs1)
        self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.create(self.slave2, 't', self.tid, self.pid, 144000, 8, 'false', self.slave2)
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 'k1',
                 self.now() - 1,
                 'v1')
        rs2 = self.delreplica(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('DelReplica ok' in rs2)
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 'k2',
                 self.now() - 1,
                 'v2')
        time.sleep(1)         
        self.assertTrue('v1' in self.scan(self.slave1, self.tid, self.pid, 'k1', self.now(), 1))
        self.assertFalse('v2' in self.scan(self.slave1, self.tid, self.pid, 'k2', self.now(), 1))
        self.assertTrue('v1' in self.scan(self.slave2, self.tid, self.pid, 'k1', self.now(), 1))
        self.assertTrue('v2' in self.scan(self.slave2, self.tid, self.pid, 'k2', self.now(), 1))


if __name__ == "__main__":
    import sys
    import os
    suite = unittest.TestSuite()
    if len(sys.argv) == 1:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestAddReplica)
    else:
        for test_name in sys.argv[1:]:
            suite.addTest(TestAddReplica(test_name))
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'))
    runner.run(suite)
