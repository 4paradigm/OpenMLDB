# -*- coding: utf-8 -*-
import unittest
from framework import TestCaseBase

class TestAddReplica(TestCaseBase):

    def test_addreplica_leader_add(self):
        '''
        主节点addreplica slave成功
        :return:
        '''
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('Create table ok' in rs2)
        rs3 = self.addreplica(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('AddReplica ok' in rs3)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableFollower', 'kTableNormal', '144000'])


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
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kTableNormal', '144000'])


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


if __name__ == "__main__":
    import sys
    suite = unittest.TestSuite()
    if len(sys.argv) == 1:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestAddReplica)
    else:
        for test_name in sys.argv[1:]:
            suite.addTest(TestAddReplica(test_name))
    unittest.TextTestRunner(verbosity=2).run(suite)