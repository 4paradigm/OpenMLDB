# -*- coding: utf-8 -*-
import unittest
from framework import TestCaseBase
import xmlrunner

class TestChangeRole(TestCaseBase):

    def test_changerole_to_leader(self):
        '''
        换从节点为主节点后，mode变为Leader
        :return:
        '''
        rs1 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.changerole(self.slave1, self.tid, self.pid, 'leader')
        self.assertTrue('ChangeRole ok' in rs2)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kTableNormal', '144000'])


    def test_changerole_to_leader_can_put(self):
        '''
        换从节点为主节点后，mode变为Leader
        :return:
        '''
        rs1 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.changerole(self.slave1, self.tid, self.pid, 'leader')
        self.assertTrue('ChangeRole ok' in rs2)
        rs3 = self.put(self.slave1,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertTrue('Put ok' in rs3)


    def test_changerole_to_leader_can_makesnapshot(self):
        '''
        切换从节点为主节点后，可以成功pausesnapshot
        :return:
        '''
        rs1 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.changerole(self.slave1, self.tid, self.pid, 'leader')
        self.assertTrue('ChangeRole ok' in rs2)
        rs3 = self.put(self.slave1,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertTrue('Put ok' in rs3)
        rs4 = self.makesnapshot(self.slave1, self.tid, self.pid)
        self.assertTrue('MakeSnapshot ok' in rs4)
        mf = self.get_manifest(self.slave1path, self.tid, self.pid)
        self.assertEqual(mf['offset'], '1')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '1')


    def test_changerole_to_leader_can_pausesnapshot(self):
        '''
        切换从节点为主节点后，可以成功pausesnapshot
        :return:
        '''
        rs1 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.changerole(self.slave1, self.tid, self.pid, 'leader')
        self.assertTrue('ChangeRole ok' in rs2)
        rs3 = self.pausesnapshot(self.slave1, self.tid, self.pid)
        self.assertTrue('PauseSnapshot ok' in rs3)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kSnapshotPaused', '144000'])


    def test_changerole_to_leader_can_addreplica(self):
        '''
        切换从节点为主节点后，可以成功addreplica slave，slave可以同步leader数据
        :return:
        '''
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'false', self.leader)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.changerole(self.leader, self.tid, self.pid, 'leader')
        self.assertTrue('ChangeRole ok' in rs2)
        rs3 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('Create table ok' in rs3)
        rs4 = self.addreplica(self.leader, self.tid, self.pid, self.slave1)
        self.assertTrue('AddReplica ok' in rs4)
        rs5 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertTrue('Put ok' in rs5)
        self.assertTrue('testvalue0' in self.scan(self.slave1, self.tid, self.pid, 'testkey0', self.now(), 1))


if __name__ == "__main__":
    import sys
    import os
    suite = unittest.TestSuite()
    if len(sys.argv) == 1:
        suite = unittest.TestLoader().loadTestsFromTestCase(TestChangeRole)
    else:
        for test_name in sys.argv[1:]:
            suite.addTest(TestChangeRole(test_name))
    runner = xmlrunner.XMLTestRunner(output=os.getenv('reportpath'))
    runner.run(suite)
