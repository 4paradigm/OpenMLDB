# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
from libs.test_loader import load


class TestChangeRole(TestCaseBase):

    def test_changerole_to_leader_success(self):
        """
        换从节点为主节点后，mode变为Leader
        :return:
        """
        rs1 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false')
        self.assertIn('Create table ok', rs1)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])
        rs2 = self.changerole(self.slave1, self.tid, self.pid, 'leader')
        self.assertIn('ChangeRole ok', rs2)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])


    def test_changerole_to_leader_can_put(self):
        """
        换从节点为主节点后，mode变为Leader，可以成功put
        :return:
        """
        rs1 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false')
        self.assertIn('Create table ok', rs1)
        rs2 = self.changerole(self.slave1, self.tid, self.pid, 'leader')
        self.assertIn('ChangeRole ok', rs2)
        rs3 = self.put(self.slave1,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertIn('Put ok', rs3)


    def test_changerole_to_leader_can_makesnapshot(self):
        """
        切换从节点为主节点后，可以成功pausesnapshot
        :return:
        """
        rs1 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false')
        self.assertIn('Create table ok', rs1)
        rs2 = self.changerole(self.slave1, self.tid, self.pid, 'leader')
        self.assertIn('ChangeRole ok', rs2)
        rs3 = self.put(self.slave1,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertIn('Put ok', rs3)
        rs4 = self.makesnapshot(self.slave1, self.tid, self.pid)
        self.assertIn('MakeSnapshot ok', rs4)
        mf = self.get_manifest(self.slave1path, self.tid, self.pid)
        self.assertEqual(mf['offset'], '1')
        self.assertTrue(mf['name'])
        self.assertEqual(mf['count'], '1')


    def test_changerole_to_leader_can_pausesnapshot(self):
        """
        切换从节点为主节点后，可以成功pausesnapshot
        :return:
        """
        rs1 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 2, 'false')
        self.assertIn('Create table ok', rs1)
        rs2 = self.changerole(self.slave1, self.tid, self.pid, 'leader')
        self.assertIn('ChangeRole ok', rs2)
        rs3 = self.pausesnapshot(self.slave1, self.tid, self.pid)
        self.assertIn('PauseSnapshot ok', rs3)
        table_status = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableLeader', 'kSnapshotPaused', 'true', '144000min', '0s'])


    def test_changerole_to_leader_can_addreplica(self):
        """
        切换从节点为主节点后，可以成功addreplica slave，slave可以同步leader数据
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'false')
        self.assertIn('Create table ok', rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertIn('Create table ok', rs2)
        rs3 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('Fail to Add Replica', rs3)
        rs4 = self.changerole(self.leader, self.tid, self.pid, 'leader')
        self.assertIn('ChangeRole ok', rs4)
        rs5 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('AddReplica ok', rs5)
        rs6 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'testkey0',
                       self.now(),
                       'testvalue0')
        self.assertIn('Put ok', rs6)
        self.assertIn('testvalue0', self.scan(self.slave1, self.tid, self.pid, 'testkey0', self.now(), 1))


if __name__ == "__main__":
    load(TestChangeRole)
