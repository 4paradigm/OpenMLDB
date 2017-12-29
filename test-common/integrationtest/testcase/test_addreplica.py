# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
from libs.test_loader import load
from libs.deco import multi_dimension


class TestAddReplica(TestCaseBase):

    @multi_dimension(False)
    def test_addreplica_leader_add(self):
        """
        主节点addreplica slave成功
        :return:
        """
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
        rs4 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertTrue('AddReplica ok' in rs4)
        rs5 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave2)
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


    @multi_dimension(True)
    def test_addreplica_leader_add_md(self):
        """
        主节点addreplica slave成功
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 '',
                 self.now() - 1,
                 'v1','1.1','k1')
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('Create table ok' in rs2)
        rs3 = self.create(self.slave2, 't', self.tid, self.pid, 144000, 8, 'false', self.slave2)
        self.assertTrue('Create table ok' in rs3)
        rs4 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertTrue('AddReplica ok' in rs4)
        rs5 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave2)
        self.assertTrue('AddReplica ok' in rs5)
        table_status1 = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status1, ['1', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])
        table_status2 = self.get_table_status(self.slave2, self.tid, self.pid)
        self.assertEqual(table_status2, ['1', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 '',
                 self.now() - 1,
                 'v2', '1.1','k2')
        time.sleep(1)
        self.assertTrue('v1' in self.scan(self.slave1, self.tid, self.pid, {'card':'k1'}, self.now(), 1))
        self.assertTrue('v2' in self.scan(self.slave1, self.tid, self.pid, {'card':'k2'}, self.now(), 1))
        self.assertTrue('v1' in self.scan(self.slave2, self.tid, self.pid, {'card':'k1'}, self.now(), 1))
        self.assertTrue('v2' in self.scan(self.slave2, self.tid, self.pid, {'card':'k2'}, self.now(), 1))


    def test_addreplica_change_to_normal(self):
        """
        主节点addreplica之后，状态变回normal
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('Create table ok' in rs2)
        rs3 = self.pausesnapshot(self.slave1, self.tid, self.pid)
        self.assertTrue('PauseSnapshot ok' in rs3)
        rs4 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertTrue('AddReplica ok' in rs4)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status, ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])


    def test_addreplica_slave_cannot_add(self):
        """
        从节点不允许addreplica
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertTrue('Create table ok' in rs2)
        rs3 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertTrue('Fail to Add Replica' in rs3)


    @multi_dimension(False)
    def test_delreplica_slave_cannot_scan(self):
        """
        主节点删除replica后put数据，slave scan不出来
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true', self.slave1, self.slave2)
        self.assertTrue('Create table ok' in rs1)
        self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.create(self.slave2, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 'k1',
                 self.now() - 1,
                 'v1')
        time.sleep(1)         
        rs2 = self.delreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
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


    @multi_dimension(True)
    def test_delreplica_slave_cannot_scan_md(self):
        """
        主节点删除replica后put数据，slave scan不出来
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertTrue('Create table ok' in rs1)
        self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.create(self.slave2, 't', self.tid, self.pid, 144000, 8, 'false', self.slave2)
        rs2 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertTrue('AddReplica ok' in rs2)
        rs2 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave2)
        self.assertTrue('AddReplica ok' in rs2)
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 '',
                 self.now() - 1,
                 'v1', '1.1', 'k1')
        time.sleep(1)
        self.assertTrue('v1' in self.scan(self.slave2, self.tid, self.pid, {'card':'k1'}, self.now(), 1))
        rs3 = self.delreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertTrue('DelReplica ok' in rs3)
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 '',
                 self.now() - 1,
                 'v2', '1.1', 'k2')
        time.sleep(1)
        self.assertFalse('v1' not in self.scan(self.slave1, self.tid, self.pid, {'card':'k1'}, self.now(), 1))
        self.assertFalse('v2' in self.scan(self.slave1, self.tid, self.pid, {'card':'k2'}, self.now(), 1))
        self.assertFalse('v1' not in self.scan(self.slave2, self.tid, self.pid, {'card':'k1'}, self.now(), 1))
        self.assertFalse('v2' not in self.scan(self.slave2, self.tid, self.pid, {'card':'k2'}, self.now(), 1))


if __name__ == "__main__":
    load(TestAddReplica)
