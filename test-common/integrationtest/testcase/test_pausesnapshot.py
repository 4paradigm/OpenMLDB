# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
from libs.test_loader import load
from libs.deco import multi_dimension


class TestPauseSnapshot(TestCaseBase):

    def test_pausesnapshot_slave_can_be_paused(self):
        """
        从节点允许暂停snapshot
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.pausesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('PauseSnapshot ok' in rs2)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableFollower', 'kSnapshotPaused', 'true', '144000min', '0s'])


    @multi_dimension(False)
    def test_pausesnapshot_leader_can_put_can_be_synchronized(self):
        """
        暂停主节点指定表的snapshot，仍可以put数据且被同步
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'true', self.slave1)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false', self.slave1)
        self.assertTrue('Create table ok' in rs2)
        rs3 = self.pausesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('PauseSnapshot ok' in rs3)

        rs4 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'k',
                       self.now() - 1,
                       'v')
        self.assertTrue('Put ok', rs4)
        time.sleep(1)

        self.assertTrue('v' in self.scan(self.slave1, self.tid, self.pid, 'k', self.now(), 1))
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['1', 'kTableLeader', 'kSnapshotPaused', 'true', '144000min', '0s'])


    @multi_dimension(True)
    def test_pausesnapshot_leader_can_put_can_be_synchronized_md(self):
        """
        暂停主节点指定表的snapshot，仍可以put数据且被同步
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'true')
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertTrue('Create table ok' in rs2)
        rs3 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertTrue('AddReplica ok' in rs3)
        time.sleep(1)
        rs4 = self.pausesnapshot(self.leader, self.tid, self.pid)
        self.assertTrue('PauseSnapshot ok' in rs4)

        rs5 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       '',
                       self.now() - 1,
                       'v', '1.1', 'k')
        self.assertTrue('Put ok', rs5)
        time.sleep(1)

        self.multidimension_scan_vk = {'card': 'k'}  # for multidimension test
        self.assertTrue('v' in self.scan(self.slave1, self.tid, self.pid, 'k', self.now(), 1))
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['1', 'kTableLeader', 'kSnapshotPaused', 'true', '144000min', '0s'])


if __name__ == "__main__":
    load(TestPauseSnapshot)
