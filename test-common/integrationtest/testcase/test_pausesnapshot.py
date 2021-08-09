#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
        self.assertIn('Create table ok', rs1)
        rs2 = self.pausesnapshot(self.leader, self.tid, self.pid)
        self.assertIn('PauseSnapshot ok', rs2)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableFollower', 'kSnapshotPaused', 'true', '144000min', '0s'])


    @multi_dimension(False)
    def test_pausesnapshot_leader_can_put_can_be_synchronized(self):
        """
        暂停主节点指定表的snapshot，仍可以put数据且被同步
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'true')
        self.assertIn('Create table ok', rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertIn('Create table ok', rs2)
        rs = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('AddReplica ok', rs)
        rs3 = self.pausesnapshot(self.leader, self.tid, self.pid)
        self.assertIn('PauseSnapshot ok', rs3)

        rs4 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       'k',
                       self.now() - 1,
                       'v')
        self.assertIn('Put ok', rs4)
        time.sleep(1)

        self.assertIn('v', self.scan(self.slave1, self.tid, self.pid, 'k', self.now(), 1))
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['1', 'kTableLeader', 'kSnapshotPaused', 'true', '144000min', '0s'])


    @multi_dimension(True)
    def test_pausesnapshot_leader_can_put_can_be_synchronized_md(self):
        """
        暂停主节点指定表的snapshot，仍可以put数据且被同步
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'true')
        self.assertIn('Create table ok', rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertIn('Create table ok', rs2)
        rs3 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('AddReplica ok', rs3)
        time.sleep(1)
        rs4 = self.pausesnapshot(self.leader, self.tid, self.pid)
        self.assertIn('PauseSnapshot ok', rs4)

        rs5 = self.put(self.leader,
                       self.tid,
                       self.pid,
                       '',
                       self.now() - 1,
                       'v', '1.1', 'k')
        self.assertIn('Put ok', rs5)
        time.sleep(1)

        self.multidimension_scan_vk = {'card': 'k'}  # for multidimension test
        self.assertIn('v', self.scan(self.slave1, self.tid, self.pid, 'k', self.now(), 1))
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['1', 'kTableLeader', 'kSnapshotPaused', 'true', '144000min', '0s'])


if __name__ == "__main__":
    load(TestPauseSnapshot)
