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
import libs.ddt as ddt


@ddt.ddt
class TestAddReplica(TestCaseBase):

    @multi_dimension(False)
    def test_addreplica_leader_add(self):
        """
        主节点addreplica slave成功
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('Create table ok', rs1)
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 'k1',
                 self.now() - 1,
                 'v1')
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertIn('Create table ok', rs2)
        rs3 = self.create(self.slave2, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertIn('Create table ok', rs3)
        rs4 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('AddReplica ok', rs4)
        rs5 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave2)
        self.assertIn('AddReplica ok', rs5)
        table_status1 = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status1[:6], ['1', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])
        table_status2 = self.get_table_status(self.slave2, self.tid, self.pid)
        self.assertEqual(table_status2[:6], ['1', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 'k2',
                 self.now() - 1,
                 'v2')
        time.sleep(1)
        self.assertIn('v1', self.scan(self.slave1, self.tid, self.pid, 'k1', self.now(), 1))
        self.assertIn('v2', self.scan(self.slave1, self.tid, self.pid, 'k2', self.now(), 1))
        self.assertIn('v1', self.scan(self.slave2, self.tid, self.pid, 'k1', self.now(), 1))
        self.assertIn('v2', self.scan(self.slave2, self.tid, self.pid, 'k2', self.now(), 1))


    @multi_dimension(True)
    def test_addreplica_leader_add_md(self):
        """
        主节点addreplica slave成功
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('Create table ok', rs1)
        rs = self.put(self.leader,
                 self.tid,
                 self.pid,
                 '',
                 self.now() - 1,
                 'v1','1.1','k1')
        self.assertIn('Put ok', rs)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertIn('Create table ok', rs2)
        rs3 = self.create(self.slave2, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertIn('Create table ok', rs3)
        rs4 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('AddReplica ok', rs4)
        rs5 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave2)
        self.assertIn('AddReplica ok', rs5)
        table_status1 = self.get_table_status(self.slave1, self.tid, self.pid)
        self.assertEqual(table_status1[:6], ['1', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])
        table_status2 = self.get_table_status(self.slave2, self.tid, self.pid)
        self.assertEqual(table_status2[:6], ['1', 'kTableFollower', 'kTableNormal', 'true', '144000min', '0s'])
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 '',
                 self.now() - 1,
                 'v2', '1.1','k2')
        time.sleep(1)
        self.assertIn('v1', self.scan(self.slave1, self.tid, self.pid, {'card':'k1'}, self.now(), 1))
        self.assertIn('v2', self.scan(self.slave1, self.tid, self.pid, {'card':'k2'}, self.now(), 1))
        self.assertIn('v1', self.scan(self.slave2, self.tid, self.pid, {'card':'k1'}, self.now(), 1))
        self.assertIn('v2', self.scan(self.slave2, self.tid, self.pid, {'card':'k2'}, self.now(), 1))


    def test_addreplica_change_to_normal(self):
        """
        主节点addreplica之后，状态变回normal
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('Create table ok', rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertIn('Create table ok', rs2)
        rs3 = self.pausesnapshot(self.slave1, self.tid, self.pid)
        self.assertIn('PauseSnapshot ok', rs3)
        rs4 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('AddReplica ok', rs4)
        table_status = self.get_table_status(self.leader, self.tid, self.pid)
        self.assertEqual(table_status[:6], ['0', 'kTableLeader', 'kTableNormal', 'true', '144000min', '0s'])


    def test_addreplica_slave_cannot_add(self):
        """
        从节点不允许addreplica
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertIn('Create table ok', rs1)
        rs2 = self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.assertIn('Create table ok', rs2)
        rs3 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('Fail to Add Replica', rs3)


    @multi_dimension(False)
    def test_delreplica_slave_cannot_scan(self):
        """
        主节点删除replica后put数据，slave scan不出来
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid, 144000, 2, 'true')
        self.assertIn('Create table ok', rs1)
        self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.create(self.slave2, 't', self.tid, self.pid, 144000, 8, 'false')
        rs = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('AddReplica ok', rs)
        rs = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave2)
        self.assertIn('AddReplica ok', rs)
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 'k1',
                 self.now() - 1,
                 'v1')
        time.sleep(1)         
        rs2 = self.delreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('DelReplica ok', rs2)
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 'k2',
                 self.now() - 1,
                 'v2')
        time.sleep(1)
        self.assertIn('v1', self.scan(self.slave1, self.tid, self.pid, 'k1', self.now(), 1))
        self.assertFalse('v2' in self.scan(self.slave1, self.tid, self.pid, 'k2', self.now(), 1))
        self.assertIn('v1', self.scan(self.slave2, self.tid, self.pid, 'k1', self.now(), 1))
        self.assertIn('v2', self.scan(self.slave2, self.tid, self.pid, 'k2', self.now(), 1))


    @multi_dimension(True)
    def test_delreplica_slave_cannot_scan_md(self):
        """
        主节点删除replica后put数据，slave scan不出来
        :return:
        """
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('Create table ok', rs1)
        self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        self.create(self.slave2, 't', self.tid, self.pid, 144000, 8, 'false')
        rs2 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('AddReplica ok', rs2)
        rs2 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave2)
        self.assertIn('AddReplica ok', rs2)
        self.put(self.leader,
                 self.tid,
                 self.pid,
                 '',
                 self.now() - 1,
                 'v1', '1.1', 'k1')
        time.sleep(1)
        self.assertIn('v1', self.scan(self.slave2, self.tid, self.pid, {'card':'k1'}, self.now(), 1))
        rs3 = self.delreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('DelReplica ok', rs3)
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


    @multi_dimension(True)
    @ddt.data(
        ({'k2': ('string:index', 'testvalue1'),
          'k3': ('double', 1.1)}),
        ({'k0': ('string:index', 1.1),
          'k2': ('string:index', 'testvalue1'),
          'k3': ('double', 1.1)}),
        ({'k1': ('double:index', 1.1),
          'k2': ('string:index', 'testvalue1'),
          'k3': ('double', 1.1)}),
        ({'k1': ('string', 1.1),
          'k2': ('string:index', 'testvalue1'),
          'k3': ('double', 1.1)}),
    )
    def test_addreplica_fail_schema_mismatch(self, slave_schema):  # RTIDB-166
        """
        添加高维副本表时，副本schema主表不匹配，添加失败
        :return:
        """
        self.multidimension_vk = {'k1': ('string:index', 'testvalue0'),
                                  'k2': ('string:index', 'testvalue1'),
                                  'k3': ('double', 1.1)}
        rs1 = self.create(self.leader, 't', self.tid, self.pid)
        self.assertIn('Create table ok', rs1)
        self.multidimension_vk = slave_schema
        self.create(self.slave1, 't', self.tid, self.pid, 144000, 8, 'false')
        rs2 = self.addreplica(self.leader, self.tid, self.pid, 'client', self.slave1)
        self.assertIn('AddReplica ok', rs2)



if __name__ == "__main__":
    load(TestAddReplica)
