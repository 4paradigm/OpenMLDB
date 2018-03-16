# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
import os
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt
import collections

@ddt.ddt
class TestAutoFailover(TestCaseBase):

    @ddt.data(
        'killed','network_failure'
    )
    def test_auto_failover_master_exception(self, failover_reason):
        """
        auto_failover=true：主节点挂掉或断网后，自动切换到新的主节点，原主is_alive为no
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertEqual('Create table ok' in rs0, True)

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'true')
        self.confset(self.ns_leader, 'auto_recover_table', 'false')

        if failover_reason == 'killed':
            self.stop_client(self.leader)
        elif failover_reason == 'network_failure':
            self.disconnectzk(self.leader)
        time.sleep(10)

        rs2 = self.showtablet(self.ns_leader)
        rs3 = self.showtable(self.ns_leader)
        if failover_reason == 'killed':
            self.start_client(self.leader)
        elif failover_reason == 'network_failure':
            self.connectzk(self.leader)
        time.sleep(10)
        self.assertEqual('kTabletOffline' in rs2[self.leader], True)

        # leader to offline
        self.assertEqual(rs3[(name, tid, '0', self.leader)], ['leader', '2', '144000', 'no'])  # RTIDB-203
        self.assertEqual(rs3[(name, tid, '1', self.leader)], ['leader', '2', '144000', 'no'])
        self.assertEqual(rs3[(name, tid, '2', self.leader)], ['leader', '2', '144000', 'no'])
        self.assertEqual(rs3[(name, tid, '3', self.leader)], ['leader', '2', '144000', 'no'])

        # slave to leader
        self.assertEqual(rs3[(name, tid, '1', self.slave1)], ['leader', '2', '144000', 'yes'])
        act1 = rs3[(name, tid, '2', self.slave1)]
        act2 = rs3[(name, tid, '2', self.slave2)]
        roles = [x[0] for x in [act1, act2]]
        self.assertEqual(roles.count('leader'), 1)
        self.assertEqual(roles.count('follower'), 1)
        self.assertEqual(rs3[(name, tid, '3', self.slave2)], ['leader', '2', '144000', 'yes'])


    @ddt.data(
        'killed','network_failure'
    )
    def test_auto_failover_slave_exception(self, failover_reason):
        """
        auto_failover=true：从节点挂掉或断网后，showtable中从节点is_alive为no
        auto_failover=true：从节点挂掉或断网后，showtablet中从节点为offline状态
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-2"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertEqual('Create table ok' in rs0, True)

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'true')
        self.confset(self.ns_leader, 'auto_recover_table', 'false')

        if failover_reason == 'killed':
            self.stop_client(self.slave1)
        elif failover_reason == 'network_failure':
            self.disconnectzk(self.slave1)
        time.sleep(10)

        rs2 = self.showtablet(self.ns_leader)
        rs3 = self.showtable(self.ns_leader)
        if failover_reason == 'killed':
            self.start_client(self.slave1)
        elif failover_reason == 'network_failure':
            self.connectzk(self.slave1)
        self.assertEqual('kTabletOffline' in rs2[self.slave1], True)

        self.assertEqual(rs3[(name, tid, '0', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '1', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '2', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '0', self.slave1)], ['follower', '2', '144000', 'no'])
        self.assertEqual(rs3[(name, tid, '1', self.slave1)], ['follower', '2', '144000', 'no'])
        self.assertEqual(rs3[(name, tid, '1', self.slave2)], ['follower', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '2', self.slave2)], ['follower', '2', '144000', 'yes'])


    def test_auto_failover_slave_network_flashbreak(self):
        """
        auto_failover=true：连续两次主节点闪断，故障切换成功
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs0)

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'true')
        self.confset(self.ns_leader, 'auto_recover_table', 'true')

        self.connectzk(self.leader)  # flashbreak
        self.showtable(self.ns_leader)
        time.sleep(10)
        rs2 = self.showtable(self.ns_leader)
        self.connectzk(self.slave1)  # flashbreak
        self.showtable(self.ns_leader)
        time.sleep(10)
        rs3 = self.showtable(self.ns_leader)
        rs4 = self.showtablet(self.ns_leader)
        self.assertEqual('kTabletHealthy' in rs4[self.leader], True)
        self.assertEqual('kTabletHealthy' in rs4[self.slave1], True)

        self.assertEqual(rs2[(name, tid, '0', self.leader)], ['follower', '2', '144000', 'yes'])
        self.assertEqual(rs2[(name, tid, '1', self.leader)], ['follower', '2', '144000', 'yes'])
        self.assertEqual(rs2[(name, tid, '2', self.leader)], ['follower', '2', '144000', 'yes'])
        self.assertEqual(rs2[(name, tid, '0', self.slave1)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs2[(name, tid, '1', self.slave1)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs2[(name, tid, '2', self.slave2)], ['leader', '2', '144000', 'yes'])

        self.assertEqual(rs3[(name, tid, '0', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '1', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '2', self.leader)], ['follower', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '0', self.slave1)], ['follower', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '1', self.slave1)], ['follower', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '2', self.slave2)], ['leader', '2', '144000', 'yes'])


    def test_select_leader(self):
        """
        slave1改为leader role，put数据后改回follower role，leader发生故障后，新主会切换到slave1，数据同步正确
        :return:
        """
        self.confset(self.ns_leader, 'auto_failover', 'true')
        self.confset(self.ns_leader, 'auto_recover_table', 'false')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"0"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'true'),
            ('column_desc', '"k3"', '"string"', 'true')
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertEqual('Create table ok' in rs1, True)
        rs2 = self.showtable(self.ns_leader)
        tid = rs2.keys()[0][1]
        pid = 0

        d = {'k1': ('string:index', 'card1'),
             'k2': ('string:index', 'card2'),
             'k3': ('string:index', 'card3')}
        self.multidimension_vk = collections.OrderedDict(sorted(d.items(), key = lambda t:t[0]))

        self.changerole(self.slave1, tid, pid, 'leader')
        rs3 = self.put(self.slave1, tid, pid, "testkey0", self.now() + 9999, "testvalue0")
        self.assertEqual("ok" in rs3, True)
        self.changerole(self.slave1, tid, pid, 'follower')

        self.stop_client(self.leader)
        time.sleep(10)
        self.showtable(self.ns_leader)
        new_tb_leader1 = self.find_new_tb_leader(name, tid, pid)

        self.start_client(self.leader)
        time.sleep(8)
        self.recoverendpoint(self.ns_leader, self.leader)
        self.showtable(self.ns_leader)

        self.changerole(self.slave2, tid, pid, 'leader')
        rs4 = self.put(self.slave2, tid, pid, "testkey0", self.now() + 9999, "testvalue0")
        self.assertEqual("ok" in rs4, True)
        self.changerole(self.slave2, tid, pid, 'follower')

        self.disconnectzk(new_tb_leader1)
        time.sleep(10)
        self.showtable(self.ns_leader)
        new_tb_leader2 = self.find_new_tb_leader(name, tid, pid)

        self.connectzk(new_tb_leader1)
        time.sleep(10)
        self.showtable(self.ns_leader)

        self.assertEqual(new_tb_leader1, self.slave1)
        self.assertEqual(new_tb_leader2, self.slave2)

        d = {'k1': ('string:index', 'ccard1'),
             'k2': ('string:index', 'ccard2'),
             'k3': ('string:index', 'ccard3')}
        self.multidimension_vk = collections.OrderedDict(sorted(d.items(), key = lambda t:t[0]))
        self.multidimension_scan_vk = {'k1': 'ccard1'}
        rs5 = self.put(new_tb_leader2, tid, pid, "testkey1", self.now() + 9999, "ccard1")
        self.assertEqual("ok" in rs5, True)
        self.assertTrue(
            'ccard1' in self.scan(self.leader, tid, pid, 'testkey1', self.now() + 9999, 1))


if __name__ == "__main__":
    load(TestAutoFailover)
