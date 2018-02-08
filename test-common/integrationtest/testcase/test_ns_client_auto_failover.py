# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
import os
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt

@ddt.ddt
class TestAutoFailover(TestCaseBase):

    def test_auto_failover_master_killed(self):
        """
        auto_failover=true：主节点挂掉后，自动选择offset最大的从节点作为主节点，原主is_alive为no
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(int(time.time() * 1000000 % 10000000000))
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"1-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs0)

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'true')

        self.stop_client(self.leader)
        time.sleep(10)

        rs2 = self.showtablet(self.ns_leader)
        rs3 = self.showtable(self.ns_leader)
        self.start_client(self.leaderpath)
        self.assertEqual('kTabletOffline' in rs2[self.leader], True)

        # leader to offline
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


    def test_auto_failover_slave_killed(self):
        """
        auto_failover=true：从节点挂掉后，showtable中从节点is_alive为no
        auto_failover=true：从节点挂掉后，showtablet中从节点为offline状态
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(int(time.time() * 1000000 % 10000000000))
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"1-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs0)

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'true')

        self.stop_client(self.slave1)
        time.sleep(10)

        rs2 = self.showtablet(self.ns_leader)
        rs3 = self.showtable(self.ns_leader)
        self.start_client(self.slave1path)
        self.assertEqual('kTabletOffline' in rs2[self.slave1], True)

        self.assertEqual(rs3[(name, tid, '1', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '2', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '3', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '1', self.slave1)], ['follower', '2', '144000', 'no'])
        self.assertEqual(rs3[(name, tid, '2', self.slave1)], ['follower', '2', '144000', 'no'])
        self.assertEqual(rs3[(name, tid, '2', self.slave2)], ['follower', '2', '144000', 'yes'])
        self.assertEqual(rs3[(name, tid, '3', self.slave2)], ['follower', '2', '144000', 'yes'])


if __name__ == "__main__":
    load(TestAutoFailover)
