# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
import os
import libs.conf as conf
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt

@ddt.ddt
class TestOfflineEndpoint(TestCaseBase):

    def test_offlineendpoint_master_killed(self):
        """
        offlineendpoint功能正常，主节点挂掉后，可以手工故障切换，切换后可put及同步数据
        :return:
        """
        self.start_client(self.leaderpath)
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(int(time.time() * 1000000 % 10000000000))
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false')
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs0)

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'false')
        self.confset(self.ns_leader, 'auto_recover_table', 'false')

        self.stop_client(self.leader)
        time.sleep(10)

        self.offlineendpoint(self.ns_leader, self.leader)

        rs2 = self.showtable(self.ns_leader)
        self.start_client(self.leaderpath)

        # showtable ok
        self.assertEqual(rs2[(name, tid, '1', self.leader)], ['leader', '2', '144000', 'no'])
        self.assertEqual(rs2[(name, tid, '2', self.leader)], ['leader', '2', '144000', 'no'])
        self.assertEqual(rs2[(name, tid, '3', self.leader)], ['leader', '2', '144000', 'no'])
        self.assertEqual(rs2[(name, tid, '1', self.slave1)], ['leader', '2', '144000', 'yes'])
        act1 = rs2[(name, tid, '2', self.slave1)]
        act2 = rs2[(name, tid, '2', self.slave2)]
        roles = [x[0] for x in [act1, act2]]
        self.assertEqual(roles.count('leader'), 1)
        self.assertEqual(roles.count('follower'), 1)
        self.assertEqual(rs2[(name, tid, '3', self.slave2)], ['leader', '2', '144000', 'yes'])

        # put and sync ok
        leader_new = self.slave1 if 'leader' in act1 else self.slave2
        follower = self.slave1 if 'follower' in act1 else self.slave2
        rs2 = self.put(self.slave1, tid, 1, '', self.now(), 'pk1' ,'v2', 'v3')
        self.assertTrue('Put ok' in rs2)
        rs3 = self.put(self.slave2, tid, 3, '', self.now(), 'pk1' ,'v2', 'v3')
        self.assertTrue('Put ok' in rs3)
        rs4 = self.put(leader_new, tid, 2, '', self.now(), 'pk1' ,'v2', 'v3')
        self.assertTrue('Put ok' in rs4)
        time.sleep(1)
        self.assertTrue('v2' in self.scan(follower, tid, 2, {'k1': 'pk1'}, self.now(), 1))


    def test_offlineendpoint_slave_killed(self):
        """
        offlineendpoint功能正常，从节点挂掉后，副本可以被删掉
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(int(time.time() * 1000000 % 10000000000))
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs0)

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'false')
        self.confset(self.ns_leader, 'auto_recover_table', 'false')

        self.stop_client(self.slave1)
        time.sleep(10)

        self.offlineendpoint(self.ns_leader, self.slave1)
        time.sleep(1)

        rs2 = self.showtable(self.ns_leader)
        self.start_client(self.slave1path)

        # showtable ok
        self.assertEqual(rs2[(name, tid, '1', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs2[(name, tid, '2', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs2[(name, tid, '3', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs2[(name, tid, '1', self.slave1)], ['follower', '2', '144000', 'no'])
        self.assertEqual(rs2[(name, tid, '2', self.slave1)], ['follower', '2', '144000', 'no'])
        self.assertEqual(rs2[(name, tid, '2', self.slave2)], ['follower', '2', '144000', 'yes'])
        self.assertEqual(rs2[(name, tid, '3', self.slave2)], ['follower', '2', '144000', 'yes'])


    @ddt.data(
        (conf.tb_endpoints[0][1], 'failed to offline endpoint. error msg: endpoint is healthy'),
        ('127.0.0.1:80', 'failed to offline endpoint. error msg: endpoint is not exist'),
    )
    @ddt.unpack
    def test_offlineendpoint_failed(self, endpoint, exp_msg):
        """
        offlineendpoint传入不正确的节点，执行失败
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format('tname{}'.format(time.time())), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertEqual('Create table ok' in rs1, True)

        rs2 = self.offlineendpoint(self.ns_leader, endpoint)
        self.assertEqual(exp_msg in rs2, True)


if __name__ == "__main__":
    load(TestOfflineEndpoint)
