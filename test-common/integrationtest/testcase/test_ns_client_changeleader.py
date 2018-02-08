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
class TestChangeLeader(TestCaseBase):

    def test_changeleader_master_killed(self):
        """
        changeleader功能正常，主节点挂掉后，可以手工故障切换
        :return:
        """
        self.start_client(self.leaderpath)
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"1-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"1-3"', 'false'),
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

        self.changeleader(self.ns_leader, name, 1)

        rs2 = self.showtable(self.ns_leader)
        self.start_client(self.leaderpath)

        self.assertEqual(rs2[(name, tid, '1', self.leader)], ['leader', '2', '144000', 'no'])
        self.assertEqual(rs2[(name, tid, '2', self.leader)], ['leader', '2', '144000', 'yes'])
        self.assertEqual(rs2[(name, tid, '3', self.leader)], ['leader', '2', '144000', 'yes'])
        act1 = rs2[(name, tid, '1', self.slave1)]
        act2 = rs2[(name, tid, '1', self.slave2)]
        roles = [x[0] for x in [act1, act2]]
        self.assertEqual(roles.count('leader'), 1)
        self.assertEqual(roles.count('follower'), 1)

        leader_new = self.slave1 if 'leader' in act1 else self.slave2
        follower = self.slave1 if 'follower' in act1 else self.slave2
        rs2 = self.put(self.leader, tid, 2, 'testkey0', self.now(), 'testvalue0')
        self.assertTrue('Put failed' in rs2)
        rs3 = self.put(self.slave1, tid, 2, 'testkey0', self.now(), 'testvalue0')
        self.assertTrue('Put failed' in rs3)
        rs4 = self.put(leader_new, tid, 1, 'testkey0', self.now(), 'testvalue0')
        self.assertTrue('Put ok' in rs4)
        time.sleep(1)
        self.assertTrue('testvalue0' in self.scan(follower, tid, 1, 'testkey0', self.now(), 1))


    def test_changeleader_master_alive(self):
        """
        changeleader传入有主节点的表，执行失败
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"1-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertEqual('Create table ok' in rs1, True)

        rs2 = self.changeleader(self.ns_leader, name, 1)
        self.assertEqual('failed to change leader. error msg: change leader failed' in rs2, True)


    def test_changeleader_tname_notexist(self):
        """
        changeleader传入不存在的表明，执行失败
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format('tname{}'.format(time.time())), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"1-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"1-2"', 'false'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertEqual('Create table ok' in rs1, True)

        rs2 = self.changeleader(self.ns_leader, 'nullnullnull', 1)
        self.assertEqual('failed to change leader. error msg: change leader failed' in rs2, True)


if __name__ == "__main__":
    load(TestChangeLeader)
