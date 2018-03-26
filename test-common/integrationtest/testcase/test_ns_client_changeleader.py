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


    def test_changeleader_master_disconnect(self):
        """
        changeleader功能正常，主节点断网后，可以手工故障切换，切换成功后从节点可以同步数据
        :return:
        """
        self.start_client(self.leader)
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"0-2"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false')
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs0)
        self.multidimension_vk = {'k1': ('string:index', 'testvalue0'),
                                  'k2': ('string', 'testvalue0'),
                                  'k3': ('string', 'testvalue0')}
        self.multidimension_scan_vk = {'k1': 'testvalue0'}

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'false')
        self.confset(self.ns_leader, 'auto_recover_table', 'false')

        self.disconnectzk(self.leader)
        time.sleep(10)

        self.changeleader(self.ns_leader, name, 0)

        rs2 = self.showtable(self.ns_leader)
        self.connectzk(self.leader)

        self.assertEqual(rs2[(name, tid, '0', self.leader)], ['leader', '2', '144000', 'no'])
        self.assertEqual(rs2[(name, tid, '1', self.leader)], ['leader', '2', '144000', 'no'])
        self.assertEqual(rs2[(name, tid, '2', self.leader)], ['leader', '2', '144000', 'no'])
        act1 = rs2[(name, tid, '0', self.slave1)]
        act2 = rs2[(name, tid, '0', self.slave2)]
        roles = [x[0] for x in [act1, act2]]
        self.assertEqual(roles.count('leader'), 1)
        self.assertEqual(roles.count('follower'), 1)

        leader_new = self.slave1 if 'leader' in act1 else self.slave2
        follower = self.slave1 if 'follower' in act1 else self.slave2
        rs2 = self.put(self.leader, tid, 1, 'testkey0', self.now(), 'testvalue0')
        self.assertIn('Put ok', rs2)
        rs3 = self.put(self.slave1, tid, 1, 'testkey0', self.now(), 'testvalue0')
        self.assertIn('Put failed', rs3)
        rs4 = self.put(leader_new, tid, 0, 'testkey0', self.now(), 'testvalue0')
        self.assertIn('Put ok', rs4)
        time.sleep(1)
        self.assertIn('testvalue0', self.scan(follower, tid, 0, 'testkey0', self.now(), 1))


    def test_changeleader_master_killed(self):
        """
        changeleader功能正常，主节点挂掉后，可以手工故障切换，切换成功后从节点可以同步数据
        原主节点启动后可以手工recoversnapshot成功
        :return:
        """
        self.start_client(self.leader)
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"0-1"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false')
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs0)
        self.multidimension_vk = {'k1': ('string:index', 'testvalue0'),
                                  'k2': ('string', 'testvalue0'),
                                  'k3': ('string', 'testvalue0')}
        self.multidimension_scan_vk = {'k1': 'testvalue0'}

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'false')
        self.confset(self.ns_leader, 'auto_recover_table', 'false')

        self.stop_client(self.leader)
        time.sleep(10)

        self.changeleader(self.ns_leader, name, 0)
        time.sleep(1)
        rs2 = self.showtable(self.ns_leader)
        self.start_client(self.leader)
        time.sleep(3)
        self.assertEqual(rs2[(name, tid, '0', self.leader)], ['leader', '2', '144000', 'no'])
        self.assertEqual(rs2[(name, tid, '1', self.leader)], ['leader', '2', '144000', 'no'])
        self.assertEqual(rs2[(name, tid, '2', self.leader)], ['leader', '2', '144000', 'no'])
        act1 = rs2[(name, tid, '0', self.slave1)]
        act2 = rs2[(name, tid, '0', self.slave2)]
        roles = [x[0] for x in [act1, act2]]
        self.assertEqual(roles.count('leader'), 1)
        self.assertEqual(roles.count('follower'), 1)

        leader_new = self.slave1 if 'leader' in act1 else self.slave2
        follower = self.slave1 if 'follower' in act1 else self.slave2
        rs2 = self.put(self.leader, tid, 1, 'testkey0', self.now(), 'testvalue0')
        rs3 = self.put(self.slave1, tid, 1, 'testkey0', self.now(), 'testvalue0')
        rs4 = self.put(leader_new, tid, 0, 'testkey0', self.now(), 'testvalue0')
        self.assertFalse('Put ok' in rs2)
        self.assertFalse('Put ok' in rs3)
        self.assertIn('Put ok', rs4)
        time.sleep(1)
        self.assertIn('testvalue0', self.scan(follower, tid, 0, 'testkey0', self.now(), 1))
        self.recoverendpoint(self.ns_leader, self.leader)
        time.sleep(10)


    def test_changeleader_master_alive(self):
        """
        changeleader传入有主节点的表，执行失败
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs1)

        rs2 = self.changeleader(self.ns_leader, name, 0)
        self.assertIn('failed to change leader', rs2)


    def test_changeleader_tname_notexist(self):
        """
        changeleader传入不存在的表名，执行失败
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format('tname{}'.format(time.time())), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs1)

        rs2 = self.changeleader(self.ns_leader, 'nullnullnull', 0)
        self.assertIn('failed to change leader', rs2)


if __name__ == "__main__":
    load(TestChangeLeader)
