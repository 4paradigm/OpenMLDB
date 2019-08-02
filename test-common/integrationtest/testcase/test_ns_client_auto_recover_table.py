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
class TestAutoRecoverTable(TestCaseBase):

    def createtable_put(self, data_count, data_thread=2):
        self.tname = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(self.tname), '"kAbsoluteTime"', 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-9"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-9"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-9"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        table_info = self.showtable(self.ns_leader, self.tname)
        self.tid = int(table_info.keys()[0][1])
        self.pid = 3
        self.put_large_datas(data_count, data_thread)

    def put_data(self, endpoint):
        rs = self.put(endpoint, self.tid, self.pid, "testkey0", self.now() + 1000, "testvalue0")
        self.assertIn("ok", rs)

    @staticmethod
    def get_steps_dict():
        return {
            -1: 'time.sleep(5)',
            0: 'time.sleep(10)',
            1: 'self.createtable_put(1)',
            2: 'self.stop_client(self.leader)',
            3: 'self.disconnectzk(self.leader)',
            4: 'self.stop_client(self.slave1)',
            5: 'self.disconnectzk(self.slave1)',
            6: 'self.find_new_tb_leader(self.tname, self.tid, self.pid)',
            7: 'self.put_data(self.leader)',
            8: 'self.put_data(self.new_tb_leader)',
            10: 'self.makesnapshot(self.leader, self.tid, self.pid)',
            11: 'self.makesnapshot(self.slave1, self.tid, self.pid), self.makesnapshot(self.slave2, self.tid, self.pid)',
            12: 'self.makesnapshot(self.ns_leader, self.tname, self.pid, \'ns_client\')',
            13: 'self.start_client(self.leader)',
            14: 'self.start_client(self.slave1)',
            15: 'self.connectzk(self.leader)',
            16: 'self.connectzk(self.slave1)',
            17: 'self.assertEqual(self.get_op_by_opid(self.latest_opid), "kReAddReplicaOP")',
            18: 'self.assertEqual(self.get_op_by_opid(self.latest_opid), "kReAddReplicaNoSendOP")',
            19: 'self.assertEqual(self.get_op_by_opid(self.latest_opid), "kReAddReplicaWithDropOP")',
            20: 'self.assertEqual(self.get_op_by_opid(self.latest_opid), "kReAddReplicaSimplifyOP")',
            21: 'self.check_re_add_replica_op(self.latest_opid)',
            22: 'self.check_re_add_replica_no_send_op(self.latest_opid)',
            23: 'self.check_re_add_replica_with_drop_op(self.latest_opid)',
            24: 'self.check_re_add_replica_simplify_op(self.latest_opid)',
            33: 'self.get_latest_opid_by_tname_pid(self.tname, self.pid)',
            34: 'self.confset(self.ns_leader, "auto_failover", "true")',
            35: 'self.confset(self.ns_leader, "auto_failover", "false")',
            36: 'self.wait_op_done(self.tname)',
        }

    @ddt.data(
        (34, 1, 3, -1, 36, 6, 15, -1, 36, 33, 20, 24, 35),  # failover not finish and start recover  RTIDB-259
        (34, 1, 2, -1, 36, 6, 13, -1, 36, 33, 17, 21, 35),  # failover not finish and start recover  RTIDB-259
        (34, 1, 3, -1, 36, 6, 15, -1, 36, 33, 20, 24, 35),  # offset = manifest.offset
        (34, 1, 3, -1, 36, 6, 12, 15, -1, 36, 33, 20, 35),  # offset = manifest.offset
        (34, 1, 3, -1, 36, 6, 8, 15, -1, 36, 33, 20, 35),  # offset = manifest.offset  RTIDB-210
        (34, 1, 3, -1, 36, 6, 8, 12, 15, -1, 36, 33, 19, 23, 35),  # offset < manifest.offset
        (34, 1, 12, 3, -1, 12, 15, -1, 36, 33, 20, 35),  # offset = manifest.offset
        (34, 1, 11, 7, 10, 3, -1, 15, -1, 36, 33, 20, 35),  # offset > manifest.offset
        (34, 1, 3, -1, 36, 6, 7, 15, -1, 36, 33, 19, 35),  # not match
        (34, 1, 3, -1, 36, 6, 7, 12, 15, -1, 36, 33, 19, 35),  # not match
        (34, 1, 3, -1, 36, 6, 7, 8, 15, -1, 36, 33, 19, 35),  # not match
        (34, 1, 3, 0, 7, 10, 2, -1, 12, 13, -1, 36, 33, 17, 35),  # not match
        (34, 1, 12, 36, 2, -1, 36, 6, 12, 13, -1, 36, 33, 18, 22, 35),  # offset = manifest.offset
        (34, 1, 11, -1, 7, 10, 2, 0, 13, -1, 36, 33, 18, 35),  # 12 offset > manifest.offset
        (34, 1, 11, -1, 7, 7, 10, 2, -1, 36, 6, 8, 13, -1, 36, 33, 18, 35),  # 13 offset > manifest.offset
        (34, 1, 2, -1, 36, 6, 13, -1, 36, 33, 17, 21, 35),  # offset < manifest.offset
        (34, 1, 2, -1, 36, 6, 12, 13, -1, 36, 33, 17, 35),  # offset < manifest.offset
        (34, 1, 2, -1, 36, 6, 8, 13, -1, 36, 33, 17, 35),
        (34, 1, 2, -1, 36, 6, 10, 12, 13, -1, 36, 33, 17, 35),
        (34, 1, 2, -1, 36, 6, 8, 12, 13, -1, 36, 33, 17, 35),
        (34, 1, 2, -1, 36, 6, 8, 12, 8, 13, -1, 36, 33, 17, 35),  # 19 new leader makesnapshot and put data, ori leader recover
        (34, 1, 5, -1, 16, -1, 36, 33, 20, 35),
        (34, 1, 4, 0, 14, -1, 36, 33, 17, 35),  # RTIDB-213
        (34, 1, 12, 3, 7, 2, 0, 13, -1, 36, 33, 18, 35),  # RTIDB-222
    )
    @ddt.unpack
    def test_auto_recover_table(self, *steps):
        """
        tablet故障恢复流程测试
        :param steps:
        :return:
        """
        self.get_new_ns_leader()
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])

        rs = self.showtable(self.ns_leader, self.tname)
        role_x = [v[0] for k, v in rs.items()]
        is_alive_x = [v[-2] for k, v in rs.items()]
        for repeat in range(10):
            rs = self.showtable(self.ns_leader, self.tname)
            role_x = [v[0] for k, v in rs.items()]
            is_alive_x = [v[-2] for k, v in rs.items()]
            if role_x.count('leader') == 10 and role_x.count('follower') == 18 and is_alive_x.count('yes') == 28:
                break
            time.sleep(2)
        self.assertEqual(role_x.count('leader'), 10)
        self.assertEqual(role_x.count('follower'), 18)
        self.assertEqual(is_alive_x.count('yes'), 28)
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[0],
                         self.get_table_status(self.slave1, self.tid, self.pid)[0])
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[0],
                         self.get_table_status(self.slave2, self.tid, self.pid)[0])
        self.ns_drop(self.ns_leader, self.tname)


    @TestCaseBase.skip('FIXME')
    @ddt.data(
        (3, 0, 6, 32, 7, 15, 28, 0, 29, 0, 30),  # recover when ns killed: RTIDB-243
    )
    @ddt.unpack
    def test_auto_recover_table_ns_killed(self, *steps):
        """
        ns_leader挂掉，可以sendsnapshot成功，可以故障恢复成功
        :param steps:
        :return:
        """
        self.update_conf(self.slave1path, 'stream_block_size', 1)
        self.update_conf(self.slave1path, 'stream_bandwidth_limit', 1)
        self.update_conf(self.slave2path, 'stream_block_size', 1)
        self.update_conf(self.slave2path, 'stream_bandwidth_limit', 1)
        self.stop_client(self.slave1)
        self.stop_client(self.slave2)
        time.sleep(5)
        self.start_client(self.slave1)
        self.start_client(self.slave2)

        self.createtable_put(50, 7)
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])
        rs = self.showtable(self.ns_leader, self.tname)
        role_x = [v[0] for k, v in rs.items()]
        is_alive_x = [v[-1] for k, v in rs.items()]
        self.get_table_status(self.leader)

        self.update_conf(self.slave1path, 'stream_bandwidth_limit', 0)
        self.update_conf(self.slave2path, 'stream_bandwidth_limit', 0)
        self.stop_client(self.slave1)
        self.stop_client(self.slave2)
        time.sleep(5)
        self.start_client(self.slave1)
        self.start_client(self.slave2)

        self.assertEqual(role_x.count('leader'), 10)
        self.assertEqual(role_x.count('follower'), 18)
        self.assertEqual(is_alive_x.count('yes'), 28)
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[0],
                         self.get_table_status(self.slave1, self.tid, self.pid)[0])
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[0],
                         self.get_table_status(self.slave2, self.tid, self.pid)[0])
        self.ns_drop(self.ns_leader, self.tname)

    @ddt.data((34, 1, 15, 35))
    @ddt.unpack
    def test_ns_deadlock_bug(self, *steps):  # RTIDB-216
        """
        主节点网络闪断后发生死锁bug验证
        :param steps:
        :return:
        """
        steps_dict = self.get_steps_dict()
        for i in steps:
            eval(steps_dict[i])
        rs = self.showtable(self.ns_leader, self.tname)
        self.assertIn(self.tname, rs.keys()[0])
        time.sleep(10)
        self.ns_drop(self.ns_leader, self.tname)


    @ddt.data(
        (34, 2, 0, 13, 0, 35),
        (34, 3, -1, 15, 0, 35),
    )
    @ddt.unpack
    def test_no_replica_bug(self, *steps):  # RTIDB-221
        """
        没有副本的分片，挂掉后再恢复，会恢复为主节点
        :param steps:
        :return:
        """
        self.tname = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(self.tname), '"kAbsoluteTime"', 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"2-3"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        table_info = self.showtable(self.ns_leader, self.tname)
        self.tid = int(table_info.keys()[0][1])
        self.pid = 1
        for _ in range(10):
            self.put(self.leader, self.tid, self.pid, 'testkey0', self.now() + 90000, 'testvalue0')

        steps_dict = self.get_steps_dict()
        for i in steps:
            eval(steps_dict[i])
        rs = self.showtable(self.ns_leader, self.tname)
        self.assertEqual(rs[(self.tname, str(self.tid), str(self.pid), self.leader)],
                         ['leader', '144000min', 'yes', 'kNoCompress'])
        self.ns_drop(self.ns_leader, self.tname)


if __name__ == "__main__":
    load(TestAutoRecoverTable)
