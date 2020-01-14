# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
import os
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt
import libs.conf as conf


@ddt.ddt
class TestRecoverEndpoint(TestCaseBase):

    def createtable_put(self, data_count):
        self.tname = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": self.tname,
            "ttl": 144000,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-3","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-3","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "2-3","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "false"},
                {"name": "k3", "type": "string", "add_ts_idx": "false"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        table_info = self.showtable(self.ns_leader, self.tname)
        self.tid = int(table_info.keys()[0][1])
        self.pid = 3
        self.put_large_datas(data_count, 7)
        time.sleep(1)

    def createtable_nofollower_put(self, data_count):
        self.tname = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": self.tname,
            "ttl": 144000,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-3","is_leader": "true"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "false"},
                {"name": "k3", "type": "string", "add_ts_idx": "false"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        table_info = self.showtable(self.ns_leader, self.tname)
        self.tid = int(table_info.keys()[0][1])
        self.pid = 3
        self.put_large_datas(data_count, 7)


    def put_data(self, endpoint):
        rs = self.put(endpoint, self.tid, self.pid, "testkey0", self.now() + 1000, "testvalue0")
        self.assertIn("ok", rs)


    @staticmethod
    def get_steps_dict():
        return {
            -2: 'time.sleep(2)',
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
            25: 'self.recoverendpoint(self.ns_leader, self.leader)',
            26: 'self.offlineendpoint(self.ns_leader, self.leader)',
            27: 'self.offlineendpoint(self.ns_leader, self.slave1)',
            28: 'self.stop_client(self.ns_leader)',
            29: 'self.start_client(self.ns_leader, "nameserver")',
            30: 'self.get_new_ns_leader()',
            31: 'self.get_table_status(self.leader)',
            32: 'self.makesnapshot(self.ns_leader, self.tname, self.pid, "ns_client")',
            33: 'self.get_latest_opid_by_tname_pid(self.tname, self.pid)',
            34: 'self.recoverendpoint(self.ns_leader, self.slave1)',
            35: 'self.createtable_nofollower_put(1)',
            36: 'self.assertEqual(self.get_op_by_opid(self.latest_opid), "kUpdatePartitionStatusOP")',
            37: 'self.assertEqual(self.get_op_by_opid(self.latest_opid), "kReLoadTableOP")',
            38: 'self.wait_op_done(self.tname)',
        }

    @ddt.data(
        (1, 3, -2, 26, -1, 38, 6, 15, -2, 25, -1, 38, 33, 20, 24),  # 主节点断网后恢复，手动恢复后加为从节点
        (1, 3, -2, 26, -1, 38, 6, 8, 12, 15, -2, 25, -1, 38, 33, 19, 23),
        (1, 12, -2, 2, -1, 38, 26, -1, 38, 6, 12, -1, 13, -2, 25, -1, 38, 33, 18, 22),
        (1, 2, -2, 26, -1, 38, 6, 13, -2, 25, -1, 38, 33, 17, 21),
        (1, 4, -2, 27, -1, 38, 14, -2, 34, -1, 38, 33, 17),  # 从节点挂掉后恢复，手动恢复后重新加为从节点
    )
    @ddt.unpack
    def test_recover_endpoint(self, *steps):
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
        print self.showopstatus(self.ns_leader)
        self.assertEqual(role_x.count('leader'), 4)
        self.assertEqual(role_x.count('follower'), 6)
        self.assertEqual(is_alive_x.count('yes'), 10)
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[0],
                         self.get_table_status(self.slave1, self.tid, self.pid)[0])
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[0],
                         self.get_table_status(self.slave2, self.tid, self.pid)[0])
        self.ns_drop(self.ns_leader, self.tname)

    @ddt.data(
        (35, 3, -2, 26, -2, 38, 15, -2, 25, -2, 38, 33, 36, 27),  # 没有从节点的情况下主节点断网后恢复，手动恢复后仍为主节点
        (35, 2, -2, 26, -2, 38, 13, -2, 25, -2, 38, 33, 37, 27),  # 没有从节点的情况下主节点挂掉后恢复，手动恢复后仍为主节点
    )
    @ddt.unpack
    def test_recover_endpoint1(self, *steps):
        """
        tablet故障恢复流程测试
        只有主节点没有从节点
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
        infoLogger.info('{}'.format(self.showopstatus(self.ns_leader)))
        self.assertEqual(role_x.count('leader'), 4)
        self.assertEqual(is_alive_x.count('yes'), 4)
        infoLogger.info(self.get_table_status(self.leader, self.tid, self.pid))
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[1], "kTableLeader")
        self.ns_drop(self.ns_leader, self.tname)

    def test_recoverendpoint_offline_master_failed(self):
        """
        主节点挂掉未启动，不可以手工recoverendpoint成功
        :return:
        """
        self.start_client(self.leader)
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        # m = utils.gen_table_metadata(
        #     '"{}"'.format(name), None, 144000, 2,
        #     ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
        #     ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'),
        #     ('table_partition', '"{}"'.format(self.slave2), '"0-1"', 'false'),
        #     ('column_desc', '"k1"', '"string"', 'true'),
        #     ('column_desc', '"k2"', '"string"', 'false'),
        #     ('column_desc', '"k3"', '"string"', 'false')
        # )
        # utils.gen_table_metadata_file(m, metadata_path)

        table_meta = {
            "name": name,
            "ttl": 144000,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-1","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "0-1","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "false"},
                {"name": "k3", "type": "string", "add_ts_idx": "false"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs0)
        self.multidimension_vk = {'k1': ('string:index', 'testvalue0'),
                                  'k2': ('string', 'testvalue0'),
                                  'k3': ('string', 'testvalue0')}
        self.multidimension_scan_vk = {'k1': 'testvalue0'}

        self.stop_client(self.leader)
        time.sleep(10)
        rs = self.recoverendpoint(self.ns_leader, self.leader)
        self.assertIn('failed', rs)
        self.ns_drop(self.ns_leader, name)


    def test_recoversnapshot_offline_master_after_changeleader(self):
        """
        主节点挂掉，手动故障切换成功后启动，
        做了故障切换的分片，可以手工recovertable为follower
        无副本的分片，手工recovertablet为leader
        :return:
        """
        self.start_client(self.leader)
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        table_meta = {
            "name": name,
            "ttl": 144000,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-1","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "0-1","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "false"},
                {"name": "k3", "type": "string", "add_ts_idx": "false"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs0)
        self.multidimension_vk = {'k1': ('string:index', 'testvalue0'),
                                  'k2': ('string', 'testvalue0'),
                                  'k3': ('string', 'testvalue0')}
        self.multidimension_scan_vk = {'k1': 'testvalue0'}

        rs1 = self.showtable(self.ns_leader, name)
        tid = rs1.keys()[0][1]

        self.stop_client(self.leader)
        self.updatetablealive(self.ns_leader, name, '*', self.leader, 'no')
        time.sleep(5)
        self.changeleader(self.ns_leader, name, 0)
        self.changeleader(self.ns_leader, name, 1)
        self.start_client(self.leader)
        time.sleep(5)
        self.recoverendpoint(self.ns_leader, self.leader)
        time.sleep(10)

        rs5 = self.showtable(self.ns_leader, name)
        self.assertEqual(rs5[(name, tid, '0', self.leader)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs5[(name, tid, '1', self.leader)], ['follower',  '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs5[(name, tid, '2', self.leader)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.ns_drop(self.ns_leader, name)

    def test_recoverendpoint_need_restore(self):
        """
        recoverendpoint恢复最初的表结构
        :return:
        """
        self.start_client(self.leader)
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        table_meta = {
            "name": name,
            "ttl": 144000,
            "storage_mode": "kSSD",
            "table_partition": [
                {"endpoint": self.leader,"pid_group": "0-2","is_leader": "true"},
                {"endpoint": self.slave1,"pid_group": "0-3","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "0-1","is_leader": "false"},
                {"endpoint": self.slave2,"pid_group": "3","is_leader": "true"},
                {"endpoint": self.leader,"pid_group": "3","is_leader": "false"},
            ],
            "column_desc":[
                {"name": "k1", "type": "string", "add_ts_idx": "true"},
                {"name": "k2", "type": "string", "add_ts_idx": "false"},
                {"name": "k3", "type": "string", "add_ts_idx": "false"},
            ],
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs0)
        self.multidimension_vk = {'k1': ('string:index', 'testvalue0'),
                                  'k2': ('string', 'testvalue0'),
                                  'k3': ('string', 'testvalue0')}
        self.multidimension_scan_vk = {'k1': 'testvalue0'}

        rs1 = self.showtable(self.ns_leader, name)
        tid = rs1.keys()[0][1]

        rs = self.put(self.leader, tid, 0, '', self.now(), 'pk1' ,'v1', 'w1')
        self.assertIn('Put ok', rs)
        rs = self.put(self.leader, tid, 1, '', self.now(), 'pk2' ,'v2', 'w2')
        self.assertIn('Put ok', rs)
        time.sleep(1)

        self.stop_client(self.leader)
        time.sleep(10)
        self.offlineendpoint(self.ns_leader, self.leader)
        self.start_client(self.leader)
        time.sleep(3)
        self.recoverendpoint(self.ns_leader, self.leader, 'true')
        time.sleep(10)

        rs5 = self.showtable(self.ns_leader, name)
        self.assertEqual(rs5[(name, tid, '0', self.leader)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs5[(name, tid, '1', self.leader)], ['leader',  '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs5[(name, tid, '3', self.leader)], ['follower',  '144000min', 'yes', 'kNoCompress'])

        rs = self.put(self.leader, tid, 0, '', self.now(), 'pk3' ,'v3', 'w5')
        self.assertIn('Put ok', rs)
        rs = self.put(self.leader, tid, 1, '', self.now(), 'pk4' ,'v4', 'w4')
        self.assertIn('Put ok', rs)
        time.sleep(1)

        self.assertIn('v1', self.scan(self.slave1, tid, 0, {'k1': 'pk1'}, self.now(), 1))
        self.assertIn('v4', self.scan(self.slave1, tid, 1, {'k1': 'pk4'}, self.now(), 1))
        self.ns_drop(self.ns_leader, name)


if __name__ == "__main__":
    load(TestRecoverEndpoint)
