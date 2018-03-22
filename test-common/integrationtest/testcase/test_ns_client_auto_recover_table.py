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

    def confset_createtable_put(self, data_count):
        self.confset(self.ns_leader, 'auto_failover', 'true')
        self.confset(self.ns_leader, 'auto_recover_table', 'true')
        self.tname = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(self.tname), '"kAbsoluteTime"', 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"0-3"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertEqual('Create table ok' in rs, True)
        table_info = self.showtable(self.ns_leader)
        self.tid = int(table_info.keys()[0][1])
        self.pid = 3
        self.put_large_datas(data_count, 7)


    def get_opid_by_tname_pid(self, tname, pid):
        opid_rs = utils.exe_shell("grep -a {} {}/info.log|grep op_id|grep \"name\[\"|grep \"pid\[{}\]\""
                                  "|sed 's/\(.*\)op_id\[\(.*\)\] name\(.*\)/\\2/g'".format(
            tname, self.ns_leader_path, pid))
        opid_x = opid_rs.split('\n')
        return opid_x


    def get_latest_opid_by_tname_pid(self, tname, pid):
        latest_opid = self.get_opid_by_tname_pid(tname, pid)[-1]
        self.latest_opid = latest_opid
        return latest_opid


    def get_op_by_opid(self, op_id):
        rs = self.showopstatus(self.ns_leader)
        return rs[int(op_id)][0]


    def get_task_dict_by_opid(self, opid):
        time.sleep(1)
        task_dict = {}
        cmd = "cat {}/info.log |grep -a \"op_id\[{}\]\"|grep task_type".format(self.ns_leader_path, opid) \
              + "|awk -F '\\\\[' '{print $4\"]\"$5\"]\"$6}'" \
                "|awk -F '\\\\]' '{print $1\",\"$3\",\"$5}'"
        infoLogger.info(cmd)
        rs = utils.exe_shell(cmd).split('\n')
        infoLogger.info(rs)
        for x in rs:
            x = x.split(',')
            task_dict[(int(x[1]), x[2])] = x[0]
        self.task_dict = task_dict


    def put_data(self, endpoint):
        rs = self.put(endpoint, self.tid, self.pid, "testkey0", self.now() + 1000, "testvalue0")
        self.assertEqual("ok" in rs, True)


    def check_tasks(self, op_id, *args):
        self.get_task_dict_by_opid(op_id)
        tasks = [k[1] for k, v in self.task_dict.items() if k[0] == int(op_id) and v == 'kDone']
        infoLogger.info(self.task_dict)
        infoLogger.info(op_id)
        infoLogger.info([k[1] for k, v in self.task_dict.items()])
        infoLogger.info(tasks)
        infoLogger.info(args)
        self.assertEqual(set(args) - set(tasks), set([]))


    def check_re_add_replica_op(self, op_id):
        self.check_tasks(op_id,
                         'kPauseSnapshot',
                         'kSendSnapshot',
                         'kLoadTable',
                         'kRecoverSnapshot',
                         'kAddReplica')


    def check_re_add_replica_no_send_op(self, op_id):
        self.check_tasks(op_id,
                         'kPauseSnapshot',
                         'kLoadTable',
                         'kRecoverSnapshot',
                         'kAddReplica')


    def check_re_add_replica_with_drop_op(self, op_id):
        self.check_tasks(op_id,
                         'kPauseSnapshot',
                         'kSendSnapshot',
                         'kLoadTable',
                         'kDropTable',
                         'kRecoverSnapshot',
                         'kAddReplica')


    def check_re_add_replica_simplify_op(self, op_id):
        self.check_tasks(op_id,
                         'kAddReplica')


    @staticmethod
    def get_steps_dict():
        return {
            0: 'time.sleep(10)',
            1: 'self.confset_createtable_put(1)',
            2: 'self.stop_client(self.leader)',
            3: 'self.disconnectzk(self.leader)',
            4: 'self.stop_client(self.slave1)',
            5: 'self.disconnectzk(self.slave1)',
            6: 'self.find_new_tb_leader(self.tname, self.tid, self.pid)',
            7: 'self.put_data(self.leader)',
            8: 'self.put_data(self.new_tb_leader)',
            9: 'self.confset(self.ns_leader, "auto_recover_table", "false")',
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
            26: 'self.confset(self.ns_leader, "auto_recover_table", "false")',
            27: 'self.confset(self.ns_leader, "auto_recover_table", "true")',
            28: 'self.stop_client(self.ns_leader)',
            29: 'self.start_client(self.ns_leader, "nameserver")',
            30: 'self.get_new_ns_leader()',
            31: 'self.get_table_status(self.leader)',
            32: 'self.makesnapshot(self.ns_leader, self.tname, self.pid, "ns_client")',
            33: 'self.get_latest_opid_by_tname_pid(self.tname, self.pid)',
        }


    @ddt.data(
        (1, 3, 0, 6, 15, 0, 33, 20, 24),  # offset = manifest.offset
        (1, 3, 0, 6, 12, 15, 0, 33, 20),  # offset = manifest.offset
        (1, 3, 0, 6, 8, 15, 0, 33, 20),  # offset = manifest.offset
        (1, 3, 0, 6, 8, 12, 15, 0, 33, 19, 23),  # offset < manifest.offset
        (1, 12, 3, 0, 12, 15, 0, 33, 20),  # offset = manifest.offset
        (1, 11, 7, 10, 3, 0, 15, 0, 33, 20),  # offset > manifest.offset
        (1, 3, 0, 6, 7, 15, 0, 33, 19),  # not match
        (1, 3, 0, 6, 7, 12, 15, 0, 33, 19),  # not match
        (1, 3, 0, 6, 7, 8, 15, 0, 33, 19),  # not match
        (1, 3, 0, 7, 10, 2, 12, 13, 0, 33, 17),  # not match
        (1, 12, 2, 0, 6, 12, 13, 0, 33, 18, 22),  # offset = manifest.offset
        (1, 11, 7, 10, 2, 0, 13, 0, 33, 18),  # 12 offset > manifest.offset
        (1, 11, 7, 7, 10, 2, 0, 6, 8, 13, 0, 33, 18),  # 13 offset > manifest.offset
        (1, 2, 0, 6, 13, 0, 33, 17, 21),  # offset < manifest.offset
        (1, 2, 0, 6, 12, 13, 0, 33, 17),  # offset < manifest.offset
        (1, 2, 0, 6, 8, 13, 0, 33, 17),
        (1, 2, 0, 6, 10, 12, 13, 0, 33, 17),
        (1, 2, 0, 6, 8, 12, 13, 0, 33, 17),
        (1, 2, 0, 6, 8, 12, 8, 13, 0, 33, 17),  # 19 new leader makesnapshot and put data, ori leader recover
        (1, 5, 0, 16, 0, 33, 20),
        (1, 4, 0, 14, 0, 33, 17),
        (1, 12, 3, 7, 2, 0, 13, 0, 33, 18),  # RTIDB-222
        (1, 26, 3, 0, 6, 15, 25, 0, 33, 20, 24, 27),  # 23-26 recoverendpoint
        (1, 26, 3, 0, 6, 8, 12, 15, 25, 0, 33, 19, 23, 27),
        (1, 26, 12, 2, 0, 6, 12, 13, 25, 0, 33, 18, 22, 27),
        (1, 26, 2, 0, 6, 13, 25, 0, 33, 17, 21, 27),
    )
    @ddt.unpack
    def test_auto_recover_table(self, *steps):
        self.get_new_ns_leader()
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])
        rs = self.showtable(self.ns_leader)
        role_x = [v[0] for k, v in rs.items()]
        is_alive_x = [v[-1] for k, v in rs.items()]

        self.assertEqual(role_x.count('leader'), 4)
        self.assertEqual(role_x.count('follower'), 6)
        self.assertEqual(is_alive_x.count('yes'), 10)
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[0],
                         self.get_table_status(self.slave1, self.tid, self.pid)[0])
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[0],
                         self.get_table_status(self.slave2, self.tid, self.pid)[0])


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

        self.confset_createtable_put(50)
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])
        rs = self.showtable(self.ns_leader)
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

        self.assertEqual(role_x.count('leader'), 4)
        self.assertEqual(role_x.count('follower'), 6)
        self.assertEqual(is_alive_x.count('yes'), 10)
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[0],
                         self.get_table_status(self.slave1, self.tid, self.pid)[0])
        self.assertEqual(self.get_table_status(self.leader, self.tid, self.pid)[0],
                         self.get_table_status(self.slave2, self.tid, self.pid)[0])


    @ddt.data((1, 9, 15))
    @ddt.unpack
    def test_deadlock_bug(self, *steps):
        steps_dict = self.get_steps_dict()
        for i in steps:
            eval(steps_dict[i])
        rs = self.showtable(self.ns_leader)
        self.assertEqual(self.tname in rs.keys()[0], True)


    @ddt.data(
        (2, 0, 13, 0),
        (3, 0, 15, 0),
    )
    @ddt.unpack
    def test_no_replica_bug(self, *steps):  # RTIDB-221
        """
        没有副本的分片，挂掉后再恢复，会恢复为主节点
        :param steps:
        :return:
        """
        self.confset(self.ns_leader, 'auto_failover', 'true')
        self.confset(self.ns_leader, 'auto_recover_table', 'true')
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
        self.assertEqual('Create table ok' in rs, True)
        table_info = self.showtable(self.ns_leader)
        self.tid = int(table_info.keys()[0][1])
        self.pid = 1
        for _ in range(10):
            self.put(self.leader, self.tid, self.pid, 'testkey0', self.now() + 90000, 'testvalue0')

        steps_dict = self.get_steps_dict()
        for i in steps:
            eval(steps_dict[i])
        rs = self.showtable(self.ns_leader)
        infoLogger.info(rs)
        self.assertEqual(rs[(self.tname, str(self.tid), str(self.pid), self.leader)],
                         ['leader', '8', '144000', 'yes'])


if __name__ == "__main__":
    load(TestAutoRecoverTable)
