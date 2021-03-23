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
import time
import os
from testcasebase import TestCaseBase
from libs.deco import *
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
import libs.ddt as ddt
import threading

@ddt.ddt
@multi_dimension(False)
class TestSendSnapshot(TestCaseBase):

    def put_data(self, endpoint, tid, pid, count):
        for _ in range(int(count)):
            rs = self.put(endpoint, tid, pid, "testkey0", self.now() + 9999,
                          "testvalue0testvalue0testvalue0testvalue00testvalue0testvalue0")
            self.assertIn("ok", rs)


    def sendsnapshot_concurrently(self, from_endpoint, tid, pid, *target_endpoints):
        """
        一个主同时发给多个从
        :param from_endpoint:
        :param tid:
        :param pid:
        :param target_endpoints:
        :return:
        """
        rs_list = []
        def sendsnapshot(target_edp):
            rs = self.sendsnapshot(from_endpoint, tid, pid, target_edp)
            rs_list.append(rs)
        threads = []
        for edp in target_endpoints:
            threads.append(threading.Thread(
                target=sendsnapshot, args=(edp,)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return rs_list


    def recieve_snapshot_concurrently(self, from_endpoint, tid_list, pid, target_endpoint):
        """
        多个主同时发给1个从
        :param from_endpoint:
        :param tid_list:
        :param pid:
        :param target_endpoint:
        :return:
        """
        rs_list = []
        def sendsnapshot(t_id):
            rs = self.sendsnapshot(from_endpoint, t_id, pid, target_endpoint)
            rs_list.append(rs)
        threads = []
        for tid in tid_list:
            threads.append(threading.Thread(
                target=sendsnapshot, args=(tid,)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return rs_list


    def send_and_recieve_concurrently(self, endpoint_1, tid_list_1, endpoint_2, tid_list_2, pid):
        """
        多个主同时发给多个从，不同endpoint间相互发送
        :param endpoint_1:
        :param tid_list_1:
        :param endpoint_2:
        :param tid_list_2:
        :param pid:
        :return:
        """
        rs_list = []
        def sendsnapshot(from_edp, t_id, target_adp):
            rs = self.sendsnapshot(from_edp, t_id, pid, target_adp)
            rs_list.append(rs)
        threads = []
        for tid in tid_list_1:
            threads.append(threading.Thread(
                target=sendsnapshot, args=(endpoint_1, tid, endpoint_2,)))
        for tid in tid_list_2:
            threads.append(threading.Thread(
                target=sendsnapshot, args=(endpoint_2, tid, endpoint_1,)))
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return rs_list


    def get_table_meta_tname(self):
        table_name = utils.exe_shell("cat {}/db/{}_{}/table_meta.txt|grep name|".format(
            self.slave1path, self.tid, self.pid) + "awk '{print $2}'")
        self.table_meta_tname = table_name[1:-1]
        return table_name[1:-1]


    def get_sdb_name(self, tid, pid):
        sdb_name = utils.exe_shell("ls {}/db/{}_{}/snapshot/|grep sdb".format(
            self.slave1path, tid, pid))
        self.sdb_name = sdb_name
        return sdb_name


    def check_manifest(self, db_path, tid, pid, offset, name, count):
        try:
            mf = self.get_manifest(db_path, tid, pid)
            infoLogger.info(mf)
            self.assertEqual(mf['offset'], offset)
            self.assertEqual(mf['name'], name)
            self.assertEqual(mf['count'], count)
            return True
        except Exception as e:
            infoLogger.info(e)

            return False


    def assert_send_fail_by_log(self):
        rs = utils.exe_shell('cat {}/warning.log | grep "Init file receiver failed. '
                              'tid\[{}\] pid\[{}\]"'.format(self.leaderpath, self.tid, self.pid, self.tid, self.pid))
        self.assertTrue(rs)

    def assert_init_fail_by_log(self):
        rs = utils.exe_shell('cat {}/warning.log |grep "tid\[{}\] pid\[{}\]"'
                             '|grep "Init FileSender failed"'.format(self.leaderpath, self.tid, self.pid))
        self.assertTrue(rs)


    @staticmethod
    def get_steps_dict():
        return {
            -2: 'time.sleep(1)',
            -1: 'time.sleep(5)',
            0: 'self.create(self.slave1, self.tname, self.tid, self.pid, 144000, 2, "false")',
            1: 'self.create(self.leader, self.tname, self.tid, self.pid, 144000, 2, "true")',
            2: 'self.put_data(self.leader, self.tid, self.pid, 100)',
            3: 'self.get_table_meta_tname()',
            4: 'self.get_sdb_name(self.tid, self.pid)',
            5: 'self.changerole(self.leader, self.tid, self.pid, "follower")',
            6: '',
            7: '',
            8: '',
            9: 'self.assertIn("SendSnapshot ok", self.sendsnapshot(self.leader, self.tid, self.pid, "111.222.333.444:80"))',
            10: 'self.assertIn("SendSnapshot ok", self.sendsnapshot(self.leader, self.tid, self.pid, "0.0.0.0:80"))',
            11: 'self.assertIn("Fail to SendSnapshot", self.sendsnapshot(self.leader, self.tid, self.pid, self.slave1))',
            12: 'self.assertIn("MakeSnapshot ok", self.makesnapshot(self.leader, self.tid, self.pid))',
            13: 'self.assertIn("PauseSnapshot ok", self.pausesnapshot(self.leader, self.tid, self.pid))',
            14: 'self.assertIn("RecoverSnapshot ok", self.recoversnapshot(self.leader, self.tid, self.pid))',
            15: 'self.assertIn("SendSnapshot ok", self.sendsnapshot(self.leader, self.tid, self.pid, self.slave1))',
            16: 'self.assertIn("LoadTable ok", self.loadtable(self.slave1, self.tname, self.tid, self.pid))',
            17: 'self.check_manifest(self.slave1path, self.tid, self.pid, "100", self.sdb_name, "50")',
            18: 'self.check_manifest(self.slave1path, self.tid, self.pid, "200", self.sdb_name, "100")',
            19: 'self.assertEqual("100", self.get_table_status(self.slave1, self.tid, self.pid)[0])',
            20: 'self.assertEqual("200", self.get_table_status(self.slave1, self.tid, self.pid)[0])',
            21: 'self.assert_send_fail_by_log()',
            22: 'self.check_manifest(self.slave2path, self.tid, self.pid, "100", self.sdb_name, "100")',
            23: 'self.assertEqual(len(filter(lambda x:"SendSnapshot ok" in x,'
                'self.sendsnapshot_concurrently(self.leader, self.tid, self.pid, self.slave1, self.slave2))), 2)',
            24: 'self.assertEqual(len(filter(lambda x:"SendSnapshot ok" in x,'
                'self.sendsnapshot_concurrently(self.leader, self.tid, self.pid,'
                'self.slave1, self.slave1, self.slave1, self.slave1, self.slave1))), 1)',
            25: 'self.assert_init_fail_by_log()',
            100: 'None'
        }


    def test_sendsnapshot_normal_0(self):
        """
        测试前准备：test_sendsnapshot_normal执行之前改成不限速并重启
        :return:
        """
        self.update_conf(self.leaderpath, 'stream_bandwidth_limit', 0)
        self.stop_client(self.leader)
        time.sleep(5)
        self.assertTrue(self.start_client(self.leader)[0])


    @ddt.data(
        (1, 2, 12, 13, 15, 14, -1, 3, 4, 17, 2, 12, 13, 15, 14, 18, 16, 20),  # 主表可以多次sendsnapshot给新的目标节点
        (11, 100),  # 表不存在不能sendsnapshot
        (1, 2, 11),  # 主表没有生成snapshot，不可以sendsnapshot给目标节点
        (1, 2, 12, 11),  # 主表没有pausesnapshot，不可以sendsnapshot给目标节点
        (1, 2, 5, 12, 13, 11),  # 目标从表不能执行sendsnapshot命令
        (1, 0, 2, 12, 13, 0, 15, -2, 21),  # 目标从表存在时，主表sendsnapshot失败
        (1, 2, 12, 13, 15, 10, -2, 21),  # 主表sendsnapshot给不存在的目标endpoint，失败
        (1, 2, 12, 13, 15, 9, -2, 25),  # 主表sendsnapshot给不存在的目标endpoint，失败
        (1, 2, 12, 13, 23, -1, 3, 4, 17, 22),  # 并发sendsnapshot给两个从节点，成功
        (1, 2, 12, 13, 24, -1, 3, 4, 17),  # 同一个snapshot，并发sendsnapshot给同一个从节点，只有1个成功
    )
    @ddt.unpack
    def test_sendsnapshot_normal(self, *steps):
        """
        各种情况下sendsnapshot功能检查
        :param steps:
        :return:
        """
        self.tname = str(self.now())
        steps_dict = self.get_steps_dict()
        for i in steps:
            infoLogger.info('*' * 10 + ' Executing step {}: {}'.format(i, steps_dict[i]))
            eval(steps_dict[i])


    def test_sendsnapshot_normal_z(self):
        """
        测试完成后：test_sendsnapshot_normal执行之后还原配置并重启
        :return:
        """
        self.update_conf(self.leaderpath, 'stream_bandwidth_limit', None)
        self.stop_client(self.leader)
        time.sleep(5)
        self.start_client(self.leader)


    def test_sendsnapshot_multi_to_one(self):
        """
        将1个snapshot同时并发地发给同1个从节点，只有1次可以成功
        :return:
        """
        self.tname = str(self.now())
        tid_list = [self.tid + x for x in range(5)]
        for t in tid_list:
            self.create(self.leader, self.tname, t, self.pid, 144000, 2, "true")
            self.put_data(self.leader, t, self.pid, 100)
            self.makesnapshot(self.leader, t, self.pid)
            self.pausesnapshot(self.leader, t, self.pid)
        self.recieve_snapshot_concurrently(self.leader, tid_list, self.pid, self.slave1)
        time.sleep(5)
        for t in tid_list:
            self.check_manifest(self.slave1path, t, self.pid, "100", self.get_sdb_name(t, self.pid), "100")


    def test_sendsnapshot_send_recieve_concurrently(self):
        """
        收发并行
        :return:
        """
        pid = self.pid
        self.tname = str(self.now())
        tid_list_1 = [x for x in range(10, 19, 2)]
        tid_list_2 = [x for x in range(11, 20, 2)]
        for t in tid_list_1 + tid_list_2:
            if t % 2 == 0:
                edp = self.leader
            else:
                edp = self.slave1
            self.create(edp, self.tname, t, pid, 144000, 2, "true")
            self.put_data(edp, t, pid, t)
            self.makesnapshot(edp, t, pid, 'client', 1)
            self.pausesnapshot(edp, t, pid)
        self.send_and_recieve_concurrently(self.leader, tid_list_1, self.slave1, tid_list_2, pid)
        time.sleep(5)
        for t in tid_list_1:
            self.check_manifest(self.slave1path, t, pid, str(t), self.get_sdb_name(t, pid), str(t))
        for t in tid_list_2:
            self.check_manifest(self.leaderpath, t, pid, str(t), self.get_sdb_name(t, pid), str(t))


    def test_sendsnapshot_speed_limit(self):  # RTIDB-227
        """
        限速测试，stream_bandwidth_limit = 1024, 10k左右文件会在8s-12s之间发送成功
        :return:
        """
        offset = str(120)
        self.update_conf(self.leaderpath, 'stream_block_size', 32)
        self.update_conf(self.leaderpath, 'stream_bandwidth_limit', 1024)
        self.stop_client(self.leader)
        time.sleep(5)
        self.start_client(self.leader)
        tname = self.now()
        self.create(self.leader, tname, self.tid, self.pid, 144000, 2, "true")
        self.put_data(self.leader, self.tid, self.pid, offset)
        self.assertIn("MakeSnapshot ok", self.makesnapshot(self.leader, self.tid, self.pid))
        self.assertIn("PauseSnapshot ok", self.pausesnapshot(self.leader, self.tid, self.pid))
        self.assertIn("SendSnapshot ok", self.sendsnapshot(self.leader, self.tid, self.pid, self.slave1))
        time.sleep(1)
        check_manifest_sent0 = self.check_manifest(
            self.slave1path, self.tid, self.pid, offset, self.get_sdb_name(self.tid, self.pid), offset) \
                               and self.get_sdb_name(self.tid, self.pid).endswith('sdb')
        time.sleep(7)
        check_manifest_sent1 = self.check_manifest(
            self.slave1path, self.tid, self.pid, offset, self.get_sdb_name(self.tid, self.pid), offset) \
                               and self.get_sdb_name(self.tid, self.pid).endswith('sdb')
        time.sleep(4)
        infoLogger.info(self.get_sdb_name(self.tid, self.pid).endswith('sdb'))
        check_manifest_sent = self.check_manifest(
            self.slave1path, self.tid, self.pid, offset, self.get_sdb_name(self.tid, self.pid), offset) \
                               and self.get_sdb_name(self.tid, self.pid).endswith('sdb')
        # Teardown
        self.update_conf(self.leaderpath, 'stream_block_size', None)
        self.update_conf(self.leaderpath, 'stream_bandwidth_limit', None)
        self.stop_client(self.leader)
        time.sleep(5)
        self.start_client(self.leader)
        self.assertFalse(check_manifest_sent0)  # files sending because of stream_bandwidth_limit
        self.assertFalse(check_manifest_sent1)  # files sending because of stream_bandwidth_limit
        self.assertTrue(check_manifest_sent)


    def test_sendsnapshot_speed_without_limit(self):
        """
        限速测试，stream_bandwidth_limit = 0，10k左右文件会立即发送成功
        :return:
        """
        self.update_conf(self.leaderpath, 'stream_bandwidth_limit', 0)
        self.stop_client(self.leader)
        time.sleep(5)
        self.start_client(self.leader)
        tname = self.now()
        self.create(self.leader, tname, self.tid, self.pid, 144000, 2, "true")
        self.put_data(self.leader, self.tid, self.pid, 100)
        self.assertIn("MakeSnapshot ok", self.makesnapshot(self.leader, self.tid, self.pid))
        self.assertIn("PauseSnapshot ok", self.pausesnapshot(self.leader, self.tid, self.pid))
        self.assertIn("SendSnapshot ok", self.sendsnapshot(self.leader, self.tid, self.pid, self.slave1))
        time.sleep(1)
        self.check_manifest(self.slave1path, self.tid, self.pid,
                            '100', self.get_sdb_name(self.tid, self.pid), '100')
        # Teardown
        self.update_conf(self.leaderpath, 'stream_bandwidth_limit', None)
        self.stop_client(self.leader)
        time.sleep(1)
        self.start_client(self.leader)
        time.sleep(10)
    
if __name__ == "__main__":
    load(TestSendSnapshot)
