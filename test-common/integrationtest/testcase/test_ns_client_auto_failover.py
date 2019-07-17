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
        self.assertIn('Create table ok', rs0)

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'true')

        if failover_reason == 'killed':
            self.stop_client(self.leader)
        elif failover_reason == 'network_failure':
            self.disconnectzk(self.leader)
        time.sleep(5)
        self.wait_op_done(name)

        rs2 = self.showtablet(self.ns_leader)
        rs3 = self.showtable(self.ns_leader)
        if failover_reason == 'killed':
            self.start_client(self.leader)
        elif failover_reason == 'network_failure':
            self.connectzk(self.leader)
        time.sleep(2)
        self.wait_op_done(name)
        self.assertIn('kTabletOffline', rs2[self.leader])
        self.confset(self.ns_leader, 'auto_failover', 'false')

        # leader to offline
        self.assertEqual(rs3[(name, tid, '0', self.leader)], ['leader', '144000min', 'no', 'kNoCompress'])  # RTIDB-203
        self.assertEqual(rs3[(name, tid, '1', self.leader)], ['leader', '144000min', 'no', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '2', self.leader)], ['leader', '144000min', 'no', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '3', self.leader)], ['leader', '144000min', 'no', 'kNoCompress'])

        # slave to leader
        self.assertEqual(rs3[(name, tid, '1', self.slave1)], ['leader', '144000min', 'yes', 'kNoCompress'])
        act1 = rs3[(name, tid, '2', self.slave1)]
        act2 = rs3[(name, tid, '2', self.slave2)]
        roles = [x[0] for x in [act1, act2]]
        self.assertEqual(roles.count('leader'), 1)
        self.assertEqual(roles.count('follower'), 1)
        self.assertEqual(rs3[(name, tid, '3', self.slave2)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.get_new_ns_leader()
        self.ns_drop(self.ns_leader, name)


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
        self.assertIn('Create table ok', rs0)

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'true')

        if failover_reason == 'killed':
            self.stop_client(self.slave1)
        elif failover_reason == 'network_failure':
            self.disconnectzk(self.slave1)
        time.sleep(5)

        rs2 = self.showtablet(self.ns_leader)
        rs3 = self.showtable(self.ns_leader)
        if failover_reason == 'killed':
            self.start_client(self.slave1)
        elif failover_reason == 'network_failure':
            self.connectzk(self.slave1)
        time.sleep(5)
        self.assertIn('kTabletOffline', rs2[self.slave1])
        self.confset(self.ns_leader, 'auto_failover', 'false')

        self.assertEqual(rs3[(name, tid, '0', self.leader)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '1', self.leader)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '2', self.leader)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '0', self.slave1)], ['follower', '144000min', 'no', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '1', self.slave1)], ['follower', '144000min', 'no', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '1', self.slave2)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '2', self.slave2)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.get_new_ns_leader()
        self.ns_drop(self.ns_leader, name)

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
        self.assertIn('Create table ok', rs0)

        rs1 = self.showtable(self.ns_leader)
        tid = rs1.keys()[0][1]

        self.confset(self.ns_leader, 'auto_failover', 'true')

        self.connectzk(self.leader)  # flashbreak
        self.showtable(self.ns_leader)
        time.sleep(2)
        self.wait_op_done(name)
        rs2 = self.showtable(self.ns_leader)
        self.connectzk(self.slave1)  # flashbreak
        self.showtable(self.ns_leader)
        time.sleep(2)
        self.wait_op_done(name)
        rs3 = self.showtable(self.ns_leader)
        rs4 = self.showtablet(self.ns_leader)
        self.assertIn('kTabletHealthy', rs4[self.leader])
        self.assertIn('kTabletHealthy', rs4[self.slave1])
        self.confset(self.ns_leader, 'auto_failover', 'false')

        self.assertEqual(rs2[(name, tid, '0', self.leader)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs2[(name, tid, '1', self.leader)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs2[(name, tid, '2', self.leader)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs2[(name, tid, '0', self.slave1)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs2[(name, tid, '1', self.slave1)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs2[(name, tid, '2', self.slave2)], ['leader', '144000min', 'yes', 'kNoCompress'])

        self.assertEqual(rs3[(name, tid, '0', self.leader)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '1', self.leader)], ['leader', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '2', self.leader)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '0', self.slave1)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '1', self.slave1)], ['follower', '144000min', 'yes', 'kNoCompress'])
        self.assertEqual(rs3[(name, tid, '2', self.slave2)], ['leader', '144000min', 'yes', 'kNoCompress'])
        rs = self.ns_drop(self.ns_leader, name)


    @multi_dimension(True)
    def test_select_leader(self):
        """
        slave1改为leader role，put数据后改回follower role，leader发生故障后，新主会切换到slave1，数据同步正确
        :return:
        """
        self.confset(self.ns_leader, 'auto_failover', 'true')

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
        self.assertIn('Create table ok', rs1)
        rs2 = self.showtable(self.ns_leader)
        tid = rs2.keys()[0][1]
        pid = 0

        d = {'k1': ('string:index', 'card1'),
             'k2': ('string:index', 'card2'),
             'k3': ('string:index', 'card3')}
        self.multidimension_vk = collections.OrderedDict(sorted(d.items(), key = lambda t:t[0]))

        self.changerole(self.slave1, tid, pid, 'leader')
        rs3 = self.put(self.slave1, tid, pid, "testkey0", self.now() + 9999, "testvalue0")
        self.assertIn("ok", rs3)
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
        self.assertIn("ok", rs4)
        self.changerole(self.slave2, tid, pid, 'follower')

        self.disconnectzk(new_tb_leader1)
        time.sleep(10)
        self.showtable(self.ns_leader)
        new_tb_leader2 = self.find_new_tb_leader(name, tid, pid)

        self.connectzk(new_tb_leader1)
        time.sleep(10)
        self.showtable(self.ns_leader)
        self.confset(self.ns_leader, 'auto_failover', 'false')

        self.assertEqual(new_tb_leader1, self.slave1)
        self.assertEqual(new_tb_leader2, self.slave2)

        d = {'k1': ('string:index', 'ccard1'),
             'k2': ('string:index', 'ccard2'),
             'k3': ('string:index', 'ccard3')}
        self.multidimension_vk = collections.OrderedDict(sorted(d.items(), key = lambda t:t[0]))
        self.multidimension_scan_vk = {'k1': 'ccard1'}
        rs5 = self.put(new_tb_leader2, tid, pid, "testkey1", self.now() + 9999, "ccard1")
        self.assertIn("ok", rs5)
        time.sleep(1)
        rs_scan = self.scan(self.leader, tid, pid, 'testkey1', self.now() + 19999, 1)
        self.assertTrue('ccard1' in rs_scan)
        rs = self.ns_drop(self.ns_leader, name)

    def test_enable_auto_failover(self):
        """
        auto_failover开启时不能执行手动恢复命令
        :return:
        """
        name = 't{}'.format(time.time())
        rs1 = self.ns_create_cmd(self.ns_leader, name, 144000, 8, 3, '')
        self.assertIn('Create table ok', rs1)
        number = 2
        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))
            self.assertIn('Put ok', rs_put)
        msg = "auto_failover is enabled"
        self.confset(self.ns_leader, 'auto_failover', 'true')
        rs = self.ns_gettablepartition(self.ns_leader, 'gettablepartition', name, 0)
        self.assertIn('get table partition ok', rs)
        partition_file = '{}_{}.txt'.format(name, 0)
        rs = self.settablepartition(self.ns_leader, name, partition_file)
        self.assertIn(msg, rs)
        os.remove(partition_file)
        rs = self.offlineendpoint(self.ns_leader, self.leader)
        self.assertIn(msg, rs)
        rs = self.recoverendpoint(self.ns_leader, self.leader)
        self.assertIn(msg, rs)
        rs = self.changeleader(self.ns_leader, "test", 0)
        self.assertIn(msg, rs)
        rs = self.recovertable(self.ns_leader, "test", 0, self.leader)
        self.assertIn(msg, rs)
        rs = self.migrate(self.ns_leader, self.slave1, "test", "4-6", self.slave2)
        self.assertIn(msg, rs)
        rs = self.updatetablealive(self.ns_leader, "test", "0", self.slave1, "yes")
        self.assertIn(msg, rs)
        self.confset(self.ns_leader, 'auto_failover', 'false')
        rs = self.ns_drop(self.ns_leader, name)

    def test_unable_auto_failover(self):
        """
        auto_failover关闭时，可以手动执行恢复相关命名
        :return:
        """
        name = 't{}'.format(time.time())
        rs1 = self.ns_create_cmd(self.ns_leader, name, 144000, 8, 3, '')
        self.assertIn('Create table ok', rs1)
        number = 2
        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))
            self.assertIn('Put ok', rs_put)

        self.confset(self.ns_leader, 'auto_failover', 'false')

        rs = self.ns_gettablepartition(self.ns_leader, 'gettablepartition', name, 0)
        self.assertIn('get table partition ok', rs)
        partition_file = '{}_{}.txt'.format(name, 0)
        rs = self.settablepartition(self.ns_leader, name, partition_file)
        self.assertIn('set table partition ok', rs)
        os.remove(partition_file)

        rs = self.offlineendpoint(self.ns_leader, self.leader)
        self.assertIn("offline endpoint ok", rs)
        time.sleep(3)
        rs = self.changeleader(self.ns_leader, name, 0, 'auto')
        self.assertIn('change leader ok', rs)
        time.sleep(3)
        rs = self.recovertable(self.ns_leader, name, 0, self.leader)
        self.assertIn('recover table ok', rs)
        rs = self.recoverendpoint(self.ns_leader, self.leader)
        self.assertIn("recover endpoint ok", rs)
        time.sleep(1)
        rs = self.updatetablealive(self.ns_leader, name, "0", self.slave1, "yes")
        self.assertIn('update ok', rs)
        self.get_new_ns_leader()
        rs = self.ns_drop(self.ns_leader, name)

    def test_auto_failover_kill_tablet(self):
        """
        auto_failover打开后，下线一个tablet，自动恢复数据后，follower追平leader的offset
        :return:
        """
        self.start_client(self.ns_leader, 'nameserver')
        time.sleep(1)
        self.start_client(self.ns_slaver, 'nameserver')
        time.sleep(1)
        self.get_new_ns_leader()
        self.confset(self.ns_leader, 'auto_failover', 'true')
        name = 't{}'.format(time.time())
        infoLogger.info(name)
        rs1 = self.ns_create_cmd(self.ns_leader, name, 144000, 1, 3, '')
        self.assertIn('Create table ok', rs1)

        number = 2
        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))
            self.assertIn('Put ok', rs_put)
        time.sleep(1)

        rs = self.stop_client(self.slave1)
        rs_before = self.gettablestatus(self.leader)
        rs_before = self.parse_tb(rs_before, ' ', [0, 1, 2, 3], [4, 5, 6, 7,8, 9,10])
        time.sleep(10)
        rs = self.start_client(self.slave1)
        time.sleep(1)
        self.wait_op_done(name)
        for i in range(20):
            rs_after = self.gettablestatus(self.slave1)
            rs_after = self.parse_tb(rs_after, ' ', [0, 1, 2, 3], [4, 5, 6, 7,8, 9,10])
            if '{}'.format(rs_after) == 'gettablestatus failed' or isinstance(rs_after, str) or len(rs_after.keys()) == 0:
                time.sleep(2)
                continue
            if rs_before.keys()[0][2] == rs_after.keys()[0][2]:
                self.assertIn(rs_before.keys()[0][2], rs_after.keys()[0][2])
                break
        infoLogger.info('{}'.format(rs_before))
        infoLogger.info('{}'.format(rs_after))
        self.assertIn(rs_before.keys()[0][2], rs_after.keys()[0][2])
        self.confset(self.ns_leader, 'auto_failover', 'false')
        self.get_new_ns_leader()
        self.ns_drop(self.ns_leader, name)

    def test_auto_failover_kill_ns(self):
        """
        auto_failover打开后，下线一个tablet和nameserver,在超时时间内，tablet自动恢复，leader和下线的节点的offset保持一致
        :return:
        """
        self.confset(self.ns_leader, 'auto_failover', 'true')
        name = 't{}'.format(time.time())
        infoLogger.info(name)
        rs1 = self.ns_create_cmd(self.ns_leader, name, 144000, 1, 3, '')
        self.assertIn('Create table ok', rs1)

        number = 3
        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))
            self.assertIn('Put ok', rs_put)
        rs = self.showtable_with_tablename(self.ns_leader, name)
        rs = self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7, 8, 9, 10])
        tid = rs.keys()[0][1]
        pid = rs.keys()[0][2]
        self.print_table('', name)
        rs_before = self.gettablestatus(self.slave1, tid, pid)
        rs_before = self.parse_tb(rs_before, ' ', [0, 1, 2, 3], [4, 5, 6, 7,8, 9,10])
        for i in range(20):
            rs_before = self.gettablestatus(self.slave1, tid, pid)
            rs_before = self.parse_tb(rs_before, ' ', [0, 1, 2, 3], [4, 5, 6, 7,8, 9,10])
            if '{}'.format(rs_before) == 'gettablestatus failed':
                time.sleep(2)
                continue
            break
        infoLogger.info(rs_before)
        self.assertFalse('gettablestatus failed' in '{}'.format(rs_before))
        rs = self.ns_showopstatus(self.ns_leader)
        self.stop_client(self.slave1)
        time.sleep(2)
        self.stop_client(self.ns_leader)
        time.sleep(10)
        self.start_client(self.slave1)
        time.sleep(2)
        self.wait_op_done(name)
        rs_after = self.gettablestatus(self.slave1, tid, pid)
        rs_after = self.parse_tb(rs_after, ' ', [0, 1, 2, 3], [4, 5, 6, 7,8, 9,10])
        for i in range(20):
            time.sleep(2)
            rs_after = self.gettablestatus(self.slave1, tid, pid)
            rs_after = self.parse_tb(rs_after, ' ', [0, 1, 2, 3], [4, 5, 6, 7,8, 9,10])
            if '{}'.format(rs_after) == 'gettablestatus failed':
                continue
            if rs_before.keys()[0][2] == rs_after.keys()[0][2]:
                self.assertIn(rs_before.keys()[0][2], rs_after.keys()[0][2])
                break
        rs = self.ns_showopstatus(self.ns_slaver)
        if '{}'.format(rs_after) == 'gettablestatus failed':
            infoLogger.debug('{}'.format(rs))
        self.print_table(self.ns_slaver, '')
        self.assertIn(rs_before.keys()[0][2], rs_after.keys()[0][2])
        self.confset(self.ns_leader, 'auto_failover', 'false')
        self.start_client(self.ns_leader, 'nameserver')
        self.get_new_ns_leader()
        self.ns_drop(self.ns_leader, name)

    def test_auto_failover_disconnectzk(self):
        """
        auto_failover打开后，然后put数据，disconnectzk的所有tablet。最后一个节点状态应该不变
        :return:
        """
        self.confset(self.ns_leader, 'auto_failover', 'true')
        name = 't{}'.format(time.time())
        infoLogger.info(name)
        rs1 = self.ns_create_cmd(self.ns_leader, name, 144000, 1, 3, '')
        self.assertIn('Create table ok', rs1)

        number = 3
        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))
            self.assertIn('Put ok', rs_put)
        time.sleep(1)

        rs = self.disconnectzk(self.slave1)
        self.assertIn('disconnect zk ok', rs)
        time.sleep(1)
        rs = self.disconnectzk(self.slave2)
        self.assertIn('disconnect zk ok', rs)
        time.sleep(1)
        rs = self.disconnectzk(self.leader)
        self.assertIn('disconnect zk ok', rs)
        time.sleep(1)
        flag_yes = 0
        flag_no = 0
        for repeat in range(10):
            time.sleep(2)
            result = ['']
            rs_show = self.showtable(self.ns_leader)
            row = 0
            for i in rs_show:
                result.append(rs_show.values()[row][2])
                row = row + 1
            flag_yes = 0
            flag_no = 0
            for i in result:
                if i == 'yes':
                    flag_yes = flag_yes + 1
                if i == 'no':
                    flag_no = flag_no + 1
            if flag_no == 2 and flag_yes == 1:
                break
        self.assertEqual(flag_no, 2)
        self.assertEqual(flag_yes, 1)

        self.confset(self.ns_leader, 'auto_failover', 'false')

        self.connectzk(self.leader)
        self.connectzk(self.slave1)
        self.connectzk(self.slave2)
        self.get_new_ns_leader()
        time.sleep(3)
        self.ns_drop(self.ns_leader, name)

    def test_auto_failover_restart_tablet_twice(self):
        """
        重启两次tablet,kill掉一个tablet,然后重启，数据恢复后，再次重启。
        判断逻辑，通过put数据前后之差是否一致。如果一致，说明主从关系正常
        ；return:
        """
        self.confset(self.ns_leader, 'auto_failover', 'true')
        name = 't{}'.format(time.time())
        infoLogger.info(name)
        pid_number = 8
        endpoint_number = 3
        ttl = 144000
        rs1 = self.ns_create_cmd(self.ns_leader, name, ttl, pid_number, endpoint_number, '')
        self.assertIn('Create table ok', rs1)

        number = 3
        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))
            self.assertIn('Put ok', rs_put)

        time.sleep(2)
        rs = self.showtable_with_tablename(self.ns_leader, name)
        rs_before = self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7,8, 9,10])
        tid = rs_before.keys()[0][1]
        pid = rs_before.keys()[0][2]
        self.stop_client(self.slave1)
        time.sleep(5)
        self.start_client(self.slave1)
        time.sleep(1)
        self.wait_op_done(name)
        self.stop_client(self.slave1)
        time.sleep(5)
        self.start_client(self.slave1)
        time.sleep(1)
        self.wait_op_done(name)

        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))
            self.assertIn('Put ok', rs_put)

        rs = self.showtable_with_tablename(self.ns_leader, name)
        rs_after = self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7,8, 9,10])
        for repeat in range(20):
            offset_number = 0
            rs = self.showtable_with_tablename(self.ns_leader, name)
            rs_after = self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7,8, 9,10])
            for table_info in rs_after:
                if rs_after[table_info][4] == '2':
                    offset_number = offset_number + 1
            if offset_number == 9:
                break
            time.sleep(2)
        diff = 1
        for table_info in rs_after:
            if rs_after[table_info][4] == '0':
                continue
            self.assertEqual(diff, int(rs_after[table_info][4]) - int(rs_before[table_info][4]))

        self.confset(self.ns_leader, 'auto_failover', 'false')
        self.ns_drop(self.ns_leader, name)

    def test_auto_failover_restart_tablet_twice_continuously(self):
        """
        重启两次tablet,kill掉一个tablet,然后重启，数据正在恢复中，再次重启
        判断逻辑，通过put数据前后之差是否一致。如果一致，说明主从关系正常
        ；return:
        """
        self.confset(self.ns_leader, 'auto_failover', 'true')
        name = 't{}'.format(time.time())
        pid_number = 8
        endpoint_number = 3
        ttl = 144000
        rs1 = self.ns_create_cmd(self.ns_leader, name, ttl, pid_number, endpoint_number, '')
        self.assertIn('Create table ok', rs1)

        number = 3
        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))
            self.assertIn('Put ok', rs_put)

        time.sleep(2)
        rs = self.showtable_with_tablename(self.ns_leader, name)
        rs_before = self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7, 8, 9,10])

        self.stop_client(self.slave1)
        time.sleep(2)
        self.start_client(self.slave1)
        time.sleep(1)
        self.start_client(self.slave1)
        time.sleep(1)
        self.wait_op_done(name)

        # self.assertEqual(row, index)
        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))
            self.assertIn('Put ok', rs_put)

        rs = self.showtable_with_tablename(self.ns_leader, name)
        rs_after = self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7, 8, 9, 10])
        for i in range(10):
            time.sleep(2)
            offset_number = 0
            rs = self.showtable_with_tablename(self.ns_leader, name)
            rs_after = self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7, 8, 9, 10])
            for table_info in rs_after:
                if rs_after[table_info][4] == '2':
                    offset_number = offset_number + 1
            if offset_number == 9:
                break
        diff = 1
        for table_info in rs_after:
            if rs_after[table_info][4] == '0':
                continue
            self.assertEqual(diff, int(rs_after[table_info][4]) - int(rs_before[table_info][4]))

        self.confset(self.ns_leader, 'auto_failover', 'false')
        self.ns_drop(self.ns_leader, name)

if __name__ == "__main__":
    load(TestAutoFailover)
