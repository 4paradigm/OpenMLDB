# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt
import libs.conf as conf
from libs.utils import exe_shell
import sys
import os
sys.path.append(os.getenv('testpath'))

@ddt.ddt
class TestAddReplicaNs(TestCaseBase):

    leader, slave1, slave2 = (i for i in conf.tb_endpoints)

    @multi_dimension(False)
    def test_addreplica_scenario(self):  # RTIDB-250
        """
        创建主表，put数据后makesnapshot，添加副本后再put导主表，数据全部同步正确
        :return:
        """
        rs = self.showopstatus(self.ns_leader)
        old_last_op_id = max(rs.keys()) if rs != {} else 1
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        infoLogger.info(name)
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave2), '"0"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),)
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs1)

        rs2 = self.showtable(self.ns_leader, name)
        tid = rs2.keys()[0][1]

        rs3 = self.put(self.leader, tid, 1, 'testkey0', self.now() + 10000, 'testvalue0')
        self.assertIn('Put ok', rs3)

        rs4 = self.makesnapshot(self.ns_leader, name, 1, 'ns_client')
        self.assertIn('MakeSnapshot ok', rs4)
        self.assertFalse('Put ok' not in self.put(self.leader, tid, 1, 'testkey0', self.now() + 9999, 'test0.5'))
        time.sleep(2)
        rs6 = self.addreplica(self.ns_leader, name, 1, 'ns_client', self.slave1)
        self.assertIn('AddReplica ok', rs6)
        time.sleep(5)
        self.showopstatus(self.ns_leader)
        last_op_id = max(self.showopstatus(self.ns_leader).keys())
        self.assertTrue(old_last_op_id != last_op_id)
        last_opstatus = self.showopstatus(self.ns_leader)[last_op_id]
        self.assertIn('kAddReplicaOP', last_opstatus)

        self.put(self.leader, tid, 1, 'testkey0', self.now() + 10000, 'testvalue1')
        self.showtable(self.ns_leader, name)
        self.assertIn('testvalue0', self.scan(self.slave1, tid, 1, 'testkey0', self.now() + 90000, 1))
        self.assertIn('test0.5', self.scan(self.slave1, tid, 1, 'testkey0', self.now() + 90000, 1))
        self.assertIn('testvalue1', self.scan(self.slave1, tid, 1, 'testkey0', self.now() + 90000, 1))
        self.ns_drop(self.ns_leader, name)


    @multi_dimension(False)
    def test_addreplica_no_snapshot(self):
        """
        没有snapshot，添加副本成功，数据追加成功
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        infoLogger.info(name)
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs1)
        rs2 = self.showtable(self.ns_leader, name)
        tid = rs2.keys()[0][1]
        rs3 = self.put(self.leader, tid, 1, 'testkey0', self.now() + 9999, 'testvalue0')
        self.assertIn('Put ok', rs3)
        rs4 = self.addreplica(self.ns_leader, name, 1, 'ns_client', self.slave1)
        self.assertIn('AddReplica ok', rs4)
        time.sleep(5)
        rs5 = self.showtable(self.ns_leader, name)
        self.assertIn((name, tid, '1', self.leader), rs5.keys())
        self.assertIn((name, tid, '1', self.slave1), rs5.keys())
        self.assertIn('testvalue0', self.scan(self.slave1, tid, 1, 'testkey0', self.now() + 9999, 1))
        self.ns_drop(self.ns_leader, name)


    @multi_dimension(False)
    def test_addreplica_offline(self):
        """
        添加一个offline的副本，添加失败
        :return:
        """
        self.start_client(self.slave1)
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"tname{}"'.format(time.time())
        infoLogger.info(name)
        m = utils.gen_table_metadata(
            name, None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)

        self.stop_client(self.slave1)
        time.sleep(10)

        infoLogger.info(self.showtablet(self.ns_leader))
        rs2 = self.addreplica(self.ns_leader, name, 1, 'ns_client', self.slave1)
        self.assertIn('Fail to addreplica', rs2)
        self.start_client(self.slave1)
        self.ns_drop(self.ns_leader, name)


    @ddt.data(
        (None, None, slave1, 'AddReplica ok'),  # 需要log中看是fail的
        ('notexsit', None, None, 'Fail to addreplica'),
        (None, 10, None, 'Fail to addreplica'),
        (None, None, '127.1.1.1:6666', 'Fail to addreplica'),
    )
    @ddt.unpack
    def test_addreplica_args_invalid(self, tname, pid, endpoint, exp_msg):  # RTIDB-201
        """
        建表时带副本，然后添加新副本时，参数异常检查
        :return:
        """
        name = 't{}'.format(time.time())
        infoLogger.info(name)
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata('"{}"'.format(name), '"kLatestTime"', 100, 8,
                                     ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
                                     ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs1)

        table_name = name if tname is None else tname
        tpid = 1 if pid is None else pid
        tendpoint = self.slave2 if endpoint is None else endpoint

        rs2 = self.addreplica(self.ns_leader, table_name, tpid, 'ns_client', tendpoint)
        self.assertIn(exp_msg, rs2)
        self.ns_drop(self.ns_leader, name)

    @multi_dimension(False)
    def test_addreplica_check_binlog_sync_progress(self):
        """
        测试binlog在不同阈值的时候，数据追平之后，添加备份的状态是否为yes
        :return:
        """
        name = 't{}'.format(time.time())
        infoLogger.info(name)
        endponints = self.get_tablet_endpoints()

        rs1 = self.ns_create_cmd(self.ns_leader, name, 144000, 1, 2, '')
        self.assertIn('Create table ok' ,rs1)
        number = 200
        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))


        tables = self.showtable(self.ns_leader, name)
        tid = tables.keys()[0][1]
        pid = tables.keys()[0][2]
        table_endpoints = set()
        table_endpoints.add(tables.keys()[0][3])
        table_endpoints.add(tables.keys()[1][3])

        replica_endpoint = endponints - table_endpoints
        slave = replica_endpoint.pop()
        row = ''
        self.ns_addreplica(self.ns_leader, 'addreplica', name, pid, slave)
        for repeat in range(10):
            time.sleep(2)
            rs = self.ns_showopstatus(self.ns_leader)
            ops = self.parse_tb(rs, ' ', [0], [1, 2, 3, 4, 5, 6])
            row = ''
            infoLogger.debug('{}'.format(rs))
            for status in ops:
                if ops[status][1] == name and ops[status][3] == 'kDone':
                    row = status
                    break
            if row != '':
                break
        self.assertIn('kDone', ops[row][3])
        self.assertIn(name, ops[row][1])
        self.ns_drop(self.ns_leader, name)

    @multi_dimension(False)
    def test_configset_binlog_threshold(self):
        """
        修改check_binlog_sync_progress_delta 配置为0，添加新的副本后，查看offset追平
        :return:
        """
        test_path = os.getenv('testpath')

        name = 't{}'.format(time.time())
        infoLogger.info(name)
        self.stop_client(self.ns_leader)
        self.stop_client(self.ns_slaver)
        sleep(3);
        endponints = self.get_tablet_endpoints()
        conf = 'nameserver'

        client_path = self.node_path_dict[self.ns_leader]
        nameserver_path = '{}/conf/{}.flags'.format(client_path, conf)
        utils.exe_shell("echo '--check_binlog_sync_progress_delta=0' >> {}".format(nameserver_path))
        client_path = self.node_path_dict[self.ns_slaver]
        nameserver_path = '{}/conf/{}.flags'.format(client_path, conf)
        utils.exe_shell("echo '--check_binlog_sync_progress_delta=0' >> {}".format(nameserver_path))

        self.start_client(self.ns_leader,'nameserver')
        time.sleep(1)
        self.start_client(self.ns_slaver,'nameserver')
        time.sleep(1)
        self.get_new_ns_leader()

        rs1 = self.ns_create_cmd(self.ns_leader, name, 144000, 1, 2, '')
        self.assertIn('Create table ok', rs1)
        time.sleep(1)
        number = 20
        for i in range(number):
            rs_put = self.ns_put_kv(self.ns_leader, name, 'key{}'.format(i), self.now() - 1, 'value{}'.format(i))
            self.assertIn('Put ok', rs_put)

        time.sleep(1)
        tables = self.showtable(self.ns_leader, name)
        tid = tables.keys()[0][1]
        pid = tables.keys()[0][2]
        table_endpoints = set()
        table_endpoints.add(tables.keys()[0][3])
        table_endpoints.add(tables.keys()[1][3])

        replica_endpoint = endponints - table_endpoints
        slave = replica_endpoint.pop()

        self.ns_addreplica(self.ns_leader, 'addreplica', name, pid, slave)
        for i in range(10):
            time.sleep(2)
            rs = self.ns_showopstatus(self.ns_leader)
            tablestatus = self.parse_tb(rs, ' ', [0], [1, 2, 3, 4, 5, 6])
            op_id = tablestatus.keys()[0][0]
            ops = self.showopstatus(self.ns_leader)
            if ops[int(op_id)][1] != 'kDone':
                continue
            rs = self.gettablestatus(slave, tid, pid)
            table_status = self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6])
            rs = self.showtable_with_tablename(self.ns_leader, name)
            table_infos = self.parse_tb(rs, ' ', [0, 1, 2, 3], [4, 5, 6, 7,8, 9,10])
            for table_info in table_infos:
                self.assertEqual(table_status.keys()[0][2], table_infos[table_info][4])
            break
        self.ns_drop(self.ns_leader, name)

if __name__ == "__main__":
    load(TestAddReplicaNs)
