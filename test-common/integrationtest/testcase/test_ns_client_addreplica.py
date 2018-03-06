# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import time
from libs.test_loader import load
import libs.utils as utils
from libs.logger import infoLogger
from libs.deco import multi_dimension
import libs.ddt as ddt

@ddt.ddt
class TestAddReplicaNs(TestCaseBase):

    def get_base_attr(attr):
        TestCaseBase.setUpClass()
        return TestCaseBase.__getattribute__(TestCaseBase, attr)


    @multi_dimension(False)
    def test_addreplica_scenario(self):
        """
        创建主表，put数据后makesnapshot，添加副本后再put导主表，数据全部同步正确
        :return:
        """
        rs = self.showopstatus(self.ns_leader)
        old_last_op_id = max(rs.keys()) if rs != {} else 1
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(int(time.time() * 1000000 % 10000000000))
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-3"', 'true'),
            ('table_partition', '"{}"'.format(self.slave2), '"0"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"2-3"', 'false'),)
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs1)

        rs2 = self.showtable(self.ns_leader)
        tid = rs2.keys()[0][1]

        rs3 = self.put(self.leader, tid, 1, 'testkey0', self.now() + 10000, 'testvalue0')
        self.assertTrue('Put ok' in rs3)

        rs4 = self.makesnapshot(self.ns_leader, name, 1, 'ns_client')
        self.assertTrue('MakeSnapshot ok' in rs4)
        time.sleep(2)
        rs6 = self.addreplica(self.ns_leader, name, 1, 'ns_client', self.slave1)
        self.assertTrue('AddReplica ok' in rs6)
        time.sleep(5)
        self.showopstatus(self.ns_leader)
        last_op_id = max(self.showopstatus(self.ns_leader).keys())
        self.assertTrue(old_last_op_id != last_op_id)
        last_opstatus = self.showopstatus(self.ns_leader)[last_op_id]
        self.assertTrue('kAddReplicaOP', last_opstatus)

        self.put(self.leader, tid, 1, 'testkey0', self.now() + 10000, 'testvalue1')
        self.showtable(self.ns_leader)
        self.assertTrue('testvalue0' in self.scan(self.slave1, tid, 1, 'testkey0', self.now() + 90000, 1))
        self.assertTrue('testvalue1' in self.scan(self.slave1, tid, 1, 'testkey0', self.now() + 90000, 1))


    @multi_dimension(False)
    def test_addreplica_no_snapshot(self):
        """
        没有snapshot，添加副本成功，数据追加成功
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs1)
        rs2 = self.showtable(self.ns_leader)
        tid = rs2.keys()[0][1]
        rs3 = self.put(self.leader, tid, 1, 'testkey0', self.now() + 9999, 'testvalue0')
        self.assertTrue('Put ok' in rs3)
        rs4 = self.addreplica(self.ns_leader, name, 1, 'ns_client', self.slave1)
        self.assertTrue('AddReplica ok' in rs4)
        time.sleep(5)
        rs5 = self.showtable(self.ns_leader)
        self.assertEqual((name, tid, '1', self.leader) in rs5.keys(), True)
        self.assertEqual((name, tid, '1', self.slave1) in rs5.keys(), True)
        self.assertEqual('testvalue0' in self.scan(self.slave1, tid, 1, 'testkey0', self.now() + 9999, 1), True)


    @multi_dimension(False)
    def test_addreplica_offline(self):
        """
        添加一个offline的副本，添加失败
        :return:
        """
        self.start_client(self.slave1path)
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = '"tname{}"'.format(int(time.time() * 1000000 % 10000000000))
        m = utils.gen_table_metadata(
            name, None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs)

        self.stop_client(self.slave1)
        time.sleep(5)

        infoLogger.info(self.showtablet(self.ns_leader))
        rs2 = self.addreplica(self.ns_leader, name, 1, 'ns_client', self.slave1)
        self.assertTrue('Fail to addreplica. error msg:tablet is not online' in rs2)
        self.start_client(self.slave1path)


    @ddt.data(
        (None, None, get_base_attr('slave1'), 'AddReplica ok'),  # 需要log中看是fail的
        ('notexsit', None, None, 'Fail to addreplica. error msg:table is not  exist!'),
        (None, 10, None, 'Fail to addreplica. error msg:get leader failed'),
        (None, None, '127.1.1.1:6666', 'Fail to addreplica. error msg:tablet is not online'),
    )
    @ddt.unpack
    def test_addreplica_args_invalid(self, tname, pid, endpoint, exp_msg):
        """
        建表时带副本，然后添加新副本时，参数异常检查
        :return:
        """
        name = 't{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata('"{}"'.format(name), '"kLatestTime"', 100, 8,
                                     ('table_partition', '"{}"'.format(self.leader), '"0-2"', 'true'),
                                     ('table_partition', '"{}"'.format(self.slave1), '"0-1"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs1 = self.ns_create(self.ns_leader, metadata_path)
        self.assertTrue('Create table ok' in rs1)

        table_name = name if tname is None else tname
        tpid = 1 if pid is None else pid
        tendpoint = self.slave2 if endpoint is None else endpoint

        rs2 = self.addreplica(self.ns_leader, table_name, tpid, 'ns_client', tendpoint)
        self.assertEqual(exp_msg in rs2, True)


if __name__ == "__main__":
    load(TestAddReplicaNs)
