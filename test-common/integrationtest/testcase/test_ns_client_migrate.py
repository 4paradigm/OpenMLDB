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
class TestNameserverMigrate(TestCaseBase):

    leader, slave1, slave2 = (i for i in conf.tb_endpoints)

    def createtable_put(self, tname, data_count):
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(tname), '"kAbsoluteTime"', 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-10"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"3-8"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"0-3"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs)
        table_info = self.showtable(self.ns_leader)
        self.tid = int(table_info.keys()[0][1])
        self.pid = 4
        self.put_large_datas(data_count, 7)

    @ddt.data(
        ('4-6', [4, 5, 6]),
        ('4,6', [4, 6]),
        ('4', [4])
    )
    @ddt.unpack
    def test_ns_client_migrate_normal(self, pid_group, pid_range):
        """
        正常情况下迁移成功
        :param pid_group:
        :param pid_range:
        :return:
        """
        tname = str(time.time())
        self.createtable_put(tname, 500)
        time.sleep(2)
        rs1 = self.get_table_status(self.slave1)
        rs2 = self.get_table_status(self.slave2)
        rs3 = self.migrate(self.ns_leader, self.slave1, tname, pid_group, self.slave2)
        time.sleep(2)
        rs4 = self.showtable(self.ns_leader)
        rs5 = self.get_table_status(self.slave1)
        rs6 = self.get_table_status(self.slave2)
        self.assertIn('partition migrate ok', rs3)
        for i in pid_range:
            self.assertNotIn((tname, str(self.tid), str(i), self.slave1), rs4)
            self.assertIn((tname, str(self.tid), str(i), self.slave2), rs4)
            self.assertIn((self.tid, i), rs1.keys())
            self.assertNotIn((self.tid, i), rs2.keys())
            self.assertNotIn((self.tid, i), rs5.keys())
            self.assertIn((self.tid, i), rs6.keys())

    def test_ns_client_migrate_endpoint_offline(self):
        """
        节点离线，迁移失败
        """
        tname = str(time.time())
        self.createtable_put(tname, 1)
        self.stop_client(self.slave1)
        time.sleep(10)
        self.showtablet(self.ns_leader)
        self.showtable(self.ns_leader)
        rs1 = self.migrate(self.ns_leader, self.slave1, tname, '4-6', self.slave2)
        rs2 = self.migrate(self.ns_leader, self.slave2, tname, '0-2', self.slave1)
        self.start_client(self.slave1)
        time.sleep(10)
        self.assertIn('src_endpoint is not exist or not healthy', rs1)
        self.assertIn('des_endpoint is not exist or not healthy', rs2)

    @ddt.data(
        (slave1, time.time(), '4-6', slave1,
         'src_endpoint is same as des_endpoint'),
        (leader, time.time(), '4-6', slave1,
         'cannot migrate leader'),
        ('src_notexists', time.time(), '4-6', slave1,
         'src_endpoint is not exist or not healthy'),
        (slave1, time.time(), '4-6', 'des_notexists',
         'des_endpoint is not exist or not healthy'),
        (slave1, 'table_not_exists', '4-6', slave2,
         'table is not exist'),
        (slave1, time.time(), '20', slave2,
         'leader endpoint is empty'),
        (slave1, time.time(), 'pid', slave2,
         'format error'),
        (slave1, time.time(), '', slave2,
         'Bad format.'),
        (slave1, time.time(), '8-9', slave2,
         'failed to migrate partition'),
        (slave1, time.time(), '3-4', slave2,
         'is already in des_endpoint'),
        (slave1, time.time(), '6-4', slave2,
         'has not valid pid'),
        (slave1, time.time(), '8,9', slave2,
         'has not partition[9]'),
    )
    @ddt.unpack
    def test_ns_client_migrate_args_invalid(self, src, tname, pid_group, des, exp_msg):
        """
        参数异常时返回失败
        :param src:
        :param tname:
        :param pid_group:
        :param des:
        :param exp_msg:
        :return:
        """
        self.createtable_put(tname, 1)
        if tname != 'table_not_exists':
            rs2 = self.migrate(self.ns_leader, src, tname, pid_group, des)
        else:
            rs2 = self.migrate(self.ns_leader, src, 'table_not_exists_', pid_group, des)
        self.assertIn(exp_msg, rs2)


    def test_ns_client_migrate_failover_and_recover(self):  # RTIDB-252
        """
        迁移时发生故障切换，故障切换成功，迁移失败
        原leader故障恢复成follower之后，可以被迁移成功
        :return:
        """
        tname = str(time.time())
        self.createtable_put(tname, 100)
        time.sleep(2)
        rs0 = self.get_table_status(self.leader, self.tid, self.pid)  # get offset leader
        self.stop_client(self.leader)
        time.sleep(2)
        self.offlineendpoint(self.ns_leader, self.leader)
        rs1 = self.migrate(self.ns_leader, self.slave1, tname, '4-6', self.slave2)
        time.sleep(8)
        rs2 = self.showtable(self.ns_leader)

        self.start_client(self.leader)  # recover table
        time.sleep(5)
        self.recoverendpoint(self.ns_leader, self.leader)
        time.sleep(10)
        self.showtable(self.ns_leader)
        rs6 = self.get_table_status(self.slave1, self.tid, self.pid)  # get offset slave1
        rs3 = self.migrate(self.ns_leader, self.leader, tname, '4-6', self.slave2)
        time.sleep(2)
        rs4 = self.showtable(self.ns_leader)
        rs5 = self.get_table_status(self.slave2, self.tid, self.pid)  # get offset slave2
        self.showopstatus(self.ns_leader)

        self.assertIn('partition migrate ok', rs1)
        self.assertIn('partition migrate ok', rs3)
        for i in range(4, 7):
            self.assertIn((tname, str(self.tid), str(i), self.slave1), rs2)
            self.assertNotIn((tname, str(self.tid), str(i), self.slave2), rs2)
            self.assertNotIn((tname, str(self.tid), str(i), self.leader), rs4)
            self.assertIn((tname, str(self.tid), str(i), self.slave2), rs4)
        self.assertEqual(rs0[0], rs5[0])
        self.assertEqual(rs0[0], rs6[0])

    def test_ns_client_migrate_no_leader(self):
        """
        无主状态下迁移失败
        :return:
        """
        tname = str(time.time())
        self.createtable_put(tname, 500)
        self.stop_client(self.leader)
        time.sleep(2)
        rs1 = self.migrate(self.ns_leader, self.slave1, tname, "4-6", self.slave2)
        time.sleep(2)
        rs2 = self.showtable(self.ns_leader)

        self.start_client(self.leader)
        time.sleep(10)
        self.showtable(self.ns_leader)
        self.assertIn('partition migrate ok', rs1)
        for i in range(4, 7):
            self.assertIn((tname, str(self.tid), str(i), self.slave1), rs2)
            self.assertNotIn((tname, str(self.tid), str(i), self.slave2), rs2)


if __name__ == "__main__":
    load(TestNameserverMigrate)
