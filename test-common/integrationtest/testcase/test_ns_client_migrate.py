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

    def get_base_attr(attr):
        TestCaseBase.setUpClass()
        return TestCaseBase.__getattribute__(TestCaseBase, attr)


    def createtable_put(self):
        self.confset(self.ns_leader, 'auto_failover', 'true')
        self.confset(self.ns_leader, 'auto_recover_table', 'true')
        self.tname = 'tname{}'.format(time.time())
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        m = utils.gen_table_metadata(
            '"{}"'.format(self.tname), '"kAbsoluteTime"', 144000, 8,
            ('table_partition', '"{}"'.format(self.leader), '"0-9"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"1-9"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"0"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'false'))
        utils.gen_table_metadata_file(m, metadata_path)
        rs = self.ns_create(self.ns_leader, metadata_path)
        self.assertEqual('Create table ok' in rs, True)
        table_info = self.showtable(self.ns_leader)
        self.tid = int(table_info.keys()[0][1])
        self.pid = 3
        self.put_large_datas(500, 7)


    @ddt.data(
        (get_base_attr('leader'), time.time(), '0,1,2', get_base_attr('slave1'), True),
    )
    @ddt.unpack
    def test_ns_client_migrate_args_check(self, src, tname, pid_group, des, migrate_ok):
        pass


    def test_ns_client_migrate_normal(self):
        """
        正常情况下迁移成功
        :return:
        """
        self.createtable_put()
        time.sleep(2)
        rs1 = self.get_table_status(self.slave1, self.tid, self.pid)
        rs2 = self.migrate(self.ns_leader, self.slave1, self.tname, "1-3", self.slave2)
        time.sleep(2)
        rs3 = self.showtable(self.ns_leader)
        rs4 = self.get_table_status(self.slave2, self.tid, self.pid)

        self.assertEqual('partition migrate ok' in rs2, True)
        for i in range(1, 4):
            self.assertEqual((self.tname, str(self.tid), str(i), self.slave1) in rs3, False)
            self.assertEqual((self.tname, str(self.tid), str(i), self.slave2) in rs3, True)
        self.assertEqual(rs1[0], rs4[0])


    def test_ns_client_migrate_no_leader(self):
        """
        无主状态下迁移失败
        :return:
        """
        self.confset(self.ns_leader, 'auto_failover', 'false')
        self.createtable_put()
        self.stop_client(self.leader)
        time.sleep(2)
        rs1 = self.migrate(self.ns_leader, self.slave1, self.tname, "4-6", self.slave2)
        time.sleep(2)
        rs2 = self.showtable(self.ns_leader)

        self.start_client(self.leader)
        time.sleep(10)
        self.showtable(self.ns_leader)
        self.confset(self.ns_leader, 'auto_failover', 'true')
        self.assertEqual('partition migrate ok' in rs1, True)
        for i in range(4, 7):
            self.assertEqual((self.tname, str(self.tid), str(i), self.slave1) in rs2, True)
            self.assertEqual((self.tname, str(self.tid), str(i), self.slave2) in rs2, False)


if __name__ == "__main__":
    load(TestNameserverMigrate)
