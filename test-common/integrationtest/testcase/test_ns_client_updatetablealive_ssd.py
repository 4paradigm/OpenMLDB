# -*- coding: utf-8 -*-
from testcasebase import TestCaseBase
import threading
import time
from libs.deco import multi_dimension
from libs.logger import infoLogger
import libs.ddt as ddt
from libs.test_loader import load
import libs.utils as utils


@ddt.ddt
class TestUpdateTableAlive(TestCaseBase):
    @ddt.data(
        ['kSSD'],
        ['kHDD'],
    )
    @ddt.unpack
    def test_update_table_alive_normal(self,storage_mode):
        """
        测试updatetable函数，设置参数为no和yes
        :return:
        """
        # rs_absolute1 = self.ns_create_cmd(self.ns_leader, 't1', '10', str(8), str(3), '')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": "t1",
            "ttl": 10,
            "partition_num": 8,
            "replica_num": 3,
            "storage_mode": storage_mode,
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs_absolute1 = self.ns_create(self.ns_leader, metadata_path)

        # rs_latest1 = self.ns_create_cmd(self.ns_leader, 'latest1', 'latest:10', str(8), str(3), '')

        metadata_path = '{}/metadata.txt'.format(self.testpath)
        table_meta = {
            "name": "latest1",
            "ttl": 10,
            "ttl_type": "kLatestTime",
            "partition_num": 8,
            "replica_num": 3,
            "storage_mode": "kSSD",
        }
        utils.gen_table_meta_file(table_meta, metadata_path)
        rs_latest1 = self.ns_create(self.ns_leader, metadata_path)

        self.assertIn('Create table ok', rs_absolute1)
        self.assertIn('Create table ok', rs_latest1)

        rs_absolute2 = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', 't1', '*', self.slave1, 'no')
        rs_absolute3 = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', 't1', '*', self.slave2, 'no')
        self.assertIn('update ok', rs_absolute2)
        self.assertIn('update ok', rs_absolute3)


        rs_absolute2 = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', 't1', '*', self.slave1, 'yes')
        rs_absolute3 = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', 't1', '*', self.slave2, 'yes')
        self.assertIn('update ok', rs_absolute2)
        self.assertIn('update ok', rs_absolute3)


        rs_absolute2 = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', 'tt1', '*', self.slave1, 'no')
        rs_absolute3 = self.ns_update_table_alive_cmd(self.ns_leader, 'updatetablealive', 't1', '10', self.slave2, 'no')
        self.assertIn('Fail to update table alive. error msg: table is not exist', rs_absolute2)
        self.assertIn('Fail to update table alive. error msg: no pid has update', rs_absolute3)

if __name__ == "__main__":
    load(TestUpdateTableAlive)
