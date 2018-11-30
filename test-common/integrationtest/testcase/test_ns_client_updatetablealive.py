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

    def test_update_table_alive_normal(self):
        """
        测试updatetable函数，设置参数为no和yes
        :return:
        """
        rs_absolute1 = self.ns_create_cmd(self.ns_leader, 't1', '10', str(8), str(3), '')
        rs_latest1 = self.ns_create_cmd(self.ns_leader, 'latest1', 'latest:10', str(8), str(3), '')
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



