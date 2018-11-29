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
class TestConfSetGet(TestCaseBase):

    @ddt.data(
        ('false', 'true', 'set auto_failover ok','true'),
        ('false', 'TRUE', 'set auto_failover ok','true'),
        ('true', 'FALSE', 'set auto_failover ok','false'),
        ('true', 'FalsE', 'set auto_failover ok','false'),
        ('true', '0', 'failed to set auto_failover. error msg: invalid value', 'true'),
        ('true', 'FAlsee', 'failed to set auto_failover. error msg: invalid value', 'true'),
        ('true', 'true', 'set auto_failover ok','true'),
        ('true', 'false', 'set auto_failover ok','false'),
    )
    @ddt.unpack
    def test_auto_failover_confset(self, pre_set, set_value, msg, get_value):
        """

        :return:
        """
        self.confset(self.ns_leader, 'auto_failover', pre_set)
        rs = self.confset(self.ns_leader, 'auto_failover', set_value)
        self.assertIn(msg, rs)
        rs1 = self.confget(self.ns_leader, 'auto_failover')
        self.assertIn(get_value, rs1)

if __name__ == "__main__":
    load(TestConfSetGet)
