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
class TestNsDropTable(TestCaseBase):


    def test_ns_drop_table(self):
        """
        通过ns建的表，可以通过ns全部drop掉，drop后gettablestatus为空
        :return:
        """
        metadata_path = '{}/metadata.txt'.format(self.testpath)
        name = 'tname{}'.format(time.time())
        m = utils.gen_table_metadata(
            '"{}"'.format(name), None, 144000, 2,
            ('table_partition', '"{}"'.format(self.leader), '"0-30"', 'true'),
            ('table_partition', '"{}"'.format(self.slave1), '"10-20"', 'false'),
            ('table_partition', '"{}"'.format(self.slave2), '"20-30"', 'false'),
            ('column_desc', '"k1"', '"string"', 'true'),
            ('column_desc', '"k2"', '"string"', 'false'),
            ('column_desc', '"k3"', '"string"', 'true'),
        )
        utils.gen_table_metadata_file(m, metadata_path)
        rs0 = self.ns_create(self.ns_leader, metadata_path)
        self.assertIn('Create table ok', rs0)

        rs1 = self.showtable(self.ns_leader)
        self.ns_drop(self.ns_leader, name)
        rs2 = self.showtable(self.ns_leader)
        rs3 = self.get_table_status(self.leader)

        self.assertEqual(len(rs1), 53)
        self.assertEqual(rs2, {})
        self.assertEqual(rs3, {})


if __name__ == "__main__":
    load(TestNsDropTable)
